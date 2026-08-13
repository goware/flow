package store

import (
	"bytes"
	"context"
	"crypto/sha256"
	"encoding/json"
	"errors"
	"fmt"
	"slices"
	"strings"
	"time"

	"github.com/goware/flow/internal/canonical"
	"github.com/goware/flow/internal/durable"
	"github.com/goware/flow/internal/failure"
	"github.com/goware/flow/internal/fault"
	"github.com/goware/flow/internal/flowerr"
	"github.com/goware/flow/internal/pgschema"
	retrypolicy "github.com/goware/flow/internal/retry"
	"github.com/goware/flow/internal/store/journalcodec"
	"github.com/goware/flow/internal/uuid"
	"github.com/jackc/pgx/v5"
)

type CommandKind struct {
	Name    string
	Version int
}

type CommandCandidate struct {
	CommandID uuid.UUID
	RunID     uuid.UUID
	Queue     string
	Name      string
	Version   int
	NextRunAt time.Time
}

type CommandProbeCursor struct {
	NextRunAt time.Time
	Queue     string
	CommandID uuid.UUID
}

type CommandProbeResult struct {
	Candidates  []CommandCandidate
	FutureDelay *time.Duration
}

func (s *Store) ProbeCommands(ctx context.Context, kinds []CommandKind, limit int) ([]CommandCandidate, error) {
	result, err := s.ProbeCommandsExcluding(ctx, kinds, limit, nil, nil, nil)
	return result.Candidates, err
}

// ProbeCommandsExcluding returns runnable candidates while omitting runs
// already found to be busy and queues with no process-local lane capacity
// during the caller's current scheduling pass. This lets a bounded probe make
// room for other work without broadening the database transaction that tests
// a run fence.
func (s *Store) ProbeCommandsExcluding(
	ctx context.Context,
	kinds []CommandKind,
	limit int,
	excludedRunIDs []uuid.UUID,
	excludedQueues []string,
	after *CommandProbeCursor,
) (CommandProbeResult, error) {
	if len(kinds) == 0 || limit <= 0 {
		return CommandProbeResult{}, nil
	}
	for _, runID := range excludedRunIDs {
		if runID == uuid.Nil {
			return CommandProbeResult{}, fmt.Errorf("%w: invalid excluded run", flowerr.ErrInvalid)
		}
	}
	for _, queue := range excludedQueues {
		if queue == "" {
			return CommandProbeResult{}, fmt.Errorf("%w: invalid excluded queue", flowerr.ErrInvalid)
		}
	}
	var afterNextRunAt *time.Time
	afterQueue := ""
	afterCommandID := uuid.Nil
	if after != nil {
		if after.NextRunAt.IsZero() || after.Queue == "" || after.CommandID == uuid.Nil {
			return CommandProbeResult{}, fmt.Errorf("%w: invalid command probe cursor", flowerr.ErrInvalid)
		}
		afterNextRunAt, afterQueue, afterCommandID = &after.NextRunAt, after.Queue, after.CommandID
	}
	names := make([]string, len(kinds))
	versions := make([]int32, len(kinds))
	for index, kind := range kinds {
		if kind.Name == "" || kind.Version <= 0 {
			return CommandProbeResult{}, fmt.Errorf("%w: invalid command probe kind", flowerr.ErrInvalid)
		}
		version, err := durable.PostgresInteger32("command probe version", kind.Version, 1, durable.PostgresIntegerMax)
		if err != nil {
			return CommandProbeResult{}, err
		}
		names[index], versions[index] = kind.Name, version
	}
	rows, err := s.db.Conn.Query(ctx, `WITH observed AS MATERIALIZED (
		SELECT clock_timestamp() AS now
	), handled(name,version) AS (
		SELECT * FROM unnest($1::text[],$2::integer[])
	), future_at AS (
		SELECT MIN(candidate.next_run_at) AS next_run_at
		FROM handled h
		JOIN `+pgschema.Table(s.schema, "flow_command_queue")+` candidate
		  ON candidate.name=h.name AND candidate.version=h.version
		JOIN `+pgschema.Table(s.schema, "flow_runs")+` e ON e.run_id=candidate.run_id
		CROSS JOIN observed
		WHERE candidate.state IN ('ready','retry_wait')
		  AND candidate.next_run_at>observed.now
		  AND e.status IN ('running','failing')
	), future AS (
		SELECT CASE WHEN future_at.next_run_at IS NULL THEN NULL
			ELSE GREATEST(0::double precision,
				EXTRACT(EPOCH FROM future_at.next_run_at-observed.now)) END AS delay_seconds
		FROM future_at CROSS JOIN observed
	), due AS (
		SELECT q.command_id,q.run_id,q.queue,q.name,q.version,q.next_run_at
		FROM handled h
		CROSS JOIN observed
		CROSS JOIN LATERAL (
			SELECT candidate.command_id,candidate.run_id,candidate.queue,candidate.name,candidate.version,candidate.next_run_at
			FROM `+pgschema.Table(s.schema, "flow_command_queue")+` candidate
			WHERE candidate.name=h.name AND candidate.version=h.version
			  AND candidate.state IN ('ready','retry_wait') AND candidate.next_run_at<=observed.now
			  AND NOT (candidate.run_id=ANY(COALESCE($4::uuid[],'{}'::uuid[])))
			  AND NOT (candidate.queue=ANY(COALESCE($5::text[],'{}'::text[])))
			  AND ($6::timestamptz IS NULL OR
			       (candidate.next_run_at,candidate.queue,candidate.command_id)>($6::timestamptz,$7::text,$8::uuid))
			ORDER BY candidate.next_run_at,candidate.queue,candidate.command_id
			LIMIT $3
		) q
		JOIN `+pgschema.Table(s.schema, "flow_runs")+` e ON e.run_id=q.run_id
		WHERE e.status IN ('running','failing')
		ORDER BY q.next_run_at,q.queue,q.command_id
		LIMIT $3
	)
	SELECT due.command_id,due.run_id,due.queue,due.name,due.version,due.next_run_at,future.delay_seconds
	FROM future LEFT JOIN due ON true
	ORDER BY due.next_run_at,due.queue,due.command_id`, names, versions, limit, excludedRunIDs, excludedQueues,
		afterNextRunAt, afterQueue, afterCommandID)
	if err != nil {
		return CommandProbeResult{}, MapError("probe commands", err)
	}
	defer rows.Close()
	result := CommandProbeResult{Candidates: make([]CommandCandidate, 0, limit)}
	for rows.Next() {
		var commandID, runID *uuid.UUID
		var queue, name *string
		var version *int
		var nextRunAt *time.Time
		var delaySeconds *float64
		if err := rows.Scan(&commandID, &runID, &queue, &name, &version, &nextRunAt, &delaySeconds); err != nil {
			return CommandProbeResult{}, MapError("scan command candidate", err)
		}
		if delaySeconds != nil && result.FutureDelay == nil {
			delay := max(0, time.Duration(*delaySeconds*float64(time.Second)))
			result.FutureDelay = &delay
		}
		if commandID != nil {
			result.Candidates = append(result.Candidates, CommandCandidate{
				CommandID: *commandID, RunID: *runID, Queue: *queue, Name: *name,
				Version: *version, NextRunAt: *nextRunAt,
			})
		}
	}
	if err := rows.Err(); err != nil {
		return CommandProbeResult{}, MapError("read command candidates", err)
	}
	return result, nil
}

type ClaimedCommand struct {
	CommandID              uuid.UUID
	RunID                  uuid.UUID
	RunKey                 string
	CommandKey             string
	Name                   string
	Version                int
	Queue                  string
	Args                   []byte
	EventInputs            []ClaimedEventInput
	RetryMaxElapsed        *time.Duration
	AttemptTimeout         time.Duration
	CreatedAt              time.Time
	BudgetStartedAt        time.Time
	RunDeadline            *time.Time
	Attempt                int
	ConsumedAttempts       int
	AttemptID              uuid.UUID
	LeaseToken             uuid.UUID
	DBNow                  time.Time
	LeaseDuration          time.Duration
	LeaseExpiresAt         time.Time
	LocalLeaseExpiresAt    time.Time
	AttemptStartedPosition int64
}

type ClaimedEventInput struct {
	Name     string
	Key      string
	Position int64
	Payload  []byte
}

type ClaimResult struct {
	Command    *ClaimedCommand
	Progressed bool
}

type ClaimBatchResult struct {
	Commands   []ClaimedCommand
	Progressed bool
}

func (s *Store) ClaimCommand(
	ctx context.Context,
	candidate CommandCandidate,
	defaultLease time.Duration,
	owner string,
	hook fault.Hook,
) (ClaimResult, error) {
	batch, err := s.ClaimCommands(ctx, []CommandCandidate{candidate}, defaultLease, owner, hook)
	result := ClaimResult{Progressed: batch.Progressed}
	if len(batch.Commands) > 0 {
		result.Command = &batch.Commands[0]
	}
	return result, err
}

// ClaimCommands claims a bounded set of candidates from one run under
// one run lock and one commit. Candidate rows remain individually
// skip-locked, so a busy sibling does not make the batch wait.
func (s *Store) ClaimCommands(
	ctx context.Context,
	candidates []CommandCandidate,
	defaultLease time.Duration,
	owner string,
	hook fault.Hook,
) (ClaimBatchResult, error) {
	if len(candidates) == 0 {
		return ClaimBatchResult{}, nil
	}
	runID := candidates[0].RunID
	seen := make(map[uuid.UUID]struct{}, len(candidates))
	for _, candidate := range candidates {
		if candidate.CommandID == uuid.Nil || candidate.RunID == uuid.Nil || candidate.RunID != runID ||
			candidate.Name == "" || candidate.Version <= 0 {
			return ClaimBatchResult{}, fmt.Errorf("%w: incomplete or mixed-run command claim batch", flowerr.ErrInvalid)
		}
		if err := durable.PostgresInteger("queue command version", candidate.Version, 1, durable.PostgresIntegerMax); err != nil {
			return ClaimBatchResult{}, err
		}
		if _, duplicate := seen[candidate.CommandID]; duplicate {
			return ClaimBatchResult{}, fmt.Errorf("%w: duplicate command in claim batch", flowerr.ErrInvalid)
		}
		seen[candidate.CommandID] = struct{}{}
	}
	if defaultLease <= 0 || owner == "" {
		return ClaimBatchResult{}, fmt.Errorf("%w: incomplete command claim", flowerr.ErrInvalid)
	}
	if _, err := durable.ExactMilliseconds("default command lease", defaultLease); err != nil {
		return ClaimBatchResult{}, err
	}
	if hook == nil {
		hook = fault.None{}
	}
	if err := hook.Hit(ctx, fault.ClaimRunLock); err != nil {
		return ClaimBatchResult{}, err
	}
	semantic, err := s.BeginSemantic(ctx, runID, LockSkipLocked)
	if errors.Is(err, ErrLockUnavailable) {
		return ClaimBatchResult{}, nil
	}
	if err != nil {
		return ClaimBatchResult{}, err
	}
	defer semantic.Rollback(context.WithoutCancel(ctx))

	initial, ok := semantic.InitialLockedSnapshot()
	if !ok {
		return ClaimBatchResult{}, fmt.Errorf("%w: claim run has no initial locked snapshot", flowerr.ErrInvalidState)
	}
	if (initial.Head.Status != "running" && initial.Head.Status != "failing") ||
		(initial.Deadline != nil && !semantic.DBNow().Before(*initial.Deadline)) {
		return ClaimBatchResult{}, nil
	}

	locked, err := s.lockClaimBatch(ctx, semantic, candidates)
	if err != nil {
		return ClaimBatchResult{}, err
	}
	if len(locked) == 0 {
		return ClaimBatchResult{}, nil
	}
	commandIDs := make([]uuid.UUID, len(locked))
	for index := range locked {
		commandIDs[index] = locked[index].candidate.CommandID
	}
	eventInputs, err := s.loadClaimedEventInputBatch(ctx, semantic, commandIDs)
	if err != nil {
		return ClaimBatchResult{}, err
	}
	claimable := make([]claimBatchCommand, 0, len(locked))
	expired := make([]claimBatchCommand, 0)
	for index := range locked {
		command := locked[index]
		command.eventInputs = eventInputs[command.candidate.CommandID]
		policy, policyErr := retrypolicy.PublicFromCanonical(command.policyBytes)
		if policyErr != nil {
			return ClaimBatchResult{}, fmt.Errorf("%w: stored retry policy is invalid", flowerr.ErrInvalidState)
		}
		command.retryPolicy = retrypolicy.ValueOf(policy)
		if command.retryPolicy.MaxElapsed != nil {
			retryDeadline, addErr := durable.AddExactDuration("retry elapsed bound", command.budgetStartedAt,
				*command.retryPolicy.MaxElapsed)
			if addErr != nil {
				return ClaimBatchResult{}, addErr
			}
			if !semantic.DBNow().Before(retryDeadline) {
				expired = append(expired, command)
				continue
			}
		}
		command.attempt, err = durable.AddPostgresInteger("attempt ordinal", command.ordinal, 1, 0, durable.PostgresIntegerMax)
		if err != nil {
			return ClaimBatchResult{}, err
		}
		if command.timeoutMS != nil {
			command.attemptTimeout, err = durable.MillisecondsDuration("stored command attempt timeout", *command.timeoutMS)
			if err != nil {
				return ClaimBatchResult{}, fmt.Errorf("%w: invalid stored command attempt timeout", flowerr.ErrInvalidState)
			}
		}
		command.leaseDuration = defaultLease
		if command.recoveryLeaseMS != nil {
			command.leaseDuration, err = durable.MillisecondsDuration("stored recovery lease", *command.recoveryLeaseMS)
			if err != nil || command.leaseDuration < 30*time.Millisecond {
				return ClaimBatchResult{}, fmt.Errorf("%w: invalid stored recovery lease", flowerr.ErrInvalidState)
			}
		}
		command.leaseMilliseconds, err = durable.ExactMilliseconds("resolved command lease", command.leaseDuration)
		if err != nil {
			return ClaimBatchResult{}, err
		}
		command.leaseExpiresAt, err = durable.AddExactDuration("resolved command lease", semantic.DBNow(), command.leaseDuration)
		if err != nil {
			return ClaimBatchResult{}, err
		}
		command.attemptID, command.leaseToken = uuid.New(), uuid.New()
		claimable = append(claimable, command)
	}

	result := ClaimBatchResult{Commands: make([]ClaimedCommand, 0, len(claimable))}
	current := semantic
	if len(claimable) > 0 {
		if err := hook.Hit(ctx, fault.ClaimBeforeJournal); err != nil {
			return ClaimBatchResult{}, err
		}
		entries := make([]JournalEntry, len(claimable))
		for index := range claimable {
			command := &claimable[index]
			started, entryErr := NewJournalEntry(AttemptStarted, journalcodec.AttemptStartedBody{
				V: 1, AttemptID: command.attemptID.String(), CommandID: command.candidate.CommandID.String(),
				CommandKey: command.key, Attempt: command.attempt, StartedAt: semantic.DBNow(), Worker: owner,
				LeaseDurationMS: command.leaseMilliseconds, ConsumedAttempts: command.consumed,
				BudgetStartedAt: command.budgetStartedAt,
			})
			if entryErr != nil {
				return ClaimBatchResult{}, entryErr
			}
			started.CommandID = clonePointer(&command.candidate.CommandID)
			started.AttemptID = clonePointer(&command.attemptID)
			started.CausationPosition = clonePointer(&command.createdPosition)
			entries[index] = started
		}
		journal, applyErr := semantic.Apply(ctx, PersistedChangeSet{Journal: entries})
		if applyErr != nil {
			return ClaimBatchResult{}, applyErr
		}
		if err := s.updateClaimBatch(ctx, semantic, claimable, owner); err != nil {
			return ClaimBatchResult{}, err
		}
		for index := range claimable {
			command := claimable[index]
			row := journal.Journal[index]
			if row.Kind != AttemptStarted || row.CommandID == nil || *row.CommandID != command.candidate.CommandID ||
				row.AttemptID == nil || *row.AttemptID != command.attemptID {
				return ClaimBatchResult{}, fmt.Errorf("%w: claimed attempt journal mapping differs", flowerr.ErrInvalidState)
			}
			result.Commands = append(result.Commands, ClaimedCommand{
				CommandID: command.candidate.CommandID, RunID: command.candidate.RunID, RunKey: initial.RunKey,
				CommandKey: command.key, Name: command.name, Version: command.version, Queue: command.queue,
				Args: slices.Clone(command.args), EventInputs: command.eventInputs,
				RetryMaxElapsed: clonePointer(command.retryPolicy.MaxElapsed), AttemptTimeout: command.attemptTimeout,
				CreatedAt: command.createdAt, BudgetStartedAt: command.budgetStartedAt,
				RunDeadline: clonePointer(initial.Deadline), Attempt: command.attempt,
				ConsumedAttempts: command.consumed, AttemptID: command.attemptID, LeaseToken: command.leaseToken,
				DBNow: semantic.DBNow(), LeaseDuration: command.leaseDuration,
				LeaseExpiresAt: command.leaseExpiresAt, AttemptStartedPosition: row.Position,
			})
		}
		result.Progressed = true
		current = semantic.continueBatch()
	}

	// Elapsed-budget expiry is uncommon and can fail the run, append several
	// semantic rows, and terminalize the run. Keep that transition focused
	// and auditable after ordinary siblings have installed their running fences.
	for index := range expired {
		command := expired[index]
		eligible, eligibleErr := s.claimCandidateStillEligible(ctx, current, command.candidate.CommandID)
		if eligibleErr != nil {
			return ClaimBatchResult{}, eligibleErr
		}
		if !eligible {
			continue
		}
		if err := s.failBeforeClaimLocked(ctx, current, command.candidate.CommandID, command.key,
			"retry_elapsed", "retry elapsed budget expired", command.createdPosition); err != nil {
			return ClaimBatchResult{}, err
		}
		result.Progressed = true
		current = current.continueBatch()
	}
	if !result.Progressed {
		return result, nil
	}
	if err := hook.Hit(ctx, fault.ClaimBeforeCommit); err != nil {
		return ClaimBatchResult{}, err
	}
	if err := semantic.Commit(ctx); err != nil {
		// Once Commit has been attempted PostgreSQL's outcome can be ambiguous.
		// Preserve every prepared attempt fence so the runtime can resolve or
		// conservatively account for it instead of abandoning a committed claim.
		return result, err
	}
	if err := hook.Hit(ctx, fault.ClaimCommitAmbiguous); err != nil {
		return result, err
	}
	return result, nil
}

type claimBatchCommand struct {
	candidate         CommandCandidate
	key               string
	name              string
	version           int
	queue             string
	args              []byte
	timeoutMS         *int64
	recoveryLeaseMS   *int64
	policyBytes       []byte
	createdAt         time.Time
	budgetStartedAt   time.Time
	ordinal           int
	consumed          int
	createdPosition   int64
	retryPolicy       retrypolicy.Policy
	attemptTimeout    time.Duration
	leaseDuration     time.Duration
	leaseMilliseconds int64
	leaseExpiresAt    time.Time
	attempt           int
	attemptID         uuid.UUID
	leaseToken        uuid.UUID
	eventInputs       []ClaimedEventInput
}

func (s *Store) lockClaimBatch(ctx context.Context, semantic *SemanticTx, candidates []CommandCandidate) ([]claimBatchCommand, error) {
	commandIDs := make([]uuid.UUID, len(candidates))
	for index := range candidates {
		commandIDs[index] = candidates[index].CommandID
	}
	rows, err := semantic.PGX().Query(ctx, `WITH requested AS (
		SELECT command_id,ordinality FROM unnest($1::uuid[]) WITH ORDINALITY AS r(command_id,ordinality)
	)
	SELECT r.ordinality,c.command_key,c.name,c.version,c.args,c.queue,c.attempt_timeout_ms,c.recovery_lease_ms,
		c.retry_policy,c.created_at,c.budget_started_at,c.attempt_ordinal,c.consumed_attempts,
		c.created_position,c.state,q.state,q.next_run_at
	FROM requested r
	JOIN `+pgschema.Table(s.schema, "flow_command_queue")+` q ON q.command_id=r.command_id
	JOIN `+pgschema.Table(s.schema, "flow_commands")+` c ON c.command_id=q.command_id
	WHERE q.run_id=$2
	ORDER BY r.ordinality
	FOR UPDATE OF q,c SKIP LOCKED`, commandIDs, semantic.RunID())
	if err != nil {
		return nil, MapError("lock command claim batch", err)
	}
	defer rows.Close()
	locked := make([]claimBatchCommand, 0, len(candidates))
	for rows.Next() {
		var ordinality int64
		var command claimBatchCommand
		var commandState, queueState string
		var nextRunAt time.Time
		if err := rows.Scan(&ordinality, &command.key, &command.name, &command.version, &command.args, &command.queue,
			&command.timeoutMS, &command.recoveryLeaseMS, &command.policyBytes, &command.createdAt, &command.budgetStartedAt, &command.ordinal,
			&command.consumed, &command.createdPosition, &commandState, &queueState, &nextRunAt); err != nil {
			return nil, MapError("scan command claim batch", err)
		}
		if ordinality < 1 || ordinality > int64(len(candidates)) {
			return nil, fmt.Errorf("%w: claimed command batch order differs", flowerr.ErrInvalidState)
		}
		command.candidate = candidates[ordinality-1]
		if command.name != command.candidate.Name || command.version != command.candidate.Version ||
			command.queue != command.candidate.Queue || (commandState != "ready" && commandState != "retry_wait") ||
			commandState != queueState || semantic.DBNow().Before(nextRunAt) {
			continue
		}
		locked = append(locked, command)
	}
	if err := rows.Err(); err != nil {
		return nil, MapError("read command claim batch", err)
	}
	return locked, nil
}

func (s *Store) loadClaimedEventInputBatch(
	ctx context.Context,
	semantic *SemanticTx,
	commandIDs []uuid.UUID,
) (map[uuid.UUID][]ClaimedEventInput, error) {
	if len(commandIDs) == 0 {
		return nil, nil
	}
	rows, err := semantic.PGX().Query(ctx, `SELECT w.event_name,w.event_key,w.satisfied_position,
		j.event_namespace,j.event_name,j.event_key,j.event_class,j.body,j.body_hash,w.command_id
		FROM `+pgschema.Table(s.schema, "flow_command_event_waits")+` w
		LEFT JOIN `+pgschema.Table(s.schema, "flow_journal")+` j
		  ON j.run_id=w.run_id AND j.position=w.satisfied_position
		WHERE w.command_id=ANY($1::uuid[])
		ORDER BY w.command_id,w.event_name,w.event_key`, commandIDs)
	if err != nil {
		return nil, MapError("load claimed event input batch", err)
	}
	defer rows.Close()
	inputs := make(map[uuid.UUID][]ClaimedEventInput, len(commandIDs))
	for rows.Next() {
		var commandID uuid.UUID
		var name, key string
		var position *int64
		var namespace, journalName, journalKey, class *string
		var body, bodyHash []byte
		if err := rows.Scan(&name, &key, &position, &namespace, &journalName, &journalKey, &class, &body, &bodyHash,
			&commandID); err != nil {
			return nil, MapError("scan claimed event input", err)
		}
		if len(inputs[commandID]) >= MaxCommandEventWaits {
			return nil, fmt.Errorf("%w: claimed command exceeds event-wait limit", flowerr.ErrInvalidState)
		}
		if position == nil || namespace == nil || journalName == nil || journalKey == nil || class == nil ||
			*namespace != "application" || *class != "application" || *journalName != name || *journalKey != key {
			return nil, fmt.Errorf("%w: command event input has an invalid satisfying journal row", flowerr.ErrInvalidState)
		}
		if digest := sha256.Sum256(body); !bytes.Equal(digest[:], bodyHash) {
			return nil, fmt.Errorf("%w: command event input body is invalid", flowerr.ErrInvalidState)
		}
		decoded, err := journalcodec.DecodeApplicationEvent(body)
		if err != nil {
			return nil, fmt.Errorf("%w: command event input body cannot be decoded", flowerr.ErrInvalidState)
		}
		inputs[commandID] = append(inputs[commandID], ClaimedEventInput{
			Name: name, Key: key, Position: *position, Payload: decoded.Payload,
		})
	}
	if err := rows.Err(); err != nil {
		return nil, MapError("read claimed event input batch", err)
	}
	return inputs, nil
}

func (s *Store) updateClaimBatch(
	ctx context.Context,
	semantic *SemanticTx,
	commands []claimBatchCommand,
	owner string,
) error {
	commandIDs := make([]uuid.UUID, len(commands))
	attemptIDs := make([]uuid.UUID, len(commands))
	leaseTokens := make([]uuid.UUID, len(commands))
	leaseExpiries := make([]time.Time, len(commands))
	attempts := make([]int32, len(commands))
	for index := range commands {
		commandIDs[index] = commands[index].candidate.CommandID
		attemptIDs[index] = commands[index].attemptID
		leaseTokens[index] = commands[index].leaseToken
		leaseExpiries[index] = commands[index].leaseExpiresAt
		attempts[index] = int32(commands[index].attempt)
	}
	queueSQL := `WITH claimed(command_id,attempt_id,lease_token,lease_expires_at) AS (
		SELECT * FROM unnest($1::uuid[],$2::uuid[],$3::uuid[],$4::timestamptz[])
	)
	UPDATE ` + pgschema.Table(s.schema, "flow_command_queue") + ` q
	SET state='running',active_attempt_id=claimed.attempt_id,lease_token=claimed.lease_token,lease_owner=$5,
	    lease_started_at=$6,lease_expires_at=claimed.lease_expires_at
	FROM claimed
	WHERE q.command_id=claimed.command_id AND q.run_id=$7 AND q.state IN ('ready','retry_wait')`
	commandSQL := `WITH claimed(command_id,attempt) AS (
		SELECT * FROM unnest($1::uuid[],$2::integer[])
	)
	UPDATE ` + pgschema.Table(s.schema, "flow_commands") + ` c
	SET state='running',attempt_ordinal=claimed.attempt,updated_at=$3,status_at=$3
	FROM claimed
	WHERE c.command_id=claimed.command_id AND c.run_id=$4 AND c.state IN ('ready','retry_wait')`
	want := int64(len(commands))
	return executeProjectionBatch(ctx, semantic.PGX(),
		projectionStatement{
			operation: "claim command queue batch", query: queueSQL,
			arguments: []any{commandIDs, attemptIDs, leaseTokens, leaseExpiries, owner, semantic.DBNow(), semantic.RunID()},
			validateRows: func(got int64) error {
				if got != want {
					return fmt.Errorf("%w: claimed %d of %d command queue rows", flowerr.ErrInvalidState, got, want)
				}
				return nil
			},
		},
		projectionStatement{
			operation: "mark command batch running", query: commandSQL,
			arguments: []any{commandIDs, attempts, semantic.DBNow(), semantic.RunID()},
			validateRows: func(got int64) error {
				if got != want {
					return fmt.Errorf("%w: marked %d of %d commands running", flowerr.ErrInvalidState, got, want)
				}
				return nil
			},
		},
	)
}

type projectionStatement struct {
	operation    string
	query        string
	arguments    []any
	validateRows func(int64) error
}

func executeProjectionBatch(ctx context.Context, tx pgx.Tx, statements ...projectionStatement) error {
	batch := &pgx.Batch{}
	for _, statement := range statements {
		batch.Queue(statement.query, statement.arguments...)
	}
	results := tx.SendBatch(ctx, batch)
	var resultErr error
	for _, statement := range statements {
		tag, err := results.Exec()
		if err != nil {
			if resultErr == nil {
				resultErr = MapError(statement.operation, err)
			}
			continue
		}
		if statement.validateRows != nil {
			if err := statement.validateRows(tag.RowsAffected()); err != nil && resultErr == nil {
				resultErr = err
			}
		}
	}
	if err := results.Close(); err != nil && resultErr == nil {
		resultErr = MapError("close projection batch", err)
	}
	return resultErr
}

func (s *Store) claimCandidateStillEligible(ctx context.Context, semantic *SemanticTx, commandID uuid.UUID) (bool, error) {
	var eligible bool
	if err := semantic.PGX().QueryRow(ctx, `SELECT EXISTS (
		SELECT 1 FROM `+pgschema.Table(s.schema, "flow_commands")+` c
		JOIN `+pgschema.Table(s.schema, "flow_command_queue")+` q ON q.command_id=c.command_id
		WHERE c.command_id=$1 AND c.run_id=$2 AND c.state IN ('ready','retry_wait')
		  AND q.state=c.state AND q.next_run_at<=$3
	)`, commandID, semantic.RunID(), semantic.DBNow()).Scan(&eligible); err != nil {
		return false, MapError("recheck elapsed claim candidate", err)
	}
	return eligible, nil
}

func (s *Store) failBeforeClaimLocked(
	ctx context.Context,
	semantic *SemanticTx,
	commandID uuid.UUID,
	key string,
	code, message string,
	causation int64,
) error {
	head, err := s.LoadRunHead(ctx, semantic)
	if err != nil {
		return err
	}
	failureEffects, err := s.resolveCommandFailureLocked(ctx, semantic, commandID, "failed")
	if err != nil {
		return err
	}
	commandEvent, err := terminalEventWithCode(commandID, key, "failed", code, message, "flow.command_failed", "command_terminal")
	if err != nil {
		return err
	}
	commandEvent.CausationPosition = clonePointer(&causation)
	entries := []JournalEntry{commandEvent}
	if head.Status == "running" {
		survivors := make([]string, len(failureEffects.survivors))
		for index, command := range failureEffects.survivors {
			survivors[index] = command.key
		}
		failing, err := NewJournalEntry(RunFailing, journalcodec.RunFailingBody{
			V: 1, Status: "failing", Reason: message, CommandKey: key, Survivors: survivors,
		})
		if err != nil {
			return err
		}
		zero := 0
		failing.CausationBatchIndex = &zero
		entries = append(entries, failing)
	}
	cancelledOffset := len(entries)
	cancelledEntries, err := failureEffects.cancellationEntries(0, "cancelled after command failure")
	if err != nil {
		return err
	}
	entries = append(entries, cancelledEntries...)
	effectiveOpen, err := durable.AddPostgresInteger("run open commands", head.OpenCommands,
		-1, 0, durable.PostgresIntegerMax)
	if err != nil {
		return err
	}
	effectiveOpen, err = durable.AddPostgresInteger("run open commands", effectiveOpen,
		-len(failureEffects.cancelled), 0, durable.PostgresIntegerMax)
	if err != nil {
		return err
	}
	terminalRun := effectiveOpen == 0
	if terminalRun {
		terminal, err := runTerminalEvent("failed", message, "flow.run_failed")
		if err != nil {
			return err
		}
		zero := 0
		terminal.CausationBatchIndex = &zero
		entries = append(entries, terminal)
	}
	journal, err := semantic.Apply(ctx, PersistedChangeSet{Journal: entries})
	if err != nil {
		return err
	}
	failure := terminalFailure{Code: code, Message: message}
	if _, err := semantic.PGX().Exec(ctx, `UPDATE `+pgschema.Table(s.schema, "flow_commands")+`
		SET state='failed',last_error=$2::jsonb,terminal_failure=$2::jsonb,terminal_position=$3,
		    finished_at=$4,updated_at=$4,status_at=$4 WHERE command_id=$1`, commandID,
		jsonString(failure), journal.Journal[0].Position, semantic.DBNow()); err != nil {
		return MapError("fail command before claim", err)
	}
	if _, err := semantic.PGX().Exec(ctx, `DELETE FROM `+pgschema.Table(s.schema, "flow_command_queue")+` WHERE command_id=$1`, commandID); err != nil {
		return MapError("remove failed command queue row", err)
	}
	if err := s.applyFailureResolution(ctx, semantic, failureEffects, journal, cancelledOffset,
		"cancelled after command failure"); err != nil {
		return err
	}
	status := "failing"
	if terminalRun {
		status = "failed"
	}
	if _, err := semantic.PGX().Exec(ctx, `UPDATE `+pgschema.Table(s.schema, "flow_runs")+`
		SET status=$2,open_commands=$5,failure=CASE WHEN $6 THEN $3::jsonb ELSE failure END,
		    finished_at=CASE WHEN $2 IN ('failed','succeeded') THEN $4 ELSE finished_at END,
		    updated_at=$4,status_at=CASE WHEN status<>$2 THEN $4 ELSE status_at END
		WHERE run_id=$1`, semantic.RunID(), status, jsonString(failure), semantic.DBNow(),
		effectiveOpen, true); err != nil {
		return MapError("update run after pre-claim failure", err)
	}
	return nil
}

type LeaseRenewal struct {
	CommandID uuid.UUID
	AttemptID uuid.UUID
	Token     uuid.UUID
	Duration  time.Duration
}

type LeaseRenewalOutcome string

const (
	LeaseRenewed   LeaseRenewalOutcome = "renewed"
	LeaseLost      LeaseRenewalOutcome = "lost"
	LeaseUncertain LeaseRenewalOutcome = "uncertain"
)

type LeaseRenewalResult struct {
	CommandID      uuid.UUID
	AttemptID      uuid.UUID
	Outcome        LeaseRenewalOutcome
	LeaseExpiresAt *time.Time
}

func (s *Store) RenewCommandLeases(ctx context.Context, leases []LeaseRenewal) ([]LeaseRenewalResult, error) {
	if len(leases) == 0 {
		return nil, nil
	}
	commandIDs := make([]uuid.UUID, len(leases))
	attemptIDs := make([]uuid.UUID, len(leases))
	tokens := make([]uuid.UUID, len(leases))
	durationMilliseconds := make([]int64, len(leases))
	seen := make(map[uuid.UUID]struct{}, len(leases))
	for index, lease := range leases {
		if lease.CommandID == uuid.Nil || lease.AttemptID == uuid.Nil || lease.Token == uuid.Nil || lease.Duration <= 0 {
			return nil, fmt.Errorf("%w: incomplete command lease renewal", flowerr.ErrInvalid)
		}
		milliseconds, err := durable.ExactMilliseconds("lease duration", lease.Duration)
		if err != nil {
			return nil, err
		}
		if _, duplicate := seen[lease.CommandID]; duplicate {
			return nil, fmt.Errorf("%w: duplicate command lease renewal", flowerr.ErrInvalid)
		}
		seen[lease.CommandID] = struct{}{}
		commandIDs[index], attemptIDs[index], tokens[index] = lease.CommandID, lease.AttemptID, lease.Token
		durationMilliseconds[index] = milliseconds
	}
	rows, err := s.db.Conn.Query(ctx, `WITH now_value AS (SELECT clock_timestamp() AS now), requested(command_id,attempt_id,token,duration_ms,ordinal) AS (
		SELECT command_id,attempt_id,token,duration_ms,ordinality
		FROM unnest($1::uuid[],$2::uuid[],$3::uuid[],$4::bigint[])
			WITH ORDINALITY AS input(command_id,attempt_id,token,duration_ms,ordinality)
	), observed AS MATERIALIZED (
		SELECT r.command_id AS requested_command_id,r.attempt_id AS requested_attempt_id,r.token AS requested_token,r.ordinal,
		       q.command_id,q.state,q.active_attempt_id,q.lease_token,q.lease_expires_at
		FROM requested r
		LEFT JOIN `+pgschema.Table(s.schema, "flow_command_queue")+` q ON q.command_id=r.command_id
	), lockable AS (
		SELECT q.command_id,r.duration_ms
		FROM `+pgschema.Table(s.schema, "flow_command_queue")+` q
		JOIN requested r ON r.command_id=q.command_id
		CROSS JOIN now_value n
		WHERE q.active_attempt_id=r.attempt_id AND q.lease_token=r.token
		  AND q.state='running' AND q.lease_expires_at>n.now
		FOR UPDATE OF q SKIP LOCKED
	), renewed AS (
		UPDATE `+pgschema.Table(s.schema, "flow_command_queue")+` q
		SET lease_expires_at=n.now+(l.duration_ms * interval '1 millisecond')
		FROM lockable l,now_value n
		WHERE q.command_id=l.command_id
		RETURNING q.command_id,q.lease_expires_at
	)
	SELECT o.requested_command_id,o.requested_attempt_id,
	       CASE
	         WHEN x.command_id IS NOT NULL THEN 'renewed'
	         WHEN o.command_id IS NULL
	           OR o.state IS DISTINCT FROM 'running'
	           OR o.active_attempt_id IS DISTINCT FROM o.requested_attempt_id
	           OR o.lease_token IS DISTINCT FROM o.requested_token
	           OR o.lease_expires_at IS NULL
	           OR o.lease_expires_at<=n.now THEN 'lost'
	         ELSE 'uncertain'
	       END AS outcome,
	       x.lease_expires_at
	FROM observed o
	CROSS JOIN now_value n
	LEFT JOIN renewed x ON x.command_id=o.requested_command_id
		ORDER BY o.ordinal`, commandIDs, attemptIDs, tokens, durationMilliseconds)
	if err != nil {
		return nil, MapError("renew command leases", err)
	}
	defer rows.Close()
	result := make([]LeaseRenewalResult, 0, len(leases))
	for rows.Next() {
		var renewed LeaseRenewalResult
		if err := rows.Scan(&renewed.CommandID, &renewed.AttemptID, &renewed.Outcome, &renewed.LeaseExpiresAt); err != nil {
			return nil, MapError("scan renewed command lease", err)
		}
		if renewed.CommandID == uuid.Nil || renewed.AttemptID == uuid.Nil ||
			(renewed.Outcome != LeaseRenewed && renewed.Outcome != LeaseLost && renewed.Outcome != LeaseUncertain) ||
			(renewed.Outcome == LeaseRenewed) != (renewed.LeaseExpiresAt != nil) {
			return nil, fmt.Errorf("%w: command lease renewal result is invalid", flowerr.ErrInvalidState)
		}
		result = append(result, renewed)
	}
	if err := rows.Err(); err != nil {
		return nil, MapError("read renewed command leases", err)
	}
	if len(result) != len(leases) {
		return nil, fmt.Errorf("%w: command lease renewal result count differs", flowerr.ErrInvalidState)
	}
	for index := range result {
		if result[index].CommandID != leases[index].CommandID || result[index].AttemptID != leases[index].AttemptID {
			return nil, fmt.Errorf("%w: command lease renewal result identity differs", flowerr.ErrInvalidState)
		}
	}
	return result, nil
}

type AttemptOwnership string

const (
	AttemptOwnershipStillOwned AttemptOwnership = "running"
	AttemptOwnershipConcluded  AttemptOwnership = "concluded"
	AttemptOwnershipLost       AttemptOwnership = "lost"
)

func (s *Store) ResolveCommandAttempt(ctx context.Context, commandID, attemptID, token uuid.UUID) (AttemptOwnership, error) {
	var commandState string
	var activeAttempt, activeToken *uuid.UUID
	err := s.db.Conn.QueryRow(ctx, `SELECT c.state,q.active_attempt_id,q.lease_token
		FROM `+pgschema.Table(s.schema, "flow_commands")+` c
		LEFT JOIN `+pgschema.Table(s.schema, "flow_command_queue")+` q ON q.command_id=c.command_id
		WHERE c.command_id=$1`, commandID).Scan(&commandState, &activeAttempt, &activeToken)
	if err != nil {
		return "", MapError("resolve command attempt", err)
	}
	if isCommandTerminal(commandState) || commandState == "retry_wait" || commandState == "ready" {
		return AttemptOwnershipConcluded, nil
	}
	if commandState == "running" && activeAttempt != nil && activeToken != nil &&
		*activeAttempt == attemptID && *activeToken == token {
		return AttemptOwnershipStillOwned, nil
	}
	return AttemptOwnershipLost, nil
}

func safeAttemptError(code, message string) terminalFailure {
	code = strings.TrimSpace(code)
	message = strings.TrimSpace(message)
	if code == "" {
		code = "worker_error"
	}
	if len(code) > 128 {
		code = code[:128]
	}
	if len(message) > 1024 {
		message = message[:1024]
	}
	return terminalFailure{Code: code, Message: message}
}

func equalBytes(a, b []byte) bool { return bytes.Equal(a, b) }

func rawJSON(value []byte) json.RawMessage { return json.RawMessage(slices.Clone(value)) }

func canonicalBody(value any) (canonical.Value, error) { return canonical.Marshal(value, 0) }

type CommandSuccess struct {
	Claim    ClaimedCommand
	Result   canonical.Value
	Events   []ApplicationEvent
	Children []CommandCreate
	Commit   func(pgx.Tx) error
}

type successfulSettlementJournalLayout struct {
	applicationEventStart int
	applicationEventCount int
	childCreatedStart     int
	childCreatedCount     int
	parentTerminal        int
}

func newSuccessfulSettlementJournalLayout(applicationEvents, children int) successfulSettlementJournalLayout {
	applicationEventStart := 1
	childCreatedStart := applicationEventStart + applicationEvents
	return successfulSettlementJournalLayout{
		applicationEventStart: applicationEventStart,
		applicationEventCount: applicationEvents,
		childCreatedStart:     childCreatedStart,
		childCreatedCount:     children,
		parentTerminal:        childCreatedStart + children,
	}
}

func (layout successfulSettlementJournalLayout) validateAccepted(result ApplyResult) error {
	if len(result.Journal) <= layout.parentTerminal || result.Journal[0].Kind != AttemptConcluded {
		return fmt.Errorf("%w: successful settlement journal shape differs", flowerr.ErrInvalidState)
	}
	for index := 0; index < layout.applicationEventCount; index++ {
		row := result.Journal[layout.applicationEventStart+index]
		if row.Kind != EventRecorded || row.EventClass == nil || *row.EventClass != "application" {
			return fmt.Errorf("%w: successful settlement application-event mapping differs", flowerr.ErrInvalidState)
		}
	}
	for index := 0; index < layout.childCreatedCount; index++ {
		if result.Journal[layout.childCreatedStart+index].Kind != CommandCreated {
			return fmt.Errorf("%w: successful settlement child mapping differs", flowerr.ErrInvalidState)
		}
	}
	terminal := result.Journal[layout.parentTerminal]
	if terminal.Kind != EventRecorded || terminal.EventClass == nil || *terminal.EventClass != "command_terminal" {
		return fmt.Errorf("%w: successful settlement parent-terminal mapping differs", flowerr.ErrInvalidState)
	}
	return nil
}

func (layout successfulSettlementJournalLayout) applicationEventPosition(result ApplyResult, index int) int64 {
	return result.Journal[layout.applicationEventStart+index].Position
}

func (layout successfulSettlementJournalLayout) childCreatedPosition(result ApplyResult, index int) int64 {
	return result.Journal[layout.childCreatedStart+index].Position
}

func (layout successfulSettlementJournalLayout) parentTerminalPosition(result ApplyResult) int64 {
	return result.Journal[layout.parentTerminal].Position
}

type CommandConclusion struct {
	Claim          ClaimedCommand
	Classification retrypolicy.ErrorClass
	ExplicitDelay  *time.Duration
	Failure        failure.Value
}

type SettleResult struct {
	Retry         bool
	Terminal      bool
	NextAttemptAt *time.Time
	Status        string
}

type CommitFunctionError struct{ Err error }

func (e *CommitFunctionError) Error() string {
	if e == nil || e.Err == nil {
		return "command commit function failed"
	}
	return "command commit function failed: " + e.Err.Error()
}

func (e *CommitFunctionError) Unwrap() error {
	if e == nil {
		return nil
	}
	return e.Err
}

type commandFence struct {
	Head                   RunHead
	RunDeadline            *time.Time
	Key                    string
	Name                   string
	Version                int
	State                  string
	QueueState             string
	Attempt                int
	ConsumedAttempts       int
	BudgetStartedAt        time.Time
	RetryPolicy            []byte
	ActiveAttemptID        uuid.UUID
	LeaseToken             uuid.UUID
	LeaseExpiresAt         time.Time
	AttemptStartedPosition int64
}

func (s *Store) SettleCommandSuccess(ctx context.Context, request CommandSuccess, hook fault.Hook) (SettleResult, error) {
	if len(request.Result.Bytes) == 0 {
		return SettleResult{}, fmt.Errorf("%w: command result is empty", flowerr.ErrInvalid)
	}
	if hook == nil {
		hook = fault.None{}
	}
	semantic, err := s.BeginSemantic(ctx, request.Claim.RunID, LockBlocking)
	if err != nil {
		return SettleResult{}, err
	}
	defer semantic.Rollback(context.WithoutCancel(ctx))
	fence, err := s.lockCommandFence(ctx, semantic, request.Claim)
	if err != nil {
		return SettleResult{}, err
	}
	if err := hook.Hit(ctx, fault.SettleAfterFence); err != nil {
		return SettleResult{}, err
	}
	if fence.RunDeadline != nil && !semantic.DBNow().Before(*fence.RunDeadline) {
		if err := s.expireRunLocked(ctx, semantic, "run deadline reached"); err != nil {
			return SettleResult{}, err
		}
		if err := semantic.Commit(ctx); err != nil {
			return SettleResult{}, err
		}
		return SettleResult{Terminal: true, Status: "expired"}, nil
	}
	request.Events, err = s.coalesceApplicationEvents(ctx, semantic, request.Events)
	if err != nil {
		return SettleResult{}, err
	}
	if err := s.validateSuccessfulDecision(ctx, semantic, fence, request); err != nil {
		return SettleResult{}, err
	}
	childCreates := make([]commandBatchCreate, len(request.Children))
	for index, child := range request.Children {
		next, addErr := durable.AddExactDuration("initial delay", semantic.DBNow(), child.InitialDelay)
		if addErr != nil {
			return SettleResult{}, addErr
		}
		childCreates[index] = commandBatchCreate{
			command: child, budgetStartedAt: next, nextAttemptAt: next,
		}
	}
	var preparedChildren preparedCommandBatch
	if len(childCreates) > 0 {
		preparedChildren, err = s.prepareCommandBatch(ctx, semantic.PGX(), semantic.RunID(),
			childCreates, semantic.DBNow(), fence.RunDeadline, request.Events)
		if err != nil {
			return SettleResult{}, err
		}
	}
	layout := newSuccessfulSettlementJournalLayout(len(request.Events), len(request.Children))

	concluded, err := NewJournalEntry(AttemptConcluded, journalcodec.AttemptConcludedBody{
		V: 1, AttemptID: request.Claim.AttemptID.String(), CommandID: request.Claim.CommandID.String(),
		CommandKey: fence.Key, Attempt: fence.Attempt, Classification: "succeeded",
		ConsumedAttempts: fence.ConsumedAttempts, FinishedAt: semantic.DBNow(),
	})
	if err != nil {
		return SettleResult{}, err
	}
	concluded.CommandID = clonePointer(&request.Claim.CommandID)
	concluded.AttemptID = clonePointer(&request.Claim.AttemptID)
	concluded.CausationPosition = clonePointer(&fence.AttemptStartedPosition)

	entries := []JournalEntry{concluded}
	for _, event := range request.Events {
		entry := JournalEntry{
			EntryID: uuid.New(), Kind: EventRecorded, EventID: clonePointer(&event.ID),
			EventNamespace: stringPointer("application"), EventName: clonePointer(&event.Name),
			EventKey: clonePointer(&event.Key), EventClass: stringPointer("application"), Body: event.Body,
		}
		entry.CommandID = clonePointer(&request.Claim.CommandID)
		zero := 0
		entry.CausationBatchIndex = &zero
		entries = append(entries, entry)
	}
	for index, child := range request.Children {
		initialState, budgetStartedAt, nextAttemptAt, stateErr := preparedChildren.initialJournalState(index)
		if stateErr != nil {
			return SettleResult{}, stateErr
		}
		created, createErr := commandCreatedEntry(child, initialState, budgetStartedAt, nextAttemptAt)
		if createErr != nil {
			return SettleResult{}, createErr
		}
		zero := 0
		created.CausationBatchIndex = &zero
		entries = append(entries, created)
	}
	succeeded, err := NewJournalEntry(EventRecorded, journalcodec.CommandSucceededBody{
		V: 1, CommandKey: fence.Key, Result: rawJSON(request.Result.Bytes), CommitApplied: request.Commit != nil,
	})
	if err != nil {
		return SettleResult{}, err
	}
	eventID := uuid.New()
	succeeded.CommandID = clonePointer(&request.Claim.CommandID)
	succeeded.EventID = &eventID
	succeeded.EventNamespace = stringPointer("runtime")
	succeeded.EventName = clonePointer(&fence.Name)
	succeeded.EventKey = clonePointer(&fence.Key)
	succeeded.EventClass = stringPointer("command_terminal")
	succeeded.TerminalStatus = stringPointer("succeeded")
	zero := 0
	succeeded.CausationBatchIndex = &zero
	entries = append(entries, succeeded)
	parentTerminalIndex := len(entries) - 1
	if parentTerminalIndex != layout.parentTerminal {
		return SettleResult{}, fmt.Errorf("%w: successful settlement journal construction differs", flowerr.ErrInvalidState)
	}
	cancelStagedChildren := fence.Head.Status == "failing"
	childCancellationIndexes := make([]int, len(request.Children))
	if cancelStagedChildren {
		for index, child := range request.Children {
			cancelled, err := terminalEventWithCode(child.ID, child.Key, "cancelled", "run_failing",
				"cancelled because the run is failing", "flow.command_cancelled", "command_terminal")
			if err != nil {
				return SettleResult{}, err
			}
			cancelled.CausationBatchIndex = &parentTerminalIndex
			childCancellationIndexes[index] = len(entries)
			entries = append(entries, cancelled)
		}
	}
	effectiveChildren := len(request.Children)
	if cancelStagedChildren {
		effectiveChildren = 0
	}
	effectiveOpen, err := durable.AddPostgresInteger("run open commands", fence.Head.OpenCommands,
		-1, 0, durable.PostgresIntegerMax)
	if err != nil {
		return SettleResult{}, err
	}
	effectiveOpen, err = durable.AddPostgresInteger("run open commands", effectiveOpen,
		effectiveChildren, 0, durable.PostgresIntegerMax)
	if err != nil {
		return SettleResult{}, err
	}
	commandCount, err := durable.AddPostgresInteger("run command count", fence.Head.CommandCount,
		len(request.Children), 0, durable.PostgresIntegerMax)
	if err != nil {
		return SettleResult{}, err
	}
	terminalRun := effectiveOpen == 0
	terminalStatus := "succeeded"
	terminalName := "flow.run_succeeded"
	if fence.Head.Status == "failing" {
		terminalStatus = "failed"
		terminalName = "flow.run_failed"
	}
	if terminalRun {
		terminal, err := runTerminalEvent(terminalStatus, "", terminalName)
		if err != nil {
			return SettleResult{}, err
		}
		terminal.CausationBatchIndex = &zero
		entries = append(entries, terminal)
	}
	journal, err := semantic.Apply(ctx, PersistedChangeSet{Journal: entries})
	if err != nil {
		return SettleResult{}, err
	}
	if err := layout.validateAccepted(journal); err != nil {
		return SettleResult{}, err
	}
	acceptedEvents := make([]acceptedEventPosition, len(request.Events))
	acceptedEventPositions := make([]int64, len(request.Events))
	for index, event := range request.Events {
		row := journal.Journal[layout.applicationEventStart+index]
		if row.EventName == nil || *row.EventName != event.Name || row.EventKey == nil || *row.EventKey != event.Key {
			return SettleResult{}, fmt.Errorf("%w: staged application-event journal mapping differs", flowerr.ErrInvalidState)
		}
		acceptedEventPositions[index] = row.Position
		acceptedEvents[index] = acceptedEventPosition{
			name: event.Name, key: event.Key, position: acceptedEventPositions[index],
		}
	}
	childCreatedPositions := make([]int64, len(request.Children))
	for index, child := range request.Children {
		row := journal.Journal[layout.childCreatedStart+index]
		if row.CommandID == nil || *row.CommandID != child.ID {
			return SettleResult{}, fmt.Errorf("%w: staged-child command-created journal mapping differs", flowerr.ErrInvalidState)
		}
		childCreatedPositions[index] = row.Position
	}
	if len(request.Children) > 0 {
		if err := preparedChildren.assignJournalPositions(childCreatedPositions, acceptedEventPositions); err != nil {
			return SettleResult{}, err
		}
	}
	if err := hook.Hit(ctx, fault.SettleAfterAttempt); err != nil {
		return SettleResult{}, err
	}
	if err := executeProjectionBatch(ctx, semantic.PGX(), projectionStatement{
		operation: "settle successful command", query: `UPDATE ` + pgschema.Table(s.schema, "flow_commands") + `
		SET state='succeeded',result=$2,last_error=NULL,terminal_failure=NULL,
		    terminal_position=$3,finished_at=$4,updated_at=$4,status_at=$4
		WHERE command_id=$1`, arguments: []any{
			request.Claim.CommandID, request.Result.Bytes, layout.parentTerminalPosition(journal), semantic.DBNow(),
		},
	}, projectionStatement{
		operation: "remove successful command queue row",
		query:     `DELETE FROM ` + pgschema.Table(s.schema, "flow_command_queue") + ` WHERE command_id=$1`,
		arguments: []any{request.Claim.CommandID},
	}); err != nil {
		return SettleResult{}, err
	}
	immediatelyRunnable := preparedChildren.immediatelyRunnable && !cancelStagedChildren
	if len(request.Children) > 0 {
		if err := s.insertPreparedCommandBatch(ctx, semantic.PGX(), semantic.RunID(), semantic.DBNow(), preparedChildren); err != nil {
			return SettleResult{}, err
		}
	}
	if cancelStagedChildren {
		cancellationPositions := make([]int64, len(request.Children))
		for index, child := range request.Children {
			row := journal.Journal[childCancellationIndexes[index]]
			if row.Kind != EventRecorded || row.EventClass == nil || *row.EventClass != "command_terminal" ||
				row.TerminalStatus == nil || *row.TerminalStatus != "cancelled" || row.CommandID == nil || *row.CommandID != child.ID {
				return SettleResult{}, fmt.Errorf("%w: staged-child cancellation journal mapping differs", flowerr.ErrInvalidState)
			}
			cancellationPositions[index] = row.Position
		}
		if err := s.cancelStagedCommandBatch(ctx, semantic, preparedChildren, cancellationPositions); err != nil {
			return SettleResult{}, err
		}
	}
	if err := hook.Hit(ctx, fault.SettleAfterChildren); err != nil {
		return SettleResult{}, err
	}
	releasedRunnable, err := s.resolveEventReadinessLocked(ctx, semantic, acceptedEvents)
	if err != nil {
		return SettleResult{}, err
	}
	immediatelyRunnable = immediatelyRunnable || releasedRunnable
	if terminalRun {
		if _, err := semantic.PGX().Exec(ctx, `UPDATE `+pgschema.Table(s.schema, "flow_runs")+`
			SET status=$3,open_commands=$4,command_count=$5,finished_at=$2,updated_at=$2,status_at=$2
			WHERE run_id=$1`, semantic.RunID(), semantic.DBNow(), terminalStatus, effectiveOpen, commandCount); err != nil {
			return SettleResult{}, MapError("complete direct run", err)
		}
	} else {
		if _, err := semantic.PGX().Exec(ctx, `UPDATE `+pgschema.Table(s.schema, "flow_runs")+`
			SET open_commands=$3,command_count=$4,
			    updated_at=$2 WHERE run_id=$1`, semantic.RunID(), semantic.DBNow(), effectiveOpen, commandCount); err != nil {
			return SettleResult{}, MapError("update run after command success", err)
		}
	}
	if err := hook.Hit(ctx, fault.SettleAfterEvents); err != nil {
		return SettleResult{}, err
	}
	if request.Commit != nil {
		if err := hook.Hit(ctx, fault.SettleBeforeCommitFunction); err != nil {
			return SettleResult{}, err
		}
		if err := request.Commit(semantic.PGX()); err != nil {
			return SettleResult{}, &CommitFunctionError{Err: err}
		}
		if err := hook.Hit(ctx, fault.SettleAfterCommitFunction); err != nil {
			return SettleResult{}, err
		}
	}
	if immediatelyRunnable {
		if err := semantic.NotifyRunnableCommands(ctx); err != nil {
			return SettleResult{}, err
		}
	}
	if err := hook.Hit(ctx, fault.SettleBeforeCommit); err != nil {
		return SettleResult{}, err
	}
	if err := semantic.Commit(ctx); err != nil {
		return SettleResult{}, err
	}
	if err := hook.Hit(ctx, fault.SettleCommitAmbiguous); err != nil {
		return SettleResult{}, err
	}
	return SettleResult{Terminal: true, Status: "succeeded"}, nil
}

func (s *Store) cancelStagedCommandBatch(
	ctx context.Context,
	semantic *SemanticTx,
	batch preparedCommandBatch,
	terminalPositions []int64,
) error {
	if len(batch.commands) == 0 || len(terminalPositions) != len(batch.commands) {
		return fmt.Errorf("%w: staged-child cancellation batch differs", flowerr.ErrInvalidState)
	}
	commandIDs := make([]uuid.UUID, len(batch.commands))
	for index, command := range batch.commands {
		if terminalPositions[index] < 1 {
			return fmt.Errorf("%w: staged-child cancellation position is invalid", flowerr.ErrInvalidState)
		}
		commandIDs[index] = command.command.ID
	}
	failure := terminalFailure{Code: "run_failing", Message: "cancelled because the run is failing"}
	commandTag, err := semantic.PGX().Exec(ctx, `WITH cancelled(command_id,terminal_position) AS (
		SELECT * FROM unnest($2::uuid[],$3::bigint[])
	)
	UPDATE `+pgschema.Table(s.schema, "flow_commands")+` AS c
	SET state='cancelled',terminal_failure=$4::jsonb,terminal_position=cancelled.terminal_position,
	    finished_at=$5,updated_at=$5,status_at=$5
	FROM cancelled
	WHERE c.run_id=$1 AND c.command_id=cancelled.command_id
	  AND c.state IN ('pending','ready')`,
		semantic.RunID(), commandIDs, terminalPositions, jsonString(failure), semantic.DBNow())
	if err != nil {
		return MapError("cancel staged command batch while run is failing", err)
	}
	if commandTag.RowsAffected() != int64(len(commandIDs)) {
		return fmt.Errorf("%w: staged-child cancellation batch updated %d of %d rows",
			flowerr.ErrInvalidState, commandTag.RowsAffected(), len(commandIDs))
	}
	if len(batch.queues) == 0 {
		return nil
	}
	queueTag, err := semantic.PGX().Exec(ctx, `DELETE FROM `+pgschema.Table(s.schema, "flow_command_queue")+`
		WHERE run_id=$1 AND command_id=ANY($2::uuid[])`, semantic.RunID(), commandIDs)
	if err != nil {
		return MapError("remove staged command queue batch while run is failing", err)
	}
	if queueTag.RowsAffected() != int64(len(batch.queues)) {
		return fmt.Errorf("%w: staged-child queue batch removed %d of %d rows",
			flowerr.ErrInvalidState, queueTag.RowsAffected(), len(batch.queues))
	}
	return nil
}

func (s *Store) validateSuccessfulDecision(ctx context.Context, semantic *SemanticTx, fence commandFence, request CommandSuccess) error {
	if len(request.Children) > 0 {
		if err := durable.PostgresInteger("child command count", len(request.Children), 0, durable.PostgresIntegerMax); err != nil {
			return err
		}
		commandCount, err := durable.AddPostgresInteger("run command count", fence.Head.CommandCount,
			len(request.Children), 0, durable.PostgresIntegerMax)
		if err != nil {
			return err
		}
		if fence.Head.MaxCommands > 0 && commandCount > fence.Head.MaxCommands {
			return fmt.Errorf("%w: run command ceiling exceeded", flowerr.ErrInvalidState)
		}
		openCommands, err := durable.AddPostgresInteger("run open commands", fence.Head.OpenCommands,
			-1, 0, durable.PostgresIntegerMax)
		if err != nil {
			return err
		}
		if _, err := durable.AddPostgresInteger("run open commands", openCommands,
			len(request.Children), 0, durable.PostgresIntegerMax); err != nil {
			return err
		}
		keys := make([]string, len(request.Children))
		for index, child := range request.Children {
			if err := validateCommandCreate(child); err != nil {
				return err
			}
			if child.ParentCommandID == nil || *child.ParentCommandID != request.Claim.CommandID {
				return fmt.Errorf("%w: invalid worker-spawned child", flowerr.ErrInvalid)
			}
			keys[index] = child.Key
		}
		var conflicts int
		if err := semantic.PGX().QueryRow(ctx, `SELECT count(*) FROM `+pgschema.Table(s.schema, "flow_commands")+`
			WHERE run_id=$1 AND command_key=ANY($2)`, semantic.RunID(), keys).Scan(&conflicts); err != nil {
			return MapError("validate spawned child keys", err)
		}
		if conflicts != 0 {
			return fmt.Errorf("%w: spawned command key already exists", flowerr.ErrConflict)
		}
	}
	return nil
}

func (s *Store) SettleCommandConclusion(ctx context.Context, request CommandConclusion, hook fault.Hook) (SettleResult, error) {
	if request.ExplicitDelay != nil {
		if *request.ExplicitDelay <= 0 {
			return SettleResult{}, fmt.Errorf("%w: retry-after delay must be positive", flowerr.ErrInvalid)
		}
		if _, err := durable.ExactMilliseconds("retry-after delay", *request.ExplicitDelay); err != nil {
			return SettleResult{}, err
		}
	}
	if hook == nil {
		hook = fault.None{}
	}
	semantic, err := s.BeginSemantic(ctx, request.Claim.RunID, LockBlocking)
	if err != nil {
		return SettleResult{}, err
	}
	defer semantic.Rollback(context.WithoutCancel(ctx))
	fence, err := s.lockCommandFence(ctx, semantic, request.Claim)
	if err != nil {
		return SettleResult{}, err
	}
	if err := hook.Hit(ctx, fault.SettleAfterFence); err != nil {
		return SettleResult{}, err
	}
	if fence.RunDeadline != nil && !semantic.DBNow().Before(*fence.RunDeadline) {
		if err := s.expireRunLocked(ctx, semantic, "run deadline reached"); err != nil {
			return SettleResult{}, err
		}
		if err := semantic.Commit(ctx); err != nil {
			return SettleResult{}, err
		}
		return SettleResult{Terminal: true, Status: "expired"}, nil
	}
	policy, err := retrypolicy.PublicFromCanonical(fence.RetryPolicy)
	if err != nil {
		return SettleResult{}, fmt.Errorf("%w: stored retry policy is invalid", flowerr.ErrInvalidState)
	}
	decision, err := retrypolicy.DecidePublic(policy, retrypolicy.Input{
		DBNow: semantic.DBNow(), BudgetStartedAt: fence.BudgetStartedAt,
		ConsumedAttempts: fence.ConsumedAttempts, AttemptID: request.Claim.AttemptID.String(),
		Classification: request.Classification, ExplicitDelay: request.ExplicitDelay,
		RunDeadline: fence.RunDeadline,
	})
	if err != nil {
		return SettleResult{}, fmt.Errorf("%w: retry decision: %s", flowerr.ErrInvalidState, err)
	}
	if err := durable.PostgresInteger("consumed attempts", decision.ConsumedAttempts, 0, durable.PostgresIntegerMax); err != nil {
		return SettleResult{}, err
	}
	if decision.Retry && decision.NextAttemptAt.IsZero() {
		decision.NextAttemptAt = semantic.DBNow()
	}
	failure := safeAttemptError(request.Failure.Code, request.Failure.Message)
	var next *time.Time
	if decision.Retry {
		next = clonePointer(&decision.NextAttemptAt)
	}
	concluded, err := NewJournalEntry(AttemptConcluded, journalcodec.AttemptConcludedBody{
		V: 1, AttemptID: request.Claim.AttemptID.String(), CommandID: request.Claim.CommandID.String(),
		CommandKey: fence.Key, Attempt: fence.Attempt, Classification: string(request.Classification),
		ConsumedBudget: decision.ConsumesAttempt, ConsumedAttempts: decision.ConsumedAttempts,
		FinishedAt: semantic.DBNow(), NextAttemptAt: next, ErrorCode: failure.Code, ErrorMessage: failure.Message,
	})
	if err != nil {
		return SettleResult{}, err
	}
	concluded.CommandID = clonePointer(&request.Claim.CommandID)
	concluded.AttemptID = clonePointer(&request.Claim.AttemptID)
	concluded.CausationPosition = clonePointer(&fence.AttemptStartedPosition)
	entries := []JournalEntry{concluded}
	failureEffects := failureResolution{}
	cancelledOffset := 0
	terminalRun := false
	effectiveOpen := fence.Head.OpenCommands
	if !decision.Retry {
		failureEffects, err = s.resolveCommandFailureLocked(ctx, semantic, request.Claim.CommandID, "failed")
		if err != nil {
			return SettleResult{}, err
		}
		failed, err := terminalEventWithCode(request.Claim.CommandID, fence.Key, "failed", failure.Code, failure.Message, "flow.command_failed", "command_terminal")
		if err != nil {
			return SettleResult{}, err
		}
		zero := 0
		failed.CausationBatchIndex = &zero
		entries = append(entries, failed)
		failedIndex := len(entries) - 1
		if fence.Head.Status == "running" {
			survivors := make([]string, len(failureEffects.survivors))
			for index, command := range failureEffects.survivors {
				survivors[index] = command.key
			}
			failing, err := NewJournalEntry(RunFailing, journalcodec.RunFailingBody{
				V: 1, Status: "failing", Reason: failure.Message, CommandKey: fence.Key, Survivors: survivors,
			})
			if err != nil {
				return SettleResult{}, err
			}
			failing.CausationBatchIndex = &failedIndex
			entries = append(entries, failing)
		}
		cancelledOffset = len(entries)
		cancelledEntries, err := failureEffects.cancellationEntries(failedIndex, "cancelled after command failure")
		if err != nil {
			return SettleResult{}, err
		}
		entries = append(entries, cancelledEntries...)
		effectiveOpen, err = durable.AddPostgresInteger("run open commands", fence.Head.OpenCommands,
			-1, 0, durable.PostgresIntegerMax)
		if err != nil {
			return SettleResult{}, err
		}
		effectiveOpen, err = durable.AddPostgresInteger("run open commands", effectiveOpen,
			-len(failureEffects.cancelled), 0, durable.PostgresIntegerMax)
		if err != nil {
			return SettleResult{}, err
		}
		terminalRun = effectiveOpen == 0
		if terminalRun {
			terminal, err := runTerminalEvent("failed", failure.Message, "flow.run_failed")
			if err != nil {
				return SettleResult{}, err
			}
			terminal.CausationBatchIndex = &failedIndex
			entries = append(entries, terminal)
		}
	}
	journal, err := semantic.Apply(ctx, PersistedChangeSet{Journal: entries})
	if err != nil {
		return SettleResult{}, err
	}
	if err := hook.Hit(ctx, fault.SettleAfterAttempt); err != nil {
		return SettleResult{}, err
	}
	if decision.Retry {
		if err := executeProjectionBatch(ctx, semantic.PGX(), projectionStatement{
			operation: "schedule command retry", query: `UPDATE ` + pgschema.Table(s.schema, "flow_commands") + `
			SET state='retry_wait',consumed_attempts=$2,last_error=$3::jsonb,next_attempt_at=$4,
			    updated_at=$5,status_at=$5 WHERE command_id=$1`, arguments: []any{
				request.Claim.CommandID, decision.ConsumedAttempts, jsonString(failure), decision.NextAttemptAt, semantic.DBNow(),
			},
		}, projectionStatement{
			operation: "release command for retry", query: `UPDATE ` + pgschema.Table(s.schema, "flow_command_queue") + `
			SET state='retry_wait',next_run_at=$2,active_attempt_id=NULL,lease_token=NULL,lease_owner=NULL,
			    lease_started_at=NULL,lease_expires_at=NULL WHERE command_id=$1`,
			arguments: []any{request.Claim.CommandID, decision.NextAttemptAt},
		}); err != nil {
			return SettleResult{}, err
		}
	} else {
		terminalPosition := journal.Journal[1].Position
		if err := executeProjectionBatch(ctx, semantic.PGX(), projectionStatement{
			operation: "fail command", query: `UPDATE ` + pgschema.Table(s.schema, "flow_commands") + `
			SET state='failed',consumed_attempts=$2,last_error=$3::jsonb,terminal_failure=$3::jsonb,
			    terminal_position=$4,finished_at=$5,updated_at=$5,status_at=$5 WHERE command_id=$1`,
			arguments: []any{
				request.Claim.CommandID, decision.ConsumedAttempts, jsonString(failure), terminalPosition, semantic.DBNow(),
			},
		}, projectionStatement{
			operation: "remove failed command queue row",
			query:     `DELETE FROM ` + pgschema.Table(s.schema, "flow_command_queue") + ` WHERE command_id=$1`,
			arguments: []any{request.Claim.CommandID},
		}); err != nil {
			return SettleResult{}, err
		}
		if err := s.applyFailureResolution(ctx, semantic, failureEffects, journal, cancelledOffset,
			"cancelled after command failure"); err != nil {
			return SettleResult{}, err
		}
		status := "failing"
		if terminalRun {
			status = "failed"
		}
		if _, err := semantic.PGX().Exec(ctx, `UPDATE `+pgschema.Table(s.schema, "flow_runs")+`
			SET status=$2,open_commands=$5,
			    failure=CASE WHEN $6 THEN $3::jsonb ELSE failure END,
			    finished_at=CASE WHEN $2 IN ('failed','succeeded') THEN $4 ELSE finished_at END,
			    updated_at=$4,status_at=CASE WHEN status<>$2 THEN $4 ELSE status_at END
			WHERE run_id=$1`, semantic.RunID(), status, jsonString(failure), semantic.DBNow(),
			effectiveOpen, true); err != nil {
			return SettleResult{}, MapError("update run after command failure", err)
		}
	}
	if err := hook.Hit(ctx, fault.SettleAfterEvents); err != nil {
		return SettleResult{}, err
	}
	if decision.Retry && !decision.NextAttemptAt.After(semantic.DBNow()) {
		if err := semantic.NotifyRunnableCommands(ctx); err != nil {
			return SettleResult{}, err
		}
	}
	if err := hook.Hit(ctx, fault.SettleBeforeCommit); err != nil {
		return SettleResult{}, err
	}
	if err := semantic.Commit(ctx); err != nil {
		return SettleResult{}, err
	}
	if err := hook.Hit(ctx, fault.SettleCommitAmbiguous); err != nil {
		return SettleResult{}, err
	}
	return SettleResult{Retry: decision.Retry, Terminal: !decision.Retry, NextAttemptAt: next,
		Status: map[bool]string{true: "retry_wait", false: "failed"}[decision.Retry]}, nil
}

func (s *Store) lockCommandFence(ctx context.Context, semantic *SemanticTx, claim ClaimedCommand) (commandFence, error) {
	initial, ok := semantic.InitialLockedSnapshot()
	if !ok {
		return commandFence{}, fmt.Errorf("%w: command fence has no initial locked snapshot", flowerr.ErrInvalidState)
	}
	if initial.Head.Status != "running" && initial.Head.Status != "failing" {
		return commandFence{}, fmt.Errorf("%w: run is terminal", flowerr.ErrTerminal)
	}
	result := commandFence{Head: initial.Head, RunDeadline: clonePointer(initial.Deadline)}
	var activeAttempt, token *uuid.UUID
	err := semantic.PGX().QueryRow(ctx, `SELECT c.command_key,c.name,c.version,c.state,c.attempt_ordinal,
		c.consumed_attempts,c.budget_started_at,c.retry_policy,
		q.state,q.active_attempt_id,q.lease_token,q.lease_expires_at,
		(SELECT position FROM `+pgschema.Table(s.schema, "flow_journal")+`
		 WHERE run_id=c.run_id AND attempt_id=$2 AND entry_kind='attempt_started')
		FROM `+pgschema.Table(s.schema, "flow_commands")+` c
		JOIN `+pgschema.Table(s.schema, "flow_command_queue")+` q ON q.command_id=c.command_id
		WHERE c.command_id=$1 AND c.run_id=$3
		FOR UPDATE OF c,q`, claim.CommandID, claim.AttemptID, semantic.RunID()).
		Scan(&result.Key, &result.Name, &result.Version, &result.State,
			&result.Attempt, &result.ConsumedAttempts, &result.BudgetStartedAt, &result.RetryPolicy,
			&result.QueueState, &activeAttempt, &token, &result.LeaseExpiresAt, &result.AttemptStartedPosition)
	if errors.Is(err, pgx.ErrNoRows) {
		return commandFence{}, s.mapFenceMiss(ctx, semantic.PGX(), claim)
	}
	if err != nil {
		return commandFence{}, MapError("lock command fence", err)
	}
	if result.State != "running" || result.QueueState != "running" || activeAttempt == nil || token == nil ||
		*activeAttempt != claim.AttemptID || *token != claim.LeaseToken || !semantic.DBNow().Before(result.LeaseExpiresAt) {
		return commandFence{}, fmt.Errorf("%w: command attempt no longer owns its lease", flowerr.ErrLeaseLost)
	}
	return result, nil
}

func (s *Store) mapFenceMiss(ctx context.Context, tx pgx.Tx, claim ClaimedCommand) error {
	var state string
	err := tx.QueryRow(ctx, `SELECT state FROM `+pgschema.Table(s.schema, "flow_commands")+` WHERE command_id=$1`, claim.CommandID).Scan(&state)
	if errors.Is(err, pgx.ErrNoRows) {
		return fmt.Errorf("%w: command does not exist", flowerr.ErrNotFound)
	}
	if err != nil {
		return MapError("diagnose command fence", err)
	}
	if isCommandTerminal(state) {
		return fmt.Errorf("%w: command is terminal", flowerr.ErrTerminal)
	}
	return fmt.Errorf("%w: command attempt no longer owns its lease", flowerr.ErrLeaseLost)
}

func (s *Store) ProbeExpiredRuns(ctx context.Context, limit int) ([]uuid.UUID, error) {
	if limit <= 0 {
		return nil, nil
	}
	rows, err := s.db.Conn.Query(ctx, `SELECT run_id FROM `+pgschema.Table(s.schema, "flow_runs")+`
		WHERE status IN ('running','failing') AND deadline_at IS NOT NULL AND deadline_at<=clock_timestamp()
		ORDER BY deadline_at,run_id LIMIT $1`, limit)
	if err != nil {
		return nil, MapError("probe expired runs", err)
	}
	defer rows.Close()
	var result []uuid.UUID
	for rows.Next() {
		var id uuid.UUID
		if err := rows.Scan(&id); err != nil {
			return nil, MapError("scan expired run", err)
		}
		result = append(result, id)
	}
	if err := rows.Err(); err != nil {
		return nil, MapError("read expired runs", err)
	}
	return result, nil
}

func (s *Store) ExpireRun(ctx context.Context, id uuid.UUID, reason string) (bool, error) {
	semantic, err := s.BeginSemantic(ctx, id, LockSkipLocked)
	if errors.Is(err, ErrLockUnavailable) {
		return false, nil
	}
	if err != nil {
		return false, err
	}
	defer semantic.Rollback(context.WithoutCancel(ctx))
	var status string
	var deadline *time.Time
	if err := semantic.PGX().QueryRow(ctx, `SELECT status,deadline_at FROM `+
		pgschema.Table(s.schema, "flow_runs")+` WHERE run_id=$1`, id).Scan(&status, &deadline); err != nil {
		return false, MapError("load expiring run", err)
	}
	if status != "running" && status != "failing" || deadline == nil || semantic.DBNow().Before(*deadline) {
		return false, nil
	}
	if err := s.expireRunLocked(ctx, semantic, reason); err != nil {
		return false, err
	}
	if err := semantic.Commit(ctx); err != nil {
		return false, err
	}
	return true, nil
}

type expiringCommand struct {
	ID                     uuid.UUID
	Key                    string
	State                  string
	Attempt                int
	ConsumedAttempts       int
	CreatedPosition        int64
	AttemptID              *uuid.UUID
	AttemptStartedPosition *int64
}

func (s *Store) expireRunLocked(ctx context.Context, semantic *SemanticTx, reason string) error {
	head, err := s.LoadRunHead(ctx, semantic)
	if err != nil {
		return err
	}
	if head.Status != "running" && head.Status != "failing" {
		return nil
	}
	rows, err := semantic.PGX().Query(ctx, `SELECT command_id,command_key,state,attempt_ordinal,consumed_attempts,created_position
		FROM `+pgschema.Table(s.schema, "flow_commands")+`
		WHERE run_id=$1 AND state NOT IN ('succeeded','failed','cancelled','expired')
		ORDER BY command_id FOR UPDATE`, semantic.RunID())
	if err != nil {
		return MapError("lock expiring commands", err)
	}
	commands := make([]expiringCommand, 0, head.OpenCommands)
	for rows.Next() {
		var command expiringCommand
		if err := rows.Scan(&command.ID, &command.Key, &command.State, &command.Attempt,
			&command.ConsumedAttempts, &command.CreatedPosition); err != nil {
			rows.Close()
			return MapError("scan expiring command", err)
		}
		commands = append(commands, command)
	}
	if err := rows.Err(); err != nil {
		rows.Close()
		return MapError("read expiring commands", err)
	}
	rows.Close()
	if len(commands) != head.OpenCommands {
		return fmt.Errorf("%w: expiring run command counter differs", flowerr.ErrInvalidState)
	}
	runningIDs := make([]uuid.UUID, 0, len(commands))
	runningIndexes := make(map[uuid.UUID]int, len(commands))
	for index := range commands {
		if commands[index].State == "running" {
			runningIDs = append(runningIDs, commands[index].ID)
			runningIndexes[commands[index].ID] = index
		}
	}
	if len(runningIDs) > 0 {
		rows, err := semantic.PGX().Query(ctx, `SELECT q.command_id,q.active_attempt_id,q.lease_token,
			(SELECT position FROM `+pgschema.Table(s.schema, "flow_journal")+`
			 WHERE run_id=$2 AND attempt_id=q.active_attempt_id AND entry_kind='attempt_started')
			FROM `+pgschema.Table(s.schema, "flow_command_queue")+` q
			WHERE q.command_id=ANY($1::uuid[]) AND q.run_id=$2 AND q.state='running'
			ORDER BY q.command_id FOR UPDATE`, runningIDs, semantic.RunID())
		if err != nil {
			return MapError("lock expiring command deliveries", err)
		}
		read := 0
		for rows.Next() {
			var commandID uuid.UUID
			var attemptID, token *uuid.UUID
			var attemptStartedPosition *int64
			if err := rows.Scan(&commandID, &attemptID, &token, &attemptStartedPosition); err != nil {
				rows.Close()
				return MapError("scan expiring command delivery", err)
			}
			if read >= len(runningIDs) || commandID != runningIDs[read] ||
				attemptID == nil || token == nil || attemptStartedPosition == nil {
				rows.Close()
				return fmt.Errorf("%w: expiring command delivery projection differs", flowerr.ErrInvalidState)
			}
			index := runningIndexes[commandID]
			commands[index].AttemptID = attemptID
			commands[index].AttemptStartedPosition = attemptStartedPosition
			read++
		}
		if err := rows.Err(); err != nil {
			rows.Close()
			return MapError("read expiring command deliveries", err)
		}
		rows.Close()
		if read != len(runningIDs) {
			return fmt.Errorf("%w: expiring run has %d of %d delivery projections",
				flowerr.ErrInvalidState, read, len(runningIDs))
		}
	}
	slices.SortFunc(commands, func(a, b expiringCommand) int {
		if a.Key < b.Key {
			return -1
		}
		if a.Key > b.Key {
			return 1
		}
		return bytes.Compare(a.ID[:], b.ID[:])
	})
	entries := make([]JournalEntry, 0, len(commands)*2+1)
	terminalBatchIndex := make(map[uuid.UUID]int, len(commands))
	for _, command := range commands {
		if command.AttemptID != nil && command.AttemptStartedPosition != nil {
			concluded, err := NewJournalEntry(AttemptConcluded, journalcodec.AttemptConcludedBody{
				V: 1, AttemptID: command.AttemptID.String(), CommandID: command.ID.String(), CommandKey: command.Key,
				Attempt: command.Attempt, Classification: "run_expired", ConsumedAttempts: command.ConsumedAttempts,
				FinishedAt: semantic.DBNow(), ErrorCode: "run_expired", ErrorMessage: reason,
			})
			if err != nil {
				return err
			}
			concluded.CommandID = clonePointer(&command.ID)
			concluded.AttemptID = clonePointer(command.AttemptID)
			concluded.CausationPosition = clonePointer(command.AttemptStartedPosition)
			entries = append(entries, concluded)
		}
		cancelled, err := terminalEventWithCode(command.ID, command.Key, "cancelled", "run_expired", reason, "flow.command_cancelled", "command_terminal")
		if err != nil {
			return err
		}
		if command.AttemptID != nil {
			index := len(entries) - 1
			cancelled.CausationBatchIndex = &index
		} else {
			cancelled.CausationPosition = clonePointer(&command.CreatedPosition)
		}
		terminalBatchIndex[command.ID] = len(entries)
		entries = append(entries, cancelled)
	}
	terminal, err := runTerminalEvent("expired", reason, "flow.run_expired")
	if err != nil {
		return err
	}
	if len(entries) > 0 {
		index := len(entries) - 1
		terminal.CausationBatchIndex = &index
	}
	entries = append(entries, terminal)
	journal, err := semantic.Apply(ctx, PersistedChangeSet{Journal: entries})
	if err != nil {
		return err
	}
	failure := terminalFailure{Code: "run_expired", Message: reason}
	commandIDs := make([]uuid.UUID, len(commands))
	terminalPositions := make([]int64, len(commands))
	for index, command := range commands {
		journalIndex, exists := terminalBatchIndex[command.ID]
		if !exists || journalIndex < 0 || journalIndex >= len(journal.Journal) {
			return fmt.Errorf("%w: run expiry journal mapping is invalid", flowerr.ErrInvalidState)
		}
		row := journal.Journal[journalIndex]
		if row.CommandID == nil || *row.CommandID != command.ID || row.TerminalStatus == nil || *row.TerminalStatus != "cancelled" {
			return fmt.Errorf("%w: run expiry journal mapping differs", flowerr.ErrInvalidState)
		}
		commandIDs[index], terminalPositions[index] = command.ID, row.Position
	}
	commandTag, err := semantic.PGX().Exec(ctx, `UPDATE `+pgschema.Table(s.schema, "flow_commands")+` AS c
		SET state='cancelled',last_error=$4::jsonb,terminal_failure=$4::jsonb,
		    terminal_position=expired.position,finished_at=$5,updated_at=$5,status_at=$5
		FROM unnest($1::uuid[],$2::bigint[]) AS expired(command_id,position)
		WHERE c.run_id=$3 AND c.command_id=expired.command_id
		  AND c.state NOT IN ('succeeded','failed','cancelled','expired')`,
		commandIDs, terminalPositions, semantic.RunID(), jsonString(failure), semantic.DBNow())
	if err != nil {
		return MapError("expire command batch", err)
	}
	if commandTag.RowsAffected() != int64(len(commandIDs)) {
		return fmt.Errorf("%w: run expiry set changed", flowerr.ErrInvalidState)
	}
	if _, err := semantic.PGX().Exec(ctx, `DELETE FROM `+pgschema.Table(s.schema, "flow_command_queue")+` WHERE run_id=$1`, semantic.RunID()); err != nil {
		return MapError("remove expired run deliveries", err)
	}
	if _, err := semantic.PGX().Exec(ctx, `UPDATE `+pgschema.Table(s.schema, "flow_runs")+`
		SET status='expired',open_commands=0,failure=$2::jsonb,finished_at=$3,updated_at=$3,status_at=$3
		WHERE run_id=$1`, semantic.RunID(),
		jsonString(failure), semantic.DBNow()); err != nil {
		return MapError("expire run", err)
	}
	return nil
}

type ExpiredLeaseCandidate struct {
	CommandID uuid.UUID
	RunID     uuid.UUID
}

func (s *Store) ProbeExpiredCommandLeases(ctx context.Context, limit int) ([]ExpiredLeaseCandidate, error) {
	if limit <= 0 {
		return nil, nil
	}
	rows, err := s.db.Conn.Query(ctx, `SELECT command_id,run_id FROM `+
		pgschema.Table(s.schema, "flow_command_queue")+`
		WHERE state='running' AND lease_expires_at<=clock_timestamp()
		ORDER BY lease_expires_at,command_id LIMIT $1`, limit)
	if err != nil {
		return nil, MapError("probe expired command leases", err)
	}
	defer rows.Close()
	var result []ExpiredLeaseCandidate
	for rows.Next() {
		var candidate ExpiredLeaseCandidate
		if err := rows.Scan(&candidate.CommandID, &candidate.RunID); err != nil {
			return nil, MapError("scan expired command lease", err)
		}
		result = append(result, candidate)
	}
	if err := rows.Err(); err != nil {
		return nil, MapError("read expired command leases", err)
	}
	return result, nil
}

func (s *Store) RecoverExpiredCommandLease(ctx context.Context, candidate ExpiredLeaseCandidate) (bool, error) {
	semantic, err := s.BeginSemantic(ctx, candidate.RunID, LockSkipLocked)
	if errors.Is(err, ErrLockUnavailable) {
		return false, nil
	}
	if err != nil {
		return false, err
	}
	defer semantic.Rollback(context.WithoutCancel(ctx))
	var status string
	var deadline *time.Time
	if err := semantic.PGX().QueryRow(ctx, `SELECT status,deadline_at FROM `+
		pgschema.Table(s.schema, "flow_runs")+` WHERE run_id=$1`, candidate.RunID).Scan(&status, &deadline); err != nil {
		return false, MapError("load lease run", err)
	}
	if status != "running" && status != "failing" {
		return false, nil
	}
	if deadline != nil && !semantic.DBNow().Before(*deadline) {
		if err := s.expireRunLocked(ctx, semantic, "run deadline reached"); err != nil {
			return false, err
		}
		if err := semantic.Commit(ctx); err != nil {
			return false, err
		}
		return true, nil
	}
	var key, commandState, queueState string
	var attempt, consumed int
	var attemptID uuid.UUID
	var leaseExpiresAt time.Time
	var attemptPosition int64
	err = semantic.PGX().QueryRow(ctx, `SELECT c.command_key,c.state,c.attempt_ordinal,c.consumed_attempts,
		q.state,q.active_attempt_id,q.lease_expires_at,
		(SELECT position FROM `+pgschema.Table(s.schema, "flow_journal")+`
		 WHERE run_id=c.run_id AND attempt_id=q.active_attempt_id AND entry_kind='attempt_started')
		FROM `+pgschema.Table(s.schema, "flow_commands")+` c
		JOIN `+pgschema.Table(s.schema, "flow_command_queue")+` q ON q.command_id=c.command_id
		WHERE c.command_id=$1 AND c.run_id=$2 FOR UPDATE OF c,q SKIP LOCKED`,
		candidate.CommandID, candidate.RunID).Scan(&key, &commandState, &attempt, &consumed,
		&queueState, &attemptID, &leaseExpiresAt, &attemptPosition)
	if errors.Is(err, pgx.ErrNoRows) {
		return false, nil
	}
	if err != nil {
		return false, MapError("lock expired command lease", err)
	}
	if commandState != "running" || queueState != "running" || semantic.DBNow().Before(leaseExpiresAt) {
		return false, nil
	}
	next := semantic.DBNow()
	concluded, err := NewJournalEntry(AttemptConcluded, journalcodec.AttemptConcludedBody{
		V: 1, AttemptID: attemptID.String(), CommandID: candidate.CommandID.String(), CommandKey: key,
		Attempt: attempt, Classification: string(retrypolicy.ClassLeaseLost), ConsumedAttempts: consumed,
		FinishedAt: semantic.DBNow(), NextAttemptAt: &next, ErrorCode: "lease_lost", ErrorMessage: "lease expired before settlement",
	})
	if err != nil {
		return false, err
	}
	concluded.CommandID = clonePointer(&candidate.CommandID)
	concluded.AttemptID = clonePointer(&attemptID)
	concluded.CausationPosition = clonePointer(&attemptPosition)
	if _, err := semantic.Apply(ctx, PersistedChangeSet{Journal: []JournalEntry{concluded}}); err != nil {
		return false, err
	}
	failure := terminalFailure{Code: "lease_lost", Message: "lease expired before settlement"}
	if _, err := semantic.PGX().Exec(ctx, `UPDATE `+pgschema.Table(s.schema, "flow_commands")+`
		SET state='retry_wait',last_error=$2::jsonb,next_attempt_at=$3,updated_at=$3,status_at=$3 WHERE command_id=$1`,
		candidate.CommandID, jsonString(failure), semantic.DBNow()); err != nil {
		return false, MapError("recover expired command", err)
	}
	if _, err := semantic.PGX().Exec(ctx, `UPDATE `+pgschema.Table(s.schema, "flow_command_queue")+`
		SET state='retry_wait',next_run_at=$2,active_attempt_id=NULL,lease_token=NULL,lease_owner=NULL,
		    lease_started_at=NULL,lease_expires_at=NULL WHERE command_id=$1`,
		candidate.CommandID, semantic.DBNow()); err != nil {
		return false, MapError("recover expired command delivery", err)
	}
	if err := semantic.NotifyRunnableCommands(ctx); err != nil {
		return false, err
	}
	if err := semantic.Commit(ctx); err != nil {
		return false, err
	}
	return true, nil
}
