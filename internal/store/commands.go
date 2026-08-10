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
	CommandID   uuid.UUID
	ExecutionID uuid.UUID
	Queue       string
	Name        string
	Version     int
	NextRunAt   time.Time
}

type CommandProbeCursor struct {
	NextRunAt time.Time
	Queue     string
	CommandID uuid.UUID
}

func (s *Store) ProbeCommands(ctx context.Context, kinds []CommandKind, limit int) ([]CommandCandidate, error) {
	return s.ProbeCommandsExcluding(ctx, kinds, limit, nil, nil, nil)
}

// ProbeCommandsExcluding returns runnable candidates while omitting executions
// already found to be busy and queues with no process-local lane capacity
// during the caller's current scheduling pass. This lets a bounded probe make
// room for other work without broadening the database transaction that tests
// an execution fence.
func (s *Store) ProbeCommandsExcluding(
	ctx context.Context,
	kinds []CommandKind,
	limit int,
	excludedExecutionIDs []uuid.UUID,
	excludedQueues []string,
	after *CommandProbeCursor,
) ([]CommandCandidate, error) {
	if len(kinds) == 0 || limit <= 0 {
		return nil, nil
	}
	for _, executionID := range excludedExecutionIDs {
		if executionID == uuid.Nil {
			return nil, fmt.Errorf("%w: invalid excluded execution", flowerr.ErrInvalid)
		}
	}
	for _, queue := range excludedQueues {
		if queue == "" {
			return nil, fmt.Errorf("%w: invalid excluded queue", flowerr.ErrInvalid)
		}
	}
	var afterNextRunAt *time.Time
	afterQueue := ""
	afterCommandID := uuid.Nil
	if after != nil {
		if after.NextRunAt.IsZero() || after.Queue == "" || after.CommandID == uuid.Nil {
			return nil, fmt.Errorf("%w: invalid command probe cursor", flowerr.ErrInvalid)
		}
		afterNextRunAt, afterQueue, afterCommandID = &after.NextRunAt, after.Queue, after.CommandID
	}
	names := make([]string, len(kinds))
	versions := make([]int32, len(kinds))
	for index, kind := range kinds {
		if kind.Name == "" || kind.Version <= 0 {
			return nil, fmt.Errorf("%w: invalid command probe kind", flowerr.ErrInvalid)
		}
		version, err := durable.PostgresInteger32("command probe version", kind.Version, 1, durable.PostgresIntegerMax)
		if err != nil {
			return nil, err
		}
		names[index], versions[index] = kind.Name, version
	}
	rows, err := s.db.Conn.Query(ctx, `WITH handled(name,version) AS (
		SELECT * FROM unnest($1::text[],$2::integer[])
	)
	SELECT q.command_id,q.execution_id,q.queue,q.name,q.version,q.next_run_at
	FROM handled h
	CROSS JOIN LATERAL (
		SELECT candidate.command_id,candidate.execution_id,candidate.queue,candidate.name,candidate.version,candidate.next_run_at
		FROM `+pgschema.Table(s.schema, "flow_command_queue")+` candidate
		WHERE candidate.name=h.name AND candidate.version=h.version
		  AND candidate.state IN ('ready','retry_wait') AND candidate.next_run_at<=clock_timestamp()
		  AND NOT (candidate.execution_id=ANY(COALESCE($4::uuid[],'{}'::uuid[])))
		  AND NOT (candidate.queue=ANY(COALESCE($5::text[],'{}'::text[])))
		  AND ($6::timestamptz IS NULL OR
		       (candidate.next_run_at,candidate.queue,candidate.command_id)>($6::timestamptz,$7::text,$8::uuid))
		ORDER BY candidate.next_run_at,candidate.queue,candidate.command_id
		LIMIT $3
	) q
	JOIN `+pgschema.Table(s.schema, "flow_executions")+` e ON e.execution_id=q.execution_id
	WHERE e.status IN ('running','failing')
	ORDER BY q.next_run_at,q.queue,q.command_id
	LIMIT $3`, names, versions, limit, excludedExecutionIDs, excludedQueues,
		afterNextRunAt, afterQueue, afterCommandID)
	if err != nil {
		return nil, MapError("probe commands", err)
	}
	defer rows.Close()
	result := make([]CommandCandidate, 0, limit)
	for rows.Next() {
		var candidate CommandCandidate
		if err := rows.Scan(&candidate.CommandID, &candidate.ExecutionID, &candidate.Queue,
			&candidate.Name, &candidate.Version, &candidate.NextRunAt); err != nil {
			return nil, MapError("scan command candidate", err)
		}
		result = append(result, candidate)
	}
	if err := rows.Err(); err != nil {
		return nil, MapError("read command candidates", err)
	}
	return result, nil
}

type ClaimedCommand struct {
	CommandID              uuid.UUID
	ExecutionID            uuid.UUID
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
	ExecutionDeadline      *time.Time
	Attempt                int
	ConsumedAttempts       int
	AttemptID              uuid.UUID
	LeaseToken             uuid.UUID
	DBNow                  time.Time
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
	lease time.Duration,
	owner string,
	hook fault.Hook,
) (ClaimResult, error) {
	batch, err := s.ClaimCommands(ctx, []CommandCandidate{candidate}, lease, owner, hook)
	result := ClaimResult{Progressed: batch.Progressed}
	if len(batch.Commands) > 0 {
		result.Command = &batch.Commands[0]
	}
	return result, err
}

// ClaimCommands claims a bounded set of candidates from one execution under
// one execution lock and one commit. Candidate rows remain individually
// skip-locked, so a busy sibling does not make the batch wait.
func (s *Store) ClaimCommands(
	ctx context.Context,
	candidates []CommandCandidate,
	lease time.Duration,
	owner string,
	hook fault.Hook,
) (ClaimBatchResult, error) {
	if len(candidates) == 0 {
		return ClaimBatchResult{}, nil
	}
	executionID := candidates[0].ExecutionID
	seen := make(map[uuid.UUID]struct{}, len(candidates))
	for _, candidate := range candidates {
		if candidate.CommandID == uuid.Nil || candidate.ExecutionID == uuid.Nil || candidate.ExecutionID != executionID ||
			candidate.Name == "" || candidate.Version <= 0 {
			return ClaimBatchResult{}, fmt.Errorf("%w: incomplete or mixed-execution command claim batch", flowerr.ErrInvalid)
		}
		if err := durable.PostgresInteger("queue command version", candidate.Version, 1, durable.PostgresIntegerMax); err != nil {
			return ClaimBatchResult{}, err
		}
		if _, duplicate := seen[candidate.CommandID]; duplicate {
			return ClaimBatchResult{}, fmt.Errorf("%w: duplicate command in claim batch", flowerr.ErrInvalid)
		}
		seen[candidate.CommandID] = struct{}{}
	}
	if lease <= 0 || owner == "" {
		return ClaimBatchResult{}, fmt.Errorf("%w: incomplete command claim", flowerr.ErrInvalid)
	}
	if _, err := durable.ExactMilliseconds("command lease", lease); err != nil {
		return ClaimBatchResult{}, err
	}
	if hook == nil {
		hook = fault.None{}
	}
	if err := hook.Hit(ctx, fault.ClaimExecutionLock); err != nil {
		return ClaimBatchResult{}, err
	}
	semantic, err := s.BeginSemantic(ctx, executionID, LockSkipLocked)
	if errors.Is(err, ErrLockUnavailable) {
		return ClaimBatchResult{}, nil
	}
	if err != nil {
		return ClaimBatchResult{}, err
	}
	defer semantic.Rollback(context.WithoutCancel(ctx))

	// The execution row is FOR-UPDATE locked for the whole batch (BeginSemantic
	// above), so its status and deadline cannot change until this transaction
	// commits or rolls back: one read here covers every candidate instead of
	// one read per candidate.
	var executionStatus string
	var executionDeadline *time.Time
	if err := semantic.PGX().QueryRow(ctx, `SELECT status,deadline_at FROM `+
		pgschema.Table(s.schema, "flow_executions")+` WHERE execution_id=$1`, executionID).
		Scan(&executionStatus, &executionDeadline); err != nil {
		return ClaimBatchResult{}, MapError("load claim execution", err)
	}
	if (executionStatus != "running" && executionStatus != "failing") ||
		(executionDeadline != nil && !semantic.DBNow().Before(*executionDeadline)) {
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
	leaseMilliseconds, err := durable.ExactMilliseconds("command lease", lease)
	if err != nil {
		return ClaimBatchResult{}, err
	}
	leaseExpiresAt, err := durable.AddExactDuration("command lease", semantic.DBNow(), lease)
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
				LeaseDurationMS: leaseMilliseconds, ConsumedAttempts: command.consumed,
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
		if err := s.updateClaimBatch(ctx, semantic, claimable, owner, leaseExpiresAt); err != nil {
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
				CommandID: command.candidate.CommandID, ExecutionID: command.candidate.ExecutionID,
				CommandKey: command.key, Name: command.name, Version: command.version, Queue: command.queue,
				Args: slices.Clone(command.args), EventInputs: command.eventInputs,
				RetryMaxElapsed: clonePointer(command.retryPolicy.MaxElapsed), AttemptTimeout: command.attemptTimeout,
				CreatedAt: command.createdAt, BudgetStartedAt: command.budgetStartedAt,
				ExecutionDeadline: clonePointer(executionDeadline), Attempt: command.attempt,
				ConsumedAttempts: command.consumed, AttemptID: command.attemptID, LeaseToken: command.leaseToken,
				DBNow: semantic.DBNow(), LeaseExpiresAt: leaseExpiresAt, AttemptStartedPosition: row.Position,
			})
		}
		result.Progressed = true
		current = semantic.continueBatch()
	}

	// Elapsed-budget expiry is uncommon and can enter fail-fast, append several
	// semantic rows, and terminalize the execution. Keep that transition focused
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
		if err := s.failBeforeClaimLocked(ctx, current, command.candidate.CommandID, command.key, command.required,
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
	candidate       CommandCandidate
	key             string
	name            string
	version         int
	queue           string
	args            []byte
	timeoutMS       *int64
	policyBytes     []byte
	createdAt       time.Time
	budgetStartedAt time.Time
	ordinal         int
	consumed        int
	createdPosition int64
	required        bool
	retryPolicy     retrypolicy.Policy
	attemptTimeout  time.Duration
	attempt         int
	attemptID       uuid.UUID
	leaseToken      uuid.UUID
	eventInputs     []ClaimedEventInput
}

func (s *Store) lockClaimBatch(ctx context.Context, semantic *SemanticTx, candidates []CommandCandidate) ([]claimBatchCommand, error) {
	commandIDs := make([]uuid.UUID, len(candidates))
	for index := range candidates {
		commandIDs[index] = candidates[index].CommandID
	}
	rows, err := semantic.PGX().Query(ctx, `WITH requested AS (
		SELECT command_id,ordinality FROM unnest($1::uuid[]) WITH ORDINALITY AS r(command_id,ordinality)
	)
	SELECT r.ordinality,c.command_key,c.name,c.version,c.args,c.queue,c.attempt_timeout_ms,
		c.retry_policy,c.created_at,c.budget_started_at,c.attempt_ordinal,c.consumed_attempts,
		c.created_position,c.required,c.state,q.state,q.next_run_at
	FROM requested r
	JOIN `+pgschema.Table(s.schema, "flow_command_queue")+` q ON q.command_id=r.command_id
	JOIN `+pgschema.Table(s.schema, "flow_commands")+` c ON c.command_id=q.command_id
	WHERE q.execution_id=$2
	ORDER BY r.ordinality
	FOR UPDATE OF q,c SKIP LOCKED`, commandIDs, semantic.ExecutionID())
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
			&command.timeoutMS, &command.policyBytes, &command.createdAt, &command.budgetStartedAt, &command.ordinal,
			&command.consumed, &command.createdPosition, &command.required, &commandState, &queueState, &nextRunAt); err != nil {
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
		  ON j.execution_id=w.execution_id AND j.position=w.satisfied_position
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
	leaseExpiresAt time.Time,
) error {
	commandIDs := make([]uuid.UUID, len(commands))
	attemptIDs := make([]uuid.UUID, len(commands))
	leaseTokens := make([]uuid.UUID, len(commands))
	attempts := make([]int32, len(commands))
	for index := range commands {
		commandIDs[index] = commands[index].candidate.CommandID
		attemptIDs[index] = commands[index].attemptID
		leaseTokens[index] = commands[index].leaseToken
		attempts[index] = int32(commands[index].attempt)
	}
	queueResult, err := semantic.PGX().Exec(ctx, `WITH claimed(command_id,attempt_id,lease_token) AS (
		SELECT * FROM unnest($1::uuid[],$2::uuid[],$3::uuid[])
	)
	UPDATE `+pgschema.Table(s.schema, "flow_command_queue")+` q
	SET state='running',active_attempt_id=claimed.attempt_id,lease_token=claimed.lease_token,lease_owner=$4,
	    lease_started_at=$5,lease_expires_at=$6
	FROM claimed
	WHERE q.command_id=claimed.command_id AND q.execution_id=$7 AND q.state IN ('ready','retry_wait')`,
		commandIDs, attemptIDs, leaseTokens, owner, semantic.DBNow(), leaseExpiresAt, semantic.ExecutionID())
	if err != nil {
		return MapError("claim command queue batch", err)
	}
	if queueResult.RowsAffected() != int64(len(commands)) {
		return fmt.Errorf("%w: claimed %d of %d command queue rows", flowerr.ErrInvalidState,
			queueResult.RowsAffected(), len(commands))
	}
	commandResult, err := semantic.PGX().Exec(ctx, `WITH claimed(command_id,attempt) AS (
		SELECT * FROM unnest($1::uuid[],$2::integer[])
	)
	UPDATE `+pgschema.Table(s.schema, "flow_commands")+` c
	SET state='running',attempt_ordinal=claimed.attempt,updated_at=$3,status_at=$3
	FROM claimed
	WHERE c.command_id=claimed.command_id AND c.execution_id=$4 AND c.state IN ('ready','retry_wait')`,
		commandIDs, attempts, semantic.DBNow(), semantic.ExecutionID())
	if err != nil {
		return MapError("mark command batch running", err)
	}
	if commandResult.RowsAffected() != int64(len(commands)) {
		return fmt.Errorf("%w: marked %d of %d commands running", flowerr.ErrInvalidState,
			commandResult.RowsAffected(), len(commands))
	}
	return nil
}

func (s *Store) claimCandidateStillEligible(ctx context.Context, semantic *SemanticTx, commandID uuid.UUID) (bool, error) {
	var eligible bool
	if err := semantic.PGX().QueryRow(ctx, `SELECT EXISTS (
		SELECT 1 FROM `+pgschema.Table(s.schema, "flow_commands")+` c
		JOIN `+pgschema.Table(s.schema, "flow_command_queue")+` q ON q.command_id=c.command_id
		WHERE c.command_id=$1 AND c.execution_id=$2 AND c.state IN ('ready','retry_wait')
		  AND q.state=c.state AND q.next_run_at<=$3
	)`, commandID, semantic.ExecutionID(), semantic.DBNow()).Scan(&eligible); err != nil {
		return false, MapError("recheck elapsed claim candidate", err)
	}
	return eligible, nil
}

func (s *Store) failBeforeClaimLocked(
	ctx context.Context,
	semantic *SemanticTx,
	commandID uuid.UUID,
	key string,
	required bool,
	code, message string,
	causation int64,
) error {
	head, err := s.LoadExecutionHead(ctx, semantic)
	if err != nil {
		return err
	}
	failureEffects := failureResolution{}
	if required {
		failureEffects, err = s.resolveRequiredFailureLocked(ctx, semantic, commandID, "failed", head.FailFast)
	}
	if err != nil {
		return err
	}
	commandEvent, err := terminalEventWithCode(commandID, key, "failed", code, message, "flow.command_failed", "command_terminal")
	if err != nil {
		return err
	}
	commandEvent.CausationPosition = clonePointer(&causation)
	entries := []JournalEntry{commandEvent}
	if required && head.Status == "running" {
		survivors := make([]string, len(failureEffects.survivors))
		for index, command := range failureEffects.survivors {
			survivors[index] = command.key
		}
		failing, err := NewJournalEntry(ExecutionFailing, map[string]any{
			"v": 1, "status": "failing", "reason": message, "command_key": key,
			"fail_fast": head.FailFast, "survivors": survivors,
		})
		if err != nil {
			return err
		}
		zero := 0
		failing.CausationBatchIndex = &zero
		entries = append(entries, failing)
	}
	cancelledOffset := len(entries)
	cancelledEntries, err := failureEffects.cancellationEntries(0, "cancelled by fail-fast after required command failure")
	if err != nil {
		return err
	}
	entries = append(entries, cancelledEntries...)
	executionFailed := required || head.Status == "failing"
	effectiveOpen, err := durable.AddPostgresInteger("execution open commands", head.OpenCommands,
		-1, 0, durable.PostgresIntegerMax)
	if err != nil {
		return err
	}
	effectiveOpen, err = durable.AddPostgresInteger("execution open commands", effectiveOpen,
		-len(failureEffects.cancelled), 0, durable.PostgresIntegerMax)
	if err != nil {
		return err
	}
	terminalExecution := effectiveOpen == 0
	if terminalExecution {
		status, eventName, reason := "succeeded", "flow.execution_succeeded", ""
		if executionFailed {
			status, eventName, reason = "failed", "flow.execution_failed", message
		}
		terminal, err := executionTerminalEvent(status, reason, eventName)
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
	if required {
		if err := s.applyFailureResolution(ctx, semantic, failureEffects, journal, cancelledOffset,
			"cancelled by fail-fast after required command failure"); err != nil {
			return err
		}
	}
	status := head.Status
	if required {
		status = "failing"
	}
	if terminalExecution {
		status = "succeeded"
		if executionFailed {
			status = "failed"
		}
	}
	if _, err := semantic.PGX().Exec(ctx, `UPDATE `+pgschema.Table(s.schema, "flow_executions")+`
		SET status=$2,open_commands=$5,failure=CASE WHEN $6 THEN $3::jsonb ELSE failure END,
		    finished_at=CASE WHEN $2 IN ('failed','succeeded') THEN $4 ELSE finished_at END,
		    updated_at=$4,status_at=CASE WHEN status<>$2 THEN $4 ELSE status_at END
		WHERE execution_id=$1`, semantic.ExecutionID(), status, jsonString(failure), semantic.DBNow(),
		effectiveOpen, executionFailed); err != nil {
		return MapError("update execution after pre-claim failure", err)
	}
	return nil
}

type LeaseRenewal struct {
	CommandID uuid.UUID
	AttemptID uuid.UUID
	Token     uuid.UUID
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

func (s *Store) RenewCommandLeases(ctx context.Context, leases []LeaseRenewal, duration time.Duration) ([]LeaseRenewalResult, error) {
	if len(leases) == 0 {
		return nil, nil
	}
	if duration <= 0 {
		return nil, fmt.Errorf("%w: lease duration must be positive", flowerr.ErrInvalid)
	}
	durationMilliseconds, err := durable.ExactMilliseconds("lease duration", duration)
	if err != nil {
		return nil, err
	}
	commandIDs := make([]uuid.UUID, len(leases))
	attemptIDs := make([]uuid.UUID, len(leases))
	tokens := make([]uuid.UUID, len(leases))
	seen := make(map[uuid.UUID]struct{}, len(leases))
	for index, lease := range leases {
		if lease.CommandID == uuid.Nil || lease.AttemptID == uuid.Nil || lease.Token == uuid.Nil {
			return nil, fmt.Errorf("%w: incomplete command lease renewal", flowerr.ErrInvalid)
		}
		if _, duplicate := seen[lease.CommandID]; duplicate {
			return nil, fmt.Errorf("%w: duplicate command lease renewal", flowerr.ErrInvalid)
		}
		seen[lease.CommandID] = struct{}{}
		commandIDs[index], attemptIDs[index], tokens[index] = lease.CommandID, lease.AttemptID, lease.Token
	}
	rows, err := s.db.Conn.Query(ctx, `WITH now_value AS (SELECT clock_timestamp() AS now), requested(command_id,attempt_id,token,ordinal) AS (
		SELECT command_id,attempt_id,token,ordinality
		FROM unnest($1::uuid[],$2::uuid[],$3::uuid[]) WITH ORDINALITY AS input(command_id,attempt_id,token,ordinality)
	), observed AS MATERIALIZED (
		SELECT r.command_id AS requested_command_id,r.attempt_id AS requested_attempt_id,r.token AS requested_token,r.ordinal,
		       q.command_id,q.state,q.active_attempt_id,q.lease_token,q.lease_expires_at
		FROM requested r
		LEFT JOIN `+pgschema.Table(s.schema, "flow_command_queue")+` q ON q.command_id=r.command_id
	), lockable AS (
		SELECT q.command_id
		FROM `+pgschema.Table(s.schema, "flow_command_queue")+` q
		JOIN requested r ON r.command_id=q.command_id
		CROSS JOIN now_value n
		WHERE q.active_attempt_id=r.attempt_id AND q.lease_token=r.token
		  AND q.state='running' AND q.lease_expires_at>n.now
		FOR UPDATE OF q SKIP LOCKED
	), renewed AS (
		UPDATE `+pgschema.Table(s.schema, "flow_command_queue")+` q
		SET lease_expires_at=n.now+($4 * interval '1 millisecond')
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
	Head                   ExecutionHead
	ExecutionDeadline      *time.Time
	Key                    string
	Name                   string
	Version                int
	State                  string
	QueueState             string
	Required               bool
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
	semantic, err := s.BeginSemantic(ctx, request.Claim.ExecutionID, LockBlocking)
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
	if fence.ExecutionDeadline != nil && !semantic.DBNow().Before(*fence.ExecutionDeadline) {
		if err := s.expireExecutionLocked(ctx, semantic, "execution deadline reached"); err != nil {
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
		preparedChildren, err = s.prepareCommandBatch(ctx, semantic.PGX(), semantic.ExecutionID(),
			childCreates, semantic.DBNow(), fence.ExecutionDeadline, request.Events)
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
	cancelStagedChildren := fence.Head.Status == "failing" && fence.Head.FailFast
	childCancellationIndexes := make([]int, len(request.Children))
	if cancelStagedChildren {
		for index, child := range request.Children {
			cancelled, err := terminalEventWithCode(child.ID, child.Key, "cancelled", "fail_fast",
				"cancelled because the execution is failing", "flow.command_cancelled", "command_terminal")
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
	effectiveOpen, err := durable.AddPostgresInteger("execution open commands", fence.Head.OpenCommands,
		-1, 0, durable.PostgresIntegerMax)
	if err != nil {
		return SettleResult{}, err
	}
	effectiveOpen, err = durable.AddPostgresInteger("execution open commands", effectiveOpen,
		effectiveChildren, 0, durable.PostgresIntegerMax)
	if err != nil {
		return SettleResult{}, err
	}
	commandCount, err := durable.AddPostgresInteger("execution command count", fence.Head.CommandCount,
		len(request.Children), 0, durable.PostgresIntegerMax)
	if err != nil {
		return SettleResult{}, err
	}
	terminalExecution := effectiveOpen == 0
	terminalStatus := "succeeded"
	terminalName := "flow.execution_succeeded"
	if fence.Head.Status == "failing" {
		terminalStatus = "failed"
		terminalName = "flow.execution_failed"
	}
	if terminalExecution {
		terminal, err := executionTerminalEvent(terminalStatus, "", terminalName)
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
	if _, err := semantic.PGX().Exec(ctx, `UPDATE `+pgschema.Table(s.schema, "flow_commands")+`
		SET state='succeeded',result=$2,last_error=NULL,terminal_failure=NULL,
		    terminal_position=$3,finished_at=$4,updated_at=$4,status_at=$4
		WHERE command_id=$1`, request.Claim.CommandID, request.Result.Bytes,
		layout.parentTerminalPosition(journal), semantic.DBNow()); err != nil {
		return SettleResult{}, MapError("settle successful command", err)
	}
	if _, err := semantic.PGX().Exec(ctx, `DELETE FROM `+pgschema.Table(s.schema, "flow_command_queue")+` WHERE command_id=$1`, request.Claim.CommandID); err != nil {
		return SettleResult{}, MapError("remove successful command queue row", err)
	}
	immediatelyRunnable := preparedChildren.immediatelyRunnable && !cancelStagedChildren
	if len(request.Children) > 0 {
		if err := s.insertPreparedCommandBatch(ctx, semantic.PGX(), semantic.ExecutionID(), semantic.DBNow(), preparedChildren); err != nil {
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
	if terminalExecution {
		if _, err := semantic.PGX().Exec(ctx, `UPDATE `+pgschema.Table(s.schema, "flow_executions")+`
			SET status=$3,open_commands=$4,command_count=$5,finished_at=$2,updated_at=$2,status_at=$2
			WHERE execution_id=$1`, semantic.ExecutionID(), semantic.DBNow(), terminalStatus, effectiveOpen, commandCount); err != nil {
			return SettleResult{}, MapError("complete direct execution", err)
		}
	} else {
		if _, err := semantic.PGX().Exec(ctx, `UPDATE `+pgschema.Table(s.schema, "flow_executions")+`
			SET open_commands=$3,command_count=$4,
			    updated_at=$2 WHERE execution_id=$1`, semantic.ExecutionID(), semantic.DBNow(), effectiveOpen, commandCount); err != nil {
			return SettleResult{}, MapError("update execution after command success", err)
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
	failure := terminalFailure{Code: "fail_fast", Message: "cancelled because the execution is failing"}
	commandTag, err := semantic.PGX().Exec(ctx, `WITH cancelled(command_id,terminal_position) AS (
		SELECT * FROM unnest($2::uuid[],$3::bigint[])
	)
	UPDATE `+pgschema.Table(s.schema, "flow_commands")+` AS c
	SET state='cancelled',terminal_failure=$4::jsonb,terminal_position=cancelled.terminal_position,
	    finished_at=$5,updated_at=$5,status_at=$5
	FROM cancelled
	WHERE c.execution_id=$1 AND c.command_id=cancelled.command_id
	  AND c.state IN ('pending','ready')`,
		semantic.ExecutionID(), commandIDs, terminalPositions, jsonString(failure), semantic.DBNow())
	if err != nil {
		return MapError("cancel staged command batch while execution is failing", err)
	}
	if commandTag.RowsAffected() != int64(len(commandIDs)) {
		return fmt.Errorf("%w: staged-child cancellation batch updated %d of %d rows",
			flowerr.ErrInvalidState, commandTag.RowsAffected(), len(commandIDs))
	}
	if len(batch.queues) == 0 {
		return nil
	}
	queueTag, err := semantic.PGX().Exec(ctx, `DELETE FROM `+pgschema.Table(s.schema, "flow_command_queue")+`
		WHERE execution_id=$1 AND command_id=ANY($2::uuid[])`, semantic.ExecutionID(), commandIDs)
	if err != nil {
		return MapError("remove staged command queue batch while execution is failing", err)
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
		commandCount, err := durable.AddPostgresInteger("execution command count", fence.Head.CommandCount,
			len(request.Children), 0, durable.PostgresIntegerMax)
		if err != nil {
			return err
		}
		if fence.Head.MaxCommands > 0 && commandCount > fence.Head.MaxCommands {
			return fmt.Errorf("%w: execution command ceiling exceeded", flowerr.ErrInvalidState)
		}
		openCommands, err := durable.AddPostgresInteger("execution open commands", fence.Head.OpenCommands,
			-1, 0, durable.PostgresIntegerMax)
		if err != nil {
			return err
		}
		if _, err := durable.AddPostgresInteger("execution open commands", openCommands,
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
			WHERE execution_id=$1 AND command_key=ANY($2)`, semantic.ExecutionID(), keys).Scan(&conflicts); err != nil {
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
	semantic, err := s.BeginSemantic(ctx, request.Claim.ExecutionID, LockBlocking)
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
	if fence.ExecutionDeadline != nil && !semantic.DBNow().Before(*fence.ExecutionDeadline) {
		if err := s.expireExecutionLocked(ctx, semantic, "execution deadline reached"); err != nil {
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
		ExecutionDeadline: fence.ExecutionDeadline,
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
	terminalExecution := false
	executionFailed := false
	effectiveOpen := fence.Head.OpenCommands
	if !decision.Retry {
		if fence.Required {
			failureEffects, err = s.resolveRequiredFailureLocked(ctx, semantic, request.Claim.CommandID, "failed", fence.Head.FailFast)
		}
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
		executionFailed = fence.Required || fence.Head.Status == "failing"
		if fence.Required && fence.Head.Status == "running" {
			survivors := make([]string, len(failureEffects.survivors))
			for index, command := range failureEffects.survivors {
				survivors[index] = command.key
			}
			failing, err := NewJournalEntry(ExecutionFailing, map[string]any{
				"v": 1, "status": "failing", "reason": failure.Message, "command_key": fence.Key,
				"fail_fast": fence.Head.FailFast, "survivors": survivors,
			})
			if err != nil {
				return SettleResult{}, err
			}
			failing.CausationBatchIndex = &failedIndex
			entries = append(entries, failing)
		}
		cancelledOffset = len(entries)
		cancelledEntries, err := failureEffects.cancellationEntries(failedIndex, "cancelled by fail-fast after required command failure")
		if err != nil {
			return SettleResult{}, err
		}
		entries = append(entries, cancelledEntries...)
		effectiveOpen, err = durable.AddPostgresInteger("execution open commands", fence.Head.OpenCommands,
			-1, 0, durable.PostgresIntegerMax)
		if err != nil {
			return SettleResult{}, err
		}
		effectiveOpen, err = durable.AddPostgresInteger("execution open commands", effectiveOpen,
			-len(failureEffects.cancelled), 0, durable.PostgresIntegerMax)
		if err != nil {
			return SettleResult{}, err
		}
		terminalExecution = effectiveOpen == 0
		if terminalExecution {
			status, eventName, reason := "succeeded", "flow.execution_succeeded", ""
			if executionFailed {
				status, eventName, reason = "failed", "flow.execution_failed", failure.Message
			}
			terminal, err := executionTerminalEvent(status, reason, eventName)
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
		if _, err := semantic.PGX().Exec(ctx, `UPDATE `+pgschema.Table(s.schema, "flow_commands")+`
			SET state='retry_wait',consumed_attempts=$2,last_error=$3::jsonb,next_attempt_at=$4,
			    updated_at=$5,status_at=$5 WHERE command_id=$1`, request.Claim.CommandID,
			decision.ConsumedAttempts, jsonString(failure), decision.NextAttemptAt, semantic.DBNow()); err != nil {
			return SettleResult{}, MapError("schedule command retry", err)
		}
		if _, err := semantic.PGX().Exec(ctx, `UPDATE `+pgschema.Table(s.schema, "flow_command_queue")+`
			SET state='retry_wait',next_run_at=$2,active_attempt_id=NULL,lease_token=NULL,lease_owner=NULL,
			    lease_started_at=NULL,lease_expires_at=NULL WHERE command_id=$1`,
			request.Claim.CommandID, decision.NextAttemptAt); err != nil {
			return SettleResult{}, MapError("release command for retry", err)
		}
	} else {
		terminalPosition := journal.Journal[1].Position
		if _, err := semantic.PGX().Exec(ctx, `UPDATE `+pgschema.Table(s.schema, "flow_commands")+`
			SET state='failed',consumed_attempts=$2,last_error=$3::jsonb,terminal_failure=$3::jsonb,
			    terminal_position=$4,finished_at=$5,updated_at=$5,status_at=$5 WHERE command_id=$1`,
			request.Claim.CommandID, decision.ConsumedAttempts, jsonString(failure), terminalPosition, semantic.DBNow()); err != nil {
			return SettleResult{}, MapError("fail command", err)
		}
		if _, err := semantic.PGX().Exec(ctx, `DELETE FROM `+pgschema.Table(s.schema, "flow_command_queue")+` WHERE command_id=$1`, request.Claim.CommandID); err != nil {
			return SettleResult{}, MapError("remove failed command queue row", err)
		}
		if fence.Required {
			if err := s.applyFailureResolution(ctx, semantic, failureEffects, journal, cancelledOffset,
				"cancelled by fail-fast after required command failure"); err != nil {
				return SettleResult{}, err
			}
		}
		status := fence.Head.Status
		if fence.Required {
			status = "failing"
		}
		if terminalExecution {
			status = "succeeded"
			if executionFailed {
				status = "failed"
			}
		}
		if _, err := semantic.PGX().Exec(ctx, `UPDATE `+pgschema.Table(s.schema, "flow_executions")+`
			SET status=$2,open_commands=$5,
			    failure=CASE WHEN $6 THEN $3::jsonb ELSE failure END,
			    finished_at=CASE WHEN $2 IN ('failed','succeeded') THEN $4 ELSE finished_at END,
			    updated_at=$4,status_at=CASE WHEN status<>$2 THEN $4 ELSE status_at END
			WHERE execution_id=$1`, semantic.ExecutionID(), status, jsonString(failure), semantic.DBNow(),
			effectiveOpen, executionFailed); err != nil {
			return SettleResult{}, MapError("update execution after command failure", err)
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
	head, err := s.LoadExecutionHead(ctx, semantic)
	if err != nil {
		return commandFence{}, err
	}
	if head.Status != "running" && head.Status != "failing" {
		return commandFence{}, fmt.Errorf("%w: execution is terminal", flowerr.ErrTerminal)
	}
	var result commandFence
	result.Head = head
	if err := semantic.PGX().QueryRow(ctx, `SELECT deadline_at FROM `+pgschema.Table(s.schema, "flow_executions")+`
		WHERE execution_id=$1`, semantic.ExecutionID()).Scan(&result.ExecutionDeadline); err != nil {
		return commandFence{}, MapError("load execution deadline", err)
	}
	var activeAttempt, token *uuid.UUID
	err = semantic.PGX().QueryRow(ctx, `SELECT c.command_key,c.name,c.version,c.state,c.required,c.attempt_ordinal,
		c.consumed_attempts,c.budget_started_at,c.retry_policy,
		q.state,q.active_attempt_id,q.lease_token,q.lease_expires_at,
		(SELECT position FROM `+pgschema.Table(s.schema, "flow_journal")+`
		 WHERE execution_id=c.execution_id AND attempt_id=$2 AND entry_kind='attempt_started')
		FROM `+pgschema.Table(s.schema, "flow_commands")+` c
		JOIN `+pgschema.Table(s.schema, "flow_command_queue")+` q ON q.command_id=c.command_id
		WHERE c.command_id=$1 AND c.execution_id=$3
		FOR UPDATE OF c,q`, claim.CommandID, claim.AttemptID, semantic.ExecutionID()).
		Scan(&result.Key, &result.Name, &result.Version, &result.State, &result.Required,
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

func (s *Store) ProbeExpiredExecutions(ctx context.Context, limit int) ([]uuid.UUID, error) {
	if limit <= 0 {
		return nil, nil
	}
	rows, err := s.db.Conn.Query(ctx, `SELECT execution_id FROM `+pgschema.Table(s.schema, "flow_executions")+`
		WHERE status IN ('running','failing') AND deadline_at IS NOT NULL AND deadline_at<=clock_timestamp()
		ORDER BY deadline_at,execution_id LIMIT $1`, limit)
	if err != nil {
		return nil, MapError("probe expired executions", err)
	}
	defer rows.Close()
	var result []uuid.UUID
	for rows.Next() {
		var id uuid.UUID
		if err := rows.Scan(&id); err != nil {
			return nil, MapError("scan expired execution", err)
		}
		result = append(result, id)
	}
	if err := rows.Err(); err != nil {
		return nil, MapError("read expired executions", err)
	}
	return result, nil
}

func (s *Store) ExpireExecution(ctx context.Context, id uuid.UUID, reason string) (bool, error) {
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
		pgschema.Table(s.schema, "flow_executions")+` WHERE execution_id=$1`, id).Scan(&status, &deadline); err != nil {
		return false, MapError("load expiring execution", err)
	}
	if status != "running" && status != "failing" || deadline == nil || semantic.DBNow().Before(*deadline) {
		return false, nil
	}
	if err := s.expireExecutionLocked(ctx, semantic, reason); err != nil {
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

func (s *Store) expireExecutionLocked(ctx context.Context, semantic *SemanticTx, reason string) error {
	head, err := s.LoadExecutionHead(ctx, semantic)
	if err != nil {
		return err
	}
	if head.Status != "running" && head.Status != "failing" {
		return nil
	}
	rows, err := semantic.PGX().Query(ctx, `SELECT command_id,command_key,state,attempt_ordinal,consumed_attempts,created_position
		FROM `+pgschema.Table(s.schema, "flow_commands")+`
		WHERE execution_id=$1 AND state NOT IN ('succeeded','failed','cancelled','expired')
		ORDER BY command_id FOR UPDATE`, semantic.ExecutionID())
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
		return fmt.Errorf("%w: expiring execution command counter differs", flowerr.ErrInvalidState)
	}
	for index := range commands {
		if commands[index].State != "running" {
			continue
		}
		var token uuid.UUID
		err := semantic.PGX().QueryRow(ctx, `SELECT q.active_attempt_id,q.lease_token,
			(SELECT position FROM `+pgschema.Table(s.schema, "flow_journal")+`
			 WHERE execution_id=$2 AND attempt_id=q.active_attempt_id AND entry_kind='attempt_started')
			FROM `+pgschema.Table(s.schema, "flow_command_queue")+` q WHERE command_id=$1 FOR UPDATE`,
			commands[index].ID, semantic.ExecutionID()).Scan(&commands[index].AttemptID, &token, &commands[index].AttemptStartedPosition)
		if err != nil {
			return MapError("lock expiring command delivery", err)
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
				Attempt: command.Attempt, Classification: "execution_expired", ConsumedAttempts: command.ConsumedAttempts,
				FinishedAt: semantic.DBNow(), ErrorCode: "execution_expired", ErrorMessage: reason,
			})
			if err != nil {
				return err
			}
			concluded.CommandID = clonePointer(&command.ID)
			concluded.AttemptID = clonePointer(command.AttemptID)
			concluded.CausationPosition = clonePointer(command.AttemptStartedPosition)
			entries = append(entries, concluded)
		}
		cancelled, err := terminalEventWithCode(command.ID, command.Key, "cancelled", "execution_expired", reason, "flow.command_cancelled", "command_terminal")
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
	terminal, err := executionTerminalEvent("expired", reason, "flow.execution_expired")
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
	failure := terminalFailure{Code: "execution_expired", Message: reason}
	commandIDs := make([]uuid.UUID, len(commands))
	terminalPositions := make([]int64, len(commands))
	for index, command := range commands {
		journalIndex, exists := terminalBatchIndex[command.ID]
		if !exists || journalIndex < 0 || journalIndex >= len(journal.Journal) {
			return fmt.Errorf("%w: execution expiry journal mapping is invalid", flowerr.ErrInvalidState)
		}
		row := journal.Journal[journalIndex]
		if row.CommandID == nil || *row.CommandID != command.ID || row.TerminalStatus == nil || *row.TerminalStatus != "cancelled" {
			return fmt.Errorf("%w: execution expiry journal mapping differs", flowerr.ErrInvalidState)
		}
		commandIDs[index], terminalPositions[index] = command.ID, row.Position
	}
	commandTag, err := semantic.PGX().Exec(ctx, `UPDATE `+pgschema.Table(s.schema, "flow_commands")+` AS c
		SET state='cancelled',last_error=$4::jsonb,terminal_failure=$4::jsonb,
		    terminal_position=expired.position,finished_at=$5,updated_at=$5,status_at=$5
		FROM unnest($1::uuid[],$2::bigint[]) AS expired(command_id,position)
		WHERE c.execution_id=$3 AND c.command_id=expired.command_id
		  AND c.state NOT IN ('succeeded','failed','cancelled','expired')`,
		commandIDs, terminalPositions, semantic.ExecutionID(), jsonString(failure), semantic.DBNow())
	if err != nil {
		return MapError("expire command batch", err)
	}
	if commandTag.RowsAffected() != int64(len(commandIDs)) {
		return fmt.Errorf("%w: execution expiry set changed", flowerr.ErrInvalidState)
	}
	if _, err := semantic.PGX().Exec(ctx, `DELETE FROM `+pgschema.Table(s.schema, "flow_command_queue")+` WHERE execution_id=$1`, semantic.ExecutionID()); err != nil {
		return MapError("remove expired execution deliveries", err)
	}
	if _, err := semantic.PGX().Exec(ctx, `UPDATE `+pgschema.Table(s.schema, "flow_executions")+`
		SET status='expired',open_commands=0,failure=$2::jsonb,finished_at=$3,updated_at=$3,status_at=$3
		WHERE execution_id=$1`, semantic.ExecutionID(),
		jsonString(failure), semantic.DBNow()); err != nil {
		return MapError("expire execution", err)
	}
	return nil
}

type ExpiredLeaseCandidate struct {
	CommandID   uuid.UUID
	ExecutionID uuid.UUID
}

func (s *Store) ProbeExpiredCommandLeases(ctx context.Context, limit int) ([]ExpiredLeaseCandidate, error) {
	if limit <= 0 {
		return nil, nil
	}
	rows, err := s.db.Conn.Query(ctx, `SELECT command_id,execution_id FROM `+
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
		if err := rows.Scan(&candidate.CommandID, &candidate.ExecutionID); err != nil {
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
	semantic, err := s.BeginSemantic(ctx, candidate.ExecutionID, LockSkipLocked)
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
		pgschema.Table(s.schema, "flow_executions")+` WHERE execution_id=$1`, candidate.ExecutionID).Scan(&status, &deadline); err != nil {
		return false, MapError("load lease execution", err)
	}
	if status != "running" && status != "failing" {
		return false, nil
	}
	if deadline != nil && !semantic.DBNow().Before(*deadline) {
		if err := s.expireExecutionLocked(ctx, semantic, "execution deadline reached"); err != nil {
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
		 WHERE execution_id=c.execution_id AND attempt_id=q.active_attempt_id AND entry_kind='attempt_started')
		FROM `+pgschema.Table(s.schema, "flow_commands")+` c
		JOIN `+pgschema.Table(s.schema, "flow_command_queue")+` q ON q.command_id=c.command_id
		WHERE c.command_id=$1 AND c.execution_id=$2 FOR UPDATE OF c,q SKIP LOCKED`,
		candidate.CommandID, candidate.ExecutionID).Scan(&key, &commandState, &attempt, &consumed,
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
