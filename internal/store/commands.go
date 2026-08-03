package store

import (
	"bytes"
	"context"
	"encoding/json"
	"errors"
	"fmt"
	"slices"
	"strings"
	"time"

	"github.com/google/uuid"
	"github.com/goware/flow/internal/canonical"
	"github.com/goware/flow/internal/fault"
	"github.com/goware/flow/internal/flowerr"
	"github.com/goware/flow/internal/pgschema"
	retrypolicy "github.com/goware/flow/internal/retry"
	"github.com/goware/flow/internal/store/journalcodec"
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

func (s *Store) ProbeCommands(ctx context.Context, kinds []CommandKind, limit int) ([]CommandCandidate, error) {
	if len(kinds) == 0 || limit <= 0 {
		return nil, nil
	}
	names := make([]string, len(kinds))
	versions := make([]int32, len(kinds))
	for index, kind := range kinds {
		if kind.Name == "" || kind.Version <= 0 {
			return nil, fmt.Errorf("%w: invalid command probe kind", flowerr.ErrInvalid)
		}
		names[index], versions[index] = kind.Name, int32(kind.Version)
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
		ORDER BY candidate.next_run_at,candidate.queue,candidate.command_id
		LIMIT $3
	) q
	JOIN `+pgschema.Table(s.schema, "flow_executions")+` e ON e.execution_id=q.execution_id
	WHERE e.status IN ('running','failing')
	ORDER BY q.next_run_at,q.queue,q.command_id
	LIMIT $3`, names, versions, limit)
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
	AttemptStartedPosition int64
}

type ClaimResult struct {
	Command    *ClaimedCommand
	Progressed bool
}

type ClaimBatchResult struct {
	Commands   []ClaimedCommand
	Progressed bool
}

type CommandInput struct {
	Key     string
	Name    string
	Version int
	State   string
	Result  []byte
	Failure *terminalFailure
}

// LoadCommandInputs returns every durable predecessor explicitly named by a
// dependency edge of commandID. The query is batched once before invocation;
// terminal predecessor values are immutable thereafter.
func (s *Store) LoadCommandInputs(ctx context.Context, commandID uuid.UUID) ([]CommandInput, error) {
	rows, err := s.db.Conn.Query(ctx, `SELECT DISTINCT p.command_key,p.name,p.version,p.state,p.result,p.terminal_failure
		FROM `+pgschema.Table(s.schema, "flow_command_dependency_groups")+` g
		JOIN `+pgschema.Table(s.schema, "flow_command_dependency_members")+` m ON m.group_id=g.group_id
		JOIN `+pgschema.Table(s.schema, "flow_commands")+` p ON p.command_id=m.predecessor_command_id
		WHERE g.dependent_command_id=$1 ORDER BY p.command_key`, commandID)
	if err != nil {
		return nil, MapError("load command dependency inputs", err)
	}
	defer rows.Close()
	var result []CommandInput
	for rows.Next() {
		var item CommandInput
		var failureBytes []byte
		if err := rows.Scan(&item.Key, &item.Name, &item.Version, &item.State, &item.Result, &failureBytes); err != nil {
			return nil, MapError("scan command dependency input", err)
		}
		if len(failureBytes) > 0 {
			var failure terminalFailure
			if err := json.Unmarshal(failureBytes, &failure); err != nil {
				return nil, fmt.Errorf("%w: invalid stored terminal failure", flowerr.ErrInvalidState)
			}
			item.Failure = &failure
		}
		result = append(result, item)
	}
	if err := rows.Err(); err != nil {
		return nil, MapError("read command dependency inputs", err)
	}
	return result, nil
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
		if _, duplicate := seen[candidate.CommandID]; duplicate {
			return ClaimBatchResult{}, fmt.Errorf("%w: duplicate command in claim batch", flowerr.ErrInvalid)
		}
		seen[candidate.CommandID] = struct{}{}
	}
	if lease <= 0 || owner == "" {
		return ClaimBatchResult{}, fmt.Errorf("%w: incomplete command claim", flowerr.ErrInvalid)
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

	result := ClaimBatchResult{Commands: make([]ClaimedCommand, 0, len(candidates))}
	current := semantic
	for _, candidate := range candidates {
		claimed, stale, claimErr := s.claimCommandLocked(ctx, current, candidate, executionStatus, executionDeadline, lease, owner, hook)
		if claimErr != nil {
			return ClaimBatchResult{}, claimErr
		}
		if current.applied {
			current = semantic.continueBatch()
		}
		if stale {
			continue
		}
		result.Progressed = true
		if claimed != nil {
			result.Commands = append(result.Commands, *claimed)
		}
	}
	if !result.Progressed {
		return result, nil
	}
	if err := hook.Hit(ctx, fault.ClaimBeforeCommit); err != nil {
		return ClaimBatchResult{}, err
	}
	if err := semantic.Commit(ctx); err != nil {
		return ClaimBatchResult{}, err
	}
	if err := hook.Hit(ctx, fault.ClaimCommitAmbiguous); err != nil {
		return result, err
	}
	return result, nil
}

func (s *Store) claimCommandLocked(
	ctx context.Context,
	semantic *SemanticTx,
	candidate CommandCandidate,
	executionStatus string,
	executionDeadline *time.Time,
	lease time.Duration,
	owner string,
	hook fault.Hook,
) (*ClaimedCommand, bool, error) {
	if executionStatus != "running" && executionStatus != "failing" {
		return nil, true, nil
	}
	if executionDeadline != nil && !semantic.DBNow().Before(*executionDeadline) {
		return nil, true, nil
	}

	var key, name, queue, commandState, queueState string
	var version, ordinal, consumed int
	var args, policyBytes []byte
	var timeoutMS *int64
	var createdAt, budgetStartedAt, nextRunAt time.Time
	var createdPosition int64
	var failureScope, required bool
	err := semantic.PGX().QueryRow(ctx, `SELECT c.command_key,c.name,c.version,c.args,c.queue,c.attempt_timeout_ms,
		c.retry_policy::text::bytea,c.created_at,c.budget_started_at,c.attempt_ordinal,c.consumed_attempts,
		c.created_position,c.failure_scope,c.required,c.state,q.state,q.next_run_at
		FROM `+pgschema.Table(s.schema, "flow_command_queue")+` q
		JOIN `+pgschema.Table(s.schema, "flow_commands")+` c ON c.command_id=q.command_id
		WHERE q.command_id=$1 AND q.execution_id=$2
		FOR UPDATE OF q,c SKIP LOCKED`, candidate.CommandID, semantic.ExecutionID()).
		Scan(&key, &name, &version, &args, &queue, &timeoutMS, &policyBytes, &createdAt,
			&budgetStartedAt, &ordinal, &consumed, &createdPosition, &failureScope, &required, &commandState, &queueState, &nextRunAt)
	if errors.Is(err, pgx.ErrNoRows) {
		return nil, true, nil
	}
	if err != nil {
		return nil, false, MapError("lock command claim", err)
	}
	if name != candidate.Name || version != candidate.Version || queue != candidate.Queue ||
		(commandState != "ready" && commandState != "retry_wait") || commandState != queueState ||
		semantic.DBNow().Before(nextRunAt) {
		return nil, true, nil
	}
	policy, err := retrypolicy.PublicFromCanonical(policyBytes)
	if err != nil {
		return nil, false, fmt.Errorf("%w: stored retry policy is invalid", flowerr.ErrInvalidState)
	}
	policyValue := retrypolicy.ValueOf(policy)
	if policyValue.MaxElapsed != nil && !semantic.DBNow().Before(budgetStartedAt.Add(*policyValue.MaxElapsed)) {
		if err := s.failBeforeClaimLocked(ctx, semantic, candidate.CommandID, key, required,
			"retry_elapsed", "retry elapsed budget expired", createdPosition); err != nil {
			return nil, false, err
		}
		return nil, false, nil
	}

	if err := hook.Hit(ctx, fault.ClaimBeforeJournal); err != nil {
		return nil, false, err
	}
	attemptID, token := uuid.New(), uuid.New()
	started, err := NewJournalEntry(AttemptStarted, journalcodec.AttemptStartedBody{
		V: 1, AttemptID: attemptID.String(), CommandID: candidate.CommandID.String(), CommandKey: key,
		Attempt: ordinal + 1, StartedAt: semantic.DBNow(), Worker: owner,
		LeaseDurationMS: lease.Milliseconds(), ConsumedAttempts: consumed, BudgetStartedAt: budgetStartedAt,
	})
	if err != nil {
		return nil, false, err
	}
	started.CommandID = clonePointer(&candidate.CommandID)
	started.AttemptID = clonePointer(&attemptID)
	started.CausationPosition = clonePointer(&createdPosition)
	journal, err := semantic.Apply(ctx, PersistedChangeSet{Journal: []JournalEntry{started}})
	if err != nil {
		return nil, false, err
	}
	leaseExpiresAt := semantic.DBNow().Add(lease)
	if _, err := semantic.PGX().Exec(ctx, `UPDATE `+pgschema.Table(s.schema, "flow_command_queue")+`
		SET state='running',active_attempt_id=$2,lease_token=$3,lease_owner=$4,
		    lease_started_at=$5,lease_expires_at=$6,updated_at=$5
		WHERE command_id=$1`, candidate.CommandID, attemptID, token, owner, semantic.DBNow(), leaseExpiresAt); err != nil {
		return nil, false, MapError("claim command queue row", err)
	}
	if _, err := semantic.PGX().Exec(ctx, `UPDATE `+pgschema.Table(s.schema, "flow_commands")+`
		SET state='running',attempt_ordinal=attempt_ordinal+1,updated_at=$2,status_at=$2
		WHERE command_id=$1`, candidate.CommandID, semantic.DBNow()); err != nil {
		return nil, false, MapError("mark command running", err)
	}
	var attemptTimeout time.Duration
	if timeoutMS != nil {
		attemptTimeout = time.Duration(*timeoutMS) * time.Millisecond
	}
	return &ClaimedCommand{
		CommandID: candidate.CommandID, ExecutionID: candidate.ExecutionID, CommandKey: key,
		Name: name, Version: version, Queue: queue, Args: slices.Clone(args),
		RetryMaxElapsed: clonePointer(policyValue.MaxElapsed),
		AttemptTimeout:  attemptTimeout, CreatedAt: createdAt, BudgetStartedAt: budgetStartedAt,
		ExecutionDeadline: clonePointer(executionDeadline), Attempt: ordinal + 1, ConsumedAttempts: consumed,
		AttemptID: attemptID, LeaseToken: token, DBNow: semantic.DBNow(), LeaseExpiresAt: leaseExpiresAt,
		AttemptStartedPosition: journal.Journal[0].Position,
	}, false, nil
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
	resolution := graphResolution{}
	failureEffects := failureResolution{}
	if required {
		failureEffects, err = s.resolveRequiredFailureLocked(ctx, semantic, commandID, "failed", head.FailFast)
		resolution = failureEffects.graphResolution
	} else {
		resolution, err = s.resolveGraphLocked(ctx, semantic, map[uuid.UUID]string{commandID: "failed"}, nil)
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
	skippedOffset := len(entries)
	skippedEntries, err := resolution.skippedEntries(0)
	if err != nil {
		return err
	}
	entries = append(entries, skippedEntries...)
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
	effectiveOpen := head.OpenCommands - 1 - len(resolution.skipped) - len(failureEffects.cancelled)
	terminalExecution := head.Mode == DriverDirect && effectiveOpen == 0
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
		if err := s.applyFailureResolution(ctx, semantic, failureEffects, journal, skippedOffset, cancelledOffset,
			"cancelled by fail-fast after required command failure"); err != nil {
			return err
		}
	} else if err := s.applyGraphResolution(ctx, semantic, resolution, journal, skippedOffset); err != nil {
		return err
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
		SET status=$2,open_commands=open_commands-1-$5,failure=CASE WHEN $6 THEN $3::jsonb ELSE failure END,
		    finished_at=CASE WHEN $2 IN ('failed','succeeded') THEN $4 ELSE finished_at END,
		    plan_dirty=CASE WHEN driver_mode='plan' THEN true ELSE plan_dirty END,
		    plan_dirty_since=CASE WHEN driver_mode='plan' THEN COALESCE(plan_dirty_since,$4) ELSE plan_dirty_since END,
		    plan_quiescent=CASE WHEN driver_mode='plan' THEN false ELSE plan_quiescent END,
		    updated_at=$4,status_at=CASE WHEN status<>$2 THEN $4 ELSE status_at END
		WHERE execution_id=$1`, semantic.ExecutionID(), status, jsonString(failure), semantic.DBNow(),
		len(resolution.skipped)+len(failureEffects.cancelled), executionFailed); err != nil {
		return MapError("update execution after pre-claim failure", err)
	}
	return nil
}

type LeaseRenewal struct {
	CommandID uuid.UUID
	AttemptID uuid.UUID
	Token     uuid.UUID
}

type RenewedLease struct {
	CommandID      uuid.UUID
	LeaseExpiresAt time.Time
}

func (s *Store) RenewCommandLeases(ctx context.Context, leases []LeaseRenewal, duration time.Duration) ([]RenewedLease, error) {
	if len(leases) == 0 {
		return nil, nil
	}
	if duration <= 0 {
		return nil, fmt.Errorf("%w: lease duration must be positive", flowerr.ErrInvalid)
	}
	commandIDs := make([]uuid.UUID, len(leases))
	attemptIDs := make([]uuid.UUID, len(leases))
	tokens := make([]uuid.UUID, len(leases))
	for index, lease := range leases {
		commandIDs[index], attemptIDs[index], tokens[index] = lease.CommandID, lease.AttemptID, lease.Token
	}
	rows, err := s.db.Conn.Query(ctx, `WITH now_value AS (SELECT clock_timestamp() AS now), requested(command_id,attempt_id,token) AS (
		SELECT * FROM unnest($1::uuid[],$2::uuid[],$3::uuid[])
	), renewed AS (
		UPDATE `+pgschema.Table(s.schema, "flow_command_queue")+` q
		SET lease_expires_at=n.now+($4 * interval '1 millisecond'),updated_at=n.now
		FROM requested r,now_value n
		WHERE q.command_id=r.command_id AND q.active_attempt_id=r.attempt_id AND q.lease_token=r.token
		  AND q.state='running' AND q.lease_expires_at>n.now
		RETURNING q.command_id,q.lease_expires_at
	)
	SELECT command_id,lease_expires_at FROM renewed`, commandIDs, attemptIDs, tokens, duration.Milliseconds())
	if err != nil {
		return nil, MapError("renew command leases", err)
	}
	defer rows.Close()
	result := make([]RenewedLease, 0, len(leases))
	for rows.Next() {
		var renewed RenewedLease
		if err := rows.Scan(&renewed.CommandID, &renewed.LeaseExpiresAt); err != nil {
			return nil, MapError("scan renewed command lease", err)
		}
		result = append(result, renewed)
	}
	if err := rows.Err(); err != nil {
		return nil, MapError("read renewed command leases", err)
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

type CommandConclusion struct {
	Claim          ClaimedCommand
	Classification retrypolicy.ErrorClass
	ExplicitDelay  *time.Duration
	ErrorCode      string
	ErrorMessage   string
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
	FailureScope           bool
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
	for index := range request.Children {
		// A child created by a survivor belongs to the same failure-handling
		// closure. This is accepted in the settle transaction, never inferred
		// later from application payloads.
		request.Children[index].FailureScope = fence.FailureScope
	}
	request.Events, err = s.coalesceApplicationEvents(ctx, semantic, request.Events)
	if err != nil {
		return SettleResult{}, err
	}
	if err := s.validateSuccessfulDecision(ctx, semantic, fence, request); err != nil {
		return SettleResult{}, err
	}
	firstPosition, err := semantic.nextJournalPosition(ctx)
	if err != nil {
		return SettleResult{}, err
	}
	var waitUpdates []graphWaitUpdate
	for index, event := range request.Events {
		waits, matchErr := s.matchingWaitsLocked(ctx, semantic, event.Name, event.Key, firstPosition+1+int64(index))
		if matchErr != nil {
			return SettleResult{}, matchErr
		}
		waitUpdates = append(waitUpdates, waits...)
	}
	resolution, err := s.resolveGraphLocked(ctx, semantic, map[uuid.UUID]string{request.Claim.CommandID: "succeeded"}, waitUpdates)
	if err != nil {
		return SettleResult{}, err
	}

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
	for _, child := range request.Children {
		next := semantic.DBNow().Add(child.InitialDelay)
		created, createErr := commandCreatedEntry(child, next, next)
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
	skippedOffset := len(entries)
	skippedEntries, err := resolution.skippedEntries(parentTerminalIndex)
	if err != nil {
		return SettleResult{}, err
	}
	entries = append(entries, skippedEntries...)
	effectiveOpen := fence.Head.OpenCommands - 1 + len(request.Children) - len(resolution.skipped)
	terminalExecution := fence.Head.Mode == DriverDirect && effectiveOpen == 0
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
	if err := hook.Hit(ctx, fault.SettleAfterAttempt); err != nil {
		return SettleResult{}, err
	}
	if _, err := semantic.PGX().Exec(ctx, `UPDATE `+pgschema.Table(s.schema, "flow_commands")+`
		SET state='succeeded',result=$2,result_hash=$3,last_error=NULL,terminal_failure=NULL,
		    terminal_position=$4,child_membership_closed=true,finished_at=$5,updated_at=$5,status_at=$5
		WHERE command_id=$1`, request.Claim.CommandID, request.Result.Bytes, request.Result.Digest[:],
		journal.Journal[parentTerminalIndex].Position, semantic.DBNow()); err != nil {
		return SettleResult{}, MapError("settle successful command", err)
	}
	if _, err := semantic.PGX().Exec(ctx, `DELETE FROM `+pgschema.Table(s.schema, "flow_command_queue")+` WHERE command_id=$1`, request.Claim.CommandID); err != nil {
		return SettleResult{}, MapError("remove successful command queue row", err)
	}
	for index, child := range request.Children {
		next := semantic.DBNow().Add(child.InitialDelay)
		if err := s.insertCommand(ctx, semantic.PGX(), semantic.ExecutionID(), child,
			journal.Journal[1+len(request.Events)+index].Position, next, next); err != nil {
			return SettleResult{}, err
		}
	}
	if err := hook.Hit(ctx, fault.SettleAfterChildren); err != nil {
		return SettleResult{}, err
	}
	if err := s.applyGraphResolution(ctx, semantic, resolution, journal, skippedOffset); err != nil {
		return SettleResult{}, err
	}
	if terminalExecution {
		if _, err := semantic.PGX().Exec(ctx, `UPDATE `+pgschema.Table(s.schema, "flow_executions")+`
			SET status=$3,open_commands=0,finished_at=$2,updated_at=$2,status_at=$2
			WHERE execution_id=$1`, semantic.ExecutionID(), semantic.DBNow(), terminalStatus); err != nil {
			return SettleResult{}, MapError("complete direct execution", err)
		}
	} else {
		if _, err := semantic.PGX().Exec(ctx, `UPDATE `+pgschema.Table(s.schema, "flow_executions")+`
			SET open_commands=open_commands-1+$3-$4,command_count=command_count+$3,
			    plan_dirty=CASE WHEN driver_mode='plan' THEN true ELSE plan_dirty END,
			    plan_dirty_since=CASE WHEN driver_mode='plan' THEN COALESCE(plan_dirty_since,$2) ELSE plan_dirty_since END,
			    plan_quiescent=CASE WHEN driver_mode='plan' THEN false ELSE plan_quiescent END,
			    updated_at=$2 WHERE execution_id=$1`, semantic.ExecutionID(), semantic.DBNow(), len(request.Children), len(resolution.skipped)); err != nil {
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

func (s *Store) validateSuccessfulDecision(ctx context.Context, semantic *SemanticTx, fence commandFence, request CommandSuccess) error {
	if len(request.Children) > 0 {
		if fence.Head.MaxCommands > 0 && fence.Head.CommandCount+len(request.Children) > fence.Head.MaxCommands {
			return fmt.Errorf("%w: execution command ceiling exceeded", flowerr.ErrInvalidState)
		}
		keys := make([]string, len(request.Children))
		for index, child := range request.Children {
			if child.ParentCommandID == nil || *child.ParentCommandID != request.Claim.CommandID || child.Origin != "worker_child" {
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
	if decision.Retry && decision.NextAttemptAt.IsZero() {
		decision.NextAttemptAt = semantic.DBNow()
	}
	failure := safeAttemptError(request.ErrorCode, request.ErrorMessage)
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
	resolution := graphResolution{}
	failureEffects := failureResolution{}
	skippedOffset := 0
	cancelledOffset := 0
	terminalExecution := false
	executionFailed := false
	if !decision.Retry {
		if fence.Required {
			failureEffects, err = s.resolveRequiredFailureLocked(ctx, semantic, request.Claim.CommandID, "failed", fence.Head.FailFast)
			resolution = failureEffects.graphResolution
		} else {
			resolution, err = s.resolveGraphLocked(ctx, semantic, map[uuid.UUID]string{request.Claim.CommandID: "failed"}, nil)
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
		skippedOffset = len(entries)
		skippedEntries, err := resolution.skippedEntries(failedIndex)
		if err != nil {
			return SettleResult{}, err
		}
		entries = append(entries, skippedEntries...)
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
		effectiveOpen := fence.Head.OpenCommands - 1 - len(resolution.skipped) - len(failureEffects.cancelled)
		terminalExecution = fence.Head.Mode == DriverDirect && effectiveOpen == 0
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
			    lease_started_at=NULL,lease_expires_at=NULL,updated_at=$3 WHERE command_id=$1`,
			request.Claim.CommandID, decision.NextAttemptAt, semantic.DBNow()); err != nil {
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
			if err := s.applyFailureResolution(ctx, semantic, failureEffects, journal, skippedOffset, cancelledOffset,
				"cancelled by fail-fast after required command failure"); err != nil {
				return SettleResult{}, err
			}
		} else if err := s.applyGraphResolution(ctx, semantic, resolution, journal, skippedOffset); err != nil {
			return SettleResult{}, err
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
			SET status=$2,open_commands=open_commands-1-$5,
			    failure=CASE WHEN $6 THEN $3::jsonb ELSE failure END,
			    finished_at=CASE WHEN $2 IN ('failed','succeeded') THEN $4 ELSE finished_at END,
			    plan_dirty=CASE WHEN driver_mode='plan' THEN true ELSE plan_dirty END,
			    plan_dirty_since=CASE WHEN driver_mode='plan' THEN COALESCE(plan_dirty_since,$4) ELSE plan_dirty_since END,
			    plan_quiescent=CASE WHEN driver_mode='plan' THEN false ELSE plan_quiescent END,
			    updated_at=$4,status_at=CASE WHEN status<>$2 THEN $4 ELSE status_at END
			WHERE execution_id=$1`, semantic.ExecutionID(), status, jsonString(failure), semantic.DBNow(),
			len(resolution.skipped)+len(failureEffects.cancelled), executionFailed); err != nil {
			return SettleResult{}, MapError("update execution after command failure", err)
		}
	}
	if err := hook.Hit(ctx, fault.SettleAfterEvents); err != nil {
		return SettleResult{}, err
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
	if err := s.lockCoordinatorForExecution(ctx, semantic); err != nil {
		return commandFence{}, err
	}
	var result commandFence
	result.Head = head
	if err := semantic.PGX().QueryRow(ctx, `SELECT deadline_at FROM `+pgschema.Table(s.schema, "flow_executions")+`
		WHERE execution_id=$1`, semantic.ExecutionID()).Scan(&result.ExecutionDeadline); err != nil {
		return commandFence{}, MapError("load execution deadline", err)
	}
	var activeAttempt, token *uuid.UUID
	err = semantic.PGX().QueryRow(ctx, `SELECT c.command_key,c.name,c.version,c.state,c.failure_scope,c.required,c.attempt_ordinal,
		c.consumed_attempts,c.budget_started_at,c.retry_policy::text::bytea,
		q.state,q.active_attempt_id,q.lease_token,q.lease_expires_at,
		(SELECT position FROM `+pgschema.Table(s.schema, "flow_journal")+`
		 WHERE execution_id=c.execution_id AND attempt_id=$2 AND entry_kind='attempt_started')
		FROM `+pgschema.Table(s.schema, "flow_commands")+` c
		JOIN `+pgschema.Table(s.schema, "flow_command_queue")+` q ON q.command_id=c.command_id
		WHERE c.command_id=$1 AND c.execution_id=$3
		FOR UPDATE OF c,q`, claim.CommandID, claim.AttemptID, semantic.ExecutionID()).
		Scan(&result.Key, &result.Name, &result.Version, &result.State, &result.FailureScope, &result.Required,
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
	if err := s.lockCoordinatorForExecution(ctx, semantic); err != nil {
		return err
	}
	rows, err := semantic.PGX().Query(ctx, `SELECT command_id,command_key,state,attempt_ordinal,consumed_attempts,created_position
		FROM `+pgschema.Table(s.schema, "flow_commands")+`
		WHERE execution_id=$1 AND state NOT IN ('succeeded','failed','cancelled','expired','skipped')
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
	for _, command := range commands {
		position := journal.Journal[terminalBatchIndex[command.ID]].Position
		if _, err := semantic.PGX().Exec(ctx, `UPDATE `+pgschema.Table(s.schema, "flow_commands")+`
			SET state='cancelled',last_error=$2::jsonb,terminal_failure=$2::jsonb,terminal_position=$3,
			    finished_at=$4,updated_at=$4,status_at=$4 WHERE command_id=$1`, command.ID,
			jsonString(failure), position, semantic.DBNow()); err != nil {
			return MapError("expire command", err)
		}
	}
	if _, err := semantic.PGX().Exec(ctx, `DELETE FROM `+pgschema.Table(s.schema, "flow_command_queue")+` WHERE execution_id=$1`, semantic.ExecutionID()); err != nil {
		return MapError("remove expired execution deliveries", err)
	}
	if _, err := semantic.PGX().Exec(ctx, `UPDATE `+pgschema.Table(s.schema, "flow_coordinators")+`
		SET status='cancelled',start_pending=false,delivery_state='idle',delivery_key=NULL,delivery_position=NULL,
		    active_attempt_id=NULL,lease_token=NULL,lease_owner=NULL,lease_started_at=NULL,lease_expires_at=NULL,
		    finished_at=$2,updated_at=$2 WHERE execution_id=$1 AND status='active'`, semantic.ExecutionID(), semantic.DBNow()); err != nil {
		return MapError("expire coordinator", err)
	}
	if _, err := semantic.PGX().Exec(ctx, `UPDATE `+pgschema.Table(s.schema, "flow_executions")+`
		SET status='expired',open_commands=0,failure=$2::jsonb,finished_at=$3,updated_at=$3,status_at=$3,
		    plan_dirty=false,plan_dirty_since=NULL WHERE execution_id=$1`, semantic.ExecutionID(),
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
	if err := s.lockCoordinatorForExecution(ctx, semantic); err != nil {
		return false, err
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
		    lease_started_at=NULL,lease_expires_at=NULL,updated_at=$2 WHERE command_id=$1`,
		candidate.CommandID, semantic.DBNow()); err != nil {
		return false, MapError("recover expired command delivery", err)
	}
	if err := semantic.Commit(ctx); err != nil {
		return false, err
	}
	return true, nil
}
