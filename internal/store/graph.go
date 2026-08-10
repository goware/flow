package store

import (
	"context"
	"encoding/json"
	"errors"
	"fmt"
	"sort"
	"time"

	"github.com/google/uuid"
	"github.com/goware/flow/internal/durable"
	"github.com/goware/flow/internal/flowerr"
	"github.com/goware/flow/internal/pgschema"
	"github.com/jackc/pgx/v5"
)

type acceptedEventPosition struct {
	name     string
	key      string
	position int64
}

type failureCommand struct {
	id    uuid.UUID
	key   string
	state string
}

// failureResolution describes the pending work cancelled by fail-fast.
// Attempts already running are deliberately left alone and may settle.
type failureResolution struct {
	survivors []failureCommand
	cancelled []failureCommand
}

func (s *Store) resolveRequiredFailureLocked(
	ctx context.Context,
	semantic *SemanticTx,
	commandID uuid.UUID,
	terminalState string,
	failFast bool,
) (failureResolution, error) {
	return s.resolveRequiredFailuresLocked(ctx, semantic, map[uuid.UUID]string{commandID: terminalState}, failFast)
}

func (s *Store) resolveRequiredFailuresLocked(
	ctx context.Context,
	semantic *SemanticTx,
	baseOverrides map[uuid.UUID]string,
	failFast bool,
) (failureResolution, error) {
	if !failFast {
		return failureResolution{}, nil
	}
	rows, err := semantic.PGX().Query(ctx, `SELECT command_id,command_key,state
		FROM `+pgschema.Table(s.schema, "flow_commands")+`
		WHERE execution_id=$1 AND state NOT IN ('succeeded','failed','cancelled','expired')
		ORDER BY command_key FOR UPDATE`, semantic.ExecutionID())
	if err != nil {
		return failureResolution{}, MapError("lock fail-fast commands", err)
	}
	defer rows.Close()
	result := failureResolution{}
	for rows.Next() {
		var command failureCommand
		if err := rows.Scan(&command.id, &command.key, &command.state); err != nil {
			return failureResolution{}, MapError("scan fail-fast command", err)
		}
		if _, failed := baseOverrides[command.id]; failed {
			continue
		}
		if command.state == "running" {
			result.survivors = append(result.survivors, command)
			continue
		}
		result.cancelled = append(result.cancelled, command)
	}
	if err := rows.Err(); err != nil {
		return failureResolution{}, MapError("read fail-fast commands", err)
	}
	return result, nil
}

type ExpiredWaitCandidate struct {
	CommandID   uuid.UUID
	ExecutionID uuid.UUID
}

func (s *Store) ProbeExpiredCommandWaits(ctx context.Context, limit int) ([]ExpiredWaitCandidate, error) {
	if limit <= 0 {
		return nil, nil
	}
	rows, err := s.db.Conn.Query(ctx, `SELECT command_id,execution_id FROM `+pgschema.Table(s.schema, "flow_commands")+`
		WHERE state='pending' AND unsatisfied_waits>0 AND wait_deadline_at IS NOT NULL
		AND wait_deadline_at<=clock_timestamp() ORDER BY wait_deadline_at,command_id LIMIT $1`, limit)
	if err != nil {
		return nil, MapError("probe expired command waits", err)
	}
	defer rows.Close()
	var result []ExpiredWaitCandidate
	for rows.Next() {
		var candidate ExpiredWaitCandidate
		if err := rows.Scan(&candidate.CommandID, &candidate.ExecutionID); err != nil {
			return nil, MapError("scan expired command wait", err)
		}
		result = append(result, candidate)
	}
	if err := rows.Err(); err != nil {
		return nil, MapError("read expired command waits", err)
	}
	return result, nil
}

func (s *Store) ExpireCommandWait(ctx context.Context, candidate ExpiredWaitCandidate) (bool, error) {
	semantic, err := s.BeginSemantic(ctx, candidate.ExecutionID, LockSkipLocked)
	if errors.Is(err, ErrLockUnavailable) {
		return false, nil
	}
	if err != nil {
		return false, err
	}
	defer semantic.Rollback(context.WithoutCancel(ctx))
	head, err := s.LoadExecutionHead(ctx, semantic)
	if err != nil {
		return false, err
	}
	if head.Status != "running" && head.Status != "failing" {
		return false, nil
	}
	var key, state string
	var required bool
	var deadline time.Time
	var createdPosition int64
	err = semantic.PGX().QueryRow(ctx, `SELECT command_key,state,required,wait_deadline_at,created_position
		FROM `+pgschema.Table(s.schema, "flow_commands")+`
		WHERE command_id=$1 AND execution_id=$2 FOR UPDATE`, candidate.CommandID, candidate.ExecutionID).
		Scan(&key, &state, &required, &deadline, &createdPosition)
	if err != nil {
		if errors.Is(err, pgx.ErrNoRows) {
			return false, nil
		}
		return false, MapError("lock expired command wait", err)
	}
	if state != "pending" || semantic.DBNow().Before(deadline) {
		return false, nil
	}

	// A fact committed on or before the persisted deadline wins, even when
	// expiry maintenance observes the row later.
	rows, err := semantic.PGX().Query(ctx, `SELECT w.event_name,w.event_key,
		(SELECT position FROM `+pgschema.Table(s.schema, "flow_journal")+` j
		 WHERE j.execution_id=w.execution_id AND j.entry_kind='event_recorded'
		 AND j.event_class='application' AND j.event_namespace='application'
		 AND j.event_name=w.event_name AND j.event_key=w.event_key AND j.recorded_at<=$3
		 ORDER BY position LIMIT 1)
		FROM `+pgschema.Table(s.schema, "flow_command_event_waits")+` w
		WHERE w.command_id=$1 AND w.execution_id=$2 AND w.satisfied_position IS NULL FOR UPDATE`,
		candidate.CommandID, candidate.ExecutionID, deadline)
	if err != nil {
		return false, MapError("lock expiring event waits", err)
	}
	type acceptedWait struct {
		name     string
		key      string
		position *int64
	}
	var accepted []acceptedWait
	missing := 0
	for rows.Next() {
		var wait acceptedWait
		if err := rows.Scan(&wait.name, &wait.key, &wait.position); err != nil {
			rows.Close()
			return false, MapError("scan expiring event wait", err)
		}
		accepted = append(accepted, wait)
		if wait.position == nil {
			missing++
		}
	}
	if err := rows.Err(); err != nil {
		rows.Close()
		return false, MapError("read expiring event waits", err)
	}
	rows.Close()
	if missing == 0 && len(accepted) > 0 {
		events := make([]acceptedEventPosition, 0, len(accepted))
		for _, wait := range accepted {
			events = append(events, acceptedEventPosition{
				name: wait.name, key: wait.key, position: *wait.position})
		}
		immediatelyRunnable, err := s.resolveEventReadinessLocked(ctx, semantic, events)
		if err != nil {
			return false, err
		}
		if immediatelyRunnable {
			if err := semantic.NotifyRunnableCommands(ctx); err != nil {
				return false, err
			}
		}
		if err := semantic.Commit(ctx); err != nil {
			return false, err
		}
		return true, nil
	}

	failureEffects := failureResolution{}
	if required {
		failureEffects, err = s.resolveRequiredFailureLocked(ctx, semantic, candidate.CommandID, "expired", head.FailFast)
	}
	if err != nil {
		return false, err
	}
	expired, err := terminalEventWithCode(candidate.CommandID, key, "expired", "wait_expired",
		"awaited event deadline expired", "flow.command_expired", "command_terminal")
	if err != nil {
		return false, err
	}
	expired.CausationPosition = clonePointer(&createdPosition)
	entries := []JournalEntry{expired}
	executionFailed := required || head.Status == "failing"
	if required && head.Status == "running" {
		survivors := make([]string, len(failureEffects.survivors))
		for index, command := range failureEffects.survivors {
			survivors[index] = command.key
		}
		failing, err := NewJournalEntry(ExecutionFailing, map[string]any{
			"v": 1, "status": "failing", "reason": "awaited event deadline expired", "command_key": key,
			"fail_fast": head.FailFast, "survivors": survivors,
		})
		if err != nil {
			return false, err
		}
		zero := 0
		failing.CausationBatchIndex = &zero
		entries = append(entries, failing)
	}
	cancelledOffset := len(entries)
	cancelledEntries, err := failureEffects.cancellationEntries(0, "cancelled by fail-fast after required command expiry")
	if err != nil {
		return false, err
	}
	entries = append(entries, cancelledEntries...)
	effectiveOpen, err := durable.AddPostgresInteger("execution open commands", head.OpenCommands,
		-1, 0, durable.PostgresIntegerMax)
	if err != nil {
		return false, err
	}
	effectiveOpen, err = durable.AddPostgresInteger("execution open commands", effectiveOpen,
		-len(failureEffects.cancelled), 0, durable.PostgresIntegerMax)
	if err != nil {
		return false, err
	}
	terminalExecution := effectiveOpen == 0
	if terminalExecution {
		status, name, reason := "succeeded", "flow.execution_succeeded", ""
		if executionFailed {
			status, name, reason = "failed", "flow.execution_failed", "awaited event deadline expired"
		}
		terminal, err := executionTerminalEvent(status, reason, name)
		if err != nil {
			return false, err
		}
		zero := 0
		terminal.CausationBatchIndex = &zero
		entries = append(entries, terminal)
	}
	journal, err := semantic.Apply(ctx, PersistedChangeSet{Journal: entries})
	if err != nil {
		return false, err
	}
	failure := terminalFailure{Code: "wait_expired", Message: "awaited event deadline expired"}
	if _, err := semantic.PGX().Exec(ctx, `UPDATE `+pgschema.Table(s.schema, "flow_commands")+`
		SET state='expired',terminal_failure=$2::jsonb,terminal_position=$3,finished_at=$4,updated_at=$4,status_at=$4
		WHERE command_id=$1`, candidate.CommandID, jsonString(failure), journal.Journal[0].Position, semantic.DBNow()); err != nil {
		return false, MapError("expire command wait", err)
	}
	if required {
		if err := s.applyFailureResolution(ctx, semantic, failureEffects, journal, cancelledOffset,
			"cancelled by fail-fast after required command expiry"); err != nil {
			return false, err
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
		SET status=$2,open_commands=$5,
		failure=CASE WHEN $6 THEN $3::jsonb ELSE failure END,
		finished_at=CASE WHEN $2 IN ('failed','succeeded') THEN $4 ELSE finished_at END,
		updated_at=$4,status_at=CASE WHEN status<>$2 THEN $4 ELSE status_at END WHERE execution_id=$1`,
		head.ID, status, jsonString(failure), semantic.DBNow(), effectiveOpen, executionFailed); err != nil {
		return false, MapError("update execution after wait expiry", err)
	}
	if err := semantic.Commit(ctx); err != nil {
		return false, err
	}
	return true, nil
}

func (resolution failureResolution) cancellationEntries(causeBatchIndex int, reason string) ([]JournalEntry, error) {
	entries := make([]JournalEntry, 0, len(resolution.cancelled))
	for _, command := range resolution.cancelled {
		entry, err := terminalEventWithCode(command.id, command.key, "cancelled", "fail_fast", reason,
			"flow.command_cancelled", "command_terminal")
		if err != nil {
			return nil, err
		}
		cause := causeBatchIndex
		entry.CausationBatchIndex = &cause
		entries = append(entries, entry)
	}
	return entries, nil
}

func (s *Store) applyFailureResolution(
	ctx context.Context,
	semantic *SemanticTx,
	resolution failureResolution,
	journal ApplyResult,
	cancelledOffset int,
	reason string,
) error {
	if len(resolution.cancelled) == 0 {
		return nil
	}
	failure := terminalFailure{Code: "fail_fast", Message: reason}
	commandIDs := make([]uuid.UUID, len(resolution.cancelled))
	positions := make([]int64, len(resolution.cancelled))
	for index, command := range resolution.cancelled {
		journalIndex := cancelledOffset + index
		if journalIndex < 0 || journalIndex >= len(journal.Journal) {
			return fmt.Errorf("%w: fail-fast journal mapping is invalid", flowerr.ErrInvalidState)
		}
		commandIDs[index] = command.id
		positions[index] = journal.Journal[journalIndex].Position
	}
	commandTag, err := semantic.PGX().Exec(ctx, `UPDATE `+pgschema.Table(s.schema, "flow_commands")+` AS c
		SET state='cancelled',last_error=$4::jsonb,terminal_failure=$4::jsonb,
		    terminal_position=cancelled.position,finished_at=$5,updated_at=$5,status_at=$5
		FROM unnest($1::uuid[],$2::bigint[]) AS cancelled(command_id,position)
		WHERE c.execution_id=$3 AND c.command_id=cancelled.command_id
		AND c.state NOT IN ('succeeded','failed','cancelled','expired')`,
		commandIDs, positions, semantic.ExecutionID(), jsonString(failure), semantic.DBNow())
	if err != nil {
		return MapError("cancel commands after fail-fast", err)
	}
	if commandTag.RowsAffected() != int64(len(commandIDs)) {
		return fmt.Errorf("%w: fail-fast cancellation set changed", flowerr.ErrInvalidState)
	}
	if _, err := semantic.PGX().Exec(ctx, `DELETE FROM `+pgschema.Table(s.schema, "flow_command_queue")+`
		WHERE execution_id=$1 AND command_id=ANY($2::uuid[])`, semantic.ExecutionID(), commandIDs); err != nil {
		return MapError("remove fail-fast cancelled commands", err)
	}
	return nil
}

func (s *Store) resolveEventReadinessLocked(
	ctx context.Context,
	semantic *SemanticTx,
	events []acceptedEventPosition,
) (bool, error) {
	if len(events) == 0 {
		return false, nil
	}
	names := make([]string, 0, len(events))
	keys := make([]string, 0, len(events))
	positions := make([]int64, 0, len(events))
	seen := make(map[string]int64, len(events))
	for _, event := range events {
		if event.name == "" || event.key == "" || event.position < 1 {
			return false, fmt.Errorf("%w: invalid accepted event position", flowerr.ErrInvalidState)
		}
		identity := event.name + "\x00" + event.key
		if prior, ok := seen[identity]; ok {
			if prior != event.position {
				return false, fmt.Errorf("%w: accepted event identity has multiple positions", flowerr.ErrInvalidState)
			}
			continue
		}
		seen[identity] = event.position
		names = append(names, event.name)
		keys = append(keys, event.key)
		positions = append(positions, event.position)
	}

	rows, err := semantic.PGX().Query(ctx, s.satisfyMatchingEventWaitsSQL(),
		semantic.ExecutionID(), names, keys, positions)
	if err != nil {
		return false, MapError("satisfy matching command event waits", err)
	}
	newlySatisfied := make(map[uuid.UUID]int)
	for rows.Next() {
		var commandID uuid.UUID
		if err := rows.Scan(&commandID); err != nil {
			rows.Close()
			return false, MapError("scan satisfied command event wait", err)
		}
		newlySatisfied[commandID]++
	}
	if err := rows.Err(); err != nil {
		rows.Close()
		return false, MapError("read satisfied command event waits", err)
	}
	rows.Close()
	if len(newlySatisfied) == 0 {
		return false, nil
	}

	commandIDs := make([]uuid.UUID, 0, len(newlySatisfied))
	for commandID := range newlySatisfied {
		commandIDs = append(commandIDs, commandID)
	}
	sort.Slice(commandIDs, func(i, j int) bool { return commandIDs[i].String() < commandIDs[j].String() })
	counts := make([]int32, len(commandIDs))
	for index, commandID := range commandIDs {
		counts[index] = int32(newlySatisfied[commandID])
	}

	rows, err = semantic.PGX().Query(ctx, `WITH satisfied(command_id,satisfied_count) AS (
		SELECT * FROM unnest($2::uuid[],$3::integer[])
	)
	UPDATE `+pgschema.Table(s.schema, "flow_commands")+` AS c
	SET unsatisfied_waits=c.unsatisfied_waits-satisfied.satisfied_count,
	    state=CASE WHEN c.state='pending' AND c.unsatisfied_waits=satisfied.satisfied_count THEN 'ready' ELSE c.state END,
	    budget_started_at=CASE
	      WHEN c.state='pending' AND c.unsatisfied_waits=satisfied.satisfied_count
	      THEN GREATEST($4::timestamptz,c.created_at+COALESCE(c.initial_delay_ms,0)*INTERVAL '1 millisecond')
	      ELSE c.budget_started_at END,
	    next_attempt_at=CASE
	      WHEN c.state='pending' AND c.unsatisfied_waits=satisfied.satisfied_count
	      THEN GREATEST($4::timestamptz,c.created_at+COALESCE(c.initial_delay_ms,0)*INTERVAL '1 millisecond')
	      ELSE c.next_attempt_at END,
	    updated_at=$4,
	    status_at=CASE WHEN c.state='pending' AND c.unsatisfied_waits=satisfied.satisfied_count THEN $4 ELSE c.status_at END
	FROM satisfied
	WHERE c.execution_id=$1 AND c.command_id=satisfied.command_id
	  AND c.unsatisfied_waits>=satisfied.satisfied_count
	RETURNING c.command_id,c.state,c.next_attempt_at`,
		semantic.ExecutionID(), commandIDs, counts, semantic.DBNow())
	if err != nil {
		return false, MapError("apply satisfied command wait counts", err)
	}
	releasedIDs := make([]uuid.UUID, 0, len(commandIDs))
	releasedNext := make([]time.Time, 0, len(commandIDs))
	updated := 0
	for rows.Next() {
		var commandID uuid.UUID
		var state string
		var nextAttemptAt *time.Time
		if err := rows.Scan(&commandID, &state, &nextAttemptAt); err != nil {
			rows.Close()
			return false, MapError("scan satisfied command wait count", err)
		}
		updated++
		if state == "ready" {
			if nextAttemptAt == nil {
				rows.Close()
				return false, fmt.Errorf("%w: released command has no next attempt", flowerr.ErrInvalidState)
			}
			releasedIDs = append(releasedIDs, commandID)
			releasedNext = append(releasedNext, *nextAttemptAt)
		}
	}
	if err := rows.Err(); err != nil {
		rows.Close()
		return false, MapError("read satisfied command wait counts", err)
	}
	rows.Close()
	if updated != len(commandIDs) {
		return false, fmt.Errorf("%w: satisfied wait count exceeds command projection", flowerr.ErrInvalidState)
	}
	if len(releasedIDs) == 0 {
		return false, nil
	}
	commandTag, err := semantic.PGX().Exec(ctx, `INSERT INTO `+pgschema.Table(s.schema, "flow_command_queue")+`
		(command_id,execution_id,queue,name,version,state,next_run_at)
		SELECT c.command_id,c.execution_id,c.queue,c.name,c.version,'ready',c.next_attempt_at
		FROM `+pgschema.Table(s.schema, "flow_commands")+` AS c
		WHERE c.execution_id=$1 AND c.command_id=ANY($2::uuid[]) AND c.state='ready' AND c.unsatisfied_waits=0
		ON CONFLICT (command_id) DO NOTHING`,
		semantic.ExecutionID(), releasedIDs)
	if err != nil {
		return false, MapError("enqueue released commands", err)
	}
	if commandTag.RowsAffected() != int64(len(releasedIDs)) {
		return false, fmt.Errorf("%w: released command queue set changed", flowerr.ErrInvalidState)
	}
	for _, nextRun := range releasedNext {
		if !nextRun.After(semantic.DBNow()) {
			return true, nil
		}
	}
	return false, nil
}

func (s *Store) satisfyMatchingEventWaitsSQL() string {
	return `WITH incoming(event_name,event_key,position) AS (
		SELECT * FROM unnest($2::text[],$3::text[],$4::bigint[])
	)
	UPDATE ` + pgschema.Table(s.schema, "flow_command_event_waits") + ` AS w
	SET satisfied_position=incoming.position
	FROM incoming, ` + pgschema.Table(s.schema, "flow_journal") + ` AS j,
	     ` + pgschema.Table(s.schema, "flow_commands") + ` AS c
	WHERE w.execution_id=$1 AND w.event_name=incoming.event_name AND w.event_key=incoming.event_key
	  AND w.satisfied_position IS NULL
	  AND j.execution_id=$1 AND j.position=incoming.position
	  AND j.entry_kind='event_recorded' AND j.event_class='application'
	  AND j.event_namespace='application' AND j.event_name=incoming.event_name AND j.event_key=incoming.event_key
	  AND c.execution_id=w.execution_id AND c.command_id=w.command_id
	  AND (c.wait_deadline_at IS NULL OR j.recorded_at<=c.wait_deadline_at)
	RETURNING w.command_id`
}

func graphNoRows(err error) bool { return errors.Is(err, pgx.ErrNoRows) }

// Keep encoding/json linked here because terminal failures are persisted as
// JSON by the wait-expiry path through jsonString.
var _ = json.Valid
