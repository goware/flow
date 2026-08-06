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

// readinessCommand is the small projection needed to release exact event
// gates. Event waits and the initial delay are independent prerequisites.
type readinessCommand struct {
	id               uuid.UUID
	key              string
	name             string
	version          int
	queue            string
	state            string
	required         bool
	createdPosition  int64
	unsatisfiedWaits int
	initialDelay     time.Duration
	createdAt        time.Time
	waitStartedAt    *time.Time
	waitTimeout      time.Duration
}

type waitUpdate struct {
	commandID uuid.UUID
	name      string
	key       string
	position  int64
}

type readinessResolution struct {
	waits   []waitUpdate
	ready   []readinessCommand
	waiting []readinessCommand
}

// failureResolution describes the pending work cancelled by fail-fast.
// Attempts already running are deliberately left alone and may settle.
type failureResolution struct {
	readinessResolution
	survivors []readinessCommand
	cancelled []readinessCommand
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
	readiness, err := s.resolveReadinessLocked(ctx, semantic, baseOverrides, nil)
	if err != nil {
		return failureResolution{}, err
	}
	result := failureResolution{readinessResolution: readiness}
	if !failFast {
		return result, nil
	}
	commands, err := s.loadReadinessCommands(ctx, semantic)
	if err != nil {
		return failureResolution{}, err
	}
	for _, command := range commands {
		if _, failed := baseOverrides[command.id]; failed || isCommandTerminal(command.state) {
			continue
		}
		if command.state == "running" {
			result.survivors = append(result.survivors, command)
			continue
		}
		result.cancelled = append(result.cancelled, command)
	}
	sort.Slice(result.survivors, func(i, j int) bool { return result.survivors[i].key < result.survivors[j].key })
	sort.Slice(result.cancelled, func(i, j int) bool { return result.cancelled[i].key < result.cancelled[j].key })
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
	if errors.Is(err, pgx.ErrNoRows) || state != "pending" || semantic.DBNow().Before(deadline) {
		return false, nil
	}
	if err != nil {
		return false, MapError("lock expired command wait", err)
	}

	// A fact committed on or before the persisted deadline wins, even when
	// expiry maintenance observes the row later.
	rows, err := semantic.PGX().Query(ctx, `SELECT w.event_name,w.event_key,
		(SELECT position FROM `+pgschema.Table(s.schema, "flow_journal")+` j
		 WHERE j.execution_id=w.execution_id AND j.event_namespace='application'
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
		updates := make([]waitUpdate, 0, len(accepted))
		for _, wait := range accepted {
			updates = append(updates, waitUpdate{commandID: candidate.CommandID,
				name: wait.name, key: wait.key, position: *wait.position})
		}
		resolution, err := s.resolveReadinessLocked(ctx, semantic, nil, updates)
		if err != nil {
			return false, err
		}
		if err := s.applyReadinessResolution(ctx, semantic, resolution); err != nil {
			return false, err
		}
		if err := semantic.Commit(ctx); err != nil {
			return false, err
		}
		return true, nil
	}

	resolution := readinessResolution{}
	failureEffects := failureResolution{}
	if required {
		failureEffects, err = s.resolveRequiredFailureLocked(ctx, semantic, candidate.CommandID, "expired", head.FailFast)
		resolution = failureEffects.readinessResolution
	} else {
		resolution, err = s.resolveReadinessLocked(ctx, semantic, map[uuid.UUID]string{candidate.CommandID: "expired"}, nil)
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
		if err := s.applyFailureResolution(ctx, semantic, failureEffects, journal, 0, cancelledOffset,
			"cancelled by fail-fast after required command expiry"); err != nil {
			return false, err
		}
	} else if err := s.applyReadinessResolution(ctx, semantic, resolution); err != nil {
		return false, err
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
	_ int,
	cancelledOffset int,
	reason string,
) error {
	if err := s.applyReadinessResolution(ctx, semantic, resolution.readinessResolution); err != nil {
		return err
	}
	failure := terminalFailure{Code: "fail_fast", Message: reason}
	for index, command := range resolution.cancelled {
		position := journal.Journal[cancelledOffset+index].Position
		if _, err := semantic.PGX().Exec(ctx, `UPDATE `+pgschema.Table(s.schema, "flow_commands")+`
			SET state='cancelled',last_error=$2::jsonb,terminal_failure=$2::jsonb,terminal_position=$3,
			    finished_at=$4,updated_at=$4,status_at=$4
			WHERE command_id=$1 AND state NOT IN ('succeeded','failed','cancelled','expired')`,
			command.id, jsonString(failure), position, semantic.DBNow()); err != nil {
			return MapError("cancel command after fail-fast", err)
		}
		if _, err := semantic.PGX().Exec(ctx, `DELETE FROM `+pgschema.Table(s.schema, "flow_command_queue")+`
			WHERE command_id=$1`, command.id); err != nil {
			return MapError("remove fail-fast cancelled command", err)
		}
	}
	return nil
}

// resolveReadinessLocked calculates exact-wait releases without mutating
// storage. Overrides describe terminal states accepted by the transaction.
func (s *Store) resolveReadinessLocked(
	ctx context.Context,
	semantic *SemanticTx,
	overrides map[uuid.UUID]string,
	waits []waitUpdate,
) (readinessResolution, error) {
	commands, err := s.loadReadinessCommands(ctx, semantic)
	if err != nil {
		return readinessResolution{}, err
	}
	byID := make(map[uuid.UUID]*readinessCommand, len(commands))
	for index := range commands {
		byID[commands[index].id] = &commands[index]
	}
	for id, state := range overrides {
		if command := byID[id]; command != nil {
			command.state = state
		}
	}
	resolution := readinessResolution{waits: waits}
	for _, wait := range waits {
		if command := byID[wait.commandID]; command != nil && command.unsatisfiedWaits > 0 {
			command.unsatisfiedWaits--
		}
	}
	for index := range commands {
		command := &commands[index]
		if command.state == "pending" && command.unsatisfiedWaits == 0 {
			resolution.ready = append(resolution.ready, *command)
		} else if command.state == "pending" && command.unsatisfiedWaits > 0 && command.waitStartedAt == nil {
			resolution.waiting = append(resolution.waiting, *command)
		}
	}
	sort.Slice(resolution.ready, func(i, j int) bool { return resolution.ready[i].key < resolution.ready[j].key })
	sort.Slice(resolution.waiting, func(i, j int) bool { return resolution.waiting[i].key < resolution.waiting[j].key })
	return resolution, nil
}

func (s *Store) loadReadinessCommands(ctx context.Context, semantic *SemanticTx) ([]readinessCommand, error) {
	rows, err := semantic.PGX().Query(ctx, `SELECT command_id,command_key,name,version,queue,state,required,created_position,
		unsatisfied_waits,COALESCE(initial_delay_ms,0),created_at,wait_started_at,COALESCE(wait_timeout_ms,0)
		FROM `+pgschema.Table(s.schema, "flow_commands")+` WHERE execution_id=$1 ORDER BY command_key`, semantic.ExecutionID())
	if err != nil {
		return nil, MapError("load readiness commands", err)
	}
	defer rows.Close()
	var result []readinessCommand
	for rows.Next() {
		var item readinessCommand
		var delayMS, waitTimeoutMS int64
		if err := rows.Scan(&item.id, &item.key, &item.name, &item.version, &item.queue, &item.state,
			&item.required, &item.createdPosition, &item.unsatisfiedWaits, &delayMS, &item.createdAt,
			&item.waitStartedAt, &waitTimeoutMS); err != nil {
			return nil, MapError("scan readiness command", err)
		}
		item.initialDelay, err = durable.MillisecondsDuration("stored command initial delay", delayMS)
		if err != nil {
			return nil, fmt.Errorf("%w: invalid command initial delay", flowerr.ErrInvalidState)
		}
		item.waitTimeout, err = durable.MillisecondsDuration("stored command wait timeout", waitTimeoutMS)
		if err != nil {
			return nil, fmt.Errorf("%w: invalid command wait timeout", flowerr.ErrInvalidState)
		}
		result = append(result, item)
	}
	if err := rows.Err(); err != nil {
		return nil, MapError("read readiness commands", err)
	}
	return result, nil
}

func (s *Store) applyReadinessResolution(
	ctx context.Context,
	semantic *SemanticTx,
	resolution readinessResolution,
) error {
	for _, wait := range resolution.waits {
		commandTag, err := semantic.PGX().Exec(ctx, `UPDATE `+pgschema.Table(s.schema, "flow_command_event_waits")+`
			SET satisfied_position=$4 WHERE command_id=$1 AND event_name=$2 AND event_key=$3
			AND satisfied_position IS NULL`, wait.commandID, wait.name, wait.key, wait.position)
		if err != nil {
			return MapError("satisfy command event wait", err)
		}
		if commandTag.RowsAffected() > 0 {
			if _, err := semantic.PGX().Exec(ctx, `UPDATE `+pgschema.Table(s.schema, "flow_commands")+`
				SET state=CASE WHEN state='pending' AND unsatisfied_waits=1 THEN 'ready' ELSE state END,
				    unsatisfied_waits=GREATEST(0,unsatisfied_waits-1),updated_at=$2,
				    status_at=CASE WHEN state='pending' AND unsatisfied_waits=1 THEN $2 ELSE status_at END
				WHERE command_id=$1`, wait.commandID, semantic.DBNow()); err != nil {
				return MapError("update satisfied wait count", err)
			}
		}
	}
	for _, command := range resolution.ready {
		nextRun := semantic.DBNow()
		if command.initialDelay > 0 {
			var err error
			nextRun, err = durable.AddExactDuration("command initial delay", command.createdAt, command.initialDelay)
			if err != nil {
				return fmt.Errorf("%w: invalid stored command initial delay", flowerr.ErrInvalidState)
			}
			if nextRun.Before(semantic.DBNow()) {
				nextRun = semantic.DBNow()
			}
		}
		commandTag, err := semantic.PGX().Exec(ctx, `UPDATE `+pgschema.Table(s.schema, "flow_commands")+`
			SET state='ready',budget_started_at=$2,next_attempt_at=$2,updated_at=$3,status_at=$3
			WHERE command_id=$1 AND state IN ('pending','ready') AND unsatisfied_waits=0
			AND budget_started_at IS NULL`, command.id, nextRun, semantic.DBNow())
		if err != nil {
			return MapError("make gated command ready", err)
		}
		if commandTag.RowsAffected() > 0 {
			_, err = semantic.PGX().Exec(ctx, `INSERT INTO `+pgschema.Table(s.schema, "flow_command_queue")+`
				(command_id,execution_id,queue,name,version,state,next_run_at)
				VALUES ($1,$2,$3,$4,$5,'ready',$6) ON CONFLICT (command_id) DO NOTHING`,
				command.id, semantic.ExecutionID(), command.queue, command.name, command.version, nextRun)
			if err != nil {
				return MapError("enqueue gated command", err)
			}
		}
	}
	for _, command := range resolution.waiting {
		var deadline *time.Time
		if command.waitTimeout > 0 {
			value, err := durable.AddExactDuration("command wait timeout", semantic.DBNow(), command.waitTimeout)
			if err != nil {
				return fmt.Errorf("%w: invalid stored command wait timeout", flowerr.ErrInvalidState)
			}
			var executionDeadline *time.Time
			if err := semantic.PGX().QueryRow(ctx, `SELECT deadline_at FROM `+pgschema.Table(s.schema, "flow_executions")+`
				WHERE execution_id=$1`, semantic.ExecutionID()).Scan(&executionDeadline); err != nil {
				return MapError("load execution deadline for wait", err)
			}
			if executionDeadline != nil && executionDeadline.Before(value) {
				value = *executionDeadline
			}
			deadline = &value
		}
		if _, err := semantic.PGX().Exec(ctx, `UPDATE `+pgschema.Table(s.schema, "flow_commands")+`
			SET wait_started_at=COALESCE(wait_started_at,$2),wait_deadline_at=COALESCE(wait_deadline_at,$3),updated_at=$2
			WHERE command_id=$1 AND state='pending' AND unsatisfied_waits>0`,
			command.id, semantic.DBNow(), deadline); err != nil {
			return MapError("start command event wait", err)
		}
	}
	return nil
}

func (s *Store) matchingWaitsLocked(
	ctx context.Context,
	semantic *SemanticTx,
	name, key string,
	position int64,
) ([]waitUpdate, error) {
	rows, err := semantic.PGX().Query(ctx, `SELECT w.command_id,w.event_name,w.event_key
		FROM `+pgschema.Table(s.schema, "flow_command_event_waits")+` w
		JOIN `+pgschema.Table(s.schema, "flow_commands")+` c USING (command_id)
		WHERE w.execution_id=$1 AND w.event_name=$2 AND w.event_key=$3
		AND w.satisfied_position IS NULL AND (c.wait_deadline_at IS NULL OR $4<=c.wait_deadline_at)
		ORDER BY w.command_id FOR UPDATE OF w`, semantic.ExecutionID(), name, key, semantic.DBNow())
	if err != nil {
		return nil, MapError("lock matching command waits", err)
	}
	defer rows.Close()
	var waits []waitUpdate
	for rows.Next() {
		var wait waitUpdate
		if err := rows.Scan(&wait.commandID, &wait.name, &wait.key); err != nil {
			return nil, MapError("scan matching command wait", err)
		}
		wait.position = position
		waits = append(waits, wait)
	}
	if err := rows.Err(); err != nil {
		return nil, MapError("read matching command waits", err)
	}
	return waits, nil
}

func graphNoRows(err error) bool { return errors.Is(err, pgx.ErrNoRows) }

// Keep encoding/json linked here because terminal failures are persisted as
// JSON by the wait-expiry path through jsonString.
var _ = json.Valid
