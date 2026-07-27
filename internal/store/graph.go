package store

import (
	"context"
	"errors"
	"fmt"
	"sort"
	"time"

	"github.com/google/uuid"
	"github.com/goware/flow/internal/flowerr"
	"github.com/goware/flow/internal/pgschema"
	"github.com/jackc/pgx/v5"
)

type graphGroup struct {
	id           uuid.UUID
	dependent    uuid.UUID
	dependentKey string
	ordinal      int
	kind         string
	threshold    *int
	members      []graphMember
}

type graphMember struct {
	id    uuid.UUID
	state string
}

type graphCommand struct {
	id                uuid.UUID
	key               string
	name              string
	version           int
	queue             string
	parent            *uuid.UUID
	state             string
	required          bool
	failureScope      bool
	createdPosition   int64
	unsatisfiedGroups int
	unsatisfiedWaits  int
	scheduleKind      string
	initialDelay      time.Duration
	createdAt         time.Time
	waitStartedAt     *time.Time
	waitTimeout       time.Duration
}

type graphGroupUpdate struct {
	id    uuid.UUID
	state string
}

type graphWaitUpdate struct {
	commandID uuid.UUID
	namespace string
	name      string
	version   int
	position  int64
}

type graphResolution struct {
	groups  []graphGroupUpdate
	waits   []graphWaitUpdate
	ready   []graphCommand
	waiting []graphCommand
	skipped []graphCommand
}

// failureResolution is the complete graph effect of one unsuccessful required
// command. In fail-fast mode it also records the durable survivor closure and
// every queued/pending command that must be cancelled. Running commands are
// always survivors; fencing lets them finish without being torn down midway.
type failureResolution struct {
	graphResolution
	survivors []graphCommand
	cancelled []graphCommand
}

func (s *Store) resolveRequiredFailureLocked(
	ctx context.Context,
	semantic *SemanticTx,
	commandID uuid.UUID,
	terminalState string,
	failFast bool,
) (failureResolution, error) {
	baseOverrides := map[uuid.UUID]string{commandID: terminalState}
	resolution, err := s.resolveGraphLocked(ctx, semantic, baseOverrides, nil)
	if err != nil {
		return failureResolution{}, err
	}
	if !failFast {
		return failureResolution{graphResolution: resolution}, nil
	}

	commands, err := s.loadGraphCommands(ctx, semantic)
	if err != nil {
		return failureResolution{}, err
	}
	adjacency, err := s.loadGraphAdjacency(ctx, semantic)
	if err != nil {
		return failureResolution{}, err
	}
	byID := make(map[uuid.UUID]graphCommand, len(commands))
	scope := make(map[uuid.UUID]bool, len(commands))
	for _, command := range commands {
		byID[command.id] = command
		if command.failureScope || command.state == "running" {
			scope[command.id] = true
		}
	}
	// Any work directly made viable by the failure is part of failure
	// handling. Closing over dependency and parent/child edges retains all of
	// its descendants without application-authored scope bookkeeping.
	for _, command := range append(append([]graphCommand(nil), resolution.ready...), resolution.waiting...) {
		scope[command.id] = true
	}
	for _, dependent := range adjacency[commandID] {
		scope[dependent] = true
	}
	closeFailureScope(scope, adjacency)

	for {
		cancelled := failureCancellations(commands, commandID, scope)
		overrides := map[uuid.UUID]string{commandID: terminalState}
		for _, command := range cancelled {
			overrides[command.id] = "cancelled"
		}
		resolution, err = s.resolveGraphLocked(ctx, semantic, overrides, nil)
		if err != nil {
			return failureResolution{}, err
		}
		before := len(scope)
		for _, command := range append(append([]graphCommand(nil), resolution.ready...), resolution.waiting...) {
			scope[command.id] = true
		}
		closeFailureScope(scope, adjacency)
		if len(scope) == before {
			survivors := make([]graphCommand, 0, len(scope))
			for id := range scope {
				if command, ok := byID[id]; ok && !isCommandTerminal(command.state) && id != commandID {
					survivors = append(survivors, command)
				}
			}
			sort.Slice(survivors, func(i, j int) bool { return survivors[i].key < survivors[j].key })
			return failureResolution{graphResolution: resolution, survivors: survivors, cancelled: cancelled}, nil
		}
	}
}

func failureCancellations(commands []graphCommand, failedID uuid.UUID, scope map[uuid.UUID]bool) []graphCommand {
	result := make([]graphCommand, 0, len(commands))
	for _, command := range commands {
		if command.id == failedID || isCommandTerminal(command.state) || scope[command.id] || command.state == "running" {
			continue
		}
		result = append(result, command)
	}
	sort.Slice(result, func(i, j int) bool { return result[i].key < result[j].key })
	return result
}

func closeFailureScope(scope map[uuid.UUID]bool, adjacency map[uuid.UUID][]uuid.UUID) {
	queue := make([]uuid.UUID, 0, len(scope))
	for id := range scope {
		queue = append(queue, id)
	}
	for len(queue) > 0 {
		id := queue[0]
		queue = queue[1:]
		for _, dependent := range adjacency[id] {
			if scope[dependent] {
				continue
			}
			scope[dependent] = true
			queue = append(queue, dependent)
		}
	}
}

func (s *Store) loadGraphAdjacency(ctx context.Context, semantic *SemanticTx) (map[uuid.UUID][]uuid.UUID, error) {
	rows, err := semantic.PGX().Query(ctx, `SELECT m.predecessor_command_id,g.dependent_command_id
		FROM `+pgschema.Table(s.schema, "flow_command_dependency_members")+` m
		JOIN `+pgschema.Table(s.schema, "flow_command_dependency_groups")+` g USING (group_id)
		WHERE g.execution_id=$1
		UNION ALL
		SELECT parent_command_id,command_id FROM `+pgschema.Table(s.schema, "flow_commands")+`
		WHERE execution_id=$1 AND parent_command_id IS NOT NULL`, semantic.ExecutionID())
	if err != nil {
		return nil, MapError("load graph adjacency", err)
	}
	defer rows.Close()
	result := make(map[uuid.UUID][]uuid.UUID)
	for rows.Next() {
		var from, to uuid.UUID
		if err := rows.Scan(&from, &to); err != nil {
			return nil, MapError("scan graph adjacency", err)
		}
		result[from] = append(result[from], to)
	}
	if err := rows.Err(); err != nil {
		return nil, MapError("read graph adjacency", err)
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
	if errors.Is(err, pgx.ErrNoRows) || state != "pending" || semantic.DBNow().Before(deadline) {
		return false, nil
	}
	if err != nil {
		return false, MapError("lock expired command wait", err)
	}

	// A fact accepted on or before the persisted deadline wins even if this
	// maintenance transaction runs later.
	rows, err := semantic.PGX().Query(ctx, `SELECT w.event_namespace,w.event_name,w.event_version,
		(SELECT position FROM `+pgschema.Table(s.schema, "flow_journal")+` j
		 WHERE j.execution_id=w.execution_id AND j.event_namespace=w.event_namespace
		 AND j.event_name=w.event_name AND j.event_version=w.event_version AND j.recorded_at<=$3
		 ORDER BY position LIMIT 1)
		FROM `+pgschema.Table(s.schema, "flow_command_event_waits")+` w
		WHERE w.command_id=$1 AND w.execution_id=$2 AND w.satisfied_position IS NULL FOR UPDATE`,
		candidate.CommandID, candidate.ExecutionID, deadline)
	if err != nil {
		return false, MapError("lock expiring event waits", err)
	}
	type acceptedWait struct {
		namespace string
		name      string
		version   int
		position  *int64
	}
	var accepted []acceptedWait
	missing := 0
	for rows.Next() {
		var wait acceptedWait
		if err := rows.Scan(&wait.namespace, &wait.name, &wait.version, &wait.position); err != nil {
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
		waitUpdates := make([]graphWaitUpdate, 0, len(accepted))
		for _, wait := range accepted {
			waitUpdates = append(waitUpdates, graphWaitUpdate{commandID: candidate.CommandID, namespace: wait.namespace,
				name: wait.name, version: wait.version, position: *wait.position})
		}
		resolution, err := s.resolveGraphLocked(ctx, semantic, nil, waitUpdates)
		if err != nil {
			return false, err
		}
		if err := s.applyGraphResolution(ctx, semantic, resolution, ApplyResult{}, 0); err != nil {
			return false, err
		}
		if err := semantic.Commit(ctx); err != nil {
			return false, err
		}
		return true, nil
	}

	resolution := graphResolution{}
	failureEffects := failureResolution{}
	if required {
		failureEffects, err = s.resolveRequiredFailureLocked(ctx, semantic, candidate.CommandID, "expired", head.FailFast)
		resolution = failureEffects.graphResolution
	} else {
		resolution, err = s.resolveGraphLocked(ctx, semantic, map[uuid.UUID]string{candidate.CommandID: "expired"}, nil)
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
	skippedOffset := len(entries)
	skipped, err := resolution.skippedEntries(0)
	if err != nil {
		return false, err
	}
	entries = append(entries, skipped...)
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
	effectiveOpen := head.OpenCommands - 1 - len(resolution.skipped) - len(failureEffects.cancelled)
	terminalExecution := head.Mode == DriverDirect && effectiveOpen == 0
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
		if err := s.applyFailureResolution(ctx, semantic, failureEffects, journal, skippedOffset, cancelledOffset,
			"cancelled by fail-fast after required command expiry"); err != nil {
			return false, err
		}
	} else if err := s.applyGraphResolution(ctx, semantic, resolution, journal, skippedOffset); err != nil {
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
		SET status=$2,open_commands=open_commands-1-$5,
		failure=CASE WHEN $6 THEN $3::jsonb ELSE failure END,
		finished_at=CASE WHEN $2 IN ('failed','succeeded') THEN $4 ELSE finished_at END,
		plan_dirty=CASE WHEN driver_mode='plan' THEN true ELSE plan_dirty END,
		plan_dirty_since=CASE WHEN driver_mode='plan' THEN COALESCE(plan_dirty_since,$4) ELSE plan_dirty_since END,
		plan_quiescent=CASE WHEN driver_mode='plan' THEN false ELSE plan_quiescent END,
		updated_at=$4,status_at=CASE WHEN status<>$2 THEN $4 ELSE status_at END WHERE execution_id=$1`,
		head.ID, status, jsonString(failure), semantic.DBNow(), len(resolution.skipped)+len(failureEffects.cancelled), executionFailed); err != nil {
		return false, MapError("update execution after wait expiry", err)
	}
	if err := semantic.Commit(ctx); err != nil {
		return false, err
	}
	return true, nil
}

func (resolution graphResolution) skippedEntries(causeBatchIndex int) ([]JournalEntry, error) {
	entries := make([]JournalEntry, 0, len(resolution.skipped))
	for _, command := range resolution.skipped {
		entry, err := terminalEventWithCode(command.id, command.key, "skipped", "dependency_unsatisfiable",
			"a command dependency became unsatisfiable", "flow.command_skipped", "command_terminal")
		if err != nil {
			return nil, err
		}
		cause := causeBatchIndex
		entry.CausationBatchIndex = &cause
		entries = append(entries, entry)
	}
	return entries, nil
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
	skippedOffset int,
	cancelledOffset int,
	reason string,
) error {
	if err := s.applyGraphResolution(ctx, semantic, resolution.graphResolution, journal, skippedOffset); err != nil {
		return err
	}
	if len(resolution.survivors) > 0 {
		ids := make([]uuid.UUID, len(resolution.survivors))
		for index, command := range resolution.survivors {
			ids[index] = command.id
		}
		if _, err := semantic.PGX().Exec(ctx, `UPDATE `+pgschema.Table(s.schema, "flow_commands")+`
			SET failure_scope=true,updated_at=$3 WHERE execution_id=$1 AND command_id=ANY($2)`,
			semantic.ExecutionID(), ids, semantic.DBNow()); err != nil {
			return MapError("mark failure survivor scope", err)
		}
	}
	failure := terminalFailure{Code: "fail_fast", Message: reason}
	for index, command := range resolution.cancelled {
		position := journal.Journal[cancelledOffset+index].Position
		if _, err := semantic.PGX().Exec(ctx, `UPDATE `+pgschema.Table(s.schema, "flow_commands")+`
			SET state='cancelled',last_error=$2::jsonb,terminal_failure=$2::jsonb,terminal_position=$3,
			    finished_at=$4,updated_at=$4,status_at=$4
			WHERE command_id=$1 AND state NOT IN ('succeeded','failed','cancelled','expired','skipped')`,
			command.id, jsonString(failure), position, semantic.DBNow()); err != nil {
			return MapError("cancel command outside failure scope", err)
		}
		if _, err := semantic.PGX().Exec(ctx, `DELETE FROM `+pgschema.Table(s.schema, "flow_command_queue")+`
			WHERE command_id=$1`, command.id); err != nil {
			return MapError("remove fail-fast cancelled command", err)
		}
	}
	return nil
}

// resolveGraphLocked calculates the full terminal dependency cascade without
// mutating storage. Overrides represent terminal states being accepted by the
// current transaction but not materialized yet. The result can therefore be
// journaled in one ordered batch before all projections are updated.
func (s *Store) resolveGraphLocked(
	ctx context.Context,
	semantic *SemanticTx,
	overrides map[uuid.UUID]string,
	waits []graphWaitUpdate,
) (graphResolution, error) {
	commands, err := s.loadGraphCommands(ctx, semantic)
	if err != nil {
		return graphResolution{}, err
	}
	groups, err := s.loadUnresolvedGraphGroups(ctx, semantic)
	if err != nil {
		return graphResolution{}, err
	}
	states := make(map[uuid.UUID]string, len(commands)+len(overrides))
	byID := make(map[uuid.UUID]*graphCommand, len(commands))
	for index := range commands {
		states[commands[index].id] = commands[index].state
		byID[commands[index].id] = &commands[index]
	}
	for id, state := range overrides {
		states[id] = state
		if command := byID[id]; command != nil {
			command.state = state
		}
	}
	resolution := graphResolution{waits: waits}
	for _, wait := range waits {
		if command := byID[wait.commandID]; command != nil && command.unsatisfiedWaits > 0 {
			command.unsatisfiedWaits--
		}
	}
	remaining := append([]graphGroup(nil), groups...)
	for {
		progressed := false
		next := remaining[:0]
		for _, group := range remaining {
			state := evaluateDependencyGroup(group, states)
			if state == "unresolved" {
				next = append(next, group)
				continue
			}
			progressed = true
			resolution.groups = append(resolution.groups, graphGroupUpdate{id: group.id, state: state})
			dependent := byID[group.dependent]
			if dependent == nil {
				return graphResolution{}, fmt.Errorf("%w: dependency group has no command", flowerr.ErrInvalidState)
			}
			if dependent.unsatisfiedGroups > 0 {
				dependent.unsatisfiedGroups--
			}
			if state == "unsatisfiable" && !isCommandTerminal(states[dependent.id]) {
				states[dependent.id] = "skipped"
				dependent.state = "skipped"
				resolution.skipped = append(resolution.skipped, *dependent)
			}
		}
		remaining = next
		if !progressed {
			break
		}
	}
	for index := range commands {
		command := &commands[index]
		if command.state == "pending" && command.unsatisfiedGroups == 0 && command.unsatisfiedWaits == 0 {
			resolution.ready = append(resolution.ready, *command)
		} else if command.state == "pending" && command.unsatisfiedGroups == 0 && command.unsatisfiedWaits > 0 && command.waitStartedAt == nil {
			resolution.waiting = append(resolution.waiting, *command)
		}
	}
	sort.Slice(resolution.groups, func(i, j int) bool { return resolution.groups[i].id.String() < resolution.groups[j].id.String() })
	sort.Slice(resolution.ready, func(i, j int) bool { return resolution.ready[i].key < resolution.ready[j].key })
	sort.Slice(resolution.waiting, func(i, j int) bool { return resolution.waiting[i].key < resolution.waiting[j].key })
	sort.Slice(resolution.skipped, func(i, j int) bool { return resolution.skipped[i].key < resolution.skipped[j].key })
	return resolution, nil
}

func evaluateDependencyGroup(group graphGroup, states map[uuid.UUID]string) string {
	succeeded, unsuccessful, terminal := 0, 0, 0
	for _, member := range group.members {
		state := states[member.id]
		if state == "succeeded" {
			succeeded++
			terminal++
		} else if isCommandTerminal(state) {
			unsuccessful++
			terminal++
		}
	}
	switch group.kind {
	case "all_succeeded":
		if unsuccessful > 0 {
			return "unsatisfiable"
		}
		if succeeded == len(group.members) {
			return "satisfied"
		}
	case "all_settled":
		if terminal == len(group.members) {
			return "satisfied"
		}
	case "all_failed":
		if succeeded > 0 {
			return "unsatisfiable"
		}
		if unsuccessful == len(group.members) {
			return "satisfied"
		}
	case "at_least":
		if group.threshold == nil {
			return "unsatisfiable"
		}
		if succeeded >= *group.threshold {
			return "satisfied"
		}
		if succeeded+(len(group.members)-terminal) < *group.threshold {
			return "unsatisfiable"
		}
	}
	return "unresolved"
}

func (s *Store) loadGraphCommands(ctx context.Context, semantic *SemanticTx) ([]graphCommand, error) {
	rows, err := semantic.PGX().Query(ctx, `SELECT command_id,command_key,name,version,queue,parent_command_id,state,
		required,failure_scope,created_position,
		unsatisfied_groups,unsatisfied_waits,schedule_kind,COALESCE(initial_delay_ms,0),created_at,
		wait_started_at,COALESCE(wait_timeout_ms,0)
		FROM `+pgschema.Table(s.schema, "flow_commands")+` WHERE execution_id=$1 ORDER BY command_key`, semantic.ExecutionID())
	if err != nil {
		return nil, MapError("load graph commands", err)
	}
	defer rows.Close()
	var result []graphCommand
	for rows.Next() {
		var item graphCommand
		var delayMS, waitTimeoutMS int64
		if err := rows.Scan(&item.id, &item.key, &item.name, &item.version, &item.queue, &item.parent, &item.state,
			&item.required, &item.failureScope, &item.createdPosition,
			&item.unsatisfiedGroups, &item.unsatisfiedWaits, &item.scheduleKind, &delayMS, &item.createdAt,
			&item.waitStartedAt, &waitTimeoutMS); err != nil {
			return nil, MapError("scan graph command", err)
		}
		item.initialDelay = time.Duration(delayMS) * time.Millisecond
		item.waitTimeout = time.Duration(waitTimeoutMS) * time.Millisecond
		result = append(result, item)
	}
	if err := rows.Err(); err != nil {
		return nil, MapError("read graph commands", err)
	}
	return result, nil
}

func (s *Store) loadUnresolvedGraphGroups(ctx context.Context, semantic *SemanticTx) ([]graphGroup, error) {
	rows, err := semantic.PGX().Query(ctx, `SELECT g.group_id,g.dependent_command_id,d.command_key,g.ordinal,g.kind,g.threshold,
		m.predecessor_command_id,p.state
		FROM `+pgschema.Table(s.schema, "flow_command_dependency_groups")+` g
		JOIN `+pgschema.Table(s.schema, "flow_commands")+` d ON d.command_id=g.dependent_command_id
		JOIN `+pgschema.Table(s.schema, "flow_command_dependency_members")+` m ON m.group_id=g.group_id
		JOIN `+pgschema.Table(s.schema, "flow_commands")+` p ON p.command_id=m.predecessor_command_id
		WHERE g.execution_id=$1 AND g.state='unresolved'
		ORDER BY d.command_key,g.ordinal,m.predecessor_key`, semantic.ExecutionID())
	if err != nil {
		return nil, MapError("load unresolved dependency groups", err)
	}
	defer rows.Close()
	var result []graphGroup
	var current *graphGroup
	for rows.Next() {
		var groupID, dependentID, predecessorID uuid.UUID
		var dependentKey, kind, predecessorState string
		var ordinal int
		var threshold *int
		if err := rows.Scan(&groupID, &dependentID, &dependentKey, &ordinal, &kind, &threshold, &predecessorID, &predecessorState); err != nil {
			return nil, MapError("scan unresolved dependency group", err)
		}
		if current == nil || current.id != groupID {
			result = append(result, graphGroup{id: groupID, dependent: dependentID, dependentKey: dependentKey,
				ordinal: ordinal, kind: kind, threshold: threshold})
			current = &result[len(result)-1]
		}
		current.members = append(current.members, graphMember{id: predecessorID, state: predecessorState})
	}
	if err := rows.Err(); err != nil {
		return nil, MapError("read unresolved dependency groups", err)
	}
	return result, nil
}

func (s *Store) applyGraphResolution(
	ctx context.Context,
	semantic *SemanticTx,
	resolution graphResolution,
	journal ApplyResult,
	skippedEntryOffset int,
) error {
	for _, group := range resolution.groups {
		if _, err := semantic.PGX().Exec(ctx, `UPDATE `+pgschema.Table(s.schema, "flow_command_dependency_groups")+`
			SET state=$2,resolved_at=$3 WHERE group_id=$1 AND state='unresolved'`, group.id, group.state, semantic.DBNow()); err != nil {
			return MapError("resolve dependency group", err)
		}
		if _, err := semantic.PGX().Exec(ctx, `UPDATE `+pgschema.Table(s.schema, "flow_commands")+` c
			SET state=CASE WHEN c.state='pending' AND c.unsatisfied_groups=1 AND c.unsatisfied_waits=0 THEN 'ready' ELSE c.state END,
			    unsatisfied_groups=GREATEST(0,c.unsatisfied_groups-1),updated_at=$2,
			    status_at=CASE WHEN c.state='pending' AND c.unsatisfied_groups=1 AND c.unsatisfied_waits=0 THEN $2 ELSE c.status_at END
			FROM `+pgschema.Table(s.schema, "flow_command_dependency_groups")+` g
			WHERE g.group_id=$1 AND c.command_id=g.dependent_command_id`, group.id, semantic.DBNow()); err != nil {
			return MapError("update resolved dependency count", err)
		}
	}
	for _, wait := range resolution.waits {
		commandTag, err := semantic.PGX().Exec(ctx, `UPDATE `+pgschema.Table(s.schema, "flow_command_event_waits")+`
			SET satisfied_position=$5 WHERE command_id=$1 AND event_namespace=$2 AND event_name=$3 AND event_version=$4
			AND satisfied_position IS NULL`, wait.commandID, wait.namespace, wait.name, wait.version, wait.position)
		if err != nil {
			return MapError("satisfy command event wait", err)
		}
		if commandTag.RowsAffected() > 0 {
			if _, err := semantic.PGX().Exec(ctx, `UPDATE `+pgschema.Table(s.schema, "flow_commands")+`
				SET state=CASE WHEN state='pending' AND unsatisfied_waits=1 AND unsatisfied_groups=0 THEN 'ready' ELSE state END,
				    unsatisfied_waits=GREATEST(0,unsatisfied_waits-1),updated_at=$2,
				    status_at=CASE WHEN state='pending' AND unsatisfied_waits=1 AND unsatisfied_groups=0 THEN $2 ELSE status_at END
				WHERE command_id=$1`, wait.commandID, semantic.DBNow()); err != nil {
				return MapError("update satisfied wait count", err)
			}
		}
	}
	for index, command := range resolution.skipped {
		position := journal.Journal[skippedEntryOffset+index].Position
		failure := terminalFailure{Code: "dependency_unsatisfiable", Message: "a command dependency became unsatisfiable"}
		if _, err := semantic.PGX().Exec(ctx, `UPDATE `+pgschema.Table(s.schema, "flow_commands")+`
			SET state='skipped',terminal_failure=$2::jsonb,terminal_position=$3,finished_at=$4,
			updated_at=$4,status_at=$4 WHERE command_id=$1 AND state NOT IN ('succeeded','failed','cancelled','expired','skipped')`,
			command.id, jsonString(failure), position, semantic.DBNow()); err != nil {
			return MapError("skip unsatisfiable command", err)
		}
		if _, err := semantic.PGX().Exec(ctx, `DELETE FROM `+pgschema.Table(s.schema, "flow_command_queue")+` WHERE command_id=$1`, command.id); err != nil {
			return MapError("remove skipped command queue row", err)
		}
	}
	for _, command := range resolution.ready {
		nextRun := semantic.DBNow()
		if command.initialDelay > 0 {
			nextRun = command.createdAt.Add(command.initialDelay)
			if nextRun.Before(semantic.DBNow()) {
				nextRun = semantic.DBNow()
			}
		}
		commandTag, err := semantic.PGX().Exec(ctx, `UPDATE `+pgschema.Table(s.schema, "flow_commands")+`
			SET state='ready',budget_started_at=$2,next_attempt_at=$2,updated_at=$3,status_at=$3
			WHERE command_id=$1 AND state IN ('pending','ready') AND unsatisfied_groups=0 AND unsatisfied_waits=0
			AND budget_started_at IS NULL`,
			command.id, nextRun, semantic.DBNow())
		if err != nil {
			return MapError("make dependency command ready", err)
		}
		if commandTag.RowsAffected() > 0 {
			_, err = semantic.PGX().Exec(ctx, `INSERT INTO `+pgschema.Table(s.schema, "flow_command_queue")+`
				(command_id,execution_id,queue,name,version,state,next_run_at,updated_at)
				VALUES ($1,$2,$3,$4,$5,'ready',$6,$7) ON CONFLICT (command_id) DO NOTHING`,
				command.id, semantic.ExecutionID(), command.queue, command.name, command.version, nextRun, semantic.DBNow())
			if err != nil {
				return MapError("enqueue dependency command", err)
			}
		}
	}
	for _, command := range resolution.waiting {
		var deadline *time.Time
		if command.waitTimeout > 0 {
			value := semantic.DBNow().Add(command.waitTimeout)
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
			WHERE command_id=$1 AND state='pending' AND unsatisfied_groups=0 AND unsatisfied_waits>0`,
			command.id, semantic.DBNow(), deadline); err != nil {
			return MapError("start command event wait", err)
		}
	}
	return nil
}

func (s *Store) matchingWaitsLocked(
	ctx context.Context,
	semantic *SemanticTx,
	namespace, name string,
	version int,
	position int64,
) ([]graphWaitUpdate, error) {
	rows, err := semantic.PGX().Query(ctx, `SELECT w.command_id,w.event_namespace,w.event_name,w.event_version
		FROM `+pgschema.Table(s.schema, "flow_command_event_waits")+` w
		JOIN `+pgschema.Table(s.schema, "flow_commands")+` c USING (command_id)
		WHERE w.execution_id=$1 AND w.event_namespace=$2 AND w.event_name=$3 AND w.event_version=$4
		AND w.satisfied_position IS NULL AND (c.wait_deadline_at IS NULL OR $5<=c.wait_deadline_at)
		ORDER BY w.command_id FOR UPDATE OF w`, semantic.ExecutionID(), namespace, name, version, semantic.DBNow())
	if err != nil {
		return nil, MapError("lock matching command waits", err)
	}
	defer rows.Close()
	var waits []graphWaitUpdate
	for rows.Next() {
		var wait graphWaitUpdate
		if err := rows.Scan(&wait.commandID, &wait.namespace, &wait.name, &wait.version); err != nil {
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
