package store

import (
	"context"
	"encoding/hex"
	"encoding/json"
	"fmt"
	"slices"
	"sort"
	"strings"
	"time"

	"github.com/google/uuid"
	"github.com/goware/flow/internal/flowerr"
	"github.com/goware/flow/internal/pgschema"
	"github.com/goware/flow/internal/store/journalcodec"
)

type PlanKind struct {
	Name    string
	Version int
}

type PlanCandidate struct {
	ExecutionID uuid.UUID
	Name        string
	Version     int
	DirtySince  time.Time
}

type PlanCommandSnapshot struct {
	ID                     uuid.UUID
	Key                    string
	Name                   string
	Version                int
	Origin                 string
	State                  string
	Args                   []byte
	Result                 []byte
	ResultLoaded           bool
	FailureScope           bool
	FailureCode            string
	FailureMessage         string
	ChildMembershipClosed  bool
	Children               []string
	DeclarationFingerprint []byte
}

type PlanEventSelector struct {
	Namespace string
	Name      string
	Version   int
}

type PlanEventSnapshot struct {
	Position  int64
	Namespace string
	Name      string
	Version   int
	Key       string
	Payload   []byte
}

type PlanSnapshot struct {
	ExecutionID     uuid.UUID
	DecisionAt      time.Time
	Status          string
	Input           []byte
	DeadlineAt      *time.Time
	MaxCommands     int
	CommandCount    int
	OpenCommands    int
	Revision        int64
	JournalThrough  int64
	Commands        []PlanCommandSnapshot
	Events          []PlanEventSnapshot
	LoadedSelectors []PlanEventSelector
}

type PlanReconciliation struct {
	ExpectedRevision int64
	ConsumedThrough  int64
	WaitingReads     int
	WaitingOn        []string
	Quiescent        bool
	Commands         []CommandCreate
	ImmediateExpired []uuid.UUID
}

type PlanReconcileResult struct {
	Created  int
	Skipped  int
	Terminal bool
	Status   string
}

func (s *Store) ProbeDirtyPlans(ctx context.Context, kinds []PlanKind, limit int) ([]PlanCandidate, error) {
	if len(kinds) == 0 || limit <= 0 {
		return nil, nil
	}
	names := make([]string, len(kinds))
	versions := make([]int32, len(kinds))
	for index, kind := range kinds {
		if kind.Name == "" || kind.Version <= 0 {
			return nil, fmt.Errorf("%w: invalid plan probe kind", flowerr.ErrInvalid)
		}
		names[index], versions[index] = kind.Name, int32(kind.Version)
	}
	rows, err := s.db.Conn.Query(ctx, `WITH handled(name,version) AS (SELECT * FROM unnest($1::text[],$2::integer[]))
		SELECT candidate.execution_id,candidate.definition_name,candidate.definition_version,candidate.plan_dirty_since
		FROM handled h CROSS JOIN LATERAL (
			SELECT e.execution_id,e.definition_name,e.definition_version,e.plan_dirty_since
			FROM `+pgschema.Table(s.schema, "flow_executions")+` e
			WHERE e.definition_name=h.name AND e.definition_version=h.version
			  AND e.driver_mode='plan' AND e.status IN ('running','failing') AND e.plan_dirty
			ORDER BY e.plan_dirty_since,e.execution_id LIMIT $3
		) candidate
		ORDER BY candidate.plan_dirty_since,candidate.execution_id LIMIT $3`, names, versions, limit)
	if err != nil {
		return nil, MapError("probe dirty plans", err)
	}
	defer rows.Close()
	var result []PlanCandidate
	for rows.Next() {
		var candidate PlanCandidate
		if err := rows.Scan(&candidate.ExecutionID, &candidate.Name, &candidate.Version, &candidate.DirtySince); err != nil {
			return nil, MapError("scan dirty plan", err)
		}
		result = append(result, candidate)
	}
	if err := rows.Err(); err != nil {
		return nil, MapError("read dirty plans", err)
	}
	return result, nil
}

func (s *Store) LoadPlanSnapshot(ctx context.Context, semantic *SemanticTx) (PlanSnapshot, error) {
	var result PlanSnapshot
	result.ExecutionID = semantic.ExecutionID()
	result.DecisionAt = semantic.DBNow()
	err := semantic.PGX().QueryRow(ctx, `SELECT status,input,deadline_at,max_commands,command_count,open_commands,
		plan_revision,next_journal_position-1 FROM `+pgschema.Table(s.schema, "flow_executions")+`
		WHERE execution_id=$1 AND driver_mode='plan' AND plan_dirty`, semantic.ExecutionID()).
		Scan(&result.Status, &result.Input, &result.DeadlineAt, &result.MaxCommands, &result.CommandCount,
			&result.OpenCommands, &result.Revision, &result.JournalThrough)
	if err != nil {
		return PlanSnapshot{}, MapError("load dirty plan execution", err)
	}
	rows, err := semantic.PGX().Query(ctx, `SELECT command_id,command_key,name,version,origin,state,args,
		terminal_failure,child_membership_closed,declaration_fingerprint,failure_scope
		FROM `+pgschema.Table(s.schema, "flow_commands")+` WHERE execution_id=$1 ORDER BY command_key`, semantic.ExecutionID())
	if err != nil {
		return PlanSnapshot{}, MapError("load plan commands", err)
	}
	for rows.Next() {
		var command PlanCommandSnapshot
		var failureBytes []byte
		if err := rows.Scan(&command.ID, &command.Key, &command.Name, &command.Version, &command.Origin,
			&command.State, &command.Args, &failureBytes, &command.ChildMembershipClosed,
			&command.DeclarationFingerprint, &command.FailureScope); err != nil {
			rows.Close()
			return PlanSnapshot{}, MapError("scan plan command", err)
		}
		if len(failureBytes) > 0 {
			var failure terminalFailure
			if err := json.Unmarshal(failureBytes, &failure); err != nil {
				rows.Close()
				return PlanSnapshot{}, fmt.Errorf("%w: invalid plan command failure", flowerr.ErrInvalidState)
			}
			command.FailureCode, command.FailureMessage = failure.Code, failure.Message
		}
		result.Commands = append(result.Commands, command)
	}
	if err := rows.Err(); err != nil {
		rows.Close()
		return PlanSnapshot{}, MapError("read plan commands", err)
	}
	rows.Close()
	byID := make(map[uuid.UUID]int, len(result.Commands))
	for index := range result.Commands {
		byID[result.Commands[index].ID] = index
	}
	rows, err = semantic.PGX().Query(ctx, `SELECT parent_command_id,command_key FROM `+pgschema.Table(s.schema, "flow_commands")+`
		WHERE execution_id=$1 AND parent_command_id IS NOT NULL ORDER BY parent_command_id,command_key`, semantic.ExecutionID())
	if err != nil {
		return PlanSnapshot{}, MapError("load plan child membership", err)
	}
	for rows.Next() {
		var parent uuid.UUID
		var key string
		if err := rows.Scan(&parent, &key); err != nil {
			rows.Close()
			return PlanSnapshot{}, MapError("scan plan child membership", err)
		}
		if index, ok := byID[parent]; ok {
			result.Commands[index].Children = append(result.Commands[index].Children, key)
		}
	}
	if err := rows.Err(); err != nil {
		rows.Close()
		return PlanSnapshot{}, MapError("read plan child membership", err)
	}
	rows.Close()

	result.Input = slices.Clone(result.Input)
	return result, nil
}

func (s *Store) LoadPlanEventsLocked(
	ctx context.Context,
	semantic *SemanticTx,
	through int64,
	selectors []PlanEventSelector,
) ([]PlanEventSnapshot, error) {
	if len(selectors) == 0 {
		return nil, nil
	}
	const maxSelectorsPerQuery = 1_000
	var result []PlanEventSnapshot
	for start := 0; start < len(selectors); start += maxSelectorsPerQuery {
		end := min(start+maxSelectorsPerQuery, len(selectors))
		loaded, err := s.loadPlanEventSelectorsLocked(ctx, semantic, through, selectors[start:end])
		if err != nil {
			return nil, err
		}
		result = append(result, loaded...)
	}
	sort.Slice(result, func(i, j int) bool { return result[i].Position < result[j].Position })
	return result, nil
}

func (s *Store) loadPlanEventSelectorsLocked(
	ctx context.Context,
	semantic *SemanticTx,
	through int64,
	selectors []PlanEventSelector,
) ([]PlanEventSnapshot, error) {
	args := make([]any, 0, 2+len(selectors)*3)
	args = append(args, semantic.ExecutionID(), through)
	var predicates strings.Builder
	for index, selector := range selectors {
		if selector.Namespace == "" || selector.Name == "" || selector.Version <= 0 {
			return nil, fmt.Errorf("%w: invalid plan event selector", flowerr.ErrInvalid)
		}
		if index > 0 {
			predicates.WriteString(" OR ")
		}
		first := 3 + index*3
		fmt.Fprintf(&predicates, "(event_namespace=$%d AND event_name=$%d AND event_version=$%d)", first, first+1, first+2)
		args = append(args, selector.Namespace, selector.Name, int32(selector.Version))
	}
	query := `SELECT position,event_namespace,event_name,event_version,COALESCE(event_key,''),body
		FROM ` + pgschema.Table(s.schema, "flow_journal") + `
		WHERE execution_id=$1 AND position<=$2 AND entry_kind='event_recorded' AND (` + predicates.String() + `)
		ORDER BY position`
	rows, err := semantic.PGX().Query(ctx, query, args...)
	if err != nil {
		return nil, MapError("load selected plan events", err)
	}
	defer rows.Close()
	var result []PlanEventSnapshot
	for rows.Next() {
		var event PlanEventSnapshot
		var body []byte
		if err := rows.Scan(&event.Position, &event.Namespace, &event.Name, &event.Version, &event.Key, &body); err != nil {
			return nil, MapError("scan selected plan event", err)
		}
		switch event.Namespace {
		case "application":
			decoded, err := journalcodec.Decode[journalcodec.ApplicationEventBody](body)
			if err != nil {
				return nil, err
			}
			event.Payload = slices.Clone(decoded.Payload)
		case "command_success":
			decoded, err := journalcodec.Decode[journalcodec.CommandSucceededBody](body)
			if err != nil {
				return nil, err
			}
			event.Payload = slices.Clone(decoded.Result)
		default:
			return nil, fmt.Errorf("%w: unsupported plan event namespace", flowerr.ErrInvalidState)
		}
		result = append(result, event)
	}
	if err := rows.Err(); err != nil {
		return nil, MapError("read selected plan events", err)
	}
	return result, nil
}

func (s *Store) LoadPlanCommandResultsLocked(
	ctx context.Context,
	semantic *SemanticTx,
	commandIDs []uuid.UUID,
) (map[uuid.UUID][]byte, error) {
	if len(commandIDs) == 0 {
		return nil, nil
	}
	rows, err := semantic.PGX().Query(ctx, `SELECT command_id,result FROM `+pgschema.Table(s.schema, "flow_commands")+`
		WHERE execution_id=$1 AND command_id=ANY($2) AND state='succeeded' ORDER BY command_id`,
		semantic.ExecutionID(), commandIDs)
	if err != nil {
		return nil, MapError("load selected plan results", err)
	}
	defer rows.Close()
	result := make(map[uuid.UUID][]byte, len(commandIDs))
	for rows.Next() {
		var id uuid.UUID
		var value []byte
		if err := rows.Scan(&id, &value); err != nil {
			return nil, MapError("scan selected plan result", err)
		}
		result[id] = slices.Clone(value)
	}
	if err := rows.Err(); err != nil {
		return nil, MapError("read selected plan results", err)
	}
	return result, nil
}

func (s *Store) ReconcilePlanLocked(
	ctx context.Context,
	semantic *SemanticTx,
	request PlanReconciliation,
) (PlanReconcileResult, error) {
	head, err := s.LoadExecutionHead(ctx, semantic)
	if err != nil {
		return PlanReconcileResult{}, err
	}
	if head.Mode != DriverPlan || head.Status != "running" && head.Status != "failing" {
		return PlanReconcileResult{}, fmt.Errorf("%w: execution is not a live plan", flowerr.ErrInvalidState)
	}
	var revision int64
	var dirty bool
	if err := semantic.PGX().QueryRow(ctx, `SELECT plan_revision,plan_dirty FROM `+pgschema.Table(s.schema, "flow_executions")+`
		WHERE execution_id=$1`, semantic.ExecutionID()).Scan(&revision, &dirty); err != nil {
		return PlanReconcileResult{}, MapError("lock plan revision", err)
	}
	if !dirty || revision != request.ExpectedRevision {
		return PlanReconcileResult{}, fmt.Errorf("%w: plan reconciliation snapshot is stale", flowerr.ErrConflict)
	}
	if head.MaxCommands > 0 && head.CommandCount+len(request.Commands) > head.MaxCommands {
		return PlanReconcileResult{}, fmt.Errorf("%w: execution command ceiling exceeded", flowerr.ErrInvalidState)
	}
	commands := append([]CommandCreate(nil), request.Commands...)
	sort.Slice(commands, func(i, j int) bool { return commands[i].Key < commands[j].Key })
	commandByID := make(map[uuid.UUID]CommandCreate, len(commands))
	for _, command := range commands {
		commandByID[command.ID] = command
	}
	expiredSet := make(map[uuid.UUID]string, len(request.ImmediateExpired))
	expiredCommands := make([]CommandCreate, 0, len(request.ImmediateExpired))
	requiredExpired := false
	for _, id := range request.ImmediateExpired {
		command, exists := commandByID[id]
		if !exists {
			return PlanReconcileResult{}, fmt.Errorf("%w: immediate expiry references a command outside the plan delta", flowerr.ErrInvalidState)
		}
		if _, duplicate := expiredSet[id]; duplicate {
			continue
		}
		expiredSet[id] = "expired"
		expiredCommands = append(expiredCommands, command)
		requiredExpired = requiredExpired || command.Required
	}
	sort.Slice(expiredCommands, func(i, j int) bool { return expiredCommands[i].Key < expiredCommands[j].Key })
	decisionBody := journalcodec.PlanReconciledBody{
		V: 1, Revision: revision + 1, ConsumedThrough: request.ConsumedThrough,
		WaitingReads: request.WaitingReads, Quiescent: request.Quiescent,
	}
	for _, command := range commands {
		decisionBody.Declarations = append(decisionBody.Declarations, journalcodec.PlanReconciledDeclaration{
			Key: command.Key, CommandID: command.ID.String(), Fingerprint: hex.EncodeToString(command.DeclarationFingerprint[:]),
		})
	}
	decision, err := NewJournalEntry(PlanReconciled, decisionBody)
	if err != nil {
		return PlanReconcileResult{}, err
	}
	decision.PlanRevision = clonePointer(&decisionBody.Revision)
	entries := []JournalEntry{decision}
	for _, command := range commands {
		created, err := commandCreatedEntry(command, semantic.DBNow(), semantic.DBNow().Add(command.InitialDelay))
		if err != nil {
			return PlanReconcileResult{}, err
		}
		zero := 0
		created.CausationBatchIndex = &zero
		entries = append(entries, created)
	}
	journal, err := semantic.Apply(ctx, PersistedChangeSet{Journal: entries})
	if err != nil {
		return PlanReconcileResult{}, err
	}
	positionByID := make(map[uuid.UUID]int64, len(commands))
	for index, command := range commands {
		positionByID[command.ID] = journal.Journal[index+1].Position
	}
	for _, command := range topologicalCommandOrder(commands) {
		next := semantic.DBNow().Add(command.InitialDelay)
		if err := s.insertCommand(ctx, semantic.PGX(), semantic.ExecutionID(), command, positionByID[command.ID], semantic.DBNow(), next); err != nil {
			return PlanReconcileResult{}, err
		}
	}
	resolution := graphResolution{}
	failureEffects := failureResolution{}
	if requiredExpired {
		failureEffects, err = s.resolveRequiredFailuresLocked(ctx, semantic, expiredSet, head.FailFast)
		resolution = failureEffects.graphResolution
	} else {
		resolution, err = s.resolveGraphLocked(ctx, semantic, expiredSet, nil)
	}
	if err != nil {
		return PlanReconcileResult{}, err
	}
	result := PlanReconcileResult{Created: len(commands), Skipped: len(expiredCommands) + len(resolution.skipped) + len(failureEffects.cancelled)}
	continuation := semantic.continueBatch()
	continuationEntries := make([]JournalEntry, 0, result.Skipped+2)
	for _, command := range expiredCommands {
		expired, err := terminalEventWithCode(command.ID, command.Key, "expired", "initial_schedule_expired",
			"first eligible time is not before the execution deadline", "flow.command_expired", "command_terminal")
		if err != nil {
			return PlanReconcileResult{}, err
		}
		continuationEntries = append(continuationEntries, expired)
	}
	skippedOffset := len(continuationEntries)
	skippedEntries, err := resolution.skippedEntries(0)
	if err != nil {
		return PlanReconcileResult{}, err
	}
	continuationEntries = append(continuationEntries, skippedEntries...)
	if requiredExpired && head.Status == "running" {
		failing, err := NewJournalEntry(ExecutionFailing, map[string]any{
			"v": 1, "status": "failing", "reason": "initial command schedule exceeds execution deadline",
			"fail_fast": head.FailFast,
		})
		if err != nil {
			return PlanReconcileResult{}, err
		}
		continuationEntries = append(continuationEntries, failing)
	}
	cancelledOffset := len(continuationEntries)
	cancelledEntries, err := failureEffects.cancellationEntries(0, "cancelled by fail-fast after required command expiry")
	if err != nil {
		return PlanReconcileResult{}, err
	}
	continuationEntries = append(continuationEntries, cancelledEntries...)
	for index := range continuationEntries {
		continuationEntries[index].CausationBatchIndex = nil
		continuationEntries[index].CausationPosition = clonePointer(&journal.Journal[0].Position)
	}
	effectiveOpen := head.OpenCommands + len(commands) - len(expiredCommands) - len(resolution.skipped) - len(failureEffects.cancelled)
	terminalStatus := ""
	if request.Quiescent && effectiveOpen == 0 {
		if head.Status == "failing" || requiredExpired {
			terminalStatus = "failed"
		} else if request.WaitingReads == 0 {
			terminalStatus = "succeeded"
		}
	}
	if terminalStatus != "" {
		name := "flow.execution_succeeded"
		if terminalStatus == "failed" {
			name = "flow.execution_failed"
		}
		terminal, err := executionTerminalEvent(terminalStatus, "", name)
		if err != nil {
			return PlanReconcileResult{}, err
		}
		terminal.CausationPosition = clonePointer(&journal.Journal[0].Position)
		continuationEntries = append(continuationEntries, terminal)
	}
	var continuationJournal ApplyResult
	if len(continuationEntries) > 0 {
		continuationJournal, err = continuation.Apply(ctx, PersistedChangeSet{Journal: continuationEntries})
		if err != nil {
			return PlanReconcileResult{}, err
		}
	}
	failure := terminalFailure{Code: "initial_schedule_expired", Message: "first eligible time is not before the execution deadline"}
	for index, command := range expiredCommands {
		position := continuationJournal.Journal[index].Position
		if _, err := semantic.PGX().Exec(ctx, `UPDATE `+pgschema.Table(s.schema, "flow_commands")+`
			SET state='expired',terminal_failure=$2::jsonb,terminal_position=$3,finished_at=$4,updated_at=$4,status_at=$4
			WHERE command_id=$1 AND state NOT IN ('succeeded','failed','cancelled','expired','skipped')`,
			command.ID, jsonString(failure), position, semantic.DBNow()); err != nil {
			return PlanReconcileResult{}, MapError("expire initial plan schedule", err)
		}
		if _, err := semantic.PGX().Exec(ctx, `DELETE FROM `+pgschema.Table(s.schema, "flow_command_queue")+` WHERE command_id=$1`, command.ID); err != nil {
			return PlanReconcileResult{}, MapError("remove expired initial plan schedule", err)
		}
	}
	if requiredExpired {
		if err := s.applyFailureResolution(ctx, continuation, failureEffects, continuationJournal, skippedOffset,
			cancelledOffset, "cancelled by fail-fast after required command expiry"); err != nil {
			return PlanReconcileResult{}, err
		}
	} else if err := s.applyGraphResolution(ctx, continuation, resolution, continuationJournal, skippedOffset); err != nil {
		return PlanReconcileResult{}, err
	}
	waitingOn := request.WaitingOn
	if waitingOn == nil {
		waitingOn = []string{}
	}
	waitingOnBytes, err := json.Marshal(waitingOn)
	if err != nil {
		return PlanReconcileResult{}, fmt.Errorf("%w: encode plan waiting diagnostics", flowerr.ErrInvalidState)
	}
	waitingOnJSON := string(waitingOnBytes)
	status := head.Status
	if requiredExpired {
		status = "failing"
	}
	finishedAt := (*time.Time)(nil)
	if terminalStatus != "" {
		status = terminalStatus
		finishedAt = clonePointer(&semantic.dbNow)
		result.Terminal, result.Status = true, terminalStatus
	}
	_, err = semantic.PGX().Exec(ctx, `UPDATE `+pgschema.Table(s.schema, "flow_executions")+`
		SET command_count=command_count+$2,open_commands=open_commands+$2-$3,
		plan_revision=$4,plan_dirty=$5,plan_dirty_since=CASE WHEN $5 THEN COALESCE(plan_dirty_since,$6) ELSE NULL END,
		plan_quiescent=$7,plan_waiting_count=$8,plan_waiting_on=$9::jsonb,
		status=$10,failure=CASE WHEN $12 THEN $13::jsonb ELSE failure END,
		finished_at=$11,updated_at=$6,status_at=CASE WHEN status<>$10 THEN $6 ELSE status_at END
		WHERE execution_id=$1`, semantic.ExecutionID(), len(commands),
		len(expiredCommands)+len(resolution.skipped)+len(failureEffects.cancelled), revision+1,
		false, semantic.DBNow(), request.Quiescent, request.WaitingReads, waitingOnJSON, status, finishedAt,
		requiredExpired, jsonString(failure))
	if err != nil {
		return PlanReconcileResult{}, MapError("materialize plan reconciliation", err)
	}
	return result, nil
}

func topologicalCommandOrder(commands []CommandCreate) []CommandCreate {
	byID := make(map[uuid.UUID]CommandCreate, len(commands))
	for _, command := range commands {
		byID[command.ID] = command
	}
	visited := make(map[uuid.UUID]bool, len(commands))
	result := make([]CommandCreate, 0, len(commands))
	var visit func(CommandCreate)
	visit = func(command CommandCreate) {
		if visited[command.ID] {
			return
		}
		visited[command.ID] = true
		for _, group := range command.Dependencies {
			for _, member := range group.Members {
				if dependency, ok := byID[member.CommandID]; ok {
					visit(dependency)
				}
			}
		}
		result = append(result, command)
	}
	for _, command := range commands {
		visit(command)
	}
	return result
}

func (s *Store) FailPlanLocked(ctx context.Context, semantic *SemanticTx, code, reason string) error {
	head, err := s.LoadExecutionHead(ctx, semantic)
	if err != nil {
		return err
	}
	if head.Mode != DriverPlan || head.Status != "running" && head.Status != "failing" {
		return fmt.Errorf("%w: execution is not a live plan", flowerr.ErrInvalidState)
	}
	if len(code) > 128 {
		code = code[:128]
	}
	if len(reason) > 1024 {
		reason = reason[:1024]
	}
	planFailed, err := NewJournalEntry(EventRecorded, journalcodec.TerminalEventBody{
		V: 1, Status: "failed", Code: code, Reason: reason,
	})
	if err != nil {
		return err
	}
	planEventID := uuid.New()
	planFailed.EventID = &planEventID
	planFailed.EventNamespace = stringPointer("runtime")
	planFailed.EventName = stringPointer("flow.plan_failed")
	version := 1
	planFailed.EventVersion = &version
	planFailed.EventClass = stringPointer("plan_terminal")
	planFailed.TerminalStatus = stringPointer("failed")
	entries := []JournalEntry{planFailed}
	type command struct {
		id    uuid.UUID
		key   string
		state string
	}
	rows, err := semantic.PGX().Query(ctx, `SELECT command_id,command_key,state FROM `+pgschema.Table(s.schema, "flow_commands")+`
		WHERE execution_id=$1 AND state NOT IN ('succeeded','failed','cancelled','expired','skipped')
		ORDER BY command_key FOR UPDATE`, semantic.ExecutionID())
	if err != nil {
		return MapError("lock commands for plan failure", err)
	}
	var commands []command
	for rows.Next() {
		var item command
		if err := rows.Scan(&item.id, &item.key, &item.state); err != nil {
			rows.Close()
			return MapError("scan command for plan failure", err)
		}
		commands = append(commands, item)
	}
	if err := rows.Err(); err != nil {
		rows.Close()
		return MapError("read commands for plan failure", err)
	}
	rows.Close()
	terminalIndexes := make(map[uuid.UUID]int, len(commands))
	for _, command := range commands {
		if command.state == "running" {
			attempt, err := s.lockActiveCommandAttempt(ctx, semantic, command.id, command.state)
			if err != nil {
				return err
			}
			if attempt != nil {
				concluded, err := cancelledAttemptEvent(command.id, command.key, *attempt, "plan_failed", reason, semantic.DBNow())
				if err != nil {
					return err
				}
				entries = append(entries, concluded)
			}
		}
		cancelled, err := terminalEventWithCode(command.id, command.key, "cancelled", "plan_failed", reason,
			"flow.command_cancelled", "command_terminal")
		if err != nil {
			return err
		}
		zero := 0
		cancelled.CausationBatchIndex = &zero
		terminalIndexes[command.id] = len(entries)
		entries = append(entries, cancelled)
	}
	executionFailed, err := executionTerminalEvent("failed", reason, "flow.execution_failed")
	if err != nil {
		return err
	}
	zero := 0
	executionFailed.CausationBatchIndex = &zero
	entries = append(entries, executionFailed)
	journal, err := semantic.Apply(ctx, PersistedChangeSet{Journal: entries})
	if err != nil {
		return err
	}
	failure := terminalFailure{Code: code, Message: reason}
	for _, command := range commands {
		position := journal.Journal[terminalIndexes[command.id]].Position
		if _, err := semantic.PGX().Exec(ctx, `UPDATE `+pgschema.Table(s.schema, "flow_commands")+`
			SET state='cancelled',terminal_failure=$2::jsonb,terminal_position=$3,finished_at=$4,updated_at=$4,status_at=$4
			WHERE command_id=$1`, command.id, jsonString(failure), position, semantic.DBNow()); err != nil {
			return MapError("cancel command after plan failure", err)
		}
	}
	if _, err := semantic.PGX().Exec(ctx, `DELETE FROM `+pgschema.Table(s.schema, "flow_command_queue")+` WHERE execution_id=$1`, semantic.ExecutionID()); err != nil {
		return MapError("remove command queue after plan failure", err)
	}
	if _, err := semantic.PGX().Exec(ctx, `UPDATE `+pgschema.Table(s.schema, "flow_executions")+`
		SET status='failed',open_commands=0,failure=$2::jsonb,plan_dirty=false,plan_dirty_since=NULL,
		plan_quiescent=false,finished_at=$3,updated_at=$3,status_at=$3 WHERE execution_id=$1`,
		semantic.ExecutionID(), jsonString(failure), semantic.DBNow()); err != nil {
		return MapError("fail execution after plan defect", err)
	}
	return nil
}
