package store

import (
	"context"
	"encoding/hex"
	"encoding/json"
	"fmt"
	"slices"
	"sort"
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
	FailureCode            string
	FailureMessage         string
	ChildMembershipClosed  bool
	Children               []string
	DeclarationFingerprint []byte
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
	ExecutionID    uuid.UUID
	Status         string
	Input          []byte
	DeadlineAt     *time.Time
	MaxCommands    int
	CommandCount   int
	OpenCommands   int
	Revision       int64
	JournalThrough int64
	Commands       []PlanCommandSnapshot
	Events         []PlanEventSnapshot
}

type PlanReconciliation struct {
	ExpectedRevision int64
	ConsumedThrough  int64
	WaitingReads     int
	Commands         []CommandCreate
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
		SELECT e.execution_id,e.definition_name,e.definition_version,e.plan_dirty_since
		FROM `+pgschema.Table(s.schema, "flow_executions")+` e JOIN handled h
		ON h.name=e.definition_name AND h.version=e.definition_version
		WHERE e.driver_mode='plan' AND e.status IN ('running','failing') AND e.plan_dirty
		ORDER BY e.plan_dirty_since,e.execution_id LIMIT $3`, names, versions, limit)
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
	err := semantic.PGX().QueryRow(ctx, `SELECT status,input,deadline_at,max_commands,command_count,open_commands,
		plan_revision,next_journal_position-1 FROM `+pgschema.Table(s.schema, "flow_executions")+`
		WHERE execution_id=$1 AND driver_mode='plan' AND plan_dirty`, semantic.ExecutionID()).
		Scan(&result.Status, &result.Input, &result.DeadlineAt, &result.MaxCommands, &result.CommandCount,
			&result.OpenCommands, &result.Revision, &result.JournalThrough)
	if err != nil {
		return PlanSnapshot{}, MapError("load dirty plan execution", err)
	}
	rows, err := semantic.PGX().Query(ctx, `SELECT command_id,command_key,name,version,origin,state,args,result,
		terminal_failure,child_membership_closed,declaration_fingerprint
		FROM `+pgschema.Table(s.schema, "flow_commands")+` WHERE execution_id=$1 ORDER BY command_key`, semantic.ExecutionID())
	if err != nil {
		return PlanSnapshot{}, MapError("load plan commands", err)
	}
	for rows.Next() {
		var command PlanCommandSnapshot
		var failureBytes []byte
		if err := rows.Scan(&command.ID, &command.Key, &command.Name, &command.Version, &command.Origin,
			&command.State, &command.Args, &command.Result, &failureBytes, &command.ChildMembershipClosed,
			&command.DeclarationFingerprint); err != nil {
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

	rows, err = semantic.PGX().Query(ctx, `SELECT position,event_namespace,event_name,event_version,COALESCE(event_key,''),body
		FROM `+pgschema.Table(s.schema, "flow_journal")+`
		WHERE execution_id=$1 AND entry_kind='event_recorded'
		AND event_namespace IN ('application','command_success') ORDER BY position`, semantic.ExecutionID())
	if err != nil {
		return PlanSnapshot{}, MapError("load plan events", err)
	}
	for rows.Next() {
		var event PlanEventSnapshot
		var body []byte
		if err := rows.Scan(&event.Position, &event.Namespace, &event.Name, &event.Version, &event.Key, &body); err != nil {
			rows.Close()
			return PlanSnapshot{}, MapError("scan plan event", err)
		}
		switch event.Namespace {
		case "application":
			decoded, err := journalcodec.Decode[journalcodec.ApplicationEventBody](body)
			if err != nil {
				rows.Close()
				return PlanSnapshot{}, err
			}
			event.Payload = slices.Clone(decoded.Payload)
		case "command_success":
			decoded, err := journalcodec.Decode[journalcodec.CommandSucceededBody](body)
			if err != nil {
				rows.Close()
				return PlanSnapshot{}, err
			}
			event.Payload = slices.Clone(decoded.Result)
		}
		result.Events = append(result.Events, event)
	}
	if err := rows.Err(); err != nil {
		rows.Close()
		return PlanSnapshot{}, MapError("read plan events", err)
	}
	rows.Close()
	result.Input = slices.Clone(result.Input)
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
	decisionBody := journalcodec.PlanReconciledBody{
		V: 1, Revision: revision + 1, ConsumedThrough: request.ConsumedThrough,
		WaitingReads: request.WaitingReads, Quiescent: len(commands) == 0,
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
	resolution, err := s.resolveGraphLocked(ctx, semantic, nil, nil)
	if err != nil {
		return PlanReconcileResult{}, err
	}
	result := PlanReconcileResult{Created: len(commands), Skipped: len(resolution.skipped)}
	continuation := semantic.continueBatch()
	continuationEntries, err := resolution.skippedEntries(0)
	if err != nil {
		return PlanReconcileResult{}, err
	}
	for index := range continuationEntries {
		continuationEntries[index].CausationBatchIndex = nil
		continuationEntries[index].CausationPosition = clonePointer(&journal.Journal[0].Position)
	}
	// Immediate terminal declarations require another dirty pass so the pure
	// plan can observe them before completion. Ordinary open declarations wait
	// for their own terminal trigger; an empty delta is quiescent.
	remainDirty := len(resolution.skipped) > 0
	effectiveOpen := head.OpenCommands + len(commands) - len(resolution.skipped)
	terminalStatus := ""
	if !remainDirty && len(commands) == 0 && effectiveOpen == 0 {
		if head.Status == "failing" {
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
		if err := s.applyGraphResolution(ctx, continuation, resolution, continuationJournal, 0); err != nil {
			return PlanReconcileResult{}, err
		}
	} else if err := s.applyGraphResolution(ctx, continuation, resolution, ApplyResult{}, 0); err != nil {
		return PlanReconcileResult{}, err
	}
	waitingOn := "[]"
	status := head.Status
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
		status=$10,finished_at=$11,updated_at=$6,status_at=CASE WHEN status<>$10 THEN $6 ELSE status_at END
		WHERE execution_id=$1`, semantic.ExecutionID(), len(commands), len(resolution.skipped), revision+1,
		remainDirty, semantic.DBNow(), len(commands) == 0, request.WaitingReads, waitingOn, status, finishedAt)
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
