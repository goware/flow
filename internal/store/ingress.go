package store

import (
	"bytes"
	"context"
	"encoding/hex"
	"encoding/json"
	"errors"
	"fmt"
	"slices"
	"time"

	"github.com/google/uuid"
	"github.com/goware/flow/internal/canonical"
	"github.com/goware/flow/internal/flowerr"
	"github.com/goware/flow/internal/pgschema"
	"github.com/goware/flow/internal/store/journalcodec"
	"github.com/jackc/pgx/v5"
)

type DriverMode string

const (
	DriverDirect      DriverMode = "direct"
	DriverPlan        DriverMode = "plan"
	DriverCoordinator DriverMode = "coordinator"
)

type DeadlineSpec struct {
	Mode     string
	Duration time.Duration
}

type CommandCreate struct {
	ID                     uuid.UUID
	Key                    string
	Name                   string
	Version                int
	Args                   canonical.Value
	DeclarationFingerprint [32]byte
	Origin                 string
	ParentCommandID        *uuid.UUID
	Required               bool
	FailureScope           bool
	Queue                  string
	AttemptTimeout         time.Duration
	RetryPolicy            canonical.Value
	ScheduleKind           string
	InitialDelay           time.Duration
}

type CoordinatorCreate struct {
	ID          uuid.UUID
	State       canonical.Value
	RetryPolicy canonical.Value
}

type StartRequest struct {
	ID                uuid.UUID
	Mode              DriverMode
	DefinitionName    string
	DefinitionVersion int
	Key               string
	StartFingerprint  [32]byte
	Input             canonical.Value
	Metadata          canonical.Value
	FailFast          bool
	Deadline          DeadlineSpec
	MaxCommands       int
	Root              *CommandCreate
	Coordinator       *CoordinatorCreate
}

type StartResult struct {
	ExecutionID   uuid.UUID
	RootCommandID *uuid.UUID
	Created       bool
}

// StartInTx creates or idempotently rediscovers an execution inside tx. It
// never commits or rolls back tx.
func (s *Store) StartInTx(ctx context.Context, tx pgx.Tx, request StartRequest) (StartResult, error) {
	if tx == nil {
		return StartResult{}, fmt.Errorf("%w: transaction is nil", flowerr.ErrInvalid)
	}
	if err := validateStartRequest(request); err != nil {
		return StartResult{}, err
	}
	var dbNow time.Time
	if err := tx.QueryRow(ctx, `SELECT clock_timestamp()`).Scan(&dbNow); err != nil {
		return StartResult{}, MapError("capture start time", err)
	}
	deadlineAt := deadlineAt(dbNow, request.Deadline)
	planDirty := request.Mode == DriverPlan
	var planDirtySince *time.Time
	if planDirty {
		planDirtySince = &dbNow
	}
	var rootID *uuid.UUID
	commandCount := 0
	if request.Root != nil {
		rootID = clonePointer(&request.Root.ID)
		commandCount = 1
	}

	var inserted uuid.UUID
	err := tx.QueryRow(ctx, `INSERT INTO `+pgschema.Table(s.schema, "flow_executions")+` (
		execution_id,driver_mode,definition_name,definition_version,execution_key,status,fail_fast,
		start_fingerprint,input,input_hash,metadata,metadata_canonical,metadata_hash,
		deadline_at,max_commands,command_count,open_commands,plan_dirty,plan_dirty_since,
		next_journal_position,root_command_id,created_at,updated_at,status_at
	) VALUES ($1,$2,$3,$4,$5,'running',$6,$7,$8,$9,$10::jsonb,$11,$12,$13,$14,$15,$15,$16,$17,1,$18,$19,$19,$19)
	ON CONFLICT DO NOTHING RETURNING execution_id`,
		request.ID, string(request.Mode), request.DefinitionName, request.DefinitionVersion, request.Key,
		request.FailFast, request.StartFingerprint[:], request.Input.Bytes, request.Input.Digest[:],
		string(request.Metadata.Bytes), request.Metadata.Bytes, request.Metadata.Digest[:], deadlineAt,
		request.MaxCommands, commandCount, planDirty, planDirtySince, rootID, dbNow,
	).Scan(&inserted)
	if err != nil && !errors.Is(err, pgx.ErrNoRows) {
		return StartResult{}, MapError("insert execution", err)
	}
	if errors.Is(err, pgx.ErrNoRows) {
		return s.loadEquivalentStart(ctx, tx, request)
	}
	if inserted != request.ID {
		return StartResult{}, fmt.Errorf("%w: inserted execution identity differs", flowerr.ErrInvalidState)
	}

	semantic, err := s.AdoptSemantic(tx, request.ID, dbNow)
	if err != nil {
		return StartResult{}, err
	}
	entries, err := startJournalEntries(request, dbNow, deadlineAt)
	if err != nil {
		return StartResult{}, err
	}
	journal, err := semantic.Apply(ctx, PersistedChangeSet{Journal: entries})
	if err != nil {
		return StartResult{}, err
	}

	if request.Root != nil {
		if len(journal.Journal) != 2 {
			return StartResult{}, fmt.Errorf("%w: direct start journal shape", flowerr.ErrInvalidState)
		}
		if err := s.insertCommand(ctx, tx, request.ID, *request.Root, journal.Journal[1].Position, dbNow, dbNow); err != nil {
			return StartResult{}, err
		}
	}
	if request.Coordinator != nil {
		if len(journal.Journal) != 1 {
			return StartResult{}, fmt.Errorf("%w: coordinator start journal shape", flowerr.ErrInvalidState)
		}
		if err := s.insertCoordinator(ctx, tx, request, journal.Journal[0].Position, dbNow); err != nil {
			return StartResult{}, err
		}
	}
	return StartResult{ExecutionID: request.ID, RootCommandID: rootID, Created: true}, nil
}

func (s *Store) loadEquivalentStart(ctx context.Context, tx pgx.Tx, request StartRequest) (StartResult, error) {
	if request.Key == "" {
		return StartResult{}, fmt.Errorf("%w: unkeyed start unexpectedly conflicted", flowerr.ErrInvalidState)
	}
	var id uuid.UUID
	var version int
	var fingerprint, input, metadata []byte
	var rootID *uuid.UUID
	err := tx.QueryRow(ctx, `SELECT execution_id,definition_version,start_fingerprint,input,metadata_canonical,root_command_id
		FROM `+pgschema.Table(s.schema, "flow_executions")+`
		WHERE driver_mode=$1 AND definition_name=$2 AND execution_key=$3 FOR UPDATE`,
		string(request.Mode), request.DefinitionName, request.Key,
	).Scan(&id, &version, &fingerprint, &input, &metadata, &rootID)
	if err != nil {
		return StartResult{}, MapError("load existing execution", err)
	}
	if version != request.DefinitionVersion || !bytes.Equal(fingerprint, request.StartFingerprint[:]) ||
		!bytes.Equal(input, request.Input.Bytes) || !bytes.Equal(metadata, request.Metadata.Bytes) {
		return StartResult{}, fmt.Errorf("%w: execution start identity differs", flowerr.ErrConflict)
	}
	return StartResult{ExecutionID: id, RootCommandID: clonePointer(rootID), Created: false}, nil
}

func startJournalEntries(request StartRequest, dbNow time.Time, deadlineAt *time.Time) ([]JournalEntry, error) {
	body := journalcodec.ExecutionStartedBody{
		V: 1, ExecutionID: request.ID.String(), DriverMode: string(request.Mode),
		DefinitionName: request.DefinitionName, DefinitionVersion: request.DefinitionVersion,
		ExecutionKey: request.Key, Input: json.RawMessage(request.Input.BytesCopy()), FailFast: request.FailFast,
		DeadlineMode: request.Deadline.Mode, DeadlineDuration: request.Deadline.Duration.Milliseconds(),
		DeadlineAt: clonePointer(deadlineAt), MaxCommands: request.MaxCommands,
		Metadata: json.RawMessage(request.Metadata.BytesCopy()),
	}
	if request.Coordinator != nil {
		body.CoordinatorID = request.Coordinator.ID.String()
		body.CoordinatorPolicy = json.RawMessage(request.Coordinator.RetryPolicy.BytesCopy())
	}
	start, err := NewJournalEntry(ExecutionStarted, body)
	if err != nil {
		return nil, fmt.Errorf("encode execution start: %w", err)
	}
	entries := []JournalEntry{start}
	if request.Root != nil {
		created, err := commandCreatedEntry(*request.Root, dbNow, dbNow)
		if err != nil {
			return nil, err
		}
		zero := 0
		created.CausationBatchIndex = &zero
		entries = append(entries, created)
	}
	return entries, nil
}

func commandCreatedEntry(command CommandCreate, budgetStartedAt, nextAttemptAt time.Time) (JournalEntry, error) {
	var timeoutMS *int64
	if command.AttemptTimeout > 0 {
		value := command.AttemptTimeout.Milliseconds()
		timeoutMS = &value
	}
	var initialDelayMS *int64
	if command.InitialDelay > 0 {
		value := command.InitialDelay.Milliseconds()
		initialDelayMS = &value
	}
	body := journalcodec.CommandCreatedBody{
		V: 1, CommandID: command.ID.String(), CommandKey: command.Key, Name: command.Name,
		Version: command.Version, Args: json.RawMessage(command.Args.BytesCopy()), Origin: command.Origin,
		Required: command.Required, FailureScope: command.FailureScope, InitialState: "ready",
		Queue: command.Queue, AttemptTimeoutMS: timeoutMS,
		RetryPolicy: json.RawMessage(command.RetryPolicy.BytesCopy()), ScheduleKind: command.ScheduleKind,
		InitialDelayMS: initialDelayMS, BudgetStartedAt: &budgetStartedAt, NextAttemptAt: &nextAttemptAt,
		DeclarationFingerprint: hex.EncodeToString(command.DeclarationFingerprint[:]),
	}
	if command.ParentCommandID != nil {
		body.ParentCommandID = command.ParentCommandID.String()
	}
	entry, err := NewJournalEntry(CommandCreated, body)
	if err != nil {
		return JournalEntry{}, fmt.Errorf("encode command creation: %w", err)
	}
	entry.CommandID = clonePointer(&command.ID)
	return entry, nil
}

func (s *Store) insertCommand(ctx context.Context, tx pgx.Tx, executionID uuid.UUID, command CommandCreate, createdPosition int64, budgetStartedAt, nextAttemptAt time.Time) error {
	var timeoutMS *int64
	if command.AttemptTimeout > 0 {
		value := command.AttemptTimeout.Milliseconds()
		timeoutMS = &value
	}
	var initialDelayMS *int64
	if command.InitialDelay > 0 {
		value := command.InitialDelay.Milliseconds()
		initialDelayMS = &value
	}
	_, err := tx.Exec(ctx, `INSERT INTO `+pgschema.Table(s.schema, "flow_commands")+` (
		command_id,execution_id,command_key,name,version,origin,parent_command_id,required,
		args,args_hash,declaration_fingerprint,state,child_membership_closed,failure_scope,
		queue,attempt_timeout_ms,retry_policy,retry_policy_hash,schedule_kind,initial_delay_ms,
		budget_started_at,next_attempt_at,created_position,created_at,updated_at,status_at
	) VALUES ($1,$2,$3,$4,$5,$6,$7,$8,$9,$10,$11,'ready',false,$12,$13,$14,$15::jsonb,$16,$17,$18,$19,$20,$21,$22,$22,$22)`,
		command.ID, executionID, command.Key, command.Name, command.Version, command.Origin,
		command.ParentCommandID, command.Required, command.Args.Bytes, command.Args.Digest[:],
		command.DeclarationFingerprint[:], command.FailureScope, command.Queue, timeoutMS,
		string(command.RetryPolicy.Bytes), command.RetryPolicy.Digest[:], command.ScheduleKind, initialDelayMS,
		budgetStartedAt, nextAttemptAt, createdPosition, budgetStartedAt,
	)
	if err != nil {
		return MapError("insert command", err)
	}
	_, err = tx.Exec(ctx, `INSERT INTO `+pgschema.Table(s.schema, "flow_command_queue")+`
		(command_id,execution_id,queue,name,version,state,next_run_at,updated_at)
		VALUES ($1,$2,$3,$4,$5,'ready',$6,$7)`,
		command.ID, executionID, command.Queue, command.Name, command.Version, nextAttemptAt, budgetStartedAt)
	if err != nil {
		return MapError("enqueue command", err)
	}
	return nil
}

func (s *Store) insertCoordinator(ctx context.Context, tx pgx.Tx, request StartRequest, statePosition int64, dbNow time.Time) error {
	coordinator := request.Coordinator
	_, err := tx.Exec(ctx, `INSERT INTO `+pgschema.Table(s.schema, "flow_coordinators")+` (
		coordinator_id,execution_id,name,version,status,state,state_hash,state_position,
		start_pending,inbox_position,delivery_key,delivery_state,retry_policy,retry_policy_hash,
		budget_started_at,next_attempt_at,created_at,updated_at
	) VALUES ($1,$2,$3,$4,'active',$5,$6,$7,true,0,'start','ready',$8::jsonb,$9,$10,$10,$10,$10)`,
		coordinator.ID, request.ID, request.DefinitionName, request.DefinitionVersion,
		coordinator.State.Bytes, coordinator.State.Digest[:], statePosition,
		string(coordinator.RetryPolicy.Bytes), coordinator.RetryPolicy.Digest[:], dbNow)
	if err != nil {
		return MapError("insert coordinator", err)
	}
	return nil
}

type ExecutionHead struct {
	ID           uuid.UUID
	Mode         DriverMode
	Status       string
	FailFast     bool
	MaxCommands  int
	CommandCount int
	OpenCommands int
}

func (s *Store) LoadExecutionHead(ctx context.Context, semantic *SemanticTx) (ExecutionHead, error) {
	if semantic == nil || semantic.PGX() == nil {
		return ExecutionHead{}, fmt.Errorf("%w: semantic transaction is nil", flowerr.ErrInvalid)
	}
	var result ExecutionHead
	var mode string
	err := semantic.PGX().QueryRow(ctx, `SELECT execution_id,driver_mode,status,fail_fast,max_commands,command_count,open_commands
		FROM `+pgschema.Table(s.schema, "flow_executions")+` WHERE execution_id=$1`, semantic.ExecutionID()).
		Scan(&result.ID, &mode, &result.Status, &result.FailFast, &result.MaxCommands, &result.CommandCount, &result.OpenCommands)
	if err != nil {
		return ExecutionHead{}, MapError("load execution", err)
	}
	result.Mode = DriverMode(mode)
	return result, nil
}

type IssueResult struct {
	CommandID uuid.UUID
	Created   bool
}

func (s *Store) IssueLocked(ctx context.Context, semantic *SemanticTx, command CommandCreate) (IssueResult, error) {
	head, err := s.LoadExecutionHead(ctx, semantic)
	if err != nil {
		return IssueResult{}, err
	}
	var existingID uuid.UUID
	var name, origin string
	var version int
	var args, fingerprint []byte
	err = semantic.PGX().QueryRow(ctx, `SELECT command_id,name,version,origin,args,declaration_fingerprint
		FROM `+pgschema.Table(s.schema, "flow_commands")+`
		WHERE execution_id=$1 AND command_key=$2 FOR UPDATE`, head.ID, command.Key).
		Scan(&existingID, &name, &version, &origin, &args, &fingerprint)
	if err == nil {
		if name == command.Name && version == command.Version && origin == command.Origin &&
			bytes.Equal(args, command.Args.Bytes) && bytes.Equal(fingerprint, command.DeclarationFingerprint[:]) {
			return IssueResult{CommandID: existingID, Created: false}, nil
		}
		return IssueResult{}, fmt.Errorf("%w: command key is owned by a different declaration", flowerr.ErrConflict)
	}
	if !errors.Is(err, pgx.ErrNoRows) {
		return IssueResult{}, MapError("lookup command key", err)
	}
	if head.Mode == DriverDirect {
		return IssueResult{}, fmt.Errorf("%w: direct executions reject Issue", flowerr.ErrInvalidState)
	}
	if head.Status != "running" && head.Status != "failing" {
		return IssueResult{}, fmt.Errorf("%w: execution is terminal", flowerr.ErrTerminal)
	}
	if head.MaxCommands != 0 && head.CommandCount+1 > head.MaxCommands {
		return IssueResult{}, fmt.Errorf("%w: execution command ceiling reached", flowerr.ErrInvalid)
	}
	command.Origin = "external_issue"
	command.Required = true
	command.ParentCommandID = nil
	command.ScheduleKind = "none"
	command.InitialDelay = 0
	entry, err := commandCreatedEntry(command, semantic.DBNow(), semantic.DBNow())
	if err != nil {
		return IssueResult{}, err
	}
	one := int64(1)
	entry.CausationPosition = &one
	journal, err := semantic.Apply(ctx, PersistedChangeSet{Journal: []JournalEntry{entry}})
	if err != nil {
		return IssueResult{}, err
	}
	if _, err := semantic.PGX().Exec(ctx, `UPDATE `+pgschema.Table(s.schema, "flow_executions")+`
		SET command_count=command_count+1,open_commands=open_commands+1,updated_at=$2
		WHERE execution_id=$1`, head.ID, semantic.DBNow()); err != nil {
		return IssueResult{}, MapError("increment command count", err)
	}
	if err := s.insertCommand(ctx, semantic.PGX(), head.ID, command, journal.Journal[0].Position, semantic.DBNow(), semantic.DBNow()); err != nil {
		return IssueResult{}, err
	}
	return IssueResult{CommandID: command.ID, Created: true}, nil
}

type ApplicationEvent struct {
	ID      uuid.UUID
	Name    string
	Version int
	Key     string
	Body    canonical.Value
}

type ExistingEvent struct {
	ID      uuid.UUID
	Version int
	Body    []byte
	Found   bool
}

func (s *Store) LookupApplicationEvent(ctx context.Context, tx pgx.Tx, executionID uuid.UUID, name, key string) (ExistingEvent, error) {
	var row pgx.Row
	query := `SELECT event_id,event_version,body FROM ` + pgschema.Table(s.schema, "flow_journal") + `
		WHERE execution_id=$1 AND event_namespace='application' AND event_name=$2 AND event_key=$3`
	if tx != nil {
		row = tx.QueryRow(ctx, query, executionID, name, key)
	} else {
		row = s.db.Conn.QueryRow(ctx, query, executionID, name, key)
	}
	var result ExistingEvent
	if err := row.Scan(&result.ID, &result.Version, &result.Body); err != nil {
		if errors.Is(err, pgx.ErrNoRows) {
			return ExistingEvent{}, nil
		}
		return ExistingEvent{}, MapError("lookup application event", err)
	}
	result.Found = true
	return result, nil
}

func (s *Store) PublishLocked(ctx context.Context, semantic *SemanticTx, event ApplicationEvent) (bool, error) {
	existing, err := s.LookupApplicationEvent(ctx, semantic.PGX(), semantic.ExecutionID(), event.Name, event.Key)
	if err != nil {
		return false, err
	}
	if existing.Found {
		if existing.Version == event.Version && bytes.Equal(existing.Body, event.Body.Bytes) {
			return false, nil
		}
		return false, fmt.Errorf("%w: application event identity differs", flowerr.ErrConflict)
	}
	head, err := s.LoadExecutionHead(ctx, semantic)
	if err != nil {
		return false, err
	}
	if head.Status != "running" && head.Status != "failing" {
		return false, fmt.Errorf("%w: execution is terminal", flowerr.ErrTerminal)
	}
	entry := JournalEntry{
		EntryID: uuid.New(), Kind: EventRecorded, EventID: clonePointer(&event.ID),
		EventNamespace: stringPointer("application"), EventName: clonePointer(&event.Name),
		EventVersion: clonePointer(&event.Version), EventKey: clonePointer(&event.Key),
		EventClass: stringPointer("application"), Body: event.Body,
	}
	if _, err := semantic.Apply(ctx, PersistedChangeSet{Journal: []JournalEntry{entry}}); err != nil {
		return false, err
	}
	if head.Mode == DriverPlan {
		_, err := semantic.PGX().Exec(ctx, `UPDATE `+pgschema.Table(s.schema, "flow_executions")+`
			SET plan_dirty=true,plan_dirty_since=COALESCE(plan_dirty_since,$2),plan_quiescent=false,updated_at=$2
			WHERE execution_id=$1`, head.ID, semantic.DBNow())
		if err != nil {
			return false, MapError("mark plan dirty", err)
		}
	}
	return true, nil
}

type CancelResult struct {
	Created bool
}

type terminalFailure struct {
	Code    string `json:"code"`
	Message string `json:"message"`
}

func (s *Store) LookupCommandExecution(ctx context.Context, tx pgx.Tx, commandID uuid.UUID) (uuid.UUID, error) {
	query := `SELECT execution_id FROM ` + pgschema.Table(s.schema, "flow_commands") + ` WHERE command_id=$1`
	var row pgx.Row
	if tx != nil {
		row = tx.QueryRow(ctx, query, commandID)
	} else {
		row = s.db.Conn.QueryRow(ctx, query, commandID)
	}
	var executionID uuid.UUID
	if err := row.Scan(&executionID); err != nil {
		return uuid.Nil, MapError("lookup command execution", err)
	}
	return executionID, nil
}

func (s *Store) CancelCommandLocked(ctx context.Context, semantic *SemanticTx, commandID uuid.UUID, reason string) (CancelResult, error) {
	head, err := s.LoadExecutionHead(ctx, semantic)
	if err != nil {
		return CancelResult{}, err
	}
	if err := s.lockCoordinatorForExecution(ctx, semantic); err != nil {
		return CancelResult{}, err
	}
	var key, state string
	var required bool
	var failureBytes []byte
	err = semantic.PGX().QueryRow(ctx, `SELECT command_key,state,required,COALESCE(terminal_failure,'null'::jsonb)
		FROM `+pgschema.Table(s.schema, "flow_commands")+`
		WHERE command_id=$1 AND execution_id=$2 FOR UPDATE`, commandID, head.ID).
		Scan(&key, &state, &required, &failureBytes)
	if err != nil {
		return CancelResult{}, MapError("lock command for cancellation", err)
	}
	if isCommandTerminal(state) {
		if state == "cancelled" && sameTerminalReason(failureBytes, reason) {
			return CancelResult{}, nil
		}
		return CancelResult{}, fmt.Errorf("%w: command is terminal", flowerr.ErrTerminal)
	}
	if head.Status != "running" && head.Status != "failing" {
		return CancelResult{}, fmt.Errorf("%w: execution is terminal", flowerr.ErrTerminal)
	}

	commandEvent, err := terminalEvent(commandID, key, "cancelled", reason, "flow.command_cancelled", "command_terminal")
	if err != nil {
		return CancelResult{}, err
	}
	entries := []JournalEntry{commandEvent}
	becameFailing := required && head.Status == "running"
	if becameFailing {
		failing, err := NewJournalEntry(ExecutionFailing, journalcodec.TerminalEventBody{V: 1, Status: "failing", Reason: reason, CommandKey: key})
		if err != nil {
			return CancelResult{}, err
		}
		zero := 0
		failing.CausationBatchIndex = &zero
		entries = append(entries, failing)
	}
	terminalExecution := required && head.OpenCommands == 1 && head.Mode != DriverPlan
	if terminalExecution {
		executionEvent, err := executionTerminalEvent("failed", reason, "flow.execution_failed")
		if err != nil {
			return CancelResult{}, err
		}
		zero := 0
		executionEvent.CausationBatchIndex = &zero
		entries = append(entries, executionEvent)
	}
	journal, err := semantic.Apply(ctx, PersistedChangeSet{Journal: entries})
	if err != nil {
		return CancelResult{}, err
	}
	failure := terminalFailure{Code: "cancelled", Message: reason}
	if _, err := semantic.PGX().Exec(ctx, `UPDATE `+pgschema.Table(s.schema, "flow_commands")+`
		SET state='cancelled',terminal_failure=$2::jsonb,terminal_position=$3,finished_at=$4,updated_at=$4,status_at=$4
		WHERE command_id=$1`, commandID, jsonString(failure), journal.Journal[0].Position, semantic.DBNow()); err != nil {
		return CancelResult{}, MapError("cancel command", err)
	}
	if _, err := semantic.PGX().Exec(ctx, `DELETE FROM `+pgschema.Table(s.schema, "flow_command_queue")+` WHERE command_id=$1`, commandID); err != nil {
		return CancelResult{}, MapError("remove cancelled command delivery", err)
	}

	if terminalExecution {
		if _, err := semantic.PGX().Exec(ctx, `UPDATE `+pgschema.Table(s.schema, "flow_executions")+`
			SET status='failed',open_commands=open_commands-1,failure=$2::jsonb,finished_at=$3,updated_at=$3,status_at=$3,
			    plan_dirty=false,plan_dirty_since=NULL
			WHERE execution_id=$1`, head.ID, jsonString(terminalFailure{Code: "command_cancelled", Message: reason}), semantic.DBNow()); err != nil {
			return CancelResult{}, MapError("fail execution after command cancellation", err)
		}
		if _, err := semantic.PGX().Exec(ctx, `UPDATE `+pgschema.Table(s.schema, "flow_coordinators")+`
			SET status='failed',start_pending=false,delivery_state='idle',delivery_key=NULL,delivery_position=NULL,
			    active_attempt_id=NULL,lease_token=NULL,lease_owner=NULL,lease_started_at=NULL,lease_expires_at=NULL,
			    finished_at=$2,updated_at=$2 WHERE execution_id=$1 AND status='active'`, head.ID, semantic.DBNow()); err != nil {
			return CancelResult{}, MapError("fail execution coordinator", err)
		}
	} else {
		status := head.Status
		if becameFailing {
			status = "failing"
		}
		if _, err := semantic.PGX().Exec(ctx, `UPDATE `+pgschema.Table(s.schema, "flow_executions")+`
			SET status=$2,open_commands=open_commands-1,updated_at=$3,status_at=CASE WHEN status<>$2 THEN $3 ELSE status_at END,
			    plan_dirty=CASE WHEN driver_mode='plan' THEN true ELSE plan_dirty END,
			    plan_dirty_since=CASE WHEN driver_mode='plan' THEN COALESCE(plan_dirty_since,$3) ELSE plan_dirty_since END,
			    plan_quiescent=CASE WHEN driver_mode='plan' THEN false ELSE plan_quiescent END
			WHERE execution_id=$1`, head.ID, status, semantic.DBNow()); err != nil {
			return CancelResult{}, MapError("update execution after command cancellation", err)
		}
	}
	return CancelResult{Created: true}, nil
}

func (s *Store) CancelExecutionLocked(ctx context.Context, semantic *SemanticTx, reason string) (CancelResult, error) {
	head, err := s.LoadExecutionHead(ctx, semantic)
	if err != nil {
		return CancelResult{}, err
	}
	if err := s.lockCoordinatorForExecution(ctx, semantic); err != nil {
		return CancelResult{}, err
	}
	var executionFailure []byte
	if err := semantic.PGX().QueryRow(ctx, `SELECT COALESCE(failure,'null'::jsonb) FROM `+
		pgschema.Table(s.schema, "flow_executions")+` WHERE execution_id=$1`, head.ID).Scan(&executionFailure); err != nil {
		return CancelResult{}, MapError("load execution cancellation", err)
	}
	if head.Status != "running" && head.Status != "failing" {
		if head.Status == "cancelled" && sameTerminalReason(executionFailure, reason) {
			return CancelResult{}, nil
		}
		return CancelResult{}, fmt.Errorf("%w: execution is terminal", flowerr.ErrTerminal)
	}

	rows, err := semantic.PGX().Query(ctx, `SELECT command_id,command_key FROM `+
		pgschema.Table(s.schema, "flow_commands")+`
		WHERE execution_id=$1 AND state NOT IN ('succeeded','failed','cancelled','expired','skipped')
		ORDER BY command_id FOR UPDATE`, head.ID)
	if err != nil {
		return CancelResult{}, MapError("lock execution commands for cancellation", err)
	}
	type command struct {
		id  uuid.UUID
		key string
	}
	commands := make([]command, 0, head.OpenCommands)
	for rows.Next() {
		var item command
		if err := rows.Scan(&item.id, &item.key); err != nil {
			rows.Close()
			return CancelResult{}, MapError("scan execution commands for cancellation", err)
		}
		commands = append(commands, item)
	}
	if err := rows.Err(); err != nil {
		rows.Close()
		return CancelResult{}, MapError("read execution commands for cancellation", err)
	}
	rows.Close()
	if len(commands) != head.OpenCommands {
		return CancelResult{}, fmt.Errorf("%w: execution open-command counter differs from materialized commands", flowerr.ErrInvalidState)
	}
	slices.SortFunc(commands, func(a, b command) int {
		if a.key < b.key {
			return -1
		}
		if a.key > b.key {
			return 1
		}
		return bytes.Compare(a.id[:], b.id[:])
	})

	entries := make([]JournalEntry, 0, len(commands)+1)
	for _, command := range commands {
		entry, err := terminalEvent(command.id, command.key, "cancelled", reason, "flow.command_cancelled", "command_terminal")
		if err != nil {
			return CancelResult{}, err
		}
		entries = append(entries, entry)
	}
	executionEvent, err := executionTerminalEvent("cancelled", reason, "flow.execution_cancelled")
	if err != nil {
		return CancelResult{}, err
	}
	entries = append(entries, executionEvent)
	journal, err := semantic.Apply(ctx, PersistedChangeSet{Journal: entries})
	if err != nil {
		return CancelResult{}, err
	}
	failure := jsonString(terminalFailure{Code: "cancelled", Message: reason})
	for index, command := range commands {
		if _, err := semantic.PGX().Exec(ctx, `UPDATE `+pgschema.Table(s.schema, "flow_commands")+`
			SET state='cancelled',terminal_failure=$2::jsonb,terminal_position=$3,finished_at=$4,updated_at=$4,status_at=$4
			WHERE command_id=$1`, command.id, failure, journal.Journal[index].Position, semantic.DBNow()); err != nil {
			return CancelResult{}, MapError("cancel execution command", err)
		}
	}
	if _, err := semantic.PGX().Exec(ctx, `DELETE FROM `+pgschema.Table(s.schema, "flow_command_queue")+` WHERE execution_id=$1`, head.ID); err != nil {
		return CancelResult{}, MapError("remove execution deliveries", err)
	}
	if _, err := semantic.PGX().Exec(ctx, `UPDATE `+pgschema.Table(s.schema, "flow_coordinators")+`
		SET status='cancelled',start_pending=false,delivery_state='idle',delivery_key=NULL,delivery_position=NULL,
		    active_attempt_id=NULL,lease_token=NULL,lease_owner=NULL,lease_started_at=NULL,lease_expires_at=NULL,
		    finished_at=$2,updated_at=$2 WHERE execution_id=$1 AND status='active'`, head.ID, semantic.DBNow()); err != nil {
		return CancelResult{}, MapError("cancel coordinator", err)
	}
	if _, err := semantic.PGX().Exec(ctx, `UPDATE `+pgschema.Table(s.schema, "flow_executions")+`
		SET status='cancelled',open_commands=0,failure=$2::jsonb,finished_at=$3,updated_at=$3,status_at=$3,
		    plan_dirty=false,plan_dirty_since=NULL
		WHERE execution_id=$1`, head.ID, failure, semantic.DBNow()); err != nil {
		return CancelResult{}, MapError("cancel execution", err)
	}
	return CancelResult{Created: true}, nil
}

func terminalEvent(commandID uuid.UUID, key, status, reason, name, class string) (JournalEntry, error) {
	body, err := NewJournalEntry(EventRecorded, journalcodec.TerminalEventBody{V: 1, Status: status, Reason: reason, CommandKey: key})
	if err != nil {
		return JournalEntry{}, err
	}
	eventID := uuid.New()
	version := 1
	body.CommandID = clonePointer(&commandID)
	body.EventID = &eventID
	body.EventNamespace = stringPointer("runtime")
	body.EventName = clonePointer(&name)
	body.EventVersion = &version
	body.EventKey = clonePointer(&key)
	body.EventClass = clonePointer(&class)
	body.TerminalStatus = clonePointer(&status)
	return body, nil
}

func executionTerminalEvent(status, reason, name string) (JournalEntry, error) {
	body, err := NewJournalEntry(EventRecorded, journalcodec.TerminalEventBody{V: 1, Status: status, Reason: reason})
	if err != nil {
		return JournalEntry{}, err
	}
	eventID := uuid.New()
	version := 1
	body.EventID = &eventID
	body.EventNamespace = stringPointer("runtime")
	body.EventName = clonePointer(&name)
	body.EventVersion = &version
	body.EventClass = stringPointer("execution_terminal")
	body.TerminalStatus = clonePointer(&status)
	return body, nil
}

func sameTerminalReason(value []byte, reason string) bool {
	var failure terminalFailure
	return json.Unmarshal(value, &failure) == nil && failure.Message == reason
}

func jsonString(value any) string {
	encoded, _ := json.Marshal(value)
	return string(encoded)
}

func isCommandTerminal(state string) bool {
	switch state {
	case "succeeded", "failed", "cancelled", "expired", "skipped":
		return true
	default:
		return false
	}
}

func (s *Store) lockCoordinatorForExecution(ctx context.Context, semantic *SemanticTx) error {
	var coordinatorID uuid.UUID
	err := semantic.PGX().QueryRow(ctx, `SELECT coordinator_id FROM `+
		pgschema.Table(s.schema, "flow_coordinators")+` WHERE execution_id=$1 FOR UPDATE`, semantic.ExecutionID()).Scan(&coordinatorID)
	if errors.Is(err, pgx.ErrNoRows) {
		return nil
	}
	if err != nil {
		return MapError("lock execution coordinator", err)
	}
	return nil
}

func deadlineAt(now time.Time, spec DeadlineSpec) *time.Time {
	if spec.Mode == "none" {
		return nil
	}
	value := now.Add(spec.Duration)
	return &value
}

func validateStartRequest(request StartRequest) error {
	if request.ID == uuid.Nil || request.DefinitionName == "" || request.DefinitionVersion <= 0 ||
		request.MaxCommands < 0 || len(request.Input.Bytes) == 0 || len(request.Metadata.Bytes) == 0 {
		return fmt.Errorf("%w: incomplete execution start", flowerr.ErrInvalid)
	}
	if request.Mode != DriverDirect && request.Mode != DriverPlan && request.Mode != DriverCoordinator {
		return fmt.Errorf("%w: invalid driver mode", flowerr.ErrInvalid)
	}
	if request.Deadline.Mode != "none" && request.Deadline.Mode != "duration" {
		return fmt.Errorf("%w: invalid deadline mode", flowerr.ErrInvalid)
	}
	if request.Deadline.Mode == "duration" && request.Deadline.Duration <= 0 {
		return fmt.Errorf("%w: invalid execution deadline", flowerr.ErrInvalid)
	}
	switch request.Mode {
	case DriverDirect:
		if request.Root == nil || request.Coordinator != nil {
			return fmt.Errorf("%w: invalid direct start shape", flowerr.ErrInvalid)
		}
	case DriverPlan:
		if request.Root != nil || request.Coordinator != nil {
			return fmt.Errorf("%w: invalid plan start shape", flowerr.ErrInvalid)
		}
	case DriverCoordinator:
		if request.Root != nil || request.Coordinator == nil {
			return fmt.Errorf("%w: invalid coordinator start shape", flowerr.ErrInvalid)
		}
	}
	return nil
}

func stringPointer(value string) *string { return &value }
