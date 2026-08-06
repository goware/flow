package store

import (
	"bytes"
	"context"
	"encoding/hex"
	"encoding/json"
	"errors"
	"fmt"
	"slices"
	"sort"
	"time"

	"github.com/google/uuid"
	"github.com/goware/flow/internal/canonical"
	"github.com/goware/flow/internal/durable"
	"github.com/goware/flow/internal/failure"
	"github.com/goware/flow/internal/flowerr"
	"github.com/goware/flow/internal/pgschema"
	retrypolicy "github.com/goware/flow/internal/retry"
	"github.com/goware/flow/internal/store/journalcodec"
	"github.com/jackc/pgx/v5"
)

const MaxCommandEventWaits = 256

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
	ParentCommandID        *uuid.UUID
	Required               bool
	Queue                  string
	AttemptTimeout         time.Duration
	RetryPolicy            canonical.Value
	InitialDelay           time.Duration
	Waits                  []EventWaitCreate
	Within                 time.Duration
}

type EventWaitCreate struct {
	Name string
	Key  string
}

// Execution key scopes. Permanent keys identify one execution forever and
// rediscover it idempotently; live keys are held only while their execution
// is non-terminal and are released at settlement.
const (
	KeyScopePermanent = "permanent"
	KeyScopeLive      = "live"
)

type StartRequest struct {
	ID                uuid.UUID
	DefinitionName    string
	DefinitionVersion int
	Key               string
	KeyScope          string
	StartFingerprint  [32]byte
	Input             canonical.Value
	Metadata          canonical.Value
	FailFast          bool
	Deadline          DeadlineSpec
	MaxCommands       int
	Root              *CommandCreate
}

// StartResult carries the accepted execution row as of durable acceptance.
// Created is false when an idempotent start rediscovered an existing
// execution.
type StartResult struct {
	Row     ExecutionRow
	Created bool
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
	if request.KeyScope == "" {
		request.KeyScope = KeyScopePermanent
	}
	// A live-keyed insert can conflict with a live holder that settles before
	// the rediscovery lookup runs; one further insert attempt covers that
	// window without looping unboundedly.
	attempts := 1
	if request.KeyScope == KeyScopeLive {
		attempts = 2
	}
	for attempt := 0; ; attempt++ {
		result, retry, err := s.startAttempt(ctx, tx, request)
		if err == nil || !retry || attempt+1 >= attempts {
			return result, err
		}
	}
}

func (s *Store) startAttempt(ctx context.Context, tx pgx.Tx, request StartRequest) (StartResult, bool, error) {
	var dbNow time.Time
	if err := tx.QueryRow(ctx, `SELECT clock_timestamp()`).Scan(&dbNow); err != nil {
		return StartResult{}, false, MapError("capture start time", err)
	}
	deadlineAt, err := deadlineAt(dbNow, request.Deadline)
	if err != nil {
		return StartResult{}, false, err
	}
	var rootID *uuid.UUID
	commandCount := 0
	if request.Root != nil {
		rootID = clonePointer(&request.Root.ID)
		commandCount = 1
	}

	var inserted uuid.UUID
	err = tx.QueryRow(ctx, `INSERT INTO `+pgschema.Table(s.schema, "flow_executions")+` (
		execution_id,definition_name,definition_version,execution_key,key_scope,status,fail_fast,
		start_fingerprint,input,metadata,metadata_canonical,
		deadline_at,max_commands,command_count,open_commands,
		next_journal_position,root_command_id,created_at,updated_at,status_at
	) VALUES ($1,$2,$3,$4,$5,'running',$6,$7,$8,$9::jsonb,$10,$11,$12,$13,$13,1,$14,$15,$15,$15)
	ON CONFLICT DO NOTHING RETURNING execution_id`,
		request.ID, request.DefinitionName, request.DefinitionVersion, request.Key, request.KeyScope,
		request.FailFast, request.StartFingerprint[:], request.Input.Bytes,
		string(request.Metadata.Bytes), request.Metadata.Bytes, deadlineAt,
		request.MaxCommands, commandCount, rootID, dbNow,
	).Scan(&inserted)
	if err != nil && !errors.Is(err, pgx.ErrNoRows) {
		return StartResult{}, false, MapError("insert execution", err)
	}
	if errors.Is(err, pgx.ErrNoRows) {
		result, retry, loadErr := s.loadEquivalentStart(ctx, tx, request)
		return result, retry, loadErr
	}
	if inserted != request.ID {
		return StartResult{}, false, fmt.Errorf("%w: inserted execution identity differs", flowerr.ErrInvalidState)
	}

	semantic, err := s.AdoptSemantic(tx, request.ID, dbNow)
	if err != nil {
		return StartResult{}, false, err
	}
	entries, err := startJournalEntries(request, dbNow, deadlineAt)
	if err != nil {
		return StartResult{}, false, err
	}
	journal, err := semantic.Apply(ctx, PersistedChangeSet{Journal: entries})
	if err != nil {
		return StartResult{}, false, err
	}

	if len(journal.Journal) != 2 {
		return StartResult{}, false, fmt.Errorf("%w: command start journal shape", flowerr.ErrInvalidState)
	}
	nextAttemptAt, err := durable.AddExactDuration("initial delay", dbNow, request.Root.InitialDelay)
	if err != nil {
		return StartResult{}, false, err
	}
	if err := s.insertCommand(ctx, tx, request.ID, *request.Root, journal.Journal[1].Position, dbNow, nextAttemptAt); err != nil {
		return StartResult{}, false, err
	}
	// Mirror the row this transaction just inserted instead of re-reading it.
	return StartResult{Row: ExecutionRow{
		ID: request.ID, DefinitionName: request.DefinitionName, DefinitionVersion: request.DefinitionVersion,
		Key: request.Key, RootCommandID: clonePointer(rootID), Status: "running", FailFast: request.FailFast,
		MaxCommands: request.MaxCommands, CommandCount: commandCount, OpenCommands: commandCount,
		DeadlineAt: clonePointer(deadlineAt), CreatedAt: dbNow, UpdatedAt: dbNow, StatusAt: dbNow,
		Metadata: request.Metadata.BytesCopy(),
	}, Created: true}, false, nil
}

// loadEquivalentStart resolves an insert that conflicted on the idempotency
// index. Permanent keys rediscover the one execution that owns the key and
// require an equivalent start identity. Live keys rediscover whichever live
// execution currently holds the key, with no identity comparison — the caller
// asked to ensure a live execution exists, not to describe a specific one.
// retry=true means the conflicting live holder settled before the lookup;
// the caller may attempt the insert again.
func (s *Store) loadEquivalentStart(ctx context.Context, tx pgx.Tx, request StartRequest) (StartResult, bool, error) {
	if request.Key == "" {
		return StartResult{}, false, fmt.Errorf("%w: unkeyed start unexpectedly conflicted", flowerr.ErrInvalidState)
	}
	if request.KeyScope == KeyScopeLive {
		var id uuid.UUID
		err := tx.QueryRow(ctx, `SELECT execution_id
			FROM `+pgschema.Table(s.schema, "flow_executions")+`
			WHERE definition_name=$1 AND execution_key=$2
			  AND key_scope=$3 AND status IN ('running','failing') FOR UPDATE`,
			request.DefinitionName, request.Key, KeyScopeLive,
		).Scan(&id)
		if errors.Is(err, pgx.ErrNoRows) {
			return StartResult{}, true, fmt.Errorf("%w: live key holder settled during start", flowerr.ErrConflict)
		}
		if err != nil {
			return StartResult{}, false, MapError("load live execution", err)
		}
		row, err := s.GetExecutionInTx(ctx, tx, id)
		if err != nil {
			return StartResult{}, false, err
		}
		return StartResult{Row: row, Created: false}, false, nil
	}
	var id uuid.UUID
	var version int
	var fingerprint, input, metadata []byte
	err := tx.QueryRow(ctx, `SELECT execution_id,definition_version,start_fingerprint,input,metadata_canonical
		FROM `+pgschema.Table(s.schema, "flow_executions")+`
		WHERE definition_name=$1 AND execution_key=$2 AND key_scope=$3 FOR UPDATE`,
		request.DefinitionName, request.Key, KeyScopePermanent,
	).Scan(&id, &version, &fingerprint, &input, &metadata)
	if err != nil {
		return StartResult{}, false, MapError("load existing execution", err)
	}
	if version != request.DefinitionVersion || !bytes.Equal(fingerprint, request.StartFingerprint[:]) ||
		!bytes.Equal(input, request.Input.Bytes) || !bytes.Equal(metadata, request.Metadata.Bytes) {
		return StartResult{}, false, fmt.Errorf("%w: execution start identity differs", flowerr.ErrConflict)
	}
	row, err := s.GetExecutionInTx(ctx, tx, id)
	if err != nil {
		return StartResult{}, false, err
	}
	return StartResult{Row: row, Created: false}, false, nil
}

func startJournalEntries(request StartRequest, dbNow time.Time, deadlineAt *time.Time) ([]JournalEntry, error) {
	keyScope := ""
	if request.KeyScope != KeyScopePermanent {
		keyScope = request.KeyScope
	}
	deadlineMilliseconds, err := durable.ExactMilliseconds("execution deadline", request.Deadline.Duration)
	if err != nil {
		return nil, err
	}
	body := journalcodec.ExecutionStartedBody{
		V: 1, ExecutionID: request.ID.String(),
		DefinitionName: request.DefinitionName, DefinitionVersion: request.DefinitionVersion,
		ExecutionKey: request.Key, KeyScope: keyScope,
		Input: json.RawMessage(request.Input.BytesCopy()), FailFast: request.FailFast,
		DeadlineMode: request.Deadline.Mode, DeadlineDuration: deadlineMilliseconds,
		DeadlineAt: clonePointer(deadlineAt), MaxCommands: request.MaxCommands,
		Metadata: json.RawMessage(request.Metadata.BytesCopy()),
	}
	start, err := NewJournalEntry(ExecutionStarted, body)
	if err != nil {
		return nil, fmt.Errorf("encode execution start: %w", err)
	}
	entries := []JournalEntry{start}
	nextAttemptAt, err := durable.AddExactDuration("initial delay", dbNow, request.Root.InitialDelay)
	if err != nil {
		return nil, err
	}
	created, err := commandCreatedEntry(*request.Root, dbNow, nextAttemptAt)
	if err != nil {
		return nil, err
	}
	zero := 0
	created.CausationBatchIndex = &zero
	entries = append(entries, created)
	return entries, nil
}

func commandCreatedEntry(command CommandCreate, budgetStartedAt, nextAttemptAt time.Time) (JournalEntry, error) {
	if err := validateCommandCreate(command); err != nil {
		return JournalEntry{}, err
	}
	var timeoutMS *int64
	if command.AttemptTimeout > 0 {
		value, err := durable.ExactMilliseconds("attempt timeout", command.AttemptTimeout)
		if err != nil {
			return JournalEntry{}, err
		}
		timeoutMS = &value
	}
	var initialDelayMS *int64
	if command.InitialDelay > 0 {
		value, err := durable.ExactMilliseconds("initial delay", command.InitialDelay)
		if err != nil {
			return JournalEntry{}, err
		}
		initialDelayMS = &value
	}
	initialState := "ready"
	var recordedBudget, recordedNext *time.Time
	if len(command.Waits) > 0 {
		initialState = "pending"
	} else {
		recordedBudget, recordedNext = &budgetStartedAt, &nextAttemptAt
	}
	body := journalcodec.CommandCreatedBody{
		V: 1, CommandID: command.ID.String(), CommandKey: command.Key, Name: command.Name,
		Version: command.Version, Args: json.RawMessage(command.Args.BytesCopy()),
		Required: command.Required, InitialState: initialState,
		Queue: command.Queue, AttemptTimeoutMS: timeoutMS,
		RetryPolicy:    json.RawMessage(command.RetryPolicy.BytesCopy()),
		InitialDelayMS: initialDelayMS, BudgetStartedAt: recordedBudget, NextAttemptAt: recordedNext,
		DeclarationFingerprint: hex.EncodeToString(command.DeclarationFingerprint[:]),
	}
	for _, wait := range command.Waits {
		body.Waits = append(body.Waits, journalcodec.EventWaitBody{Name: wait.Name, Key: wait.Key})
	}
	if command.Within > 0 {
		value, err := durable.ExactMilliseconds("wait timeout", command.Within)
		if err != nil {
			return JournalEntry{}, err
		}
		body.WithinMS = &value
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
	if err := validateCommandCreate(command); err != nil {
		return err
	}
	var timeoutMS *int64
	if command.AttemptTimeout > 0 {
		value, err := durable.ExactMilliseconds("attempt timeout", command.AttemptTimeout)
		if err != nil {
			return err
		}
		timeoutMS = &value
	}
	var initialDelayMS *int64
	if command.InitialDelay > 0 {
		value, err := durable.ExactMilliseconds("initial delay", command.InitialDelay)
		if err != nil {
			return err
		}
		initialDelayMS = &value
	}
	state := "ready"
	unsatisfiedWaits := 0
	waitPositions := make(map[int]int64, len(command.Waits))
	for index, wait := range command.Waits {
		var position int64
		err := tx.QueryRow(ctx, `SELECT position FROM `+pgschema.Table(s.schema, "flow_journal")+`
			WHERE execution_id=$1 AND event_namespace='application' AND event_name=$2 AND event_key=$3
			ORDER BY position LIMIT 1`, executionID, wait.Name, wait.Key).Scan(&position)
		if errors.Is(err, pgx.ErrNoRows) {
			unsatisfiedWaits++
			continue
		}
		if err != nil {
			return MapError("find retained event for command wait", err)
		}
		waitPositions[index] = position
	}
	if unsatisfiedWaits > 0 {
		state = "pending"
	}
	var acceptedBudget, acceptedNext *time.Time
	if state == "ready" {
		acceptedBudget, acceptedNext = &budgetStartedAt, &nextAttemptAt
	}
	var withinMS *int64
	if command.Within > 0 {
		value, err := durable.ExactMilliseconds("wait timeout", command.Within)
		if err != nil {
			return err
		}
		withinMS = &value
	}
	_, err := tx.Exec(ctx, `INSERT INTO `+pgschema.Table(s.schema, "flow_commands")+` (
		command_id,execution_id,command_key,name,version,parent_command_id,required,
		args,declaration_fingerprint,state,unsatisfied_waits,
		queue,attempt_timeout_ms,retry_policy,initial_delay_ms,
		budget_started_at,next_attempt_at,wait_timeout_ms,created_position,created_at,updated_at,status_at
		) VALUES ($1,$2,$3,$4,$5,$6,$7,$8,$9,$10,$11,$12,$13,$14,$15,$16,$17,$18,$19,$20,$20,$20)`,
		command.ID, executionID, command.Key, command.Name, command.Version,
		command.ParentCommandID, command.Required, command.Args.Bytes,
		command.DeclarationFingerprint[:], state, unsatisfiedWaits, command.Queue, timeoutMS,
		command.RetryPolicy.Bytes, initialDelayMS,
		acceptedBudget, acceptedNext, withinMS, createdPosition, budgetStartedAt,
	)
	if err != nil {
		return MapError("insert command", err)
	}
	if state == "pending" && unsatisfiedWaits > 0 {
		var deadline *time.Time
		if command.Within > 0 {
			value, addErr := durable.AddExactDuration("wait timeout", budgetStartedAt, command.Within)
			if addErr != nil {
				return addErr
			}
			var executionDeadline *time.Time
			if err := tx.QueryRow(ctx, `SELECT deadline_at FROM `+pgschema.Table(s.schema, "flow_executions")+`
				WHERE execution_id=$1`, executionID).Scan(&executionDeadline); err != nil {
				return MapError("load execution deadline for initial wait", err)
			}
			if executionDeadline != nil && executionDeadline.Before(value) {
				value = *executionDeadline
			}
			deadline = &value
		}
		if _, err := tx.Exec(ctx, `UPDATE `+pgschema.Table(s.schema, "flow_commands")+`
			SET wait_started_at=$2,wait_deadline_at=$3 WHERE command_id=$1`, command.ID, budgetStartedAt, deadline); err != nil {
			return MapError("start initial command wait", err)
		}
	}
	if state == "ready" {
		_, err = tx.Exec(ctx, `INSERT INTO `+pgschema.Table(s.schema, "flow_command_queue")+`
			(command_id,execution_id,queue,name,version,state,next_run_at)
			VALUES ($1,$2,$3,$4,$5,'ready',$6)`,
			command.ID, executionID, command.Queue, command.Name, command.Version, nextAttemptAt)
		if err != nil {
			return MapError("enqueue command", err)
		}
	}
	for index, wait := range command.Waits {
		var satisfied *int64
		if position, ok := waitPositions[index]; ok {
			satisfied = &position
		}
		if _, err := tx.Exec(ctx, `INSERT INTO `+pgschema.Table(s.schema, "flow_command_event_waits")+`
			(command_id,execution_id,event_name,event_key,satisfied_position)
			VALUES ($1,$2,$3,$4,$5)`, command.ID, executionID, wait.Name, wait.Key, satisfied); err != nil {
			return MapError("insert command event wait", err)
		}
	}
	return nil
}

type ExecutionHead struct {
	ID           uuid.UUID
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
	err := semantic.PGX().QueryRow(ctx, `SELECT execution_id,status,fail_fast,max_commands,command_count,open_commands
		FROM `+pgschema.Table(s.schema, "flow_executions")+` WHERE execution_id=$1`, semantic.ExecutionID()).
		Scan(&result.ID, &result.Status, &result.FailFast, &result.MaxCommands, &result.CommandCount, &result.OpenCommands)
	if err != nil {
		return ExecutionHead{}, MapError("load execution", err)
	}
	return result, nil
}

type ApplicationEvent struct {
	ID   uuid.UUID
	Name string
	Key  string
	Body canonical.Value
}

func (s *Store) coalesceApplicationEvents(ctx context.Context, semantic *SemanticTx, events []ApplicationEvent) ([]ApplicationEvent, error) {
	if len(events) == 0 {
		return nil, nil
	}
	ordered := append([]ApplicationEvent(nil), events...)
	sort.Slice(ordered, func(i, j int) bool {
		if ordered[i].Name != ordered[j].Name {
			return ordered[i].Name < ordered[j].Name
		}
		return ordered[i].Key < ordered[j].Key
	})
	result := make([]ApplicationEvent, 0, len(ordered))
	seen := make(map[string]ApplicationEvent, len(ordered))
	for _, event := range ordered {
		if event.ID == uuid.Nil || event.Name == "" || event.Key == "" || len(event.Body.Bytes) == 0 {
			return nil, fmt.Errorf("%w: incomplete staged application event", flowerr.ErrInvalid)
		}
		identity := event.Name + "\x00" + event.Key
		if prior, exists := seen[identity]; exists {
			if bytes.Equal(prior.Body.Bytes, event.Body.Bytes) {
				continue
			}
			return nil, fmt.Errorf("%w: staged application event identity differs", flowerr.ErrConflict)
		}
		seen[identity] = event
		existing, err := s.LookupApplicationEvent(ctx, semantic.PGX(), semantic.ExecutionID(), event.Name, event.Key)
		if err != nil {
			return nil, err
		}
		if existing.Found {
			if bytes.Equal(existing.Body, event.Body.Bytes) {
				continue
			}
			return nil, fmt.Errorf("%w: application event identity differs", flowerr.ErrConflict)
		}
		result = append(result, event)
	}
	return result, nil
}

type ExistingEvent struct {
	ID    uuid.UUID
	Body  []byte
	Found bool
}

func (s *Store) LookupApplicationEvent(ctx context.Context, tx pgx.Tx, executionID uuid.UUID, name, key string) (ExistingEvent, error) {
	var row pgx.Row
	query := `SELECT event_id,body FROM ` + pgschema.Table(s.schema, "flow_journal") + `
		WHERE execution_id=$1 AND event_namespace='application' AND event_name=$2 AND event_key=$3`
	if tx != nil {
		row = tx.QueryRow(ctx, query, executionID, name, key)
	} else {
		row = s.db.Conn.QueryRow(ctx, query, executionID, name, key)
	}
	var result ExistingEvent
	if err := row.Scan(&result.ID, &result.Body); err != nil {
		if errors.Is(err, pgx.ErrNoRows) {
			return ExistingEvent{}, nil
		}
		return ExistingEvent{}, MapError("lookup application event", err)
	}
	result.Found = true
	return result, nil
}

func (s *Store) EmitLocked(ctx context.Context, semantic *SemanticTx, event ApplicationEvent) (bool, error) {
	existing, err := s.LookupApplicationEvent(ctx, semantic.PGX(), semantic.ExecutionID(), event.Name, event.Key)
	if err != nil {
		return false, err
	}
	if existing.Found {
		if bytes.Equal(existing.Body, event.Body.Bytes) {
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
		EventKey:   clonePointer(&event.Key),
		EventClass: stringPointer("application"), Body: event.Body,
	}
	journal, err := semantic.Apply(ctx, PersistedChangeSet{Journal: []JournalEntry{entry}})
	if err != nil {
		return false, err
	}
	waits, err := s.matchingWaitsLocked(ctx, semantic, event.Name, event.Key, journal.Journal[0].Position)
	if err != nil {
		return false, err
	}
	resolution, err := s.resolveReadinessLocked(ctx, semantic, nil, waits)
	if err != nil {
		return false, err
	}
	if err := s.applyReadinessResolution(ctx, semantic, resolution); err != nil {
		return false, err
	}
	return true, nil
}

type CancelResult struct {
	Created bool
}

type terminalFailure = failure.Value

type activeCommandAttempt struct {
	ID               uuid.UUID
	StartedPosition  int64
	Ordinal          int
	ConsumedAttempts int
}

func (s *Store) lockActiveCommandAttempt(
	ctx context.Context,
	semantic *SemanticTx,
	commandID uuid.UUID,
	state string,
) (*activeCommandAttempt, error) {
	if state != "running" {
		return nil, nil
	}
	var attempt activeCommandAttempt
	err := semantic.PGX().QueryRow(ctx, `SELECT q.active_attempt_id,c.attempt_ordinal,c.consumed_attempts,
		(SELECT position FROM `+pgschema.Table(s.schema, "flow_journal")+`
		 WHERE execution_id=c.execution_id AND attempt_id=q.active_attempt_id AND entry_kind='attempt_started')
		FROM `+pgschema.Table(s.schema, "flow_command_queue")+` q
		JOIN `+pgschema.Table(s.schema, "flow_commands")+` c ON c.command_id=q.command_id
		WHERE q.command_id=$1 AND q.execution_id=$2 FOR UPDATE OF q`, commandID, semantic.ExecutionID()).
		Scan(&attempt.ID, &attempt.Ordinal, &attempt.ConsumedAttempts, &attempt.StartedPosition)
	if err != nil {
		return nil, MapError("lock active command attempt", err)
	}
	return &attempt, nil
}

func cancelledAttemptEvent(
	commandID uuid.UUID,
	key string,
	attempt activeCommandAttempt,
	code string,
	reason string,
	dbNow time.Time,
) (JournalEntry, error) {
	entry, err := NewJournalEntry(AttemptConcluded, journalcodec.AttemptConcludedBody{
		V: 1, AttemptID: attempt.ID.String(), CommandID: commandID.String(), CommandKey: key,
		Attempt: attempt.Ordinal, Classification: "cancelled", ConsumedBudget: false,
		ConsumedAttempts: attempt.ConsumedAttempts, FinishedAt: dbNow, ErrorCode: code, ErrorMessage: reason,
	})
	if err != nil {
		return JournalEntry{}, err
	}
	entry.CommandID = clonePointer(&commandID)
	entry.AttemptID = clonePointer(&attempt.ID)
	entry.CausationPosition = clonePointer(&attempt.StartedPosition)
	return entry, nil
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
	activeAttempt, err := s.lockActiveCommandAttempt(ctx, semantic, commandID, state)
	if err != nil {
		return CancelResult{}, err
	}

	commandEvent, err := terminalEvent(commandID, key, "cancelled", reason, "flow.command_cancelled", "command_terminal")
	if err != nil {
		return CancelResult{}, err
	}
	entries := make([]JournalEntry, 0, 4)
	if activeAttempt != nil {
		concluded, err := cancelledAttemptEvent(commandID, key, *activeAttempt, "cancelled", reason, semantic.DBNow())
		if err != nil {
			return CancelResult{}, err
		}
		entries = append(entries, concluded)
		zero := 0
		commandEvent.CausationBatchIndex = &zero
	}
	terminalIndex := len(entries)
	entries = append(entries, commandEvent)
	resolution := readinessResolution{}
	failureEffects := failureResolution{}
	if required {
		failureEffects, err = s.resolveRequiredFailureLocked(ctx, semantic, commandID, "cancelled", head.FailFast)
		resolution = failureEffects.readinessResolution
	} else {
		resolution, err = s.resolveReadinessLocked(ctx, semantic, map[uuid.UUID]string{commandID: "cancelled"}, nil)
	}
	if err != nil {
		return CancelResult{}, err
	}
	becameFailing := required && head.Status == "running"
	if becameFailing {
		survivors := make([]string, len(failureEffects.survivors))
		for index, command := range failureEffects.survivors {
			survivors[index] = command.key
		}
		failing, err := NewJournalEntry(ExecutionFailing, map[string]any{
			"v": 1, "status": "failing", "reason": reason, "command_key": key,
			"fail_fast": head.FailFast, "survivors": survivors,
		})
		if err != nil {
			return CancelResult{}, err
		}
		failing.CausationBatchIndex = clonePointer(&terminalIndex)
		entries = append(entries, failing)
	}
	cancelledOffset := len(entries)
	cancelledEntries, err := failureEffects.cancellationEntries(terminalIndex, "cancelled by fail-fast after required command cancellation")
	if err != nil {
		return CancelResult{}, err
	}
	entries = append(entries, cancelledEntries...)
	effectiveOpen, err := durable.AddPostgresInteger("execution open commands", head.OpenCommands,
		-1, 0, durable.PostgresIntegerMax)
	if err != nil {
		return CancelResult{}, err
	}
	effectiveOpen, err = durable.AddPostgresInteger("execution open commands", effectiveOpen,
		-len(failureEffects.cancelled), 0, durable.PostgresIntegerMax)
	if err != nil {
		return CancelResult{}, err
	}
	executionFailed := required || head.Status == "failing"
	terminalExecution := effectiveOpen == 0
	if terminalExecution {
		status, eventName, terminalReason := "succeeded", "flow.execution_succeeded", ""
		if executionFailed {
			status, eventName, terminalReason = "failed", "flow.execution_failed", reason
		}
		executionEvent, err := executionTerminalEvent(status, terminalReason, eventName)
		if err != nil {
			return CancelResult{}, err
		}
		executionEvent.CausationBatchIndex = clonePointer(&terminalIndex)
		entries = append(entries, executionEvent)
	}
	journal, err := semantic.Apply(ctx, PersistedChangeSet{Journal: entries})
	if err != nil {
		return CancelResult{}, err
	}
	failure := terminalFailure{Code: "cancelled", Message: reason}
	if _, err := semantic.PGX().Exec(ctx, `UPDATE `+pgschema.Table(s.schema, "flow_commands")+`
		SET state='cancelled',terminal_failure=$2::jsonb,terminal_position=$3,finished_at=$4,updated_at=$4,status_at=$4
		WHERE command_id=$1`, commandID, jsonString(failure), journal.Journal[terminalIndex].Position, semantic.DBNow()); err != nil {
		return CancelResult{}, MapError("cancel command", err)
	}
	if _, err := semantic.PGX().Exec(ctx, `DELETE FROM `+pgschema.Table(s.schema, "flow_command_queue")+` WHERE command_id=$1`, commandID); err != nil {
		return CancelResult{}, MapError("remove cancelled command delivery", err)
	}
	if required {
		if err := s.applyFailureResolution(ctx, semantic, failureEffects, journal, 0, cancelledOffset,
			"cancelled by fail-fast after required command cancellation"); err != nil {
			return CancelResult{}, err
		}
	} else if err := s.applyReadinessResolution(ctx, semantic, resolution); err != nil {
		return CancelResult{}, err
	}

	if terminalExecution {
		status := "succeeded"
		if executionFailed {
			status = "failed"
		}
		if _, err := semantic.PGX().Exec(ctx, `UPDATE `+pgschema.Table(s.schema, "flow_executions")+`
			SET status=$4,open_commands=0,failure=CASE WHEN $5 THEN $2::jsonb ELSE failure END,
			    finished_at=$3,updated_at=$3,status_at=$3
			WHERE execution_id=$1`, head.ID, jsonString(terminalFailure{Code: "command_cancelled", Message: reason}), semantic.DBNow(), status, executionFailed); err != nil {
			return CancelResult{}, MapError("fail execution after command cancellation", err)
		}
	} else {
		status := head.Status
		if becameFailing {
			status = "failing"
		}
		if _, err := semantic.PGX().Exec(ctx, `UPDATE `+pgschema.Table(s.schema, "flow_executions")+`
			SET status=$2,open_commands=$4,updated_at=$3,status_at=CASE WHEN status<>$2 THEN $3 ELSE status_at END
			WHERE execution_id=$1`, head.ID, status, semantic.DBNow(), effectiveOpen); err != nil {
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

	rows, err := semantic.PGX().Query(ctx, `SELECT command_id,command_key,state FROM `+
		pgschema.Table(s.schema, "flow_commands")+`
		WHERE execution_id=$1 AND state NOT IN ('succeeded','failed','cancelled','expired')
		ORDER BY command_id FOR UPDATE`, head.ID)
	if err != nil {
		return CancelResult{}, MapError("lock execution commands for cancellation", err)
	}
	type command struct {
		id      uuid.UUID
		key     string
		state   string
		attempt *activeCommandAttempt
	}
	commands := make([]command, 0, head.OpenCommands)
	for rows.Next() {
		var item command
		if err := rows.Scan(&item.id, &item.key, &item.state); err != nil {
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
	for index := range commands {
		attempt, err := s.lockActiveCommandAttempt(ctx, semantic, commands[index].id, commands[index].state)
		if err != nil {
			return CancelResult{}, err
		}
		commands[index].attempt = attempt
	}

	entries := make([]JournalEntry, 0, len(commands)*2+1)
	terminalBatchIndex := make(map[uuid.UUID]int, len(commands))
	for _, command := range commands {
		if command.attempt != nil {
			concluded, err := cancelledAttemptEvent(command.id, command.key, *command.attempt,
				"execution_cancelled", reason, semantic.DBNow())
			if err != nil {
				return CancelResult{}, err
			}
			entries = append(entries, concluded)
		}
		entry, err := terminalEvent(command.id, command.key, "cancelled", reason, "flow.command_cancelled", "command_terminal")
		if err != nil {
			return CancelResult{}, err
		}
		if command.attempt != nil {
			index := len(entries) - 1
			entry.CausationBatchIndex = &index
		}
		terminalBatchIndex[command.id] = len(entries)
		entries = append(entries, entry)
	}
	executionEvent, err := executionTerminalEvent("cancelled", reason, "flow.execution_cancelled")
	if err != nil {
		return CancelResult{}, err
	}
	if len(entries) > 0 {
		index := len(entries) - 1
		executionEvent.CausationBatchIndex = &index
	}
	entries = append(entries, executionEvent)
	journal, err := semantic.Apply(ctx, PersistedChangeSet{Journal: entries})
	if err != nil {
		return CancelResult{}, err
	}
	failure := jsonString(terminalFailure{Code: "cancelled", Message: reason})
	for _, command := range commands {
		if _, err := semantic.PGX().Exec(ctx, `UPDATE `+pgschema.Table(s.schema, "flow_commands")+`
			SET state='cancelled',terminal_failure=$2::jsonb,terminal_position=$3,finished_at=$4,updated_at=$4,status_at=$4
			WHERE command_id=$1`, command.id, failure, journal.Journal[terminalBatchIndex[command.id]].Position, semantic.DBNow()); err != nil {
			return CancelResult{}, MapError("cancel execution command", err)
		}
	}
	if _, err := semantic.PGX().Exec(ctx, `DELETE FROM `+pgschema.Table(s.schema, "flow_command_queue")+` WHERE execution_id=$1`, head.ID); err != nil {
		return CancelResult{}, MapError("remove execution deliveries", err)
	}
	if _, err := semantic.PGX().Exec(ctx, `UPDATE `+pgschema.Table(s.schema, "flow_executions")+`
		SET status='cancelled',open_commands=0,failure=$2::jsonb,finished_at=$3,updated_at=$3,status_at=$3
		WHERE execution_id=$1`, head.ID, failure, semantic.DBNow()); err != nil {
		return CancelResult{}, MapError("cancel execution", err)
	}
	return CancelResult{Created: true}, nil
}

func terminalEvent(commandID uuid.UUID, key, status, reason, name, class string) (JournalEntry, error) {
	return terminalEventWithCode(commandID, key, status, status, reason, name, class)
}

func terminalEventWithCode(commandID uuid.UUID, key, status, code, reason, name, class string) (JournalEntry, error) {
	body, err := NewJournalEntry(EventRecorded, journalcodec.TerminalEventBody{
		V: 1, Status: status, Code: code, Reason: reason, CommandKey: key,
	})
	if err != nil {
		return JournalEntry{}, err
	}
	eventID := uuid.New()
	body.CommandID = clonePointer(&commandID)
	body.EventID = &eventID
	body.EventNamespace = stringPointer("runtime")
	body.EventName = clonePointer(&name)
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
	body.EventID = &eventID
	body.EventNamespace = stringPointer("runtime")
	body.EventName = clonePointer(&name)
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
	case "succeeded", "failed", "cancelled", "expired":
		return true
	default:
		return false
	}
}

func deadlineAt(now time.Time, spec DeadlineSpec) (*time.Time, error) {
	if spec.Mode == "none" {
		return nil, nil
	}
	value, err := durable.AddExactDuration("execution deadline", now, spec.Duration)
	if err != nil {
		return nil, err
	}
	return &value, nil
}

func validateStartRequest(request StartRequest) error {
	if request.ID == uuid.Nil || request.DefinitionName == "" || request.DefinitionVersion <= 0 ||
		request.MaxCommands < 0 || len(request.Input.Bytes) == 0 || len(request.Metadata.Bytes) == 0 {
		return fmt.Errorf("%w: incomplete execution start", flowerr.ErrInvalid)
	}
	if request.KeyScope != "" && request.KeyScope != KeyScopePermanent && request.KeyScope != KeyScopeLive {
		return fmt.Errorf("%w: invalid execution key scope", flowerr.ErrInvalid)
	}
	if request.KeyScope == KeyScopeLive && request.Key == "" {
		return fmt.Errorf("%w: live key scope requires an execution key", flowerr.ErrInvalid)
	}
	if request.Deadline.Mode != "none" && request.Deadline.Mode != "duration" {
		return fmt.Errorf("%w: invalid deadline mode", flowerr.ErrInvalid)
	}
	if request.Deadline.Mode == "duration" && request.Deadline.Duration <= 0 {
		return fmt.Errorf("%w: invalid execution deadline", flowerr.ErrInvalid)
	}
	if request.Deadline.Mode == "duration" {
		if _, err := durable.ExactMilliseconds("execution deadline", request.Deadline.Duration); err != nil {
			return err
		}
	}
	if err := durable.PostgresInteger("definition version", request.DefinitionVersion, 1, durable.PostgresIntegerMax); err != nil {
		return err
	}
	if err := durable.PostgresInteger("maximum commands", request.MaxCommands, 0, durable.PostgresIntegerMax); err != nil {
		return err
	}
	if request.Root == nil {
		return fmt.Errorf("%w: command root is required", flowerr.ErrInvalid)
	}
	if len(request.Root.Waits) > MaxCommandEventWaits {
		return fmt.Errorf("%w: command exceeds event-wait limit", flowerr.ErrInvalid)
	}
	if err := validateCommandCreate(*request.Root); err != nil {
		return err
	}
	return nil
}

func validateCommandCreate(command CommandCreate) error {
	if command.ID == uuid.Nil || command.Key == "" || command.Name == "" || command.Queue == "" ||
		len(command.Args.Bytes) == 0 || len(command.RetryPolicy.Bytes) == 0 {
		return fmt.Errorf("%w: incomplete command creation", flowerr.ErrInvalid)
	}
	if err := durable.PostgresInteger("command version", command.Version, 1, durable.PostgresIntegerMax); err != nil {
		return err
	}
	if err := durable.PostgresInteger("unsatisfied waits", len(command.Waits), 0, durable.PostgresIntegerMax); err != nil {
		return err
	}
	for field, value := range map[string]time.Duration{
		"attempt timeout": command.AttemptTimeout,
		"initial delay":   command.InitialDelay,
		"wait timeout":    command.Within,
	} {
		if _, err := durable.ExactMilliseconds(field, value); err != nil {
			return err
		}
	}
	if _, err := retrypolicy.PublicFromCanonical(command.RetryPolicy.Bytes); err != nil {
		return fmt.Errorf("%w: retry policy is invalid", flowerr.ErrInvalid)
	}
	return nil
}

func stringPointer(value string) *string { return &value }
