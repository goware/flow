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

	"github.com/goware/flow/internal/canonical"
	"github.com/goware/flow/internal/durable"
	"github.com/goware/flow/internal/failure"
	"github.com/goware/flow/internal/flowerr"
	"github.com/goware/flow/internal/pgschema"
	retrypolicy "github.com/goware/flow/internal/retry"
	"github.com/goware/flow/internal/store/journalcodec"
	"github.com/goware/flow/internal/uuid"
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
	Queue                  string
	AttemptTimeout         time.Duration
	RecoveryLease          time.Duration
	RetryPolicy            canonical.Value
	InitialDelay           time.Duration
	Waits                  []EventWaitCreate
	Within                 time.Duration
}

type EventWaitCreate struct {
	Name string
	Key  string
}

// Run key scopes. Permanent keys identify one run forever and
// rediscover it idempotently; live keys are held only while their run
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
	Deadline          DeadlineSpec
	MaxCommands       int
	Root              *CommandCreate
}

// StartResult carries the accepted run identity. Created is false when an
// idempotent start rediscovered an existing run.
type StartResult struct {
	ID      uuid.UUID
	Created bool
}

type ReplaceRunRequest struct {
	Expected uuid.UUID
	Start    StartRequest
	Reason   string
}

type ReplaceRunResult struct {
	Start    StartResult
	Replaced bool
}

// ReplaceCurrentRunInTx atomically cancels the exact expected live-key run and
// starts its successor. The expected predecessor is the serialization anchor:
// a stale expected ID can rediscover an equivalent current successor but can
// never mutate it.
func (s *Store) ReplaceCurrentRunInTx(
	ctx context.Context,
	tx pgx.Tx,
	request ReplaceRunRequest,
	order *LockOrder,
) (ReplaceRunResult, error) {
	if tx == nil || request.Expected == uuid.Nil || request.Reason == "" {
		return ReplaceRunResult{}, fmt.Errorf("%w: incomplete current-run replacement", flowerr.ErrInvalid)
	}
	if err := validateStartRequest(request.Start); err != nil {
		return ReplaceRunResult{}, err
	}
	if request.Start.Key == "" || request.Start.KeyScope != KeyScopeLive || request.Start.Root == nil {
		return ReplaceRunResult{}, fmt.Errorf("%w: replacement requires a live-key root", flowerr.ErrInvalid)
	}
	if request.Start.ID == request.Expected {
		return ReplaceRunResult{}, fmt.Errorf("%w: replacement run must have a distinct identity", flowerr.ErrInvalid)
	}
	if order != nil {
		if err := order.BeforeRun(request.Expected); err != nil {
			return ReplaceRunResult{}, err
		}
	}
	semantic, err := s.AttachSemantic(ctx, tx, request.Expected, LockBlocking)
	if err != nil {
		if errors.Is(err, flowerr.ErrNotFound) {
			return ReplaceRunResult{}, fmt.Errorf("%w: expected run does not exist", flowerr.ErrInvalidState)
		}
		return ReplaceRunResult{}, err
	}

	var currentID uuid.UUID
	var currentFingerprint []byte
	err = tx.QueryRow(ctx, `SELECT run_id,start_fingerprint
		FROM `+pgschema.Table(s.schema, "flow_runs")+`
		WHERE definition_name=$1 AND run_key=$2 AND key_scope='live'
		  AND status IN ('running','failing')`, request.Start.DefinitionName, request.Start.Key).
		Scan(&currentID, &currentFingerprint)
	if errors.Is(err, pgx.ErrNoRows) {
		return ReplaceRunResult{}, fmt.Errorf("%w: no current live run", flowerr.ErrConflict)
	}
	if err != nil {
		return ReplaceRunResult{}, MapError("load current run for replacement", err)
	}
	if currentID != request.Expected {
		if !bytes.Equal(currentFingerprint, request.Start.StartFingerprint[:]) {
			return ReplaceRunResult{}, fmt.Errorf("%w: current run declaration differs", flowerr.ErrConflict)
		}
		return ReplaceRunResult{Start: StartResult{ID: currentID}}, nil
	}

	cancelled, err := s.CancelRunLocked(ctx, semantic, request.Reason)
	if err != nil {
		return ReplaceRunResult{}, err
	}
	if !cancelled.Created {
		return ReplaceRunResult{}, fmt.Errorf("%w: expected run was not cancelled", flowerr.ErrInvalidState)
	}
	started, err := s.StartInTx(ctx, tx, request.Start, order)
	if err != nil {
		return ReplaceRunResult{}, err
	}
	if !started.Created || started.ID == request.Expected {
		return ReplaceRunResult{}, fmt.Errorf("%w: replacement run was not created", flowerr.ErrInvalidState)
	}
	return ReplaceRunResult{Start: started, Replaced: true}, nil
}

// StartInTx creates or idempotently rediscovers a run inside tx. It
// never commits or rolls back tx.
func (s *Store) StartInTx(ctx context.Context, tx pgx.Tx, request StartRequest, order *LockOrder) (StartResult, error) {
	if tx == nil {
		return StartResult{}, fmt.Errorf("%w: transaction is nil", flowerr.ErrInvalid)
	}
	if err := validateStartRequest(request); err != nil {
		return StartResult{}, err
	}
	if order != nil {
		if err := order.BeforeFlowOperation(); err != nil {
			return StartResult{}, err
		}
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
		result, retry, err := s.startAttempt(ctx, tx, request, order)
		if err == nil || !retry || attempt+1 >= attempts {
			return result, err
		}
	}
}

func (s *Store) startAttempt(ctx context.Context, tx pgx.Tx, request StartRequest, order *LockOrder) (StartResult, bool, error) {
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
	err = tx.QueryRow(ctx, `INSERT INTO `+pgschema.Table(s.schema, "flow_runs")+` (
		run_id,definition_name,definition_version,run_key,key_scope,status,
		start_fingerprint,
		deadline_at,max_commands,command_count,open_commands,
		next_journal_position,root_command_id,created_at,updated_at,status_at
	) VALUES ($1,$2,$3,$4,$5,'running',$6,$7,$8,$9,$9,1,$10,$11,$11,$11)
	ON CONFLICT DO NOTHING RETURNING run_id`,
		request.ID, request.DefinitionName, request.DefinitionVersion, request.Key, request.KeyScope,
		request.StartFingerprint[:], deadlineAt, request.MaxCommands, commandCount, rootID, dbNow,
	).Scan(&inserted)
	if err != nil && !errors.Is(err, pgx.ErrNoRows) {
		return StartResult{}, false, MapError("insert run", err)
	}
	if errors.Is(err, pgx.ErrNoRows) {
		result, retry, loadErr := s.loadEquivalentStart(ctx, tx, request, order)
		return result, retry, loadErr
	}
	if inserted != request.ID {
		return StartResult{}, false, fmt.Errorf("%w: inserted run identity differs", flowerr.ErrInvalidState)
	}
	if order != nil {
		if err := order.RegisterOwnedRun(request.ID); err != nil {
			return StartResult{}, false, err
		}
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
	rootReady, err := s.insertCommand(ctx, tx, request.ID, *request.Root, journal.Journal[1].Position,
		dbNow, dbNow, nextAttemptAt, deadlineAt)
	if err != nil {
		return StartResult{}, false, err
	}
	if rootReady && !nextAttemptAt.After(dbNow) {
		if err := semantic.NotifyRunnableCommands(ctx); err != nil {
			return StartResult{}, false, err
		}
	}
	return StartResult{ID: request.ID, Created: true}, false, nil
}

// loadEquivalentStart resolves an insert that conflicted on the idempotency
// index. Permanent keys rediscover the one run that owns the key and
// require an equivalent start identity. Live keys rediscover whichever live
// run currently holds the key, with no identity comparison — the caller
// asked to ensure a live run exists, not to describe a specific one.
// retry=true means the conflicting live holder settled before the lookup;
// the caller may attempt the insert again.
func (s *Store) loadEquivalentStart(
	ctx context.Context,
	tx pgx.Tx,
	request StartRequest,
	order *LockOrder,
) (StartResult, bool, error) {
	if request.Key == "" {
		return StartResult{}, false, fmt.Errorf("%w: unkeyed start unexpectedly conflicted", flowerr.ErrInvalidState)
	}
	if request.KeyScope == KeyScopeLive {
		var id uuid.UUID
		err := tx.QueryRow(ctx, `SELECT run_id
			FROM `+pgschema.Table(s.schema, "flow_runs")+`
			WHERE definition_name=$1 AND run_key=$2
			  AND key_scope=$3 AND status IN ('running','failing')`,
			request.DefinitionName, request.Key, KeyScopeLive,
		).Scan(&id)
		if errors.Is(err, pgx.ErrNoRows) {
			return StartResult{}, true, fmt.Errorf("%w: live key holder settled during start", flowerr.ErrConflict)
		}
		if err != nil {
			return StartResult{}, false, MapError("resolve live run", err)
		}
		if order != nil {
			if err := order.BeforeRun(id); err != nil {
				return StartResult{}, false, err
			}
		}
		var definitionName, runKey, keyScope, status string
		err = tx.QueryRow(ctx, `SELECT definition_name,run_key,key_scope,status
			FROM `+pgschema.Table(s.schema, "flow_runs")+`
			WHERE run_id=$1 FOR UPDATE`, id).
			Scan(&definitionName, &runKey, &keyScope, &status)
		if errors.Is(err, pgx.ErrNoRows) {
			return StartResult{}, true, fmt.Errorf("%w: live key holder changed during start", flowerr.ErrConflict)
		}
		if err != nil {
			return StartResult{}, false, MapError("lock live run", err)
		}
		if definitionName != request.DefinitionName || runKey != request.Key || keyScope != KeyScopeLive ||
			status != "running" && status != "failing" {
			return StartResult{}, true, fmt.Errorf("%w: live key holder changed during start", flowerr.ErrConflict)
		}
		return StartResult{ID: id, Created: false}, false, nil
	}
	var id uuid.UUID
	err := tx.QueryRow(ctx, `SELECT run_id
		FROM `+pgschema.Table(s.schema, "flow_runs")+`
		WHERE definition_name=$1 AND run_key=$2 AND key_scope=$3`,
		request.DefinitionName, request.Key, KeyScopePermanent,
	).Scan(&id)
	if err != nil {
		return StartResult{}, false, MapError("resolve existing run", err)
	}
	if order != nil {
		if err := order.BeforeRun(id); err != nil {
			return StartResult{}, false, err
		}
	}
	var definitionName, runKey, keyScope string
	var version int
	var fingerprint, input []byte
	err = tx.QueryRow(ctx, `WITH locked_run AS MATERIALIZED (
		SELECT definition_name,run_key,key_scope,definition_version,start_fingerprint,root_command_id
		FROM `+pgschema.Table(s.schema, "flow_runs")+`
		WHERE run_id=$1 FOR UPDATE
	)
	SELECT r.definition_name,r.run_key,r.key_scope,r.definition_version,r.start_fingerprint,c.args
	FROM locked_run r
	JOIN `+pgschema.Table(s.schema, "flow_commands")+` c
	  ON c.run_id=$1 AND c.command_id=r.root_command_id
	FOR UPDATE OF c`, id).
		Scan(&definitionName, &runKey, &keyScope, &version, &fingerprint, &input)
	if err != nil {
		return StartResult{}, false, MapError("lock existing run", err)
	}
	if definitionName != request.DefinitionName || runKey != request.Key || keyScope != KeyScopePermanent {
		return StartResult{}, false, fmt.Errorf("%w: run key owner changed during start", flowerr.ErrConflict)
	}
	if version != request.DefinitionVersion || !bytes.Equal(fingerprint, request.StartFingerprint[:]) ||
		!bytes.Equal(input, request.Input.Bytes) {
		return StartResult{}, false, fmt.Errorf("%w: run start identity differs", flowerr.ErrConflict)
	}
	return StartResult{ID: id, Created: false}, false, nil
}

func startJournalEntries(request StartRequest, dbNow time.Time, deadlineAt *time.Time) ([]JournalEntry, error) {
	keyScope := ""
	if request.KeyScope != KeyScopePermanent {
		keyScope = request.KeyScope
	}
	deadlineMilliseconds, err := durable.ExactMilliseconds("run deadline", request.Deadline.Duration)
	if err != nil {
		return nil, err
	}
	body := journalcodec.RunStartedBody{
		V: 1, RunID: request.ID.String(),
		DefinitionName: request.DefinitionName, DefinitionVersion: request.DefinitionVersion,
		RunKey: request.Key, KeyScope: keyScope,
		DeadlineMode: request.Deadline.Mode, DeadlineDuration: deadlineMilliseconds,
		DeadlineAt: clonePointer(deadlineAt), MaxCommands: request.MaxCommands,
	}
	start, err := NewJournalEntry(RunStarted, body)
	if err != nil {
		return nil, fmt.Errorf("encode run start: %w", err)
	}
	entries := []JournalEntry{start}
	nextAttemptAt, err := durable.AddExactDuration("initial delay", dbNow, request.Root.InitialDelay)
	if err != nil {
		return nil, err
	}
	initialState := "ready"
	var budgetStartedAt, recordedNextAttemptAt *time.Time
	if len(request.Root.Waits) > 0 {
		initialState = "pending"
	} else {
		budgetStartedAt = clonePointer(&dbNow)
		recordedNextAttemptAt = clonePointer(&nextAttemptAt)
	}
	created, err := commandCreatedEntry(*request.Root, initialState, budgetStartedAt, recordedNextAttemptAt)
	if err != nil {
		return nil, err
	}
	zero := 0
	created.CausationBatchIndex = &zero
	entries = append(entries, created)
	return entries, nil
}

func commandCreatedEntry(
	command CommandCreate,
	initialState string,
	budgetStartedAt *time.Time,
	nextAttemptAt *time.Time,
) (JournalEntry, error) {
	if err := validateCommandCreate(command); err != nil {
		return JournalEntry{}, err
	}
	if initialState != "pending" && initialState != "ready" {
		return JournalEntry{}, fmt.Errorf("%w: invalid initial command state", flowerr.ErrInvalid)
	}
	if initialState == "pending" && (budgetStartedAt != nil || nextAttemptAt != nil) ||
		initialState == "ready" && (budgetStartedAt == nil || nextAttemptAt == nil) {
		return JournalEntry{}, fmt.Errorf("%w: invalid initial command timing", flowerr.ErrInvalid)
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
	var recoveryLeaseMS *int64
	if command.RecoveryLease > 0 {
		value, err := durable.ExactMilliseconds("recovery lease", command.RecoveryLease)
		if err != nil {
			return JournalEntry{}, err
		}
		recoveryLeaseMS = &value
	}
	if command.InitialDelay > 0 {
		value, err := durable.ExactMilliseconds("initial delay", command.InitialDelay)
		if err != nil {
			return JournalEntry{}, err
		}
		initialDelayMS = &value
	}
	body := journalcodec.CommandCreatedBody{
		V: 1, CommandID: command.ID.String(), CommandKey: command.Key, Name: command.Name,
		Version: command.Version, Args: json.RawMessage(command.Args.BytesCopy()),
		InitialState: initialState,
		Queue:        command.Queue, AttemptTimeoutMS: timeoutMS, RecoveryLeaseMS: recoveryLeaseMS,
		RetryPolicy:    json.RawMessage(command.RetryPolicy.BytesCopy()),
		InitialDelayMS: initialDelayMS, BudgetStartedAt: clonePointer(budgetStartedAt), NextAttemptAt: clonePointer(nextAttemptAt),
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

type commandBatchCreate struct {
	command         CommandCreate
	createdPosition int64
	budgetStartedAt time.Time
	nextAttemptAt   time.Time
}

type applicationEventIdentity struct {
	name string
	key  string
}

type preparedCommandInsert struct {
	command          CommandCreate
	createdPosition  int64
	state            string
	unsatisfiedWaits int
	attemptTimeoutMS *int64
	recoveryLeaseMS  *int64
	initialDelayMS   *int64
	budgetStartedAt  *time.Time
	nextAttemptAt    *time.Time
	waitStartedAt    *time.Time
	waitDeadlineAt   *time.Time
	waitTimeoutMS    *int64
}

type preparedWaitInsert struct {
	commandID        uuid.UUID
	name             string
	key              string
	satisfied        *int64
	stagedEventIndex int
}

type preparedQueueInsert struct {
	commandID uuid.UUID
	queue     string
	name      string
	version   int
	nextRunAt time.Time
}

type preparedCommandBatch struct {
	commands            []preparedCommandInsert
	waits               []preparedWaitInsert
	queues              []preparedQueueInsert
	immediatelyRunnable bool
}

func (batch *preparedCommandBatch) assignJournalPositions(created, stagedEvents []int64) error {
	if batch == nil || len(created) != len(batch.commands) {
		return fmt.Errorf("%w: command-created journal position count differs", flowerr.ErrInvalidState)
	}
	for index, position := range created {
		if position < 1 {
			return fmt.Errorf("%w: command-created journal position is invalid", flowerr.ErrInvalidState)
		}
		batch.commands[index].createdPosition = position
	}
	for index := range batch.waits {
		wait := &batch.waits[index]
		if wait.stagedEventIndex < 0 {
			continue
		}
		if wait.stagedEventIndex >= len(stagedEvents) || stagedEvents[wait.stagedEventIndex] < 1 {
			return fmt.Errorf("%w: staged-event wait journal position differs", flowerr.ErrInvalidState)
		}
		wait.satisfied = clonePointer(&stagedEvents[wait.stagedEventIndex])
	}
	return nil
}

func (batch preparedCommandBatch) initialJournalState(index int) (string, *time.Time, *time.Time, error) {
	if index < 0 || index >= len(batch.commands) {
		return "", nil, nil, fmt.Errorf("%w: prepared command index is invalid", flowerr.ErrInvalidState)
	}
	command := batch.commands[index]
	return command.state, clonePointer(command.budgetStartedAt), clonePointer(command.nextAttemptAt), nil
}

func (s *Store) insertCommand(
	ctx context.Context,
	tx pgx.Tx,
	runID uuid.UUID,
	command CommandCreate,
	createdPosition int64,
	createdAt time.Time,
	budgetStartedAt time.Time,
	nextAttemptAt time.Time,
	runDeadline *time.Time,
) (bool, error) {
	batch, err := s.prepareCommandBatch(ctx, tx, runID, []commandBatchCreate{{
		command: command, createdPosition: createdPosition,
		budgetStartedAt: budgetStartedAt, nextAttemptAt: nextAttemptAt,
	}}, createdAt, runDeadline, nil)
	if err != nil {
		return false, err
	}
	if err := s.insertPreparedCommandBatch(ctx, tx, runID, createdAt, batch); err != nil {
		return false, err
	}
	return batch.commands[0].state == "ready", nil
}

func (s *Store) prepareCommandBatch(
	ctx context.Context,
	tx pgx.Tx,
	runID uuid.UUID,
	creates []commandBatchCreate,
	createdAt time.Time,
	runDeadline *time.Time,
	stagedEvents []ApplicationEvent,
) (preparedCommandBatch, error) {
	if tx == nil || runID == uuid.Nil || createdAt.IsZero() {
		return preparedCommandBatch{}, fmt.Errorf("%w: invalid command batch context", flowerr.ErrInvalid)
	}
	if err := durable.PostgresInteger("command batch count", len(creates), 1, durable.PostgresIntegerMax); err != nil {
		return preparedCommandBatch{}, err
	}

	staged := make(map[applicationEventIdentity]int, len(stagedEvents))
	for index, event := range stagedEvents {
		identity := applicationEventIdentity{name: event.Name, key: event.Key}
		if event.Name == "" || event.Key == "" {
			return preparedCommandBatch{}, fmt.Errorf("%w: incomplete staged application event", flowerr.ErrInvalid)
		}
		if _, exists := staged[identity]; exists {
			return preparedCommandBatch{}, fmt.Errorf("%w: duplicate normalized staged application event", flowerr.ErrInvalidState)
		}
		staged[identity] = index
	}

	wanted := make([]applicationEventIdentity, 0)
	wantedSet := make(map[applicationEventIdentity]struct{})
	commandIDs := make(map[uuid.UUID]struct{}, len(creates))
	commandKeys := make(map[string]struct{}, len(creates))
	for _, create := range creates {
		if err := validateCommandCreate(create.command); err != nil {
			return preparedCommandBatch{}, err
		}
		if create.budgetStartedAt.IsZero() || create.nextAttemptAt.IsZero() {
			return preparedCommandBatch{}, fmt.Errorf("%w: command batch timing is empty", flowerr.ErrInvalid)
		}
		if create.createdPosition < 0 {
			return preparedCommandBatch{}, fmt.Errorf("%w: command-created position is invalid", flowerr.ErrInvalid)
		}
		if _, exists := commandIDs[create.command.ID]; exists {
			return preparedCommandBatch{}, fmt.Errorf("%w: duplicate command ID in batch", flowerr.ErrConflict)
		}
		if _, exists := commandKeys[create.command.Key]; exists {
			return preparedCommandBatch{}, fmt.Errorf("%w: duplicate command key in batch", flowerr.ErrConflict)
		}
		commandIDs[create.command.ID] = struct{}{}
		commandKeys[create.command.Key] = struct{}{}
		for _, wait := range create.command.Waits {
			identity := applicationEventIdentity{name: wait.Name, key: wait.Key}
			if _, exists := wantedSet[identity]; exists {
				continue
			}
			wantedSet[identity] = struct{}{}
			wanted = append(wanted, identity)
		}
	}

	retained, err := s.lookupRetainedEventPositions(ctx, tx, runID, wanted)
	if err != nil {
		return preparedCommandBatch{}, err
	}
	result := preparedCommandBatch{
		commands: make([]preparedCommandInsert, 0, len(creates)),
	}
	for _, create := range creates {
		command := create.command
		prepared := preparedCommandInsert{command: command, createdPosition: create.createdPosition, state: "ready"}
		if command.AttemptTimeout > 0 {
			value, err := durable.ExactMilliseconds("attempt timeout", command.AttemptTimeout)
			if err != nil {
				return preparedCommandBatch{}, err
			}
			prepared.attemptTimeoutMS = &value
		}
		if command.RecoveryLease > 0 {
			value, err := durable.ExactMilliseconds("recovery lease", command.RecoveryLease)
			if err != nil {
				return preparedCommandBatch{}, err
			}
			prepared.recoveryLeaseMS = &value
		}
		if command.InitialDelay > 0 {
			value, err := durable.ExactMilliseconds("initial delay", command.InitialDelay)
			if err != nil {
				return preparedCommandBatch{}, err
			}
			prepared.initialDelayMS = &value
		}
		if command.Within > 0 {
			value, err := durable.ExactMilliseconds("wait timeout", command.Within)
			if err != nil {
				return preparedCommandBatch{}, err
			}
			prepared.waitTimeoutMS = &value
		}

		for _, wait := range command.Waits {
			identity := applicationEventIdentity{name: wait.Name, key: wait.Key}
			preparedWait := preparedWaitInsert{
				commandID: command.ID, name: wait.Name, key: wait.Key, stagedEventIndex: -1,
			}
			if position, ok := retained[identity]; ok {
				preparedWait.satisfied = clonePointer(&position)
			} else if eventIndex, ok := staged[identity]; ok {
				preparedWait.stagedEventIndex = eventIndex
			} else {
				prepared.unsatisfiedWaits++
			}
			result.waits = append(result.waits, preparedWait)
		}

		if prepared.unsatisfiedWaits > 0 {
			prepared.state = "pending"
			prepared.waitStartedAt = clonePointer(&createdAt)
			if command.Within > 0 {
				deadline, err := durable.AddExactDuration("wait timeout", createdAt, command.Within)
				if err != nil {
					return preparedCommandBatch{}, err
				}
				if runDeadline != nil && runDeadline.Before(deadline) {
					deadline = *runDeadline
				}
				prepared.waitDeadlineAt = &deadline
			}
		} else {
			prepared.budgetStartedAt = clonePointer(&create.budgetStartedAt)
			prepared.nextAttemptAt = clonePointer(&create.nextAttemptAt)
			result.queues = append(result.queues, preparedQueueInsert{
				commandID: command.ID, queue: command.Queue, name: command.Name,
				version: command.Version, nextRunAt: create.nextAttemptAt,
			})
			if !create.nextAttemptAt.After(createdAt) {
				result.immediatelyRunnable = true
			}
		}
		result.commands = append(result.commands, prepared)
	}
	return result, nil
}

func (s *Store) lookupRetainedEventPositions(
	ctx context.Context,
	tx pgx.Tx,
	runID uuid.UUID,
	identities []applicationEventIdentity,
) (map[applicationEventIdentity]int64, error) {
	result := make(map[applicationEventIdentity]int64, len(identities))
	if len(identities) == 0 {
		return result, nil
	}
	names := make([]string, len(identities))
	keys := make([]string, len(identities))
	for index, identity := range identities {
		names[index], keys[index] = identity.name, identity.key
	}
	rows, err := tx.Query(ctx, `WITH wanted(event_name,event_key) AS (
		SELECT * FROM unnest($2::text[],$3::text[])
	)
	SELECT j.event_name,j.event_key,j.position
	FROM wanted
	JOIN `+pgschema.Table(s.schema, "flow_journal")+` AS j
	  ON j.run_id=$1 AND j.entry_kind='event_recorded'
	 AND j.event_class='application' AND j.event_namespace='application'
	 AND j.event_name=wanted.event_name AND j.event_key=wanted.event_key`, runID, names, keys)
	if err != nil {
		return nil, MapError("load retained events for command batch", err)
	}
	defer rows.Close()
	for rows.Next() {
		var identity applicationEventIdentity
		var position int64
		if err := rows.Scan(&identity.name, &identity.key, &position); err != nil {
			return nil, MapError("scan retained event for command batch", err)
		}
		if position < 1 {
			return nil, fmt.Errorf("%w: retained application-event position is invalid", flowerr.ErrInvalidState)
		}
		if _, exists := result[identity]; exists {
			return nil, fmt.Errorf("%w: retained application-event identity is not unique", flowerr.ErrInvalidState)
		}
		result[identity] = position
	}
	if err := rows.Err(); err != nil {
		return nil, MapError("read retained events for command batch", err)
	}
	return result, nil
}

func (s *Store) insertPreparedCommandBatch(
	ctx context.Context,
	tx pgx.Tx,
	runID uuid.UUID,
	createdAt time.Time,
	batch preparedCommandBatch,
) error {
	commandRows := make([][]any, len(batch.commands))
	for index, prepared := range batch.commands {
		if prepared.createdPosition < 1 {
			return fmt.Errorf("%w: command-created journal position is missing", flowerr.ErrInvalidState)
		}
		command := prepared.command
		commandRows[index] = []any{
			command.ID, runID, command.Key, command.Name, command.Version,
			command.ParentCommandID, command.Args.Bytes,
			command.DeclarationFingerprint[:], prepared.state, prepared.unsatisfiedWaits,
			command.Queue, prepared.attemptTimeoutMS, prepared.recoveryLeaseMS,
			command.RetryPolicy.Bytes, prepared.initialDelayMS,
			prepared.budgetStartedAt, prepared.nextAttemptAt, prepared.waitStartedAt,
			prepared.waitDeadlineAt, prepared.waitTimeoutMS, prepared.createdPosition,
			createdAt, createdAt, createdAt,
		}
	}
	count, err := tx.CopyFrom(ctx, pgx.Identifier{s.schema, "flow_commands"}, []string{
		"command_id", "run_id", "command_key", "name", "version", "parent_command_id",
		"args", "declaration_fingerprint", "state", "unsatisfied_waits", "queue", "attempt_timeout_ms", "recovery_lease_ms",
		"retry_policy", "initial_delay_ms", "budget_started_at", "next_attempt_at", "wait_started_at",
		"wait_deadline_at", "wait_timeout_ms", "created_position", "created_at", "updated_at", "status_at",
	}, pgx.CopyFromRows(commandRows))
	if err != nil {
		return MapError("insert command batch", err)
	}
	if count != int64(len(commandRows)) {
		return fmt.Errorf("%w: command batch inserted %d of %d rows", flowerr.ErrInvalidState, count, len(commandRows))
	}

	if len(batch.waits) > 0 {
		waitRows := make([][]any, len(batch.waits))
		for index, wait := range batch.waits {
			waitRows[index] = []any{wait.commandID, runID, wait.name, wait.key, wait.satisfied}
		}
		count, err = tx.CopyFrom(ctx, pgx.Identifier{s.schema, "flow_command_event_waits"}, []string{
			"command_id", "run_id", "event_name", "event_key", "satisfied_position",
		}, pgx.CopyFromRows(waitRows))
		if err != nil {
			return MapError("insert command event wait batch", err)
		}
		if count != int64(len(waitRows)) {
			return fmt.Errorf("%w: command event wait batch inserted %d of %d rows", flowerr.ErrInvalidState, count, len(waitRows))
		}
	}

	if len(batch.queues) > 0 {
		queueRows := make([][]any, len(batch.queues))
		for index, queue := range batch.queues {
			queueRows[index] = []any{
				queue.commandID, runID, queue.queue, queue.name, queue.version, "ready", queue.nextRunAt,
			}
		}
		count, err = tx.CopyFrom(ctx, pgx.Identifier{s.schema, "flow_command_queue"}, []string{
			"command_id", "run_id", "queue", "name", "version", "state", "next_run_at",
		}, pgx.CopyFromRows(queueRows))
		if err != nil {
			return MapError("insert command queue batch", err)
		}
		if count != int64(len(queueRows)) {
			return fmt.Errorf("%w: command queue batch inserted %d of %d rows", flowerr.ErrInvalidState, count, len(queueRows))
		}
	}
	return nil
}

type RunHead struct {
	ID           uuid.UUID
	Status       string
	MaxCommands  int
	CommandCount int
	OpenCommands int
	RunKey       string
	Definition   string
}

func (s *Store) LoadRunHead(ctx context.Context, semantic *SemanticTx) (RunHead, error) {
	if semantic == nil || semantic.PGX() == nil {
		return RunHead{}, fmt.Errorf("%w: semantic transaction is nil", flowerr.ErrInvalid)
	}
	var result RunHead
	err := semantic.PGX().QueryRow(ctx, `SELECT run_id,status,max_commands,command_count,open_commands,run_key,definition_name
		FROM `+pgschema.Table(s.schema, "flow_runs")+` WHERE run_id=$1`, semantic.RunID()).
		Scan(&result.ID, &result.Status, &result.MaxCommands, &result.CommandCount, &result.OpenCommands,
			&result.RunKey, &result.Definition)
	if err != nil {
		return RunHead{}, MapError("load run", err)
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
	unique := make([]ApplicationEvent, 0, len(ordered))
	seen := make(map[applicationEventIdentity]ApplicationEvent, len(ordered))
	for _, event := range ordered {
		if event.ID == uuid.Nil || event.Name == "" || event.Key == "" || len(event.Body.Bytes) == 0 {
			return nil, fmt.Errorf("%w: incomplete staged application event", flowerr.ErrInvalid)
		}
		identity := applicationEventIdentity{name: event.Name, key: event.Key}
		if prior, exists := seen[identity]; exists {
			if bytes.Equal(prior.Body.Bytes, event.Body.Bytes) {
				continue
			}
			return nil, fmt.Errorf("%w: staged application event identity differs", flowerr.ErrConflict)
		}
		seen[identity] = event
		unique = append(unique, event)
	}

	names := make([]string, len(unique))
	keys := make([]string, len(unique))
	for index, event := range unique {
		names[index], keys[index] = event.Name, event.Key
	}
	rows, err := semantic.PGX().Query(ctx, `WITH staged(event_name,event_key) AS (
		SELECT * FROM unnest($2::text[],$3::text[])
	)
	SELECT j.event_name,j.event_key,j.body
	FROM staged
	JOIN `+pgschema.Table(s.schema, "flow_journal")+` AS j
	  ON j.run_id=$1 AND j.entry_kind='event_recorded'
	 AND j.event_class='application' AND j.event_namespace='application'
	 AND j.event_name=staged.event_name AND j.event_key=staged.event_key`,
		semantic.RunID(), names, keys)
	if err != nil {
		return nil, MapError("load staged application event identities", err)
	}
	existing := make(map[applicationEventIdentity][]byte, len(unique))
	for rows.Next() {
		var identity applicationEventIdentity
		var body []byte
		if err := rows.Scan(&identity.name, &identity.key, &body); err != nil {
			rows.Close()
			return nil, MapError("scan staged application event identity", err)
		}
		if _, exists := existing[identity]; exists {
			rows.Close()
			return nil, fmt.Errorf("%w: application event identity is not unique", flowerr.ErrInvalidState)
		}
		existing[identity] = body
	}
	if err := rows.Err(); err != nil {
		rows.Close()
		return nil, MapError("read staged application event identities", err)
	}
	rows.Close()

	result := make([]ApplicationEvent, 0, len(unique))
	for _, event := range unique {
		body, found := existing[applicationEventIdentity{name: event.Name, key: event.Key}]
		if !found {
			result = append(result, event)
			continue
		}
		if !bytes.Equal(body, event.Body.Bytes) {
			return nil, fmt.Errorf("%w: application event identity differs", flowerr.ErrConflict)
		}
	}
	return result, nil
}

type EventRecord struct {
	ID   uuid.UUID
	Body []byte
}

func (s *Store) GetEvent(ctx context.Context, tx pgx.Tx, runID uuid.UUID, name, key string) (EventRecord, bool, error) {
	var row pgx.Row
	query := `SELECT event_id,body FROM ` + pgschema.Table(s.schema, "flow_journal") + `
		WHERE run_id=$1 AND entry_kind='event_recorded' AND event_class='application'
		AND event_namespace='application' AND event_name=$2 AND event_key=$3`
	if tx != nil {
		row = tx.QueryRow(ctx, query, runID, name, key)
	} else {
		row = s.db.Conn.QueryRow(ctx, query, runID, name, key)
	}
	var result EventRecord
	if err := row.Scan(&result.ID, &result.Body); err != nil {
		if errors.Is(err, pgx.ErrNoRows) {
			return EventRecord{}, false, nil
		}
		return EventRecord{}, false, MapError("get application event", err)
	}
	return result, true, nil
}

func (s *Store) EmitLocked(ctx context.Context, semantic *SemanticTx, event ApplicationEvent) (bool, error) {
	existing, found, err := s.GetEvent(ctx, semantic.PGX(), semantic.RunID(), event.Name, event.Key)
	if err != nil {
		return false, err
	}
	if found {
		if bytes.Equal(existing.Body, event.Body.Bytes) {
			return false, nil
		}
		return false, fmt.Errorf("%w: application event identity differs", flowerr.ErrConflict)
	}
	head, err := s.LoadRunHead(ctx, semantic)
	if err != nil {
		return false, err
	}
	if head.Status != "running" && head.Status != "failing" {
		return false, fmt.Errorf("%w: run is terminal", flowerr.ErrTerminal)
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
	immediatelyRunnable, err := s.resolveEventReadinessLocked(ctx, semantic, []acceptedEventPosition{{
		name: event.Name, key: event.Key, position: journal.Journal[0].Position,
	}})
	if err != nil {
		return false, err
	}
	if immediatelyRunnable {
		if err := semantic.NotifyRunnableCommands(ctx); err != nil {
			return false, err
		}
	}
	return true, nil
}

type CancelResult struct {
	Created bool
	// TerminalRun reports that the cancellation terminalized the run.
	// RunStatus, RunKey, and Definition identify the terminal run fact.
	TerminalRun bool
	RunStatus   string
	RunKey      string
	Definition  string
	// CommandKey is the cancelled command's key on command cancellation.
	CommandKey string
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
		 WHERE run_id=c.run_id AND attempt_id=q.active_attempt_id AND entry_kind='attempt_started')
		FROM `+pgschema.Table(s.schema, "flow_command_queue")+` q
		JOIN `+pgschema.Table(s.schema, "flow_commands")+` c ON c.command_id=q.command_id
		WHERE q.command_id=$1 AND q.run_id=$2 FOR UPDATE OF q`, commandID, semantic.RunID()).
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

func (s *Store) GetCommandRunID(ctx context.Context, tx pgx.Tx, commandID uuid.UUID) (uuid.UUID, error) {
	query := `SELECT run_id FROM ` + pgschema.Table(s.schema, "flow_commands") + ` WHERE command_id=$1`
	var row pgx.Row
	if tx != nil {
		row = tx.QueryRow(ctx, query, commandID)
	} else {
		row = s.db.Conn.QueryRow(ctx, query, commandID)
	}
	var runID uuid.UUID
	if err := row.Scan(&runID); err != nil {
		return uuid.Nil, MapError("lookup command run", err)
	}
	return runID, nil
}

func (s *Store) CancelCommandLocked(ctx context.Context, semantic *SemanticTx, commandID uuid.UUID, reason string) (CancelResult, error) {
	head, err := s.LoadRunHead(ctx, semantic)
	if err != nil {
		return CancelResult{}, err
	}
	var key, state string
	var failureBytes []byte
	err = semantic.PGX().QueryRow(ctx, `SELECT command_key,state,COALESCE(terminal_failure,'null'::jsonb)
		FROM `+pgschema.Table(s.schema, "flow_commands")+`
		WHERE command_id=$1 AND run_id=$2 FOR UPDATE`, commandID, head.ID).
		Scan(&key, &state, &failureBytes)
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
		return CancelResult{}, fmt.Errorf("%w: run is terminal", flowerr.ErrTerminal)
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
	failureEffects, err := s.resolveCommandFailureLocked(ctx, semantic, commandID, "cancelled")
	if err != nil {
		return CancelResult{}, err
	}
	becameFailing := head.Status == "running"
	if becameFailing {
		survivors := make([]string, len(failureEffects.survivors))
		for index, command := range failureEffects.survivors {
			survivors[index] = command.key
		}
		failing, err := NewJournalEntry(RunFailing, journalcodec.RunFailingBody{
			V: 1, Status: "failing", Reason: reason, CommandKey: key, Survivors: survivors,
		})
		if err != nil {
			return CancelResult{}, err
		}
		failing.CausationBatchIndex = clonePointer(&terminalIndex)
		entries = append(entries, failing)
	}
	cancelledOffset := len(entries)
	cancelledEntries, err := failureEffects.cancellationEntries(terminalIndex, "cancelled after command cancellation")
	if err != nil {
		return CancelResult{}, err
	}
	entries = append(entries, cancelledEntries...)
	effectiveOpen, err := durable.AddPostgresInteger("run open commands", head.OpenCommands,
		-1, 0, durable.PostgresIntegerMax)
	if err != nil {
		return CancelResult{}, err
	}
	effectiveOpen, err = durable.AddPostgresInteger("run open commands", effectiveOpen,
		-len(failureEffects.cancelled), 0, durable.PostgresIntegerMax)
	if err != nil {
		return CancelResult{}, err
	}
	terminalRun := effectiveOpen == 0
	if terminalRun {
		runEvent, err := runTerminalEvent("failed", reason, "flow.run_failed")
		if err != nil {
			return CancelResult{}, err
		}
		runEvent.CausationBatchIndex = clonePointer(&terminalIndex)
		entries = append(entries, runEvent)
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
	if err := s.applyFailureResolution(ctx, semantic, failureEffects, journal, cancelledOffset,
		"cancelled after command cancellation"); err != nil {
		return CancelResult{}, err
	}

	if terminalRun {
		if _, err := semantic.PGX().Exec(ctx, `UPDATE `+pgschema.Table(s.schema, "flow_runs")+`
			SET status='failed',open_commands=0,failure=$2::jsonb,
			    finished_at=$3,updated_at=$3,status_at=$3
			WHERE run_id=$1`, head.ID, jsonString(terminalFailure{Code: "command_cancelled", Message: reason}), semantic.DBNow()); err != nil {
			return CancelResult{}, MapError("fail run after command cancellation", err)
		}
	} else {
		status := head.Status
		if becameFailing {
			status = "failing"
		}
		if _, err := semantic.PGX().Exec(ctx, `UPDATE `+pgschema.Table(s.schema, "flow_runs")+`
			SET status=$2,open_commands=$4,updated_at=$3,status_at=CASE WHEN status<>$2 THEN $3 ELSE status_at END
			WHERE run_id=$1`, head.ID, status, semantic.DBNow(), effectiveOpen); err != nil {
			return CancelResult{}, MapError("update run after command cancellation", err)
		}
	}
	result := CancelResult{Created: true, CommandKey: key, RunKey: head.RunKey, Definition: head.Definition}
	if terminalRun {
		result.TerminalRun, result.RunStatus = true, "failed"
	}
	return result, nil
}

func (s *Store) CancelRunLocked(ctx context.Context, semantic *SemanticTx, reason string) (CancelResult, error) {
	head, err := s.LoadRunHead(ctx, semantic)
	if err != nil {
		return CancelResult{}, err
	}
	var runFailure []byte
	if err := semantic.PGX().QueryRow(ctx, `SELECT COALESCE(failure,'null'::jsonb) FROM `+
		pgschema.Table(s.schema, "flow_runs")+` WHERE run_id=$1`, head.ID).Scan(&runFailure); err != nil {
		return CancelResult{}, MapError("load run cancellation", err)
	}
	if head.Status != "running" && head.Status != "failing" {
		if head.Status == "cancelled" && sameTerminalReason(runFailure, reason) {
			return CancelResult{}, nil
		}
		return CancelResult{}, fmt.Errorf("%w: run is terminal", flowerr.ErrTerminal)
	}

	rows, err := semantic.PGX().Query(ctx, `SELECT command_id,command_key,state FROM `+
		pgschema.Table(s.schema, "flow_commands")+`
		WHERE run_id=$1 AND state NOT IN ('succeeded','failed','cancelled','expired')
		ORDER BY command_id FOR UPDATE`, head.ID)
	if err != nil {
		return CancelResult{}, MapError("lock run commands for cancellation", err)
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
			return CancelResult{}, MapError("scan run commands for cancellation", err)
		}
		commands = append(commands, item)
	}
	if err := rows.Err(); err != nil {
		rows.Close()
		return CancelResult{}, MapError("read run commands for cancellation", err)
	}
	rows.Close()
	if len(commands) != head.OpenCommands {
		return CancelResult{}, fmt.Errorf("%w: run open-command counter differs from materialized commands", flowerr.ErrInvalidState)
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
				"run_cancelled", reason, semantic.DBNow())
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
	runEvent, err := runTerminalEvent("cancelled", reason, "flow.run_cancelled")
	if err != nil {
		return CancelResult{}, err
	}
	if len(entries) > 0 {
		index := len(entries) - 1
		runEvent.CausationBatchIndex = &index
	}
	entries = append(entries, runEvent)
	journal, err := semantic.Apply(ctx, PersistedChangeSet{Journal: entries})
	if err != nil {
		return CancelResult{}, err
	}
	failure := jsonString(terminalFailure{Code: "cancelled", Message: reason})
	commandIDs := make([]uuid.UUID, len(commands))
	terminalPositions := make([]int64, len(commands))
	for index, command := range commands {
		journalIndex, exists := terminalBatchIndex[command.id]
		if !exists || journalIndex < 0 || journalIndex >= len(journal.Journal) {
			return CancelResult{}, fmt.Errorf("%w: run cancellation journal mapping is invalid", flowerr.ErrInvalidState)
		}
		row := journal.Journal[journalIndex]
		if row.CommandID == nil || *row.CommandID != command.id || row.TerminalStatus == nil || *row.TerminalStatus != "cancelled" {
			return CancelResult{}, fmt.Errorf("%w: run cancellation journal mapping differs", flowerr.ErrInvalidState)
		}
		commandIDs[index], terminalPositions[index] = command.id, row.Position
	}
	commandTag, err := semantic.PGX().Exec(ctx, `UPDATE `+pgschema.Table(s.schema, "flow_commands")+` AS c
		SET state='cancelled',terminal_failure=$4::jsonb,terminal_position=cancelled.position,
		    finished_at=$5,updated_at=$5,status_at=$5
		FROM unnest($1::uuid[],$2::bigint[]) AS cancelled(command_id,position)
		WHERE c.run_id=$3 AND c.command_id=cancelled.command_id
		  AND c.state NOT IN ('succeeded','failed','cancelled','expired')`,
		commandIDs, terminalPositions, head.ID, failure, semantic.DBNow())
	if err != nil {
		return CancelResult{}, MapError("cancel run command batch", err)
	}
	if commandTag.RowsAffected() != int64(len(commandIDs)) {
		return CancelResult{}, fmt.Errorf("%w: run cancellation set changed", flowerr.ErrInvalidState)
	}
	if _, err := semantic.PGX().Exec(ctx, `DELETE FROM `+pgschema.Table(s.schema, "flow_command_queue")+` WHERE run_id=$1`, head.ID); err != nil {
		return CancelResult{}, MapError("remove run deliveries", err)
	}
	if _, err := semantic.PGX().Exec(ctx, `UPDATE `+pgschema.Table(s.schema, "flow_runs")+`
		SET status='cancelled',open_commands=0,failure=$2::jsonb,finished_at=$3,updated_at=$3,status_at=$3
		WHERE run_id=$1`, head.ID, failure, semantic.DBNow()); err != nil {
		return CancelResult{}, MapError("cancel run", err)
	}
	return CancelResult{Created: true, TerminalRun: true, RunStatus: "cancelled",
		RunKey: head.RunKey, Definition: head.Definition}, nil
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

func runTerminalEvent(status, reason, name string) (JournalEntry, error) {
	body, err := NewJournalEntry(EventRecorded, journalcodec.TerminalEventBody{V: 1, Status: status, Reason: reason})
	if err != nil {
		return JournalEntry{}, err
	}
	eventID := uuid.New()
	body.EventID = &eventID
	body.EventNamespace = stringPointer("runtime")
	body.EventName = clonePointer(&name)
	body.EventClass = stringPointer("run_terminal")
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
	value, err := durable.AddExactDuration("run deadline", now, spec.Duration)
	if err != nil {
		return nil, err
	}
	return &value, nil
}

func validateStartRequest(request StartRequest) error {
	if request.ID == uuid.Nil || request.DefinitionName == "" || request.DefinitionVersion <= 0 ||
		request.MaxCommands < 0 || len(request.Input.Bytes) == 0 {
		return fmt.Errorf("%w: incomplete run start", flowerr.ErrInvalid)
	}
	if request.KeyScope != "" && request.KeyScope != KeyScopePermanent && request.KeyScope != KeyScopeLive {
		return fmt.Errorf("%w: invalid run key scope", flowerr.ErrInvalid)
	}
	if request.KeyScope == KeyScopeLive && request.Key == "" {
		return fmt.Errorf("%w: live key scope requires a run key", flowerr.ErrInvalid)
	}
	if request.Deadline.Mode != "none" && request.Deadline.Mode != "duration" {
		return fmt.Errorf("%w: invalid deadline mode", flowerr.ErrInvalid)
	}
	if request.Deadline.Mode == "duration" && request.Deadline.Duration <= 0 {
		return fmt.Errorf("%w: invalid run deadline", flowerr.ErrInvalid)
	}
	if request.Deadline.Mode == "duration" {
		if _, err := durable.ExactMilliseconds("run deadline", request.Deadline.Duration); err != nil {
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
	if len(command.Waits) > MaxCommandEventWaits {
		return fmt.Errorf("%w: command exceeds event-wait limit", flowerr.ErrInvalid)
	}
	waits := make(map[applicationEventIdentity]struct{}, len(command.Waits))
	for _, wait := range command.Waits {
		identity := applicationEventIdentity{name: wait.Name, key: wait.Key}
		if wait.Name == "" || wait.Key == "" {
			return fmt.Errorf("%w: incomplete command event wait", flowerr.ErrInvalid)
		}
		if _, exists := waits[identity]; exists {
			return fmt.Errorf("%w: duplicate command event wait", flowerr.ErrConflict)
		}
		waits[identity] = struct{}{}
	}
	for field, value := range map[string]time.Duration{
		"attempt timeout": command.AttemptTimeout,
		"recovery lease":  command.RecoveryLease,
		"initial delay":   command.InitialDelay,
		"wait timeout":    command.Within,
	} {
		if _, err := durable.ExactMilliseconds(field, value); err != nil {
			return err
		}
	}
	if command.RecoveryLease > 0 && command.RecoveryLease < 30*time.Millisecond {
		return fmt.Errorf("%w: recovery lease must be at least 30ms", flowerr.ErrInvalid)
	}
	if _, err := retrypolicy.PublicFromCanonical(command.RetryPolicy.Bytes); err != nil {
		return fmt.Errorf("%w: retry policy is invalid", flowerr.ErrInvalid)
	}
	return nil
}

func stringPointer(value string) *string { return &value }
