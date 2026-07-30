package flow

import (
	"context"
	"encoding/json"
	"errors"
	"sort"
	"time"

	"github.com/goware/flow/internal/replay"
	"github.com/goware/flow/internal/store"
	"github.com/jackc/pgx/v5"
)

type Execution struct {
	ID               ExecutionID
	Mode             string
	Type             string
	Version          int
	Key              string
	Status           string
	FailFast         bool
	MaxCommands      int
	CommandCount     int
	OpenCommands     int
	PlanDirty        bool
	PlanQuiescent    bool
	PlanRevision     PlanRevision
	PlanWaitingCount int
	PlanWaitingOn    []string
	DeadlineAt       *time.Time
	FailureCode      string
	FailureMessage   string
	CreatedAt        time.Time
	UpdatedAt        time.Time
	StatusAt         time.Time
	FinishedAt       *time.Time
	Metadata         json.RawMessage
}

type TraceAttempt struct {
	ID               AttemptID
	Attempt          int
	StartedAt        time.Time
	FinishedAt       *time.Time
	Worker           string
	Classification   string
	ConsumedBudget   bool
	ConsumedAttempts int
	NextAttemptAt    *time.Time
	ErrorCode        string
	ErrorMessage     string
}

type TraceCommand struct {
	ID                    CommandID
	Key                   string
	Name                  string
	Version               int
	Origin                string
	ParentCommandID       CommandID
	Required              bool
	State                 string
	Args                  json.RawMessage
	Result                json.RawMessage
	Queue                 string
	ScheduleKind          string
	InitialDelay          time.Duration
	BudgetStartedAt       *time.Time
	NextAttemptAt         *time.Time
	Within                time.Duration
	ChildMembershipClosed bool
	Dependencies          []TraceDependencyGroup
	Waits                 []TraceEventWait
	CreatedPosition       JournalPosition
	TerminalPosition      *JournalPosition
	FailureCode           string
	FailureMessage        string
	LastErrorCode         string
	LastErrorMessage      string
	UnsatisfiedGroups     int
	UnsatisfiedWaits      int
	AttemptOrdinal        int
	ConsumedAttempts      int
	WaitStartedAt         *time.Time
	WaitDeadlineAt        *time.Time
	DeliveryState         string
	LeaseOwner            string
	LeaseStartedAt        *time.Time
	LeaseExpiresAt        *time.Time
	CreatedAt             time.Time
	UpdatedAt             time.Time
	StatusAt              time.Time
	FinishedAt            *time.Time
	Attempts              []TraceAttempt
}

type TraceDependencyGroup struct {
	Kind    string
	Members []string
}

type TraceEventWait struct {
	Name string
	Key  string
}

type TraceEvent struct {
	ID                EventID
	Position          JournalPosition
	Namespace         string
	Name              string
	Key               string
	Class             string
	TerminalStatus    string
	CommandID         CommandID
	CoordinatorID     CoordinatorID
	RecordedAt        time.Time
	CausationPosition *JournalPosition
	Body              json.RawMessage
}

type TraceCoordinator struct {
	ID               CoordinatorID
	Name             string
	Version          int
	Status           string
	State            json.RawMessage
	StatePosition    JournalPosition
	StateRevision    uint64
	StartPending     bool
	DeliveryState    string
	DeliveryKey      string
	InboxPosition    JournalPosition
	DeliveryPosition *JournalPosition
	AttemptOrdinal   int
	ConsumedAttempts int
	LeaseOwner       string
	LeaseStartedAt   *time.Time
	LeaseExpiresAt   *time.Time
	CreatedAt        time.Time
	UpdatedAt        time.Time
	FinishedAt       *time.Time
	Attempts         []TraceAttempt
}

type ExecutionTrace struct {
	Execution   Execution
	Commands    []TraceCommand
	Events      []TraceEvent
	Coordinator *TraceCoordinator
	History     []HistoryEntry
	results     resultSourceState
}

func (trace ExecutionTrace) flowResultSource() *resultSourceState { return &trace.results }

type TraceOption interface {
	applyTrace(*traceOptions)
}

type traceOptions struct{ errs []error }

const maxTraceEntries = 100_000

func Trace(ctx context.Context, c Client, id ExecutionID, opts ...TraceOption) (ExecutionTrace, error) {
	executionID, err := parseExecutionID(id)
	if err != nil {
		return ExecutionTrace{}, err
	}
	client, err := resolveClient(c)
	if err != nil {
		return ExecutionTrace{}, err
	}
	options := traceOptions{}
	for _, option := range opts {
		if option == nil {
			options.errs = append(options.errs, errors.New("nil trace option"))
			continue
		}
		option.applyTrace(&options)
	}
	if err := errors.Join(options.errs...); err != nil {
		return ExecutionTrace{}, newError(ErrInvalid, "trace", "options", "", err.Error())
	}
	var ownedTx pgx.Tx
	if client.tx == nil {
		ownedTx, err = client.runtime.db.Conn.BeginTx(ctx, pgx.TxOptions{IsoLevel: pgx.RepeatableRead, AccessMode: pgx.ReadOnly})
		if err != nil {
			return ExecutionTrace{}, store.MapError("begin trace snapshot", err)
		}
		defer ownedTx.Rollback(context.WithoutCancel(ctx))
		client.tx = ownedTx
	}
	rows := make([]store.JournalRow, 0, 64)
	var after uint64
	for len(rows) < maxTraceEntries {
		page, err := client.runtime.store.HistoryInTx(ctx, client.tx, executionID, after, store.MaxHistoryLimit)
		if err != nil {
			return ExecutionTrace{}, err
		}
		rows = append(rows, page...)
		if len(page) < store.MaxHistoryLimit {
			break
		}
		after = uint64(page[len(page)-1].Position)
	}
	if len(rows) == 0 {
		return ExecutionTrace{}, newError(ErrNotFound, "trace", "execution", string(id), "execution does not exist")
	}
	if len(rows) >= maxTraceEntries {
		return ExecutionTrace{}, newError(ErrInvalid, "trace", "execution", string(id), "trace exceeds the initial bounded history limit")
	}
	state, err := replay.Fold(rows)
	if err != nil {
		return ExecutionTrace{}, newError(ErrInvalidState, "trace", "execution", string(id), "retained history cannot be replayed")
	}
	live, err := client.runtime.store.GetExecutionInTx(ctx, client.tx, executionID)
	if err != nil {
		return ExecutionTrace{}, err
	}
	result := ExecutionTrace{Execution: executionFromStore(live), History: historyEntries(rows),
		results: resultSourceState{values: make(map[string]resultSourceValue)}}
	operational, err := client.runtime.store.TraceOperationalInTx(ctx, client.tx, executionID)
	if err != nil {
		return ExecutionTrace{}, err
	}
	operationalCommands := make(map[string]store.TraceCommandRow, len(operational.Commands))
	for _, command := range operational.Commands {
		operationalCommands[command.ID.String()] = command
	}
	result.Commands = make([]TraceCommand, 0, len(state.Commands))
	for _, command := range state.Commands {
		item := TraceCommand{
			ID: CommandID(command.ID.String()), Key: command.Key, Name: command.Name, Version: command.Version,
			Origin: command.Origin, Required: command.Required, State: command.State,
			Args: json.RawMessage(append([]byte(nil), command.Args...)), Result: json.RawMessage(append([]byte(nil), command.Result...)),
			Queue: command.Queue, CreatedPosition: JournalPosition(command.CreatedPosition),
			ScheduleKind: command.ScheduleKind, BudgetStartedAt: cloneTimePointer(command.BudgetStartedAt),
			NextAttemptAt: cloneTimePointer(command.NextAttemptAt), ChildMembershipClosed: command.ChildMembershipClosed,
			FailureCode: command.FailureCode, FailureMessage: command.FailureMessage,
		}
		if command.ParentCommandID != nil {
			item.ParentCommandID = CommandID(command.ParentCommandID.String())
		}
		if command.InitialDelayMS != nil {
			item.InitialDelay = time.Duration(*command.InitialDelayMS) * time.Millisecond
		}
		if command.WithinMS != nil {
			item.Within = time.Duration(*command.WithinMS) * time.Millisecond
		}
		for _, group := range command.Dependencies {
			item.Dependencies = append(item.Dependencies, TraceDependencyGroup{
				Kind: group.Kind, Members: append([]string(nil), group.Members...),
			})
		}
		for _, wait := range command.Waits {
			item.Waits = append(item.Waits, TraceEventWait{Name: wait.Name, Key: wait.Key})
		}
		if command.TerminalPosition != nil {
			position := JournalPosition(*command.TerminalPosition)
			item.TerminalPosition = &position
		}
		if current, ok := operationalCommands[command.ID.String()]; ok {
			item.State = current.State
			item.BudgetStartedAt = cloneTimePointer(current.BudgetStartedAt)
			item.NextAttemptAt = cloneTimePointer(current.NextAttemptAt)
			item.LastErrorCode, item.LastErrorMessage = current.LastErrorCode, current.LastErrorMessage
			item.UnsatisfiedGroups, item.UnsatisfiedWaits = current.UnsatisfiedGroups, current.UnsatisfiedWaits
			item.AttemptOrdinal, item.ConsumedAttempts = current.AttemptOrdinal, current.ConsumedAttempts
			item.WaitStartedAt, item.WaitDeadlineAt = cloneTimePointer(current.WaitStartedAt), cloneTimePointer(current.WaitDeadlineAt)
			item.DeliveryState, item.LeaseOwner = current.DeliveryState, current.LeaseOwner
			item.LeaseStartedAt, item.LeaseExpiresAt = cloneTimePointer(current.LeaseStartedAt), cloneTimePointer(current.LeaseExpiresAt)
			item.CreatedAt, item.UpdatedAt, item.StatusAt = current.CreatedAt, current.UpdatedAt, current.StatusAt
			item.FinishedAt = cloneTimePointer(current.FinishedAt)
		}
		item.Attempts = make([]TraceAttempt, len(command.Attempts))
		for index, attempt := range command.Attempts {
			item.Attempts[index] = TraceAttempt{
				ID: AttemptID(attempt.ID.String()), Attempt: attempt.Ordinal, StartedAt: attempt.StartedAt,
				FinishedAt: cloneTimePointer(attempt.FinishedAt), Worker: attempt.Worker,
				Classification: attempt.Classification, ConsumedBudget: attempt.ConsumedBudget,
				ConsumedAttempts: attempt.ConsumedAttempts, NextAttemptAt: cloneTimePointer(attempt.NextAttemptAt),
				ErrorCode: attempt.ErrorCode, ErrorMessage: attempt.ErrorMessage,
			}
		}
		result.Commands = append(result.Commands, item)
		value := resultSourceValue{
			name: command.Name, version: command.Version, status: commandStatus(command.State),
			result: append([]byte(nil), command.Result...),
		}
		if command.FailureCode != "" {
			value.failure = &CommandFailure{Code: command.FailureCode, Message: command.FailureMessage}
		} else if value.status != "" && value.status != StatusSucceeded {
			value.failure = &CommandFailure{Code: command.State, Message: "command ended " + command.State}
		}
		result.results.values[command.Key] = value
	}
	sort.Slice(result.Commands, func(i, j int) bool { return result.Commands[i].Key < result.Commands[j].Key })
	result.Events = make([]TraceEvent, 0, len(state.Events))
	historyByPosition := make(map[JournalPosition]HistoryEntry, len(result.History))
	for _, entry := range result.History {
		historyByPosition[entry.Position] = entry
	}
	for _, event := range state.Events {
		entry := historyByPosition[JournalPosition(event.Position)]
		item := TraceEvent{
			ID: EventID(event.ID.String()), Position: JournalPosition(event.Position), Namespace: event.Namespace,
			Name: event.Name, Key: event.Key, Class: event.Class,
			TerminalStatus: event.TerminalStatus, RecordedAt: entry.RecordedAt,
			CausationPosition: cloneJournalPosition(entry.CausationPosition),
			Body:              json.RawMessage(append([]byte(nil), event.Body...)),
		}
		if event.CommandID != nil {
			item.CommandID = CommandID(event.CommandID.String())
		}
		if entry.CoordinatorID != "" {
			item.CoordinatorID = entry.CoordinatorID
		}
		result.Events = append(result.Events, item)
	}
	if state.Coordinator != nil {
		coordinator := state.Coordinator
		result.Coordinator = &TraceCoordinator{
			ID: CoordinatorID(coordinator.ID.String()), Name: coordinator.Name, Version: coordinator.Version,
			Status: coordinator.Status, State: json.RawMessage(append([]byte(nil), coordinator.State...)),
			StatePosition: JournalPosition(coordinator.StatePosition), StateRevision: uint64(coordinator.StateRevision),
			StartPending: coordinator.StartPending, DeliveryState: coordinator.DeliveryState,
			DeliveryKey: coordinator.DeliveryKey, InboxPosition: JournalPosition(coordinator.InboxPosition),
			Attempts: traceAttempts(coordinator.Attempts),
		}
		if current := operational.Coordinator; current != nil {
			result.Coordinator.Status = current.Status
			result.Coordinator.State = json.RawMessage(append([]byte(nil), current.State...))
			result.Coordinator.StatePosition = JournalPosition(current.StatePosition)
			result.Coordinator.StateRevision = uint64(current.StateRevision)
			result.Coordinator.StartPending = current.StartPending
			result.Coordinator.InboxPosition = JournalPosition(current.InboxPosition)
			result.Coordinator.DeliveryState, result.Coordinator.DeliveryKey = current.DeliveryState, current.DeliveryKey
			if current.DeliveryPosition != nil {
				position := JournalPosition(*current.DeliveryPosition)
				result.Coordinator.DeliveryPosition = &position
			}
			result.Coordinator.AttemptOrdinal, result.Coordinator.ConsumedAttempts = current.AttemptOrdinal, current.ConsumedAttempts
			result.Coordinator.LeaseOwner = current.LeaseOwner
			result.Coordinator.LeaseStartedAt = cloneTimePointer(current.LeaseStartedAt)
			result.Coordinator.LeaseExpiresAt = cloneTimePointer(current.LeaseExpiresAt)
			result.Coordinator.CreatedAt, result.Coordinator.UpdatedAt = current.CreatedAt, current.UpdatedAt
			result.Coordinator.FinishedAt = cloneTimePointer(current.FinishedAt)
		}
	}
	if ownedTx != nil {
		if err := ownedTx.Commit(ctx); err != nil {
			return ExecutionTrace{}, store.MapError("commit trace snapshot", err)
		}
	}
	return result, nil
}

func traceAttempts(attempts []replay.Attempt) []TraceAttempt {
	result := make([]TraceAttempt, len(attempts))
	for index, attempt := range attempts {
		result[index] = TraceAttempt{
			ID: AttemptID(attempt.ID.String()), Attempt: attempt.Ordinal, StartedAt: attempt.StartedAt,
			FinishedAt: cloneTimePointer(attempt.FinishedAt), Worker: attempt.Worker,
			Classification: attempt.Classification, ConsumedBudget: attempt.ConsumedBudget,
			ConsumedAttempts: attempt.ConsumedAttempts, NextAttemptAt: cloneTimePointer(attempt.NextAttemptAt),
			ErrorCode: attempt.ErrorCode, ErrorMessage: attempt.ErrorMessage,
		}
	}
	return result
}

func cloneJournalPosition(value *JournalPosition) *JournalPosition {
	if value == nil {
		return nil
	}
	copy := *value
	return &copy
}

func cloneIntPointer(value *int) *int {
	if value == nil {
		return nil
	}
	copy := *value
	return &copy
}

func cloneTimePointer(value *time.Time) *time.Time {
	if value == nil {
		return nil
	}
	copy := *value
	return &copy
}
