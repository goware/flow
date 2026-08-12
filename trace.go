package flow

import (
	"context"
	"encoding/json"
	"errors"
	"sort"
	"time"

	"github.com/goware/flow/internal/durable"
	"github.com/goware/flow/internal/replay"
	"github.com/goware/flow/internal/store"
	"github.com/jackc/pgx/v5"
)

// Run is a durable run state snapshot. Enqueue returns the
// snapshot as of durable acceptance; GetRun, AwaitRun, and other
// inspection reads return the current or final state. Created reports whether
// the producing Enqueue call created the run; it is false for an
// idempotent rediscovery and always false on inspection reads.
type Run struct {
	ID            RunID
	Type          string
	Version       int
	Key           string
	RootCommandID CommandID
	Status        RunStatus
	FailFast      bool
	MaxCommands   int
	CommandCount  int
	OpenCommands  int
	DeadlineAt    *time.Time
	Failure       *Failure
	CreatedAt     time.Time
	UpdatedAt     time.Time
	StatusAt      time.Time
	FinishedAt    *time.Time
	Metadata      json.RawMessage
	Created       bool
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
	Failure          *Failure
}

type TraceCommand struct {
	ID               CommandID
	Key              string
	Name             string
	Version          int
	ParentCommandID  CommandID
	Required         bool
	State            CommandStatus
	Args             json.RawMessage
	Result           json.RawMessage
	Queue            string
	InitialDelay     time.Duration
	BudgetStartedAt  *time.Time
	NextAttemptAt    *time.Time
	Within           time.Duration
	Waits            []TraceEventWait
	CreatedPosition  JournalPosition
	TerminalPosition *JournalPosition
	Failure          *Failure
	LastError        *Failure
	UnsatisfiedWaits int
	AttemptOrdinal   int
	ConsumedAttempts int
	WaitStartedAt    *time.Time
	WaitDeadlineAt   *time.Time
	DeliveryState    QueueState
	LeaseOwner       string
	LeaseStartedAt   *time.Time
	LeaseExpiresAt   *time.Time
	CreatedAt        time.Time
	UpdatedAt        time.Time
	StatusAt         time.Time
	FinishedAt       *time.Time
	Attempts         []TraceAttempt
}

type TraceEventWait struct {
	Name              string
	Key               string
	SatisfiedPosition *JournalPosition
}

type TraceEvent struct {
	ID                EventID
	Position          JournalPosition
	Namespace         string
	Name              string
	Key               string
	Class             string
	TerminalStatus    TerminalStatus
	CommandID         CommandID
	RecordedAt        time.Time
	CausationPosition *JournalPosition
	Body              json.RawMessage
}

type RunTrace struct {
	Run      Run
	Commands []TraceCommand
	Events   []TraceEvent
	History  []HistoryEntry
}

type TraceOption interface {
	applyTrace(*traceOptions)
}

type traceOptions struct{ errs []error }

const maxTraceEntries = 100_000

// Trace reconstructs one run and overlays its current operational
// projections. A Runtime client gets one Flow-owned Repeatable Read snapshot.
// A transaction-scoped client inherits the caller's isolation; callers needing
// a coherent multi-statement snapshot must use Repeatable Read or Serializable.
func Trace(ctx context.Context, c Client, id RunID, opts ...TraceOption) (RunTrace, error) {
	runID, err := parseRunID(id)
	if err != nil {
		return RunTrace{}, err
	}
	client, err := resolveClient(c)
	if err != nil {
		return RunTrace{}, err
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
		return RunTrace{}, newError(ErrInvalid, "trace", "options", "", err.Error())
	}
	var ownedTx pgx.Tx
	if client.tx == nil {
		ownedTx, err = client.runtime.db.Conn.BeginTx(ctx, pgx.TxOptions{IsoLevel: pgx.RepeatableRead, AccessMode: pgx.ReadOnly})
		if err != nil {
			return RunTrace{}, store.MapError("begin trace snapshot", err)
		}
		defer ownedTx.Rollback(context.WithoutCancel(ctx))
		client.tx = ownedTx
	}
	rows := make([]store.JournalRow, 0, 64)
	var after uint64
	for len(rows) < maxTraceEntries {
		page, err := client.runtime.store.HistoryInTx(ctx, client.tx, runID, after, store.MaxHistoryLimit)
		if err != nil {
			return RunTrace{}, err
		}
		rows = append(rows, page...)
		if len(page) < store.MaxHistoryLimit {
			break
		}
		after = uint64(page[len(page)-1].Position)
	}
	if len(rows) == 0 {
		return RunTrace{}, newError(ErrNotFound, "trace", "run", string(id), "run does not exist")
	}
	if len(rows) >= maxTraceEntries {
		return RunTrace{}, newError(ErrInvalid, "trace", "run", string(id), "trace exceeds the initial bounded history limit")
	}
	state, err := replay.Fold(rows)
	if err != nil {
		return RunTrace{}, newError(ErrInvalidState, "trace", "run", string(id), "retained history cannot be replayed")
	}
	live, err := client.runtime.store.GetRunInTx(ctx, client.tx, runID)
	if err != nil {
		return RunTrace{}, err
	}
	run, err := runFromStore(live)
	if err != nil {
		return RunTrace{}, err
	}
	history, err := historyEntries(rows)
	if err != nil {
		return RunTrace{}, err
	}
	result := RunTrace{Run: run, History: history}
	operational, err := client.runtime.store.TraceOperationalInTx(ctx, client.tx, runID)
	if err != nil {
		return RunTrace{}, err
	}
	operationalCommands := make(map[string]store.TraceCommandRow, len(operational.Commands))
	for _, command := range operational.Commands {
		operationalCommands[command.ID.String()] = command
	}
	operationalWaits := make(map[string]*int64, len(operational.Waits))
	for _, wait := range operational.Waits {
		operationalWaits[wait.CommandID.String()+"\x00"+wait.Name+"\x00"+wait.Key] = wait.SatisfiedPosition
	}
	result.Commands = make([]TraceCommand, 0, len(state.Commands))
	for _, command := range state.Commands {
		status, err := commandStatusFromString(command.State)
		if err != nil {
			return RunTrace{}, newError(ErrInvalidState, "trace", "command status", command.State, "replayed status is unknown")
		}
		item := TraceCommand{
			ID: CommandID(command.ID.String()), Key: command.Key, Name: command.Name, Version: command.Version,
			Required: command.Required, State: status,
			Args: json.RawMessage(append([]byte(nil), command.Args...)), Result: json.RawMessage(append([]byte(nil), command.Result...)),
			Queue: command.Queue, CreatedPosition: JournalPosition(command.CreatedPosition),
			BudgetStartedAt: cloneTimePointer(command.BudgetStartedAt), NextAttemptAt: cloneTimePointer(command.NextAttemptAt),
			Failure: cloneFailure(command.Failure),
		}
		if command.ParentCommandID != nil {
			item.ParentCommandID = CommandID(command.ParentCommandID.String())
		}
		if command.InitialDelayMS != nil {
			item.InitialDelay, err = durable.MillisecondsDuration("replayed command initial delay", *command.InitialDelayMS)
			if err != nil {
				return RunTrace{}, newError(ErrInvalidState, "trace", "initial delay", "", "replayed duration is out of range")
			}
		}
		if command.WithinMS != nil {
			item.Within, err = durable.MillisecondsDuration("replayed command within", *command.WithinMS)
			if err != nil {
				return RunTrace{}, newError(ErrInvalidState, "trace", "within", "", "replayed duration is out of range")
			}
		}
		for _, wait := range command.Waits {
			traceWait := TraceEventWait{Name: wait.Name, Key: wait.Key}
			if position := operationalWaits[command.ID.String()+"\x00"+wait.Name+"\x00"+wait.Key]; position != nil {
				value := JournalPosition(*position)
				traceWait.SatisfiedPosition = &value
			}
			item.Waits = append(item.Waits, traceWait)
		}
		if command.TerminalPosition != nil {
			position := JournalPosition(*command.TerminalPosition)
			item.TerminalPosition = &position
		}
		if current, ok := operationalCommands[command.ID.String()]; ok {
			item.State, err = commandStatusFromString(current.State)
			if err != nil {
				return RunTrace{}, newError(ErrInvalidState, "trace", "command status", current.State, "stored status is unknown")
			}
			item.BudgetStartedAt = cloneTimePointer(current.BudgetStartedAt)
			item.NextAttemptAt = cloneTimePointer(current.NextAttemptAt)
			item.LastError = cloneFailure(current.LastError)
			item.UnsatisfiedWaits = current.UnsatisfiedWaits
			item.AttemptOrdinal, item.ConsumedAttempts = current.AttemptOrdinal, current.ConsumedAttempts
			item.WaitStartedAt, item.WaitDeadlineAt = cloneTimePointer(current.WaitStartedAt), cloneTimePointer(current.WaitDeadlineAt)
			if current.DeliveryState != "" {
				item.DeliveryState, err = queueStateFromString(current.DeliveryState)
				if err != nil {
					return RunTrace{}, newError(ErrInvalidState, "trace", "queue state", current.DeliveryState, "stored state is unknown")
				}
			}
			item.LeaseOwner = current.LeaseOwner
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
				Failure: cloneFailure(attempt.Failure),
			}
		}
		result.Commands = append(result.Commands, item)
	}
	sort.Slice(result.Commands, func(i, j int) bool { return result.Commands[i].Key < result.Commands[j].Key })
	result.Events = make([]TraceEvent, 0, len(state.Events))
	historyByPosition := make(map[JournalPosition]HistoryEntry, len(result.History))
	for _, entry := range result.History {
		historyByPosition[entry.Position] = entry
	}
	for _, event := range state.Events {
		entry := historyByPosition[JournalPosition(event.Position)]
		var terminalStatus TerminalStatus
		if event.TerminalStatus != "" {
			terminalStatus, err = terminalStatusFromString(event.TerminalStatus)
			if err != nil {
				return RunTrace{}, newError(ErrInvalidState, "trace", "terminal status", event.TerminalStatus, "replayed status is unknown")
			}
		}
		item := TraceEvent{
			ID: EventID(event.ID.String()), Position: JournalPosition(event.Position), Namespace: event.Namespace,
			Name: event.Name, Key: event.Key, Class: event.Class,
			TerminalStatus: terminalStatus, RecordedAt: entry.RecordedAt,
			CausationPosition: cloneJournalPosition(entry.CausationPosition),
			Body:              json.RawMessage(append([]byte(nil), event.Body...)),
		}
		if event.CommandID != nil {
			item.CommandID = CommandID(event.CommandID.String())
		}
		result.Events = append(result.Events, item)
	}
	if ownedTx != nil {
		if err := ownedTx.Commit(ctx); err != nil {
			return RunTrace{}, store.MapError("commit trace snapshot", err)
		}
	}
	return result, nil
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
