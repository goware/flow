package flow

import (
	"context"
	"encoding/json"
	"errors"
	"sort"
	"time"

	"github.com/goware/flow/internal/replay"
	"github.com/goware/flow/internal/store"
)

type Execution struct {
	ID           ExecutionID
	Mode         string
	Type         string
	Version      int
	Key          string
	Status       string
	FailFast     bool
	MaxCommands  int
	CommandCount int
	OpenCommands int
	DeadlineAt   *time.Time
	CreatedAt    time.Time
	UpdatedAt    time.Time
	Metadata     json.RawMessage
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
	ID               CommandID
	Key              string
	Name             string
	Version          int
	Origin           string
	Required         bool
	State            string
	Args             json.RawMessage
	Result           json.RawMessage
	Queue            string
	CreatedPosition  JournalPosition
	TerminalPosition *JournalPosition
	FailureCode      string
	FailureMessage   string
	Attempts         []TraceAttempt
}

type ExecutionTrace struct {
	Execution Execution
	Commands  []TraceCommand
	History   []HistoryEntry
	results   resultSourceState
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
	result := ExecutionTrace{Execution: Execution{
		ID: id, Mode: string(state.DriverMode), Type: state.DefinitionName, Version: state.DefinitionVersion,
		Key: state.ExecutionKey, Status: state.Status, FailFast: state.FailFast, MaxCommands: state.MaxCommands,
		CommandCount: state.CommandCount, OpenCommands: state.OpenCommands, DeadlineAt: cloneTimePointer(state.DeadlineAt),
		CreatedAt: rows[0].RecordedAt, UpdatedAt: rows[len(rows)-1].RecordedAt,
		Metadata: json.RawMessage(append([]byte(nil), state.Metadata...)),
	}, History: historyEntries(rows)}
	result.Commands = make([]TraceCommand, 0, len(state.Commands))
	for _, command := range state.Commands {
		item := TraceCommand{
			ID: CommandID(command.ID.String()), Key: command.Key, Name: command.Name, Version: command.Version,
			Origin: command.Origin, Required: command.Required, State: command.State,
			Args: json.RawMessage(append([]byte(nil), command.Args...)), Result: json.RawMessage(append([]byte(nil), command.Result...)),
			Queue: command.Queue, CreatedPosition: JournalPosition(command.CreatedPosition),
			FailureCode: command.FailureCode, FailureMessage: command.FailureMessage,
		}
		if command.TerminalPosition != nil {
			position := JournalPosition(*command.TerminalPosition)
			item.TerminalPosition = &position
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
	}
	sort.Slice(result.Commands, func(i, j int) bool { return result.Commands[i].Key < result.Commands[j].Key })
	return result, nil
}

func cloneTimePointer(value *time.Time) *time.Time {
	if value == nil {
		return nil
	}
	copy := *value
	return &copy
}
