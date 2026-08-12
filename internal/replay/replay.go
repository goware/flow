// Package replay folds retained journal entries into settled orchestration
// projections. It never invokes application code or reconstructs live leases.
package replay

import (
	"bytes"
	"crypto/sha256"
	"encoding/hex"
	"errors"
	"fmt"
	"slices"
	"time"

	"github.com/google/uuid"
	"github.com/goware/flow/internal/canonical"
	"github.com/goware/flow/internal/failure"
	"github.com/goware/flow/internal/store"
	"github.com/goware/flow/internal/store/journalcodec"
)

type Run struct {
	Initialized       bool
	ID                uuid.UUID
	DefinitionName    string
	DefinitionVersion int
	RunKey            string
	Status            string
	MaxCommands       int
	CommandCount      int
	OpenCommands      int
	RootCommandID     *uuid.UUID
	LastPosition      int64
	Failure           *failure.Value
	CreatedAt         time.Time
	UpdatedAt         time.Time
	StatusAt          time.Time
	FinishedAt        *time.Time
	DeadlineAt        *time.Time
	Commands          map[uuid.UUID]Command
	Events            []Event
}

type Command struct {
	ID                     uuid.UUID
	Key                    string
	Name                   string
	Version                int
	ParentCommandID        *uuid.UUID
	State                  string
	Args                   []byte
	DeclarationFingerprint [sha256.Size]byte
	Queue                  string
	RetryPolicy            []byte
	AttemptTimeoutMS       *int64
	InitialDelayMS         *int64
	BudgetStartedAt        *time.Time
	NextAttemptAt          *time.Time
	Waits                  []journalcodec.EventWaitBody
	WithinMS               *int64
	CreatedPosition        int64
	TerminalPosition       *int64
	Result                 []byte
	Failure                *failure.Value
	Attempts               []Attempt
}

type Attempt struct {
	ID               uuid.UUID
	Ordinal          int
	StartedAt        time.Time
	FinishedAt       *time.Time
	Worker           string
	Classification   string
	ConsumedBudget   bool
	ConsumedAttempts int
	NextAttemptAt    *time.Time
	Failure          *failure.Value
}

type Event struct {
	ID             uuid.UUID
	Position       int64
	Namespace      string
	Name           string
	Key            string
	Class          string
	TerminalStatus string
	CommandID      *uuid.UUID
	Body           []byte
}

func New() Run { return Run{Commands: make(map[uuid.UUID]Command)} }

func Fold(rows []store.JournalRow) (Run, error) {
	state := New()
	for _, row := range rows {
		if err := state.Apply(row); err != nil {
			return Run{}, err
		}
	}
	return state, nil
}

func (state *Run) Apply(row store.JournalRow) error {
	if state == nil {
		return errors.New("replay state is nil")
	}
	if sha256.Sum256(row.Body) != row.BodyHash {
		return fmt.Errorf("journal body hash differs at position %d", row.Position)
	}
	canonicalBody, err := canonical.Canonicalize(row.Body, 0)
	if err != nil {
		return fmt.Errorf("journal body is invalid at position %d: %w", row.Position, err)
	}
	if !bytes.Equal(canonicalBody.Bytes, row.Body) {
		return fmt.Errorf("journal body is noncanonical at position %d", row.Position)
	}
	if state.LastPosition == 0 {
		if row.Position != 1 || row.Kind != store.RunStarted {
			return fmt.Errorf("history does not begin with RunStarted")
		}
	} else if row.Position != state.LastPosition+1 || row.RunID != state.ID {
		return fmt.Errorf("journal position or run changed at %d", row.Position)
	}
	if state.Initialized && state.RootCommandID == nil && row.Kind != store.CommandCreated {
		return errors.New("root CommandCreated does not immediately follow RunStarted")
	}

	switch row.Kind {
	case store.RunStarted:
		if state.Initialized {
			return errors.New("run started more than once")
		}
		body, err := journalcodec.Decode[journalcodec.RunStartedBody](row.Body)
		if err != nil {
			return err
		}
		id, err := uuid.Parse(body.RunID)
		if err != nil || id != row.RunID {
			return errors.New("RunStarted identity differs")
		}
		state.Initialized = true
		state.ID = id
		state.DefinitionName = body.DefinitionName
		state.DefinitionVersion = body.DefinitionVersion
		state.RunKey = body.RunKey
		state.Status = "running"
		state.CreatedAt = row.RecordedAt
		state.StatusAt = row.RecordedAt
		state.MaxCommands = body.MaxCommands
		state.DeadlineAt = pointerClone(body.DeadlineAt)

	case store.CommandCreated:
		if !state.Initialized || row.CommandID == nil {
			return errors.New("CommandCreated has no initialized run or command")
		}
		if state.RootCommandID == nil && row.Position != 2 {
			return errors.New("root CommandCreated does not immediately follow RunStarted")
		}
		if _, exists := state.Commands[*row.CommandID]; exists {
			return errors.New("command created more than once")
		}
		body, err := journalcodec.Decode[journalcodec.CommandCreatedBody](row.Body)
		if err != nil {
			return err
		}
		bodyID, err := uuid.Parse(body.CommandID)
		if err != nil || bodyID != *row.CommandID {
			return errors.New("CommandCreated identity differs")
		}
		declarationFingerprint, err := hex.DecodeString(body.DeclarationFingerprint)
		if err != nil || len(declarationFingerprint) != sha256.Size {
			return errors.New("CommandCreated declaration fingerprint is invalid")
		}
		command := Command{
			ID: bodyID, Key: body.CommandKey, Name: body.Name, Version: body.Version,
			State: body.InitialState,
			Args:  slices.Clone(body.Args), Queue: body.Queue, RetryPolicy: slices.Clone(body.RetryPolicy),
			AttemptTimeoutMS: pointerClone(body.AttemptTimeoutMS), CreatedPosition: row.Position,
			InitialDelayMS:  pointerClone(body.InitialDelayMS),
			BudgetStartedAt: pointerClone(body.BudgetStartedAt), NextAttemptAt: pointerClone(body.NextAttemptAt),
			Waits: slices.Clone(body.Waits), WithinMS: pointerClone(body.WithinMS),
		}
		copy(command.DeclarationFingerprint[:], declarationFingerprint)
		if body.ParentCommandID != "" {
			if state.RootCommandID == nil {
				return errors.New("child CommandCreated precedes the root command")
			}
			parentID, err := uuid.Parse(body.ParentCommandID)
			if err != nil {
				return errors.New("CommandCreated parent identity is invalid")
			}
			command.ParentCommandID = &parentID
		} else {
			if state.RootCommandID != nil {
				return errors.New("run has more than one root command")
			}
			state.RootCommandID = pointer(bodyID)
		}
		state.Commands[bodyID] = command
		state.CommandCount++
		state.OpenCommands++
	case store.AttemptStarted:
		if row.CommandID == nil || row.AttemptID == nil {
			return errors.New("AttemptStarted has no command or attempt")
		}
		command, ok := state.Commands[*row.CommandID]
		if !ok || isTerminal(command.State) {
			return errors.New("AttemptStarted has invalid command state")
		}
		body, err := journalcodec.Decode[journalcodec.AttemptStartedBody](row.Body)
		if err != nil {
			return err
		}
		bodyAttemptID, err := uuid.Parse(body.AttemptID)
		if err != nil || bodyAttemptID != *row.AttemptID || body.CommandID != row.CommandID.String() {
			return errors.New("AttemptStarted identity differs")
		}
		for _, attempt := range command.Attempts {
			if attempt.ID == bodyAttemptID {
				return errors.New("attempt started more than once")
			}
		}
		command.State = "running"
		command.Attempts = append(command.Attempts, Attempt{
			ID: bodyAttemptID, Ordinal: body.Attempt, StartedAt: body.StartedAt,
			Worker: body.Worker, ConsumedAttempts: body.ConsumedAttempts,
		})
		state.Commands[*row.CommandID] = command

	case store.AttemptConcluded:
		if row.CommandID == nil || row.AttemptID == nil {
			return errors.New("AttemptConcluded has no command or attempt")
		}
		command, ok := state.Commands[*row.CommandID]
		if !ok || len(command.Attempts) == 0 {
			return errors.New("AttemptConcluded has no started attempt")
		}
		body, err := journalcodec.Decode[journalcodec.AttemptConcludedBody](row.Body)
		if err != nil {
			return err
		}
		attemptID, err := uuid.Parse(body.AttemptID)
		if err != nil || attemptID != *row.AttemptID {
			return errors.New("AttemptConcluded identity differs")
		}
		found := false
		for index := range command.Attempts {
			if command.Attempts[index].ID != attemptID {
				continue
			}
			if command.Attempts[index].FinishedAt != nil {
				return errors.New("attempt concluded more than once")
			}
			command.Attempts[index].FinishedAt = pointer(body.FinishedAt)
			command.Attempts[index].Classification = body.Classification
			command.Attempts[index].ConsumedBudget = body.ConsumedBudget
			command.Attempts[index].ConsumedAttempts = body.ConsumedAttempts
			command.Attempts[index].NextAttemptAt = pointerClone(body.NextAttemptAt)
			if body.ErrorCode != "" || body.ErrorMessage != "" {
				command.Attempts[index].Failure = &failure.Value{Code: body.ErrorCode, Message: body.ErrorMessage}
			}
			found = true
			break
		}
		if !found {
			return errors.New("AttemptConcluded references unknown attempt")
		}
		if body.NextAttemptAt != nil {
			command.State = "retry_wait"
		}
		state.Commands[*row.CommandID] = command

	case store.RunFailing:
		if state.Status != "running" {
			return errors.New("RunFailing has invalid prior state")
		}
		body, err := journalcodec.Decode[journalcodec.RunFailingBody](row.Body)
		if err != nil {
			return err
		}
		if body.Status != "failing" || body.CommandKey == "" {
			return errors.New("RunFailing body is incomplete")
		}
		state.Status = "failing"
		state.StatusAt = row.RecordedAt

	case store.EventRecorded:
		if row.EventClass == nil {
			return errors.New("EventRecorded has no class")
		}
		if row.EventID == nil || row.EventNamespace == nil || row.EventName == nil {
			return errors.New("EventRecorded has incomplete selector metadata")
		}
		event := Event{
			ID: *row.EventID, Position: row.Position, Namespace: *row.EventNamespace,
			Name: *row.EventName, Class: *row.EventClass,
			CommandID: pointerClone(row.CommandID), Body: slices.Clone(row.Body),
		}
		if row.EventKey != nil {
			event.Key = *row.EventKey
		}
		if row.TerminalStatus != nil {
			event.TerminalStatus = *row.TerminalStatus
		}
		state.Events = append(state.Events, event)
		switch *row.EventClass {
		case "application":
			if _, err := journalcodec.DecodeApplicationEvent(row.Body); err != nil {
				return fmt.Errorf("application event body is invalid: %w", err)
			}
			// Exact wait readiness is projected operationally from retained rows.
		case "command_terminal":
			if row.CommandID == nil || row.TerminalStatus == nil {
				return errors.New("command terminal event has no subject or status")
			}
			command, ok := state.Commands[*row.CommandID]
			if !ok || isTerminal(command.State) {
				return errors.New("command terminal event has invalid prior state")
			}
			command.State = *row.TerminalStatus
			command.TerminalPosition = pointer(row.Position)
			if *row.TerminalStatus == "succeeded" {
				body, err := journalcodec.Decode[journalcodec.CommandSucceededBody](row.Body)
				if err != nil {
					return err
				}
				command.Result = slices.Clone(body.Result)
			} else {
				body, err := journalcodec.Decode[journalcodec.TerminalEventBody](row.Body)
				if err != nil {
					return err
				}
				code := body.Code
				if code == "" {
					code = *row.TerminalStatus
				}
				command.Failure = &failure.Value{Code: code, Message: body.Reason}
			}
			state.Commands[*row.CommandID] = command
			state.OpenCommands--
		case "run_terminal":
			if row.TerminalStatus == nil {
				return errors.New("run terminal event has no status")
			}
			state.Status = *row.TerminalStatus
			state.StatusAt = row.RecordedAt
			state.FinishedAt = pointer(row.RecordedAt)
			if *row.TerminalStatus != "succeeded" {
				body, err := journalcodec.Decode[journalcodec.TerminalEventBody](row.Body)
				if err != nil {
					return err
				}
				code := body.Code
				if code == "" {
					code = *row.TerminalStatus
				}
				state.Failure = &failure.Value{Code: code, Message: body.Reason}
			}
		}
	}
	state.UpdatedAt = row.RecordedAt
	state.LastPosition = row.Position
	return nil
}

func pointer[T any](value T) *T { return &value }

func pointerClone[T any](value *T) *T {
	if value == nil {
		return nil
	}
	return pointer(*value)
}

func isTerminal(state string) bool {
	switch state {
	case "succeeded", "failed", "cancelled", "expired":
		return true
	default:
		return false
	}
}
