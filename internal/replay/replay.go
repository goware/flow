// Package replay folds retained journal entries into settled orchestration
// projections. It never invokes application code or reconstructs live leases.
package replay

import (
	"crypto/sha256"
	"encoding/hex"
	"errors"
	"fmt"
	"slices"
	"time"

	"github.com/google/uuid"
	"github.com/goware/flow/internal/store"
	"github.com/goware/flow/internal/store/journalcodec"
)

type Execution struct {
	Initialized       bool
	ID                uuid.UUID
	DriverMode        store.DriverMode
	DefinitionName    string
	DefinitionVersion int
	ExecutionKey      string
	Status            string
	FailFast          bool
	MaxCommands       int
	CommandCount      int
	OpenCommands      int
	PlanDirty         bool
	RootCommandID     *uuid.UUID
	LastPosition      int64
	Input             []byte
	Metadata          []byte
	DeadlineAt        *time.Time
	Commands          map[uuid.UUID]Command
	Events            []Event
	Coordinator       *Coordinator
}

type Command struct {
	ID               uuid.UUID
	Key              string
	Name             string
	Version          int
	Origin           string
	Required         bool
	State            string
	Args             []byte
	Queue            string
	RetryPolicy      []byte
	AttemptTimeoutMS *int64
	CreatedPosition  int64
	TerminalPosition *int64
	Result           []byte
	FailureCode      string
	FailureMessage   string
	Attempts         []Attempt
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
	ErrorCode        string
	ErrorMessage     string
}

type Event struct {
	ID             uuid.UUID
	Position       int64
	Namespace      string
	Name           string
	Version        int
	Key            string
	Class          string
	TerminalStatus string
	CommandID      *uuid.UUID
	Body           []byte
}

type Coordinator struct {
	ID            uuid.UUID
	Name          string
	Version       int
	Status        string
	State         []byte
	StatePosition int64
	StateRevision int64
	StartPending  bool
	DeliveryState string
	DeliveryKey   string
	RetryPolicy   []byte
}

func New() Execution { return Execution{Commands: make(map[uuid.UUID]Command)} }

func Fold(rows []store.JournalRow) (Execution, error) {
	state := New()
	for _, row := range rows {
		if err := state.Apply(row); err != nil {
			return Execution{}, err
		}
	}
	return state, nil
}

func (state *Execution) Apply(row store.JournalRow) error {
	if state == nil {
		return errors.New("replay state is nil")
	}
	if sha256.Sum256(row.Body) != row.BodyHash {
		return fmt.Errorf("journal body hash differs at position %d", row.Position)
	}
	if state.LastPosition == 0 {
		if row.Position != 1 || row.Kind != store.ExecutionStarted {
			return fmt.Errorf("history does not begin with ExecutionStarted")
		}
	} else if row.Position != state.LastPosition+1 || row.ExecutionID != state.ID {
		return fmt.Errorf("journal position or execution changed at %d", row.Position)
	}

	switch row.Kind {
	case store.ExecutionStarted:
		if state.Initialized {
			return errors.New("execution started more than once")
		}
		body, err := journalcodec.Decode[journalcodec.ExecutionStartedBody](row.Body)
		if err != nil {
			return err
		}
		id, err := uuid.Parse(body.ExecutionID)
		if err != nil || id != row.ExecutionID {
			return errors.New("ExecutionStarted identity differs")
		}
		state.Initialized = true
		state.ID = id
		state.DriverMode = store.DriverMode(body.DriverMode)
		state.DefinitionName = body.DefinitionName
		state.DefinitionVersion = body.DefinitionVersion
		state.ExecutionKey = body.ExecutionKey
		state.Status = "running"
		state.FailFast = body.FailFast
		state.MaxCommands = body.MaxCommands
		state.PlanDirty = state.DriverMode == store.DriverPlan
		state.Input = slices.Clone(body.Input)
		state.Metadata = slices.Clone(body.Metadata)
		state.DeadlineAt = pointerClone(body.DeadlineAt)
		if state.DriverMode == store.DriverCoordinator {
			coordinatorID, err := uuid.Parse(body.CoordinatorID)
			if err != nil || len(body.CoordinatorPolicy) == 0 {
				return errors.New("coordinator ExecutionStarted record is incomplete")
			}
			state.Coordinator = &Coordinator{
				ID: coordinatorID, Name: body.DefinitionName, Version: body.DefinitionVersion,
				Status: "active", State: slices.Clone(body.Input), StatePosition: row.Position,
				StartPending: true, DeliveryState: "ready", DeliveryKey: "start",
				RetryPolicy: slices.Clone(body.CoordinatorPolicy),
			}
		}

	case store.CommandCreated:
		if !state.Initialized || row.CommandID == nil {
			return errors.New("CommandCreated has no initialized execution or command")
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
		if _, err := hex.DecodeString(body.DeclarationFingerprint); err != nil {
			return errors.New("CommandCreated declaration fingerprint is invalid")
		}
		state.Commands[bodyID] = Command{
			ID: bodyID, Key: body.CommandKey, Name: body.Name, Version: body.Version,
			Origin: body.Origin, Required: body.Required, State: body.InitialState,
			Args: slices.Clone(body.Args), Queue: body.Queue, RetryPolicy: slices.Clone(body.RetryPolicy),
			AttemptTimeoutMS: pointerClone(body.AttemptTimeoutMS), CreatedPosition: row.Position,
		}
		state.CommandCount++
		state.OpenCommands++
		if state.DriverMode == store.DriverDirect && body.CommandKey == "root" {
			state.RootCommandID = pointer(bodyID)
		}

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
			command.Attempts[index].ErrorCode = body.ErrorCode
			command.Attempts[index].ErrorMessage = body.ErrorMessage
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

	case store.ExecutionFailing:
		state.Status = "failing"

	case store.EventRecorded:
		if row.EventClass == nil {
			return errors.New("EventRecorded has no class")
		}
		if row.EventID == nil || row.EventNamespace == nil || row.EventName == nil || row.EventVersion == nil {
			return errors.New("EventRecorded has incomplete selector metadata")
		}
		event := Event{
			ID: *row.EventID, Position: row.Position, Namespace: *row.EventNamespace,
			Name: *row.EventName, Version: *row.EventVersion, Class: *row.EventClass,
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
			// Application facts affect plans/coordinators, not the settled
			// execution or command projection represented in this phase.
			if state.DriverMode == store.DriverPlan {
				state.PlanDirty = true
			}
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
				command.FailureCode = body.Code
				if command.FailureCode == "" {
					command.FailureCode = *row.TerminalStatus
				}
				command.FailureMessage = body.Reason
			}
			state.Commands[*row.CommandID] = command
			state.OpenCommands--
			if state.DriverMode == store.DriverPlan {
				state.PlanDirty = true
			}
		case "execution_terminal":
			if row.TerminalStatus == nil {
				return errors.New("execution terminal event has no status")
			}
			state.Status = *row.TerminalStatus
			state.PlanDirty = false
			if state.Coordinator != nil {
				switch *row.TerminalStatus {
				case "succeeded":
					state.Coordinator.Status = "completed"
				case "cancelled":
					state.Coordinator.Status = "cancelled"
				default:
					state.Coordinator.Status = "failed"
				}
				state.Coordinator.StartPending = false
				state.Coordinator.DeliveryState = "idle"
				state.Coordinator.DeliveryKey = ""
			}
		}
	}
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
	case "succeeded", "failed", "cancelled", "expired", "skipped":
		return true
	default:
		return false
	}
}
