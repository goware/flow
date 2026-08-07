package flow

import (
	"fmt"
	"time"

	"github.com/goware/flow/internal/failure"
	"github.com/goware/flow/internal/flowerr"
)

type None = struct{}

type ExecutionID string
type CommandID string
type EventID string
type AttemptID string
type JournalEntryID string
type JournalPosition uint64

type ExecutionStatus string
type CommandStatus string
type QueueState string
type KeyScope string
type TerminalStatus string

const (
	ExecutionStatusRunning   ExecutionStatus = "running"
	ExecutionStatusFailing   ExecutionStatus = "failing"
	ExecutionStatusSucceeded ExecutionStatus = "succeeded"
	ExecutionStatusFailed    ExecutionStatus = "failed"
	ExecutionStatusCancelled ExecutionStatus = "cancelled"
	ExecutionStatusExpired   ExecutionStatus = "expired"

	CommandStatusPending   CommandStatus = "pending"
	CommandStatusReady     CommandStatus = "ready"
	CommandStatusRunning   CommandStatus = "running"
	CommandStatusRetryWait CommandStatus = "retry_wait"
	CommandStatusSucceeded CommandStatus = "succeeded"
	CommandStatusFailed    CommandStatus = "failed"
	CommandStatusCancelled CommandStatus = "cancelled"
	CommandStatusExpired   CommandStatus = "expired"

	QueueStateReady     QueueState = "ready"
	QueueStateRetryWait QueueState = "retry_wait"
	QueueStateRunning   QueueState = "running"

	KeyScopePermanent KeyScope = "permanent"
	KeyScopeLive      KeyScope = "live"

	TerminalStatusSucceeded TerminalStatus = "succeeded"
	TerminalStatusFailed    TerminalStatus = "failed"
	TerminalStatusCancelled TerminalStatus = "cancelled"
	TerminalStatusExpired   TerminalStatus = "expired"

	// Short names remain source-compatible aliases for terminal command states.
	StatusSucceeded = CommandStatusSucceeded
	StatusFailed    = CommandStatusFailed
	StatusCancelled = CommandStatusCancelled
	StatusExpired   = CommandStatusExpired
)

type Failure = failure.Value
type CommandFailure = Failure

func cloneFailure(value *Failure) *Failure { return failure.Clone(value) }

func executionStatusFromString(value string) (ExecutionStatus, error) {
	switch ExecutionStatus(value) {
	case ExecutionStatusRunning, ExecutionStatusFailing, ExecutionStatusSucceeded,
		ExecutionStatusFailed, ExecutionStatusCancelled, ExecutionStatusExpired:
		return ExecutionStatus(value), nil
	default:
		return "", fmt.Errorf("%w: unknown execution status %q", flowerr.ErrInvalidState, value)
	}
}

func commandStatusFromString(value string) (CommandStatus, error) {
	switch CommandStatus(value) {
	case CommandStatusPending, CommandStatusReady, CommandStatusRunning, CommandStatusRetryWait,
		CommandStatusSucceeded, CommandStatusFailed, CommandStatusCancelled, CommandStatusExpired:
		return CommandStatus(value), nil
	default:
		return "", fmt.Errorf("%w: unknown command status %q", flowerr.ErrInvalidState, value)
	}
}

func queueStateFromString(value string) (QueueState, error) {
	switch QueueState(value) {
	case QueueStateReady, QueueStateRetryWait, QueueStateRunning:
		return QueueState(value), nil
	default:
		return "", fmt.Errorf("%w: unknown queue state %q", flowerr.ErrInvalidState, value)
	}
}

func keyScopeFromString(value string) (KeyScope, error) {
	switch KeyScope(value) {
	case KeyScopePermanent, KeyScopeLive:
		return KeyScope(value), nil
	default:
		return "", fmt.Errorf("%w: unknown key scope %q", flowerr.ErrInvalidState, value)
	}
}

func terminalStatusFromString(value string) (TerminalStatus, error) {
	switch TerminalStatus(value) {
	case TerminalStatusSucceeded, TerminalStatusFailed, TerminalStatusCancelled, TerminalStatusExpired:
		return TerminalStatus(value), nil
	default:
		return "", fmt.Errorf("%w: unknown terminal status %q", flowerr.ErrInvalidState, value)
	}
}

type CommandInfo struct {
	ExecutionID ExecutionID
	CommandID   CommandID
	CommandKey  string
	Name        string
	Version     int

	CreatedAt        time.Time
	BudgetStartedAt  time.Time
	Attempt          int
	AttemptStartedAt time.Time
}
