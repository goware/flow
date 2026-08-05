package flow

import "time"

type None = struct{}

type ExecutionID string
type CommandID string
type EventID string
type AttemptID string
type CoordinatorID string
type JournalEntryID string
type JournalPosition uint64

type CommandStatus string

const (
	StatusSucceeded CommandStatus = "succeeded"
	StatusFailed    CommandStatus = "failed"
	StatusCancelled CommandStatus = "cancelled"
	StatusExpired   CommandStatus = "expired"
)

type CommandFailure struct {
	Code    string `json:"code"`
	Message string `json:"message"`
}

type Outcome[R any] struct {
	Status  CommandStatus   `json:"status"`
	Result  R               `json:"result,omitempty"`
	Failure *CommandFailure `json:"failure,omitempty"`
}

type ExecutionHandle struct {
	ID            ExecutionID
	Type          string
	Key           string
	RootCommandID CommandID
	Created       bool
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

type Received[T any] struct {
	EventID    EventID
	Key        string
	Position   JournalPosition
	RecordedAt time.Time
	Payload    T
}
