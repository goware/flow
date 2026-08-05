package flow

import "time"

type None = struct{}

type ExecutionID string
type CommandID string
type EventID string
type AttemptID string
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
