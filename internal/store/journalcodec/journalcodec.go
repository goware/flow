// Package journalcodec owns version validation for internal journal bodies.
// Application definition versions are separate from these schema versions.
package journalcodec

import (
	"encoding/json"
	"errors"
	"fmt"
	"time"

	"github.com/goware/flow/internal/canonical"
)

// ExecutionStartedBody is the versioned logical start record retained by the
// journal. Raw JSON fields already contain canonical application values.
type ExecutionStartedBody struct {
	V                 int             `json:"v"`
	ExecutionID       string          `json:"execution_id"`
	DriverMode        string          `json:"driver_mode"`
	DefinitionName    string          `json:"definition_name"`
	DefinitionVersion int             `json:"definition_version"`
	ExecutionKey      string          `json:"execution_key"`
	KeyScope          string          `json:"key_scope,omitempty"`
	Input             json.RawMessage `json:"input"`
	FailFast          bool            `json:"fail_fast"`
	DeadlineMode      string          `json:"deadline_mode"`
	DeadlineDuration  int64           `json:"deadline_duration_ms,omitempty"`
	DeadlineAt        *time.Time      `json:"deadline_at,omitempty"`
	MaxCommands       int             `json:"max_commands"`
	Metadata          json.RawMessage `json:"metadata"`
	CoordinatorID     string          `json:"coordinator_id,omitempty"`
	CoordinatorPolicy json.RawMessage `json:"coordinator_retry_policy,omitempty"`
}

type CommandCreatedBody struct {
	V                      int                   `json:"v"`
	CommandID              string                `json:"command_id"`
	CommandKey             string                `json:"command_key"`
	Name                   string                `json:"name"`
	Version                int                   `json:"version"`
	Args                   json.RawMessage       `json:"args"`
	Origin                 string                `json:"origin"`
	ParentCommandID        string                `json:"parent_command_id,omitempty"`
	Required               bool                  `json:"required"`
	FailureScope           bool                  `json:"failure_scope"`
	InitialState           string                `json:"initial_state"`
	Queue                  string                `json:"queue"`
	AttemptTimeoutMS       *int64                `json:"attempt_timeout_ms,omitempty"`
	RetryPolicy            json.RawMessage       `json:"retry_policy"`
	ScheduleKind           string                `json:"schedule_kind"`
	InitialDelayMS         *int64                `json:"initial_delay_ms,omitempty"`
	BudgetStartedAt        *time.Time            `json:"budget_started_at,omitempty"`
	NextAttemptAt          *time.Time            `json:"next_attempt_at,omitempty"`
	DeclarationFingerprint string                `json:"declaration_fingerprint"`
	Dependencies           []DependencyGroupBody `json:"dependencies,omitempty"`
	Waits                  []EventWaitBody       `json:"waits,omitempty"`
	WithinMS               *int64                `json:"within_ms,omitempty"`
}

type DependencyGroupBody struct {
	Kind    string   `json:"kind"`
	Members []string `json:"members"`
}

type EventWaitBody struct {
	Name string `json:"name"`
	Key  string `json:"key"`
}

type PlanReconciledBody struct {
	V               int                         `json:"v"`
	Revision        int64                       `json:"revision"`
	ConsumedThrough int64                       `json:"consumed_through"`
	WaitingReads    int                         `json:"waiting_reads"`
	Quiescent       bool                        `json:"quiescent"`
	Declarations    []PlanReconciledDeclaration `json:"declarations,omitempty"`
}

type PlanReconciledDeclaration struct {
	Key         string `json:"key"`
	CommandID   string `json:"command_id"`
	Fingerprint string `json:"fingerprint"`
}

type ApplicationEventBody struct {
	V       int             `json:"v"`
	Payload json.RawMessage `json:"payload"`
}

type TerminalEventBody struct {
	V          int    `json:"v"`
	Status     string `json:"status"`
	Code       string `json:"code,omitempty"`
	Reason     string `json:"reason,omitempty"`
	CommandKey string `json:"command_key,omitempty"`
}

type AttemptStartedBody struct {
	V                int       `json:"v"`
	AttemptID        string    `json:"attempt_id"`
	CommandID        string    `json:"command_id"`
	CommandKey       string    `json:"command_key"`
	Attempt          int       `json:"attempt"`
	StartedAt        time.Time `json:"started_at"`
	Worker           string    `json:"worker"`
	LeaseDurationMS  int64     `json:"lease_duration_ms"`
	ConsumedAttempts int       `json:"consumed_attempts"`
	BudgetStartedAt  time.Time `json:"budget_started_at"`
	CoordinatorID    string    `json:"coordinator_id,omitempty"`
	DeliveryKey      string    `json:"delivery_key,omitempty"`
}

type AttemptConcludedBody struct {
	V                int        `json:"v"`
	AttemptID        string     `json:"attempt_id"`
	CommandID        string     `json:"command_id"`
	CommandKey       string     `json:"command_key"`
	Attempt          int        `json:"attempt"`
	Classification   string     `json:"classification"`
	ConsumedBudget   bool       `json:"consumed_budget"`
	ConsumedAttempts int        `json:"consumed_attempts"`
	FinishedAt       time.Time  `json:"finished_at"`
	NextAttemptAt    *time.Time `json:"next_attempt_at,omitempty"`
	ErrorCode        string     `json:"error_code,omitempty"`
	ErrorMessage     string     `json:"error_message,omitempty"`
	CoordinatorID    string     `json:"coordinator_id,omitempty"`
	DeliveryKey      string     `json:"delivery_key,omitempty"`
}

type CoordinatorTransitionBody struct {
	V                  int             `json:"v"`
	CoordinatorID      string          `json:"coordinator_id"`
	DeliveryKey        string          `json:"delivery_key"`
	HandledPosition    *int64          `json:"handled_position,omitempty"`
	PriorStateRevision int64           `json:"prior_state_revision"`
	StateRevision      int64           `json:"state_revision"`
	State              json.RawMessage `json:"state"`
	TerminalDecision   string          `json:"terminal_decision,omitempty"`
}

type CommandSucceededBody struct {
	V             int             `json:"v"`
	CommandKey    string          `json:"command_key"`
	Result        json.RawMessage `json:"result"`
	CommitApplied bool            `json:"commit_applied"`
}

var ErrVersion = errors.New("journal body requires a positive integer v")

func Encode(body any) (canonical.Value, error) {
	encoded, err := canonical.Marshal(body, 0)
	if err != nil {
		return canonical.Value{}, err
	}
	if _, err := Version(encoded.Bytes); err != nil {
		return canonical.Value{}, err
	}
	return encoded, nil
}

func Decode[T any](body []byte) (T, error) {
	var zero T
	if _, err := Version(body); err != nil {
		return zero, err
	}
	var decoded T
	if err := canonical.Decode(body, &decoded); err != nil {
		return zero, err
	}
	return decoded, nil
}

func Version(body []byte) (int, error) {
	var header struct {
		Version json.RawMessage `json:"v"`
	}
	if err := canonical.Decode(body, &header); err != nil {
		return 0, fmt.Errorf("%w: %v", ErrVersion, err)
	}
	if len(header.Version) == 0 {
		return 0, ErrVersion
	}
	var version int
	if err := json.Unmarshal(header.Version, &version); err != nil || version <= 0 {
		return 0, ErrVersion
	}
	return version, nil
}
