// Package journalcodec owns version validation for internal journal bodies.
// Application definition versions are separate from these schema versions.
package journalcodec

import (
	"bytes"
	"encoding/json"
	"errors"
	"fmt"
	"time"

	"github.com/goware/flow/internal/canonical"
)

// RunStartedBody is the versioned logical start record retained by the
// journal. Raw JSON fields already contain canonical application values.
type RunStartedBody struct {
	V                 int        `json:"v"`
	RunID             string     `json:"run_id"`
	DefinitionName    string     `json:"definition_name"`
	DefinitionVersion int        `json:"definition_version"`
	RunKey            string     `json:"run_key"`
	KeyScope          string     `json:"key_scope,omitempty"`
	DeadlineMode      string     `json:"deadline_mode"`
	DeadlineDuration  int64      `json:"deadline_duration_ms,omitempty"`
	DeadlineAt        *time.Time `json:"deadline_at,omitempty"`
	MaxCommands       int        `json:"max_commands"`
}

type CommandCreatedBody struct {
	V                      int             `json:"v"`
	CommandID              string          `json:"command_id"`
	CommandKey             string          `json:"command_key"`
	Name                   string          `json:"name"`
	Version                int             `json:"version"`
	Args                   json.RawMessage `json:"args"`
	ParentCommandID        string          `json:"parent_command_id,omitempty"`
	InitialState           string          `json:"initial_state"`
	Queue                  string          `json:"queue"`
	AttemptTimeoutMS       *int64          `json:"attempt_timeout_ms,omitempty"`
	RetryPolicy            json.RawMessage `json:"retry_policy"`
	InitialDelayMS         *int64          `json:"initial_delay_ms,omitempty"`
	BudgetStartedAt        *time.Time      `json:"budget_started_at,omitempty"`
	NextAttemptAt          *time.Time      `json:"next_attempt_at,omitempty"`
	DeclarationFingerprint string          `json:"declaration_fingerprint"`
	Waits                  []EventWaitBody `json:"waits,omitempty"`
	WithinMS               *int64          `json:"within_ms,omitempty"`
}

type EventWaitBody struct {
	Name string `json:"name"`
	Key  string `json:"key"`
}

type ApplicationEventBody struct {
	V       int             `json:"v"`
	Payload json.RawMessage `json:"payload"`
}

const (
	ApplicationEventBodyVersion     = 1
	MaxApplicationEventPayloadBytes = 64 << 10
)

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

// DecodeApplicationEvent decodes the hot-path application-event envelope once.
// The exact envelope check relies on Flow's canonical journal write boundary;
// replay separately reconstructs canonical bodies for full diagnostics.
func DecodeApplicationEvent(body []byte) (ApplicationEventBody, error) {
	var decoded ApplicationEventBody
	if err := canonical.Decode(body, &decoded); err != nil {
		return ApplicationEventBody{}, err
	}
	if decoded.V != ApplicationEventBodyVersion {
		return ApplicationEventBody{}, fmt.Errorf("%w: unsupported application event body version %d", ErrVersion, decoded.V)
	}
	if len(decoded.Payload) == 0 {
		return ApplicationEventBody{}, errors.New("application event body requires payload")
	}
	if err := canonical.ValidateCanonical(decoded.Payload, MaxApplicationEventPayloadBytes); err != nil {
		return ApplicationEventBody{}, fmt.Errorf("application event payload is not canonical: %w", err)
	}
	prefix := []byte(`{"payload":`)
	suffix := []byte(`,"v":1}`)
	if len(body) != len(prefix)+len(decoded.Payload)+len(suffix) ||
		!bytes.Equal(body[:len(prefix)], prefix) ||
		!bytes.Equal(body[len(prefix):len(prefix)+len(decoded.Payload)], decoded.Payload) ||
		!bytes.Equal(body[len(body)-len(suffix):], suffix) {
		return ApplicationEventBody{}, errors.New("application event body has a noncanonical envelope")
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
