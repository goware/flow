package flow

import (
	"errors"
	"fmt"
	"strings"
	"time"

	"github.com/goware/flow/internal/failure"
)

var (
	ErrNotFound        = errors.New("flow: not found")
	ErrConflict        = errors.New("flow: conflict")
	ErrInvalid         = errors.New("flow: invalid")
	ErrInvalidState    = errors.New("flow: invalid state")
	ErrTerminal        = errors.New("flow: terminal")
	ErrLeaseLost       = errors.New("flow: lease lost")
	ErrPayloadTooLarge = errors.New("flow: payload too large")
	ErrClosed          = errors.New("flow: closed")
	ErrSchema          = errors.New("flow: incompatible schema")
)

// Error adds safe structured context to a sentinel category. Its fields must
// contain identifiers and bounded reasons only, never payloads, SQL, secrets,
// or lease tokens.
type Error struct {
	Category error
	Op       string
	Resource string
	ID       string
	Reason   string
}

func (e *Error) Error() string {
	if e == nil {
		return "<nil>"
	}
	parts := make([]string, 0, 4)
	if e.Op != "" {
		parts = append(parts, e.Op)
	}
	if e.Resource != "" {
		parts = append(parts, e.Resource)
	}
	if e.ID != "" {
		parts = append(parts, e.ID)
	}
	if e.Reason != "" {
		parts = append(parts, e.Reason)
	}
	message := strings.Join(parts, ": ")
	if e.Category == nil {
		return message
	}
	if message == "" {
		return e.Category.Error()
	}
	return fmt.Sprintf("%s: %s", e.Category, message)
}

func (e *Error) Unwrap() error {
	if e == nil {
		return nil
	}
	return e.Category
}

func newError(category error, op, resource, id, reason string) error {
	return &Error{Category: category, Op: op, Resource: resource, ID: id, Reason: reason}
}

// Permanent classifies an application error as terminal for the current
// command or coordinator delivery.
func Permanent(err error) error { return failure.Permanent(err) }

// RetryAfter classifies an error as retryable after a requested delay. The
// command's immutable retry bounds still apply.
func RetryAfter(delay time.Duration, err error) error { return failure.RetryAfter(delay, err) }
