// Package flowtest provides database-free helpers backed by Flow's production
// codecs and deterministic engine primitives.
package flowtest

import (
	"bytes"
	"fmt"
	"time"

	"github.com/goware/flow"
	"github.com/goware/flow/internal/canonical"
	retrypolicy "github.com/goware/flow/internal/retry"
)

type TestingT interface {
	Helper()
	Fatalf(string, ...any)
}

type CanonicalValue struct {
	Bytes     []byte
	DigestHex string
}

func Canonical(value any) (CanonicalValue, error) {
	encoded, err := canonical.Marshal(value, 0)
	if err != nil {
		return CanonicalValue{}, err
	}
	return CanonicalValue{Bytes: encoded.BytesCopy(), DigestHex: encoded.DigestHex()}, nil
}

func AssertCanonicalStable(t TestingT, value any) CanonicalValue {
	t.Helper()
	first, err := Canonical(value)
	if err != nil {
		t.Fatalf("canonicalize first pass: %v", err)
	}
	second, err := Canonical(value)
	if err != nil {
		t.Fatalf("canonicalize second pass: %v", err)
	}
	if first.DigestHex != second.DigestHex || !bytes.Equal(first.Bytes, second.Bytes) {
		t.Fatalf("canonical encoding is unstable: %q != %q", first.Bytes, second.Bytes)
	}
	return first
}

type RetryClass string

const (
	Retryable   RetryClass = "retryable"
	RetryAt     RetryClass = "retry_after"
	Permanent   RetryClass = "permanent"
	Panic       RetryClass = "panic"
	Timeout     RetryClass = "timeout"
	Interrupted RetryClass = "interrupted"
	LeaseLost   RetryClass = "lease_lost"
)

type RetryInput struct {
	DBNow            time.Time
	BudgetStartedAt  time.Time
	ConsumedAttempts int
	AttemptID        flow.AttemptID
	Class            RetryClass
	ExplicitDelay    time.Duration
	RunDeadline      *time.Time
}

type RetryDecision struct {
	Retry            bool
	ConsumesAttempt  bool
	ConsumedAttempts int
	NextAttemptAt    time.Time
	StopReason       string
}

func DecideRetry(policy flow.RetryPolicy, input RetryInput) (RetryDecision, error) {
	class := retrypolicy.ErrorClass(input.Class)
	var explicit *time.Duration
	if input.ExplicitDelay != 0 {
		delay := input.ExplicitDelay
		explicit = &delay
	}
	decision, err := retrypolicy.DecidePublic(policy, retrypolicy.Input{
		DBNow:            input.DBNow,
		BudgetStartedAt:  input.BudgetStartedAt,
		ConsumedAttempts: input.ConsumedAttempts,
		AttemptID:        string(input.AttemptID),
		Classification:   class,
		ExplicitDelay:    explicit,
		RunDeadline:      input.RunDeadline,
	})
	if err != nil {
		return RetryDecision{}, fmt.Errorf("decide retry: %w", err)
	}
	return RetryDecision{
		Retry:            decision.Retry,
		ConsumesAttempt:  decision.ConsumesAttempt,
		ConsumedAttempts: decision.ConsumedAttempts,
		NextAttemptAt:    decision.NextAttemptAt,
		StopReason:       decision.StopReason,
	}, nil
}
