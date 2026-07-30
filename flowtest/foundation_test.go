package flowtest_test

import (
	"fmt"
	"testing"
	"time"

	"github.com/goware/flow"
	"github.com/goware/flow/flowtest"
)

type fatalTestingT struct{}

func (fatalTestingT) Helper()                           {}
func (fatalTestingT) Fatalf(format string, args ...any) { panic(fmt.Sprintf(format, args...)) }

func TestFlowtestFoundation(t *testing.T) {
	t.Parallel()

	type sample struct {
		Z int    `json:"z"`
		A string `json:"a"`
	}
	canonical := flowtest.AssertCanonicalStable(t, sample{Z: 2, A: "one"})
	if string(canonical.Bytes) != `{"a":"one","z":2}` || canonical.DigestHex == "" {
		t.Fatalf("canonical = %#v", canonical)
	}

	now := time.Date(2026, 7, 26, 12, 0, 0, 0, time.UTC)
	decision, err := flowtest.DecideRetry(flow.RetryFor(time.Hour).Backoff(time.Second), flowtest.RetryInput{
		DBNow: now, BudgetStartedAt: now, AttemptID: "attempt-1", Class: flowtest.Retryable,
	})
	if err != nil {
		t.Fatalf("DecideRetry() error = %v", err)
	}
	delay := decision.NextAttemptAt.Sub(now)
	if !decision.Retry || delay < 800*time.Millisecond || delay > 1200*time.Millisecond || decision.ConsumedAttempts != 1 {
		t.Fatalf("DecideRetry() = %#v", decision)
	}
}

func TestFlowtestFoundationErrors(t *testing.T) {
	t.Parallel()

	if _, err := flowtest.Canonical(make(chan int)); err == nil {
		t.Fatal("Canonical() accepted unsupported input")
	}
	defer func() {
		if recover() == nil {
			t.Fatal("AssertCanonicalStable() did not report encoding failure")
		}
	}()
	flowtest.AssertCanonicalStable(fatalTestingT{}, make(chan int))
}

func TestFlowtestRetryError(t *testing.T) {
	t.Parallel()

	if _, err := flowtest.DecideRetry(flow.RetryFor(0), flowtest.RetryInput{}); err == nil {
		t.Fatal("DecideRetry() accepted invalid policy/input")
	}
}
