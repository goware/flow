package flow

import (
	"context"
	"errors"
	"reflect"
	"strings"
	"testing"
	"time"

	"github.com/goware/flow/internal/failure"
)

func TestSafeErrorsAndObservations(t *testing.T) {
	t.Parallel()

	err := newError(ErrConflict, "start", "run", "exec-1", "identity differs")
	if !errors.Is(err, ErrConflict) {
		t.Fatalf("errors.Is(%v, ErrConflict) = false", err)
	}
	for _, part := range []string{"start", "run", "exec-1", "identity differs"} {
		if !strings.Contains(err.Error(), part) {
			t.Fatalf("error %q omits %q", err, part)
		}
	}

	underlying := errors.New("upstream unavailable")
	permanent := Permanent(underlying)
	if !errors.Is(permanent, underlying) || !failure.IsPermanent(permanent) {
		t.Fatalf("Permanent() = %v", permanent)
	}
	delayed := RetryAfter(3*time.Second, underlying)
	delay, ok := failure.RetryDelay(delayed)
	if !ok || delay != 3*time.Second || !errors.Is(delayed, underlying) {
		t.Fatalf("RetryAfter() = %v, %s, %v", delayed, delay, ok)
	}
	if failure.ValidateRetryAfter(RetryAfter(0, underlying)) == nil {
		t.Fatal("zero RetryAfter delay was accepted")
	}

	forbidden := map[string]bool{
		"payload": true, "result": true, "state": true, "sql": true,
		"connection": true, "token": true, "error": true,
	}
	typeOf := reflect.TypeFor[Observation]()
	for i := range typeOf.NumField() {
		name := strings.ToLower(typeOf.Field(i).Name)
		if forbidden[name] {
			t.Fatalf("Observation exposes forbidden field %q", name)
		}
	}
	noOpObserver{}.Observe(context.Background(), Observation{Kind: ObservationRuntime})
	var nilError *Error
	if nilError.Error() != "<nil>" || nilError.Unwrap() != nil {
		t.Fatal("nil structured error is not safe")
	}
	if got := (&Error{Category: ErrInvalid}).Error(); got != ErrInvalid.Error() {
		t.Fatalf("category-only error = %q", got)
	}
	if got := (&Error{Reason: "reason"}).Error(); got != "reason" {
		t.Fatalf("reason-only error = %q", got)
	}
}

func TestRetryPolicyPublicBuilders(t *testing.T) {
	t.Parallel()

	base := RetryFor(time.Hour)
	changed := base.Attempts(7).Backoff(time.Second, time.Minute)
	if validateRetryPolicy(base) != nil || validateRetryPolicy(changed) != nil {
		t.Fatal("valid retry policy rejected")
	}
	if validateRetryPolicy(base.Backoff()) == nil {
		t.Fatal("empty backoff accepted")
	}
	cmd := DefineCommand[testArgs, testResult]("retry", 1, WithRetry(changed))
	if cmd.err != nil {
		t.Fatalf("retry command validation = %v", cmd.err)
	}
	if &cmd.defaults.retryPolicy == &changed {
		t.Fatal("command retained caller policy address")
	}
}
