package retry

import (
	"testing"
	"time"
)

func TestPolicyBuildersAreImmutable(t *testing.T) {
	t.Parallel()

	base := NewPublicFor(time.Hour)
	modified := base.Attempts(8).Backoff(time.Second, 2*time.Second)
	baseValue := ValueOf(base)
	modifiedValue := ValueOf(modified)
	if baseValue.MaxAttempts != nil {
		t.Fatalf("base MaxAttempts = %v, want nil", *baseValue.MaxAttempts)
	}
	if modifiedValue.MaxAttempts == nil || *modifiedValue.MaxAttempts != 8 {
		t.Fatalf("modified MaxAttempts = %v, want 8", modifiedValue.MaxAttempts)
	}
	if len(baseValue.Backoff) != len(DefaultBackoff) || baseValue.Jitter != DefaultJitter {
		t.Fatalf("base changed: %#v", baseValue)
	}
	modifiedValue.Backoff[0] = time.Hour
	if ValueOf(modified).Backoff[0] != time.Second {
		t.Fatal("ValueOf returned policy storage by reference")
	}
	if ValueOf(modified).Equal(baseValue) {
		t.Fatal("different policies compare equal")
	}
}

func TestPolicyValidation(t *testing.T) {
	t.Parallel()

	tests := []PublicPolicy{
		NewPublicFor(0),
		NewPublicAttempts(0),
		NewPublicFor(time.Minute).Backoff(),
		NewPublicFor(time.Minute).Backoff(-time.Second),
	}
	for i, policy := range tests {
		if err := ValidatePublic(policy); err == nil {
			t.Fatalf("policy %d unexpectedly valid", i)
		}
	}
	if err := ValidatePublic(DefaultPublic()); err != nil {
		t.Fatalf("default policy invalid: %v", err)
	}
}

func TestDecide(t *testing.T) {
	t.Parallel()

	now := time.Date(2026, 7, 26, 12, 0, 0, 0, time.UTC)
	policy := Attempts(3)
	policy.Jitter = 0

	t.Run("retryable", func(t *testing.T) {
		got, err := Decide(policy, Input{
			DBNow: now, BudgetStartedAt: now.Add(-time.Minute), AttemptID: "a1", Classification: ClassRetryable,
		})
		if err != nil {
			t.Fatalf("Decide() error = %v", err)
		}
		if !got.Retry || !got.ConsumesAttempt || got.ConsumedAttempts != 1 || got.NextAttemptAt != now.Add(time.Second) {
			t.Fatalf("Decide() = %#v", got)
		}
	})

	t.Run("attempt limit", func(t *testing.T) {
		got, err := Decide(policy, Input{
			DBNow: now, BudgetStartedAt: now, ConsumedAttempts: 2, AttemptID: "a3", Classification: ClassPanic,
		})
		if err != nil {
			t.Fatalf("Decide() error = %v", err)
		}
		if got.Retry || got.StopReason != "attempt_limit" || got.ConsumedAttempts != 3 {
			t.Fatalf("Decide() = %#v", got)
		}
	})

	t.Run("permanent", func(t *testing.T) {
		got, err := Decide(policy, Input{
			DBNow: now, BudgetStartedAt: now, AttemptID: "a1", Classification: ClassPermanent,
		})
		if err != nil {
			t.Fatalf("Decide() error = %v", err)
		}
		if got.Retry || got.StopReason != "permanent" || !got.ConsumesAttempt {
			t.Fatalf("Decide() = %#v", got)
		}
	})

	t.Run("explicit delay", func(t *testing.T) {
		delay := 17 * time.Second
		got, err := Decide(policy, Input{
			DBNow: now, BudgetStartedAt: now, AttemptID: "a1", Classification: ClassRetryAfter, ExplicitDelay: &delay,
		})
		if err != nil {
			t.Fatalf("Decide() error = %v", err)
		}
		if !got.Retry || got.NextAttemptAt != now.Add(delay) {
			t.Fatalf("Decide() = %#v", got)
		}
	})

	t.Run("interruption does not consume", func(t *testing.T) {
		got, err := Decide(policy, Input{
			DBNow: now, BudgetStartedAt: now, ConsumedAttempts: 2, AttemptID: "a7", Classification: ClassInterrupted,
		})
		if err != nil {
			t.Fatalf("Decide() error = %v", err)
		}
		if !got.Retry || got.ConsumesAttempt || got.ConsumedAttempts != 2 || !got.NextAttemptAt.IsZero() {
			t.Fatalf("Decide() = %#v", got)
		}
	})
}

func TestDecideElapsedDeadlineAndJitter(t *testing.T) {
	t.Parallel()

	now := time.Date(2026, 7, 26, 12, 0, 0, 0, time.UTC)
	public := NewPublicFor(time.Minute).Backoff(10 * time.Second)
	input := Input{DBNow: now, BudgetStartedAt: now.Add(-20 * time.Second), AttemptID: "stable", Classification: ClassRetryable}
	first, err := DecidePublic(public, input)
	if err != nil {
		t.Fatalf("DecidePublic() error = %v", err)
	}
	second, err := DecidePublic(public, input)
	if err != nil {
		t.Fatalf("DecidePublic() second error = %v", err)
	}
	if first != second || !first.Retry {
		t.Fatalf("jitter decision is not deterministic: %#v %#v", first, second)
	}
	delay := first.NextAttemptAt.Sub(now)
	if delay < 8*time.Second || delay > 12*time.Second {
		t.Fatalf("jitter delay = %s, want [8s,12s]", delay)
	}

	deadline := now.Add(5 * time.Second)
	input.ExecutionDeadline = &deadline
	stopped, err := DecidePublic(public, input)
	if err != nil {
		t.Fatalf("DecidePublic(deadline) error = %v", err)
	}
	if stopped.Retry || stopped.StopReason != "deadline_before_next_attempt" {
		t.Fatalf("deadline decision = %#v", stopped)
	}

	input.ExecutionDeadline = nil
	input.DBNow = input.BudgetStartedAt.Add(time.Minute)
	stopped, err = DecidePublic(public, input)
	if err != nil {
		t.Fatalf("DecidePublic(elapsed) error = %v", err)
	}
	if stopped.Retry || stopped.StopReason != "elapsed_limit" {
		t.Fatalf("elapsed decision = %#v", stopped)
	}
}

func TestDecideValidation(t *testing.T) {
	t.Parallel()

	now := time.Date(2026, 7, 26, 12, 0, 0, 0, time.UTC)
	valid := Input{DBNow: now, BudgetStartedAt: now, AttemptID: "a", Classification: ClassRetryable}
	tests := []struct {
		name   string
		policy Policy
		input  Input
	}{
		{name: "invalid policy", policy: Policy{}, input: valid},
		{name: "missing times", policy: Default(), input: Input{Classification: ClassRetryable}},
		{name: "negative consumed", policy: Default(), input: Input{DBNow: now, BudgetStartedAt: now, ConsumedAttempts: -1, Classification: ClassRetryable}},
		{name: "future budget", policy: Default(), input: Input{DBNow: now, BudgetStartedAt: now.Add(time.Second), Classification: ClassRetryable}},
		{name: "unknown class", policy: Default(), input: Input{DBNow: now, BudgetStartedAt: now, Classification: "mystery"}},
		{name: "retry after missing delay", policy: Default(), input: Input{DBNow: now, BudgetStartedAt: now, Classification: ClassRetryAfter}},
	}
	for _, tt := range tests {
		t.Run(tt.name, func(t *testing.T) {
			if _, err := Decide(tt.policy, tt.input); err == nil {
				t.Fatal("Decide() unexpectedly succeeded")
			}
		})
	}
	if _, err := DecidePublic(NewPublicFor(0), valid); err == nil {
		t.Fatal("DecidePublic() accepted invalid policy")
	}
	if !ClonePublic(DefaultPublic()).value.Equal(Default()) {
		t.Fatal("ClonePublic() changed the policy")
	}
}

func TestPublicPolicyCanonicalRoundTrip(t *testing.T) {
	t.Parallel()

	want := NewPublicFor(3*time.Hour).Attempts(11).Backoff(time.Second, 7*time.Second)
	encoded, err := CanonicalPublic(want)
	if err != nil {
		t.Fatalf("CanonicalPublic() error = %v", err)
	}
	got, err := PublicFromCanonical(encoded.Bytes)
	if err != nil {
		t.Fatalf("PublicFromCanonical() error = %v", err)
	}
	if !ValueOf(got).Equal(ValueOf(want)) {
		t.Fatalf("round trip = %#v, want %#v", ValueOf(got), ValueOf(want))
	}
	if _, err := PublicFromCanonical([]byte(`{"max_attempts":0,"backoff":[],"jitter":0}`)); err == nil {
		t.Fatal("PublicFromCanonical() accepted invalid policy")
	}
}
