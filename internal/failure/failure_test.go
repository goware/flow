package failure

import (
	"errors"
	"testing"
	"time"
)

func TestPermanent(t *testing.T) {
	t.Parallel()

	base := errors.New("broken")
	wrapped := Permanent(base)
	if wrapped.Error() != base.Error() || !errors.Is(wrapped, base) || !IsPermanent(wrapped) {
		t.Fatalf("Permanent() = %v", wrapped)
	}
	if Permanent(wrapped) != wrapped {
		t.Fatal("Permanent() double wrapped an error")
	}
	if Permanent(nil) != nil || IsPermanent(base) {
		t.Fatal("Permanent nil/plain classification is wrong")
	}
}

func TestRetryAfter(t *testing.T) {
	t.Parallel()

	base := errors.New("later")
	wrapped := RetryAfter(time.Second, base)
	delay, ok := RetryDelay(wrapped)
	if wrapped.Error() != base.Error() || !errors.Is(wrapped, base) || !ok || delay != time.Second {
		t.Fatalf("RetryAfter() = %v, %s, %v", wrapped, delay, ok)
	}
	if err := ValidateRetryAfter(wrapped); err != nil {
		t.Fatalf("ValidateRetryAfter() error = %v", err)
	}
	if _, ok := RetryDelay(base); ok {
		t.Fatal("plain error has retry delay")
	}
	if RetryAfter(time.Second, nil).Error() == "" {
		t.Fatal("nil RetryAfter error has no safe message")
	}
	if ValidateRetryAfter(RetryAfter(-time.Second, base)) == nil {
		t.Fatal("negative retry delay accepted")
	}
	if ValidateRetryAfter(RetryAfter(time.Millisecond+time.Nanosecond, base)) == nil {
		t.Fatal("fractional retry delay accepted")
	}
}
