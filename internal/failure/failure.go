package failure

import (
	"errors"
	"fmt"
	"time"
)

type permanent struct{ err error }

func (e permanent) Error() string { return e.err.Error() }
func (e permanent) Unwrap() error { return e.err }

type retryAfter struct {
	delay time.Duration
	err   error
}

func (e retryAfter) Error() string { return e.err.Error() }
func (e retryAfter) Unwrap() error { return e.err }

func Permanent(err error) error {
	if err == nil {
		return nil
	}
	var existing permanent
	if errors.As(err, &existing) {
		return err
	}
	return permanent{err: err}
}

func IsPermanent(err error) bool {
	var target permanent
	return errors.As(err, &target)
}

func RetryAfter(delay time.Duration, err error) error {
	if err == nil {
		err = errors.New("retry requested")
	}
	return retryAfter{delay: delay, err: err}
}

func RetryDelay(err error) (time.Duration, bool) {
	var target retryAfter
	if !errors.As(err, &target) {
		return 0, false
	}
	return target.delay, true
}

func ValidateRetryAfter(err error) error {
	delay, ok := RetryDelay(err)
	if ok && delay <= 0 {
		return fmt.Errorf("retry delay must be positive: %s", delay)
	}
	return nil
}
