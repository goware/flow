package failure

import (
	"encoding/json"
	"errors"
	"fmt"
	"time"

	"github.com/goware/flow/internal/durable"
)

// Value is the shared durable and public representation of an operational or
// terminal failure. Distinct lifecycle projections store this same shape.
type Value struct {
	Code    string `json:"code"`
	Message string `json:"message"`
}

func Decode(data []byte) (*Value, error) {
	if len(data) == 0 || string(data) == "null" {
		return nil, nil
	}
	var value Value
	if err := json.Unmarshal(data, &value); err != nil {
		return nil, err
	}
	return &value, nil
}

func Clone(value *Value) *Value {
	if value == nil {
		return nil
	}
	copy := *value
	return &copy
}

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
	if ok {
		if _, durationErr := durable.ExactMilliseconds("retry-after delay", delay); durationErr != nil {
			return durationErr
		}
	}
	return nil
}
