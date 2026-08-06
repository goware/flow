// Package durable contains validation shared by public construction and the
// PostgreSQL store for values whose durable representation is narrower than
// their Go representation.
package durable

import (
	"fmt"
	"math"
	"time"

	"github.com/goware/flow/internal/flowerr"
)

const (
	PostgresIntegerMin = int64(math.MinInt32)
	PostgresIntegerMax = int64(math.MaxInt32)
)

// PostgresInteger validates a Go int before it is written to a PostgreSQL
// integer. Bounds are int64 so the same call is correct on 32- and 64-bit Go.
func PostgresInteger(field string, value int, minimum, maximum int64) error {
	if minimum < PostgresIntegerMin || maximum > PostgresIntegerMax || minimum > maximum {
		return fmt.Errorf("%w: %s has invalid PostgreSQL integer bounds", flowerr.ErrInvalid, field)
	}
	converted := int64(value)
	if converted < minimum || converted > maximum {
		return fmt.Errorf("%w: %s is outside PostgreSQL integer range [%d, %d]", flowerr.ErrInvalid, field, minimum, maximum)
	}
	return nil
}

func PostgresInteger32(field string, value int, minimum, maximum int64) (int32, error) {
	if err := PostgresInteger(field, value, minimum, maximum); err != nil {
		return 0, err
	}
	return int32(value), nil
}

// AddPostgresInteger validates a PostgreSQL integer transition without doing
// architecture-sized arithmetic first.
func AddPostgresInteger(field string, value, delta int, minimum, maximum int64) (int, error) {
	if err := PostgresInteger(field, value, minimum, maximum); err != nil {
		return 0, err
	}
	result := int64(value) + int64(delta)
	if result < minimum || result > maximum {
		return 0, fmt.Errorf("%w: %s transition is outside PostgreSQL integer range [%d, %d]", flowerr.ErrInvalid, field, minimum, maximum)
	}
	return int(result), nil
}

// ExactMilliseconds converts a non-negative duration to its durable whole-
// millisecond representation. Callers enforce feature-specific zero semantics.
func ExactMilliseconds(field string, value time.Duration) (int64, error) {
	if value < 0 {
		return 0, fmt.Errorf("%w: %s must not be negative", flowerr.ErrInvalid, field)
	}
	if value%time.Millisecond != 0 {
		return 0, fmt.Errorf("%w: %s must use exact whole-millisecond precision", flowerr.ErrInvalid, field)
	}
	return int64(value / time.Millisecond), nil
}

// MillisecondsDuration converts a non-negative durable millisecond value to a
// Go duration without overflowing the narrower nanosecond representation.
func MillisecondsDuration(field string, value int64) (time.Duration, error) {
	if value < 0 || value > math.MaxInt64/int64(time.Millisecond) {
		return 0, fmt.Errorf("%w: %s is outside Go duration range", flowerr.ErrInvalid, field)
	}
	return time.Duration(value) * time.Millisecond, nil
}

// AddExactDuration makes validation precede durable timestamp arithmetic.
func AddExactDuration(field string, base time.Time, value time.Duration) (time.Time, error) {
	if _, err := ExactMilliseconds(field, value); err != nil {
		return time.Time{}, err
	}
	result := base.Add(value)
	if value > 0 && !result.After(base) {
		return time.Time{}, fmt.Errorf("%w: %s overflows timestamp arithmetic", flowerr.ErrInvalid, field)
	}
	return result, nil
}
