package durable

import (
	"errors"
	"math"
	"strconv"
	"testing"
	"time"

	"github.com/goware/flow/internal/flowerr"
)

func TestPostgresIntegerBoundaries(t *testing.T) {
	t.Parallel()
	for _, value := range []int{math.MinInt32, math.MaxInt32} {
		if err := PostgresInteger("value", value, PostgresIntegerMin, PostgresIntegerMax); err != nil {
			t.Fatalf("PostgresInteger(%d) error = %v", value, err)
		}
	}
	if strconv.IntSize > 32 {
		belowMinimum := int64(math.MinInt32) - 1
		aboveMaximum := int64(math.MaxInt32) + 1
		for _, value := range []int{int(belowMinimum), int(aboveMaximum)} {
			if err := PostgresInteger("value", value, PostgresIntegerMin, PostgresIntegerMax); !errors.Is(err, flowerr.ErrInvalid) {
				t.Fatalf("PostgresInteger(%d) error = %v, want ErrInvalid", value, err)
			}
		}
	}
	if _, err := AddPostgresInteger("value", math.MaxInt32, 1, 0, PostgresIntegerMax); !errors.Is(err, flowerr.ErrInvalid) {
		t.Fatalf("AddPostgresInteger overflow error = %v", err)
	}
}

func TestExactMilliseconds(t *testing.T) {
	t.Parallel()
	maximumExact := time.Duration(math.MaxInt64/int64(time.Millisecond)) * time.Millisecond
	for _, test := range []struct {
		name  string
		value time.Duration
		want  int64
		valid bool
	}{
		{name: "zero", valid: true},
		{name: "one", value: time.Millisecond, want: 1, valid: true},
		{name: "ordinary", value: 1500 * time.Millisecond, want: 1500, valid: true},
		{name: "maximum exact", value: maximumExact, want: int64(maximumExact / time.Millisecond), valid: true},
		{name: "fractional", value: time.Millisecond + time.Nanosecond},
		{name: "negative", value: -time.Millisecond},
		{name: "duration maximum is fractional", value: time.Duration(math.MaxInt64)},
	} {
		t.Run(test.name, func(t *testing.T) {
			got, err := ExactMilliseconds("duration", test.value)
			if test.valid && (err != nil || got != test.want) {
				t.Fatalf("ExactMilliseconds() = %d, %v; want %d", got, err, test.want)
			}
			if !test.valid && !errors.Is(err, flowerr.ErrInvalid) {
				t.Fatalf("ExactMilliseconds() error = %v, want ErrInvalid", err)
			}
		})
	}
}

func TestMillisecondsDuration(t *testing.T) {
	t.Parallel()
	maximum := int64(math.MaxInt64 / int64(time.Millisecond))
	for _, test := range []struct {
		name  string
		value int64
		want  time.Duration
		valid bool
	}{
		{name: "zero", valid: true},
		{name: "one", value: 1, want: time.Millisecond, valid: true},
		{name: "maximum", value: maximum, want: time.Duration(maximum) * time.Millisecond, valid: true},
		{name: "negative", value: -1},
		{name: "overflow", value: maximum + 1},
	} {
		t.Run(test.name, func(t *testing.T) {
			got, err := MillisecondsDuration("duration", test.value)
			if test.valid && (err != nil || got != test.want) {
				t.Fatalf("MillisecondsDuration() = %s, %v; want %s", got, err, test.want)
			}
			if !test.valid && !errors.Is(err, flowerr.ErrInvalid) {
				t.Fatalf("MillisecondsDuration() error = %v, want ErrInvalid", err)
			}
		})
	}
}
