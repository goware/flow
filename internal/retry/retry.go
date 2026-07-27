package retry

import (
	"crypto/sha256"
	"encoding/binary"
	"errors"
	"fmt"
	"math"
	"slices"
	"strconv"
	"strings"
	"time"

	"github.com/goware/flow/internal/canonical"
)

const DefaultJitter = 0.20

var DefaultBackoff = []time.Duration{
	time.Second,
	5 * time.Second,
	30 * time.Second,
	2 * time.Minute,
}

// PublicPolicy is the sealed representation re-exported by package flow. Its
// fields remain private even though immutable builder methods are public.
type PublicPolicy struct {
	value Policy
	err   error
}

func NewPublicFor(maxElapsed time.Duration) PublicPolicy {
	policy := PublicPolicy{value: For(maxElapsed)}
	policy.err = policy.value.Validate()
	return policy
}

func NewPublicAttempts(max int) PublicPolicy {
	policy := PublicPolicy{value: Attempts(max)}
	policy.err = policy.value.Validate()
	return policy
}

func DefaultPublic() PublicPolicy { return PublicPolicy{value: Default()} }

func (p PublicPolicy) Attempts(max int) PublicPolicy {
	copy := p.clone()
	copy.value.MaxAttempts = intPointer(max)
	copy.err = copy.value.Validate()
	return copy
}

func (p PublicPolicy) Backoff(delays ...time.Duration) PublicPolicy {
	copy := p.clone()
	copy.value.Backoff = slices.Clone(delays)
	copy.err = copy.value.Validate()
	return copy
}

func (p PublicPolicy) Jitter(fraction float64) PublicPolicy {
	copy := p.clone()
	copy.value.Jitter = fraction
	copy.err = copy.value.Validate()
	return copy
}

func (p PublicPolicy) clone() PublicPolicy {
	return PublicPolicy{value: p.value.Clone(), err: p.err}
}

func ValidatePublic(p PublicPolicy) error {
	if p.err != nil {
		return p.err
	}
	return p.value.Validate()
}

func ClonePublic(p PublicPolicy) PublicPolicy { return p.clone() }

func ValueOf(p PublicPolicy) Policy { return p.value.Clone() }

func DecidePublic(p PublicPolicy, input Input) (Decision, error) {
	if err := ValidatePublic(p); err != nil {
		return Decision{}, err
	}
	return Decide(p.value, input)
}

type DurablePolicy struct {
	MaxAttempts *int            `json:"max_attempts,omitempty"`
	MaxElapsed  *time.Duration  `json:"max_elapsed,omitempty"`
	Backoff     []time.Duration `json:"backoff"`
	Jitter      float64         `json:"jitter"`
}

func CanonicalPublic(p PublicPolicy) (canonical.Value, error) {
	if err := ValidatePublic(p); err != nil {
		return canonical.Value{}, err
	}
	value := ValueOf(p)
	return canonical.Marshal(DurablePolicy{
		MaxAttempts: value.MaxAttempts, MaxElapsed: value.MaxElapsed,
		Backoff: value.Backoff, Jitter: value.Jitter,
	}, 16<<10)
}

func PublicFromCanonical(data []byte) (PublicPolicy, error) {
	var durable DurablePolicy
	if err := canonical.Decode(data, &durable); err != nil {
		return PublicPolicy{}, err
	}
	value := Policy{
		MaxAttempts: durable.MaxAttempts, MaxElapsed: durable.MaxElapsed,
		Backoff: slices.Clone(durable.Backoff), Jitter: durable.Jitter,
	}
	if err := value.Validate(); err != nil {
		return PublicPolicy{}, err
	}
	return PublicPolicy{value: value}, nil
}

type Policy struct {
	MaxAttempts *int
	MaxElapsed  *time.Duration
	Backoff     []time.Duration
	Jitter      float64
}

func Default() Policy {
	maxAttempts := 5
	return Policy{
		MaxAttempts: &maxAttempts,
		Backoff:     slices.Clone(DefaultBackoff),
		Jitter:      DefaultJitter,
	}
}

func For(maxElapsed time.Duration) Policy {
	return Policy{
		MaxElapsed: durationPointer(maxElapsed),
		Backoff:    slices.Clone(DefaultBackoff),
		Jitter:     DefaultJitter,
	}
}

func Attempts(maxAttempts int) Policy {
	return Policy{
		MaxAttempts: intPointer(maxAttempts),
		Backoff:     slices.Clone(DefaultBackoff),
		Jitter:      DefaultJitter,
	}
}

func (p Policy) Clone() Policy {
	clone := p
	clone.Backoff = slices.Clone(p.Backoff)
	if p.MaxAttempts != nil {
		clone.MaxAttempts = intPointer(*p.MaxAttempts)
	}
	if p.MaxElapsed != nil {
		clone.MaxElapsed = durationPointer(*p.MaxElapsed)
	}
	return clone
}

func (p Policy) Validate() error {
	if p.MaxAttempts == nil && p.MaxElapsed == nil {
		return errors.New("retry policy requires an attempt or elapsed bound")
	}
	if p.MaxAttempts != nil && *p.MaxAttempts <= 0 {
		return errors.New("retry attempts must be positive")
	}
	if p.MaxElapsed != nil && *p.MaxElapsed <= 0 {
		return errors.New("retry elapsed bound must be positive")
	}
	if len(p.Backoff) == 0 {
		return errors.New("retry backoff must not be empty")
	}
	for _, delay := range p.Backoff {
		if delay <= 0 {
			return errors.New("retry backoff delays must be positive")
		}
	}
	if math.IsNaN(p.Jitter) || math.IsInf(p.Jitter, 0) || p.Jitter < 0 || p.Jitter > 1 {
		return errors.New("retry jitter must be between 0 and 1")
	}
	return nil
}

func (p Policy) Equal(other Policy) bool {
	return equalInt(p.MaxAttempts, other.MaxAttempts) &&
		equalDuration(p.MaxElapsed, other.MaxElapsed) &&
		slices.Equal(p.Backoff, other.Backoff) &&
		p.Jitter == other.Jitter
}

func (p Policy) Fingerprint() [sha256.Size]byte {
	var b strings.Builder
	if p.MaxAttempts != nil {
		b.WriteString("attempts=")
		b.WriteString(strconv.Itoa(*p.MaxAttempts))
	}
	b.WriteByte(';')
	if p.MaxElapsed != nil {
		b.WriteString("elapsed=")
		b.WriteString(strconv.FormatInt(int64(*p.MaxElapsed), 10))
	}
	b.WriteString(";backoff=")
	for _, delay := range p.Backoff {
		b.WriteString(strconv.FormatInt(int64(delay), 10))
		b.WriteByte(',')
	}
	b.WriteString(";jitter=")
	b.WriteString(strconv.FormatFloat(p.Jitter, 'g', -1, 64))
	return sha256.Sum256([]byte(b.String()))
}

type ErrorClass string

const (
	ClassRetryable   ErrorClass = "retryable"
	ClassRetryAfter  ErrorClass = "retry_after"
	ClassPermanent   ErrorClass = "permanent"
	ClassPanic       ErrorClass = "panic"
	ClassTimeout     ErrorClass = "timeout"
	ClassInterrupted ErrorClass = "interrupted"
	ClassLeaseLost   ErrorClass = "lease_lost"
)

type Input struct {
	DBNow             time.Time
	BudgetStartedAt   time.Time
	ConsumedAttempts  int
	AttemptID         string
	Classification    ErrorClass
	ExplicitDelay     *time.Duration
	ExecutionDeadline *time.Time
}

type Decision struct {
	Retry            bool
	ConsumesAttempt  bool
	ConsumedAttempts int
	NextAttemptAt    time.Time
	StopReason       string
}

func Decide(policy Policy, input Input) (Decision, error) {
	if err := policy.Validate(); err != nil {
		return Decision{}, err
	}
	if input.DBNow.IsZero() || input.BudgetStartedAt.IsZero() {
		return Decision{}, errors.New("retry decision requires database and budget times")
	}
	if input.ConsumedAttempts < 0 {
		return Decision{}, errors.New("consumed attempts cannot be negative")
	}
	if input.BudgetStartedAt.After(input.DBNow) {
		return Decision{}, errors.New("retry budget cannot start after database time")
	}
	switch input.Classification {
	case ClassRetryable, ClassRetryAfter, ClassPermanent, ClassPanic, ClassTimeout, ClassInterrupted, ClassLeaseLost:
	default:
		return Decision{}, fmt.Errorf("unknown retry error class %q", input.Classification)
	}

	if input.Classification == ClassInterrupted || input.Classification == ClassLeaseLost {
		return Decision{
			Retry:            true,
			ConsumesAttempt:  false,
			ConsumedAttempts: input.ConsumedAttempts,
			StopReason:       string(input.Classification),
		}, nil
	}

	consumed := input.ConsumedAttempts + 1
	decision := Decision{ConsumesAttempt: true, ConsumedAttempts: consumed}
	if input.Classification == ClassPermanent {
		decision.StopReason = "permanent"
		return decision, nil
	}
	if policy.MaxAttempts != nil && consumed >= *policy.MaxAttempts {
		decision.StopReason = "attempt_limit"
		return decision, nil
	}

	deadline, hasDeadline := effectiveDeadline(policy, input)
	if hasDeadline && !input.DBNow.Before(deadline) {
		decision.StopReason = "elapsed_limit"
		return decision, nil
	}

	delay, err := retryDelay(policy, input, consumed)
	if err != nil {
		return Decision{}, err
	}
	next := input.DBNow.Add(delay)
	if next.Before(input.DBNow) {
		return Decision{}, errors.New("retry delay overflows time")
	}
	if hasDeadline && !next.Before(deadline) {
		decision.StopReason = "deadline_before_next_attempt"
		return decision, nil
	}
	decision.Retry = true
	decision.NextAttemptAt = next
	return decision, nil
}

func effectiveDeadline(policy Policy, input Input) (time.Time, bool) {
	var deadline time.Time
	if policy.MaxElapsed != nil {
		deadline = input.BudgetStartedAt.Add(*policy.MaxElapsed)
	}
	if input.ExecutionDeadline != nil && (deadline.IsZero() || input.ExecutionDeadline.Before(deadline)) {
		deadline = *input.ExecutionDeadline
	}
	return deadline, !deadline.IsZero()
}

func retryDelay(policy Policy, input Input, consumed int) (time.Duration, error) {
	if input.Classification == ClassRetryAfter {
		if input.ExplicitDelay == nil || *input.ExplicitDelay <= 0 {
			return 0, errors.New("retry-after conclusion requires a positive delay")
		}
		return *input.ExplicitDelay, nil
	}
	index := consumed - 1
	if index >= len(policy.Backoff) {
		index = len(policy.Backoff) - 1
	}
	base := policy.Backoff[index]
	if policy.Jitter == 0 {
		return base, nil
	}
	unit := deterministicUnit(input.AttemptID, policy.Fingerprint())
	// Full proportional jitter centered on the configured delay.
	factor := 1 - policy.Jitter + 2*policy.Jitter*unit
	delay := time.Duration(math.Round(float64(base) * factor))
	if delay <= 0 {
		// Jitter(1) has a closed lower bound of zero. Keep the durable retry
		// schedule strictly increasing even for that valid extreme.
		delay = time.Nanosecond
	}
	return delay, nil
}

func deterministicUnit(attemptID string, policyHash [sha256.Size]byte) float64 {
	input := make([]byte, 0, len(attemptID)+len(policyHash))
	input = append(input, attemptID...)
	input = append(input, policyHash[:]...)
	digest := sha256.Sum256(input)
	value := binary.BigEndian.Uint64(digest[:8]) >> 11
	return float64(value) / float64(uint64(1)<<53)
}

func intPointer(value int) *int { return &value }

func durationPointer(value time.Duration) *time.Duration { return &value }

func equalInt(a, b *int) bool {
	return (a == nil && b == nil) || (a != nil && b != nil && *a == *b)
}

func equalDuration(a, b *time.Duration) bool {
	return (a == nil && b == nil) || (a != nil && b != nil && *a == *b)
}
