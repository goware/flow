package flow

import (
	"time"

	retrypolicy "github.com/goware/flow/internal/retry"
)

// RetryPolicy is immutable declarative data. Its fields are sealed so durable
// behavior can only be constructed through validated builders.
type RetryPolicy = retrypolicy.PublicPolicy

func RetryFor(maxElapsed time.Duration) RetryPolicy {
	return retrypolicy.NewPublicFor(maxElapsed)
}

func defaultRetryPolicy() RetryPolicy {
	return retrypolicy.DefaultPublic()
}

func attemptRetryPolicy(max int) RetryPolicy {
	return retrypolicy.NewPublicAttempts(max)
}

func cloneRetryPolicy(policy RetryPolicy) RetryPolicy { return retrypolicy.ClonePublic(policy) }

func validateRetryPolicy(policy RetryPolicy) error { return retrypolicy.ValidatePublic(policy) }
