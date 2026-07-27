package flow

import (
	"context"
	"time"
)

type ObservationKind string

const (
	ObservationExecution   ObservationKind = "execution"
	ObservationCommand     ObservationKind = "command"
	ObservationEvent       ObservationKind = "event"
	ObservationAttempt     ObservationKind = "attempt"
	ObservationClaim       ObservationKind = "claim"
	ObservationLease       ObservationKind = "lease"
	ObservationPlan        ObservationKind = "plan"
	ObservationWait        ObservationKind = "wait"
	ObservationDependency  ObservationKind = "dependency"
	ObservationCoordinator ObservationKind = "coordinator"
	ObservationRuntime     ObservationKind = "runtime"
)

// Observation contains only bounded operational metadata. It intentionally
// has no payload, result, coordinator state, raw SQL, connection, or lease
// token field.
type Observation struct {
	Kind        ObservationKind
	Operation   string
	Outcome     string
	ExecutionID ExecutionID
	CommandID   CommandID
	CommandKey  string
	Name        string
	Version     int
	Queue       string
	Worker      string
	Count       int64
	Duration    time.Duration
	OccurredAt  time.Time
}

type Observer interface {
	Observe(context.Context, Observation)
}

type NopObserver struct{}

func (NopObserver) Observe(context.Context, Observation) {}
