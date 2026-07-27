package flow

import (
	"context"
	"sync"
	"sync/atomic"
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

const observerQueueSize = 1024

type observerAdapter struct {
	observer Observer
	queue    chan Observation
	done     chan struct{}
	mu       sync.RWMutex
	closed   bool
	start    sync.Once
	stop     sync.Once
	dropped  atomic.Int64
}

func newObserverAdapter(observer Observer) *observerAdapter {
	return &observerAdapter{observer: observer, queue: make(chan Observation, observerQueueSize), done: make(chan struct{})}
}

func (adapter *observerAdapter) run() {
	if adapter == nil {
		return
	}
	adapter.start.Do(func() {
		go func() {
			defer close(adapter.done)
			for observation := range adapter.queue {
				adapter.deliver(observation)
			}
			if dropped := adapter.dropped.Swap(0); dropped > 0 {
				adapter.deliver(Observation{Kind: ObservationRuntime, Operation: "observer", Outcome: "dropped", Count: dropped})
			}
		}()
	})
}

func (adapter *observerAdapter) emit(observation Observation) {
	if adapter == nil {
		return
	}
	adapter.mu.RLock()
	defer adapter.mu.RUnlock()
	if adapter.closed {
		adapter.dropped.Add(1)
		return
	}
	select {
	case adapter.queue <- observation:
	default:
		adapter.dropped.Add(1)
	}
}

func (adapter *observerAdapter) close() {
	if adapter == nil {
		return
	}
	adapter.stop.Do(func() {
		adapter.mu.Lock()
		adapter.closed = true
		close(adapter.queue)
		adapter.mu.Unlock()
	})
	adapter.run()
	<-adapter.done
}

func (adapter *observerAdapter) deliver(observation Observation) {
	defer func() { _ = recover() }()
	adapter.observer.Observe(context.Background(), observation)
}
