package flow

import (
	"context"
	"sync"
	"sync/atomic"
	"time"
)

type ObservationKind string

const (
	ObservationRun     ObservationKind = "run"
	ObservationCommand ObservationKind = "command"
	ObservationEvent   ObservationKind = "event"
	ObservationAttempt ObservationKind = "attempt"
	ObservationClaim   ObservationKind = "claim"
	ObservationLease   ObservationKind = "lease"
	ObservationWait    ObservationKind = "wait"
	ObservationRuntime ObservationKind = "runtime"
)

// Exported vocabulary for the terminal-class observation tuples. String
// values equal the emitted literals, so consumers can match on constants
// instead of magic strings. Tuples are only ever added within a major
// version; consumers must ignore unknown tuples. For direct run
// cancellation, the run/cancel tuple is itself the terminal fact; there is
// no separate run/terminal report for that path.
const (
	ObservationOpCancel            = "cancel"
	ObservationOpTerminal          = "terminal"
	ObservationOpConclude          = "conclude"
	ObservationOpConcludeExhausted = "conclude_exhausted"
	ObservationOpExpire            = "expire"
	ObservationOpRecover           = "recover"
	ObservationOpLocalCancel       = "local_cancel"
	ObservationOpObserver          = "observer"

	ObservationOutcomeSucceeded       = "succeeded"
	ObservationOutcomeFailed          = "failed"
	ObservationOutcomeExpired         = "expired"
	ObservationOutcomeCancelled       = "cancelled"
	ObservationOutcomeRecovered       = "recovered"
	ObservationOutcomeDropped         = "dropped"
	ObservationOutcomeDroppedTerminal = "dropped_terminal"
)

// Observation contains only bounded operational metadata. It intentionally
// has no payload, result, raw SQL, connection, or lease
// token field.
type Observation struct {
	Kind       ObservationKind
	Operation  string
	Outcome    string
	RunID      RunID
	CommandID  CommandID
	CommandKey string
	// RunKey is the application-chosen run key, empty when the run was
	// started without one or the emission site does not hold it.
	RunKey string
	// Definition is the run's root definition name, independent of Name,
	// which on attempt-path facts is the command definition name.
	Definition string
	Name       string
	Version    int
	Queue      string
	Worker     string
	Count      int64
	Duration   time.Duration
	OccurredAt time.Time
}

// terminalClass reports whether the observation is a terminal lifecycle
// fact. Terminal facts may occupy the reserved queue capacity and are
// drop-accounted separately from duty-cycle facts.
func (o Observation) terminalClass() bool {
	switch o.Kind {
	case ObservationRun:
		return o.Operation == ObservationOpCancel || o.Operation == ObservationOpTerminal
	case ObservationCommand:
		return o.Operation == ObservationOpCancel
	case ObservationWait:
		return o.Operation == ObservationOpExpire
	case ObservationLease:
		return o.Operation == ObservationOpRecover || o.Operation == ObservationOpLocalCancel
	case ObservationAttempt:
		return o.Operation == ObservationOpConcludeExhausted ||
			(o.Operation == ObservationOpConclude && o.Outcome == ObservationOutcomeFailed)
	case ObservationRuntime:
		return o.Operation == ObservationOpObserver
	}
	return false
}

type Observer interface {
	// Observe receives best-effort operational metadata. Implementations must
	// return promptly and should stop work when ctx is cancelled. Flow never
	// waits indefinitely for an observer during runtime shutdown.
	Observe(context.Context, Observation)
}

type noOpObserver struct{}

func (noOpObserver) Observe(context.Context, Observation) {}

const (
	observerQueueSize = 1024
	// observerTerminalReserve slots of the queue are reserved for
	// terminal-class observations so a duty-cycle flood cannot evict them.
	observerTerminalReserve = 64
)

type observerAdapter struct {
	observer        Observer
	ctx             context.Context
	cancel          context.CancelFunc
	queue           chan Observation
	terminal        chan Observation
	done            chan struct{}
	mu              sync.RWMutex
	closed          bool
	start           sync.Once
	stop            sync.Once
	dropped         atomic.Int64
	droppedTerminal atomic.Int64
}

func newObserverAdapter(observer Observer) *observerAdapter {
	ctx, cancel := context.WithCancel(context.Background())
	return &observerAdapter{
		observer: observer, ctx: ctx, cancel: cancel,
		queue:    make(chan Observation, observerQueueSize-observerTerminalReserve),
		terminal: make(chan Observation, observerTerminalReserve),
		done:     make(chan struct{}),
	}
}

func (adapter *observerAdapter) run() {
	if adapter == nil {
		return
	}
	adapter.start.Do(func() {
		go func() {
			defer close(adapter.done)
			duty, terminal := adapter.queue, adapter.terminal
			for duty != nil || terminal != nil {
				select {
				case observation, ok := <-duty:
					if !ok {
						duty = nil
						continue
					}
					adapter.deliver(observation)
				case observation, ok := <-terminal:
					if !ok {
						terminal = nil
						continue
					}
					adapter.deliver(observation)
				}
			}
			if dropped := adapter.dropped.Swap(0); dropped > 0 {
				adapter.deliver(Observation{Kind: ObservationRuntime, Operation: ObservationOpObserver,
					Outcome: ObservationOutcomeDropped, Count: dropped})
			}
			if dropped := adapter.droppedTerminal.Swap(0); dropped > 0 {
				adapter.deliver(Observation{Kind: ObservationRuntime, Operation: ObservationOpObserver,
					Outcome: ObservationOutcomeDroppedTerminal, Count: dropped})
			}
		}()
	})
}

func (adapter *observerAdapter) emit(observation Observation) {
	if adapter == nil {
		return
	}
	terminal := observation.terminalClass()
	adapter.mu.RLock()
	defer adapter.mu.RUnlock()
	if adapter.closed {
		adapter.drop(terminal)
		return
	}
	select {
	case adapter.queue <- observation:
		return
	default:
	}
	if terminal {
		select {
		case adapter.terminal <- observation:
			return
		default:
		}
	}
	adapter.drop(terminal)
}

func (adapter *observerAdapter) drop(terminal bool) {
	if terminal {
		adapter.droppedTerminal.Add(1)
		return
	}
	adapter.dropped.Add(1)
}

func (adapter *observerAdapter) close() {
	if adapter == nil {
		return
	}
	adapter.stop.Do(func() {
		adapter.mu.Lock()
		adapter.closed = true
		adapter.cancel()
		close(adapter.queue)
		close(adapter.terminal)
		adapter.mu.Unlock()
	})
	adapter.run()
}

func (adapter *observerAdapter) deliver(observation Observation) {
	defer func() { _ = recover() }()
	adapter.observer.Observe(adapter.ctx, observation)
}
