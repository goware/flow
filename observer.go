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

// Observation operations. Within a major version tuples are only added, never
// renamed or removed, and consumers must ignore tuples they do not know.
const (
	ObservationOpProbe              = "probe"
	ObservationOpClaim              = "claim"
	ObservationOpHandler            = "handler"
	ObservationOpSettle             = "settle"
	ObservationOpConclude           = "conclude"
	ObservationOpConcludeExhausted  = "conclude_exhausted"
	ObservationOpDeliver            = "deliver"
	ObservationOpStart              = "start"
	ObservationOpCancel             = "cancel"
	ObservationOpTerminal           = "terminal"
	ObservationOpExpire             = "expire"
	ObservationOpRenew              = "renew"
	ObservationOpRenewResult        = "renew_result"
	ObservationOpLocalCancel        = "local_cancel"
	ObservationOpRecover            = "recover"
	ObservationOpRun                = "run"
	ObservationOpNotifyListener     = "notify_listener"
	ObservationOpNotifyHint         = "notify_hint"
	ObservationOpDeadlineProbe      = "deadline_probe"
	ObservationOpWaitExpiryProbe    = "wait_expiry_probe"
	ObservationOpLeaseRecoveryProbe = "lease_recovery_probe"
	ObservationOpDeadline           = "deadline"
	ObservationOpWaitExpiry         = "wait_expiry"
	ObservationOpLeaseRecovery      = "lease_recovery"
	ObservationOpMaintenancePass    = "maintenance_pass"
	ObservationOpObserver           = "observer"
)

// Observation outcomes.
const (
	ObservationOutcomeOK              = "ok"
	ObservationOutcomeError           = "error"
	ObservationOutcomePartial         = "partial"
	ObservationOutcomeNoop            = "noop"
	ObservationOutcomeCreated         = "created"
	ObservationOutcomeAccepted        = "accepted"
	ObservationOutcomeCancelled       = "cancelled"
	ObservationOutcomeSucceeded       = "succeeded"
	ObservationOutcomeFailed          = "failed"
	ObservationOutcomeExpired         = "expired"
	ObservationOutcomeRetryWait       = "retry_wait"
	ObservationOutcomeRenewed         = "renewed"
	ObservationOutcomeLost            = "lost"
	ObservationOutcomeUncertain       = "uncertain"
	ObservationOutcomeRecovered       = "recovered"
	ObservationOutcomeStarted         = "started"
	ObservationOutcomeStopped         = "stopped"
	ObservationOutcomeConnectError    = "connect_error"
	ObservationOutcomeListening       = "listening"
	ObservationOutcomeReconnecting    = "reconnecting"
	ObservationOutcomeReceived        = "received"
	ObservationOutcomeBroadWake       = "broad_wake"
	ObservationOutcomeBlocked         = "blocked"
	ObservationOutcomeDrain           = "drain"
	ObservationOutcomeDropped         = "dropped"
	ObservationOutcomeDroppedTerminal = "dropped_terminal"
)

type observationTuple struct {
	kind      ObservationKind
	operation string
	outcome   string
}

type observationClass uint8

const (
	classDutyCycle observationClass = iota
	classTerminal
)

// observationVocabulary is the compatibility surface of the stream: every
// legal tuple and its delivery class. Terminal-class facts are individually
// page-worthy and hold reserved queue capacity; duty-cycle facts are
// high-volume and individually valueless. Explicit run cancellation is
// reported as run/cancel/cancelled, so run/terminal carries no cancelled
// outcome.
var observationVocabulary = map[observationTuple]observationClass{
	{ObservationClaim, ObservationOpProbe, ObservationOutcomeOK}:                      classDutyCycle,
	{ObservationClaim, ObservationOpProbe, ObservationOutcomeError}:                   classDutyCycle,
	{ObservationClaim, ObservationOpClaim, ObservationOutcomeOK}:                      classDutyCycle,
	{ObservationClaim, ObservationOpClaim, ObservationOutcomeError}:                   classDutyCycle,
	{ObservationAttempt, ObservationOpHandler, ObservationOutcomeOK}:                  classDutyCycle,
	{ObservationAttempt, ObservationOpHandler, ObservationOutcomeError}:               classDutyCycle,
	{ObservationAttempt, ObservationOpSettle, ObservationOutcomeSucceeded}:            classDutyCycle,
	{ObservationAttempt, ObservationOpSettle, ObservationOutcomeError}:                classDutyCycle,
	{ObservationAttempt, ObservationOpSettle, ObservationOutcomeExpired}:              classTerminal,
	{ObservationAttempt, ObservationOpConclude, ObservationOutcomeRetryWait}:          classDutyCycle,
	{ObservationAttempt, ObservationOpConclude, ObservationOutcomeError}:              classDutyCycle,
	{ObservationAttempt, ObservationOpConclude, ObservationOutcomeFailed}:             classTerminal,
	{ObservationAttempt, ObservationOpConclude, ObservationOutcomeExpired}:            classTerminal,
	{ObservationAttempt, ObservationOpConcludeExhausted, ObservationOutcomeFailed}:    classTerminal,
	{ObservationEvent, ObservationOpSettle, ObservationOutcomeAccepted}:               classDutyCycle,
	{ObservationEvent, ObservationOpDeliver, ObservationOutcomeCreated}:               classDutyCycle,
	{ObservationRun, ObservationOpStart, ObservationOutcomeCreated}:                   classDutyCycle,
	{ObservationRun, ObservationOpCancel, ObservationOutcomeCancelled}:                classTerminal,
	{ObservationRun, ObservationOpTerminal, ObservationOutcomeSucceeded}:              classTerminal,
	{ObservationRun, ObservationOpTerminal, ObservationOutcomeFailed}:                 classTerminal,
	{ObservationRun, ObservationOpTerminal, ObservationOutcomeExpired}:                classTerminal,
	{ObservationCommand, ObservationOpCancel, ObservationOutcomeCancelled}:            classTerminal,
	{ObservationWait, ObservationOpExpire, ObservationOutcomeExpired}:                 classTerminal,
	{ObservationLease, ObservationOpRenew, ObservationOutcomeOK}:                      classDutyCycle,
	{ObservationLease, ObservationOpRenew, ObservationOutcomePartial}:                 classDutyCycle,
	{ObservationLease, ObservationOpRenew, ObservationOutcomeError}:                   classDutyCycle,
	{ObservationLease, ObservationOpRenewResult, ObservationOutcomeRenewed}:           classDutyCycle,
	{ObservationLease, ObservationOpRenewResult, ObservationOutcomeLost}:              classDutyCycle,
	{ObservationLease, ObservationOpRenewResult, ObservationOutcomeUncertain}:         classDutyCycle,
	{ObservationLease, ObservationOpLocalCancel, ObservationOutcomeLost}:              classTerminal,
	{ObservationLease, ObservationOpLocalCancel, ObservationOutcomeExpired}:           classTerminal,
	{ObservationLease, ObservationOpRecover, ObservationOutcomeRecovered}:             classTerminal,
	{ObservationRuntime, ObservationOpRun, ObservationOutcomeStarted}:                 classDutyCycle,
	{ObservationRuntime, ObservationOpRun, ObservationOutcomeStopped}:                 classDutyCycle,
	{ObservationRuntime, ObservationOpNotifyListener, ObservationOutcomeConnectError}: classDutyCycle,
	{ObservationRuntime, ObservationOpNotifyListener, ObservationOutcomeListening}:    classDutyCycle,
	{ObservationRuntime, ObservationOpNotifyListener, ObservationOutcomeReconnecting}: classDutyCycle,
	{ObservationRuntime, ObservationOpNotifyHint, ObservationOutcomeReceived}:         classDutyCycle,
	{ObservationRuntime, ObservationOpNotifyHint, ObservationOutcomeBroadWake}:        classDutyCycle,
	{ObservationRuntime, ObservationOpDeadlineProbe, ObservationOutcomeOK}:            classDutyCycle,
	{ObservationRuntime, ObservationOpDeadlineProbe, ObservationOutcomeError}:         classDutyCycle,
	{ObservationRuntime, ObservationOpWaitExpiryProbe, ObservationOutcomeOK}:          classDutyCycle,
	{ObservationRuntime, ObservationOpWaitExpiryProbe, ObservationOutcomeError}:       classDutyCycle,
	{ObservationRuntime, ObservationOpLeaseRecoveryProbe, ObservationOutcomeOK}:       classDutyCycle,
	{ObservationRuntime, ObservationOpLeaseRecoveryProbe, ObservationOutcomeError}:    classDutyCycle,
	{ObservationRuntime, ObservationOpDeadline, ObservationOutcomeOK}:                 classDutyCycle,
	{ObservationRuntime, ObservationOpDeadline, ObservationOutcomeNoop}:               classDutyCycle,
	{ObservationRuntime, ObservationOpDeadline, ObservationOutcomePartial}:            classDutyCycle,
	{ObservationRuntime, ObservationOpDeadline, ObservationOutcomeError}:              classDutyCycle,
	{ObservationRuntime, ObservationOpWaitExpiry, ObservationOutcomeOK}:               classDutyCycle,
	{ObservationRuntime, ObservationOpWaitExpiry, ObservationOutcomeNoop}:             classDutyCycle,
	{ObservationRuntime, ObservationOpWaitExpiry, ObservationOutcomePartial}:          classDutyCycle,
	{ObservationRuntime, ObservationOpWaitExpiry, ObservationOutcomeError}:            classDutyCycle,
	{ObservationRuntime, ObservationOpLeaseRecovery, ObservationOutcomeOK}:            classDutyCycle,
	{ObservationRuntime, ObservationOpLeaseRecovery, ObservationOutcomeNoop}:          classDutyCycle,
	{ObservationRuntime, ObservationOpLeaseRecovery, ObservationOutcomePartial}:       classDutyCycle,
	{ObservationRuntime, ObservationOpLeaseRecovery, ObservationOutcomeError}:         classDutyCycle,
	{ObservationRuntime, ObservationOpMaintenancePass, ObservationOutcomeBlocked}:     classDutyCycle,
	{ObservationRuntime, ObservationOpMaintenancePass, ObservationOutcomeDrain}:       classDutyCycle,
	{ObservationRuntime, ObservationOpObserver, ObservationOutcomeDropped}:            classTerminal,
	{ObservationRuntime, ObservationOpObserver, ObservationOutcomeDroppedTerminal}:    classTerminal,
}

func (observation Observation) class() observationClass {
	class, registered := observationVocabulary[observationTuple{observation.Kind, observation.Operation, observation.Outcome}]
	if !registered {
		// A tuple missing from the registry is a vocabulary mistake, not
		// noise: treat it as page-worthy so it is loud in drop accounting
		// instead of quietly evictable.
		return classTerminal
	}
	return class
}

// Observation contains only bounded operational metadata. It intentionally
// has no payload, result, raw SQL, connection, or lease
// token field.
type Observation struct {
	Kind      ObservationKind
	Operation string
	Outcome   string
	RunID     RunID
	// RunKey and Definition name the run in application terms. Both are empty
	// when the fact concerns no single run, and RunKey is also empty when the
	// run was started without a key.
	RunKey     string
	Definition string
	CommandID  CommandID
	CommandKey string
	Name       string
	Version    int
	Queue      string
	Worker     string
	Count      int64
	Duration   time.Duration
	OccurredAt time.Time
}

type Observer interface {
	// Observe receives best-effort operational metadata. Implementations must
	// return promptly and should stop work when ctx is cancelled. Flow never
	// waits indefinitely for an observer during runtime shutdown.
	Observe(context.Context, Observation)
}

type NopObserver struct{}

func (NopObserver) Observe(context.Context, Observation) {}

const (
	observerQueueSize = 1024
	// observerTerminalReserve is the tail of the queue that only terminal-class
	// observations may occupy: duty-cycle sends are refused once the queue
	// length reaches the boundary. The check is approximate, not synchronized
	// with the send, so concurrent duty-cycle emitters may briefly overrun it.
	observerTerminalReserve = 64
)

type observerAdapter struct {
	observer        Observer
	ctx             context.Context
	cancel          context.CancelFunc
	queue           chan Observation
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
		queue: make(chan Observation, observerQueueSize), done: make(chan struct{}),
	}
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
			for _, report := range adapter.dropReports() {
				tapObservation(report)
				adapter.deliver(report)
			}
		}()
	})
}

// observationTap sees every observation the process emits, whether or not it
// is delivered. The vocabulary test installs one to hold emissions to the
// registry; ordinary use leaves it unset.
var observationTap atomic.Pointer[func(Observation)]

func tapObservation(observation Observation) {
	if tap := observationTap.Load(); tap != nil {
		(*tap)(observation)
	}
}

func (adapter *observerAdapter) emit(observation Observation) {
	tapObservation(observation)
	if adapter == nil {
		return
	}
	class := observation.class()
	adapter.mu.RLock()
	defer adapter.mu.RUnlock()
	if adapter.closed || (class == classDutyCycle && len(adapter.queue) >= observerQueueSize-observerTerminalReserve) {
		adapter.recordDrop(class)
		return
	}
	select {
	case adapter.queue <- observation:
	default:
		adapter.recordDrop(class)
	}
}

func (adapter *observerAdapter) recordDrop(class observationClass) {
	adapter.dropped.Add(1)
	if class == classTerminal {
		adapter.droppedTerminal.Add(1)
	}
}

// dropReports states what the drain lost. The aggregate report keeps its Plan 8
// meaning of every dropped observation; the terminal report names the subset
// whose loss means an alert consumer missed lifecycle edges.
func (adapter *observerAdapter) dropReports() []Observation {
	dropped, terminal := adapter.dropped.Swap(0), adapter.droppedTerminal.Swap(0)
	if dropped == 0 {
		return nil
	}
	reports := []Observation{{Kind: ObservationRuntime, Operation: ObservationOpObserver,
		Outcome: ObservationOutcomeDropped, Count: dropped}}
	if terminal > 0 {
		reports = append(reports, Observation{Kind: ObservationRuntime, Operation: ObservationOpObserver,
			Outcome: ObservationOutcomeDroppedTerminal, Count: terminal})
	}
	return reports
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
		adapter.mu.Unlock()
	})
	adapter.run()
}

func (adapter *observerAdapter) deliver(observation Observation) {
	defer func() { _ = recover() }()
	adapter.observer.Observe(adapter.ctx, observation)
}
