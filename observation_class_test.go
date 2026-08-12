package flow

import (
	"context"
	"testing"
	"time"
)

func TestObservationTerminalClass(t *testing.T) {
	t.Parallel()

	terminal := []Observation{
		{Kind: ObservationRun, Operation: ObservationOpTerminal, Outcome: ObservationOutcomeFailed},
		{Kind: ObservationRun, Operation: ObservationOpTerminal, Outcome: ObservationOutcomeExpired},
		{Kind: ObservationRun, Operation: ObservationOpCancel, Outcome: ObservationOutcomeCancelled},
		{Kind: ObservationCommand, Operation: ObservationOpCancel, Outcome: ObservationOutcomeCancelled},
		{Kind: ObservationWait, Operation: ObservationOpExpire, Outcome: ObservationOutcomeExpired},
		{Kind: ObservationLease, Operation: ObservationOpRecover, Outcome: ObservationOutcomeRecovered},
		{Kind: ObservationLease, Operation: ObservationOpLocalCancel, Outcome: "lost"},
		{Kind: ObservationAttempt, Operation: ObservationOpConcludeExhausted, Outcome: ObservationOutcomeFailed},
		{Kind: ObservationAttempt, Operation: ObservationOpConclude, Outcome: ObservationOutcomeFailed},
		{Kind: ObservationRuntime, Operation: ObservationOpObserver, Outcome: ObservationOutcomeDropped},
	}
	for _, observation := range terminal {
		if !observation.terminalClass() {
			t.Errorf("%s/%s/%s is not terminal-class", observation.Kind, observation.Operation, observation.Outcome)
		}
	}
	dutyCycle := []Observation{
		{Kind: ObservationClaim, Operation: "probe", Outcome: "ok"},
		{Kind: ObservationAttempt, Operation: "handler", Outcome: "ok"},
		{Kind: ObservationAttempt, Operation: "settle", Outcome: ObservationOutcomeSucceeded},
		{Kind: ObservationAttempt, Operation: ObservationOpConclude, Outcome: "retry_wait"},
		{Kind: ObservationLease, Operation: "renew", Outcome: "ok"},
		{Kind: ObservationRun, Operation: "start", Outcome: "created"},
		{Kind: ObservationRuntime, Operation: "maintenance_pass", Outcome: "drain"},
	}
	for _, observation := range dutyCycle {
		if observation.terminalClass() {
			t.Errorf("%s/%s/%s is terminal-class", observation.Kind, observation.Operation, observation.Outcome)
		}
	}
}

func TestObserverTerminalReserveSurvivesDutyCycleFlood(t *testing.T) {
	t.Parallel()

	observer := &recordingObserver{}
	adapter := newObserverAdapter(observer)
	for range observerQueueSize * 2 {
		adapter.emit(Observation{Kind: ObservationClaim, Operation: "probe", Outcome: "ok"})
	}
	terminal := Observation{Kind: ObservationRun, Operation: ObservationOpTerminal, Outcome: ObservationOutcomeFailed}
	for range observerTerminalReserve + 10 {
		adapter.emit(terminal)
	}
	if adapter.dropped.Load() == 0 {
		t.Fatal("duty-cycle overflow was not counted")
	}
	if got, want := adapter.droppedTerminal.Load(), int64(10); got != want {
		t.Fatalf("terminal drops = %d, want %d", got, want)
	}
	adapter.close()
	select {
	case <-adapter.done:
	case <-time.After(time.Second):
		t.Fatal("observer adapter did not finish")
	}
	delivered := 0
	var droppedReport, droppedTerminalReport int64
	for _, observation := range observer.snapshot() {
		if observation.Kind == ObservationRun && observation.Operation == ObservationOpTerminal {
			delivered++
		}
		if observation.Kind == ObservationRuntime && observation.Operation == ObservationOpObserver {
			switch observation.Outcome {
			case ObservationOutcomeDropped:
				droppedReport = observation.Count
			case ObservationOutcomeDroppedTerminal:
				droppedTerminalReport = observation.Count
			}
		}
	}
	if delivered != observerTerminalReserve {
		t.Fatalf("terminal observations delivered = %d, want %d", delivered, observerTerminalReserve)
	}
	if droppedReport == 0 || droppedTerminalReport != 10 {
		t.Fatalf("drain drop reports duty=%d terminal=%d, want nonzero/10", droppedReport, droppedTerminalReport)
	}
}

func TestObserverTerminalDeliveredWithoutFlood(t *testing.T) {
	t.Parallel()

	observer := &recordingObserver{}
	adapter := newObserverAdapter(observer)
	adapter.emit(Observation{Kind: ObservationRun, Operation: ObservationOpTerminal, Outcome: ObservationOutcomeFailed})
	adapter.emit(Observation{Kind: ObservationClaim, Operation: "probe", Outcome: "ok"})
	adapter.close()
	select {
	case <-adapter.done:
	case <-time.After(time.Second):
		t.Fatal("observer adapter did not finish")
	}
	values := observer.snapshot()
	if len(values) != 2 {
		t.Fatalf("observations = %#v, want both delivered", values)
	}
}

func TestObserverEmitAfterCloseCountsPerClass(t *testing.T) {
	t.Parallel()

	adapter := newObserverAdapter(ObserverFunc(func(context.Context, Observation) {}))
	adapter.close()
	<-adapter.done
	adapter.emit(Observation{Kind: ObservationClaim, Operation: "probe"})
	adapter.emit(Observation{Kind: ObservationRun, Operation: ObservationOpTerminal, Outcome: ObservationOutcomeFailed})
	if adapter.dropped.Load() != 1 || adapter.droppedTerminal.Load() != 1 {
		t.Fatalf("post-close drops duty=%d terminal=%d, want 1/1",
			adapter.dropped.Load(), adapter.droppedTerminal.Load())
	}
}
