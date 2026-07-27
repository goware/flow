package flow

import (
	"context"
	"sync/atomic"
	"testing"
	"time"
)

func TestObserverAdapterIsBoundedAndPanicIsolated(t *testing.T) {
	observer := &blockingObserver{release: make(chan struct{})}
	adapter := newObserverAdapter(observer)
	adapter.run()
	adapter.emit(Observation{Kind: ObservationRuntime, Operation: "first"})
	deadline := time.Now().Add(time.Second)
	for observer.started.Load() == 0 && time.Now().Before(deadline) {
		time.Sleep(time.Millisecond)
	}
	if observer.started.Load() == 0 {
		t.Fatal("observer did not receive first value")
	}
	start := time.Now()
	for index := 0; index < observerQueueSize+100; index++ {
		adapter.emit(Observation{Kind: ObservationCommand, Operation: "queued"})
	}
	if time.Since(start) > 100*time.Millisecond {
		t.Fatal("bounded observer queue blocked execution")
	}
	if adapter.dropped.Load() == 0 {
		t.Fatal("observer queue overflow was not counted")
	}
	close(observer.release)
	adapter.close()

	panicking := newObserverAdapter(ObserverFunc(func(context.Context, Observation) { panic("adapter") }))
	panicking.emit(Observation{Kind: ObservationRuntime})
	panicking.close()
}

type ObserverFunc func(context.Context, Observation)

func (fn ObserverFunc) Observe(ctx context.Context, observation Observation) { fn(ctx, observation) }

type blockingObserver struct {
	release chan struct{}
	started atomic.Int64
}

func (observer *blockingObserver) Observe(context.Context, Observation) {
	observer.started.Add(1)
	<-observer.release
}
