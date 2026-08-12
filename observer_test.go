package flow

import (
	"context"
	"sync/atomic"
	"testing"
	"time"

	"github.com/goware/flow/internal/testpg"
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
		t.Fatal("bounded observer queue blocked run")
	}
	if adapter.dropped.Load() == 0 {
		t.Fatal("observer queue overflow was not counted")
	}
	close(observer.release)
	adapter.close()
	select {
	case <-adapter.done:
	case <-time.After(time.Second):
		t.Fatal("cooperative observer adapter did not finish")
	}

	panicking := newObserverAdapter(ObserverFunc(func(context.Context, Observation) { panic("adapter") }))
	panicking.emit(Observation{Kind: ObservationRuntime})
	panicking.close()
	select {
	case <-panicking.done:
	case <-time.After(time.Second):
		t.Fatal("panicking observer adapter did not finish")
	}
}

func TestObserverCloseCancelsWithoutWaitingForBlockedCallback(t *testing.T) {
	release := make(chan struct{})
	started := make(chan context.Context, 1)
	adapter := newObserverAdapter(ObserverFunc(func(ctx context.Context, _ Observation) {
		started <- ctx
		<-release
	}))
	t.Cleanup(func() {
		select {
		case <-release:
		default:
			close(release)
		}
		select {
		case <-adapter.done:
		case <-time.After(time.Second):
			t.Error("observer delivery goroutine did not finish after release")
		}
	})
	adapter.emit(Observation{Kind: ObservationRuntime, Operation: "blocked"})
	adapter.run()

	var callbackCtx context.Context
	select {
	case callbackCtx = <-started:
	case <-time.After(time.Second):
		t.Fatal("observer callback did not start")
	}
	closed := make(chan struct{})
	go func() {
		adapter.close()
		close(closed)
	}()
	select {
	case <-closed:
	case <-time.After(100 * time.Millisecond):
		t.Fatal("adapter close waited for blocked observer")
	}
	select {
	case <-callbackCtx.Done():
	case <-time.After(time.Second):
		t.Fatal("observer callback context was not cancelled")
	}
	select {
	case <-adapter.done:
		t.Fatal("blocked observer unexpectedly returned before release")
	default:
	}
}

func TestBlockedObserverCannotBlockRuntimeStop(t *testing.T) {
	t.Parallel()

	database := testpg.Open(t)
	ctx := context.Background()
	if err := Migrate(ctx, database.DB, WithSchema(database.Schema)); err != nil {
		t.Fatal(err)
	}
	release := make(chan struct{})
	started := make(chan context.Context, 1)
	observer := ObserverFunc(func(callbackCtx context.Context, _ Observation) {
		select {
		case started <- callbackCtx:
		default:
		}
		<-release
	})
	runtime, err := New(database.DB, WithSchema(database.Schema), WithNotifications(false),
		WithPollInterval(time.Hour), WithObserver(observer))
	if err != nil {
		t.Fatal(err)
	}
	_, runResult := startRuntime(t, runtime)
	var callbackCtx context.Context
	select {
	case callbackCtx = <-started:
	case <-time.After(time.Second):
		close(release)
		t.Fatal("runtime observer did not start")
	}
	t.Cleanup(func() {
		select {
		case <-release:
		default:
			close(release)
		}
		select {
		case <-runtime.observations.done:
		case <-time.After(time.Second):
			t.Error("runtime observer goroutine did not finish after release")
		}
	})
	stopCtx, cancelStop := context.WithTimeout(ctx, time.Second)
	defer cancelStop()
	if err := runtime.Stop(stopCtx); err != nil {
		t.Fatalf("Stop() with blocked observer error = %v", err)
	}
	select {
	case err := <-runResult:
		if err != nil {
			t.Fatalf("Run() error = %v", err)
		}
	case <-time.After(time.Second):
		t.Fatal("Run() waited for blocked observer")
	}
	select {
	case <-callbackCtx.Done():
	case <-time.After(time.Second):
		t.Fatal("runtime observer context was not cancelled")
	}
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
