package flow

import (
	"context"
	"errors"
	"sync"
	"testing"
	"time"

	"github.com/goware/flow/internal/fault"
	"github.com/goware/flow/internal/pgschema"
	"github.com/goware/flow/internal/testpg"
)

func TestIngressFaultRollbackAndPostCommitObservation(t *testing.T) {
	t.Parallel()

	database := testpg.Open(t)
	ctx := context.Background()
	if err := Migrate(ctx, database.DB, WithSchema(database.Schema)); err != nil {
		t.Fatalf("Migrate() error = %v", err)
	}
	observer := &recordingObserver{}
	runtime, err := New(database.DB, WithSchema(database.Schema), WithObserver(observer))
	if err != nil {
		t.Fatalf("New() error = %v", err)
	}
	// Run owns this adapter in production. This ingress-only test starts it
	// explicitly while still proving that New itself starts no goroutine.
	runtime.observations.run()
	defer runtime.observations.close()
	command := DefineCommand[ingressArgs, ingressResult]("fault.work", 1)

	for _, point := range []fault.Point{fault.IngressBeforeJournal, fault.IngressBeforeCommit} {
		runtime.faults = fault.Func(func(_ context.Context, got fault.Point) error {
			if got == point {
				return fault.Injected(got)
			}
			return nil
		})
		if _, err := command.Enqueue(ctx, runtime, "fault/"+string(point), ingressArgs{}); !errors.Is(err, fault.ErrInjected) {
			t.Fatalf("Enqueue(%s) error = %v", point, err)
		}
		var count int
		if err := database.DB.Conn.QueryRow(ctx, `SELECT count(*) FROM `+pgschema.Table(database.Schema, "flow_runs")+`
			WHERE run_key=$1`, "fault/"+string(point)).Scan(&count); err != nil {
			t.Fatalf("count fault run: %v", err)
		}
		if count != 0 {
			t.Fatalf("fault %s left %d runs", point, count)
		}
	}

	runtime.faults = fault.None{}
	exec, err := command.Enqueue(ctx, runtime, "observed", ingressArgs{})
	if err != nil {
		t.Fatalf("Enqueue(observed) error = %v", err)
	}
	observations := waitForObservations(t, observer, 1)
	if len(observations) != 1 || observations[0].RunID != exec.RunID || observations[0].Operation != "start" {
		t.Fatalf("observations = %#v", observations)
	}
	event := DefineEvent[None]("fault.observed_event")
	if err := event.Deliver(ctx, runtime, exec.RunID, "fact", None{}); err != nil {
		t.Fatalf("Deliver(observed) error = %v", err)
	}
	observations = waitForObservations(t, observer, 2)
	delivered := observations[1]
	if delivered.Kind != ObservationEvent || delivered.Operation != "deliver" ||
		delivered.RunID != exec.RunID || delivered.RunKey != "observed" ||
		delivered.RootCommandName != command.Name() || delivered.OccurredAt.IsZero() {
		t.Fatalf("deliver observation = %#v", delivered)
	}

	runtime.faults = fault.Func(func(_ context.Context, got fault.Point) error {
		if got == fault.IngressCommitAmbiguous {
			return fault.Injected(got)
		}
		return nil
	})
	if _, err := command.Enqueue(ctx, runtime, "ambiguous", ingressArgs{}); !errors.Is(err, fault.ErrInjected) {
		t.Fatalf("Enqueue(ambiguous) error = %v", err)
	}
	runtime.faults = fault.None{}
	recovered, err := command.Enqueue(ctx, runtime, "ambiguous", ingressArgs{})
	if err != nil || recovered.Created {
		t.Fatalf("Enqueue(ambiguous retry) = %#v, %v", recovered, err)
	}

	observer.panic = true
	if err := CancelRun(ctx, runtime, exec.RunID, "safe observer panic"); err != nil {
		t.Fatalf("CancelRun() with panicking observer error = %v", err)
	}
}

func waitForObservations(t *testing.T, observer *recordingObserver, count int) []Observation {
	t.Helper()
	deadline := time.Now().Add(time.Second)
	for time.Now().Before(deadline) {
		if values := observer.snapshot(); len(values) >= count {
			return values
		}
		time.Sleep(time.Millisecond)
	}
	return observer.snapshot()
}

type recordingObserver struct {
	mu     sync.Mutex
	values []Observation
	panic  bool
}

func (o *recordingObserver) Observe(_ context.Context, observation Observation) {
	o.mu.Lock()
	defer o.mu.Unlock()
	if o.panic {
		panic("observer failure")
	}
	o.values = append(o.values, observation)
}

func (o *recordingObserver) snapshot() []Observation {
	o.mu.Lock()
	defer o.mu.Unlock()
	return append([]Observation(nil), o.values...)
}
