package flow

import (
	"context"
	"errors"
	"sync"
	"testing"

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
	command := DefineCommand[ingressArgs, ingressResult]("fault.work", 1)

	for _, point := range []fault.Point{fault.IngressBeforeJournal, fault.IngressBeforeCommit} {
		runtime.faults = fault.Func(func(_ context.Context, got fault.Point) error {
			if got == point {
				return fault.Injected(got)
			}
			return nil
		})
		if _, err := command.With(runtime).Execute(ctx, "fault/"+string(point), ingressArgs{}); !errors.Is(err, fault.ErrInjected) {
			t.Fatalf("Execute(%s) error = %v", point, err)
		}
		var count int
		if err := database.DB.Conn.QueryRow(ctx, `SELECT count(*) FROM `+pgschema.Table(database.Schema, "flow_executions")+`
			WHERE execution_key=$1`, "fault/"+string(point)).Scan(&count); err != nil {
			t.Fatalf("count fault execution: %v", err)
		}
		if count != 0 {
			t.Fatalf("fault %s left %d executions", point, count)
		}
	}

	runtime.faults = fault.None{}
	handle, err := command.With(runtime).Execute(ctx, "observed", ingressArgs{})
	if err != nil {
		t.Fatalf("Execute(observed) error = %v", err)
	}
	observations := observer.snapshot()
	if len(observations) != 1 || observations[0].ExecutionID != handle.ID || observations[0].Operation != "start" {
		t.Fatalf("observations = %#v", observations)
	}

	runtime.faults = fault.Func(func(_ context.Context, got fault.Point) error {
		if got == fault.IngressCommitAmbiguous {
			return fault.Injected(got)
		}
		return nil
	})
	if _, err := command.With(runtime).Execute(ctx, "ambiguous", ingressArgs{}); !errors.Is(err, fault.ErrInjected) {
		t.Fatalf("Execute(ambiguous) error = %v", err)
	}
	runtime.faults = fault.None{}
	recovered, err := command.With(runtime).Execute(ctx, "ambiguous", ingressArgs{})
	if err != nil || recovered.Created {
		t.Fatalf("Execute(ambiguous retry) = %#v, %v", recovered, err)
	}

	observer.panic = true
	if err := CancelExecution(ctx, runtime, handle.ID, "safe observer panic"); err != nil {
		t.Fatalf("CancelExecution() with panicking observer error = %v", err)
	}
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
