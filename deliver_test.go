package flow

import (
	"context"
	"errors"
	"fmt"
	"sync/atomic"
	"testing"
	"time"

	"github.com/goware/flow/internal/testpg"
)

type detachedChildArgs struct {
	Part int `json:"part"`
}

type detachedDonePayload struct {
	Part int `json:"part"`
}

type detachedFanInState struct {
	Expected  int `json:"expected"`
	Completed int `json:"completed"`
}

// A worker attempt in one execution delivers events into a coordinator
// execution through Deliver, transactionally with its own database
// writes; the coordinator joins the set and settles when it drains. This is
// the cross-execution fan-in shape: children run as independent executions
// and the coordinator is discovered by live key.
func TestDeliverCrossExecutionFanIn(t *testing.T) {
	t.Parallel()
	database := testpg.Open(t)
	ctx := context.Background()
	if err := Migrate(ctx, database.DB, WithSchema(database.Schema)); err != nil {
		t.Fatal(err)
	}

	const parts = 3
	done := DefineEvent[detachedDonePayload]("detached.child_done")
	child := DefineCommand[detachedChildArgs, None]("detached.child", 1, WithRetry(Attempts(3)))
	coordinator := DefineCoordinator[detachedFanInState]("detached.fanin", 1,
		OnStart(func(_ context.Context, coordination *Coordination[detachedFanInState]) error {
			coordination.State.Expected = parts
			return nil
		}),
		On(done, func(_ context.Context, coordination *Coordination[detachedFanInState], _ Received[detachedDonePayload]) error {
			coordination.State.Completed++
			if coordination.State.Completed == coordination.State.Expected {
				coordination.Succeed()
			}
			return nil
		}),
	)

	runtime, err := New(database.DB, WithSchema(database.Schema), WithWorkerConcurrency(4),
		WithPollInterval(5*time.Millisecond), WithNotifications(false))
	if err != nil {
		t.Fatal(err)
	}
	var emitted atomic.Int64
	if err := runtime.Register(
		coordinator,
		Handle(child, func(ctx context.Context, work *Work[detachedChildArgs]) (None, error) {
			parent, found, err := LookupLiveExecution(ctx, runtime, coordinator.Name(), "fanin/parent")
			if err != nil || !found {
				return None{}, fmt.Errorf("lookup fan-in coordinator: found=%v err=%w", found, err)
			}
			tx, err := database.DB.Conn.Begin(ctx)
			if err != nil {
				return None{}, err
			}
			defer tx.Rollback(context.WithoutCancel(ctx)) //nolint:errcheck
			key := fmt.Sprintf("child/%d", work.Args.Part)
			payload := detachedDonePayload{Part: work.Args.Part}
			if err := done.Deliver(ctx, runtime.InTx(tx), parent.ID, key, payload); err != nil {
				return None{}, err
			}
			// Identity dedupe: a redelivered attempt re-emitting the same
			// (name, key, payload) must be a silent no-op.
			if err := done.Deliver(ctx, runtime.InTx(tx), parent.ID, key, payload); err != nil {
				return None{}, err
			}
			if err := tx.Commit(ctx); err != nil {
				return None{}, err
			}
			emitted.Add(1)
			return None{}, nil
		}),
	); err != nil {
		t.Fatal(err)
	}

	parent, err := coordinator.With(runtime).Execute(ctx, "fanin/parent", detachedFanInState{}, WithLiveKey())
	if err != nil {
		t.Fatal(err)
	}
	for part := range parts {
		if _, err := child.With(runtime).Execute(ctx, fmt.Sprintf("child/%d", part), detachedChildArgs{Part: part}); err != nil {
			t.Fatal(err)
		}
	}

	cancel, result := startRuntime(t, runtime)
	defer stopRuntime(t, cancel, result)
	waitForExecutionStatus(t, database.Schema, database.DB.Conn, parent.ID, "succeeded", 10*time.Second)
	if got := emitted.Load(); got != parts {
		t.Fatalf("emitting children = %d, want %d", got, parts)
	}
}

// A rolled-back client transaction discards the detached delivery, and plain Emit
// keeps refusing attempt contexts.
func TestDeliverTransactionalityAndEmitGuard(t *testing.T) {
	t.Parallel()
	database := testpg.Open(t)
	ctx := context.Background()
	if err := Migrate(ctx, database.DB, WithSchema(database.Schema)); err != nil {
		t.Fatal(err)
	}

	signal := DefineEvent[None]("detached.guard_signal")
	probe := DefineCommand[None, None]("detached.guard_probe", 1, WithRetry(Attempts(1)))
	target := DefineCoordinator[int]("detached.guard_target", 1,
		OnStart(func(context.Context, *Coordination[int]) error { return nil }),
		On(signal, func(_ context.Context, coordination *Coordination[int], _ Received[None]) error {
			coordination.State++
			return nil
		}),
	)

	runtime, err := New(database.DB, WithSchema(database.Schema), WithWorkerConcurrency(2),
		WithPollInterval(5*time.Millisecond), WithNotifications(false))
	if err != nil {
		t.Fatal(err)
	}
	guardErr := make(chan error, 1)
	if err := runtime.Register(
		target,
		Handle(probe, func(ctx context.Context, work *Work[None]) (None, error) {
			parent, found, err := LookupLiveExecution(ctx, runtime, target.Name(), "guard/parent")
			if err != nil || !found {
				return None{}, fmt.Errorf("lookup guard coordinator: found=%v err=%w", found, err)
			}
			tx, err := database.DB.Conn.Begin(ctx)
			if err != nil {
				return None{}, err
			}
			if err := signal.Deliver(ctx, runtime.InTx(tx), parent.ID, "discarded", None{}); err != nil {
				_ = tx.Rollback(context.WithoutCancel(ctx))
				return None{}, err
			}
			if err := tx.Rollback(context.WithoutCancel(ctx)); err != nil {
				return None{}, err
			}
			guardErr <- signal.Emit(ctx, runtime, parent.ID, "refused", None{})
			return None{}, nil
		}),
	); err != nil {
		t.Fatal(err)
	}

	parent, err := target.With(runtime).Execute(ctx, "guard/parent", 0, WithLiveKey())
	if err != nil {
		t.Fatal(err)
	}
	handle, err := probe.With(runtime).Execute(ctx, "guard/probe", None{})
	if err != nil {
		t.Fatal(err)
	}

	cancel, result := startRuntime(t, runtime)
	defer stopRuntime(t, cancel, result)
	select {
	case err := <-guardErr:
		if !errors.Is(err, ErrInvalidState) {
			t.Fatalf("Emit inside attempt = %v, want ErrInvalidState", err)
		}
	case <-time.After(10 * time.Second):
		t.Fatal("probe handler never ran")
	}
	// The Emit guard poisons the attempt scope, so the probe execution fails;
	// the rolled-back detached delivery must have left no event behind.
	waitForExecutionStatus(t, database.Schema, database.DB.Conn, handle.ID, "failed", 10*time.Second)
	var events int
	if err := database.DB.Conn.QueryRow(ctx, `SELECT count(*) FROM `+database.Schema+`.flow_journal
		WHERE execution_id=$1 AND event_name='detached.guard_signal'`, parent.ID).Scan(&events); err != nil {
		t.Fatal(err)
	}
	if events != 0 {
		t.Fatalf("guard coordinator received %d events, want 0", events)
	}
}
