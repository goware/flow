package flow

import (
	"context"
	"testing"
	"time"

	"github.com/goware/flow/internal/testpg"
)

func TestCommandCeilingRejectsWorkerPlanAndCoordinatorBatchesAtomically(t *testing.T) {
	t.Parallel()

	database := testpg.Open(t)
	ctx := context.Background()
	if err := Migrate(ctx, database.DB, WithSchema(database.Schema)); err != nil {
		t.Fatal(err)
	}
	child := DefineCommand[None, None]("ceiling.batch.child", 1)
	event := DefineEvent[None]("ceiling.batch.event", 1)

	t.Run("worker", func(t *testing.T) {
		parent := DefineCommand[None, None]("ceiling.batch.parent", 1, WithMaxAttempts(1))
		runtime, err := New(database.DB, WithSchema(database.Schema), WithMaxCommandsPerExecution(2),
			WithNotifications(false), WithPollInterval(5*time.Millisecond))
		if err != nil {
			t.Fatal(err)
		}
		if err := runtime.Register(Handle(parent, func(_ context.Context, work *Work[None]) (None, error) {
			if err := Emit(work, event, "must-rollback", None{}); err != nil {
				return None{}, err
			}
			if err := Spawn(work, "child/1", child, None{}); err != nil {
				return None{}, err
			}
			if err := Spawn(work, "child/2", child, None{}); err != nil {
				return None{}, err
			}
			return None{}, nil
		})); err != nil {
			t.Fatal(err)
		}
		cancel, result := startRuntime(t, runtime)
		handle, err := parent.With(runtime).Execute(ctx, "ceiling/worker", None{})
		if err != nil {
			t.Fatal(err)
		}
		waitForExecutionStatus(t, database.Schema, database.DB.Conn, handle.ID, "failed", 5*time.Second)
		trace, err := Trace(ctx, runtime, handle.ID)
		if err != nil {
			t.Fatal(err)
		}
		stopRuntime(t, cancel, result)
		if len(trace.Commands) != 1 || trace.Execution.CommandCount != 1 || trace.Commands[0].FailureCode != "invalid_decision" {
			t.Fatalf("worker ceiling trace = %#v", trace)
		}
		for _, recorded := range trace.Events {
			if recorded.Name == event.Name() {
				t.Fatal("worker ceiling committed a staged application event")
			}
		}
	})

	t.Run("plan", func(t *testing.T) {
		plan := DefinePlan[None]("ceiling.batch.plan", 1, func(plan *Plan, _ None) {
			Do(plan, "child/1", child, None{})
			Do(plan, "child/2", child, None{})
		})
		runtime, err := New(database.DB, WithSchema(database.Schema), WithMaxCommandsPerExecution(1),
			WithNotifications(false), WithPollInterval(5*time.Millisecond))
		if err != nil {
			t.Fatal(err)
		}
		if err := runtime.Register(plan); err != nil {
			t.Fatal(err)
		}
		cancel, result := startRuntime(t, runtime)
		handle, err := plan.With(runtime).Execute(ctx, "ceiling/plan", None{})
		if err != nil {
			t.Fatal(err)
		}
		waitForExecutionStatus(t, database.Schema, database.DB.Conn, handle.ID, "failed", 5*time.Second)
		trace, err := Trace(ctx, runtime, handle.ID)
		if err != nil {
			t.Fatal(err)
		}
		stopRuntime(t, cancel, result)
		if len(trace.Commands) != 0 || trace.Execution.CommandCount != 0 || trace.Execution.FailureCode != "plan_defect" {
			t.Fatalf("plan ceiling trace = %#v", trace)
		}
	})

	t.Run("coordinator", func(t *testing.T) {
		coordinator := DefineCoordinator[None]("ceiling.batch.coordinator", 1,
			OnStart(func(_ context.Context, coordination *Coordination[None]) error {
				if err := Spawn(coordination, "child/1", child, None{}); err != nil {
					return err
				}
				return Spawn(coordination, "child/2", child, None{})
			}),
		)
		runtime, err := New(database.DB, WithSchema(database.Schema), WithMaxCommandsPerExecution(1),
			WithNotifications(false), WithPollInterval(5*time.Millisecond))
		if err != nil {
			t.Fatal(err)
		}
		if err := runtime.Register(coordinator); err != nil {
			t.Fatal(err)
		}
		cancel, result := startRuntime(t, runtime)
		handle, err := coordinator.With(runtime).Execute(ctx, "ceiling/coordinator", None{})
		if err != nil {
			t.Fatal(err)
		}
		waitForExecutionStatus(t, database.Schema, database.DB.Conn, handle.ID, "failed", 5*time.Second)
		trace, err := Trace(ctx, runtime, handle.ID)
		if err != nil {
			t.Fatal(err)
		}
		stopRuntime(t, cancel, result)
		if len(trace.Commands) != 0 || trace.Execution.CommandCount != 0 || trace.Execution.FailureCode != "invalid_decision" {
			t.Fatalf("coordinator ceiling trace = %#v", trace)
		}
	})
}
