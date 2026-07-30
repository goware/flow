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

	t.Run("worker", func(t *testing.T) {
		parent := DefineCommand[None, None]("ceiling.batch.parent", 1, WithRetry(Attempts(1)))
		runtime, err := New(database.DB, WithSchema(database.Schema), WithMaxCommandsPerExecution(2),
			WithNotifications(false), WithPollInterval(5*time.Millisecond))
		if err != nil {
			t.Fatal(err)
		}
		if err := runtime.Register(Handle(parent, func(_ context.Context, work *Work[None]) (None, error) {
			Execute(work, "child/1", child, None{})
			Execute(work, "child/2", child, None{})
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
	})

	t.Run("plan", func(t *testing.T) {
		plan := DefinePlan[None]("ceiling.batch.plan", 1, func(plan *Plan, _ None) {
			Execute(plan, "child/1", child, None{})
			Execute(plan, "child/2", child, None{})
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
				Execute(coordination, "child/1", child, None{})
				Execute(coordination, "child/2", child, None{})
				return nil
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
