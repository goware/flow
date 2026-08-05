package flow

import (
	"context"
	"testing"
	"time"

	"github.com/goware/flow/internal/testpg"
)

func TestCommandCeilingRejectsWorkerBatchAtomically(t *testing.T) {
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
		exec, err := parent.With(runtime).Execute(ctx, "ceiling/worker", None{})
		if err != nil {
			t.Fatal(err)
		}
		waitForExecutionStatus(t, database.Schema, database.DB.Conn, exec.ID, "failed", 5*time.Second)
		trace, err := Trace(ctx, runtime, exec.ID)
		if err != nil {
			t.Fatal(err)
		}
		stopRuntime(t, cancel, result)
		if len(trace.Commands) != 1 || trace.Execution.CommandCount != 1 || trace.Commands[0].FailureCode != "invalid_decision" {
			t.Fatalf("worker ceiling trace = %#v", trace)
		}
	})
}
