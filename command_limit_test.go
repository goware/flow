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
		runtime, err := New(database.DB, WithSchema(database.Schema), WithMaxCommandsPerRun(2),
			WithNotifications(false), WithPollInterval(5*time.Millisecond))
		if err != nil {
			t.Fatal(err)
		}
		if err := runtime.Register(Handle(parent, func(_ context.Context, work *Work[None]) (None, error) {
			Enqueue(work, "child/1", child, None{})
			Enqueue(work, "child/2", child, None{})
			return None{}, nil
		})); err != nil {
			t.Fatal(err)
		}
		cancel, result := startRuntime(t, runtime)
		exec, err := parent.Enqueue(ctx, runtime, "ceiling/worker", None{})
		if err != nil {
			t.Fatal(err)
		}
		waitForRunStatus(t, database.Schema, database.DB.Conn, exec.RunID, "failed", 5*time.Second)
		trace, err := Trace(ctx, runtime, exec.RunID)
		if err != nil {
			t.Fatal(err)
		}
		stopRuntime(t, cancel, result)
		if len(trace.Commands) != 1 || trace.Run.CommandCount != 1 || trace.Commands[0].Failure == nil || trace.Commands[0].Failure.Code != "invalid_decision" {
			t.Fatalf("worker ceiling trace = %#v", trace)
		}
	})
}
