package flow

import (
	"context"
	"testing"
	"time"

	"github.com/goware/flow/internal/fault"
	"github.com/goware/flow/internal/testpg"
)

func TestMaintenanceFaultLeavesDeadlineRecoverableByAnotherRuntime(t *testing.T) {
	t.Parallel()

	database := testpg.Open(t)
	ctx := context.Background()
	if err := Migrate(ctx, database.DB, WithSchema(database.Schema)); err != nil {
		t.Fatal(err)
	}
	command := DefineCommand[None, None]("maintenance.deadline", 1)
	first, err := New(database.DB, WithSchema(database.Schema), WithNotifications(false), WithPollInterval(5*time.Millisecond))
	if err != nil {
		t.Fatal(err)
	}
	first.faults = fault.Func(func(_ context.Context, point fault.Point) error {
		if point == fault.MaintenanceAfterProbe {
			return fault.Injected(point)
		}
		return nil
	})
	cancelFirst, firstResult := startRuntime(t, first)
	exec, err := command.With(first).Execute(ctx, "maintenance/fault", None{}, WithExecutionDeadline(40*time.Millisecond))
	if err != nil {
		t.Fatal(err)
	}
	time.Sleep(120 * time.Millisecond)
	execution, err := GetExecution(ctx, first, exec.ID)
	if err != nil || execution.Status != "running" {
		t.Fatalf("faulted maintenance execution = %#v, %v", execution, err)
	}
	stopRuntime(t, cancelFirst, firstResult)

	second, err := New(database.DB, WithSchema(database.Schema), WithNotifications(false), WithPollInterval(5*time.Millisecond))
	if err != nil {
		t.Fatal(err)
	}
	cancelSecond, secondResult := startRuntime(t, second)
	waitForExecutionStatus(t, database.Schema, database.DB.Conn, exec.ID, "expired", 3*time.Second)
	stopRuntime(t, cancelSecond, secondResult)
	trace, err := Trace(ctx, mustReader(t, database), exec.ID)
	if err != nil || trace.Execution.Status != "expired" || trace.Execution.OpenCommands != 0 {
		t.Fatalf("recovered maintenance trace = %#v, %v", trace, err)
	}
}

func mustReader(t *testing.T, database testpg.Database) *Runtime {
	t.Helper()
	runtime, err := New(database.DB, WithSchema(database.Schema), WithNotifications(false))
	if err != nil {
		t.Fatal(err)
	}
	return runtime
}
