package flow

import (
	"context"
	"fmt"
	"sync"
	"sync/atomic"
	"testing"
	"time"

	"github.com/goware/flow/internal/fault"
	"github.com/goware/flow/internal/pgschema"
	"github.com/goware/flow/internal/store"
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
	observer := &recordingObserver{}
	first, err := New(database.DB, WithSchema(database.Schema), WithNotifications(false),
		WithPollInterval(5*time.Millisecond), WithObserver(observer))
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
	waitForObservation(t, observer, "deadline", "error", 1, time.Second)
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

func TestMaintenancePassReportsAndDrainsFullDeadlinePage(t *testing.T) {
	t.Parallel()

	database := testpg.Open(t)
	ctx := context.Background()
	if err := Migrate(ctx, database.DB, WithSchema(database.Schema)); err != nil {
		t.Fatal(err)
	}
	runtime, err := New(database.DB, WithSchema(database.Schema), WithNotifications(false))
	if err != nil {
		t.Fatal(err)
	}
	command := DefineCommand[None, None]("maintenance.backlog", 1)
	const executions = maintenanceExecutionPage + 1
	for index := range executions {
		if _, err := command.With(runtime).Execute(ctx, fmt.Sprintf("maintenance/backlog/%03d", index), None{},
			WithExecutionDeadline(30*time.Millisecond)); err != nil {
			t.Fatal(err)
		}
	}
	time.Sleep(40 * time.Millisecond)

	first := runtime.runMaintenancePass(ctx)
	if !first.saturated || !first.progressed {
		t.Fatalf("first maintenance pass = %+v", first)
	}
	second := runtime.runMaintenancePass(ctx)
	if second.saturated || !second.progressed {
		t.Fatalf("second maintenance pass = %+v", second)
	}
	var expired int
	if err := database.DB.Conn.QueryRow(ctx, `SELECT count(*) FROM `+
		pgschema.Table(database.Schema, "flow_executions")+` WHERE status='expired'`).Scan(&expired); err != nil {
		t.Fatal(err)
	}
	if expired != executions {
		t.Fatalf("expired executions = %d, want %d", expired, executions)
	}
}

func TestMaintenanceFullLockedPageMakesNoProgress(t *testing.T) {
	t.Parallel()

	database := testpg.Open(t)
	ctx := context.Background()
	if err := Migrate(ctx, database.DB, WithSchema(database.Schema)); err != nil {
		t.Fatal(err)
	}
	runtime, err := New(database.DB, WithSchema(database.Schema), WithNotifications(false))
	if err != nil {
		t.Fatal(err)
	}
	command := DefineCommand[None, None]("maintenance.locked_backlog", 1)
	for index := range maintenanceExecutionPage {
		if _, err := command.With(runtime).Execute(ctx, fmt.Sprintf("maintenance/locked/%03d", index), None{},
			WithExecutionDeadline(30*time.Millisecond)); err != nil {
			t.Fatal(err)
		}
	}
	time.Sleep(40 * time.Millisecond)
	lockTx, err := database.DB.Conn.Begin(ctx)
	if err != nil {
		t.Fatal(err)
	}
	defer func() { _ = lockTx.Rollback(context.Background()) }()
	if _, err := lockTx.Exec(ctx, `SELECT execution_id FROM `+
		pgschema.Table(database.Schema, "flow_executions")+`
		WHERE status='running' ORDER BY execution_id FOR UPDATE`); err != nil {
		t.Fatal(err)
	}

	result := runtime.runMaintenancePass(ctx)
	if !result.saturated || result.progressed {
		t.Fatalf("locked maintenance pass = %+v", result)
	}
}

func TestMaintenanceCategoryErrorDoesNotStarveOtherCategories(t *testing.T) {
	t.Parallel()

	database := testpg.Open(t)
	ctx := context.Background()
	if err := Migrate(ctx, database.DB, WithSchema(database.Schema)); err != nil {
		t.Fatal(err)
	}
	observer := &recordingObserver{}
	runtime, err := New(database.DB, WithSchema(database.Schema), WithNotifications(false), WithObserver(observer))
	if err != nil {
		t.Fatal(err)
	}
	runtime.observations.run()
	defer runtime.observations.close()
	deadlineCommand := DefineCommand[None, None]("maintenance.category_deadline", 1)
	waitCommand := DefineCommand[None, None]("maintenance.category_wait", 1)
	leaseCommand := DefineCommand[None, None]("maintenance.category_lease", 1)
	waitEvent := DefineEvent[None]("maintenance.category_event")
	deadlineExecution, err := deadlineCommand.With(runtime).Execute(ctx, "maintenance/category/deadline", None{},
		WithExecutionDeadline(30*time.Millisecond))
	if err != nil {
		t.Fatal(err)
	}
	waitExecution, err := waitCommand.With(runtime).Execute(ctx, "maintenance/category/wait", None{},
		WaitFor(waitEvent, "missing"), Within(30*time.Millisecond))
	if err != nil {
		t.Fatal(err)
	}
	leaseExecution, err := leaseCommand.With(runtime).Execute(ctx, "maintenance/category/lease", None{})
	if err != nil {
		t.Fatal(err)
	}
	candidates, err := runtime.store.ProbeCommands(ctx,
		[]store.CommandKind{{Name: leaseCommand.Name(), Version: leaseCommand.Version()}}, 1)
	if err != nil || len(candidates) != 1 {
		t.Fatalf("ProbeCommands() candidates=%d, err=%v", len(candidates), err)
	}
	claimed, err := runtime.store.ClaimCommands(ctx, candidates, time.Minute, "maintenance-category-test", fault.None{})
	if err != nil || len(claimed.Commands) != 1 {
		t.Fatalf("ClaimCommands() commands=%d, err=%v", len(claimed.Commands), err)
	}
	if _, err := database.DB.Conn.Exec(ctx, `UPDATE `+
		pgschema.Table(database.Schema, "flow_command_queue")+`
		SET lease_expires_at=clock_timestamp()-interval '1 second' WHERE command_id=$1`,
		claimed.Commands[0].CommandID); err != nil {
		t.Fatal(err)
	}
	time.Sleep(40 * time.Millisecond)
	var faultHits atomic.Int32
	runtime.faults = fault.Func(func(_ context.Context, point fault.Point) error {
		if point == fault.MaintenanceAfterProbe && faultHits.Add(1) == 1 {
			return fault.Injected(point)
		}
		return nil
	})

	result := runtime.runMaintenancePass(ctx)
	if !result.progressed {
		t.Fatalf("maintenance pass after one category error = %+v", result)
	}
	waitForObservation(t, observer, "deadline", "error", 1, time.Second)
	var deadlineStatus, waitStatus, leaseState string
	if err := database.DB.Conn.QueryRow(ctx, `SELECT status FROM `+
		pgschema.Table(database.Schema, "flow_executions")+` WHERE execution_id=$1`, deadlineExecution.ID).
		Scan(&deadlineStatus); err != nil {
		t.Fatal(err)
	}
	if err := database.DB.Conn.QueryRow(ctx, `SELECT status FROM `+
		pgschema.Table(database.Schema, "flow_executions")+` WHERE execution_id=$1`, waitExecution.ID).
		Scan(&waitStatus); err != nil {
		t.Fatal(err)
	}
	if err := database.DB.Conn.QueryRow(ctx, `SELECT state FROM `+
		pgschema.Table(database.Schema, "flow_commands")+` WHERE execution_id=$1`, leaseExecution.ID).
		Scan(&leaseState); err != nil {
		t.Fatal(err)
	}
	if deadlineStatus != "running" || waitStatus != "failed" || leaseState != "retry_wait" {
		t.Fatalf("maintenance states deadline=%s wait=%s lease=%s", deadlineStatus, waitStatus, leaseState)
	}
}

func TestWaitExpiryScanErrorIsReported(t *testing.T) {
	t.Parallel()

	database := testpg.Open(t)
	ctx := context.Background()
	if err := Migrate(ctx, database.DB, WithSchema(database.Schema)); err != nil {
		t.Fatal(err)
	}
	observer := &recordingObserver{}
	runtime, err := New(database.DB, WithSchema(database.Schema), WithNotifications(false), WithObserver(observer))
	if err != nil {
		t.Fatal(err)
	}
	runtime.observations.run()
	defer runtime.observations.close()
	command := DefineCommand[None, None]("maintenance.wait_scan_error", 1)
	event := DefineEvent[None]("maintenance.wait_scan_error_event")
	execution, err := command.With(runtime).Execute(ctx, "maintenance/wait-scan-error", None{},
		WaitFor(event, "missing"), Within(time.Minute))
	if err != nil {
		t.Fatal(err)
	}
	schema := pgschema.Table(database.Schema, "flow_commands")
	if _, err := database.DB.Conn.Exec(ctx, `UPDATE `+schema+`
		SET wait_deadline_at=clock_timestamp()-interval '1 second' WHERE command_id=$1`, execution.RootCommandID); err != nil {
		t.Fatal(err)
	}
	// The expiry probe does not read required, while ExpireCommandWait scans it
	// into a bool. This isolated schema corruption forces the transition query's
	// scan error without adding a production fault hook.
	if _, err := database.DB.Conn.Exec(ctx, `ALTER TABLE `+schema+`
		ALTER COLUMN required TYPE text USING required::text`); err != nil {
		t.Fatal(err)
	}

	runtime.runMaintenancePass(ctx)
	waitForObservation(t, observer, "wait_expiry", "error", 1, time.Second)
	for _, observation := range observer.snapshot() {
		if observation.Operation == "wait_expiry" && observation.Outcome == "noop" {
			t.Fatalf("wait expiry scan error was reported as noop: %#v", observer.snapshot())
		}
	}
}

func TestMaintenanceReplicasApplyEachDeadlineOnce(t *testing.T) {
	t.Parallel()

	database := testpg.Open(t)
	ctx := context.Background()
	if err := Migrate(ctx, database.DB, WithSchema(database.Schema)); err != nil {
		t.Fatal(err)
	}
	first, err := New(database.DB, WithSchema(database.Schema), WithNotifications(false))
	if err != nil {
		t.Fatal(err)
	}
	second, err := New(database.DB, WithSchema(database.Schema), WithNotifications(false))
	if err != nil {
		t.Fatal(err)
	}
	command := DefineCommand[None, None]("maintenance.replica_deadline", 1)
	const executions = 20
	for index := range executions {
		if _, err := command.With(first).Execute(ctx, fmt.Sprintf("maintenance/replica/%03d", index), None{},
			WithExecutionDeadline(30*time.Millisecond)); err != nil {
			t.Fatal(err)
		}
	}
	time.Sleep(40 * time.Millisecond)
	var group sync.WaitGroup
	group.Add(2)
	for _, runtime := range []*Runtime{first, second} {
		runtime := runtime
		go func() {
			defer group.Done()
			runtime.runMaintenancePass(ctx)
		}()
	}
	group.Wait()
	var expired, terminalEntries int
	if err := database.DB.Conn.QueryRow(ctx, `SELECT count(*) FROM `+
		pgschema.Table(database.Schema, "flow_executions")+` WHERE status='expired'`).Scan(&expired); err != nil {
		t.Fatal(err)
	}
	if err := database.DB.Conn.QueryRow(ctx, `SELECT count(*) FROM `+
		pgschema.Table(database.Schema, "flow_journal")+` WHERE event_class='execution_terminal'`).
		Scan(&terminalEntries); err != nil {
		t.Fatal(err)
	}
	if expired != executions || terminalEntries != executions {
		t.Fatalf("replica maintenance expired=%d terminal_entries=%d, want %d", expired, terminalEntries, executions)
	}
}

func TestMaintenanceLoopStopsWithContext(t *testing.T) {
	runtime := &Runtime{pollInterval: time.Hour}
	ctx, cancel := context.WithCancel(context.Background())
	done := make(chan struct{})
	go func() {
		runtime.runMaintenance(ctx)
		close(done)
	}()
	cancel()
	select {
	case <-done:
	case <-time.After(time.Second):
		t.Fatal("maintenance loop did not stop after context cancellation")
	}
}

func TestMaintenancePromptDrainDelayIsBounded(t *testing.T) {
	const pollInterval = time.Second
	for _, result := range []maintenancePassResult{
		{progressed: false, saturated: true},
		{progressed: true, saturated: false},
	} {
		delay, passes := nextMaintenanceDelay(pollInterval, result, 4)
		if delay != pollInterval || passes != 0 {
			t.Fatalf("non-draining maintenance delay=%s passes=%d", delay, passes)
		}
	}
	passes := 0
	for turn := 1; turn < maintenanceDrainPasses; turn++ {
		delay, next := nextMaintenanceDelay(pollInterval,
			maintenancePassResult{progressed: true, saturated: true}, passes)
		if delay != time.Millisecond || next != turn {
			t.Fatalf("prompt maintenance turn %d delay=%s passes=%d", turn, delay, next)
		}
		passes = next
	}
	delay, passes := nextMaintenanceDelay(pollInterval,
		maintenancePassResult{progressed: true, saturated: true}, passes)
	if delay != 25*time.Millisecond || passes != 0 {
		t.Fatalf("yielding maintenance delay=%s passes=%d", delay, passes)
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
