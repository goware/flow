package flow

import (
	"context"
	"errors"
	"sync/atomic"
	"testing"
	"time"

	"github.com/goware/flow/internal/pgschema"
	"github.com/goware/flow/internal/testpg"
)

func TestDirectRootWaitsForExactApplicationEvent(t *testing.T) {
	t.Parallel()
	database := testpg.Open(t)
	ctx := context.Background()
	if err := Migrate(ctx, database.DB, WithSchema(database.Schema)); err != nil {
		t.Fatal(err)
	}
	event := DefineEvent[None]("gate.direct_ready")
	command := DefineCommand[None, None]("gate.direct", 1)
	var calls atomic.Int32
	runtime, err := New(database.DB, WithSchema(database.Schema), WithPollInterval(5*time.Millisecond), WithNotifications(false))
	if err != nil {
		t.Fatal(err)
	}
	if err := runtime.Register(Handle(command, func(context.Context, *Work[None]) (None, error) {
		calls.Add(1)
		return None{}, nil
	})); err != nil {
		t.Fatal(err)
	}

	handle, err := command.With(runtime).Execute(ctx, "direct", None{},
		WaitFor(event, "ready"), WaitFor(event, "ready"), Within(2*time.Second), Within(2*time.Second))
	if err != nil {
		t.Fatal(err)
	}
	repeated, err := command.With(runtime).Execute(ctx, "direct", None{}, WaitFor(event, "ready"), Within(2*time.Second))
	if err != nil || repeated.Created || repeated.ID != handle.ID {
		t.Fatalf("equivalent gated start = %+v, %v", repeated, err)
	}
	if _, err := command.With(runtime).Execute(ctx, "direct", None{}, WaitFor(event, "other"), Within(2*time.Second)); !errors.Is(err, ErrConflict) {
		t.Fatalf("different gated start = %v", err)
	}

	var state string
	var unsatisfied int
	if err := database.DB.Conn.QueryRow(ctx, `SELECT state,unsatisfied_waits FROM `+
		pgschema.Table(database.Schema, "flow_commands")+` WHERE command_id=$1`, handle.RootCommandID).
		Scan(&state, &unsatisfied); err != nil {
		t.Fatal(err)
	}
	if state != "pending" || unsatisfied != 1 {
		t.Fatalf("gated root state=%s waits=%d", state, unsatisfied)
	}
	var queued int
	if err := database.DB.Conn.QueryRow(ctx, `SELECT count(*) FROM `+
		pgschema.Table(database.Schema, "flow_command_queue")+` WHERE command_id=$1`, handle.RootCommandID).Scan(&queued); err != nil {
		t.Fatal(err)
	}
	if queued != 0 {
		t.Fatalf("pending root queue rows=%d", queued)
	}

	cancel, runResult := startRuntime(t, runtime)
	defer stopRuntime(t, cancel, runResult)
	if err := event.Emit(ctx, runtime, handle.ID, "not-ready", None{}); err != nil {
		t.Fatal(err)
	}
	time.Sleep(20 * time.Millisecond)
	if calls.Load() != 0 {
		t.Fatalf("non-matching event released root, calls=%d", calls.Load())
	}
	if err := event.Emit(ctx, runtime, handle.ID, "ready", None{}); err != nil {
		t.Fatal(err)
	}
	waitForExecutionStatus(t, database.Schema, database.DB.Conn, handle.ID, "succeeded", 5*time.Second)
	if calls.Load() != 1 {
		t.Fatalf("worker calls=%d", calls.Load())
	}
	assertReplayMatches(t, runtime, handle.ID)
}

func TestWorkerEventSatisfiesNewChildGateInSameDecision(t *testing.T) {
	t.Parallel()
	database := testpg.Open(t)
	ctx := context.Background()
	if err := Migrate(ctx, database.DB, WithSchema(database.Schema)); err != nil {
		t.Fatal(err)
	}
	event := DefineEvent[None]("gate.worker_ready")
	parent := DefineCommand[None, None]("gate.parent", 1)
	child := DefineCommand[None, None]("gate.child", 1)
	var childCalls atomic.Int32
	runtime, err := New(database.DB, WithSchema(database.Schema), WithPollInterval(5*time.Millisecond), WithNotifications(false))
	if err != nil {
		t.Fatal(err)
	}
	if err := runtime.Register(
		Handle(parent, func(_ context.Context, work *Work[None]) (None, error) {
			if err := Emit(work, event, "ready", None{}); err != nil {
				return None{}, err
			}
			Execute(work, "child", child, None{}).WaitFor(event, "ready").Within(time.Second)
			return None{}, nil
		}),
		Handle(child, func(context.Context, *Work[None]) (None, error) {
			childCalls.Add(1)
			return None{}, nil
		}),
	); err != nil {
		t.Fatal(err)
	}
	cancel, runResult := startRuntime(t, runtime)
	defer stopRuntime(t, cancel, runResult)
	handle, err := parent.With(runtime).Execute(ctx, "same-decision", None{})
	if err != nil {
		t.Fatal(err)
	}
	waitForExecutionStatus(t, database.Schema, database.DB.Conn, handle.ID, "succeeded", 5*time.Second)
	if childCalls.Load() != 1 {
		t.Fatalf("child calls=%d", childCalls.Load())
	}
	var waitStarted *time.Time
	var satisfied int
	if err := database.DB.Conn.QueryRow(ctx, `SELECT c.wait_started_at,
		count(*) FILTER (WHERE w.satisfied_position IS NOT NULL) FROM `+pgschema.Table(database.Schema, "flow_commands")+` c
		JOIN `+pgschema.Table(database.Schema, "flow_command_event_waits")+` w USING (command_id)
		WHERE c.execution_id=$1 AND c.command_key='child' GROUP BY c.command_id`, handle.ID).Scan(&waitStarted, &satisfied); err != nil {
		t.Fatal(err)
	}
	if waitStarted != nil || satisfied != 1 {
		t.Fatalf("retained child wait started=%v satisfied=%d", waitStarted, satisfied)
	}
}

func TestCoordinatorStagesMultipleEventGatesWithANDSemantics(t *testing.T) {
	t.Parallel()
	database := testpg.Open(t)
	ctx := context.Background()
	if err := Migrate(ctx, database.DB, WithSchema(database.Schema)); err != nil {
		t.Fatal(err)
	}
	first := DefineEvent[None]("gate.coordinator_first")
	second := DefineEvent[None]("gate.coordinator_second")
	wrong := DefineEvent[None]("gate.coordinator_wrong")
	child := DefineCommand[None, None]("gate.coordinator_child", 1)
	coordinator := DefineCoordinator[None]("gate.coordinator", 1,
		OnStart(func(_ context.Context, coordination *Coordination[None]) error {
			if err := Emit(coordination, first, "first", None{}); err != nil {
				return err
			}
			Execute(coordination, "child", child, None{}).
				WaitFor(first, "first").
				WaitFor(second, "second").
				Within(time.Second)
			return nil
		}),
		OnOutcome(child, func(_ context.Context, coordination *Coordination[None], received Received[Outcome[None]]) error {
			if received.Payload.Status != StatusSucceeded {
				return errors.New("gated child did not succeed")
			}
			coordination.Succeed()
			return nil
		}),
	)
	var childCalls atomic.Int32
	runtime, err := New(database.DB, WithSchema(database.Schema), WithPollInterval(5*time.Millisecond), WithNotifications(false))
	if err != nil {
		t.Fatal(err)
	}
	if err := runtime.Register(coordinator, Handle(child, func(context.Context, *Work[None]) (None, error) {
		childCalls.Add(1)
		return None{}, nil
	})); err != nil {
		t.Fatal(err)
	}
	cancel, runResult := startRuntime(t, runtime)
	defer stopRuntime(t, cancel, runResult)
	handle, err := coordinator.With(runtime).Execute(ctx, "multiple", None{})
	if err != nil {
		t.Fatal(err)
	}
	deadline := time.Now().Add(3 * time.Second)
	for {
		var state string
		var unsatisfied int
		err := database.DB.Conn.QueryRow(ctx, `SELECT state,unsatisfied_waits FROM `+
			pgschema.Table(database.Schema, "flow_commands")+` WHERE execution_id=$1 AND command_key='child'`, handle.ID).
			Scan(&state, &unsatisfied)
		if err == nil && state == "pending" && unsatisfied == 1 {
			break
		}
		if time.Now().After(deadline) {
			t.Fatalf("gated coordinator child did not become pending: state=%q waits=%d err=%v", state, unsatisfied, err)
		}
		time.Sleep(5 * time.Millisecond)
	}
	if err := wrong.Emit(ctx, runtime, handle.ID, "second", None{}); err != nil {
		t.Fatal(err)
	}
	time.Sleep(20 * time.Millisecond)
	if childCalls.Load() != 0 {
		t.Fatalf("wrong event name released child, calls=%d", childCalls.Load())
	}
	if err := second.Emit(ctx, runtime, handle.ID, "second", None{}); err != nil {
		t.Fatal(err)
	}
	waitForExecutionStatus(t, database.Schema, database.DB.Conn, handle.ID, "succeeded", 5*time.Second)
	if childCalls.Load() != 1 {
		t.Fatalf("child calls=%d", childCalls.Load())
	}
	assertReplayMatches(t, runtime, handle.ID)
}

func TestWaitCanExpireWhileInitialDelayIsPending(t *testing.T) {
	t.Parallel()
	database := testpg.Open(t)
	ctx := context.Background()
	if err := Migrate(ctx, database.DB, WithSchema(database.Schema)); err != nil {
		t.Fatal(err)
	}
	event := DefineEvent[None]("gate.expiry")
	command := DefineCommand[None, None]("gate.expiring_command", 1)
	var calls atomic.Int32
	runtime, err := New(database.DB, WithSchema(database.Schema), WithPollInterval(5*time.Millisecond), WithNotifications(false))
	if err != nil {
		t.Fatal(err)
	}
	if err := runtime.Register(Handle(command, func(context.Context, *Work[None]) (None, error) {
		calls.Add(1)
		return None{}, nil
	})); err != nil {
		t.Fatal(err)
	}
	cancel, runResult := startRuntime(t, runtime)
	defer stopRuntime(t, cancel, runResult)
	handle, err := command.With(runtime).Execute(ctx, "expiry", None{},
		WaitFor(event, "missing"), Within(40*time.Millisecond), WithStartDelay(time.Hour))
	if err != nil {
		t.Fatal(err)
	}
	waitForExecutionStatus(t, database.Schema, database.DB.Conn, handle.ID, "failed", 5*time.Second)
	if calls.Load() != 0 {
		t.Fatalf("expired delayed command ran %d times", calls.Load())
	}
	trace, err := Trace(ctx, runtime, handle.ID)
	if err != nil {
		t.Fatal(err)
	}
	if len(trace.Commands) != 1 || trace.Commands[0].State != string(StatusExpired) || trace.Commands[0].FailureCode != "wait_expired" {
		t.Fatalf("expired gate trace=%+v", trace.Commands)
	}
	assertReplayMatches(t, runtime, handle.ID)
}
