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
	event := DefineEvent[string]("gate.direct_ready")
	command := DefineCommand[None, None]("gate.direct", 1)
	var calls atomic.Int32
	var received atomic.Value
	runtime, err := New(database.DB, WithSchema(database.Schema), WithPollInterval(5*time.Millisecond), WithNotifications(false))
	if err != nil {
		t.Fatal(err)
	}
	if err := runtime.Register(Handle(command, func(_ context.Context, work *Work[None]) (None, error) {
		payload, err := ReadEvent(work, event, "ready")
		if err != nil {
			return None{}, err
		}
		received.Store(payload)
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
	if err := event.Emit(ctx, runtime, handle.ID, "not-ready", "ignored"); err != nil {
		t.Fatal(err)
	}
	time.Sleep(20 * time.Millisecond)
	if calls.Load() != 0 {
		t.Fatalf("non-matching event released root, calls=%d", calls.Load())
	}
	if err := event.Emit(ctx, runtime, handle.ID, "ready", "approved"); err != nil {
		t.Fatal(err)
	}
	waitForExecutionStatus(t, database.Schema, database.DB.Conn, handle.ID, "succeeded", 5*time.Second)
	if calls.Load() != 1 {
		t.Fatalf("worker calls=%d", calls.Load())
	}
	if value, _ := received.Load().(string); value != "approved" {
		t.Fatalf("ReadEvent payload=%q", value)
	}
	trace, err := Trace(ctx, runtime, handle.ID)
	if err != nil {
		t.Fatal(err)
	}
	if len(trace.Commands) != 1 || len(trace.Commands[0].Waits) != 1 || trace.Commands[0].Waits[0].SatisfiedPosition == nil {
		t.Fatalf("trace wait satisfaction=%+v", trace.Commands)
	}
	assertReplayMatches(t, runtime, handle.ID)
}

func TestOptionalEventGatedCommandsRemainLiveUntilTerminal(t *testing.T) {
	t.Parallel()
	database := testpg.Open(t)
	ctx := context.Background()
	if err := Migrate(ctx, database.DB, WithSchema(database.Schema)); err != nil {
		t.Fatal(err)
	}
	event := DefineEvent[None]("gate.optional")
	parent := DefineCommand[bool, None]("gate.optional_parent", 1)
	child := DefineCommand[None, None]("gate.optional_child", 1)
	var calls atomic.Int32
	runtime, err := New(database.DB, WithSchema(database.Schema), WithPollInterval(5*time.Millisecond), WithNotifications(false))
	if err != nil {
		t.Fatal(err)
	}
	if err := runtime.Register(
		Handle(parent, func(_ context.Context, work *Work[bool]) (None, error) {
			node := Execute(work, "optional", child, None{}).Optional().WaitFor(event, "ready")
			if work.Args {
				node.Within(40 * time.Millisecond)
			}
			return None{}, nil
		}),
		Handle(child, func(context.Context, *Work[None]) (None, error) {
			calls.Add(1)
			return None{}, nil
		}),
	); err != nil {
		t.Fatal(err)
	}
	cancel, runResult := startRuntime(t, runtime)
	defer stopRuntime(t, cancel, runResult)

	open, err := parent.With(runtime).Execute(ctx, "optional/open", false)
	if err != nil {
		t.Fatal(err)
	}
	time.Sleep(80 * time.Millisecond)
	execution, err := GetExecution(ctx, runtime, open.ID)
	if err != nil {
		t.Fatal(err)
	}
	if execution.Status != "running" {
		t.Fatalf("optional command without Within status=%s, want running", execution.Status)
	}
	if err := event.Emit(ctx, runtime, open.ID, "ready", None{}); err != nil {
		t.Fatal(err)
	}
	waitForExecutionStatus(t, database.Schema, database.DB.Conn, open.ID, "succeeded", 5*time.Second)

	expiring, err := parent.With(runtime).Execute(ctx, "optional/expiring", true)
	if err != nil {
		t.Fatal(err)
	}
	waitForExecutionStatus(t, database.Schema, database.DB.Conn, expiring.ID, "succeeded", 5*time.Second)
	trace, err := Trace(ctx, runtime, expiring.ID)
	if err != nil {
		t.Fatal(err)
	}
	var optionalState string
	for _, command := range trace.Commands {
		if command.Key == "optional" {
			optionalState = command.State
		}
	}
	if len(trace.Commands) != 2 || optionalState != string(StatusExpired) {
		t.Fatalf("optional expiry trace=%+v", trace.Commands)
	}

	deadline, err := parent.With(runtime).Execute(ctx, "optional/deadline", false, WithExecutionDeadline(250*time.Millisecond))
	if err != nil {
		t.Fatal(err)
	}
	waitForExecutionStatus(t, database.Schema, database.DB.Conn, deadline.ID, "expired", 5*time.Second)
	deadlineTrace, err := Trace(ctx, runtime, deadline.ID)
	if err != nil {
		t.Fatal(err)
	}
	var deadlineOptional TraceCommand
	for _, command := range deadlineTrace.Commands {
		if command.Key == "optional" {
			deadlineOptional = command
		}
	}
	if deadlineOptional.State != string(StatusCancelled) || deadlineOptional.FailureCode != "execution_expired" {
		t.Fatalf("optional deadline trace=%+v", deadlineTrace.Commands)
	}
	if calls.Load() != 1 {
		t.Fatalf("optional child calls=%d, want 1", calls.Load())
	}
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

func TestWorkerStagesMultipleEventGatesWithANDSemantics(t *testing.T) {
	t.Parallel()
	database := testpg.Open(t)
	ctx := context.Background()
	if err := Migrate(ctx, database.DB, WithSchema(database.Schema)); err != nil {
		t.Fatal(err)
	}
	first := DefineEvent[None]("gate.worker_first")
	second := DefineEvent[None]("gate.worker_second")
	wrong := DefineEvent[None]("gate.worker_wrong")
	parent := DefineCommand[None, None]("gate.worker_parent", 1)
	child := DefineCommand[None, None]("gate.worker_child", 1)
	var childCalls atomic.Int32
	runtime, err := New(database.DB, WithSchema(database.Schema), WithPollInterval(5*time.Millisecond), WithNotifications(false))
	if err != nil {
		t.Fatal(err)
	}
	if err := runtime.Register(
		Handle(parent, func(_ context.Context, work *Work[None]) (None, error) {
			if err := Emit(work, first, "first", None{}); err != nil {
				return None{}, err
			}
			Execute(work, "child", child, None{}).
				WaitFor(first, "first").
				WaitFor(second, "second").
				Within(time.Second)
			return None{}, nil
		}),
		Handle(child, func(_ context.Context, work *Work[None]) (None, error) {
			if _, err := ReadEvent(work, first, "first"); err != nil {
				return None{}, err
			}
			if _, err := ReadEvent(work, second, "second"); err != nil {
				return None{}, err
			}
			childCalls.Add(1)
			return None{}, nil
		}),
	); err != nil {
		t.Fatal(err)
	}
	cancel, runResult := startRuntime(t, runtime)
	defer stopRuntime(t, cancel, runResult)
	handle, err := parent.With(runtime).Execute(ctx, "multiple", None{})
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
			t.Fatalf("gated child did not become pending: state=%q waits=%d err=%v", state, unsatisfied, err)
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

func TestRequiredChildFailureCancelsGatedJoin(t *testing.T) {
	t.Parallel()
	database := testpg.Open(t)
	ctx := context.Background()
	if err := Migrate(ctx, database.DB, WithSchema(database.Schema)); err != nil {
		t.Fatal(err)
	}
	event := DefineEvent[None]("gate.required_child_completed")
	parent := DefineCommand[None, None]("gate.required_failure_parent", 1)
	producer := DefineCommand[None, None]("gate.required_failure_producer", 1, WithRetry(Attempts(1)))
	join := DefineCommand[None, None]("gate.required_failure_join", 1)
	var joinCalls atomic.Int32
	runtime, err := New(database.DB, WithSchema(database.Schema), WithPollInterval(5*time.Millisecond), WithNotifications(false))
	if err != nil {
		t.Fatal(err)
	}
	if err := runtime.Register(
		Handle(parent, func(_ context.Context, work *Work[None]) (None, error) {
			Execute(work, "producer", producer, None{})
			Execute(work, "join", join, None{}).WaitFor(event, "producer")
			return None{}, nil
		}),
		Handle(producer, func(context.Context, *Work[None]) (None, error) {
			return None{}, Permanent(errors.New("analysis failed"))
		}),
		Handle(join, func(context.Context, *Work[None]) (None, error) {
			joinCalls.Add(1)
			return None{}, nil
		}),
	); err != nil {
		t.Fatal(err)
	}
	cancel, runResult := startRuntime(t, runtime)
	defer stopRuntime(t, cancel, runResult)
	handle, err := parent.With(runtime).Execute(ctx, "required-failure", None{})
	if err != nil {
		t.Fatal(err)
	}
	waitForExecutionStatus(t, database.Schema, database.DB.Conn, handle.ID, "failed", 5*time.Second)
	trace, err := Trace(ctx, runtime, handle.ID)
	if err != nil {
		t.Fatal(err)
	}
	states := make(map[string]TraceCommand, len(trace.Commands))
	for _, command := range trace.Commands {
		states[command.Key] = command
	}
	if states["producer"].State != string(StatusFailed) || states["join"].State != string(StatusCancelled) ||
		states["join"].FailureCode != "fail_fast" || joinCalls.Load() != 0 {
		t.Fatalf("required failure trace=%+v join calls=%d", trace.Commands, joinCalls.Load())
	}
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
	if err := event.Emit(ctx, runtime, handle.ID, "missing", None{}); !errors.Is(err, ErrTerminal) {
		t.Fatalf("late event error=%v, want ErrTerminal", err)
	}
	assertReplayMatches(t, runtime, handle.ID)
}
