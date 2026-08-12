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

func TestEventDeltaReadinessUpdatesOnlyAffectedCommands(t *testing.T) {
	t.Parallel()
	database := testpg.Open(t)
	ctx := context.Background()
	if err := Migrate(ctx, database.DB, WithSchema(database.Schema)); err != nil {
		t.Fatal(err)
	}
	shared := DefineEvent[string]("gate.delta_shared")
	second := DefineEvent[string]("gate.delta_second")
	unrelated := DefineEvent[string]("gate.delta_unrelated")
	parent := DefineCommand[None, None]("gate.delta_parent", 1)
	child := DefineCommand[None, None]("gate.delta_child", 1)
	runtime, err := New(database.DB, WithSchema(database.Schema), WithPollInterval(5*time.Millisecond), WithNotifications(false))
	if err != nil {
		t.Fatal(err)
	}
	if err := runtime.Register(Handle(parent, func(_ context.Context, work *Work[None]) (None, error) {
		Enqueue(work, "a-two-waits", child, None{}).WaitFor(shared, "shared").WaitFor(second, "second").Within(time.Minute)
		Enqueue(work, "b-shared", child, None{}).WaitFor(shared, "shared").Within(time.Minute)
		Enqueue(work, "c-shared", child, None{}).WaitFor(shared, "shared").Within(time.Minute)
		Enqueue(work, "z-unrelated", child, None{}).WaitFor(unrelated, "other").Within(time.Minute)
		return None{}, nil
	})); err != nil {
		t.Fatal(err)
	}
	cancel, runResult := startRuntime(t, runtime)
	defer stopRuntime(t, cancel, runResult)
	exec, err := parent.Enqueue(ctx, runtime, "delta", None{})
	if err != nil {
		t.Fatal(err)
	}
	execRun := mustGetRun(t, runtime, exec.RunID)

	deadline := time.Now().Add(3 * time.Second)
	for {
		var children int
		if err := database.DB.Conn.QueryRow(ctx, `SELECT count(*) FROM `+pgschema.Table(database.Schema, "flow_commands")+`
			WHERE run_id=$1 AND parent_command_id=$2`, exec.RunID, execRun.RootCommandID).Scan(&children); err != nil {
			t.Fatal(err)
		}
		if children == 4 {
			break
		}
		if time.Now().After(deadline) {
			t.Fatalf("child declarations=%d, want 4", children)
		}
		time.Sleep(5 * time.Millisecond)
	}

	readStates := func() map[string]struct {
		state       string
		unsatisfied int
	} {
		t.Helper()
		rows, err := database.DB.Conn.Query(ctx, `SELECT command_key,state,unsatisfied_waits
			FROM `+pgschema.Table(database.Schema, "flow_commands")+`
			WHERE run_id=$1 AND parent_command_id=$2 ORDER BY command_key`, exec.RunID, execRun.RootCommandID)
		if err != nil {
			t.Fatal(err)
		}
		defer rows.Close()
		states := make(map[string]struct {
			state       string
			unsatisfied int
		})
		for rows.Next() {
			var key string
			var value struct {
				state       string
				unsatisfied int
			}
			if err := rows.Scan(&key, &value.state, &value.unsatisfied); err != nil {
				t.Fatal(err)
			}
			states[key] = value
		}
		if err := rows.Err(); err != nil {
			t.Fatal(err)
		}
		return states
	}
	readQueuedChildren := func() int {
		t.Helper()
		var queued int
		if err := database.DB.Conn.QueryRow(ctx, `SELECT count(*)
			FROM `+pgschema.Table(database.Schema, "flow_command_queue")+` q
			JOIN `+pgschema.Table(database.Schema, "flow_commands")+` c USING(command_id)
			WHERE c.run_id=$1 AND c.parent_command_id=$2`, exec.RunID, execRun.RootCommandID).Scan(&queued); err != nil {
			t.Fatal(err)
		}
		return queued
	}

	before := readStates()
	queuedBefore := readQueuedChildren()
	if err := unrelated.Deliver(ctx, runtime, exec.RunID, "missing", "no match"); err != nil {
		t.Fatal(err)
	}
	afterNoMatch := readStates()
	for key, state := range before {
		if afterNoMatch[key] != state {
			t.Fatalf("unrelated event changed command %q: before=%+v after=%+v", key, state, afterNoMatch[key])
		}
	}
	if queuedAfter := readQueuedChildren(); queuedAfter != queuedBefore {
		t.Fatalf("unrelated event changed child queue rows: before=%d after=%d", queuedBefore, queuedAfter)
	}
	if err := shared.Deliver(ctx, runtime, exec.RunID, "shared", "accepted"); err != nil {
		t.Fatal(err)
	}
	afterShared := readStates()
	if afterShared["a-two-waits"].state != "pending" || afterShared["a-two-waits"].unsatisfied != 1 ||
		afterShared["b-shared"].state != "ready" || afterShared["b-shared"].unsatisfied != 0 ||
		afterShared["c-shared"].state != "ready" || afterShared["c-shared"].unsatisfied != 0 ||
		afterShared["z-unrelated"] != before["z-unrelated"] {
		t.Fatalf("states after shared event=%+v", afterShared)
	}
	if err := shared.Deliver(ctx, runtime, exec.RunID, "shared", "accepted"); err != nil {
		t.Fatal(err)
	}
	if duplicate := readStates(); duplicate["a-two-waits"] != afterShared["a-two-waits"] ||
		duplicate["b-shared"] != afterShared["b-shared"] || duplicate["c-shared"] != afterShared["c-shared"] {
		t.Fatalf("equivalent duplicate changed readiness: before=%+v after=%+v", afterShared, duplicate)
	}
	if err := shared.Deliver(ctx, runtime, exec.RunID, "shared", "conflict"); !errors.Is(err, ErrConflict) {
		t.Fatalf("conflicting duplicate error=%v, want ErrConflict", err)
	}
	if conflict := readStates(); conflict["a-two-waits"] != afterShared["a-two-waits"] {
		t.Fatalf("conflicting duplicate changed readiness: before=%+v after=%+v", afterShared, conflict)
	}
	if err := second.Deliver(ctx, runtime, exec.RunID, "second", "accepted"); err != nil {
		t.Fatal(err)
	}
	afterSecond := readStates()
	if afterSecond["a-two-waits"].state != "ready" || afterSecond["a-two-waits"].unsatisfied != 0 ||
		afterSecond["z-unrelated"] != before["z-unrelated"] {
		t.Fatalf("states after second event=%+v", afterSecond)
	}
	var queued int
	if err := database.DB.Conn.QueryRow(ctx, `SELECT count(*) FROM `+pgschema.Table(database.Schema, "flow_command_queue")+`
		WHERE run_id=$1 AND command_id<>$2`, exec.RunID, execRun.RootCommandID).Scan(&queued); err != nil {
		t.Fatal(err)
	}
	if queued != 3 {
		t.Fatalf("released child queue rows=%d, want 3", queued)
	}
	if err := CancelRun(ctx, runtime, exec.RunID, "delta readiness test complete"); err != nil {
		t.Fatal(err)
	}
	assertReplayMatches(t, runtime, exec.RunID)
}

func TestWorkerSettlementReleasesSeveralWaitsAsOneDelta(t *testing.T) {
	t.Parallel()
	database := testpg.Open(t)
	ctx := context.Background()
	if err := Migrate(ctx, database.DB, WithSchema(database.Schema)); err != nil {
		t.Fatal(err)
	}
	first := DefineEvent[None]("gate.delta_batch_first")
	second := DefineEvent[None]("gate.delta_batch_second")
	parent := DefineCommand[None, None]("gate.delta_batch_parent", 1)
	producer := DefineCommand[None, None]("gate.delta_batch_producer", 1)
	gated := DefineCommand[None, None]("gate.delta_batch_gated", 1)
	runtime, err := New(database.DB, WithSchema(database.Schema), WithPollInterval(5*time.Millisecond), WithNotifications(false))
	if err != nil {
		t.Fatal(err)
	}
	if err := runtime.Register(
		Handle(parent, func(_ context.Context, work *Work[None]) (None, error) {
			Enqueue(work, "producer", producer, None{})
			Enqueue(work, "gated", gated, None{}).
				WaitFor(first, "first").
				WaitFor(second, "second").
				Within(time.Minute)
			return None{}, nil
		}),
		Handle(producer, func(_ context.Context, work *Work[None]) (None, error) {
			if err := Emit(work, first, "first", None{}); err != nil {
				return None{}, err
			}
			if err := Emit(work, second, "second", None{}); err != nil {
				return None{}, err
			}
			return None{}, nil
		}),
	); err != nil {
		t.Fatal(err)
	}
	cancel, runResult := startRuntime(t, runtime)
	defer stopRuntime(t, cancel, runResult)
	exec, err := parent.Enqueue(ctx, runtime, "batch", None{})
	if err != nil {
		t.Fatal(err)
	}
	deadline := time.Now().Add(5 * time.Second)
	for {
		var state string
		var unsatisfied, satisfied, queued int
		err := database.DB.Conn.QueryRow(ctx, `SELECT c.state,c.unsatisfied_waits,
			count(*) FILTER (WHERE w.satisfied_position IS NOT NULL),
			(SELECT count(*) FROM `+pgschema.Table(database.Schema, "flow_command_queue")+` q WHERE q.command_id=c.command_id)
			FROM `+pgschema.Table(database.Schema, "flow_commands")+` c
			JOIN `+pgschema.Table(database.Schema, "flow_command_event_waits")+` w USING(command_id)
			WHERE c.run_id=$1 AND c.command_key='gated'
			GROUP BY c.command_id,c.state,c.unsatisfied_waits`, exec.RunID).
			Scan(&state, &unsatisfied, &satisfied, &queued)
		if err == nil && state == "ready" && unsatisfied == 0 && satisfied == 2 && queued == 1 {
			break
		}
		if time.Now().After(deadline) {
			t.Fatalf("batched staged events did not release both waits: state=%q waits=%d satisfied=%d queued=%d err=%v",
				state, unsatisfied, satisfied, queued, err)
		}
		time.Sleep(5 * time.Millisecond)
	}
	if err := CancelRun(ctx, runtime, exec.RunID, "batched event delta test complete"); err != nil {
		t.Fatal(err)
	}
	assertReplayMatches(t, runtime, exec.RunID)
}

func TestEventAfterWaitDeadlineDoesNotResurrectFailedRun(t *testing.T) {
	t.Parallel()
	database := testpg.Open(t)
	ctx := context.Background()
	if err := Migrate(ctx, database.DB, WithSchema(database.Schema)); err != nil {
		t.Fatal(err)
	}
	late := DefineEvent[None]("gate.delta_late")
	parent := DefineCommand[None, None]("gate.delta_late_parent", 1)
	child := DefineCommand[None, None]("gate.delta_late_child", 1)
	runtime, err := New(database.DB, WithSchema(database.Schema), WithPollInterval(5*time.Millisecond), WithNotifications(false))
	if err != nil {
		t.Fatal(err)
	}
	if err := runtime.Register(Handle(parent, func(_ context.Context, work *Work[None]) (None, error) {
		Enqueue(work, "expiring", child, None{}).WaitFor(late, "late").Within(40 * time.Millisecond)
		return None{}, nil
	})); err != nil {
		t.Fatal(err)
	}
	cancel, runResult := startRuntime(t, runtime)
	defer stopRuntime(t, cancel, runResult)
	exec, err := parent.Enqueue(ctx, runtime, "late", None{})
	if err != nil {
		t.Fatal(err)
	}
	deadline := time.Now().Add(5 * time.Second)
	for {
		var state, runState string
		err := database.DB.Conn.QueryRow(ctx, `SELECT c.state,e.status
			FROM `+pgschema.Table(database.Schema, "flow_commands")+` c
			JOIN `+pgschema.Table(database.Schema, "flow_runs")+` e USING(run_id)
			WHERE c.run_id=$1 AND c.command_key='expiring'`, exec.RunID).Scan(&state, &runState)
		if err == nil && state == "expired" && runState == "failed" {
			break
		}
		if time.Now().After(deadline) {
			t.Fatalf("expiring command/failed run state=%q/%q err=%v", state, runState, err)
		}
		time.Sleep(5 * time.Millisecond)
	}
	if err := late.Deliver(ctx, runtime, exec.RunID, "late", None{}); !errors.Is(err, ErrTerminal) {
		t.Fatalf("late event into failed run error = %v, want ErrTerminal", err)
	}
	var state string
	var unsatisfied, queued int
	var satisfyingPosition *int64
	var waitDeadline time.Time
	var recordedAt *time.Time
	if err := database.DB.Conn.QueryRow(ctx, `SELECT c.state,c.unsatisfied_waits,w.satisfied_position,c.wait_deadline_at,
		(SELECT recorded_at FROM `+pgschema.Table(database.Schema, "flow_journal")+`
		 WHERE run_id=c.run_id AND event_class='application' AND event_name=$2 AND event_key='late'),
		(SELECT count(*) FROM `+pgschema.Table(database.Schema, "flow_command_queue")+` q WHERE q.command_id=c.command_id)
		FROM `+pgschema.Table(database.Schema, "flow_commands")+` c
		JOIN `+pgschema.Table(database.Schema, "flow_command_event_waits")+` w USING(command_id)
		WHERE c.run_id=$1 AND c.command_key='expiring'`, exec.RunID, late.Name()).
		Scan(&state, &unsatisfied, &satisfyingPosition, &waitDeadline, &recordedAt, &queued); err != nil {
		t.Fatal(err)
	}
	if state != "expired" || unsatisfied != 1 || satisfyingPosition != nil || queued != 0 || recordedAt != nil {
		t.Fatalf("late event resurrected/changed command: state=%s waits=%d position=%v queued=%d deadline=%s event=%v",
			state, unsatisfied, satisfyingPosition, queued, waitDeadline, recordedAt)
	}
	assertReplayMatches(t, runtime, exec.RunID)
}

func TestWaitExpiryReconciliationAcceptsEventAtExactDeadline(t *testing.T) {
	t.Parallel()
	database := testpg.Open(t)
	ctx := context.Background()
	if err := Migrate(ctx, database.DB, WithSchema(database.Schema)); err != nil {
		t.Fatal(err)
	}
	event := DefineEvent[string]("gate.exact_deadline")
	command := DefineCommand[None, None]("gate.exact_deadline_command", 1)
	runtime, err := New(database.DB, WithSchema(database.Schema), WithNotifications(false))
	if err != nil {
		t.Fatal(err)
	}
	exec, err := command.Enqueue(ctx, runtime, "exact-deadline", None{}, WaitFor(event, "ready"), Within(time.Minute))
	if err != nil {
		t.Fatal(err)
	}
	execRun := mustGetRun(t, runtime, exec.RunID)
	tx, err := database.DB.Conn.Begin(ctx)
	if err != nil {
		t.Fatal(err)
	}
	defer tx.Rollback(ctx)
	if err := event.Deliver(ctx, runtime.InTx(tx), exec.RunID, "ready", "accepted"); err != nil {
		t.Fatal(err)
	}
	var position int64
	var recordedAt time.Time
	if err := tx.QueryRow(ctx, `SELECT position,recorded_at FROM `+pgschema.Table(database.Schema, "flow_journal")+`
		WHERE run_id=$1 AND event_class='application' AND event_name=$2 AND event_key='ready'`, exec.RunID, event.Name()).
		Scan(&position, &recordedAt); err != nil {
		t.Fatal(err)
	}
	if _, err := tx.Exec(ctx, `UPDATE `+pgschema.Table(database.Schema, "flow_commands")+`
		SET state='pending',unsatisfied_waits=1,budget_started_at=NULL,next_attempt_at=NULL,
		    wait_deadline_at=$2,updated_at=$2,status_at=$2 WHERE command_id=$1`, execRun.RootCommandID, recordedAt); err != nil {
		t.Fatal(err)
	}
	if _, err := tx.Exec(ctx, `UPDATE `+pgschema.Table(database.Schema, "flow_command_event_waits")+`
		SET satisfied_position=NULL WHERE command_id=$1`, execRun.RootCommandID); err != nil {
		t.Fatal(err)
	}
	if _, err := tx.Exec(ctx, `DELETE FROM `+pgschema.Table(database.Schema, "flow_command_queue")+`
		WHERE command_id=$1`, execRun.RootCommandID); err != nil {
		t.Fatal(err)
	}
	if err := tx.Commit(ctx); err != nil {
		t.Fatal(err)
	}
	candidates, err := runtime.store.ProbeExpiredCommandWaits(ctx, 10)
	if err != nil {
		t.Fatal(err)
	}
	if len(candidates) != 1 || candidates[0].CommandID.String() != string(execRun.RootCommandID) {
		t.Fatalf("expired wait candidates=%+v", candidates)
	}
	changed, err := runtime.store.ExpireCommandWait(ctx, candidates[0])
	if err != nil {
		t.Fatal(err)
	}
	if !changed {
		t.Fatal("exact-deadline reconciliation made no change")
	}
	var state string
	var unsatisfied, queued int
	var satisfiedPosition int64
	if err := database.DB.Conn.QueryRow(ctx, `SELECT c.state,c.unsatisfied_waits,w.satisfied_position,
		(SELECT count(*) FROM `+pgschema.Table(database.Schema, "flow_command_queue")+` q WHERE q.command_id=c.command_id)
		FROM `+pgschema.Table(database.Schema, "flow_commands")+` c
		JOIN `+pgschema.Table(database.Schema, "flow_command_event_waits")+` w USING(command_id)
		WHERE c.command_id=$1`, execRun.RootCommandID).Scan(&state, &unsatisfied, &satisfiedPosition, &queued); err != nil {
		t.Fatal(err)
	}
	if state != "ready" || unsatisfied != 0 || queued != 1 || satisfiedPosition != position {
		t.Fatalf("exact-deadline projection state=%s waits=%d queued=%d position=%d want %d",
			state, unsatisfied, queued, satisfiedPosition, position)
	}
	if err := CancelRun(ctx, runtime, exec.RunID, "exact deadline test complete"); err != nil {
		t.Fatal(err)
	}
	assertReplayMatches(t, runtime, exec.RunID)
}

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
		payload, found, err := GetEventValue(work, event, "ready")
		if err != nil || !found {
			if err == nil {
				err = errors.New("required gated event is absent")
			}
			return None{}, err
		}
		received.Store(payload)
		calls.Add(1)
		return None{}, nil
	})); err != nil {
		t.Fatal(err)
	}

	exec, err := command.Enqueue(ctx, runtime, "direct", None{},
		WaitFor(event, "ready"), WaitFor(event, "ready"), Within(2*time.Second), Within(2*time.Second))
	if err != nil {
		t.Fatal(err)
	}
	execRun := mustGetRun(t, runtime, exec.RunID)
	repeated, err := command.Enqueue(ctx, runtime, "direct", None{}, WaitFor(event, "ready"), Within(2*time.Second))
	if err != nil || repeated.Created || repeated.RunID != exec.RunID {
		t.Fatalf("equivalent gated start = %+v, %v", repeated, err)
	}
	if _, err := command.Enqueue(ctx, runtime, "direct", None{}, WaitFor(event, "other"), Within(2*time.Second)); !errors.Is(err, ErrConflict) {
		t.Fatalf("different gated start = %v", err)
	}

	var state string
	var unsatisfied int
	if err := database.DB.Conn.QueryRow(ctx, `SELECT state,unsatisfied_waits FROM `+
		pgschema.Table(database.Schema, "flow_commands")+` WHERE command_id=$1`, execRun.RootCommandID).
		Scan(&state, &unsatisfied); err != nil {
		t.Fatal(err)
	}
	if state != "pending" || unsatisfied != 1 {
		t.Fatalf("gated root state=%s waits=%d", state, unsatisfied)
	}
	var queued int
	if err := database.DB.Conn.QueryRow(ctx, `SELECT count(*) FROM `+
		pgschema.Table(database.Schema, "flow_command_queue")+` WHERE command_id=$1`, execRun.RootCommandID).Scan(&queued); err != nil {
		t.Fatal(err)
	}
	if queued != 0 {
		t.Fatalf("pending root queue rows=%d", queued)
	}

	cancel, runResult := startRuntime(t, runtime)
	defer stopRuntime(t, cancel, runResult)
	if err := event.Deliver(ctx, runtime, exec.RunID, "not-ready", "ignored"); err != nil {
		t.Fatal(err)
	}
	time.Sleep(20 * time.Millisecond)
	if calls.Load() != 0 {
		t.Fatalf("non-matching event released root, calls=%d", calls.Load())
	}
	if err := event.Deliver(ctx, runtime, exec.RunID, "ready", "approved"); err != nil {
		t.Fatal(err)
	}
	waitForRunStatus(t, database.Schema, database.DB.Conn, exec.RunID, "succeeded", 5*time.Second)
	if calls.Load() != 1 {
		t.Fatalf("worker calls=%d", calls.Load())
	}
	if value, _ := received.Load().(string); value != "approved" {
		t.Fatalf("GetEventValue payload=%q", value)
	}
	trace, err := Trace(ctx, runtime, exec.RunID)
	if err != nil {
		t.Fatal(err)
	}
	if len(trace.Commands) != 1 || len(trace.Commands[0].Waits) != 1 || trace.Commands[0].Waits[0].SatisfiedPosition == nil {
		t.Fatalf("trace wait satisfaction=%+v", trace.Commands)
	}
	assertReplayMatches(t, runtime, exec.RunID)
}

func TestEventGatedCommandsRemainLiveUntilTerminal(t *testing.T) {
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
			node := Enqueue(work, "waiting", child, None{}).WaitFor(event, "ready")
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

	open, err := parent.Enqueue(ctx, runtime, "optional/open", false)
	if err != nil {
		t.Fatal(err)
	}
	time.Sleep(80 * time.Millisecond)
	run, err := GetRun(ctx, runtime, open.RunID)
	if err != nil {
		t.Fatal(err)
	}
	if run.Status != "running" {
		t.Fatalf("waiting command without Within status=%s, want running", run.Status)
	}
	if err := event.Deliver(ctx, runtime, open.RunID, "ready", None{}); err != nil {
		t.Fatal(err)
	}
	waitForRunStatus(t, database.Schema, database.DB.Conn, open.RunID, "succeeded", 5*time.Second)

	expiring, err := parent.Enqueue(ctx, runtime, "optional/expiring", true)
	if err != nil {
		t.Fatal(err)
	}
	waitForRunStatus(t, database.Schema, database.DB.Conn, expiring.RunID, "failed", 5*time.Second)
	trace, err := Trace(ctx, runtime, expiring.RunID)
	if err != nil {
		t.Fatal(err)
	}
	var optionalState CommandStatus
	for _, command := range trace.Commands {
		if command.Key == "waiting" {
			optionalState = command.State
		}
	}
	if len(trace.Commands) != 2 || optionalState != CommandStatusExpired {
		t.Fatalf("optional expiry trace=%+v", trace.Commands)
	}

	deadline, err := parent.Enqueue(ctx, runtime, "optional/deadline", false, WithRunDeadline(250*time.Millisecond))
	if err != nil {
		t.Fatal(err)
	}
	waitForRunStatus(t, database.Schema, database.DB.Conn, deadline.RunID, "expired", 5*time.Second)
	deadlineTrace, err := Trace(ctx, runtime, deadline.RunID)
	if err != nil {
		t.Fatal(err)
	}
	var deadlineWaiting TraceCommand
	for _, command := range deadlineTrace.Commands {
		if command.Key == "waiting" {
			deadlineWaiting = command
		}
	}
	if deadlineWaiting.State != CommandStatusCancelled || deadlineWaiting.Failure == nil || deadlineWaiting.Failure.Code != "run_expired" {
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
			Enqueue(work, "child", child, None{}).WaitFor(event, "ready").Within(time.Second)
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
	exec, err := parent.Enqueue(ctx, runtime, "same-decision", None{})
	if err != nil {
		t.Fatal(err)
	}
	execRun := mustGetRun(t, runtime, exec.RunID)
	waitForRunStatus(t, database.Schema, database.DB.Conn, exec.RunID, "succeeded", 5*time.Second)
	if childCalls.Load() != 1 {
		t.Fatalf("child calls=%d", childCalls.Load())
	}
	var waitStarted *time.Time
	var satisfiedPosition, childCreatedPosition int64
	if err := database.DB.Conn.QueryRow(ctx, `SELECT c.wait_started_at,w.satisfied_position,c.created_position
		FROM `+pgschema.Table(database.Schema, "flow_commands")+` c
		JOIN `+pgschema.Table(database.Schema, "flow_command_event_waits")+` w USING (command_id)
		WHERE c.run_id=$1 AND c.command_key='child'`, exec.RunID).
		Scan(&waitStarted, &satisfiedPosition, &childCreatedPosition); err != nil {
		t.Fatal(err)
	}
	var applicationPosition, parentTerminalPosition int64
	if err := database.DB.Conn.QueryRow(ctx, `SELECT
		(SELECT position FROM `+pgschema.Table(database.Schema, "flow_journal")+`
		 WHERE run_id=$1 AND event_class='application' AND event_name=$2 AND event_key='ready'),
		(SELECT terminal_position FROM `+pgschema.Table(database.Schema, "flow_commands")+`
			 WHERE command_id=$3)`, exec.RunID, event.Name(), execRun.RootCommandID).
		Scan(&applicationPosition, &parentTerminalPosition); err != nil {
		t.Fatal(err)
	}
	if waitStarted != nil || satisfiedPosition != applicationPosition ||
		childCreatedPosition != applicationPosition+1 || parentTerminalPosition != childCreatedPosition+1 {
		t.Fatalf("same-decision positions wait_started=%v satisfied=%d event=%d child=%d parent_terminal=%d",
			waitStarted, satisfiedPosition, applicationPosition, childCreatedPosition, parentTerminalPosition)
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
			Enqueue(work, "child", child, None{}).
				WaitFor(first, "first").
				WaitFor(second, "second").
				Within(time.Second)
			return None{}, nil
		}),
		Handle(child, func(_ context.Context, work *Work[None]) (None, error) {
			if _, found, err := GetEventValue(work, first, "first"); err != nil {
				return None{}, err
			} else if !found {
				return None{}, errors.New("required first event is absent")
			}
			if _, found, err := GetEventValue(work, second, "second"); err != nil {
				return None{}, err
			} else if !found {
				return None{}, errors.New("required second event is absent")
			}
			childCalls.Add(1)
			return None{}, nil
		}),
	); err != nil {
		t.Fatal(err)
	}
	cancel, runResult := startRuntime(t, runtime)
	defer stopRuntime(t, cancel, runResult)
	exec, err := parent.Enqueue(ctx, runtime, "multiple", None{})
	if err != nil {
		t.Fatal(err)
	}
	deadline := time.Now().Add(3 * time.Second)
	for {
		var state string
		var unsatisfied int
		err := database.DB.Conn.QueryRow(ctx, `SELECT state,unsatisfied_waits FROM `+
			pgschema.Table(database.Schema, "flow_commands")+` WHERE run_id=$1 AND command_key='child'`, exec.RunID).
			Scan(&state, &unsatisfied)
		if err == nil && state == "pending" && unsatisfied == 1 {
			break
		}
		if time.Now().After(deadline) {
			t.Fatalf("gated child did not become pending: state=%q waits=%d err=%v", state, unsatisfied, err)
		}
		time.Sleep(5 * time.Millisecond)
	}
	if err := wrong.Deliver(ctx, runtime, exec.RunID, "second", None{}); err != nil {
		t.Fatal(err)
	}
	time.Sleep(20 * time.Millisecond)
	if childCalls.Load() != 0 {
		t.Fatalf("wrong event name released child, calls=%d", childCalls.Load())
	}
	if err := second.Deliver(ctx, runtime, exec.RunID, "second", None{}); err != nil {
		t.Fatal(err)
	}
	waitForRunStatus(t, database.Schema, database.DB.Conn, exec.RunID, "succeeded", 5*time.Second)
	if childCalls.Load() != 1 {
		t.Fatalf("child calls=%d", childCalls.Load())
	}
	assertReplayMatches(t, runtime, exec.RunID)
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
			Enqueue(work, "producer", producer, None{})
			Enqueue(work, "join", join, None{}).WaitFor(event, "producer")
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
	exec, err := parent.Enqueue(ctx, runtime, "required-failure", None{})
	if err != nil {
		t.Fatal(err)
	}
	waitForRunStatus(t, database.Schema, database.DB.Conn, exec.RunID, "failed", 5*time.Second)
	trace, err := Trace(ctx, runtime, exec.RunID)
	if err != nil {
		t.Fatal(err)
	}
	states := make(map[string]TraceCommand, len(trace.Commands))
	for _, command := range trace.Commands {
		states[command.Key] = command
	}
	if states["producer"].State != CommandStatusFailed || states["join"].State != CommandStatusCancelled ||
		states["join"].Failure == nil || states["join"].Failure.Code != "run_failing" || joinCalls.Load() != 0 {
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
	exec, err := command.Enqueue(ctx, runtime, "expiry", None{},
		WaitFor(event, "missing"), Within(40*time.Millisecond), WithStartDelay(time.Hour))
	if err != nil {
		t.Fatal(err)
	}
	waitForRunStatus(t, database.Schema, database.DB.Conn, exec.RunID, "failed", 5*time.Second)
	if calls.Load() != 0 {
		t.Fatalf("expired delayed command ran %d times", calls.Load())
	}
	trace, err := Trace(ctx, runtime, exec.RunID)
	if err != nil {
		t.Fatal(err)
	}
	if len(trace.Commands) != 1 || trace.Commands[0].State != CommandStatusExpired || trace.Commands[0].Failure == nil || trace.Commands[0].Failure.Code != "wait_expired" {
		t.Fatalf("expired gate trace=%+v", trace.Commands)
	}
	if err := event.Deliver(ctx, runtime, exec.RunID, "missing", None{}); !errors.Is(err, ErrTerminal) {
		t.Fatalf("late event error=%v, want ErrTerminal", err)
	}
	assertReplayMatches(t, runtime, exec.RunID)
}
