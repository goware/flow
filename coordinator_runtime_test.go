package flow

import (
	"context"
	"errors"
	"sync/atomic"
	"testing"
	"time"

	"github.com/goware/flow/internal/fault"
	"github.com/goware/flow/internal/pgschema"
	"github.com/goware/flow/internal/testpg"
)

type coordinatorTaskArgs struct {
	Key  string `json:"key"`
	Fail bool   `json:"fail"`
}

func TestCoordinatorLeaseTakeoverAcrossReplicas(t *testing.T) {
	t.Parallel()
	database := testpg.Open(t)
	ctx := context.Background()
	if err := Migrate(ctx, database.DB, WithSchema(database.Schema)); err != nil {
		t.Fatalf("Migrate: %v", err)
	}
	var calls atomic.Int32
	firstStarted := make(chan struct{})
	coordinator := DefineCoordinator[int]("coordinator.takeover", 1, OnStart(func(ctx context.Context, c *Coordination[int]) error {
		if calls.Add(1) == 1 {
			close(firstStarted)
			<-ctx.Done()
			return ctx.Err()
		}
		c.State++
		return SucceedExecution(c, "result/takeover")
	}))
	newReplica := func() *Runtime {
		r, err := New(database.DB, WithSchema(database.Schema), WithCoordinatorConcurrency(1), WithPollInterval(5*time.Millisecond),
			WithCommandLease(90*time.Millisecond), WithShutdownGrace(300*time.Millisecond))
		if err != nil {
			t.Fatalf("New: %v", err)
		}
		if err = r.Register(coordinator); err != nil {
			t.Fatalf("Register: %v", err)
		}
		return r
	}
	first, second := newReplica(), newReplica()
	first.faults = fault.Func(func(_ context.Context, point fault.Point) error {
		if point == fault.RenewBeforeResult {
			return fault.Injected(point)
		}
		return nil
	})
	handle, err := coordinator.With(first).Execute(ctx, "takeover", 0)
	if err != nil {
		t.Fatalf("Execute: %v", err)
	}
	cancelFirst, resultFirst := startRuntime(t, first)
	defer stopRuntime(t, cancelFirst, resultFirst)
	select {
	case <-firstStarted:
	case <-time.After(2 * time.Second):
		t.Fatal("first coordinator handler did not start")
	}
	cancelSecond, resultSecond := startRuntime(t, second)
	defer stopRuntime(t, cancelSecond, resultSecond)
	waitForExecutionStatus(t, database.Schema, database.DB.Conn, handle.ID, "succeeded", 5*time.Second)
	if calls.Load() < 2 {
		t.Fatalf("coordinator calls=%d", calls.Load())
	}
	var starts, conclusions int
	if err = database.DB.Conn.QueryRow(ctx, `SELECT count(*) FILTER (WHERE entry_kind='attempt_started'),
		count(*) FILTER (WHERE entry_kind='attempt_concluded') FROM `+pgschema.Table(database.Schema, "flow_journal")+`
		WHERE execution_id=$1 AND coordinator_id IS NOT NULL`, handle.ID).Scan(&starts, &conclusions); err != nil {
		t.Fatal(err)
	}
	if starts < 2 || starts != conclusions {
		t.Fatalf("coordinator attempts started=%d concluded=%d", starts, conclusions)
	}
}

func TestCoordinatorPermanentFailureFailsExecution(t *testing.T) {
	t.Parallel()
	database := testpg.Open(t)
	ctx := context.Background()
	if err := Migrate(ctx, database.DB, WithSchema(database.Schema)); err != nil {
		t.Fatalf("Migrate: %v", err)
	}
	coordinator := DefineCoordinator[int]("coordinator.permanent", 1, OnStart(func(context.Context, *Coordination[int]) error { return Permanent(errors.New("bad decision")) }))
	runtime, err := New(database.DB, WithSchema(database.Schema), WithPollInterval(5*time.Millisecond))
	if err != nil {
		t.Fatal(err)
	}
	if err = runtime.Register(coordinator); err != nil {
		t.Fatal(err)
	}
	handle, err := coordinator.With(runtime).Execute(ctx, "permanent", 0)
	if err != nil {
		t.Fatal(err)
	}
	cancel, result := startRuntime(t, runtime)
	defer stopRuntime(t, cancel, result)
	waitForExecutionStatus(t, database.Schema, database.DB.Conn, handle.ID, "failed", 5*time.Second)
	var coordinatorStatus, executionStatus string
	var terminalEvents int
	if err = database.DB.Conn.QueryRow(ctx, `SELECT c.status,e.status FROM `+pgschema.Table(database.Schema, "flow_coordinators")+` c
		JOIN `+pgschema.Table(database.Schema, "flow_executions")+` e USING(execution_id) WHERE c.execution_id=$1`, handle.ID).Scan(&coordinatorStatus, &executionStatus); err != nil {
		t.Fatal(err)
	}
	if err = database.DB.Conn.QueryRow(ctx, `SELECT count(*) FROM `+pgschema.Table(database.Schema, "flow_journal")+`
		WHERE execution_id=$1 AND event_class='coordinator_terminal'`, handle.ID).Scan(&terminalEvents); err != nil {
		t.Fatal(err)
	}
	if coordinatorStatus != "failed" || executionStatus != "failed" || terminalEvents != 1 {
		t.Fatalf("coordinator=%s execution=%s events=%d", coordinatorStatus, executionStatus, terminalEvents)
	}
}

type coordinatorTaskResult struct {
	Value string `json:"value"`
}
type coordinatorNotice struct {
	Value string `json:"value"`
}
type coordinatorTestState struct {
	Started  bool                     `json:"started"`
	Notice   string                   `json:"notice"`
	Outcomes map[string]CommandStatus `json:"outcomes"`
}

func TestCoordinatorHistoricalDeliveryRetryOutcomesAndCompletion(t *testing.T) {
	t.Parallel()
	database := testpg.Open(t)
	ctx := context.Background()
	if err := Migrate(ctx, database.DB, WithSchema(database.Schema)); err != nil {
		t.Fatalf("Migrate: %v", err)
	}
	task := DefineCommand[coordinatorTaskArgs, coordinatorTaskResult]("coordinator.test.task", 1, WithMaxAttempts(1))
	notice := DefineEvent[coordinatorNotice]("coordinator.test.notice", 1)
	var starts atomic.Int32
	coordinator := DefineCoordinator[coordinatorTestState]("coordinator.test", 1,
		OnStart(func(_ context.Context, c *Coordination[coordinatorTestState]) error {
			if starts.Add(1) == 1 {
				return errors.New("retry start")
			}
			c.State.Started = true
			c.State.Outcomes = map[string]CommandStatus{}
			if err := Spawn(c, "task/good", task, coordinatorTaskArgs{Key: "good"}, Optional()); err != nil {
				return err
			}
			return Spawn(c, "task/bad", task, coordinatorTaskArgs{Key: "bad", Fail: true}, Optional())
		}),
		On(notice, func(_ context.Context, c *Coordination[coordinatorTestState], received Received[coordinatorNotice]) error {
			c.State.Notice = received.Payload.Value
			return nil
		}),
		OnOutcome(task, func(_ context.Context, c *Coordination[coordinatorTestState], received Received[CommandOutcome[coordinatorTaskResult]]) error {
			c.State.Outcomes[received.Key] = received.Payload.Status
			if len(c.State.Outcomes) == 2 {
				return SucceedExecution(c, "result/coordinator")
			}
			return nil
		}),
	)
	runtime, err := New(database.DB, WithSchema(database.Schema), WithWorkerConcurrency(2), WithCoordinatorConcurrency(2),
		WithPollInterval(5*time.Millisecond), WithCommandLease(250*time.Millisecond), WithShutdownGrace(time.Second))
	if err != nil {
		t.Fatalf("New: %v", err)
	}
	if err = runtime.Register(coordinator, Handle(task, func(_ context.Context, w *Work[coordinatorTaskArgs]) (coordinatorTaskResult, error) {
		if w.Args.Fail {
			return coordinatorTaskResult{}, Permanent(errors.New("controlled failure"))
		}
		return coordinatorTaskResult{Value: w.Args.Key}, nil
	})); err != nil {
		t.Fatalf("Register: %v", err)
	}
	handle, err := coordinator.With(runtime).Execute(ctx, "coordinator-case", coordinatorTestState{})
	if err != nil {
		t.Fatalf("Execute: %v", err)
	}
	if err = Publish(ctx, runtime, handle.ID, notice, "early", coordinatorNotice{Value: "retained"}); err != nil {
		t.Fatalf("Publish: %v", err)
	}
	cancelRun, runResult := startRuntime(t, runtime)
	defer stopRuntime(t, cancelRun, runResult)
	waitForExecutionStatus(t, database.Schema, database.DB.Conn, handle.ID, "succeeded", 8*time.Second)
	if starts.Load() != 2 {
		t.Fatalf("start calls=%d", starts.Load())
	}
	var state []byte
	var revision, inbox int64
	var status string
	if err = database.DB.Conn.QueryRow(ctx, `SELECT state,state_revision,inbox_position,status FROM `+pgschema.Table(database.Schema, "flow_coordinators")+`
		WHERE execution_id=$1`, handle.ID).Scan(&state, &revision, &inbox, &status); err != nil {
		t.Fatalf("read coordinator: %v", err)
	}
	decoded, err := coordinator.def.State.Decode(state)
	if err != nil {
		t.Fatalf("decode state: %v", err)
	}
	final := decoded.(coordinatorTestState)
	if status != "completed" || revision != 4 || inbox == 0 || !final.Started || final.Notice != "retained" ||
		final.Outcomes["task/good"] != StatusSucceeded || final.Outcomes["task/bad"] != StatusFailed {
		t.Fatalf("coordinator status=%s revision=%d inbox=%d state=%+v", status, revision, inbox, final)
	}
	trace, err := Trace(ctx, runtime, handle.ID)
	if err != nil {
		t.Fatalf("Trace: %v", err)
	}
	if trace.Execution.Status != "succeeded" || len(trace.Commands) != 2 {
		t.Fatalf("trace=%+v", trace)
	}
	transitions := 0
	for _, entry := range trace.History {
		if entry.Kind == HistoryCoordinatorTransition {
			transitions++
		}
	}
	if transitions != 4 {
		t.Fatalf("coordinator transitions=%d", transitions)
	}
}
