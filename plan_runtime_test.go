package flow

import (
	"context"
	"errors"
	"fmt"
	"sync/atomic"
	"testing"
	"time"

	"github.com/goware/flow/internal/pgschema"
	"github.com/goware/flow/internal/testpg"
)

func TestPlanDynamicFanOutJoinEndToEnd(t *testing.T) {
	t.Parallel()
	database := testpg.Open(t)
	ctx := context.Background()
	if err := Migrate(ctx, database.DB, WithSchema(database.Schema)); err != nil {
		t.Fatal(err)
	}
	type reportArgs struct{ Parts int }
	type prepareArgs struct{ Parts int }
	type prepareResult struct{ Count int }
	type analysisArgs struct{ Part int }
	type analysisResult struct{ Score int }
	type generateArgs struct{ Keys []string }
	type generateResult struct{ Total int }

	analyze := DefineCommand[analysisArgs, analysisResult]("plan.analyze", 1)
	prepare := DefineCommand[prepareArgs, prepareResult]("plan.prepare", 1)
	generate := DefineCommand[generateArgs, generateResult]("plan.generate", 1)
	plan := DefinePlan[reportArgs]("plan.report", 1, func(p *Plan, args reportArgs) {
		Do(p, "prepare", prepare, prepareArgs{Parts: args.Parts})
		children, closed := Children(p, "prepare")
		if !closed {
			return
		}
		Do(p, "generate", generate, generateArgs{Keys: children}).After(children...)
	})
	var prepareCalls, analysisCalls, generateCalls atomic.Int32
	runtime, err := New(database.DB, WithSchema(database.Schema), WithPollInterval(5*time.Millisecond), WithWorkerConcurrency(8))
	if err != nil {
		t.Fatal(err)
	}
	if err := runtime.Register(
		plan,
		Handle(prepare, func(_ context.Context, work *Work[prepareArgs]) (prepareResult, error) {
			prepareCalls.Add(1)
			for part := 0; part < work.Args.Parts; part++ {
				key := fmt.Sprintf("analysis/%d", part)
				if err := Spawn(work, key, analyze, analysisArgs{Part: part}); err != nil {
					return prepareResult{}, err
				}
			}
			return prepareResult{Count: work.Args.Parts}, nil
		}),
		Handle(analyze, func(_ context.Context, work *Work[analysisArgs]) (analysisResult, error) {
			analysisCalls.Add(1)
			return analysisResult{Score: work.Args.Part + 1}, nil
		}),
		Handle(generate, func(_ context.Context, work *Work[generateArgs]) (generateResult, error) {
			generateCalls.Add(1)
			total := 0
			for _, key := range work.Args.Keys {
				result, err := ResultOf(work, key, analyze)
				if err != nil {
					return generateResult{}, err
				}
				total += result.Score
			}
			return generateResult{Total: total}, nil
		}),
	); err != nil {
		t.Fatal(err)
	}
	cancelRun, runResult := startRuntime(t, runtime)
	defer stopRuntime(t, cancelRun, runResult)
	handle, err := plan.With(runtime).Execute(ctx, "report/1", reportArgs{Parts: 3})
	if err != nil {
		t.Fatal(err)
	}
	deadline := time.Now().Add(3 * time.Second)
	status := ""
	for time.Now().Before(deadline) {
		_ = database.DB.Conn.QueryRow(ctx, `SELECT status FROM `+pgschema.Table(database.Schema, "flow_executions")+` WHERE execution_id=$1`, handle.ID).Scan(&status)
		if status == "succeeded" {
			break
		}
		time.Sleep(10 * time.Millisecond)
	}
	if status != "succeeded" {
		rows, _ := database.DB.Conn.Query(ctx, `SELECT command_key,state,unsatisfied_groups,unsatisfied_waits FROM `+pgschema.Table(database.Schema, "flow_commands")+` WHERE execution_id=$1 ORDER BY command_key`, handle.ID)
		for rows.Next() {
			var key, state string
			var groups, waits int
			_ = rows.Scan(&key, &state, &groups, &waits)
			t.Logf("command %s state=%s groups=%d waits=%d", key, state, groups, waits)
		}
		rows.Close()
		var dirty, quiescent bool
		var revision int64
		var count, open int
		_ = database.DB.Conn.QueryRow(ctx, `SELECT plan_dirty,plan_quiescent,plan_revision,command_count,open_commands FROM `+pgschema.Table(database.Schema, "flow_executions")+` WHERE execution_id=$1`, handle.ID).Scan(&dirty, &quiescent, &revision, &count, &open)
		t.Fatalf("execution status=%s dirty=%t quiescent=%t revision=%d count=%d open=%d", status, dirty, quiescent, revision, count, open)
	}
	trace, err := Trace(ctx, runtime, handle.ID)
	if err != nil {
		t.Fatal(err)
	}
	if trace.Execution.CommandCount != 5 || len(trace.Commands) != 5 || trace.Execution.OpenCommands != 0 {
		t.Fatalf("trace topology: count=%d commands=%d open=%d", trace.Execution.CommandCount, len(trace.Commands), trace.Execution.OpenCommands)
	}
	if prepareCalls.Load() != 1 || analysisCalls.Load() != 3 || generateCalls.Load() != 1 {
		t.Fatalf("calls: prepare=%d analysis=%d generate=%d", prepareCalls.Load(), analysisCalls.Load(), generateCalls.Load())
	}
	result, err := ResultOf(trace, "generate", generate)
	if err != nil || result.Total != 6 {
		t.Fatalf("generate result = %#v, %v", result, err)
	}
	var reconciliations int
	for _, entry := range trace.History {
		if entry.Kind == HistoryPlanReconciled {
			reconciliations++
		}
	}
	if reconciliations < 3 {
		t.Fatalf("plan reconciliations = %d, want at least 3", reconciliations)
	}
}

func TestPlanAwaitPublishBeforeDeclareAndWithinExpiry(t *testing.T) {
	t.Parallel()
	database := testpg.Open(t)
	ctx := context.Background()
	if err := Migrate(ctx, database.DB, WithSchema(database.Schema)); err != nil {
		t.Fatal(err)
	}
	type delivered struct{ Ref string }
	type confirmResult struct{ Ref string }
	fact := DefineEvent[delivered]("plan.bridge_delivered", 1)
	confirm := DefineCommand[None, confirmResult]("plan.confirm_bridge", 1)
	plan := DefinePlan[None]("plan.await_bridge", 1, func(p *Plan, _ None) {
		Do(p, "confirm", confirm, None{}).Await(fact).Within(100 * time.Millisecond)
	})
	var calls atomic.Int32
	runtime, err := New(database.DB, WithSchema(database.Schema), WithPollInterval(5*time.Millisecond))
	if err != nil {
		t.Fatal(err)
	}
	if err := runtime.Register(plan, Handle(confirm, func(context.Context, *Work[None]) (confirmResult, error) {
		calls.Add(1)
		return confirmResult{Ref: "delivered"}, nil
	})); err != nil {
		t.Fatal(err)
	}
	cancelRun, runResult := startRuntime(t, runtime)
	defer stopRuntime(t, cancelRun, runResult)

	early, err := plan.With(runtime).Execute(ctx, "await/early", None{})
	if err != nil {
		t.Fatal(err)
	}
	if err := Publish(ctx, runtime, early.ID, fact, "delivery/early", delivered{Ref: "early"}); err != nil {
		t.Fatal(err)
	}
	waitForExecutionStatus(t, database.Schema, database.DB.Conn, early.ID, "succeeded", 5*time.Second)

	expired, err := plan.With(runtime).Execute(ctx, "await/expired", None{})
	if err != nil {
		t.Fatal(err)
	}
	waitForExecutionStatus(t, database.Schema, database.DB.Conn, expired.ID, "failed", 5*time.Second)
	if calls.Load() != 1 {
		t.Fatalf("confirm calls = %d, want 1", calls.Load())
	}
	expiredTrace, err := Trace(ctx, runtime, expired.ID)
	if err != nil {
		t.Fatal(err)
	}
	if len(expiredTrace.Commands) != 1 || expiredTrace.Commands[0].State != "expired" || expiredTrace.Commands[0].Within != 100*time.Millisecond {
		t.Fatalf("expired trace = %#v", expiredTrace.Commands)
	}
}

func TestPlanFailureBranchAndWorkerOutcome(t *testing.T) {
	t.Parallel()
	database := testpg.Open(t)
	ctx := context.Background()
	if err := Migrate(ctx, database.DB, WithSchema(database.Schema)); err != nil {
		t.Fatal(err)
	}
	type value struct{ Value string }
	failing := DefineCommand[None, value]("plan.failing", 1, WithMaxAttempts(1))
	shouldSkip := DefineCommand[None, None]("plan.should_skip", 1)
	compensate := DefineCommand[None, value]("plan.compensate", 1)
	plan := DefinePlan[None]("plan.failure_branch", 1, func(p *Plan, _ None) {
		Do(p, "failing", failing, None{})
		Do(p, "should-skip", shouldSkip, None{}).After("failing")
		Do(p, "compensate", compensate, None{}).AfterFailed("failing")
	})
	var skippedCalls atomic.Int32
	runtime, err := New(database.DB, WithSchema(database.Schema), WithPollInterval(5*time.Millisecond))
	if err != nil {
		t.Fatal(err)
	}
	if err := runtime.Register(
		plan,
		Handle(failing, func(context.Context, *Work[None]) (value, error) {
			return value{}, Permanent(errors.New("permanent failure"))
		}),
		Handle(shouldSkip, func(context.Context, *Work[None]) (None, error) {
			skippedCalls.Add(1)
			return None{}, nil
		}),
		Handle(compensate, func(_ context.Context, work *Work[None]) (value, error) {
			outcome, err := OutcomeOf(work, "failing", failing)
			if err != nil {
				return value{}, err
			}
			return value{Value: string(outcome.Status)}, nil
		}),
	); err != nil {
		t.Fatal(err)
	}
	cancelRun, runResult := startRuntime(t, runtime)
	defer stopRuntime(t, cancelRun, runResult)
	handle, err := plan.With(runtime).Execute(ctx, "failure/1", None{})
	if err != nil {
		t.Fatal(err)
	}
	waitForExecutionStatus(t, database.Schema, database.DB.Conn, handle.ID, "failed", 5*time.Second)
	trace, err := Trace(ctx, runtime, handle.ID)
	if err != nil {
		t.Fatal(err)
	}
	states := make(map[string]string)
	for _, command := range trace.Commands {
		states[command.Key] = command.State
	}
	if states["failing"] != "failed" || states["should-skip"] != "skipped" || states["compensate"] != "succeeded" {
		t.Fatalf("command states = %#v", states)
	}
	if skippedCalls.Load() != 0 {
		t.Fatalf("skipped worker calls = %d", skippedCalls.Load())
	}
	result, err := ResultOf(trace, "compensate", compensate)
	if err != nil || result.Value != "failed" {
		t.Fatalf("compensation result = %#v, %v", result, err)
	}
}

func TestPlanDefectFailsDurablyWithoutRunningWorker(t *testing.T) {
	t.Parallel()
	database := testpg.Open(t)
	ctx := context.Background()
	if err := Migrate(ctx, database.DB, WithSchema(database.Schema)); err != nil {
		t.Fatal(err)
	}
	command := DefineCommand[None, None]("plan.defect_command", 1)
	plan := DefinePlan[None]("plan.defect", 1, func(p *Plan, _ None) {
		Do(p, "never", command, None{}).After("missing")
	})
	var calls atomic.Int32
	runtime, err := New(database.DB, WithSchema(database.Schema), WithPollInterval(5*time.Millisecond))
	if err != nil {
		t.Fatal(err)
	}
	if err := runtime.Register(plan, Handle(command, func(context.Context, *Work[None]) (None, error) {
		calls.Add(1)
		return None{}, nil
	})); err != nil {
		t.Fatal(err)
	}
	cancelRun, runResult := startRuntime(t, runtime)
	defer stopRuntime(t, cancelRun, runResult)
	handle, err := plan.With(runtime).Execute(ctx, "defect/1", None{})
	if err != nil {
		t.Fatal(err)
	}
	waitForExecutionStatus(t, database.Schema, database.DB.Conn, handle.ID, "failed", 5*time.Second)
	if calls.Load() != 0 {
		t.Fatalf("worker calls = %d", calls.Load())
	}
	history, err := History(ctx, runtime, handle.ID)
	if err != nil {
		t.Fatal(err)
	}
	found := false
	for _, entry := range history {
		if entry.EventClass == "plan_terminal" && entry.EventName == "flow.plan_failed" {
			found = true
		}
	}
	if !found {
		t.Fatal("PlanFailed event missing")
	}
}
