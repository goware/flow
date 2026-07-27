package flow

import (
	"context"
	"encoding/json"
	"errors"
	"fmt"
	"slices"
	"sync/atomic"
	"testing"
	"time"

	"github.com/goware/flow/internal/fault"
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
		if failedTrace, traceErr := Trace(ctx, runtime, handle.ID); traceErr == nil {
			for _, entry := range failedTrace.History {
				if entry.EventClass == "plan_terminal" {
					t.Logf("plan terminal body: %s", entry.Body)
				}
			}
		}
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

func TestPlanImmediateSkipReconcilesFailureBranchInOneRevision(t *testing.T) {
	t.Parallel()
	database := testpg.Open(t)
	ctx := context.Background()
	if err := Migrate(ctx, database.DB, WithSchema(database.Schema)); err != nil {
		t.Fatal(err)
	}
	failing := DefineCommand[None, None]("fixedpoint.failing", 1, WithMaxAttempts(1))
	impossible := DefineCommand[None, None]("fixedpoint.impossible", 1)
	recoverCommand := DefineCommand[None, None]("fixedpoint.recover", 1)
	plan := DefinePlan[None]("fixedpoint.plan", 1, func(p *Plan, _ None) {
		Do(p, "failing", failing, None{})
		outcome, terminal := Outcome(p, "failing", failing)
		if !terminal || outcome.Status == StatusSucceeded {
			return
		}
		Do(p, "impossible", impossible, None{}).After("failing")
		if impossibleOutcome, settled := Outcome(p, "impossible", impossible); settled && impossibleOutcome.Status == StatusSkipped {
			Do(p, "recover", recoverCommand, None{}).AfterFailed("impossible")
		}
	})
	runtime, err := New(database.DB, WithSchema(database.Schema), WithPollInterval(5*time.Millisecond), WithPlanVerification(true))
	if err != nil {
		t.Fatal(err)
	}
	if err := runtime.Register(
		plan,
		Handle(failing, func(context.Context, *Work[None]) (None, error) { return None{}, Permanent(errors.New("fail")) }),
		Handle(impossible, func(context.Context, *Work[None]) (None, error) {
			t.Fatal("impossible worker ran")
			return None{}, nil
		}),
		Handle(recoverCommand, func(context.Context, *Work[None]) (None, error) { return None{}, nil }),
	); err != nil {
		t.Fatal(err)
	}
	cancelRun, runResult := startRuntime(t, runtime)
	defer stopRuntime(t, cancelRun, runResult)
	handle, err := plan.With(runtime).Execute(ctx, "fixedpoint/1", None{})
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
	if states["failing"] != "failed" || states["impossible"] != "skipped" || states["recover"] != "succeeded" {
		t.Fatalf("states = %#v", states)
	}
	var reconciled [][]string
	for _, entry := range trace.History {
		if entry.Kind != HistoryPlanReconciled {
			continue
		}
		var body struct {
			Declarations []struct {
				Key string `json:"key"`
			} `json:"declarations"`
		}
		if err := json.Unmarshal(entry.Body, &body); err != nil {
			t.Fatal(err)
		}
		keys := make([]string, len(body.Declarations))
		for index := range body.Declarations {
			keys[index] = body.Declarations[index].Key
		}
		reconciled = append(reconciled, keys)
	}
	if len(reconciled) != 3 || !slices.Equal(reconciled[1], []string{"impossible", "recover"}) {
		t.Fatalf("reconciled deltas = %#v", reconciled)
	}
	assertReplayMatches(t, runtime, handle.ID)
}

func TestPlanInitialScheduleBeyondDeadlineExpiresInFixedPoint(t *testing.T) {
	t.Parallel()
	database := testpg.Open(t)
	ctx := context.Background()
	if err := Migrate(ctx, database.DB, WithSchema(database.Schema)); err != nil {
		t.Fatal(err)
	}
	delayed := DefineCommand[None, None]("fixedpoint.delayed", 1)
	recoverCommand := DefineCommand[None, None]("fixedpoint.delay_recovery", 1)
	plan := DefinePlan[None]("fixedpoint.delay_plan", 1, func(plan *Plan, _ None) {
		Do(plan, "delayed", delayed, None{}).Delay(time.Hour)
		if outcome, terminal := Outcome(plan, "delayed", delayed); terminal && outcome.Status == StatusExpired {
			Do(plan, "recover", recoverCommand, None{}).AfterFailed("delayed")
		}
	})
	var delayedCalls, recoveryCalls atomic.Int32
	runtime, err := New(database.DB, WithSchema(database.Schema), WithPollInterval(5*time.Millisecond), WithPlanVerification(true))
	if err != nil {
		t.Fatal(err)
	}
	if err := runtime.Register(
		plan,
		Handle(delayed, func(context.Context, *Work[None]) (None, error) {
			delayedCalls.Add(1)
			return None{}, nil
		}),
		Handle(recoverCommand, func(context.Context, *Work[None]) (None, error) {
			recoveryCalls.Add(1)
			return None{}, nil
		}),
	); err != nil {
		t.Fatal(err)
	}
	handle, err := plan.With(runtime).Execute(ctx, "fixedpoint/delay", None{}, WithExecutionDeadline(500*time.Millisecond))
	if err != nil {
		t.Fatal(err)
	}
	cancelRun, runResult := startRuntime(t, runtime)
	defer stopRuntime(t, cancelRun, runResult)
	waitForExecutionStatus(t, database.Schema, database.DB.Conn, handle.ID, "failed", 5*time.Second)
	trace, err := Trace(ctx, runtime, handle.ID)
	if err != nil {
		t.Fatal(err)
	}
	states := make(map[string]string)
	for _, command := range trace.Commands {
		states[command.Key] = command.State
	}
	if states["delayed"] != "expired" || states["recover"] != "succeeded" {
		t.Fatalf("command states = %#v", states)
	}
	if delayedCalls.Load() != 0 || recoveryCalls.Load() != 1 {
		t.Fatalf("worker calls delayed=%d recovery=%d", delayedCalls.Load(), recoveryCalls.Load())
	}
	var queued int
	if err := database.DB.Conn.QueryRow(ctx, `SELECT count(*) FROM `+pgschema.Table(database.Schema, "flow_command_queue")+`
		WHERE execution_id=$1`, handle.ID).Scan(&queued); err != nil {
		t.Fatal(err)
	}
	if queued != 0 {
		t.Fatalf("expired schedule left %d queue rows", queued)
	}
	assertReplayMatches(t, runtime, handle.ID)
}

func TestPlanLazyFactsAndDeterminismFailure(t *testing.T) {
	t.Parallel()
	database := testpg.Open(t)
	ctx := context.Background()
	if err := Migrate(ctx, database.DB, WithSchema(database.Schema)); err != nil {
		t.Fatal(err)
	}
	type route struct {
		Name string `json:"name"`
	}
	routeSelected := DefineEvent[route]("lazy.route_selected", 1)
	unrelated := DefineEvent[route]("lazy.unrelated", 1)
	command := DefineCommand[route, None]("lazy.run", 1)
	plan := DefinePlan[None]("lazy.plan", 1, func(p *Plan, _ None) {
		selected, ok := Fact(p, routeSelected)
		if ok {
			Do(p, "run", command, selected)
		}
	})
	runtime, err := New(database.DB, WithSchema(database.Schema), WithPollInterval(5*time.Millisecond), WithPlanVerification(true))
	if err != nil {
		t.Fatal(err)
	}
	if err := runtime.Register(plan, Handle(command, func(context.Context, *Work[route]) (None, error) { return None{}, nil })); err != nil {
		t.Fatal(err)
	}
	cancelRun, runResult := startRuntime(t, runtime)
	defer stopRuntime(t, cancelRun, runResult)
	handle, err := plan.With(runtime).Execute(ctx, "lazy/1", None{})
	if err != nil {
		t.Fatal(err)
	}
	if err := Publish(ctx, runtime, handle.ID, unrelated, "unrelated/1", route{Name: "ignored"}); err != nil {
		t.Fatal(err)
	}
	if err := Publish(ctx, runtime, handle.ID, routeSelected, "route/1", route{Name: "chosen"}); err != nil {
		t.Fatal(err)
	}
	waitForExecutionStatus(t, database.Schema, database.DB.Conn, handle.ID, "succeeded", 5*time.Second)
	result, err := Trace(ctx, runtime, handle.ID)
	if err != nil || len(result.Commands) != 1 || string(result.Commands[0].Args) != `{"name":"chosen"}` {
		t.Fatalf("lazy trace = %#v, %v", result.Commands, err)
	}

	var flips atomic.Int32
	nondeterministic := DefinePlan[None]("lazy.nondeterministic", 1, func(p *Plan, _ None) {
		key := "a"
		if flips.Add(1)%2 == 0 {
			key = "b"
		}
		Do(p, key, command, route{Name: key})
	})
	badRuntime, err := New(database.DB, WithSchema(database.Schema), WithPollInterval(5*time.Millisecond), WithPlanVerification(true))
	if err != nil {
		t.Fatal(err)
	}
	if err := badRuntime.Register(nondeterministic, Handle(command, func(context.Context, *Work[route]) (None, error) { return None{}, nil })); err != nil {
		t.Fatal(err)
	}
	badCancel, badRun := startRuntime(t, badRuntime)
	defer stopRuntime(t, badCancel, badRun)
	bad, err := nondeterministic.With(badRuntime).Execute(ctx, "lazy/bad", None{})
	if err != nil {
		t.Fatal(err)
	}
	waitForExecutionStatus(t, database.Schema, database.DB.Conn, bad.ID, "failed", 5*time.Second)
	badTrace, err := Trace(ctx, badRuntime, bad.ID)
	if err != nil {
		t.Fatal(err)
	}
	if len(badTrace.Commands) != 0 {
		t.Fatalf("nondeterministic plan created commands: %#v", badTrace.Commands)
	}
}

func TestPlanReconcilerRollbackLeavesDirtyForTakeover(t *testing.T) {
	t.Parallel()
	database := testpg.Open(t)
	ctx := context.Background()
	if err := Migrate(ctx, database.DB, WithSchema(database.Schema)); err != nil {
		t.Fatal(err)
	}
	command := DefineCommand[None, None]("takeover.command", 1)
	plan := DefinePlan[None]("takeover.plan", 1, func(p *Plan, _ None) { Do(p, "work", command, None{}) })
	first, err := New(database.DB, WithSchema(database.Schema), WithPollInterval(5*time.Millisecond))
	if err != nil {
		t.Fatal(err)
	}
	if err := first.Register(plan, Handle(command, func(context.Context, *Work[None]) (None, error) { return None{}, nil })); err != nil {
		t.Fatal(err)
	}
	var injected atomic.Int32
	first.faults = fault.Func(func(_ context.Context, point fault.Point) error {
		if point == fault.PlanBeforeCommit {
			injected.Add(1)
			return fault.Injected(point)
		}
		return nil
	})
	cancelFirst, firstRun := startRuntime(t, first)
	handle, err := plan.With(first).Execute(ctx, "takeover/plan", None{})
	if err != nil {
		t.Fatal(err)
	}
	waitForCount(t, &injected, 1, 3*time.Second)
	var dirty bool
	var commands int
	if err := database.DB.Conn.QueryRow(ctx, `SELECT plan_dirty,command_count FROM `+pgschema.Table(database.Schema, "flow_executions")+`
		WHERE execution_id=$1`, handle.ID).Scan(&dirty, &commands); err != nil {
		t.Fatal(err)
	}
	if !dirty || commands != 0 {
		t.Fatalf("rolled back plan = dirty %t commands %d", dirty, commands)
	}
	stopRuntime(t, cancelFirst, firstRun)

	second, err := New(database.DB, WithSchema(database.Schema), WithPollInterval(5*time.Millisecond))
	if err != nil {
		t.Fatal(err)
	}
	if err := second.Register(plan, Handle(command, func(context.Context, *Work[None]) (None, error) { return None{}, nil })); err != nil {
		t.Fatal(err)
	}
	cancelSecond, secondRun := startRuntime(t, second)
	defer stopRuntime(t, cancelSecond, secondRun)
	waitForExecutionStatus(t, database.Schema, database.DB.Conn, handle.ID, "succeeded", 5*time.Second)
	history, err := History(ctx, second, handle.ID)
	if err != nil {
		t.Fatal(err)
	}
	reconciliations := 0
	for _, entry := range history {
		if entry.Kind == HistoryPlanReconciled {
			reconciliations++
		}
	}
	if reconciliations != 2 {
		t.Fatalf("reconciliations = %d, want initial declaration and terminal quiescence", reconciliations)
	}
}
