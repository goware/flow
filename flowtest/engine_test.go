package flowtest_test

import (
	"context"
	"errors"
	"testing"
	"time"

	"github.com/goware/flow"
	"github.com/goware/flow/flowtest"
	"github.com/jackc/pgx/v5"
	"github.com/jackc/pgx/v5/pgconn"
)

type testArgs struct {
	Value string `json:"value"`
}

type testResult struct {
	Value string `json:"value"`
}

type testFact struct {
	Value string `json:"value"`
}

func TestRunWorkerCommitAndDirectUseProductionDecisionRecorder(t *testing.T) {
	child := flow.DefineCommand[testArgs, testResult]("flowtest.child", 1)
	parent := flow.DefineCommand[testArgs, testResult]("flowtest.parent", 1)
	event := flow.DefineEvent[testFact]("flowtest.fact", 1)

	parentRegistration := flow.Handle(parent, func(_ context.Context, work *flow.Work[testArgs]) (testResult, error) {
		dependency, err := flow.ResultOf(work, "dependency", child)
		if err != nil {
			return testResult{}, err
		}
		if err := flow.Emit(work, event, "observed", testFact{Value: dependency.Value}); err != nil {
			return testResult{}, err
		}
		if err := flow.Spawn(work, "child/next", child, testArgs{Value: work.Args.Value},
			flow.Optional(), flow.StartAfter(time.Second)); err != nil {
			return testResult{}, err
		}
		return testResult{Value: dependency.Value + "/" + work.Args.Value}, nil
	}, flow.WithCommit(func(ctx context.Context, tx flow.Tx, commit flow.Commit[testArgs, testResult]) error {
		_, err := tx.Exec(ctx, "record", commit.Args.Value, commit.Result.Value)
		return err
	}))

	decision, err := flowtest.RunWorker[testArgs, testResult](context.Background(), parentRegistration,
		testArgs{Value: "parent"}, flowtest.WithDependencies(flowtest.Succeeded("dependency", child, testResult{Value: "ready"})))
	if err != nil || decision.Err != nil || decision.Result.Value != "ready/parent" || len(decision.Events) != 1 ||
		len(decision.Commands) != 1 || decision.Commands[0].Required || decision.Commands[0].StartAfter != time.Second {
		t.Fatalf("RunWorker() = %#v, %v", decision, err)
	}
	tx := &recordingTx{}
	if err := flowtest.RunCommit(context.Background(), parentRegistration, tx,
		testArgs{Value: "parent"}, testResult{Value: "ready/parent"}, flow.CommandInfo{CommandKey: "root"}); err != nil {
		t.Fatalf("RunCommit() error = %v", err)
	}
	if len(tx.args) != 2 || tx.args[0] != "parent" || tx.args[1] != "ready/parent" {
		t.Fatalf("commit transaction args = %#v", tx.args)
	}

	root := flow.DefineCommand[testArgs, testResult]("flowtest.root", 1)
	rootRegistration := flow.Handle(root, func(_ context.Context, work *flow.Work[testArgs]) (testResult, error) {
		if err := flow.Spawn(work, "leaf", child, testArgs{Value: "leaf"}); err != nil {
			return testResult{}, err
		}
		return testResult{Value: "root"}, nil
	})
	childRegistration := flow.Handle(child, func(_ context.Context, work *flow.Work[testArgs]) (testResult, error) {
		return testResult{Value: work.Args.Value + "/done"}, nil
	})
	direct, err := flowtest.RunDirect[testArgs, testResult](context.Background(), rootRegistration,
		testArgs{Value: "ignored"}, 10, func(name string, version int) (flow.Registration, bool) {
			return childRegistration, name == child.Name() && version == child.Version()
		})
	if err != nil || direct.Result.Value != "root" || string(direct.Commands["leaf"]) != `{"value":"leaf/done"}` {
		t.Fatalf("RunDirect() = %#v, %v", direct, err)
	}
}

func TestPlanSimulationAndDeterminism(t *testing.T) {
	analyze := flow.DefineCommand[testArgs, testResult]("flowtest.analyze", 1)
	finish := flow.DefineCommand[testArgs, testResult]("flowtest.finish", 1)
	release := flow.DefineEvent[testFact]("flowtest.release", 1)
	plan := flow.DefinePlan[testArgs]("flowtest.plan", 1, func(plan *flow.Plan, args testArgs) {
		flow.Do(plan, "analyze", analyze, args)
		if _, ok := flow.Fact(plan, release); !ok {
			return
		}
		if outcome, ok := flow.Outcome(plan, "analyze", analyze); ok && outcome.Status == flow.StatusSucceeded {
			flow.Do(plan, "finish", finish, args).After("analyze")
		}
	})
	firstWorld := flowtest.PlanWorld{KnownEvents: []flowtest.EventRef{flowtest.EventReference(release)}}
	secondWorld := flowtest.PlanWorld{
		Commands: []flowtest.PlanCommand{flowtest.SucceededCommand("analyze", analyze, testResult{Value: "ok"})},
		Events:   []flowtest.PlanEvent{flowtest.Fact(2, release, "release/1", testFact{Value: "go"})},
	}
	steps, err := flowtest.Simulate(plan, testArgs{Value: "input"}, firstWorld, secondWorld)
	if err != nil {
		t.Fatalf("Simulate() error = %v", err)
	}
	if len(steps) != 2 || len(steps[0].Declarations) != 1 || steps[0].WaitingReads != 1 ||
		len(steps[1].Declarations) != 2 || steps[1].Declarations[1].Key != "finish" {
		t.Fatalf("simulation = %#v", steps)
	}
	checked := flowtest.AssertPlanDeterministic(t, plan, testArgs{Value: "input"}, secondWorld)
	if len(checked.Declarations) != 2 {
		t.Fatalf("deterministic result = %#v", checked)
	}
}

func TestRunCoordinatorHandlesMixedOutcomes(t *testing.T) {
	task := flow.DefineCommand[testArgs, testResult]("flowtest.tool", 1)
	type state struct {
		Pending int `json:"pending"`
		Failed  int `json:"failed"`
	}
	coordinator := flow.DefineCoordinator[state]("flowtest.agent", 1,
		flow.OnStart(func(_ context.Context, coordination *flow.Coordination[state]) error {
			coordination.State.Pending = 1
			return flow.Spawn(coordination, "tool/1", task, testArgs{Value: "search"}, flow.Optional())
		}),
		flow.OnOutcome(task, func(_ context.Context, coordination *flow.Coordination[state], received flow.Received[flow.CommandOutcome[testResult]]) error {
			coordination.State.Pending--
			if received.Payload.Status != flow.StatusSucceeded {
				coordination.State.Failed++
			}
			return flow.SucceedExecution(coordination, "agent/result")
		}),
	)
	started, err := flowtest.RunCoordinator(context.Background(), coordinator, state{}, flowtest.Start())
	if err != nil || started.Err != nil || started.State.Pending != 1 || len(started.Commands) != 1 || started.Commands[0].Required {
		t.Fatalf("coordinator start = %#v, %v", started, err)
	}
	failed := flow.CommandOutcome[testResult]{Status: flow.StatusFailed,
		Failure: &flow.CommandFailure{Code: "tool_timeout", Message: "timed out"}}
	finished, err := flowtest.RunCoordinator(context.Background(), coordinator, started.State,
		flowtest.DeliverOutcome(5, task, "tool/1", time.Now(), failed))
	if err != nil || finished.Err != nil || finished.State.Pending != 0 || finished.State.Failed != 1 ||
		finished.Terminal != "succeeded" || finished.ResultRef != "agent/result" {
		t.Fatalf("coordinator outcome = %#v, %v", finished, err)
	}
}

type recordingTx struct{ args []any }

func (tx *recordingTx) Exec(_ context.Context, _ string, args ...any) (pgconn.CommandTag, error) {
	tx.args = append(tx.args, args...)
	return pgconn.NewCommandTag("UPDATE 1"), nil
}

func (*recordingTx) Query(context.Context, string, ...any) (pgx.Rows, error) {
	return nil, errors.New("unexpected query")
}

func (*recordingTx) QueryRow(context.Context, string, ...any) pgx.Row { return errorRow{} }

type errorRow struct{}

func (errorRow) Scan(...any) error { return errors.New("unexpected query row") }
