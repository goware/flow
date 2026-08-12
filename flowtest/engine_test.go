package flowtest_test

import (
	"context"
	"encoding/json"
	"errors"
	"strings"
	"testing"
	"time"

	"github.com/goware/flow"
	"github.com/goware/flow/flowtest"
	"github.com/jackc/pgx/v5"
	"github.com/jackc/pgx/v5/pgconn"
)

func TestStagedEventsAreRecordedDeterministically(t *testing.T) {
	event := flow.DefineEvent[testFact]("flowtest.worker_event")
	command := flow.DefineCommand[testArgs, testResult]("flowtest.event_worker", 1)
	registration := flow.Handle(command, func(_ context.Context, work *flow.Work[testArgs]) (testResult, error) {
		if err := flow.Emit(work, event, "z", testFact{Value: "last"}); err != nil {
			return testResult{}, err
		}
		if err := flow.Emit(work, event, "a", testFact{Value: "first"}); err != nil {
			return testResult{}, err
		}
		_ = flow.Emit(work, event, "a", testFact{Value: "first"})
		return testResult{Value: "done"}, nil
	})

	decision, err := flowtest.RunWorker[testArgs, testResult](context.Background(), registration, testArgs{})
	if err != nil {
		t.Fatal(err)
	}
	if decision.Err != nil || len(decision.Events) != 2 || decision.Events[0].Key != "a" || decision.Events[1].Key != "z" {
		t.Fatalf("decision=%+v", decision)
	}
	var payload testFact
	if err := json.Unmarshal(decision.Events[0].Payload, &payload); err != nil || payload.Value != "first" {
		t.Fatalf("payload=%s err=%v", decision.Events[0].Payload, err)
	}
}

func TestStagedEventDefectsPoisonDecisions(t *testing.T) {
	event := flow.DefineEvent[testFact]("flowtest.event_conflict")
	command := flow.DefineCommand[testArgs, testResult]("flowtest.event_conflict_worker", 1)
	registration := flow.Handle(command, func(_ context.Context, work *flow.Work[testArgs]) (testResult, error) {
		_ = flow.Emit(work, event, "same", testFact{Value: "one"})
		_ = flow.Emit(work, event, "same", testFact{Value: "two"})
		return testResult{}, nil
	})
	decision, err := flowtest.RunWorker[testArgs, testResult](context.Background(), registration, testArgs{})
	if err != nil {
		t.Fatal(err)
	}
	if !errors.Is(decision.Err, flow.ErrConflict) {
		t.Fatalf("decision error=%v", decision.Err)
	}

	invalid := flow.Handle(command, func(_ context.Context, work *flow.Work[testArgs]) (testResult, error) {
		_ = flow.Emit(work, event, "", testFact{})
		return testResult{}, nil
	})
	invalidDecision, err := flowtest.RunWorker[testArgs, testResult](context.Background(), invalid, testArgs{})
	if err != nil || !errors.Is(invalidDecision.Err, flow.ErrInvalid) {
		t.Fatalf("invalid key decision=%+v err=%v", invalidDecision, err)
	}

	oversized := flow.Handle(command, func(_ context.Context, work *flow.Work[testArgs]) (testResult, error) {
		_ = flow.Emit(work, event, "large", testFact{Value: strings.Repeat("x", 64<<10)})
		return testResult{}, nil
	})
	oversizedDecision, err := flowtest.RunWorker[testArgs, testResult](context.Background(), oversized, testArgs{})
	if err != nil || !errors.Is(oversizedDecision.Err, flow.ErrPayloadTooLarge) {
		t.Fatalf("oversized decision=%+v err=%v", oversizedDecision, err)
	}
}

func TestRunWorkerReadsDeclaredEventInputs(t *testing.T) {
	event := flow.DefineEvent[testFact]("flowtest.worker_input")
	command := flow.DefineCommand[testArgs, testResult]("flowtest.input_worker", 1)
	registration := flow.Handle(command, func(_ context.Context, work *flow.Work[testArgs]) (testResult, error) {
		first, found, err := flow.GetEventValue(work, event, "input/1")
		if err != nil || !found {
			if err == nil {
				err = errors.New("required worker input is absent")
			}
			return testResult{}, err
		}
		second, found, err := flow.GetEventValue(work, event, "input/1")
		if err != nil || !found {
			if err == nil {
				err = errors.New("required worker input is absent")
			}
			return testResult{}, err
		}
		return testResult{Value: first.Value + "/" + second.Value}, nil
	})
	decision, err := flowtest.RunWorker[testArgs, testResult](context.Background(), registration, testArgs{},
		flowtest.WithEvent(event, "input/1", testFact{Value: "stable"}))
	if err != nil || decision.Err != nil || decision.Result.Value != "stable/stable" {
		t.Fatalf("declared event decision=%+v err=%v", decision, err)
	}
	missing, err := flowtest.RunWorker[testArgs, testResult](context.Background(), registration, testArgs{})
	if err != nil || missing.Err == nil {
		t.Fatalf("missing event decision=%+v err=%v", missing, err)
	}
	optionalRegistration := flow.Handle(command, func(_ context.Context, work *flow.Work[testArgs]) (testResult, error) {
		value, found, err := flow.GetEventValue(work, event, "input/1")
		if err != nil {
			return testResult{}, err
		}
		if !found {
			return testResult{Value: "absent"}, nil
		}
		return testResult{Value: value.Value}, nil
	})
	optional, err := flowtest.RunWorker[testArgs, testResult](context.Background(), optionalRegistration, testArgs{})
	if err != nil || optional.Err != nil || optional.Result.Value != "absent" {
		t.Fatalf("optional absent event decision=%+v err=%v", optional, err)
	}

	wrongType := flow.DefineEvent[int]("flowtest.worker_input")
	wrongRegistration := flow.Handle(command, func(_ context.Context, work *flow.Work[testArgs]) (testResult, error) {
		_, _, err := flow.GetEventValue(work, wrongType, "input/1")
		return testResult{}, err
	})
	wrong, err := flowtest.RunWorker[testArgs, testResult](context.Background(), wrongRegistration, testArgs{},
		flowtest.WithEvent(event, "input/1", testFact{Value: "stable"}))
	if err != nil || !errors.Is(wrong.Err, flow.ErrInvalidState) {
		t.Fatalf("wrong event type decision=%+v err=%v", wrong, err)
	}
}

func TestRunWorkerCarriesRunKey(t *testing.T) {
	command := flow.DefineCommand[testArgs, testResult]("flowtest.run_key", 1)
	registration := flow.Handle(command, func(_ context.Context, work *flow.Work[testArgs]) (testResult, error) {
		return testResult{Value: work.Info().RunKey}, nil
	})
	result, err := flowtest.RunWorker[testArgs, testResult](context.Background(), registration, testArgs{},
		flowtest.WithCommandInfo(flow.CommandInfo{RunID: "run-id", RunKey: "intent/42", CommandKey: "child"}))
	if err != nil || result.Err != nil || result.Result.Value != "intent/42" {
		t.Fatalf("RunWorker RunKey result = %#v, %v", result, err)
	}
}

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
	directEvent := flow.DefineEvent[testFact]("flowtest.direct_event")
	gate := flow.DefineEvent[testFact]("flowtest.child_gate")

	parentRegistration := flow.Handle(parent, func(_ context.Context, work *flow.Work[testArgs]) (testResult, error) {
		flow.Enqueue(work, "child/next", child, testArgs{Value: work.Args.Value}).
			Optional().Delay(time.Second).WaitFor(gate, "ready").Within(2 * time.Second)
		return testResult{Value: "ready/" + work.Args.Value}, nil
	}, flow.WithCommit(func(ctx context.Context, tx flow.Tx, commit flow.Commit[testArgs, testResult]) error {
		_, err := tx.Exec(ctx, "record", commit.Args.Value, commit.Result.Value)
		return err
	}))

	decision, err := flowtest.RunWorker[testArgs, testResult](context.Background(), parentRegistration,
		testArgs{Value: "parent"})
	if err != nil || decision.Err != nil || decision.Result.Value != "ready/parent" ||
		len(decision.Commands) != 1 || decision.Commands[0].Required || decision.Commands[0].StartAfter != time.Second ||
		decision.Commands[0].Within != 2*time.Second || len(decision.Commands[0].Waits) != 1 ||
		decision.Commands[0].Waits[0].Name != gate.Name() || decision.Commands[0].Waits[0].Key != "ready" {
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
		_ = flow.Emit(work, directEvent, "shared", testFact{Value: "same"})
		flow.Enqueue(work, "leaf", child, testArgs{Value: "leaf"}).WaitFor(directEvent, "shared")
		return testResult{Value: "root"}, nil
	})
	childRegistration := flow.Handle(child, func(_ context.Context, work *flow.Work[testArgs]) (testResult, error) {
		if _, found, err := flow.GetEventValue(work, directEvent, "shared"); err != nil {
			return testResult{}, err
		} else if !found {
			return testResult{}, errors.New("required direct event is absent")
		}
		_ = flow.Emit(work, directEvent, "shared", testFact{Value: "same"})
		return testResult{Value: work.Args.Value + "/done"}, nil
	})
	direct, err := flowtest.RunDirect[testArgs, testResult](context.Background(), rootRegistration,
		testArgs{Value: "ignored"}, 10, func(name string, version int) (flow.Registration, bool) {
			return childRegistration, name == child.Name() && version == child.Version()
		})
	if err != nil || direct.Result.Value != "root" || string(direct.Commands["leaf"]) != `{"value":"leaf/done"}` || len(direct.Events) != 1 {
		t.Fatalf("RunDirect() = %#v, %v", direct, err)
	}
	conflictingChild := flow.Handle(child, func(_ context.Context, work *flow.Work[testArgs]) (testResult, error) {
		_ = flow.Emit(work, directEvent, "shared", testFact{Value: "different"})
		return testResult{Value: work.Args.Value}, nil
	})
	if _, err := flowtest.RunDirect[testArgs, testResult](context.Background(), rootRegistration,
		testArgs{}, 10, func(string, int) (flow.Registration, bool) { return conflictingChild, true }); err == nil {
		t.Fatal("RunDirect accepted conflicting staged event identities")
	}
}

func TestRunDirectRejectsNegativeCommandCeilingBeforeApplicationCode(t *testing.T) {
	root := flow.DefineCommand[testArgs, testResult]("flowtest.negative_ceiling_root", 1)
	child := flow.DefineCommand[testArgs, testResult]("flowtest.negative_ceiling_child", 1)
	for _, withChild := range []bool{false, true} {
		calls := 0
		registration := flow.Handle(root, func(_ context.Context, work *flow.Work[testArgs]) (testResult, error) {
			calls++
			if withChild {
				flow.Enqueue(work, "child", child, testArgs{})
			}
			return testResult{}, nil
		})
		if _, err := flowtest.RunDirect[testArgs, testResult](context.Background(), registration, testArgs{}, -1,
			func(string, int) (flow.Registration, bool) { return registration, true }); err == nil {
			t.Fatalf("RunDirect(withChild=%v) accepted a negative ceiling", withChild)
		}
		if calls != 0 {
			t.Fatalf("RunDirect(withChild=%v) invoked application code %d times", withChild, calls)
		}
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
