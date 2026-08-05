package flow

import (
	"errors"
	"testing"
	"time"

	"github.com/goware/flow/internal/failure"
)

type decisionArgs struct{ Value string }
type decisionResult struct{ Value string }

func TestDecisionBufferCoalescesAndPoisonsConflicts(t *testing.T) {
	command := DefineCommand[decisionArgs, decisionResult]("decision_child", 1)
	scope := &Work[None]{scope: &scopeState{}}

	Execute(scope, "child/1", command, decisionArgs{Value: "one"}).Optional().Delay(time.Second)
	Execute(scope, "child/1", command, decisionArgs{Value: "one"}).Optional().Delay(time.Second)
	if scope.scope.firstError != nil {
		t.Fatalf("equivalent duplicate poison = %v", scope.scope.firstError)
	}
	if got := len(scope.scope.decision.commands); got != 1 {
		t.Fatalf("commands = %d, want 1", got)
	}

	Execute(scope, "child/1", command, decisionArgs{Value: "different"})
	if !errors.Is(scope.scope.firstError, ErrConflict) {
		t.Fatalf("poison = %v", scope.scope.firstError)
	}
}

func TestDecisionBufferRejectsInvalidOptions(t *testing.T) {
	command := DefineCommand[decisionArgs, decisionResult]("decision_options", 1)
	scope := &Work[None]{scope: &scopeState{}}
	Execute(scope, "child", command, decisionArgs{}).Delay(0).Delay(time.Second)
	if !errors.Is(scope.scope.firstError, ErrInvalid) {
		t.Fatalf("Execute poison = %v", scope.scope.firstError)
	}
}

func TestDecisionBufferNormalizesEventGates(t *testing.T) {
	command := DefineCommand[decisionArgs, decisionResult]("decision_gated", 1)
	first := DefineEvent[None]("decision.first")
	second := DefineEvent[None]("decision.second")
	scope := &Work[None]{scope: &scopeState{}}

	Execute(scope, "child", command, decisionArgs{}).
		WaitFor(second, "b").
		WaitFor(first, "a").
		Within(time.Second)
	Execute(scope, "child", command, decisionArgs{}).
		WaitFor(first, "a").
		Within(time.Second)

	if scope.scope.firstError != nil {
		t.Fatalf("event gate poison = %v", scope.scope.firstError)
	}
	if err := validateDecisionCommands(scope.scope.decision); err != nil {
		t.Fatalf("validate event gates = %v", err)
	}
	staged := scope.scope.decision.commands["child"]
	if len(staged.waits) != 2 || staged.waits[0].name != first.Name() || staged.waits[1].name != second.Name() || staged.within != time.Second {
		t.Fatalf("staged event gate = %+v", staged)
	}
}

func TestDecisionBufferRejectsInvalidEventGates(t *testing.T) {
	command := DefineCommand[decisionArgs, decisionResult]("decision_invalid_gate", 1)
	event := DefineEvent[None]("decision.gate")

	conflict := &Work[None]{scope: &scopeState{}}
	Execute(conflict, "child", command, decisionArgs{}).WaitFor(event, "event").Within(time.Second).Within(2 * time.Second)
	if !errors.Is(conflict.scope.firstError, ErrInvalid) {
		t.Fatalf("conflicting Within poison = %v", conflict.scope.firstError)
	}

	missing := &Work[None]{scope: &scopeState{}}
	Execute(missing, "child", command, decisionArgs{}).Within(time.Second)
	if err := validateDecisionCommands(missing.scope.decision); !errors.Is(err, ErrInvalid) {
		t.Fatalf("Within without WaitFor = %v", err)
	}
}

func TestResultOfAndOutcomeOfEnforceSnapshot(t *testing.T) {
	command := DefineCommand[decisionArgs, decisionResult]("decision_result", 1)
	other := DefineCommand[decisionArgs, decisionResult]("decision_result", 2)
	encoded, err := command.def.Result.Encode(decisionResult{Value: "done"}, maxCommandResultBytes)
	if err != nil {
		t.Fatal(err)
	}
	source := ExecutionTrace{results: resultSourceState{
		values: map[string]resultSourceValue{
			"success": {name: command.Name(), version: command.Version(), status: StatusSucceeded, result: encoded.Bytes},
			"failure": {name: command.Name(), version: command.Version(), status: StatusFailed, failure: &CommandFailure{Code: "boom", Message: "failed"}},
		},
	}}

	result, err := ResultOf(source, "success", command)
	if err != nil || result.Value != "done" {
		t.Fatalf("ResultOf = %#v, %v", result, err)
	}
	outcome, err := OutcomeOf(source, "failure", command)
	if err != nil || outcome.Status != StatusFailed || outcome.Failure == nil || outcome.Failure.Code != "boom" {
		t.Fatalf("OutcomeOf = %#v, %v", outcome, err)
	}
	if _, err := ResultOf(source, "failure", command); !errors.Is(err, ErrInvalidState) || !failure.IsPermanent(err) {
		t.Fatalf("failed ResultOf = %v", err)
	}
	if _, err := OutcomeOf(source, "missing", command); !errors.Is(err, ErrNotFound) || !failure.IsPermanent(err) {
		t.Fatalf("missing OutcomeOf = %v", err)
	}
	if _, err := OutcomeOf(source, "success", other); !errors.Is(err, ErrConflict) || !failure.IsPermanent(err) {
		t.Fatalf("mismatched OutcomeOf = %v", err)
	}
}
