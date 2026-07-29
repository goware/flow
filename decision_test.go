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

func TestResultOfAndOutcomeOfEnforceSnapshot(t *testing.T) {
	command := DefineCommand[decisionArgs, decisionResult]("decision_result", 1)
	other := DefineCommand[decisionArgs, decisionResult]("decision_result", 2)
	encoded, err := command.def.Result.Encode(decisionResult{Value: "done"}, maxCommandResultBytes)
	if err != nil {
		t.Fatal(err)
	}
	source := &Work[None]{scope: &scopeState{results: resultSourceState{
		restricted: true,
		values: map[string]resultSourceValue{
			"success": {name: command.Name(), version: command.Version(), status: StatusSucceeded, result: encoded.Bytes},
			"failure": {name: command.Name(), version: command.Version(), status: StatusFailed, failure: &CommandFailure{Code: "boom", Message: "failed"}},
		},
	}}}

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
