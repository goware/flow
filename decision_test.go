package flow

import (
	"errors"
	"testing"
	"time"

	"github.com/goware/flow/internal/failure"
)

type decisionArgs struct{ Value string }
type decisionResult struct{ Value string }
type decisionEvent struct{ Value string }

func TestDecisionBufferCoalescesAndPoisonsConflicts(t *testing.T) {
	command := DefineCommand[decisionArgs, decisionResult]("decision_child", 1)
	event := DefineEvent[decisionEvent]("decision_fact", 1)
	scope := &Work[None]{scope: &scopeState{}}

	if err := Emit(scope, event, "fact/1", decisionEvent{Value: "one"}); err != nil {
		t.Fatal(err)
	}
	if err := Emit(scope, event, "fact/1", decisionEvent{Value: "one"}); err != nil {
		t.Fatal(err)
	}
	if got := len(scope.scope.decision.events); got != 1 {
		t.Fatalf("events = %d, want 1", got)
	}
	if err := Spawn(scope, "child/1", command, decisionArgs{Value: "one"}, Optional(), StartAfter(time.Second)); err != nil {
		t.Fatal(err)
	}
	if err := Spawn(scope, "child/1", command, decisionArgs{Value: "one"}, Optional(), StartAfter(time.Second)); err != nil {
		t.Fatal(err)
	}
	if got := len(scope.scope.decision.commands); got != 1 {
		t.Fatalf("commands = %d, want 1", got)
	}

	err := Spawn(scope, "child/1", command, decisionArgs{Value: "different"})
	if !errors.Is(err, ErrConflict) || !errors.Is(scope.scope.firstError, ErrConflict) {
		t.Fatalf("conflict = %v, poison = %v", err, scope.scope.firstError)
	}
	if err := Emit(scope, event, "fact/2", decisionEvent{}); !errors.Is(err, ErrConflict) {
		t.Fatalf("operation after poison = %v", err)
	}
}

func TestDecisionBufferRejectsInvalidOptions(t *testing.T) {
	command := DefineCommand[decisionArgs, decisionResult]("decision_options", 1)
	scope := &Work[None]{scope: &scopeState{}}
	err := Spawn(scope, "child", command, decisionArgs{}, StartAfter(0), StartAfter(time.Second))
	if !errors.Is(err, ErrInvalid) || len(scope.scope.decision.commands) != 0 {
		t.Fatalf("Spawn = %v, commands = %d", err, len(scope.scope.decision.commands))
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
