package flow

import (
	"errors"
	"fmt"
	"testing"
	"time"

	"github.com/goware/flow/internal/failure"
)

type decisionArgs struct{ Value string }
type decisionResult struct{ Value string }

func TestDecisionBufferCoalescesAndPoisonsConflicts(t *testing.T) {
	command := DefineCommand[decisionArgs, decisionResult]("decision_child", 1)
	scope := &Work[None]{scope: &scopeState{}}

	Enqueue(scope, "child/1", command, decisionArgs{Value: "one"}).Delay(time.Second)
	Enqueue(scope, "child/1", command, decisionArgs{Value: "one"}).Delay(time.Second)
	if scope.scope.firstError != nil {
		t.Fatalf("equivalent duplicate poison = %v", scope.scope.firstError)
	}
	if got := len(scope.scope.decision.commands); got != 1 {
		t.Fatalf("commands = %d, want 1", got)
	}

	Enqueue(scope, "child/1", command, decisionArgs{Value: "different"})
	if !errors.Is(scope.scope.firstError, ErrConflict) {
		t.Fatalf("poison = %v", scope.scope.firstError)
	}
}

func TestDecisionBufferBoundsDistinctStagedEvents(t *testing.T) {
	event := DefineEvent[decisionArgs]("decision.event")
	scope := &Work[None]{scope: &scopeState{}}
	for index := range maxStagedApplicationEvents {
		if err := Emit(scope, event, fmt.Sprintf("event/%03d", index), decisionArgs{Value: "value"}); err != nil {
			t.Fatalf("Emit(%d) error = %v", index, err)
		}
	}
	if err := Emit(scope, event, "event/000", decisionArgs{Value: "value"}); err != nil {
		t.Fatalf("equivalent duplicate at limit error = %v", err)
	}
	if got := len(scope.scope.decision.events); got != maxStagedApplicationEvents {
		t.Fatalf("staged events = %d, want %d", got, maxStagedApplicationEvents)
	}
	if err := Emit(scope, event, "event/overflow", decisionArgs{Value: "value"}); !errors.Is(err, ErrInvalid) ||
		!errors.Is(scope.scope.firstError, ErrInvalid) {
		t.Fatalf("overflow error/poison = %v/%v, want ErrInvalid", err, scope.scope.firstError)
	}
}

func TestDecisionBufferUsesCanonicalRetryPolicyIdentity(t *testing.T) {
	equivalentA := DefineCommand[decisionArgs, decisionResult]("decision_retry", 1, WithRetry(Attempts(2)))
	equivalentB := DefineCommand[decisionArgs, decisionResult]("decision_retry", 1, WithRetry(Attempts(2)))
	different := DefineCommand[decisionArgs, decisionResult]("decision_retry", 1, WithRetry(Attempts(3)))

	scope := &Work[None]{scope: &scopeState{}}
	Enqueue(scope, "child", equivalentA, decisionArgs{})
	Enqueue(scope, "child", equivalentB, decisionArgs{})
	if scope.scope.firstError != nil || len(scope.scope.decision.commands) != 1 {
		t.Fatalf("equivalent canonical policies did not coalesce: %v", scope.scope.firstError)
	}
	Enqueue(scope, "child", different, decisionArgs{})
	if !errors.Is(scope.scope.firstError, ErrConflict) {
		t.Fatalf("different canonical policies error = %v, want ErrConflict", scope.scope.firstError)
	}
}

func TestDecisionBufferRejectsInvalidOptions(t *testing.T) {
	command := DefineCommand[decisionArgs, decisionResult]("decision_options", 1)
	scope := &Work[None]{scope: &scopeState{}}
	Enqueue(scope, "child", command, decisionArgs{}).Delay(0).Delay(time.Second)
	if !errors.Is(scope.scope.firstError, ErrInvalid) {
		t.Fatalf("Enqueue poison = %v", scope.scope.firstError)
	}
}

func TestDecisionBufferNormalizesEventGates(t *testing.T) {
	command := DefineCommand[decisionArgs, decisionResult]("decision_gated", 1)
	first := DefineEvent[None]("decision.first")
	second := DefineEvent[None]("decision.second")
	scope := &Work[None]{scope: &scopeState{}}

	Enqueue(scope, "child", command, decisionArgs{}).
		WaitFor(second, "b").
		WaitFor(first, "a").
		Within(time.Second)
	Enqueue(scope, "child", command, decisionArgs{}).
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
	Enqueue(conflict, "child", command, decisionArgs{}).WaitFor(event, "event").Within(time.Second).Within(2 * time.Second)
	if !errors.Is(conflict.scope.firstError, ErrInvalid) {
		t.Fatalf("conflicting Within poison = %v", conflict.scope.firstError)
	}

	missing := &Work[None]{scope: &scopeState{}}
	Enqueue(missing, "child", command, decisionArgs{}).Within(time.Second)
	if err := validateDecisionCommands(missing.scope.decision); !errors.Is(err, ErrInvalid) {
		t.Fatalf("Within without WaitFor = %v", err)
	}

	empty := &Work[None]{scope: &scopeState{}}
	Enqueue(empty, "child", command, decisionArgs{}).WaitFor(event, "")
	if !errors.Is(empty.scope.firstError, ErrInvalid) {
		t.Fatalf("empty event key poison = %v", empty.scope.firstError)
	}

	delayConflict := &Work[None]{scope: &scopeState{}}
	Enqueue(delayConflict, "child", command, decisionArgs{}).Delay(time.Second)
	Enqueue(delayConflict, "child", command, decisionArgs{}).Delay(2 * time.Second)
	if !errors.Is(delayConflict.scope.firstError, ErrInvalid) {
		t.Fatalf("repeated declaration delay poison = %v", delayConflict.scope.firstError)
	}
}

func TestDecisionBufferEnforcesEventWaitLimit(t *testing.T) {
	command := DefineCommand[decisionArgs, decisionResult]("decision_wait_limit", 1)
	event := DefineEvent[None]("decision.wait_limit")

	accepted := &Work[None]{scope: &scopeState{}}
	acceptedNode := Enqueue(accepted, "child", command, decisionArgs{})
	for index := range maxCommandEventWaits {
		acceptedNode.WaitFor(event, fmt.Sprintf("event/%03d", index))
	}
	if err := validateDecisionCommands(accepted.scope.decision); err != nil {
		t.Fatalf("256 event waits = %v", err)
	}

	rejected := &Work[None]{scope: &scopeState{}}
	rejectedNode := Enqueue(rejected, "child", command, decisionArgs{})
	for index := range maxCommandEventWaits + 1 {
		rejectedNode.WaitFor(event, fmt.Sprintf("event/%03d", index))
	}
	if err := rejected.scope.firstError; !errors.Is(err, ErrInvalid) {
		t.Fatalf("257 event waits = %v, want ErrInvalid", err)
	}
}

func TestResultOfEnforcesTraceSnapshot(t *testing.T) {
	command := DefineCommand[decisionArgs, decisionResult]("decision_result", 1)
	other := DefineCommand[decisionArgs, decisionResult]("decision_result", 2)
	encoded, err := command.def.Result.Encode(decisionResult{Value: "done"}, maxCommandResultBytes)
	if err != nil {
		t.Fatal(err)
	}
	source := RunTrace{Commands: []TraceCommand{
		{Key: "success", Name: command.Name(), Version: command.Version(), Status: CommandStatusSucceeded, Result: encoded.Bytes},
		{Key: "failure", Name: command.Name(), Version: command.Version(), Status: CommandStatusFailed, Failure: &Failure{Code: "boom", Message: "failed"}},
	}}

	result, err := ResultOf(source, "success", command)
	if err != nil || result.Value != "done" {
		t.Fatalf("ResultOf = %#v, %v", result, err)
	}
	if _, err := ResultOf(source, "failure", command); !errors.Is(err, ErrInvalidState) || !failure.IsPermanent(err) {
		t.Fatalf("failed ResultOf = %v", err)
	}
	if _, err := ResultOf(source, "missing", command); !errors.Is(err, ErrNotFound) || !failure.IsPermanent(err) {
		t.Fatalf("missing ResultOf = %v", err)
	}
	if _, err := ResultOf(source, "success", other); !errors.Is(err, ErrConflict) || !failure.IsPermanent(err) {
		t.Fatalf("mismatched ResultOf = %v", err)
	}
}
