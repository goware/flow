package flow

import (
	"errors"
	"testing"
	"time"

	"github.com/google/uuid"
	"github.com/goware/flow/internal/store"
)

func TestPlanRecorderValidatesTopologyWithoutDatabase(t *testing.T) {
	command := DefineCommand[None, None]("unit.plan.command", 1)
	snapshot := store.PlanSnapshot{ExecutionID: uuid.New(), Status: "running", MaxCommands: 100, JournalThrough: 1}

	forward := newPlan(snapshot)
	Do(forward, "second", command, None{}).After("first")
	Do(forward, "first", command, None{})
	reconciliation, err := buildPlanReconciliation(snapshot, forward)
	if err != nil || len(reconciliation.Commands) != 2 {
		t.Fatalf("forward reference commands=%d error=%v", len(reconciliation.Commands), err)
	}

	missing := newPlan(snapshot)
	Do(missing, "work", command, None{}).After("typo")
	if _, err := buildPlanReconciliation(snapshot, missing); !errors.Is(err, ErrInvalid) {
		t.Fatalf("missing dependency error=%v", err)
	}

	cycle := newPlan(snapshot)
	Do(cycle, "a", command, None{}).After("b")
	Do(cycle, "b", command, None{}).After("a")
	if _, err := buildPlanReconciliation(snapshot, cycle); !errors.Is(err, ErrInvalid) {
		t.Fatalf("cycle error=%v", err)
	}

	invalidWithin := newPlan(snapshot)
	Do(invalidWithin, "wait", command, None{}).Within(time.Second)
	if _, err := buildPlanReconciliation(snapshot, invalidWithin); !errors.Is(err, ErrInvalid) {
		t.Fatalf("Within without Await error=%v", err)
	}
}

func TestPlanRecorderReadAvailabilityAndDeterministicFingerprint(t *testing.T) {
	type result struct {
		Value string `json:"value"`
	}
	command := DefineCommand[None, result]("unit.plan.result", 1)
	encoded, err := encodeDefinitionValue(command.def.Result, result{Value: "ready"}, maxCommandResultBytes, "result")
	if err != nil {
		t.Fatal(err)
	}
	succeededID := uuid.New()
	snapshot := store.PlanSnapshot{
		ExecutionID: uuid.New(), Status: "running", JournalThrough: 7,
		Commands: []store.PlanCommandSnapshot{
			{ID: succeededID, Key: "done", Name: command.Name(), Version: command.Version(), Origin: "plan",
				State: "succeeded", Result: encoded.BytesCopy(), ResultLoaded: true},
			{ID: uuid.New(), Key: "failed", Name: command.Name(), Version: command.Version(), Origin: "plan",
				State: "failed", FailureCode: "permanent", FailureMessage: "failed"},
		},
	}
	first := newPlan(snapshot)
	if value, ok := Result(first, "done", command); !ok || value.Value != "ready" {
		t.Fatalf("successful result=%#v available=%t", value, ok)
	}
	if _, ok := Result(first, "failed", command); ok {
		t.Fatal("failed command exposed a successful result")
	}
	if outcome, ok := Outcome(first, "failed", command); !ok || outcome.Status != StatusFailed || outcome.Failure.Code != "permanent" {
		t.Fatalf("failed outcome=%#v available=%t", outcome, ok)
	}
	if first.waitingReads != 0 {
		t.Fatalf("permanently unavailable read counted as waiting: %d", first.waitingReads)
	}
	left, err := planEvaluationFingerprint(first)
	if err != nil {
		t.Fatal(err)
	}
	second := newPlan(snapshot)
	_, _ = Result(second, "failed", command)
	_, _ = Outcome(second, "failed", command)
	_, _ = Result(second, "done", command)
	right, err := planEvaluationFingerprint(second)
	if err != nil {
		t.Fatal(err)
	}
	if string(left.Bytes) != string(right.Bytes) {
		t.Fatalf("read order changed fingerprint:\n%s\n%s", left.Bytes, right.Bytes)
	}
}
