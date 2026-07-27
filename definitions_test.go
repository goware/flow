package flow

import (
	"context"
	"errors"
	"testing"
	"time"
)

type dummyClient struct{ id int }

func (*dummyClient) flowClient() {}

type testArgs struct {
	ID string `json:"id"`
}

type testResult struct {
	OK bool `json:"ok"`
}

func TestDefinitionIdentityAndBinding(t *testing.T) {
	t.Parallel()

	cmd := DefineCommand[testArgs, testResult]("send_receipt", 2, WithQueue("mail"), WithTimeout(time.Minute))
	if cmd.err != nil {
		t.Fatalf("DefineCommand() validation = %v", cmd.err)
	}
	if cmd.Name() != "send_receipt" || cmd.Version() != 2 {
		t.Fatalf("command identity = %s/%d", cmd.Name(), cmd.Version())
	}
	done := cmd.Done()
	if done.def.Name != cmd.Name() || done.def.Version != cmd.Version() || done.def.Namespace != "command_success" {
		t.Fatalf("Done() identity = %#v", done.def)
	}

	clientA, clientB := &dummyClient{id: 1}, &dummyClient{id: 2}
	boundA := cmd.With(clientA)
	boundB := boundA.With(clientB)
	if cmd.client != nil || boundA.client != clientA || boundB.client != clientB {
		t.Fatal("With mutated or failed to replace the client binding")
	}
	if cmd.def != boundA.def || boundA.def != boundB.def || boundB.Name() != cmd.Name() {
		t.Fatal("With changed durable definition identity")
	}

	plan := DefinePlan("receipt_flow", 1, func(*Plan, testArgs) {})
	coord := DefineCoordinator("receipt_agent", 1, OnStart(func(context.Context, *Coordination[testArgs]) error { return nil }))
	if plan.With(clientA).client != clientA || plan.client != nil || coord.With(clientA).client != clientA || coord.client != nil {
		t.Fatal("plan/coordinator binding is not immutable")
	}

	encoded, err := cmd.def.Args.Encode(testArgs{ID: "42"}, 0)
	if err != nil {
		t.Fatalf("args Encode() error = %v", err)
	}
	decoded, err := cmd.def.Args.Decode(encoded.Bytes)
	if err != nil || decoded != (testArgs{ID: "42"}) {
		t.Fatalf("args Decode() = %#v, %v", decoded, err)
	}
}

func TestDefinitionValidation(t *testing.T) {
	t.Parallel()

	tests := []struct {
		name string
		cmd  Command[testArgs, testResult]
	}{
		{name: "empty name", cmd: DefineCommand[testArgs, testResult]("", 1)},
		{name: "whitespace", cmd: DefineCommand[testArgs, testResult]("bad name", 1)},
		{name: "zero version", cmd: DefineCommand[testArgs, testResult]("valid", 0)},
		{name: "invalid attempts", cmd: DefineCommand[testArgs, testResult]("valid", 1, WithMaxAttempts(0))},
		{name: "duplicate retry", cmd: DefineCommand[testArgs, testResult]("valid", 1, WithMaxAttempts(2), WithRetryPolicy(RetryFor(time.Minute)))},
		{name: "invalid timeout", cmd: DefineCommand[testArgs, testResult]("valid", 1, WithTimeout(0))},
		{name: "invalid queue", cmd: DefineCommand[testArgs, testResult]("valid", 1, WithQueue("bad queue"))},
	}
	for _, tt := range tests {
		t.Run(tt.name, func(t *testing.T) {
			if tt.cmd.err == nil {
				t.Fatal("invalid command has no validation error")
			}
		})
	}
	if got := (Command[testArgs, testResult]{}).Done(); got.err == nil {
		t.Fatal("Done on zero command has no validation error")
	}
}

func TestRegistrationValidation(t *testing.T) {
	t.Parallel()

	cmd := DefineCommand[testArgs, testResult]("work", 1)
	worker := func(context.Context, *Work[testArgs]) (testResult, error) { return testResult{OK: true}, nil }
	commit := func(context.Context, Tx, Commit[testArgs, testResult]) error { return nil }
	registration := Handle(cmd, worker, WithCommit(commit), WithCommit(commit)).flowRegistration()
	if registration.validation == nil {
		t.Fatal("multiple commit functions were accepted")
	}
	if Handle(cmd, nil).flowRegistration().validation == nil {
		t.Fatal("nil worker was accepted")
	}

	event := DefineEvent[string]("noticed", 1)
	handler := func(context.Context, *Coordination[int], Received[string]) error { return nil }
	duplicate := DefineCoordinator("duplicate", 1, On(event, handler), On(event, handler))
	if duplicate.err == nil {
		t.Fatal("duplicate coordinator event selector was accepted")
	}
	overlap := DefineCoordinator("overlap", 1,
		On(cmd.Done(), func(context.Context, *Coordination[int], Received[testResult]) error { return nil }),
		OnOutcome(cmd, func(context.Context, *Coordination[int], Received[CommandOutcome[testResult]]) error { return nil }),
	)
	if overlap.err == nil {
		t.Fatal("success/outcome overlap was accepted")
	}
	starts := DefineCoordinator("starts", 1,
		OnStart(func(context.Context, *Coordination[int]) error { return nil }),
		OnStart(func(context.Context, *Coordination[int]) error { return nil }),
	)
	if starts.err == nil {
		t.Fatal("duplicate start handler was accepted")
	}
	if DefineCoordinator("zero-event", 1, On[int](Event[string]{}, handler)).err == nil {
		t.Fatal("zero event handler was accepted")
	}
	if DefineCoordinator("zero-command", 1,
		OnOutcome(Command[testArgs, testResult]{}, func(context.Context, *Coordination[int], Received[CommandOutcome[testResult]]) error { return nil }),
	).err == nil {
		t.Fatal("zero command outcome handler was accepted")
	}
}

func TestCoordinatorTerminalDecision(t *testing.T) {
	t.Parallel()

	scope := &Coordination[int]{scope: &scopeState{}}
	if err := SucceedExecution(scope, "result/1"); err != nil {
		t.Fatalf("SucceedExecution() error = %v", err)
	}
	if err := SucceedExecution(scope, "result/1"); err != nil {
		t.Fatalf("equivalent SucceedExecution() error = %v", err)
	}
	if err := SucceedExecution(scope, "result/2"); !errors.Is(err, ErrConflict) {
		t.Fatalf("conflicting SucceedExecution() error = %v", err)
	}
	if !errors.Is(scope.scope.firstError, ErrConflict) {
		t.Fatalf("scope poison = %v", scope.scope.firstError)
	}
	if err := FailExecution((*Coordination[int])(nil), errors.New("failed")); !errors.Is(err, ErrInvalidState) {
		t.Fatalf("inactive FailExecution() error = %v", err)
	}
}

func TestErasedWorkerRegistration(t *testing.T) {
	t.Parallel()

	cmd := DefineCommand[testArgs, testResult]("erase_worker", 1)
	var committed Commit[testArgs, testResult]
	registration := Handle(cmd,
		func(_ context.Context, work *Work[testArgs]) (testResult, error) {
			if work.Args.ID != "input" || work.Info().CommandKey != "work/1" || work.flowScope() == nil || work.flowResultSource() == nil {
				t.Fatalf("worker scope = %#v", work)
			}
			return testResult{OK: true}, nil
		},
		WithCommit(func(_ context.Context, _ Tx, commit Commit[testArgs, testResult]) error {
			committed = commit
			return nil
		}),
	).flowRegistration()
	if registration.validation != nil || registration.kind != workerRegistrationKind {
		t.Fatalf("registration = %#v", registration)
	}
	erased := registration.value.(erasedWorker)
	scope := &workScope{args: testArgs{ID: "input"}, info: CommandInfo{CommandKey: "work/1"}}
	result, err := erased.invoke(context.Background(), scope)
	if err != nil || result != (testResult{OK: true}) {
		t.Fatalf("invoke() = %#v, %v", result, err)
	}
	if err := erased.commit(context.Background(), nil, scope.args, result, scope.info); err != nil {
		t.Fatalf("commit() error = %v", err)
	}
	if committed.Args != (testArgs{ID: "input"}) || committed.Result != (testResult{OK: true}) {
		t.Fatalf("commit = %#v", committed)
	}
	if _, err := erased.invoke(context.Background(), &workScope{args: "wrong"}); !errors.Is(err, ErrInvalid) {
		t.Fatalf("invoke type error = %v", err)
	}
	if err := erased.commit(context.Background(), nil, "wrong", result, scope.info); !errors.Is(err, ErrInvalid) {
		t.Fatalf("commit type error = %v", err)
	}

	var nilWork *Work[testArgs]
	if nilWork.Info() != (CommandInfo{}) || nilWork.flowScope() != nil || nilWork.flowResultSource() != nil {
		t.Fatal("nil Work accessors are not safe")
	}
	if Handle(cmd, func(context.Context, *Work[testArgs]) (testResult, error) { return testResult{}, nil }, nil).flowRegistration().validation == nil {
		t.Fatal("nil WorkerOption was accepted")
	}
	if Handle(Command[testArgs, testResult]{}, func(context.Context, *Work[testArgs]) (testResult, error) { return testResult{}, nil }).flowRegistration().validation == nil {
		t.Fatal("zero command registration was accepted")
	}
	if Handle(cmd, func(context.Context, *Work[testArgs]) (testResult, error) { return testResult{}, nil }, WithCommit[testArgs, testResult](nil)).flowRegistration().validation == nil {
		t.Fatal("nil commit function was accepted")
	}
}

func TestErasedPlanRegistration(t *testing.T) {
	t.Parallel()

	called := false
	plan := DefinePlan("erase_plan", 2, func(_ *Plan, args testArgs) { called = args.ID == "input" })
	if plan.def.Name != "erase_plan" || plan.def.Version != 2 {
		t.Fatalf("plan identity = %s/%d", plan.def.Name, plan.def.Version)
	}
	registration := plan.flowRegistration()
	if registration.validation != nil || registration.kind != planRegistrationKind {
		t.Fatalf("registration = %#v", registration)
	}
	erased := registration.value.(erasedPlan)
	if err := erased.invoke(&Plan{}, testArgs{ID: "input"}); err != nil || !called {
		t.Fatalf("invoke() error = %v, called = %v", err, called)
	}
	if err := erased.invoke(&Plan{}, "wrong"); !errors.Is(err, ErrInvalid) {
		t.Fatalf("invoke type error = %v", err)
	}
	if (PlanDef[testArgs]{}).flowRegistration().validation == nil {
		t.Fatal("zero plan registration was accepted")
	}
	if DefinePlan[testArgs]("nil_plan", 1, nil).err == nil {
		t.Fatal("nil plan function was accepted")
	}
}

func TestErasedCoordinatorRegistration(t *testing.T) {
	t.Parallel()

	event := DefineEvent[string]("notice", 1)
	cmd := DefineCommand[testArgs, testResult]("erase_outcome", 1)
	coordinator := DefineCoordinator("erase_coordinator", 3,
		OnStart(func(_ context.Context, coordination *Coordination[int]) error {
			coordination.State++
			return nil
		}),
		On(event, func(_ context.Context, coordination *Coordination[int], received Received[string]) error {
			coordination.State += len(received.Payload)
			return nil
		}),
		OnOutcome(cmd, func(_ context.Context, coordination *Coordination[int], received Received[CommandOutcome[testResult]]) error {
			if received.Payload.Status == StatusSucceeded {
				coordination.State += 10
			}
			return nil
		}),
	)
	if coordinator.def.Name != "erase_coordinator" || coordinator.def.Version != 3 {
		t.Fatalf("coordinator identity = %s/%d", coordinator.def.Name, coordinator.def.Version)
	}
	registration := coordinator.flowRegistration()
	if registration.validation != nil || registration.kind != coordinatorRegistrationKind {
		t.Fatalf("registration = %#v", registration)
	}
	erased := registration.value.(erasedCoordinator)
	scope := &coordinatorScope{state: 0}
	for key, payload := range map[string]any{
		"start":                      nil,
		"event:application:notice:1": Received[string]{Payload: "abc"},
		"outcome:command_terminal:erase_outcome:1": Received[CommandOutcome[testResult]]{Payload: CommandOutcome[testResult]{Status: StatusSucceeded}},
	} {
		handler, ok := erased.handlers[key]
		if !ok {
			t.Fatalf("missing erased handler %q; have %#v", key, erased.handlers)
		}
		if err := handler.invoke(context.Background(), scope, payload); err != nil {
			t.Fatalf("handler %q error = %v", key, err)
		}
	}
	if scope.state != 14 {
		t.Fatalf("coordinator state = %v, want 14", scope.state)
	}
	start := erased.handlers["start"]
	if err := start.invoke(context.Background(), &coordinatorScope{state: "wrong"}, nil); !errors.Is(err, ErrInvalid) {
		t.Fatalf("state type error = %v", err)
	}
	eventHandler := erased.handlers["event:application:notice:1"]
	if err := eventHandler.invoke(context.Background(), scope, Received[int]{Payload: 1}); !errors.Is(err, ErrInvalid) {
		t.Fatalf("payload type error = %v", err)
	}
	if DefineCoordinator[int]("nil_handlers", 1, OnStart[int](nil), On[int](event, nil), OnOutcome[int](cmd, nil)).err == nil {
		t.Fatal("nil coordinator handlers were accepted")
	}
	if (Coordinator[int]{}).flowRegistration().validation == nil {
		t.Fatal("zero coordinator registration was accepted")
	}
}
