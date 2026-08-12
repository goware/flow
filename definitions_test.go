package flow

import (
	"context"
	"errors"
	"testing"
	"time"
)

type testArgs struct {
	ID string `json:"id"`
}

type testResult struct {
	OK bool `json:"ok"`
}

func TestDefinitionIdentity(t *testing.T) {
	t.Parallel()

	cmd := DefineCommand[testArgs, testResult]("send_receipt", 2, WithQueue("mail"), WithTimeout(time.Minute))
	if cmd.err != nil {
		t.Fatalf("DefineCommand() validation = %v", cmd.err)
	}
	if cmd.Name() != "send_receipt" || cmd.Version() != 2 {
		t.Fatalf("command identity = %s/%d", cmd.Name(), cmd.Version())
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
		{name: "invalid attempts", cmd: DefineCommand[testArgs, testResult]("valid", 1, WithRetry(Attempts(0)))},
		{name: "duplicate retry", cmd: DefineCommand[testArgs, testResult]("valid", 1, WithRetry(Attempts(2)), WithRetry(RetryFor(time.Minute)))},
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
}

func TestErasedWorkerRegistration(t *testing.T) {
	t.Parallel()

	cmd := DefineCommand[testArgs, testResult]("erase_worker", 1)
	var committed Commit[testArgs, testResult]
	registration := Handle(cmd,
		func(_ context.Context, work *Work[testArgs]) (testResult, error) {
			if work.Args.ID != "input" || work.Info().CommandKey != "work/1" || work.flowScope() == nil {
				t.Fatalf("worker scope = %#v", work)
			}
			return testResult{OK: true}, nil
		},
		WithCommit(func(_ context.Context, _ Tx, commit Commit[testArgs, testResult]) error {
			committed = commit
			return nil
		}),
	).flowRegistration()
	if registration.validation != nil {
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
	if nilWork.Info() != (CommandInfo{}) || nilWork.flowScope() != nil {
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
