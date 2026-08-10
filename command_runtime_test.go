package flow

import (
	"context"
	"errors"
	"fmt"
	"strings"
	"sync"
	"sync/atomic"
	"testing"
	"time"

	"github.com/google/uuid"
	"github.com/goware/flow/internal/fault"
	"github.com/goware/flow/internal/pgschema"
	"github.com/goware/flow/internal/store"
	"github.com/goware/flow/internal/testpg"
	"github.com/jackc/pgx/v5"
	"github.com/jackc/pgx/v5/pgxpool"
)

type runtimeArgs struct {
	Value string `json:"value"`
}

func TestRuntimeRetriesPermanentTimeoutAndCommit(t *testing.T) {
	t.Parallel()

	database := testpg.Open(t)
	ctx := context.Background()
	if err := Migrate(ctx, database.DB, WithSchema(database.Schema)); err != nil {
		t.Fatalf("Migrate() error = %v", err)
	}
	if _, err := database.DB.Conn.Exec(ctx, `CREATE TABLE `+pgschema.Table(database.Schema, "runtime_commits")+`
		(command_id text PRIMARY KEY,result text NOT NULL)`); err != nil {
		t.Fatalf("create commit table: %v", err)
	}
	retryPolicy := RetryFor(2 * time.Second).Attempts(3).Backoff(10 * time.Millisecond)
	retrying := DefineCommand[runtimeArgs, runtimeResult]("runtime.retry", 1, WithRetry(retryPolicy))
	permanent := DefineCommand[runtimeArgs, runtimeResult]("runtime.permanent", 1, WithRetry(Attempts(5)))
	timed := DefineCommand[runtimeArgs, runtimeResult]("runtime.timeout", 1, WithRetry(Attempts(1)), WithTimeout(30*time.Millisecond))
	committed := DefineCommand[runtimeArgs, runtimeResult]("runtime.commit", 1)
	commitFailed := DefineCommand[runtimeArgs, runtimeResult]("runtime.commit_failed", 1, WithRetry(Attempts(3)))
	exhausted := DefineCommand[runtimeArgs, runtimeResult]("runtime.exhausted", 1, WithRetry(Attempts(2)))
	panicked := DefineCommand[runtimeArgs, runtimeResult]("runtime.panic", 1, WithRetry(Attempts(1)))
	retryAfter := DefineCommand[runtimeArgs, runtimeResult]("runtime.retry_after", 1, WithRetry(Attempts(2)))
	oversized := DefineCommand[runtimeArgs, runtimeResult]("runtime.oversized_result", 1, WithRetry(Attempts(5)))
	lifecycleChild := DefineCommand[runtimeArgs, runtimeResult]("runtime.lifecycle_child", 1, WithRetry(Attempts(1)))
	lifecycleEvent := DefineEvent[runtimeResult]("runtime.lifecycle_event")
	stageLifecycleOutput := func(work *Work[runtimeArgs], value string) {
		_ = Emit(work, lifecycleEvent, "output", runtimeResult{Value: value})
		Execute(work, "child", lifecycleChild, runtimeArgs{Value: value})
	}

	var retryCalls atomic.Int32
	var exhaustedCalls atomic.Int32
	var retryAfterCalls atomic.Int32
	var oversizedCalls atomic.Int32
	runtime, err := New(database.DB, WithSchema(database.Schema), WithWorkerConcurrency(5),
		WithPollInterval(5*time.Millisecond), withCommandLeaseForTest(300*time.Millisecond), WithShutdownGrace(time.Second))
	if err != nil {
		t.Fatalf("New() error = %v", err)
	}
	registrations := []Registration{
		Handle(retrying, func(context.Context, *Work[runtimeArgs]) (runtimeResult, error) {
			if retryCalls.Add(1) < 3 {
				return runtimeResult{}, errors.New("temporary")
			}
			return runtimeResult{Value: "retried"}, nil
		}),
		Handle(permanent, func(context.Context, *Work[runtimeArgs]) (runtimeResult, error) {
			return runtimeResult{}, Permanent(errors.New("not retryable"))
		}),
		Handle(timed, func(ctx context.Context, work *Work[runtimeArgs]) (runtimeResult, error) {
			stageLifecycleOutput(work, "timeout")
			<-ctx.Done()
			return runtimeResult{}, ctx.Err()
		}),
		Handle(committed, func(context.Context, *Work[runtimeArgs]) (runtimeResult, error) {
			return runtimeResult{Value: "atomic"}, nil
		}, WithCommit(func(ctx context.Context, tx Tx, commit Commit[runtimeArgs, runtimeResult]) error {
			_, err := tx.Exec(ctx, `INSERT INTO `+pgschema.Table(database.Schema, "runtime_commits")+`
				(command_id,result) VALUES ($1,$2)`, commit.Info.CommandID, commit.Result.Value)
			return err
		})),
		Handle(commitFailed, func(context.Context, *Work[runtimeArgs]) (runtimeResult, error) {
			return runtimeResult{Value: "must-rollback"}, nil
		}, WithCommit(func(context.Context, Tx, Commit[runtimeArgs, runtimeResult]) error {
			return Permanent(errors.New("commit rejected"))
		})),
		Handle(exhausted, func(context.Context, *Work[runtimeArgs]) (runtimeResult, error) {
			exhaustedCalls.Add(1)
			return runtimeResult{}, errors.New("still unavailable")
		}),
		Handle(panicked, func(_ context.Context, work *Work[runtimeArgs]) (runtimeResult, error) {
			stageLifecycleOutput(work, "panic")
			panic("worker bug")
		}),
		Handle(retryAfter, func(context.Context, *Work[runtimeArgs]) (runtimeResult, error) {
			if retryAfterCalls.Add(1) == 1 {
				return runtimeResult{}, RetryAfter(15*time.Millisecond, errors.New("rate limited"))
			}
			return runtimeResult{Value: "after-delay"}, nil
		}),
		Handle(oversized, func(context.Context, *Work[runtimeArgs]) (runtimeResult, error) {
			oversizedCalls.Add(1)
			return runtimeResult{Value: strings.Repeat("x", maxCommandResultBytes+1)}, nil
		}),
		Handle(lifecycleChild, func(_ context.Context, work *Work[runtimeArgs]) (runtimeResult, error) {
			return runtimeResult{Value: work.Args.Value}, nil
		}),
	}
	if err := runtime.Register(registrations...); err != nil {
		t.Fatalf("Register() error = %v", err)
	}
	cancelRun, runResult := startRuntime(t, runtime)
	defer stopRuntime(t, cancelRun, runResult)

	retryHandle, _ := retrying.With(runtime).Execute(ctx, "retry", runtimeArgs{})
	permanentHandle, _ := permanent.With(runtime).Execute(ctx, "permanent", runtimeArgs{})
	timeoutHandle, _ := timed.With(runtime).Execute(ctx, "timeout", runtimeArgs{})
	commitHandle, _ := committed.With(runtime).Execute(ctx, "commit", runtimeArgs{})
	commitFailedHandle, _ := commitFailed.With(runtime).Execute(ctx, "commit-failed", runtimeArgs{})
	exhaustedHandle, _ := exhausted.With(runtime).Execute(ctx, "exhausted", runtimeArgs{})
	panicHandle, _ := panicked.With(runtime).Execute(ctx, "panic", runtimeArgs{})
	retryAfterHandle, _ := retryAfter.With(runtime).Execute(ctx, "retry-after", runtimeArgs{})
	oversizedHandle, _ := oversized.With(runtime).Execute(ctx, "oversized", runtimeArgs{})
	waitForExecutionStatus(t, database.Schema, database.DB.Conn, retryHandle.ID, "succeeded", 5*time.Second)
	waitForExecutionStatus(t, database.Schema, database.DB.Conn, permanentHandle.ID, "failed", 5*time.Second)
	waitForExecutionStatus(t, database.Schema, database.DB.Conn, timeoutHandle.ID, "failed", 5*time.Second)
	waitForExecutionStatus(t, database.Schema, database.DB.Conn, commitHandle.ID, "succeeded", 5*time.Second)
	waitForExecutionStatus(t, database.Schema, database.DB.Conn, commitFailedHandle.ID, "failed", 5*time.Second)
	waitForExecutionStatus(t, database.Schema, database.DB.Conn, exhaustedHandle.ID, "failed", 5*time.Second)
	waitForExecutionStatus(t, database.Schema, database.DB.Conn, panicHandle.ID, "failed", 5*time.Second)
	waitForExecutionStatus(t, database.Schema, database.DB.Conn, retryAfterHandle.ID, "succeeded", 5*time.Second)
	waitForExecutionStatus(t, database.Schema, database.DB.Conn, oversizedHandle.ID, "failed", 5*time.Second)

	if retryCalls.Load() != 3 {
		t.Fatalf("retry worker calls = %d", retryCalls.Load())
	}
	retryTrace, err := Trace(ctx, runtime, retryHandle.ID)
	if err != nil {
		t.Fatalf("retry Trace error = %v", err)
	}
	retryableAttempts, succeededAttempts := 0, 0
	validAttempts := len(retryTrace.Commands) == 1 && len(retryTrace.Commands[0].Attempts) >= 3
	if validAttempts {
		for _, attempt := range retryTrace.Commands[0].Attempts {
			switch attempt.Classification {
			case "retryable":
				retryableAttempts++
			case "succeeded":
				succeededAttempts++
			case "interrupted":
				validAttempts = validAttempts && !attempt.ConsumedBudget
			default:
				validAttempts = false
			}
		}
		last := retryTrace.Commands[0].Attempts[len(retryTrace.Commands[0].Attempts)-1]
		validAttempts = validAttempts && retryableAttempts == 2 && succeededAttempts == 1 &&
			last.Classification == "succeeded" && last.ConsumedAttempts == 2
	}
	if !validAttempts {
		t.Fatalf("retry Trace = %#v, %v", retryTrace, err)
	}
	permanentTrace, _ := Trace(ctx, runtime, permanentHandle.ID)
	if len(permanentTrace.Commands[0].Attempts) != 1 || permanentTrace.Commands[0].Attempts[0].Classification != "permanent" {
		t.Fatalf("permanent Trace = %#v", permanentTrace)
	}
	timeoutTrace, _ := Trace(ctx, runtime, timeoutHandle.ID)
	if len(timeoutTrace.Commands[0].Attempts) != 1 || timeoutTrace.Commands[0].Attempts[0].Classification != "timeout" {
		t.Fatalf("timeout Trace = %#v", timeoutTrace)
	}
	for name, exec := range map[string]Execution{"timeout": timeoutHandle, "panic": panicHandle} {
		var eventCount, childCount int
		if err := database.DB.Conn.QueryRow(ctx, `SELECT
			(SELECT count(*) FROM `+pgschema.Table(database.Schema, "flow_journal")+`
			 WHERE execution_id=$1 AND event_class='application'),
			(SELECT count(*) FROM `+pgschema.Table(database.Schema, "flow_commands")+`
			 WHERE execution_id=$1 AND parent_command_id IS NOT NULL)`, exec.ID).Scan(&eventCount, &childCount); err != nil {
			t.Fatalf("%s staged output query: %v", name, err)
		}
		if eventCount != 0 || childCount != 0 {
			t.Fatalf("%s exposed staged events=%d children=%d", name, eventCount, childCount)
		}
	}
	var committedRows int
	if err := database.DB.Conn.QueryRow(ctx, `SELECT count(*) FROM `+pgschema.Table(database.Schema, "runtime_commits")+`
		WHERE command_id=$1 AND result='atomic'`, commitHandle.RootCommandID).Scan(&committedRows); err != nil || committedRows != 1 {
		t.Fatalf("commit rows = %d, %v", committedRows, err)
	}
	var failedResult []byte
	if err := database.DB.Conn.QueryRow(ctx, `SELECT result FROM `+pgschema.Table(database.Schema, "flow_commands")+`
		WHERE command_id=$1`, commitFailedHandle.RootCommandID).Scan(&failedResult); err != nil {
		t.Fatalf("read failed commit result: %v", err)
	}
	if failedResult != nil {
		t.Fatalf("failed commit retained result %q", failedResult)
	}
	if exhaustedCalls.Load() != 2 || retryAfterCalls.Load() != 2 || oversizedCalls.Load() != 1 {
		t.Fatalf("edge worker calls exhausted=%d retry-after=%d oversized=%d",
			exhaustedCalls.Load(), retryAfterCalls.Load(), oversizedCalls.Load())
	}
	exhaustedTrace, _ := Trace(ctx, runtime, exhaustedHandle.ID)
	panicTrace, _ := Trace(ctx, runtime, panicHandle.ID)
	retryAfterTrace, _ := Trace(ctx, runtime, retryAfterHandle.ID)
	oversizedTrace, _ := Trace(ctx, runtime, oversizedHandle.ID)
	if len(exhaustedTrace.Commands[0].Attempts) != 2 || exhaustedTrace.Commands[0].Failure == nil || exhaustedTrace.Commands[0].Failure.Code != "worker_error" ||
		panicTrace.Commands[0].Attempts[0].Classification != "panic" || panicTrace.Commands[0].Failure == nil || panicTrace.Commands[0].Failure.Code != "panic" ||
		retryAfterTrace.Commands[0].Attempts[0].Classification != "retry_after" ||
		retryAfterTrace.Commands[0].Attempts[0].NextAttemptAt == nil ||
		oversizedTrace.Commands[0].Failure == nil || oversizedTrace.Commands[0].Failure.Code != "result_encode" {
		t.Fatalf("edge traces exhausted=%#v panic=%#v retry-after=%#v oversized=%#v",
			exhaustedTrace, panicTrace, retryAfterTrace, oversizedTrace)
	}
}

func TestRuntimeRecoversCommitPanicAndRollsBackItsWrites(t *testing.T) {
	t.Parallel()

	database := testpg.Open(t)
	ctx := context.Background()
	if err := Migrate(ctx, database.DB, WithSchema(database.Schema)); err != nil {
		t.Fatal(err)
	}
	if _, err := database.DB.Conn.Exec(ctx, `CREATE TABLE `+pgschema.Table(database.Schema, "commit_panic_writes")+`
		(command_id text PRIMARY KEY)`); err != nil {
		t.Fatal(err)
	}
	panicking := DefineCommand[None, None]("runtime.commit_panic", 1, WithRetry(Attempts(1)))
	sibling := DefineCommand[None, None]("runtime.commit_panic_sibling", 1, WithRetry(Attempts(1)))
	runtime, err := New(database.DB, WithSchema(database.Schema), WithNotifications(false),
		WithWorkerConcurrency(2), WithPollInterval(5*time.Millisecond))
	if err != nil {
		t.Fatal(err)
	}
	if err := runtime.Register(
		Handle(panicking, func(context.Context, *Work[None]) (None, error) {
			return None{}, nil
		}, WithCommit(func(ctx context.Context, tx Tx, commit Commit[None, None]) error {
			if _, err := tx.Exec(ctx, `INSERT INTO `+pgschema.Table(database.Schema, "commit_panic_writes")+`
				(command_id) VALUES ($1)`, commit.Info.CommandID); err != nil {
				return err
			}
			panic("secret commit panic value")
		})),
		Handle(sibling, func(context.Context, *Work[None]) (None, error) { return None{}, nil }),
	); err != nil {
		t.Fatal(err)
	}
	cancel, result := startRuntime(t, runtime)
	defer stopRuntime(t, cancel, result)
	failed, err := panicking.With(runtime).Execute(ctx, "commit-panic", None{})
	if err != nil {
		t.Fatal(err)
	}
	succeeded, err := sibling.With(runtime).Execute(ctx, "commit-panic-sibling", None{})
	if err != nil {
		t.Fatal(err)
	}
	waitForExecutionStatus(t, database.Schema, database.DB.Conn, failed.ID, "failed", 5*time.Second)
	waitForExecutionStatus(t, database.Schema, database.DB.Conn, succeeded.ID, "succeeded", 5*time.Second)

	var writes int
	if err := database.DB.Conn.QueryRow(ctx, `SELECT count(*) FROM `+pgschema.Table(database.Schema, "commit_panic_writes")).Scan(&writes); err != nil {
		t.Fatal(err)
	}
	if writes != 0 {
		t.Fatalf("panicking commit retained %d application writes", writes)
	}
	trace, err := Trace(ctx, runtime, failed.ID)
	if err != nil {
		t.Fatal(err)
	}
	if len(trace.Commands) != 1 || len(trace.Commands[0].Attempts) != 1 ||
		trace.Commands[0].Attempts[0].Classification != "panic" || trace.Commands[0].Failure == nil ||
		trace.Commands[0].Failure.Code != "panic" || strings.Contains(trace.Commands[0].Failure.Message, "secret") {
		t.Fatalf("commit-panic trace = %#v", trace)
	}
}

func TestDeterministicEarlyFailuresUseDetachedConclusionContext(t *testing.T) {
	t.Parallel()

	database := testpg.Open(t)
	ctx := context.Background()
	if err := Migrate(ctx, database.DB, WithSchema(database.Schema)); err != nil {
		t.Fatal(err)
	}
	malformed := DefineCommand[None, None]("runtime.detached_argument_failure", 1, WithRetry(Attempts(1)))
	duplicate := DefineCommand[None, None]("runtime.detached_event_failure", 1, WithRetry(Attempts(1)))
	runtime, err := New(database.DB, WithSchema(database.Schema), WithNotifications(false), WithWorkerConcurrency(1))
	if err != nil {
		t.Fatal(err)
	}
	if err := runtime.Register(
		Handle(malformed, func(context.Context, *Work[None]) (None, error) { return None{}, nil }),
		Handle(duplicate, func(context.Context, *Work[None]) (None, error) { return None{}, nil }),
	); err != nil {
		t.Fatal(err)
	}
	malformedExecution, err := malformed.With(runtime).Execute(ctx, "detached/argument", None{}, WithoutExecutionDeadline())
	if err != nil {
		t.Fatal(err)
	}
	duplicateExecution, err := duplicate.With(runtime).Execute(ctx, "detached/event", None{}, WithoutExecutionDeadline())
	if err != nil {
		t.Fatal(err)
	}
	if _, err := database.DB.Conn.Exec(ctx, `UPDATE `+pgschema.Table(database.Schema, "flow_commands")+`
		SET args=convert_to('not-json','UTF8') WHERE command_id=$1`, malformedExecution.RootCommandID); err != nil {
		t.Fatal(err)
	}

	claims := make(map[ExecutionID]store.ClaimedCommand, 2)
	for _, definition := range []struct {
		name    string
		version int
	}{
		{name: malformed.Name(), version: malformed.Version()},
		{name: duplicate.Name(), version: duplicate.Version()},
	} {
		candidates, err := runtime.store.ProbeCommands(ctx, []store.CommandKind{{Name: definition.name, Version: definition.version}}, 1)
		if err != nil || len(candidates) != 1 {
			t.Fatalf("ProbeCommands(%s) candidates=%d error=%v", definition.name, len(candidates), err)
		}
		claimed, err := runtime.store.ClaimCommands(ctx, candidates, time.Minute, "detached-test", fault.None{})
		if err != nil || len(claimed.Commands) != 1 {
			t.Fatalf("ClaimCommands(%s) commands=%d error=%v", definition.name, len(claimed.Commands), err)
		}
		claim := claimed.Commands[0]
		past := time.Now().Add(-time.Second)
		claim.ExecutionDeadline = &past
		claims[ExecutionID(claim.ExecutionID.String())] = claim
	}
	duplicateClaim := claims[duplicateExecution.ID]
	duplicateClaim.EventInputs = []store.ClaimedEventInput{
		{Name: "duplicate", Key: "same", Position: 1, Payload: []byte(`{}`)},
		{Name: "duplicate", Key: "same", Position: 1, Payload: []byte(`{}`)},
	}
	claims[duplicateExecution.ID] = duplicateClaim

	for _, test := range []struct {
		execution Execution
		command   Command[None, None]
		code      string
	}{
		{execution: malformedExecution, command: malformed, code: "argument_decode"},
		{execution: duplicateExecution, command: duplicate, code: "event_input_decode"},
	} {
		worker, ok := runtime.registry.worker(test.command.Name(), test.command.Version())
		if !ok {
			t.Fatalf("worker %s missing", test.command.Name())
		}
		claim := claims[test.execution.ID]
		slots := newCommandSlots(1, nil)
		if reserved, _ := slots.reserve(claim.Queue); !reserved {
			t.Fatal("failed to reserve direct test slot")
		}
		runtime.workerGroup.Add(1)
		runtime.executeClaim(worker, claim, slots)
		trace, err := Trace(ctx, runtime, test.execution.ID)
		if err != nil {
			t.Fatal(err)
		}
		if trace.Execution.Status != ExecutionStatusFailed || len(trace.Commands) != 1 || trace.Commands[0].Failure == nil ||
			trace.Commands[0].Failure.Code != test.code {
			t.Fatalf("detached %s trace = %#v", test.code, trace)
		}
	}
}

func TestDeadlineDiscardedDecisionEmitsOnlyExpiredSettlement(t *testing.T) {
	t.Parallel()

	database := testpg.Open(t)
	ctx := context.Background()
	if err := Migrate(ctx, database.DB, WithSchema(database.Schema)); err != nil {
		t.Fatal(err)
	}
	observer := &recordingObserver{}
	event := DefineEvent[None]("runtime.deadline_discard_event")
	command := DefineCommand[None, None]("runtime.deadline_discard", 1)
	runtime, err := New(database.DB, WithSchema(database.Schema), WithNotifications(false), WithObserver(observer))
	if err != nil {
		t.Fatal(err)
	}
	runtime.observations.run()
	defer runtime.observations.close()
	if err := runtime.Register(Handle(command, func(_ context.Context, work *Work[None]) (None, error) {
		if err := Emit(work, event, "discarded", None{}); err != nil {
			return None{}, err
		}
		return None{}, nil
	})); err != nil {
		t.Fatal(err)
	}
	execution, err := command.With(runtime).Execute(ctx, "deadline/discard", None{}, WithoutExecutionDeadline())
	if err != nil {
		t.Fatal(err)
	}
	candidates, err := runtime.store.ProbeCommands(ctx,
		[]store.CommandKind{{Name: command.Name(), Version: command.Version()}}, 1)
	if err != nil || len(candidates) != 1 {
		t.Fatalf("ProbeCommands() candidates=%d error=%v", len(candidates), err)
	}
	claimed, err := runtime.store.ClaimCommands(ctx, candidates, time.Minute, "deadline-discard-test", fault.None{})
	if err != nil || len(claimed.Commands) != 1 {
		t.Fatalf("ClaimCommands() commands=%d error=%v", len(claimed.Commands), err)
	}
	claim := claimed.Commands[0]
	claim.ExecutionDeadline = nil
	if _, err := database.DB.Conn.Exec(ctx, `UPDATE `+pgschema.Table(database.Schema, "flow_executions")+`
		SET deadline_at=clock_timestamp()-interval '1 second' WHERE execution_id=$1`, execution.ID); err != nil {
		t.Fatal(err)
	}
	worker, ok := runtime.registry.worker(command.Name(), command.Version())
	if !ok {
		t.Fatal("registered worker missing")
	}
	slots := newCommandSlots(1, nil)
	if reserved, _ := slots.reserve(claim.Queue); !reserved {
		t.Fatal("failed to reserve direct test slot")
	}
	runtime.workerGroup.Add(1)
	runtime.executeClaim(worker, claim, slots)
	waitForObservation(t, observer, "settle", "expired", 1, time.Second)
	for _, observation := range observer.snapshot() {
		if observation.ExecutionID != execution.ID {
			continue
		}
		if (observation.Kind == ObservationEvent && observation.Operation == "settle" && observation.Outcome == "accepted") ||
			(observation.Kind == ObservationAttempt && observation.Operation == "settle" && observation.Outcome == "succeeded") {
			t.Fatalf("discarded decision emitted success observation: %#v", observation)
		}
	}
	var status string
	var applicationEvents int
	if err := database.DB.Conn.QueryRow(ctx, `SELECT status,
		(SELECT count(*) FROM `+pgschema.Table(database.Schema, "flow_journal")+`
		 WHERE execution_id=e.execution_id AND event_class='application')
		FROM `+pgschema.Table(database.Schema, "flow_executions")+` e WHERE execution_id=$1`, execution.ID).
		Scan(&status, &applicationEvents); err != nil {
		t.Fatal(err)
	}
	if status != "expired" || applicationEvents != 0 {
		t.Fatalf("discarded decision status=%s application_events=%d", status, applicationEvents)
	}
}

func TestRuntimeStagesDelayedChildrenAtomically(t *testing.T) {
	t.Parallel()
	database := testpg.Open(t)
	ctx := context.Background()
	if err := Migrate(ctx, database.DB, WithSchema(database.Schema)); err != nil {
		t.Fatal(err)
	}
	type childArgs struct{ Value string }
	type childResult struct{ Value string }
	type rootResult struct{ Children int }
	child := DefineCommand[childArgs, childResult]("graph.child", 1)
	root := DefineCommand[None, rootResult]("graph.root", 1,
		WithRetry(RetryFor(time.Second).Attempts(2).Backoff(10*time.Millisecond)))
	var childStartedAt atomic.Int64
	var rootCalls atomic.Int32

	runtime, err := New(database.DB, WithSchema(database.Schema), WithPollInterval(5*time.Millisecond))
	if err != nil {
		t.Fatal(err)
	}
	if err := runtime.Register(
		Handle(root, func(_ context.Context, work *Work[None]) (rootResult, error) {
			Execute(work, "child/required", child, childArgs{Value: "required"})
			Execute(work, "child/delayed", child, childArgs{Value: "delayed"}).Optional().Delay(80 * time.Millisecond)
			if rootCalls.Add(1) == 1 {
				return rootResult{}, errors.New("retry after staging")
			}
			return rootResult{Children: 2}, nil
		}),
		Handle(child, func(_ context.Context, work *Work[childArgs]) (childResult, error) {
			if work.Args.Value == "delayed" {
				childStartedAt.Store(time.Now().UnixNano())
			}
			return childResult{Value: work.Args.Value}, nil
		}),
	); err != nil {
		t.Fatal(err)
	}
	cancelRun, runResult := startRuntime(t, runtime)
	defer stopRuntime(t, cancelRun, runResult)
	started := time.Now()
	exec, err := root.With(runtime).Execute(ctx, "graph-tree", None{})
	if err != nil {
		t.Fatal(err)
	}
	waitForExecutionStatus(t, database.Schema, database.DB.Conn, exec.ID, "succeeded", 5*time.Second)
	if got := time.Unix(0, childStartedAt.Load()).Sub(started); got < 60*time.Millisecond {
		t.Fatalf("delayed child started after %s", got)
	}
	trace, err := Trace(ctx, runtime, exec.ID)
	if err != nil {
		t.Fatal(err)
	}
	if len(trace.Commands) != 3 || trace.Execution.CommandCount != 3 || trace.Execution.OpenCommands != 0 {
		t.Fatalf("Trace topology = commands %d, count %d, open %d", len(trace.Commands), trace.Execution.CommandCount, trace.Execution.OpenCommands)
	}
	if rootCalls.Load() != 2 {
		t.Fatalf("root calls = %d, want 2", rootCalls.Load())
	}
	var parentID string
	for _, command := range trace.Commands {
		if command.Key == "root" {
			parentID = string(command.ID)
		}
	}
	if parentID == "" {
		t.Fatal("root command missing")
	}
	var linked int
	if err := database.DB.Conn.QueryRow(ctx, `SELECT count(*) FROM `+pgschema.Table(database.Schema, "flow_commands")+`
		WHERE execution_id=$1 AND parent_command_id=$2`, exec.ID, parentID).Scan(&linked); err != nil {
		t.Fatal(err)
	}
	if linked != 2 {
		t.Fatalf("linked children = %d, want 2", linked)
	}
	assertReplayMatches(t, runtime, exec.ID)
}

func TestRuntimeFailFastCancelsQueuedSiblings(t *testing.T) {
	t.Parallel()
	for _, test := range []struct {
		name        string
		failFast    bool
		wantSibling string
	}{
		{name: "enabled", failFast: true, wantSibling: string(StatusCancelled)},
		{name: "disabled", failFast: false, wantSibling: string(StatusSucceeded)},
	} {
		t.Run(test.name, func(t *testing.T) {
			database := testpg.Open(t)
			ctx := context.Background()
			if err := Migrate(ctx, database.DB, WithSchema(database.Schema)); err != nil {
				t.Fatal(err)
			}
			type args struct{ Kind string }
			child := DefineCommand[args, None]("failfast.child."+test.name, 1, WithRetry(Attempts(1)))
			root := DefineCommand[None, None]("failfast.root."+test.name, 1)
			var siblingCalls atomic.Int32
			runtime, err := New(database.DB, WithSchema(database.Schema), WithWorkerConcurrency(1),
				WithPollInterval(5*time.Millisecond))
			if err != nil {
				t.Fatal(err)
			}
			if err := runtime.Register(
				Handle(root, func(_ context.Context, work *Work[None]) (None, error) {
					Execute(work, "a-failure", child, args{Kind: "fail"})
					Execute(work, "z-sibling", child, args{Kind: "sibling"}).Delay(60 * time.Millisecond)
					return None{}, nil
				}),
				Handle(child, func(_ context.Context, work *Work[args]) (None, error) {
					if work.Args.Kind == "fail" {
						return None{}, Permanent(errors.New("expected failure"))
					}
					siblingCalls.Add(1)
					return None{}, nil
				}),
			); err != nil {
				t.Fatal(err)
			}
			cancelRun, runResult := startRuntime(t, runtime)
			defer stopRuntime(t, cancelRun, runResult)
			exec, err := root.With(runtime).Execute(ctx, "failfast/"+test.name, None{}, WithFailFast(test.failFast))
			if err != nil {
				t.Fatal(err)
			}
			waitForExecutionStatus(t, database.Schema, database.DB.Conn, exec.ID, "failed", 5*time.Second)
			trace, err := Trace(ctx, runtime, exec.ID)
			if err != nil {
				t.Fatal(err)
			}
			var siblingStatus CommandStatus
			for _, command := range trace.Commands {
				if command.Key == "z-sibling" {
					siblingStatus = command.State
				}
			}
			if siblingStatus != CommandStatus(test.wantSibling) {
				t.Fatalf("sibling status = %q, want %q", siblingStatus, test.wantSibling)
			}
			wantCalls := int32(0)
			if !test.failFast {
				wantCalls = 1
			}
			if siblingCalls.Load() != wantCalls {
				t.Fatalf("sibling calls = %d, want %d", siblingCalls.Load(), wantCalls)
			}
			assertReplayMatches(t, runtime, exec.ID)
		})
	}
}

func TestRunningAttemptSettlementAfterRequiredFailureHandlesNewChildren(t *testing.T) {
	t.Parallel()
	const stagedChildren = 10
	for _, test := range []struct {
		name       string
		failFast   bool
		childState CommandStatus
		childCalls int32
	}{
		{name: "enabled", failFast: true, childState: StatusCancelled},
		{name: "disabled", failFast: false, childState: StatusSucceeded, childCalls: stagedChildren},
	} {
		t.Run(test.name, func(t *testing.T) {
			database := testpg.Open(t)
			ctx := context.Background()
			if err := Migrate(ctx, database.DB, WithSchema(database.Schema)); err != nil {
				t.Fatal(err)
			}
			type args struct{ Kind string }
			root := DefineCommand[None, None]("settling.root."+test.name, 1)
			parallel := DefineCommand[args, None]("settling.parallel."+test.name, 1, WithRetry(Attempts(1)))
			late := DefineCommand[None, None]("settling.late."+test.name, 1)
			fact := DefineEvent[None]("settling.fact." + test.name)
			survivorStarted := make(chan struct{})
			releaseSurvivor := make(chan struct{})
			var lateCalls atomic.Int32
			runtime, err := New(database.DB, WithSchema(database.Schema), WithWorkerConcurrency(2),
				WithPollInterval(5*time.Millisecond), WithNotifications(false))
			if err != nil {
				t.Fatal(err)
			}
			if err := runtime.Register(
				Handle(root, func(_ context.Context, work *Work[None]) (None, error) {
					Execute(work, "a-failure", parallel, args{Kind: "failure"})
					Execute(work, "b-survivor", parallel, args{Kind: "survivor"})
					return None{}, nil
				}),
				Handle(parallel, func(_ context.Context, work *Work[args]) (None, error) {
					if work.Args.Kind == "failure" {
						select {
						case <-survivorStarted:
						case <-time.After(3 * time.Second):
							return None{}, Permanent(errors.New("survivor did not start"))
						}
						return None{}, Permanent(errors.New("required failure"))
					}
					close(survivorStarted)
					<-releaseSurvivor
					if err := Emit(work, fact, "committed", None{}); err != nil {
						return None{}, err
					}
					for index := range stagedChildren {
						Execute(work, fmt.Sprintf("late-child/%02d", index), late, None{})
					}
					return None{}, nil
				}),
				Handle(late, func(context.Context, *Work[None]) (None, error) {
					lateCalls.Add(1)
					return None{}, nil
				}),
			); err != nil {
				t.Fatal(err)
			}
			cancelRun, runResult := startRuntime(t, runtime)
			defer stopRuntime(t, cancelRun, runResult)
			exec, err := root.With(runtime).Execute(ctx, "settling/"+test.name, None{}, WithFailFast(test.failFast))
			if err != nil {
				t.Fatal(err)
			}
			waitForExecutionStatus(t, database.Schema, database.DB.Conn, exec.ID, "failing", 5*time.Second)
			close(releaseSurvivor)
			waitForExecutionStatus(t, database.Schema, database.DB.Conn, exec.ID, "failed", 5*time.Second)
			trace, err := Trace(ctx, runtime, exec.ID)
			if err != nil {
				t.Fatal(err)
			}
			var lateChildren int
			for _, command := range trace.Commands {
				if strings.HasPrefix(command.Key, "late-child/") {
					lateChildren++
					if command.State != test.childState {
						t.Fatalf("late child %q state=%s, want %s", command.Key, command.State, test.childState)
					}
				}
			}
			if lateChildren != stagedChildren || lateCalls.Load() != test.childCalls {
				t.Fatalf("late children/calls=%d/%d want %d/%d", lateChildren, lateCalls.Load(), stagedChildren, test.childCalls)
			}
			var events int
			if err := database.DB.Conn.QueryRow(ctx, `SELECT count(*) FROM `+pgschema.Table(database.Schema, "flow_journal")+`
				WHERE execution_id=$1 AND event_class='application' AND event_name=$2 AND event_key='committed'`,
				exec.ID, fact.Name()).Scan(&events); err != nil || events != 1 {
				t.Fatalf("survivor event count=%d err=%v", events, err)
			}
			assertReplayMatches(t, runtime, exec.ID)
		})
	}
}

func TestRuntimeCapacityLeaseRenewalAndTakeover(t *testing.T) {
	t.Parallel()

	database := testpg.Open(t)
	ctx := context.Background()
	if err := Migrate(ctx, database.DB, WithSchema(database.Schema)); err != nil {
		t.Fatalf("Migrate() error = %v", err)
	}
	command := DefineCommand[runtimeArgs, runtimeResult]("runtime.capacity", 1, WithRetry(Attempts(2)))
	blocking := make(chan struct{})
	var active, maximum, started atomic.Int32
	runtime, err := New(database.DB, WithSchema(database.Schema), WithWorkerConcurrency(2),
		WithPollInterval(5*time.Millisecond), withCommandLeaseForTest(90*time.Millisecond), WithShutdownGrace(time.Second))
	if err != nil {
		t.Fatalf("New() error = %v", err)
	}
	if err := runtime.Register(Handle(command, func(context.Context, *Work[runtimeArgs]) (runtimeResult, error) {
		current := active.Add(1)
		defer active.Add(-1)
		started.Add(1)
		for {
			old := maximum.Load()
			if current <= old || maximum.CompareAndSwap(old, current) {
				break
			}
		}
		<-blocking
		return runtimeResult{Value: "released"}, nil
	})); err != nil {
		t.Fatalf("Register() error = %v", err)
	}
	cancelRun, runResult := startRuntime(t, runtime)
	execs := make([]Execution, 5)
	for index := range execs {
		execs[index], err = command.With(runtime).Execute(ctx, fmt.Sprintf("capacity/%d", index), runtimeArgs{})
		if err != nil {
			t.Fatalf("Execute(%d) error = %v", index, err)
		}
	}
	waitForCount(t, &started, 2, 3*time.Second)
	time.Sleep(150 * time.Millisecond) // exceeds the lease; renewal must retain both attempts.
	if started.Load() != 2 || maximum.Load() > 2 {
		t.Fatalf("capacity started=%d maximum=%d", started.Load(), maximum.Load())
	}
	close(blocking)
	for _, exec := range execs {
		waitForExecutionStatus(t, database.Schema, database.DB.Conn, exec.ID, "succeeded", 5*time.Second)
	}
	stopRuntime(t, cancelRun, runResult)

	takeover := DefineCommand[runtimeArgs, runtimeResult]("runtime.takeover", 1, WithRetry(Attempts(2)))
	takeoverInput := DefineEvent[runtimeArgs]("runtime.takeover_input")
	firstStarted := make(chan struct{}, 1)
	releaseFirst := make(chan struct{})
	var firstInput, secondInput atomic.Value
	first, err := New(database.DB, WithSchema(database.Schema), WithWorkerConcurrency(1),
		WithPollInterval(5*time.Millisecond), withCommandLeaseForTest(90*time.Millisecond), WithShutdownGrace(0))
	if err != nil {
		t.Fatalf("New(first) error = %v", err)
	}
	if err := first.Register(Handle(takeover, func(_ context.Context, work *Work[runtimeArgs]) (runtimeResult, error) {
		input, err := GetEventValue(work, takeoverInput, "input")
		if err != nil {
			return runtimeResult{}, err
		}
		firstInput.Store(input.Value)
		firstStarted <- struct{}{}
		<-releaseFirst // deliberately ignores cancellation; its stale result must be fenced.
		return runtimeResult{Value: "stale"}, nil
	})); err != nil {
		t.Fatalf("Register(first) error = %v", err)
	}
	cancelFirst, firstResult := startRuntime(t, first)
	exec, err := takeover.With(first).Execute(ctx, "takeover", runtimeArgs{}, WaitFor(takeoverInput, "input"))
	if err != nil {
		t.Fatalf("Execute(takeover) error = %v", err)
	}
	if err := takeoverInput.Emit(ctx, first, exec.ID, "input", runtimeArgs{Value: "stable"}); err != nil {
		t.Fatalf("Emit(takeover input) error = %v", err)
	}
	select {
	case <-firstStarted:
	case <-time.After(3 * time.Second):
		t.Fatal("first replica did not start takeover command")
	}
	cancelFirst()
	select {
	case err := <-firstResult:
		if err != nil {
			t.Fatalf("first Run() error = %v", err)
		}
	case <-time.After(3 * time.Second):
		t.Fatal("first runtime did not stop")
	}

	second, err := New(database.DB, WithSchema(database.Schema), WithWorkerConcurrency(1),
		WithPollInterval(5*time.Millisecond), withCommandLeaseForTest(90*time.Millisecond), WithShutdownGrace(time.Second))
	if err != nil {
		t.Fatalf("New(second) error = %v", err)
	}
	if err := second.Register(Handle(takeover, func(_ context.Context, work *Work[runtimeArgs]) (runtimeResult, error) {
		input, err := GetEventValue(work, takeoverInput, "input")
		if err != nil {
			return runtimeResult{}, err
		}
		secondInput.Store(input.Value)
		return runtimeResult{Value: "takeover"}, nil
	})); err != nil {
		t.Fatalf("Register(second) error = %v", err)
	}
	cancelSecond, secondResult := startRuntime(t, second)
	waitForExecutionStatus(t, database.Schema, database.DB.Conn, exec.ID, "succeeded", 5*time.Second)
	close(releaseFirst)
	stopRuntime(t, cancelSecond, secondResult)
	trace, err := Trace(ctx, second, exec.ID)
	if !errors.Is(err, ErrClosed) {
		// The stopped runtime is intentionally closed; use a fresh API client below.
		t.Fatalf("Trace(stopped runtime) error = %v", err)
	}
	reader, err := New(database.DB, WithSchema(database.Schema))
	if err != nil {
		t.Fatalf("New(reader) error = %v", err)
	}
	trace, err = Trace(ctx, reader, exec.ID)
	if err != nil || len(trace.Commands) != 1 || len(trace.Commands[0].Attempts) != 2 ||
		trace.Commands[0].Attempts[0].Classification != "lease_lost" || trace.Commands[0].Result == nil ||
		string(trace.Commands[0].Result) != `{"value":"takeover"}` || len(trace.Commands[0].Waits) != 1 ||
		trace.Commands[0].Waits[0].SatisfiedPosition == nil || firstInput.Load() != "stable" || secondInput.Load() != "stable" {
		t.Fatalf("takeover Trace = %#v, %v", trace, err)
	}
}

func TestRuntimeReleasesDatabaseConnectionBeforeWorker(t *testing.T) {
	t.Parallel()

	// A one-connection pool makes connection ownership observable: this query
	// can complete only if the claim transaction released the pool connection
	// before the application handler began.
	database := testpg.OpenWithMaxConns(t, 1)
	ctx := context.Background()
	if err := Migrate(ctx, database.DB, WithSchema(database.Schema)); err != nil {
		t.Fatalf("Migrate() error = %v", err)
	}
	command := DefineCommand[runtimeArgs, runtimeResult]("runtime.connection_release", 1)
	runtime, err := New(database.DB, WithSchema(database.Schema), WithWorkerConcurrency(1),
		WithPollInterval(100*time.Millisecond), withCommandLeaseForTest(time.Second), WithNotifications(false))
	if err != nil {
		t.Fatalf("New() error = %v", err)
	}
	if err := runtime.Register(Handle(command, func(ctx context.Context, _ *Work[runtimeArgs]) (runtimeResult, error) {
		queryCtx, cancel := context.WithTimeout(ctx, time.Second)
		defer cancel()
		var value int
		if err := database.DB.Conn.QueryRow(queryCtx, `SELECT 1`).Scan(&value); err != nil {
			return runtimeResult{}, fmt.Errorf("worker database query: %w", err)
		}
		return runtimeResult{Value: fmt.Sprintf("db:%d", value)}, nil
	})); err != nil {
		t.Fatalf("Register() error = %v", err)
	}
	cancelRun, runResult := startRuntime(t, runtime)
	exec, err := command.With(runtime).Execute(ctx, "connection-release", runtimeArgs{})
	if err != nil {
		t.Fatalf("Execute() error = %v", err)
	}
	waitForExecutionStatus(t, database.Schema, database.DB.Conn, exec.ID, "succeeded", 5*time.Second)
	stopRuntime(t, cancelRun, runResult)
}

func TestClaimedEventInputSnapshotsRejectDuplicates(t *testing.T) {
	t.Parallel()

	payload := []byte(`{"value":"stable"}`)
	inputs := []store.ClaimedEventInput{
		{Name: "event", Key: "key", Position: 7, Payload: payload},
	}
	snapshots, duplicate := claimedEventInputSnapshots(inputs)
	if duplicate || len(snapshots) != 1 || snapshots["event\x00key"].position != 7 ||
		string(snapshots["event\x00key"].payload) != string(payload) {
		t.Fatalf("claimedEventInputSnapshots() = %#v, duplicate=%t", snapshots, duplicate)
	}
	inputs = append(inputs, store.ClaimedEventInput{Name: "event", Key: "key", Position: 8, Payload: []byte(`null`)})
	if snapshots, duplicate := claimedEventInputSnapshots(inputs); !duplicate || snapshots != nil {
		t.Fatalf("duplicate snapshots = %#v, duplicate=%t", snapshots, duplicate)
	}
}

func TestRuntimeQueueConcurrencyAndFairSelection(t *testing.T) {
	t.Parallel()

	database := testpg.Open(t)
	ctx := context.Background()
	if err := Migrate(ctx, database.DB, WithSchema(database.Schema)); err != nil {
		t.Fatalf("Migrate() error = %v", err)
	}
	bulk := DefineCommand[runtimeArgs, runtimeResult]("runtime.queue.bulk", 1, WithQueue("bulk"))
	latency := DefineCommand[runtimeArgs, runtimeResult]("runtime.queue.latency", 1, WithQueue("latency"))
	bulkRelease := make(chan struct{})
	var bulkStarted atomic.Int32
	runtime, err := New(database.DB, WithSchema(database.Schema), WithWorkerConcurrency(2),
		WithQueueConcurrency("bulk", 1), WithPollInterval(5*time.Millisecond), withCommandLeaseForTest(time.Second))
	if err != nil {
		t.Fatalf("New() error = %v", err)
	}
	if err := runtime.Register(
		Handle(bulk, func(context.Context, *Work[runtimeArgs]) (runtimeResult, error) {
			bulkStarted.Add(1)
			<-bulkRelease
			return runtimeResult{Value: "bulk"}, nil
		}),
		Handle(latency, func(context.Context, *Work[runtimeArgs]) (runtimeResult, error) {
			return runtimeResult{Value: "latency"}, nil
		}),
	); err != nil {
		t.Fatalf("Register() error = %v", err)
	}
	cancelRun, runResult := startRuntime(t, runtime)
	bulkHandles := make([]Execution, 0, commandProbeFactor+1)
	firstBulk, err := bulk.With(runtime).Execute(ctx, "queue/bulk/active", runtimeArgs{})
	if err != nil {
		t.Fatalf("bulk Execute(active) error = %v", err)
	}
	bulkHandles = append(bulkHandles, firstBulk)
	waitForCount(t, &bulkStarted, 1, 3*time.Second)
	for index := range commandProbeFactor {
		execution, executeErr := bulk.With(runtime).Execute(ctx, fmt.Sprintf("queue/bulk/backlog/%d", index), runtimeArgs{})
		if executeErr != nil {
			t.Fatalf("bulk Execute(%d) error = %v", index, executeErr)
		}
		bulkHandles = append(bulkHandles, execution)
	}
	latencyHandle, err := latency.With(runtime).Execute(ctx, "queue/latency", runtimeArgs{})
	if err != nil {
		t.Fatalf("latency Execute() error = %v", err)
	}
	waitForExecutionStatus(t, database.Schema, database.DB.Conn, latencyHandle.ID, "succeeded", 5*time.Second)
	if bulkStarted.Load() != 1 {
		t.Fatalf("bulk workers started while lane blocked = %d, want 1", bulkStarted.Load())
	}
	close(bulkRelease)
	for _, exec := range bulkHandles {
		waitForExecutionStatus(t, database.Schema, database.DB.Conn, exec.ID, "succeeded", 5*time.Second)
	}
	stopRuntime(t, cancelRun, runResult)
}

func TestRuntimeClaimsIndependentExecutionsConcurrently(t *testing.T) {
	t.Parallel()

	database := testpg.Open(t)
	ctx := context.Background()
	if err := Migrate(ctx, database.DB, WithSchema(database.Schema)); err != nil {
		t.Fatal(err)
	}
	command := DefineCommand[None, None]("runtime.concurrent_claims", 1)
	runtime, err := New(database.DB, WithSchema(database.Schema), WithWorkerConcurrency(4),
		WithPollInterval(5*time.Millisecond), WithNotifications(false))
	if err != nil {
		t.Fatal(err)
	}
	if err := runtime.Register(Handle(command, func(context.Context, *Work[None]) (None, error) {
		return None{}, nil
	})); err != nil {
		t.Fatal(err)
	}
	executions := make([]Execution, 4)
	for index := range executions {
		executions[index], err = command.With(runtime).Execute(ctx, fmt.Sprintf("concurrent-claim/%d", index), None{})
		if err != nil {
			t.Fatal(err)
		}
	}
	entered := make(chan struct{}, len(executions))
	release := make(chan struct{})
	runtime.faults = fault.Func(func(ctx context.Context, point fault.Point) error {
		if point != fault.ClaimBeforeJournal {
			return nil
		}
		select {
		case entered <- struct{}{}:
		case <-ctx.Done():
			return ctx.Err()
		}
		select {
		case <-release:
			return nil
		case <-ctx.Done():
			return ctx.Err()
		}
	})
	cancel, runResult := startRuntime(t, runtime)
	defer func() {
		select {
		case <-release:
		default:
			close(release)
		}
		stopRuntime(t, cancel, runResult)
	}()
	for range 2 {
		select {
		case <-entered:
		case <-time.After(3 * time.Second):
			t.Fatal("independent claim transactions did not overlap")
		}
	}
	close(release)
	for _, execution := range executions {
		waitForExecutionStatus(t, database.Schema, database.DB.Conn, execution.ID, "succeeded", 5*time.Second)
	}
}

func TestRuntimeClaimsOnlyAvailableSiblingSlots(t *testing.T) {
	t.Parallel()

	database := testpg.Open(t)
	ctx := context.Background()
	if err := Migrate(ctx, database.DB, WithSchema(database.Schema)); err != nil {
		t.Fatal(err)
	}
	parent := DefineCommand[None, None]("runtime.sibling_capacity_parent", 1)
	child := DefineCommand[None, None]("runtime.sibling_capacity_child", 1)
	release := make(chan struct{})
	var started atomic.Int32
	runtime, err := New(database.DB, WithSchema(database.Schema), WithWorkerConcurrency(3),
		WithMaxCommandsPerExecution(0), WithPollInterval(5*time.Millisecond), WithNotifications(false))
	if err != nil {
		t.Fatal(err)
	}
	if err := runtime.Register(
		Handle(parent, func(_ context.Context, work *Work[None]) (None, error) {
			for index := range 8 {
				Execute(work, fmt.Sprintf("child/%d", index), child, None{})
			}
			return None{}, nil
		}),
		Handle(child, func(context.Context, *Work[None]) (None, error) {
			started.Add(1)
			<-release
			return None{}, nil
		}),
	); err != nil {
		t.Fatal(err)
	}
	cancel, runResult := startRuntime(t, runtime)
	defer func() {
		select {
		case <-release:
		default:
			close(release)
		}
		stopRuntime(t, cancel, runResult)
	}()
	execution, err := parent.With(runtime).Execute(ctx, "sibling/capacity", None{})
	if err != nil {
		t.Fatal(err)
	}
	waitForCount(t, &started, 3, 3*time.Second)
	var running, starts int
	if err := database.DB.Conn.QueryRow(ctx, `SELECT
		count(*) FILTER (WHERE c.state='running'),
		(SELECT count(*) FROM `+pgschema.Table(database.Schema, "flow_journal")+` j
		 WHERE j.execution_id=$1 AND j.entry_kind='attempt_started' AND j.command_id<>$2)
	FROM `+pgschema.Table(database.Schema, "flow_commands")+` c
	WHERE c.execution_id=$1 AND c.parent_command_id=$2`, execution.ID, execution.RootCommandID).
		Scan(&running, &starts); err != nil {
		t.Fatal(err)
	}
	if started.Load() != 3 || running != 3 || starts != 3 {
		t.Fatalf("sibling capacity started=%d running=%d starts=%d, want 3", started.Load(), running, starts)
	}
	close(release)
	waitForExecutionStatus(t, database.Schema, database.DB.Conn, execution.ID, "succeeded", 5*time.Second)
}

func TestRuntimeLockedExecutionReleasesSlotForLaterCandidate(t *testing.T) {
	t.Parallel()

	database := testpg.Open(t)
	ctx := context.Background()
	if err := Migrate(ctx, database.DB, WithSchema(database.Schema)); err != nil {
		t.Fatal(err)
	}
	lockedChild := DefineCommand[None, None]("runtime.locked_claim_fairness_child", 1)
	_, locked := stageClaimFixture(t, database, "locked_claim_fairness", commandProbeFactor, func(work *Work[None]) {
		for index := range commandProbeFactor {
			Execute(work, fmt.Sprintf("child/%d", index), lockedChild, None{})
		}
	})
	availableCommand := DefineCommand[None, None]("runtime.locked_claim_fairness_available", 1)
	runtime, err := New(database.DB, WithSchema(database.Schema), WithWorkerConcurrency(1),
		WithPollInterval(5*time.Millisecond), WithNotifications(false))
	if err != nil {
		t.Fatal(err)
	}
	if err := runtime.Register(
		Handle(lockedChild, func(context.Context, *Work[None]) (None, error) { return None{}, nil }),
		Handle(availableCommand, func(context.Context, *Work[None]) (None, error) { return None{}, nil }),
	); err != nil {
		t.Fatal(err)
	}
	available, err := availableCommand.With(runtime).Execute(ctx, "locked-claim/available", None{})
	if err != nil {
		t.Fatal(err)
	}
	lockTx, err := database.DB.Conn.BeginTx(ctx, pgx.TxOptions{})
	if err != nil {
		t.Fatal(err)
	}
	defer func() { _ = lockTx.Rollback(context.WithoutCancel(ctx)) }()
	if _, err := lockTx.Exec(ctx, `SELECT execution_id FROM `+pgschema.Table(database.Schema, "flow_executions")+`
		WHERE execution_id=$1 FOR UPDATE`, locked.ID); err != nil {
		t.Fatal(err)
	}
	cancel, runResult := startRuntime(t, runtime)
	waitForExecutionStatus(t, database.Schema, database.DB.Conn, available.ID, "succeeded", 5*time.Second)
	stopRuntime(t, cancel, runResult)
	var attempts int
	if err := database.DB.Conn.QueryRow(ctx, `SELECT COALESCE(sum(attempt_ordinal),0) FROM `+
		pgschema.Table(database.Schema, "flow_commands")+` WHERE execution_id=$1 AND parent_command_id=$2`,
		locked.ID, locked.RootCommandID).Scan(&attempts); err != nil {
		t.Fatal(err)
	}
	if attempts != 0 {
		t.Fatalf("locked sibling attempts=%d, want 0", attempts)
	}
}

func TestRuntimeProbeContinuationPassesExclusionCap(t *testing.T) {
	t.Parallel()

	database := testpg.Open(t)
	ctx := context.Background()
	if err := Migrate(ctx, database.DB, WithSchema(database.Schema)); err != nil {
		t.Fatal(err)
	}
	blockedCommand := DefineCommand[None, None]("runtime.probe_continuation_blocked", 1)
	availableCommand := DefineCommand[None, None]("runtime.probe_continuation_available", 1)
	runtime, err := New(database.DB, WithSchema(database.Schema), WithWorkerConcurrency(1),
		WithPollInterval(5*time.Millisecond), WithNotifications(false))
	if err != nil {
		t.Fatal(err)
	}
	if err := runtime.Register(
		Handle(blockedCommand, func(context.Context, *Work[None]) (None, error) { return None{}, nil }),
		Handle(availableCommand, func(context.Context, *Work[None]) (None, error) { return None{}, nil }),
	); err != nil {
		t.Fatal(err)
	}
	blockedExecutionIDs := make([]uuid.UUID, maxCommandProbe)
	for index := range maxCommandProbe {
		execution, executeErr := blockedCommand.With(runtime).Execute(ctx,
			fmt.Sprintf("probe-continuation/blocked/%03d", index), None{})
		if executeErr != nil {
			t.Fatal(executeErr)
		}
		blockedExecutionIDs[index] = uuid.MustParse(string(execution.ID))
	}
	available, err := availableCommand.With(runtime).Execute(ctx, "probe-continuation/available", None{})
	if err != nil {
		t.Fatal(err)
	}
	lockTx, err := database.DB.Conn.BeginTx(ctx, pgx.TxOptions{})
	if err != nil {
		t.Fatal(err)
	}
	defer func() { _ = lockTx.Rollback(context.WithoutCancel(ctx)) }()
	if _, err := lockTx.Exec(ctx, `SELECT execution_id FROM `+pgschema.Table(database.Schema, "flow_executions")+`
		WHERE execution_id=ANY($1::uuid[]) FOR UPDATE`, blockedExecutionIDs); err != nil {
		t.Fatal(err)
	}
	cancel, runResult := startRuntime(t, runtime)
	waitForExecutionStatus(t, database.Schema, database.DB.Conn, available.ID, "succeeded", 10*time.Second)
	stopRuntime(t, cancel, runResult)
	var attempts int
	if err := database.DB.Conn.QueryRow(ctx, `SELECT COALESCE(sum(attempt_ordinal),0) FROM `+
		pgschema.Table(database.Schema, "flow_commands")+` WHERE execution_id=ANY($1::uuid[])`, blockedExecutionIDs).
		Scan(&attempts); err != nil {
		t.Fatal(err)
	}
	if attempts != 0 {
		t.Fatalf("stable blocked prefix attempts=%d, want 0", attempts)
	}
}

func TestRuntimeProbeContinuationRevisitsHeadBetweenBoundedTurns(t *testing.T) {
	t.Parallel()

	database := testpg.Open(t)
	ctx := context.Background()
	if err := Migrate(ctx, database.DB, WithSchema(database.Schema)); err != nil {
		t.Fatal(err)
	}
	command := DefineCommand[None, None]("runtime.probe_continuation_revisit", 1)
	runtime, err := New(database.DB, WithSchema(database.Schema), WithWorkerConcurrency(1),
		WithPollInterval(5*time.Millisecond), WithNotifications(false))
	if err != nil {
		t.Fatal(err)
	}
	if err := runtime.Register(Handle(command, func(context.Context, *Work[None]) (None, error) {
		return None{}, nil
	})); err != nil {
		t.Fatal(err)
	}
	head, err := command.With(runtime).Execute(ctx, "probe-revisit/head", None{})
	if err != nil {
		t.Fatal(err)
	}
	headID := uuid.MustParse(string(head.ID))
	tailIDs := make([]uuid.UUID, maxCommandProbe)
	for index := range maxCommandProbe {
		execution, executeErr := command.With(runtime).Execute(ctx, fmt.Sprintf("probe-revisit/tail/%03d", index), None{})
		if executeErr != nil {
			t.Fatal(executeErr)
		}
		tailIDs[index] = uuid.MustParse(string(execution.ID))
	}
	headTx, err := database.DB.Conn.BeginTx(ctx, pgx.TxOptions{})
	if err != nil {
		t.Fatal(err)
	}
	defer func() { _ = headTx.Rollback(context.WithoutCancel(ctx)) }()
	if _, err := headTx.Exec(ctx, `SELECT execution_id FROM `+pgschema.Table(database.Schema, "flow_executions")+`
		WHERE execution_id=$1 FOR UPDATE`, headID); err != nil {
		t.Fatal(err)
	}
	tailTx, err := database.DB.Conn.BeginTx(ctx, pgx.TxOptions{})
	if err != nil {
		t.Fatal(err)
	}
	defer func() { _ = tailTx.Rollback(context.WithoutCancel(ctx)) }()
	if _, err := tailTx.Exec(ctx, `SELECT execution_id FROM `+pgschema.Table(database.Schema, "flow_executions")+`
		WHERE execution_id=ANY($1::uuid[]) FOR UPDATE`, tailIDs); err != nil {
		t.Fatal(err)
	}
	continuing := make(chan struct{})
	releaseProbe := make(chan struct{})
	var probes atomic.Int32
	var keepArriving atomic.Bool
	var arrivalSequence atomic.Int32
	runtime.faults = fault.Func(func(hookCtx context.Context, point fault.Point) error {
		if point != fault.ProbeReturn {
			return nil
		}
		probe := probes.Add(1)
		if probe == int32(maxCommandRoundsPerTurn) {
			close(continuing)
			<-releaseProbe
		}
		if probe > int32(maxCommandRoundsPerTurn) && keepArriving.Load() && hookCtx.Err() == nil {
			index := arrivalSequence.Add(1)
			execution, executeErr := command.With(runtime).Execute(hookCtx,
				fmt.Sprintf("probe-revisit/arrival/%d", index), None{})
			if executeErr != nil {
				return executeErr
			}
			_, lockErr := tailTx.Exec(hookCtx, `SELECT execution_id FROM `+
				pgschema.Table(database.Schema, "flow_executions")+` WHERE execution_id=$1 FOR UPDATE`,
				uuid.MustParse(string(execution.ID)))
			return lockErr
		}
		return nil
	})
	cancel, runResult := startRuntime(t, runtime)
	defer func() {
		keepArriving.Store(false)
		select {
		case <-releaseProbe:
		default:
			close(releaseProbe)
		}
		stopRuntime(t, cancel, runResult)
	}()
	select {
	case <-continuing:
	case <-time.After(10 * time.Second):
		t.Fatal("scheduler did not reach its bounded continuation turn")
	}
	firstArrival, err := command.With(runtime).Execute(ctx, "probe-revisit/arrival/0", None{})
	if err != nil {
		t.Fatal(err)
	}
	if _, err := tailTx.Exec(ctx, `SELECT execution_id FROM `+pgschema.Table(database.Schema, "flow_executions")+`
		WHERE execution_id=$1 FOR UPDATE`, uuid.MustParse(string(firstArrival.ID))); err != nil {
		t.Fatal(err)
	}
	keepArriving.Store(true)
	if err := headTx.Rollback(ctx); err != nil {
		t.Fatal(err)
	}
	close(releaseProbe)
	waitForExecutionStatus(t, database.Schema, database.DB.Conn, head.ID, "succeeded", 10*time.Second)
}

func TestRuntimeShutdownDrainsInflightClaimBeforeWorkerAccounting(t *testing.T) {
	t.Parallel()

	database := testpg.Open(t)
	ctx := context.Background()
	if err := Migrate(ctx, database.DB, WithSchema(database.Schema)); err != nil {
		t.Fatal(err)
	}
	command := DefineCommand[None, None]("runtime.shutdown_inflight_claim", 1)
	runtime, err := New(database.DB, WithSchema(database.Schema), WithWorkerConcurrency(2),
		WithPollInterval(5*time.Millisecond), WithNotifications(false), WithShutdownGrace(0))
	if err != nil {
		t.Fatal(err)
	}
	var handlers atomic.Int32
	if err := runtime.Register(Handle(command, func(context.Context, *Work[None]) (None, error) {
		handlers.Add(1)
		return None{}, nil
	})); err != nil {
		t.Fatal(err)
	}
	if _, err := command.With(runtime).Execute(ctx, "shutdown/inflight-claim", None{}); err != nil {
		t.Fatal(err)
	}
	entered := make(chan struct{}, 1)
	release := make(chan struct{})
	var once sync.Once
	runtime.faults = fault.Func(func(_ context.Context, point fault.Point) error {
		if point == fault.ClaimBeforeCommit {
			once.Do(func() { close(entered) })
			<-release
			return fault.Injected(point)
		}
		return nil
	})
	cancel, runResult := startRuntime(t, runtime)
	select {
	case <-entered:
	case <-time.After(3 * time.Second):
		cancel()
		t.Fatal("claim did not reach the pre-commit barrier")
	}
	cancel()
	select {
	case err := <-runResult:
		t.Fatalf("Run returned before in-flight claim drained: %v", err)
	default:
	}
	close(release)
	select {
	case err := <-runResult:
		if err != nil {
			t.Fatal(err)
		}
	case <-time.After(3 * time.Second):
		t.Fatal("Run did not finish after in-flight claim drained")
	}
	if handlers.Load() != 0 {
		t.Fatalf("handlers started during shutdown=%d, want 0", handlers.Load())
	}
}

func TestRuntimeShutdownTransfersPostCommitClaimToWorkerAccounting(t *testing.T) {
	t.Parallel()

	database := testpg.Open(t)
	ctx := context.Background()
	if err := Migrate(ctx, database.DB, WithSchema(database.Schema)); err != nil {
		t.Fatal(err)
	}
	command := DefineCommand[None, None]("runtime.shutdown_postcommit_claim", 1)
	runtime, err := New(database.DB, WithSchema(database.Schema), WithWorkerConcurrency(1),
		WithPollInterval(5*time.Millisecond), WithNotifications(false), WithShutdownGrace(time.Second))
	if err != nil {
		t.Fatal(err)
	}
	var handlers atomic.Int32
	if err := runtime.Register(Handle(command, func(context.Context, *Work[None]) (None, error) {
		handlers.Add(1)
		return None{}, nil
	})); err != nil {
		t.Fatal(err)
	}
	execution, err := command.With(runtime).Execute(ctx, "shutdown/postcommit-claim", None{})
	if err != nil {
		t.Fatal(err)
	}
	entered := make(chan struct{})
	release := make(chan struct{})
	var once sync.Once
	runtime.faults = fault.Func(func(_ context.Context, point fault.Point) error {
		if point == fault.ClaimCommitAmbiguous {
			once.Do(func() { close(entered) })
			<-release
			return fault.Injected(point)
		}
		return nil
	})
	cancel, runResult := startRuntime(t, runtime)
	select {
	case <-entered:
	case <-time.After(3 * time.Second):
		cancel()
		t.Fatal("claim did not reach the post-commit barrier")
	}
	cancel()
	select {
	case err := <-runResult:
		t.Fatalf("Run returned before post-commit claim handoff: %v", err)
	default:
	}
	close(release)
	select {
	case err := <-runResult:
		if err != nil {
			t.Fatal(err)
		}
	case <-time.After(3 * time.Second):
		t.Fatal("Run did not finish after post-commit claim handoff")
	}
	if handlers.Load() != 1 {
		t.Fatalf("post-commit handlers=%d, want 1", handlers.Load())
	}
	var state string
	var activeFences int
	if err := database.DB.Conn.QueryRow(ctx, `SELECT c.state,
		(SELECT count(*) FROM `+pgschema.Table(database.Schema, "flow_command_queue")+` q
		 WHERE q.command_id=c.command_id AND (q.active_attempt_id IS NOT NULL OR q.lease_token IS NOT NULL))
	FROM `+pgschema.Table(database.Schema, "flow_commands")+` c WHERE c.command_id=$1`, execution.RootCommandID).
		Scan(&state, &activeFences); err != nil {
		t.Fatal(err)
	}
	if state == "running" || activeFences != 0 {
		t.Fatalf("post-commit command state=%s active fences=%d", state, activeFences)
	}
}

func TestRuntimeAmbiguousClaimResolverFailureTransfersFence(t *testing.T) {
	t.Parallel()

	database := testpg.OpenWithMaxConns(t, 2)
	ctx := context.Background()
	if err := Migrate(ctx, database.DB, WithSchema(database.Schema)); err != nil {
		t.Fatal(err)
	}
	command := DefineCommand[None, None]("runtime.ambiguous_resolver_failure", 1)
	runtime, err := New(database.DB, WithSchema(database.Schema), WithWorkerConcurrency(1),
		WithPollInterval(5*time.Millisecond), WithNotifications(false), withCommandLeaseForTest(400*time.Millisecond))
	if err != nil {
		t.Fatal(err)
	}
	handlerStarted := make(chan struct{})
	handlerRelease := make(chan struct{})
	var handlers atomic.Int32
	if err := runtime.Register(Handle(command, func(context.Context, *Work[None]) (None, error) {
		handlers.Add(1)
		close(handlerStarted)
		<-handlerRelease
		return None{}, nil
	})); err != nil {
		t.Fatal(err)
	}
	execution, err := command.With(runtime).Execute(ctx, "ambiguous/resolver-failure", None{})
	if err != nil {
		t.Fatal(err)
	}
	committed := make(chan struct{})
	releaseClaim := make(chan struct{})
	var once sync.Once
	runtime.faults = fault.Func(func(_ context.Context, point fault.Point) error {
		if point == fault.ClaimCommitAmbiguous {
			once.Do(func() { close(committed) })
			<-releaseClaim
			return fault.Injected(point)
		}
		return nil
	})
	cancel, runResult := startRuntime(t, runtime)
	defer func() {
		select {
		case <-releaseClaim:
		default:
			close(releaseClaim)
		}
		select {
		case <-handlerRelease:
		default:
			close(handlerRelease)
		}
		stopRuntime(t, cancel, runResult)
	}()
	select {
	case <-committed:
	case <-time.After(3 * time.Second):
		t.Fatal("claim did not reach post-commit ambiguity barrier")
	}
	held := make([]*pgxpool.Conn, 0, 2)
	defer func() {
		for _, connection := range held {
			connection.Release()
		}
	}()
	for range 2 {
		connection, acquireErr := database.DB.Conn.Acquire(ctx)
		if acquireErr != nil {
			t.Fatal(acquireErr)
		}
		held = append(held, connection)
	}
	close(releaseClaim)
	select {
	case <-handlerStarted:
	case <-time.After(2 * time.Second):
		t.Fatal("ambiguous committed fence was dropped after resolver timeout")
	}
	for _, connection := range held {
		connection.Release()
	}
	held = nil
	close(handlerRelease)
	waitForExecutionStatus(t, database.Schema, database.DB.Conn, execution.ID, "succeeded", 5*time.Second)
	if handlers.Load() != 1 {
		t.Fatalf("ambiguous resolver-failure handlers=%d, want 1", handlers.Load())
	}
}

func TestRuntimeFalseAmbiguityReleasesPhantomBeforeRealClaim(t *testing.T) {
	t.Parallel()

	database := testpg.OpenWithMaxConns(t, 3)
	ctx := context.Background()
	if err := Migrate(ctx, database.DB, WithSchema(database.Schema)); err != nil {
		t.Fatal(err)
	}
	command := DefineCommand[None, None]("runtime.false_ambiguity", 1)
	first, err := New(database.DB, WithSchema(database.Schema), WithWorkerConcurrency(1),
		WithPollInterval(5*time.Millisecond), WithNotifications(false), withCommandLeaseForTest(400*time.Millisecond),
		WithShutdownGrace(2*time.Second))
	if err != nil {
		t.Fatal(err)
	}
	phantomStarted := make(chan struct{})
	phantomRelease := make(chan struct{})
	var phantomHandlers atomic.Int32
	if err := first.Register(Handle(command, func(context.Context, *Work[None]) (None, error) {
		phantomHandlers.Add(1)
		close(phantomStarted)
		<-phantomRelease
		return None{}, nil
	})); err != nil {
		t.Fatal(err)
	}
	execution, err := command.With(first).Execute(ctx, "false-ambiguity/claim", None{})
	if err != nil {
		t.Fatal(err)
	}
	beforeCommit := make(chan struct{})
	releaseCommit := make(chan struct{})
	var once sync.Once
	first.faults = fault.Func(func(hookCtx context.Context, point fault.Point) error {
		if point == fault.ClaimBeforeCommit {
			once.Do(func() { close(beforeCommit) })
			<-releaseCommit
			<-hookCtx.Done()
		}
		return nil
	})
	cancelFirst, firstResult := startRuntime(t, first)
	defer func() {
		select {
		case <-releaseCommit:
		default:
			close(releaseCommit)
		}
		select {
		case <-phantomRelease:
		default:
			close(phantomRelease)
		}
		cancelFirst()
	}()
	select {
	case <-beforeCommit:
	case <-time.After(3 * time.Second):
		t.Fatal("claim did not reach the false-ambiguity commit barrier")
	}
	lockConnection, err := database.DB.Conn.Acquire(ctx)
	if err != nil {
		t.Fatal(err)
	}
	defer func() {
		if lockConnection != nil {
			lockConnection.Release()
		}
	}()
	var lockPID int
	if err := lockConnection.QueryRow(ctx, `SELECT pg_backend_pid()`).Scan(&lockPID); err != nil {
		t.Fatal(err)
	}
	lockTx, err := lockConnection.Begin(ctx)
	if err != nil {
		t.Fatal(err)
	}
	defer func() { _ = lockTx.Rollback(context.Background()) }()
	defer func() {
		cancelFirst()
		select {
		case <-releaseCommit:
		default:
			close(releaseCommit)
		}
	}()
	observerConnection, err := database.DB.Conn.Acquire(ctx)
	if err != nil {
		t.Fatal(err)
	}
	defer func() {
		if observerConnection != nil {
			observerConnection.Release()
		}
	}()
	lockResult := make(chan error, 1)
	go func() {
		_, lockErr := lockTx.Exec(context.Background(), `LOCK TABLE `+
			pgschema.Table(database.Schema, "flow_commands")+` IN ACCESS EXCLUSIVE MODE`)
		lockResult <- lockErr
	}()
	waitDeadline := time.Now().Add(3 * time.Second)
	for {
		var waitType *string
		if err := observerConnection.QueryRow(ctx, `SELECT wait_event_type FROM pg_stat_activity WHERE pid=$1`, lockPID).
			Scan(&waitType); err != nil {
			t.Fatal(err)
		}
		if waitType != nil && *waitType == "Lock" {
			break
		}
		if time.Now().After(waitDeadline) {
			t.Fatal("table-lock request did not block behind the claim transaction")
		}
		time.Sleep(5 * time.Millisecond)
	}
	cancelFirst()
	close(releaseCommit)
	select {
	case lockErr := <-lockResult:
		if lockErr != nil {
			t.Fatal(lockErr)
		}
	case <-time.After(3 * time.Second):
		t.Fatal("table lock was not granted after the false commit rolled back")
	}
	select {
	case <-phantomStarted:
	case <-time.After(2 * time.Second):
		t.Fatal("false-ambiguity metadata was dropped after ownership resolution failed")
	}
	if err := lockTx.Rollback(ctx); err != nil {
		t.Fatal(err)
	}
	lockConnection.Release()
	lockConnection = nil
	observerConnection.Release()
	observerConnection = nil
	close(phantomRelease)
	select {
	case runErr := <-firstResult:
		if runErr != nil {
			t.Fatal(runErr)
		}
	case <-time.After(3 * time.Second):
		t.Fatal("first runtime did not release the conservatively transferred slot")
	}
	if phantomHandlers.Load() != 1 {
		t.Fatalf("false-ambiguity phantom handlers=%d, want 1", phantomHandlers.Load())
	}

	second, err := New(database.DB, WithSchema(database.Schema), WithWorkerConcurrency(1),
		WithPollInterval(5*time.Millisecond), WithNotifications(false))
	if err != nil {
		t.Fatal(err)
	}
	var realHandlers atomic.Int32
	if err := second.Register(Handle(command, func(context.Context, *Work[None]) (None, error) {
		realHandlers.Add(1)
		return None{}, nil
	})); err != nil {
		t.Fatal(err)
	}
	cancelSecond, secondResult := startRuntime(t, second)
	waitForExecutionStatus(t, database.Schema, database.DB.Conn, execution.ID, "succeeded", 5*time.Second)
	stopRuntime(t, cancelSecond, secondResult)
	var attemptOrdinal, attemptStarts int
	if err := database.DB.Conn.QueryRow(ctx, `SELECT c.attempt_ordinal,
		(SELECT count(*) FROM `+pgschema.Table(database.Schema, "flow_journal")+` j
		 WHERE j.execution_id=c.execution_id AND j.command_id=c.command_id AND j.entry_kind='attempt_started')
	FROM `+pgschema.Table(database.Schema, "flow_commands")+` c WHERE c.command_id=$1`, execution.RootCommandID).
		Scan(&attemptOrdinal, &attemptStarts); err != nil {
		t.Fatal(err)
	}
	if realHandlers.Load() != 1 || attemptOrdinal != 1 || attemptStarts != 1 {
		t.Fatalf("real claim handlers=%d ordinal=%d starts=%d, want 1/1/1",
			realHandlers.Load(), attemptOrdinal, attemptStarts)
	}
}

func TestRuntimeTwoConnectionPoolRetainsMaintenanceHeadroomDuringClaim(t *testing.T) {
	t.Parallel()

	database := testpg.OpenWithMaxConns(t, 2)
	ctx := context.Background()
	if err := Migrate(ctx, database.DB, WithSchema(database.Schema)); err != nil {
		t.Fatal(err)
	}
	claimable := DefineCommand[None, None]("runtime.small_pool_claim", 1)
	unhandled := DefineCommand[None, None]("runtime.small_pool_deadline", 1)
	runtime, err := New(database.DB, WithSchema(database.Schema), WithWorkerConcurrency(8),
		WithPollInterval(5*time.Millisecond), WithNotifications(false))
	if err != nil {
		t.Fatal(err)
	}
	if err := runtime.Register(Handle(claimable, func(context.Context, *Work[None]) (None, error) {
		return None{}, nil
	})); err != nil {
		t.Fatal(err)
	}
	claimExecution, err := claimable.With(runtime).Execute(ctx, "small-pool/claim", None{})
	if err != nil {
		t.Fatal(err)
	}
	deadlineExecution, err := unhandled.With(runtime).Execute(ctx, "small-pool/deadline", None{},
		WithExecutionDeadline(25*time.Millisecond))
	if err != nil {
		t.Fatal(err)
	}
	entered := make(chan struct{}, 1)
	release := make(chan struct{})
	var once sync.Once
	runtime.faults = fault.Func(func(_ context.Context, point fault.Point) error {
		if point == fault.ClaimBeforeCommit {
			once.Do(func() { close(entered) })
			<-release
		}
		return nil
	})
	cancel, runResult := startRuntime(t, runtime)
	defer func() {
		select {
		case <-release:
		default:
			close(release)
		}
		stopRuntime(t, cancel, runResult)
	}()
	select {
	case <-entered:
	case <-time.After(3 * time.Second):
		t.Fatal("claim did not hold its bounded pool connection")
	}
	waitForExecutionStatus(t, database.Schema, database.DB.Conn, deadlineExecution.ID, "expired", 5*time.Second)
	close(release)
	waitForExecutionStatus(t, database.Schema, database.DB.Conn, claimExecution.ID, "succeeded", 5*time.Second)
}

func TestClaimConcurrencyLimitLeavesMaintenanceHeadroom(t *testing.T) {
	t.Parallel()

	tests := []struct {
		workers int
		pool    int
		want    int
	}{
		{workers: 16, pool: 1, want: 1},
		{workers: 16, pool: 2, want: 1},
		{workers: 16, pool: 3, want: 1},
		{workers: 16, pool: 4, want: 2},
		{workers: 16, pool: 12, want: 8},
		{workers: 2, pool: 12, want: 2},
	}
	for _, test := range tests {
		if got := claimConcurrencyLimit(test.workers, test.pool); got != test.want {
			t.Errorf("claimConcurrencyLimit(%d, %d)=%d, want %d", test.workers, test.pool, got, test.want)
		}
	}
}

func TestRuntimeRollingVersionLeavesUnknownWorkUnclaimed(t *testing.T) {
	t.Parallel()

	database := testpg.Open(t)
	ctx := context.Background()
	if err := Migrate(ctx, database.DB, WithSchema(database.Schema)); err != nil {
		t.Fatalf("Migrate() error = %v", err)
	}
	v1 := DefineCommand[runtimeArgs, runtimeResult]("runtime.rolling", 1)
	v2 := DefineCommand[runtimeArgs, runtimeResult]("runtime.rolling", 2)
	api, err := New(database.DB, WithSchema(database.Schema))
	if err != nil {
		t.Fatalf("New(api) error = %v", err)
	}
	exec, err := v2.With(api).Execute(ctx, "rolling/v2", runtimeArgs{})
	if err != nil {
		t.Fatalf("v2 Execute() error = %v", err)
	}
	oldReplica, err := New(database.DB, WithSchema(database.Schema), WithWorkerConcurrency(1),
		WithPollInterval(5*time.Millisecond), withCommandLeaseForTest(time.Second))
	if err != nil {
		t.Fatalf("New(old replica) error = %v", err)
	}
	if err := oldReplica.Register(Handle(v1, func(context.Context, *Work[runtimeArgs]) (runtimeResult, error) {
		return runtimeResult{Value: "wrong-version"}, nil
	})); err != nil {
		t.Fatalf("Register(old replica) error = %v", err)
	}
	cancelOld, oldResult := startRuntime(t, oldReplica)
	time.Sleep(75 * time.Millisecond)
	var state string
	var attempts int
	if err := database.DB.Conn.QueryRow(ctx, `SELECT state,attempt_ordinal FROM `+
		pgschema.Table(database.Schema, "flow_commands")+` WHERE command_id=$1`, exec.RootCommandID).
		Scan(&state, &attempts); err != nil {
		t.Fatalf("read unhandled v2: %v", err)
	}
	if state != "ready" || attempts != 0 {
		t.Fatalf("old replica changed v2 command to state=%s attempts=%d", state, attempts)
	}
	stopRuntime(t, cancelOld, oldResult)

	newReplica, err := New(database.DB, WithSchema(database.Schema), WithWorkerConcurrency(1),
		WithPollInterval(5*time.Millisecond), withCommandLeaseForTest(time.Second))
	if err != nil {
		t.Fatalf("New(new replica) error = %v", err)
	}
	if err := newReplica.Register(Handle(v2, func(context.Context, *Work[runtimeArgs]) (runtimeResult, error) {
		return runtimeResult{Value: "v2"}, nil
	})); err != nil {
		t.Fatalf("Register(new replica) error = %v", err)
	}
	cancelNew, newResult := startRuntime(t, newReplica)
	waitForExecutionStatus(t, database.Schema, database.DB.Conn, exec.ID, "succeeded", 5*time.Second)
	stopRuntime(t, cancelNew, newResult)
}

func TestRuntimeCommandCancellationConcludesOnlyOwnedAttempt(t *testing.T) {
	t.Parallel()

	database := testpg.Open(t)
	ctx := context.Background()
	if err := Migrate(ctx, database.DB, WithSchema(database.Schema)); err != nil {
		t.Fatalf("Migrate() error = %v", err)
	}
	command := DefineCommand[runtimeArgs, runtimeResult]("runtime.cancel_active", 1)
	child := DefineCommand[runtimeArgs, runtimeResult]("runtime.cancel_child", 1)
	event := DefineEvent[runtimeResult]("runtime.cancel_event")
	cancelStarted := make(chan struct{}, 1)
	cancelObserved := make(chan struct{}, 1)
	otherStarted := make(chan struct{}, 1)
	otherCancelled := make(chan struct{}, 1)
	releaseOther := make(chan struct{})
	runtime, err := New(database.DB, WithSchema(database.Schema), WithWorkerConcurrency(2),
		WithPollInterval(5*time.Millisecond), withCommandLeaseForTest(300*time.Millisecond))
	if err != nil {
		t.Fatalf("New() error = %v", err)
	}
	if err := runtime.Register(Handle(command, func(ctx context.Context, work *Work[runtimeArgs]) (runtimeResult, error) {
		if work.Args.Value == "cancel" {
			_ = Emit(work, event, "cancelled", runtimeResult{Value: "must-not-commit"})
			Execute(work, "child", child, runtimeArgs{Value: "must-not-commit"})
			cancelStarted <- struct{}{}
			<-ctx.Done()
			cancelObserved <- struct{}{}
			return runtimeResult{}, ctx.Err()
		}
		otherStarted <- struct{}{}
		select {
		case <-releaseOther:
			return runtimeResult{Value: "other"}, nil
		case <-ctx.Done():
			otherCancelled <- struct{}{}
			return runtimeResult{}, ctx.Err()
		}
	}), Handle(child, func(_ context.Context, work *Work[runtimeArgs]) (runtimeResult, error) {
		return runtimeResult{Value: work.Args.Value}, nil
	})); err != nil {
		t.Fatalf("Register() error = %v", err)
	}
	cancelRun, runResult := startRuntime(t, runtime)
	cancelHandle, err := command.With(runtime).Execute(ctx, "cancel/active", runtimeArgs{Value: "cancel"})
	if err != nil {
		t.Fatalf("Execute(cancel) error = %v", err)
	}
	otherHandle, err := command.With(runtime).Execute(ctx, "cancel/other", runtimeArgs{Value: "other"})
	if err != nil {
		t.Fatalf("Execute(other) error = %v", err)
	}
	for name, signal := range map[string]<-chan struct{}{"cancel": cancelStarted, "other": otherStarted} {
		select {
		case <-signal:
		case <-time.After(3 * time.Second):
			t.Fatalf("%s attempt did not start", name)
		}
	}
	if err := CancelCommand(ctx, runtime, cancelHandle.RootCommandID, "operator cancelled"); err != nil {
		t.Fatalf("CancelCommand() error = %v", err)
	}
	select {
	case <-cancelObserved:
	case <-time.After(time.Second):
		t.Fatal("cancelled worker did not observe lease loss")
	}
	select {
	case <-otherCancelled:
		t.Fatal("cancelling one lease cancelled an unrelated handler")
	case <-time.After(50 * time.Millisecond):
	}
	close(releaseOther)
	waitForExecutionStatus(t, database.Schema, database.DB.Conn, otherHandle.ID, "succeeded", 5*time.Second)
	trace, err := Trace(ctx, runtime, cancelHandle.ID)
	if err != nil {
		t.Fatalf("Trace(cancelled) error = %v", err)
	}
	stopRuntime(t, cancelRun, runResult)
	if trace.Execution.Status != "failed" || len(trace.Commands) != 1 || trace.Commands[0].State != "cancelled" ||
		len(trace.Commands[0].Attempts) != 1 || trace.Commands[0].Attempts[0].Classification != "cancelled" ||
		trace.Commands[0].Attempts[0].FinishedAt == nil || trace.Commands[0].Attempts[0].ConsumedBudget {
		t.Fatalf("cancelled Trace = %#v", trace)
	}
	var eventCount, childCount int
	if err := database.DB.Conn.QueryRow(ctx, `SELECT
		(SELECT count(*) FROM `+pgschema.Table(database.Schema, "flow_journal")+`
		 WHERE execution_id=$1 AND event_class='application'),
		(SELECT count(*) FROM `+pgschema.Table(database.Schema, "flow_commands")+`
		 WHERE execution_id=$1 AND parent_command_id IS NOT NULL)`, cancelHandle.ID).Scan(&eventCount, &childCount); err != nil {
		t.Fatal(err)
	}
	if eventCount != 0 || childCount != 0 {
		t.Fatalf("cancelled attempt exposed staged events=%d children=%d", eventCount, childCount)
	}
}

func TestRuntimeCooperativeShutdownIsRetryableAndBudgetNeutral(t *testing.T) {
	t.Parallel()

	database := testpg.Open(t)
	ctx := context.Background()
	if err := Migrate(ctx, database.DB, WithSchema(database.Schema)); err != nil {
		t.Fatalf("Migrate() error = %v", err)
	}
	command := DefineCommand[runtimeArgs, runtimeResult]("runtime.shutdown", 1, WithRetry(Attempts(1)))
	started := make(chan struct{}, 1)
	first, err := New(database.DB, WithSchema(database.Schema), WithWorkerConcurrency(1),
		WithPollInterval(5*time.Millisecond), withCommandLeaseForTest(300*time.Millisecond), WithShutdownGrace(20*time.Millisecond))
	if err != nil {
		t.Fatalf("New(first) error = %v", err)
	}
	if err := first.Register(Handle(command, func(ctx context.Context, _ *Work[runtimeArgs]) (runtimeResult, error) {
		started <- struct{}{}
		<-ctx.Done()
		return runtimeResult{}, ctx.Err()
	})); err != nil {
		t.Fatalf("Register(first) error = %v", err)
	}
	cancelFirst, firstResult := startRuntime(t, first)
	exec, err := command.With(first).Execute(ctx, "shutdown", runtimeArgs{})
	if err != nil {
		t.Fatalf("Execute() error = %v", err)
	}
	select {
	case <-started:
	case <-time.After(3 * time.Second):
		t.Fatal("first attempt did not start")
	}
	stopRuntime(t, cancelFirst, firstResult)

	second, err := New(database.DB, WithSchema(database.Schema), WithWorkerConcurrency(1),
		WithPollInterval(5*time.Millisecond), withCommandLeaseForTest(300*time.Millisecond))
	if err != nil {
		t.Fatalf("New(second) error = %v", err)
	}
	if err := second.Register(Handle(command, func(context.Context, *Work[runtimeArgs]) (runtimeResult, error) {
		return runtimeResult{Value: "resumed"}, nil
	})); err != nil {
		t.Fatalf("Register(second) error = %v", err)
	}
	cancelSecond, secondResult := startRuntime(t, second)
	waitForExecutionStatus(t, database.Schema, database.DB.Conn, exec.ID, "succeeded", 5*time.Second)
	trace, err := Trace(ctx, second, exec.ID)
	if err != nil {
		t.Fatalf("Trace() error = %v", err)
	}
	stopRuntime(t, cancelSecond, secondResult)
	if len(trace.Commands) != 1 || len(trace.Commands[0].Attempts) != 2 ||
		trace.Commands[0].Attempts[0].Classification != "interrupted" ||
		trace.Commands[0].Attempts[0].ConsumedBudget ||
		trace.Commands[0].Attempts[0].ConsumedAttempts != 0 ||
		trace.Commands[0].Attempts[1].Classification != "succeeded" {
		t.Fatalf("shutdown Trace = %#v", trace)
	}
}

func TestRuntimeDeadlineAndRegistrationLifecycle(t *testing.T) {
	t.Parallel()

	database := testpg.Open(t)
	ctx := context.Background()
	if err := Migrate(ctx, database.DB, WithSchema(database.Schema)); err != nil {
		t.Fatalf("Migrate() error = %v", err)
	}
	if _, err := New(database.DB, WithSchema(database.Schema), WithQueueConcurrency("", 1)); !errors.Is(err, ErrInvalid) {
		t.Fatalf("New(invalid queue concurrency) error = %v", err)
	}
	if _, err := New(database.DB, WithSchema(database.Schema),
		WithQueueConcurrency("duplicate", 1), WithQueueConcurrency("duplicate", 2)); !errors.Is(err, ErrInvalid) {
		t.Fatalf("New(duplicate queue concurrency) error = %v", err)
	}
	command := DefineCommand[runtimeArgs, runtimeResult]("runtime.unhandled", 1)
	runtime, err := New(database.DB, WithSchema(database.Schema), WithPollInterval(5*time.Millisecond),
		withCommandLeaseForTest(90*time.Millisecond), WithShutdownGrace(time.Second))
	if err != nil {
		t.Fatalf("New() error = %v", err)
	}
	if err := runtime.Register(Handle(command, nil)); !errors.Is(err, ErrInvalid) {
		t.Fatalf("Register(invalid) error = %v", err)
	}
	worker := Handle(command, func(context.Context, *Work[runtimeArgs]) (runtimeResult, error) {
		return runtimeResult{}, nil
	})
	if err := runtime.Register(worker); err != nil {
		t.Fatalf("Register() error = %v", err)
	}
	if err := runtime.Register(worker); !errors.Is(err, ErrConflict) {
		t.Fatalf("Register(duplicate) error = %v", err)
	}
	// Use a different, deliberately unregistered definition so only deadline
	// maintenance can progress it.
	unhandled := DefineCommand[runtimeArgs, runtimeResult]("runtime.deadline", 1)
	cancelRun, runResult := startRuntime(t, runtime)
	if err := runtime.Register(worker); !errors.Is(err, ErrInvalidState) {
		t.Fatalf("Register(after Run) error = %v", err)
	}
	exec, err := unhandled.With(runtime).Execute(ctx, "deadline", runtimeArgs{}, WithExecutionDeadline(40*time.Millisecond))
	if err != nil {
		t.Fatalf("Execute(deadline) error = %v", err)
	}
	waitForExecutionStatus(t, database.Schema, database.DB.Conn, exec.ID, "expired", 5*time.Second)
	stopRuntime(t, cancelRun, runResult)
	if err := runtime.Run(ctx); !errors.Is(err, ErrInvalidState) {
		t.Fatalf("Run(second) error = %v", err)
	}
	if _, err := unhandled.With(runtime).Execute(ctx, "closed", runtimeArgs{}); !errors.Is(err, ErrClosed) {
		t.Fatalf("Execute(stopped runtime) error = %v", err)
	}
}

func startRuntime(t *testing.T, runtime *Runtime) (context.CancelFunc, <-chan error) {
	t.Helper()
	ctx, cancel := context.WithCancel(context.Background())
	result := make(chan error, 1)
	go func() { result <- runtime.Run(ctx) }()
	deadline := time.Now().Add(2 * time.Second)
	for time.Now().Before(deadline) {
		runtime.mu.RLock()
		running := runtime.lifecycle == runtimeRunning
		runtime.mu.RUnlock()
		if running {
			return cancel, result
		}
		time.Sleep(time.Millisecond)
	}
	cancel()
	t.Fatal("runtime did not enter running state")
	return nil, nil
}

func stopRuntime(t *testing.T, cancel context.CancelFunc, result <-chan error) {
	t.Helper()
	cancel()
	select {
	case err := <-result:
		if err != nil {
			t.Fatalf("Run() error = %v", err)
		}
	case <-time.After(3 * time.Second):
		t.Fatal("runtime did not stop")
	}
}

func waitForCount(t *testing.T, value *atomic.Int32, want int32, timeout time.Duration) {
	t.Helper()
	deadline := time.Now().Add(timeout)
	for time.Now().Before(deadline) {
		if value.Load() >= want {
			return
		}
		time.Sleep(5 * time.Millisecond)
	}
	t.Fatalf("count = %d, want at least %d", value.Load(), want)
}

type runtimeResult struct {
	Value string `json:"value"`
}

func TestRuntimeExecutesDirectCommand(t *testing.T) {
	t.Parallel()

	database := testpg.Open(t)
	ctx := context.Background()
	if err := Migrate(ctx, database.DB, WithSchema(database.Schema)); err != nil {
		t.Fatalf("Migrate() error = %v", err)
	}
	var calls atomic.Int32
	seenInfo := make(chan CommandInfo, 1)
	command := DefineCommand[runtimeArgs, runtimeResult]("runtime.direct", 1)
	runtime, err := New(database.DB, WithSchema(database.Schema), WithWorkerConcurrency(2),
		WithPollInterval(10*time.Millisecond), withCommandLeaseForTest(300*time.Millisecond), WithShutdownGrace(time.Second))
	if err != nil {
		t.Fatalf("New() error = %v", err)
	}
	if err := runtime.Register(Handle(command, func(_ context.Context, work *Work[runtimeArgs]) (runtimeResult, error) {
		calls.Add(1)
		seenInfo <- work.Info()
		return runtimeResult{Value: "done:" + work.Args.Value}, nil
	})); err != nil {
		t.Fatalf("Register() error = %v", err)
	}
	runCtx, cancelRun := context.WithCancel(context.Background())
	runResult := make(chan error, 1)
	go func() { runResult <- runtime.Run(runCtx) }()

	exec, err := command.With(runtime).Execute(ctx, "direct", runtimeArgs{Value: "work"})
	if err != nil {
		t.Fatalf("Execute() error = %v", err)
	}
	waitForExecutionStatus(t, database.Schema, database.DB.Conn, exec.ID, "succeeded", 5*time.Second)
	if calls.Load() != 1 {
		t.Fatalf("worker calls = %d", calls.Load())
	}
	select {
	case info := <-seenInfo:
		if info.ExecutionID != exec.ID || info.CommandID != exec.RootCommandID || info.Attempt != 1 ||
			info.BudgetStartedAt.IsZero() || info.AttemptStartedAt.IsZero() {
			t.Fatalf("CommandInfo = %#v", info)
		}
	default:
		t.Fatal("worker did not report CommandInfo")
	}
	var commandState string
	var result []byte
	var queueCount int
	if err := database.DB.Conn.QueryRow(ctx, `SELECT state,result FROM `+pgschema.Table(database.Schema, "flow_commands")+`
		WHERE command_id=$1`, exec.RootCommandID).Scan(&commandState, &result); err != nil {
		t.Fatalf("read command projection: %v", err)
	}
	if err := database.DB.Conn.QueryRow(ctx, `SELECT count(*) FROM `+pgschema.Table(database.Schema, "flow_command_queue")+`
		WHERE execution_id=$1`, exec.ID).Scan(&queueCount); err != nil {
		t.Fatalf("count command queue: %v", err)
	}
	if commandState != "succeeded" || string(result) != `{"value":"done:work"}` || queueCount != 0 {
		t.Fatalf("command projection = %s/%s queue=%d", commandState, result, queueCount)
	}
	history, err := History(ctx, runtime, exec.ID)
	if err != nil {
		t.Fatalf("History() error = %v", err)
	}
	if len(history) != 6 || history[2].Kind != HistoryAttemptStarted || history[3].Kind != HistoryAttemptConcluded ||
		history[4].TerminalStatus != "succeeded" || history[5].TerminalStatus != "succeeded" {
		t.Fatalf("History() = %#v", history)
	}
	trace, err := Trace(ctx, runtime, exec.ID)
	if err != nil {
		t.Fatalf("Trace() error = %v", err)
	}
	if trace.Execution.Status != "succeeded" || len(trace.Commands) != 1 || trace.Commands[0].State != "succeeded" ||
		len(trace.Commands[0].Attempts) != 1 || trace.Commands[0].Attempts[0].Classification != "succeeded" ||
		string(trace.Commands[0].Result) != `{"value":"done:work"}` {
		t.Fatalf("Trace() = %#v", trace)
	}

	cancelRun()
	select {
	case err := <-runResult:
		if err != nil {
			t.Fatalf("Run() error = %v", err)
		}
	case <-time.After(3 * time.Second):
		t.Fatal("Run() did not stop")
	}
}

func waitForExecutionStatus(t *testing.T, schema string, db queryRower, id ExecutionID, want string, timeout time.Duration) {
	t.Helper()
	deadline := time.Now().Add(timeout)
	last := "<unreadable>"
	for time.Now().Before(deadline) {
		var status string
		if err := db.QueryRow(context.Background(), `SELECT status FROM `+pgschema.Table(schema, "flow_executions")+`
			WHERE execution_id=$1`, id).Scan(&status); err == nil {
			last = status
			if status == want {
				return
			}
		}
		time.Sleep(10 * time.Millisecond)
	}
	t.Fatalf("execution %s did not reach %s (last status %s)", id, want, last)
}
