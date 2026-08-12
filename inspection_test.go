package flow

import (
	"context"
	"errors"
	"fmt"
	"reflect"
	"testing"
	"time"

	"github.com/goware/flow/internal/pgschema"
	"github.com/goware/flow/internal/testpg"
	"github.com/jackc/pgx/v5"
)

type inspectionArgs struct {
	Value string `json:"value"`
}

type inspectionResult struct {
	Value string `json:"value"`
}

func TestGetResultReadsTypedCommandProjection(t *testing.T) {
	t.Parallel()
	database := testpg.Open(t)
	ctx := context.Background()
	if err := Migrate(ctx, database.DB, WithSchema(database.Schema)); err != nil {
		t.Fatalf("Migrate() error = %v", err)
	}
	runtime, err := New(database.DB, WithSchema(database.Schema), WithPollInterval(10*time.Millisecond), WithWorkerConcurrency(2))
	if err != nil {
		t.Fatalf("New() error = %v", err)
	}
	child := DefineCommand[inspectionArgs, inspectionResult]("inspection.result.child", 1)
	root := DefineCommand[inspectionArgs, None]("inspection.result.root", 1)
	failed := DefineCommand[inspectionArgs, inspectionResult]("inspection.result.failed", 1)
	if err := runtime.Register(
		Handle(root, func(_ context.Context, work *Work[inspectionArgs]) (None, error) {
			Enqueue(work, "child", child, work.Args)
			return None{}, nil
		}),
		Handle(child, func(_ context.Context, work *Work[inspectionArgs]) (inspectionResult, error) {
			return inspectionResult{Value: "result/" + work.Args.Value}, nil
		}),
		Handle(failed, func(context.Context, *Work[inspectionArgs]) (inspectionResult, error) {
			return inspectionResult{}, Permanent(errors.New("expected failure"))
		}),
	); err != nil {
		t.Fatalf("Register() error = %v", err)
	}

	run, err := root.Enqueue(ctx, runtime, "result/read", inspectionArgs{Value: "typed"})
	if err != nil {
		t.Fatalf("Enqueue() error = %v", err)
	}
	if result, found, err := GetResult(ctx, runtime, run.RunID, "root", root); err != nil || found || result != (None{}) {
		t.Fatalf("GetResult(pending root) = %#v, %t, %v", result, found, err)
	}
	if result, found, err := root.GetResult(ctx, runtime, run.RunID, "root"); err != nil || found || result != (None{}) {
		t.Fatalf("Command.GetResult(pending root) = %#v, %t, %v", result, found, err)
	}
	if _, found, err := GetResult(ctx, runtime, run.RunID, "missing", child); err != nil || found {
		t.Fatalf("GetResult(missing command) found=%t error=%v", found, err)
	}
	wrongVersion := DefineCommand[inspectionArgs, None](root.Name(), root.Version()+1)
	if _, _, err := GetResult(ctx, runtime, run.RunID, "root", wrongVersion); !errors.Is(err, ErrConflict) {
		t.Fatalf("GetResult(mismatched definition) error = %v", err)
	}
	if _, _, err := GetResult(ctx, runtime, RunID("00000000-0000-0000-0000-000000000001"), "root", root); !errors.Is(err, ErrNotFound) {
		t.Fatalf("GetResult(missing run) error = %v", err)
	}
	invalid := DefineCommand[inspectionArgs, None]("", 1)
	if _, _, err := GetResult(ctx, runtime, run.RunID, "root", invalid); !errors.Is(err, ErrInvalid) {
		t.Fatalf("GetResult(invalid command) error = %v", err)
	}
	if _, _, err := GetResult(ctx, runtime, run.RunID, " invalid ", root); !errors.Is(err, ErrInvalid) {
		t.Fatalf("GetResult(invalid key) error = %v", err)
	}

	runCtx, cancelRun := context.WithCancel(ctx)
	runDone := make(chan error, 1)
	go func() { runDone <- runtime.Run(runCtx) }()
	t.Cleanup(func() {
		cancelRun()
		select {
		case err := <-runDone:
			if err != nil && !errors.Is(err, context.Canceled) {
				t.Errorf("Run() error = %v", err)
			}
		case <-time.After(5 * time.Second):
			t.Error("Run() did not stop")
		}
	})
	waitCtx, cancelWait := context.WithTimeout(ctx, 5*time.Second)
	defer cancelWait()
	settled, err := AwaitRun(waitCtx, runtime, run.RunID)
	if err != nil || settled.Status != RunStatusSucceeded {
		t.Fatalf("AwaitRun() = %#v, %v", settled, err)
	}
	result, found, err := GetResult(ctx, runtime, run.RunID, "child", child)
	if err != nil || !found || result != (inspectionResult{Value: "result/typed"}) {
		t.Fatalf("GetResult(child) = %#v, %t, %v", result, found, err)
	}
	methodResult, methodFound, methodErr := child.GetResult(ctx, runtime, run.RunID, "child")
	if methodErr != nil || !methodFound || methodResult != result {
		t.Fatalf("Command.GetResult(child) = %#v, %t, %v", methodResult, methodFound, methodErr)
	}
	if _, found, err := GetResult(ctx, runtime, run.RunID, "root", root); err != nil || !found {
		t.Fatalf("GetResult(root) found=%t error=%v", found, err)
	}

	failedRun, err := failed.Enqueue(ctx, runtime, "result/failed", inspectionArgs{Value: "failed"})
	if err != nil {
		t.Fatalf("Enqueue(failed) error = %v", err)
	}
	failedWaitCtx, cancelFailedWait := context.WithTimeout(ctx, 5*time.Second)
	defer cancelFailedWait()
	failedState, err := AwaitRun(failedWaitCtx, runtime, failedRun.RunID)
	if err != nil || failedState.Status != RunStatusFailed {
		t.Fatalf("AwaitRun(failed) = %#v, %v", failedState, err)
	}
	if _, found, err := GetResult(ctx, runtime, failedRun.RunID, "root", failed); err != nil || found {
		t.Fatalf("GetResult(failed) found=%t error=%v", found, err)
	}

	if _, err := database.DB.Conn.Exec(ctx, `UPDATE `+pgschema.Table(database.Schema, "flow_commands")+` SET result=$3
		WHERE run_id=$1 AND command_key=$2`, run.RunID, "child", []byte(`{"value":`)); err != nil {
		t.Fatalf("corrupt command result: %v", err)
	}
	if _, _, err := GetResult(ctx, runtime, run.RunID, "child", child); !errors.Is(err, ErrInvalidState) {
		t.Fatalf("GetResult(corrupt result) error = %v", err)
	}
}

func TestRunInspectionAndStablePagination(t *testing.T) {
	t.Parallel()
	database := testpg.Open(t)
	ctx := context.Background()
	if err := Migrate(ctx, database.DB, WithSchema(database.Schema)); err != nil {
		t.Fatalf("Migrate() error = %v", err)
	}
	runtime, err := New(database.DB, WithSchema(database.Schema))
	if err != nil {
		t.Fatalf("New() error = %v", err)
	}
	command := DefineCommand[inspectionArgs, inspectionResult]("inspection.work", 1)

	var execs []EnqueueResult
	for index := 0; index < 5; index++ {
		exec, err := command.Enqueue(ctx, runtime, fmt.Sprintf("batch/%02d", index), inspectionArgs{Value: fmt.Sprint(index)})
		if err != nil {
			t.Fatalf("Enqueue(%d) error = %v", index, err)
		}
		execs = append(execs, exec)
	}

	got, err := GetRun(ctx, runtime, execs[2].RunID)
	if err != nil {
		t.Fatalf("GetRun() error = %v", err)
	}
	if got.ID != execs[2].RunID || got.Type != command.Name() || got.Status != "running" ||
		got.CommandCount != 1 || got.OpenCommands != 1 {
		t.Fatalf("GetRun() = %#v", got)
	}
	if _, err := GetRun(ctx, runtime, RunID("00000000-0000-0000-0000-000000000001")); !errors.Is(err, ErrNotFound) {
		t.Fatalf("GetRun(missing) error = %v", err)
	}

	filter := RunFilter{Type: command.Name(), KeyPrefix: "batch/", PageSize: 2}
	var listed []Run
	for {
		page, err := ListRuns(ctx, runtime, filter)
		if err != nil {
			t.Fatalf("ListRuns() error = %v", err)
		}
		listed = append(listed, page.Runs...)
		if page.NextCursor == "" {
			break
		}
		filter.Cursor = page.NextCursor
	}
	if len(listed) != 5 {
		t.Fatalf("listed %d runs, want 5", len(listed))
	}
	seen := make(map[RunID]struct{}, len(listed))
	for index, run := range listed {
		if _, duplicate := seen[run.ID]; duplicate {
			t.Fatalf("duplicate paged run %s", run.ID)
		}
		seen[run.ID] = struct{}{}
		if index > 0 && listed[index-1].CreatedAt.Before(run.CreatedAt) {
			t.Fatalf("pagination order increased at %d", index)
		}
	}
	filtered, err := ListRuns(ctx, runtime, RunFilter{
		Type: command.Name(), Statuses: []RunStatus{RunStatusRunning}, PageSize: 10,
	})
	if err != nil || len(filtered.Runs) != 5 {
		t.Fatalf("filtered list = %#v, %v", filtered, err)
	}
	literalWildcard, err := ListRuns(ctx, runtime, RunFilter{Type: command.Name(), KeyPrefix: "batch/%", PageSize: 10})
	if err != nil || len(literalWildcard.Runs) != 0 {
		t.Fatalf("literal wildcard prefix list = %#v, %v", literalWildcard, err)
	}
	if _, err := ListRuns(ctx, runtime, RunFilter{Statuses: []RunStatus{"unknown"}}); !errors.Is(err, ErrInvalid) {
		t.Fatalf("invalid status error = %v", err)
	}
	if _, err := ListRuns(ctx, runtime, RunFilter{Cursor: "not-a-cursor"}); !errors.Is(err, ErrInvalid) {
		t.Fatalf("invalid cursor error = %v", err)
	}

}

func TestTransactionScopedInspectionAndAwait(t *testing.T) {
	t.Parallel()
	database := testpg.Open(t)
	ctx := context.Background()
	if err := Migrate(ctx, database.DB, WithSchema(database.Schema)); err != nil {
		t.Fatalf("Migrate() error = %v", err)
	}
	runtime, err := New(database.DB, WithSchema(database.Schema), WithPollInterval(10*time.Millisecond), WithWorkerConcurrency(1))
	if err != nil {
		t.Fatalf("New() error = %v", err)
	}
	command := DefineCommand[inspectionArgs, inspectionResult]("inspection.await", 1)
	if err := runtime.Register(Handle(command, func(_ context.Context, work *Work[inspectionArgs]) (inspectionResult, error) {
		return inspectionResult{Value: work.Args.Value}, nil
	})); err != nil {
		t.Fatalf("Register() error = %v", err)
	}

	tx, err := database.DB.Conn.BeginTx(ctx, pgx.TxOptions{})
	if err != nil {
		t.Fatalf("BeginTx() error = %v", err)
	}
	txClient := runtime.InTx(tx)
	uncommitted, err := command.Enqueue(ctx, txClient, "tx/uncommitted", inspectionArgs{Value: "private"})
	if err != nil {
		t.Fatalf("transaction Enqueue() error = %v", err)
	}
	if _, err := GetRun(ctx, txClient, uncommitted.RunID); err != nil {
		t.Fatalf("transaction GetRun() error = %v", err)
	}
	if _, found, err := GetResult(ctx, txClient, uncommitted.RunID, "root", command); err != nil || found {
		t.Fatalf("transaction GetResult() found=%t error=%v", found, err)
	}
	if _, found, err := command.GetResult(ctx, txClient, uncommitted.RunID, "root"); err != nil || found {
		t.Fatalf("transaction Command.GetResult() found=%t error=%v", found, err)
	}
	if _, _, err := GetResult(ctx, runtime, uncommitted.RunID, "root", command); !errors.Is(err, ErrNotFound) {
		t.Fatalf("outside GetResult(uncommitted) error = %v", err)
	}
	if _, err := GetRun(ctx, runtime, uncommitted.RunID); !errors.Is(err, ErrNotFound) {
		t.Fatalf("outside GetRun(uncommitted) error = %v", err)
	}
	if _, err := AwaitRun(ctx, txClient, uncommitted.RunID); !errors.Is(err, ErrInvalid) {
		t.Fatalf("AwaitRun(transaction) error = %v", err)
	}
	if err := tx.Rollback(ctx); err != nil {
		t.Fatalf("Rollback() error = %v", err)
	}

	runCtx, cancelRun := context.WithCancel(ctx)
	runDone := make(chan error, 1)
	go func() { runDone <- runtime.Run(runCtx) }()
	exec, err := command.Enqueue(ctx, runtime, "await/terminal", inspectionArgs{Value: "done"})
	if err != nil {
		t.Fatalf("Enqueue() error = %v", err)
	}
	waitCtx, cancelWait := context.WithTimeout(ctx, 5*time.Second)
	defer cancelWait()
	run, err := AwaitRun(waitCtx, runtime, exec.RunID)
	if err != nil {
		t.Fatalf("AwaitRun() error = %v", err)
	}
	if run.Status != "succeeded" || run.FinishedAt == nil || run.OpenCommands != 0 {
		t.Fatalf("AwaitRun() = %#v", run)
	}
	trace, err := Trace(ctx, runtime, exec.RunID)
	if err != nil {
		t.Fatalf("Trace() error = %v", err)
	}
	if !reflect.DeepEqual(trace.Run, run) || len(trace.Events) != 2 || trace.Events[0].Class != "command_terminal" ||
		trace.Events[1].Class != "run_terminal" {
		t.Fatalf("Trace() inspection = %#v", trace)
	}
	cancelRun()
	select {
	case err := <-runDone:
		if err != nil && !errors.Is(err, context.Canceled) {
			t.Fatalf("Run() error = %v", err)
		}
	case <-time.After(5 * time.Second):
		t.Fatal("Run() did not stop")
	}
}
