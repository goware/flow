package flow

import (
	"context"
	"encoding/json"
	"errors"
	"fmt"
	"reflect"
	"testing"
	"time"

	"github.com/goware/flow/internal/testpg"
	"github.com/jackc/pgx/v5"
)

type inspectionArgs struct {
	Value string `json:"value"`
}

type inspectionResult struct {
	Value string `json:"value"`
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

	var execs []Run
	for index := 0; index < 5; index++ {
		exec, err := command.Enqueue(ctx, runtime, fmt.Sprintf("batch/%02d", index), inspectionArgs{Value: fmt.Sprint(index)},
			WithMetadata(map[string]string{"tenant": "acme", "bucket": fmt.Sprint(index % 2)}))
		if err != nil {
			t.Fatalf("Enqueue(%d) error = %v", index, err)
		}
		execs = append(execs, exec)
	}

	got, err := GetRun(ctx, runtime, execs[2].ID)
	if err != nil {
		t.Fatalf("GetRun() error = %v", err)
	}
	var metadata map[string]string
	if err := json.Unmarshal(got.Metadata, &metadata); err != nil {
		t.Fatalf("decode metadata: %v", err)
	}
	if got.ID != execs[2].ID || got.Type != command.Name() || got.Status != "running" ||
		got.CommandCount != 1 || got.OpenCommands != 1 || metadata["bucket"] != "0" || metadata["tenant"] != "acme" {
		t.Fatalf("GetRun() = %#v", got)
	}
	if _, err := GetRun(ctx, runtime, RunID("00000000-0000-0000-0000-000000000001")); !errors.Is(err, ErrNotFound) {
		t.Fatalf("GetRun(missing) error = %v", err)
	}

	filter := RunFilter{Type: command.Name(), KeyPrefix: "batch/", Metadata: map[string]string{"tenant": "acme"}, PageSize: 2}
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
		Type: command.Name(), Metadata: map[string]string{"bucket": "1"}, Statuses: []RunStatus{RunStatusRunning}, PageSize: 10,
	})
	if err != nil || len(filtered.Runs) != 2 {
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
	if _, err := GetRun(ctx, txClient, uncommitted.ID); err != nil {
		t.Fatalf("transaction GetRun() error = %v", err)
	}
	if _, err := GetRun(ctx, runtime, uncommitted.ID); !errors.Is(err, ErrNotFound) {
		t.Fatalf("outside GetRun(uncommitted) error = %v", err)
	}
	if _, err := AwaitRun(ctx, txClient, uncommitted.ID); !errors.Is(err, ErrInvalid) {
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
	run, err := AwaitRun(waitCtx, runtime, exec.ID)
	if err != nil {
		t.Fatalf("AwaitRun() error = %v", err)
	}
	if run.Status != "succeeded" || run.FinishedAt == nil || run.OpenCommands != 0 {
		t.Fatalf("AwaitRun() = %#v", run)
	}
	trace, err := Trace(ctx, runtime, exec.ID)
	if err != nil {
		t.Fatalf("Trace() error = %v", err)
	}
	if !reflect.DeepEqual(trace.Run, run) || len(trace.Events) != 2 || trace.Events[0].Class != "command_terminal" ||
		trace.Events[1].Class != "execution_terminal" {
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
