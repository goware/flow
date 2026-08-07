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

func TestExecutionInspectionAndStablePagination(t *testing.T) {
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

	var execs []Execution
	for index := 0; index < 5; index++ {
		exec, err := command.With(runtime).Execute(ctx, fmt.Sprintf("batch/%02d", index), inspectionArgs{Value: fmt.Sprint(index)},
			WithMetadata(map[string]string{"tenant": "acme", "bucket": fmt.Sprint(index % 2)}))
		if err != nil {
			t.Fatalf("Execute(%d) error = %v", index, err)
		}
		execs = append(execs, exec)
	}

	got, err := GetExecution(ctx, runtime, execs[2].ID)
	if err != nil {
		t.Fatalf("GetExecution() error = %v", err)
	}
	var metadata map[string]string
	if err := json.Unmarshal(got.Metadata, &metadata); err != nil {
		t.Fatalf("decode metadata: %v", err)
	}
	if got.ID != execs[2].ID || got.Type != command.Name() || got.Status != "running" ||
		got.CommandCount != 1 || got.OpenCommands != 1 || metadata["bucket"] != "0" || metadata["tenant"] != "acme" {
		t.Fatalf("GetExecution() = %#v", got)
	}
	if _, err := GetExecution(ctx, runtime, ExecutionID("00000000-0000-0000-0000-000000000001")); !errors.Is(err, ErrNotFound) {
		t.Fatalf("GetExecution(missing) error = %v", err)
	}

	filter := ExecutionFilter{Type: command.Name(), KeyPrefix: "batch/", Metadata: map[string]string{"tenant": "acme"}, PageSize: 2}
	var listed []Execution
	for {
		page, err := ListExecutions(ctx, runtime, filter)
		if err != nil {
			t.Fatalf("ListExecutions() error = %v", err)
		}
		listed = append(listed, page.Executions...)
		if page.NextCursor == "" {
			break
		}
		filter.Cursor = page.NextCursor
	}
	if len(listed) != 5 {
		t.Fatalf("listed %d executions, want 5", len(listed))
	}
	seen := make(map[ExecutionID]struct{}, len(listed))
	for index, execution := range listed {
		if _, duplicate := seen[execution.ID]; duplicate {
			t.Fatalf("duplicate paged execution %s", execution.ID)
		}
		seen[execution.ID] = struct{}{}
		if index > 0 && listed[index-1].CreatedAt.Before(execution.CreatedAt) {
			t.Fatalf("pagination order increased at %d", index)
		}
	}
	filtered, err := ListExecutions(ctx, runtime, ExecutionFilter{
		Type: command.Name(), Metadata: map[string]string{"bucket": "1"}, Statuses: []ExecutionStatus{ExecutionStatusRunning}, PageSize: 10,
	})
	if err != nil || len(filtered.Executions) != 2 {
		t.Fatalf("filtered list = %#v, %v", filtered, err)
	}
	literalWildcard, err := ListExecutions(ctx, runtime, ExecutionFilter{Type: command.Name(), KeyPrefix: "batch/%", PageSize: 10})
	if err != nil || len(literalWildcard.Executions) != 0 {
		t.Fatalf("literal wildcard prefix list = %#v, %v", literalWildcard, err)
	}
	if _, err := ListExecutions(ctx, runtime, ExecutionFilter{Statuses: []ExecutionStatus{"unknown"}}); !errors.Is(err, ErrInvalid) {
		t.Fatalf("invalid status error = %v", err)
	}
	if _, err := ListExecutions(ctx, runtime, ExecutionFilter{Cursor: "not-a-cursor"}); !errors.Is(err, ErrInvalid) {
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
	uncommitted, err := command.With(txClient).Execute(ctx, "tx/uncommitted", inspectionArgs{Value: "private"})
	if err != nil {
		t.Fatalf("transaction Execute() error = %v", err)
	}
	if _, err := GetExecution(ctx, txClient, uncommitted.ID); err != nil {
		t.Fatalf("transaction GetExecution() error = %v", err)
	}
	if _, err := GetExecution(ctx, runtime, uncommitted.ID); !errors.Is(err, ErrNotFound) {
		t.Fatalf("outside GetExecution(uncommitted) error = %v", err)
	}
	if _, err := AwaitExecution(ctx, txClient, uncommitted.ID); !errors.Is(err, ErrInvalid) {
		t.Fatalf("AwaitExecution(transaction) error = %v", err)
	}
	if err := tx.Rollback(ctx); err != nil {
		t.Fatalf("Rollback() error = %v", err)
	}

	runCtx, cancelRun := context.WithCancel(ctx)
	runDone := make(chan error, 1)
	go func() { runDone <- runtime.Run(runCtx) }()
	exec, err := command.With(runtime).Execute(ctx, "await/terminal", inspectionArgs{Value: "done"})
	if err != nil {
		t.Fatalf("Execute() error = %v", err)
	}
	waitCtx, cancelWait := context.WithTimeout(ctx, 5*time.Second)
	defer cancelWait()
	execution, err := AwaitExecution(waitCtx, runtime, exec.ID)
	if err != nil {
		t.Fatalf("AwaitExecution() error = %v", err)
	}
	if execution.Status != "succeeded" || execution.FinishedAt == nil || execution.OpenCommands != 0 {
		t.Fatalf("AwaitExecution() = %#v", execution)
	}
	trace, err := Trace(ctx, runtime, exec.ID)
	if err != nil {
		t.Fatalf("Trace() error = %v", err)
	}
	if !reflect.DeepEqual(trace.Execution, execution) || len(trace.Events) != 2 || trace.Events[0].Class != "command_terminal" ||
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
