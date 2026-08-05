package flow

import (
	"context"
	"testing"

	"github.com/goware/flow/internal/testpg"
)

// The by-keys read getters are the supported surface for consumers that
// decorate their own domain rows with flow state: a live-keyed execution's
// queued work is addressable by its key through LiveWorkByKeys, its journal
// through HistoryByKeys, and settling the execution removes it from live
// work while its history remains.
func TestReadGettersExposeLiveWorkAndHistory(t *testing.T) {
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

	command := DefineCommand[ingressArgs, ingressResult]("readapi.work", 1)
	started, err := command.With(runtime).Execute(ctx, "txn:42", ingressArgs{Value: "a"}, WithLiveKey())
	if err != nil {
		t.Fatalf("Execute() error = %v", err)
	}
	if !started.Created {
		t.Fatalf("execution not created: %#v", started)
	}

	work, err := LiveWorkByKeys(ctx, runtime, []string{"txn:42", "txn:missing"})
	if err != nil {
		t.Fatalf("LiveWorkByKeys() error = %v", err)
	}
	if len(work) != 1 {
		t.Fatalf("live work rows = %d, want 1 (missing keys contribute none)", len(work))
	}
	row := work[0]
	if row.ExecutionKey != "txn:42" || row.KeyScope != "live" || row.ExecutionStatus != "running" ||
		row.DefinitionName != command.Name() || row.Queue == "" || row.QueueState == "" {
		t.Fatalf("live work row = %#v", row)
	}

	history, err := HistoryByKeys(ctx, runtime, []string{"txn:42"})
	if err != nil {
		t.Fatalf("HistoryByKeys() error = %v", err)
	}
	if len(history) == 0 {
		t.Fatal("history by keys is empty for the started execution")
	}
	if history[0].ExecutionKey != "txn:42" || history[0].DefinitionName != command.Name() ||
		history[0].Kind != HistoryExecutionStarted {
		t.Fatalf("first history entry = %#v", history[0])
	}

	if err := CancelExecution(ctx, runtime, started.ID, "test settle"); err != nil {
		t.Fatalf("CancelExecution() error = %v", err)
	}

	work, err = LiveWorkByKeys(ctx, runtime, []string{"txn:42"})
	if err != nil {
		t.Fatalf("LiveWorkByKeys() after settle error = %v", err)
	}
	if len(work) != 0 {
		t.Fatalf("settled execution still has %d live-work rows", len(work))
	}
	history, err = HistoryByKeys(ctx, runtime, []string{"txn:42"})
	if err != nil {
		t.Fatalf("HistoryByKeys() after settle error = %v", err)
	}
	if len(history) == 0 {
		t.Fatal("history lost after settle")
	}

	if _, err := LiveWorkByKeys(ctx, runtime, make([]string, MaxReadKeys+1)); err == nil {
		t.Fatal("oversized key batch must error")
	}
	if _, err := HistoryByKeys(ctx, runtime, []string{""}); err == nil {
		t.Fatal("empty key must error")
	}
}
