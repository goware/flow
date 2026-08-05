package flow

import (
	"context"
	"fmt"
	"strings"
	"testing"
	"time"

	"github.com/google/uuid"
	"github.com/goware/flow/internal/canonical"
	"github.com/goware/flow/internal/pgschema"
	"github.com/goware/flow/internal/store"
	"github.com/goware/flow/internal/testpg"
)

func BenchmarkExecutionIngressNotification(b *testing.B) {
	for _, notifications := range []bool{false, true} {
		name := map[bool]string{false: "poll_only", true: "notify"}[notifications]
		b.Run(name, func(b *testing.B) {
			database := testpg.Open(b)
			ctx := context.Background()
			if err := Migrate(ctx, database.DB, WithSchema(database.Schema)); err != nil {
				b.Fatal(err)
			}
			runtime, err := New(database.DB, WithSchema(database.Schema), WithNotifications(notifications))
			if err != nil {
				b.Fatal(err)
			}
			command := DefineCommand[None, None]("benchmark.notification", 1)
			b.ResetTimer()
			for index := range b.N {
				if _, err := command.With(runtime).Execute(ctx, fmt.Sprintf("ingress/%d", index), None{}, WithoutExecutionDeadline()); err != nil {
					b.Fatal(err)
				}
			}
		})
	}
}

// BenchmarkGetEventValueLookup256 measures the worker-time O(1) lookup at the
// maximum declared-input count. No database work occurs in the benchmark.
func BenchmarkGetEventValueLookup256(b *testing.B) {
	event := DefineEvent[int]("benchmark.read_event")
	inputs := make(map[string]eventInputSnapshot, maxCommandEventWaits)
	for index := range maxCommandEventWaits {
		payload, err := canonical.Marshal(index, maxApplicationEventBytes)
		if err != nil {
			b.Fatal(err)
		}
		key := fmt.Sprintf("input/%03d", index)
		inputs[event.Name()+"\x00"+key] = eventInputSnapshot{position: int64(index + 1), payload: payload.BytesCopy()}
	}
	work := &Work[None]{scope: &scopeState{eventInputs: inputs}}
	b.ReportAllocs()
	b.ResetTimer()
	for range b.N {
		value, err := GetEventValue(work, event, "input/255")
		if err != nil || value != 255 {
			b.Fatalf("GetEventValue() = %d, %v", value, err)
		}
	}
}

// BenchmarkEventSnapshotMaterialization256 measures a command claim that
// batches 256 maximum-size event payloads into one immutable worker input
// snapshot. Setup is excluded from the timed region.
func BenchmarkEventSnapshotMaterialization256(b *testing.B) {
	database := testpg.Open(b)
	ctx := context.Background()
	if err := Migrate(ctx, database.DB, WithSchema(database.Schema)); err != nil {
		b.Fatal(err)
	}
	runtime, err := New(database.DB, WithSchema(database.Schema), WithNotifications(false))
	if err != nil {
		b.Fatal(err)
	}
	event := DefineEvent[string]("benchmark.snapshot_event")
	command := DefineCommand[None, None]("benchmark.snapshot_command", 1)
	payload := strings.Repeat("x", maxApplicationEventBytes-2)
	b.ReportAllocs()
	for index := range b.N {
		b.StopTimer()
		opts := make([]ExecutionOption, 0, maxCommandEventWaits+1)
		opts = append(opts, WithoutExecutionDeadline())
		for wait := range maxCommandEventWaits {
			opts = append(opts, WaitFor(event, fmt.Sprintf("input/%03d", wait)))
		}
		exec, err := command.With(runtime).Execute(ctx, fmt.Sprintf("snapshot/%d", index), None{}, opts...)
		if err != nil {
			b.Fatal(err)
		}
		for wait := range maxCommandEventWaits {
			if err := event.Emit(ctx, runtime, exec.ID, fmt.Sprintf("input/%03d", wait), payload); err != nil {
				b.Fatal(err)
			}
		}
		commandID, _ := uuid.Parse(string(exec.RootCommandID))
		executionID, _ := uuid.Parse(string(exec.ID))
		candidate := store.CommandCandidate{CommandID: commandID, ExecutionID: executionID,
			Queue: defaultQueue, Name: command.Name(), Version: command.Version()}
		b.StartTimer()
		result, err := runtime.store.ClaimCommand(ctx, candidate, time.Minute, "benchmark", nil)
		if err != nil || result.Command == nil || len(result.Command.EventInputs) != maxCommandEventWaits {
			count := 0
			if result.Command != nil {
				count = len(result.Command.EventInputs)
			}
			b.Fatalf("ClaimCommand() inputs=%d, err=%v", count, err)
		}
	}
}

func BenchmarkInspection100Commands(b *testing.B) {
	for _, operation := range []string{"history", "trace"} {
		b.Run(operation, func(b *testing.B) {
			database := testpg.Open(b)
			ctx := context.Background()
			if err := Migrate(ctx, database.DB, WithSchema(database.Schema)); err != nil {
				b.Fatal(err)
			}
			runtime, exec, stop := startHundredCommandExecution(b, database, ctx, "inspection")
			defer stop()
			b.ResetTimer()
			for range b.N {
				switch operation {
				case "history":
					if _, err := History(ctx, runtime, exec.ID, HistoryLimit(1_000)); err != nil {
						b.Fatal(err)
					}
				case "trace":
					if _, err := Trace(ctx, runtime, exec.ID); err != nil {
						b.Fatal(err)
					}
				}
			}
		})
	}
}

func TestJournalGrowthMeasurement100Commands(t *testing.T) {
	if testing.Short() {
		t.Skip("journal storage measurement uses PostgreSQL")
	}
	database := testpg.Open(t)
	ctx := context.Background()
	if err := Migrate(ctx, database.DB, WithSchema(database.Schema)); err != nil {
		t.Fatal(err)
	}
	_, exec, stop := startHundredCommandExecution(t, database, ctx, "journal-growth")
	defer stop()
	var rows int
	var tupleBytes, bodyBytes int64
	if err := database.DB.Conn.QueryRow(ctx, `SELECT count(*),COALESCE(sum(pg_column_size(j)),0),COALESCE(sum(octet_length(body)),0)
		FROM `+pgschema.Table(database.Schema, "flow_journal")+` j WHERE execution_id=$1`, exec.ID).
		Scan(&rows, &tupleBytes, &bodyBytes); err != nil {
		t.Fatal(err)
	}
	if rows != 402 {
		t.Fatalf("journal rows=%d want 402", rows)
	}
	t.Logf("100-command journal: rows=%d tuple_bytes=%d body_bytes=%d tuple_bytes_per_command=%.1f",
		rows, tupleBytes, bodyBytes, float64(tupleBytes)/100)
}

type benchmarkTB interface {
	Helper()
	Fatal(...any)
}

func startHundredCommandExecution(tb benchmarkTB, database testpg.Database, ctx context.Context, key string) (*Runtime, Execution, func()) {
	tb.Helper()
	child := DefineCommand[None, None]("benchmark.inspection.child", 1)
	root := DefineCommand[None, None]("benchmark.inspection.root", 1)
	runtime, err := New(database.DB, WithSchema(database.Schema), WithNotifications(false),
		WithMaxCommandsPerExecution(0), WithWorkerConcurrency(16), WithPollInterval(5*time.Millisecond))
	if err != nil {
		tb.Fatal(err)
	}
	if err := runtime.Register(
		Handle(root, func(_ context.Context, work *Work[None]) (None, error) {
			for index := range 99 {
				Execute(work, fmt.Sprintf("work/%03d", index), child, None{})
			}
			return None{}, nil
		}),
		Handle(child, func(context.Context, *Work[None]) (None, error) { return None{}, nil }),
	); err != nil {
		tb.Fatal(err)
	}
	runCtx, cancel := context.WithCancel(ctx)
	runResult := make(chan error, 1)
	go func() { runResult <- runtime.Run(runCtx) }()
	exec, err := root.With(runtime).Execute(ctx, key, None{}, WithoutExecutionDeadline())
	if err != nil {
		cancel()
		tb.Fatal(err)
	}
	deadlineCtx, deadlineCancel := context.WithTimeout(ctx, 10*time.Second)
	settled, err := AwaitExecution(deadlineCtx, runtime, exec.ID)
	deadlineCancel()
	if err != nil || settled.Status != "succeeded" || settled.CommandCount != 100 {
		cancel()
		<-runResult
		tb.Fatal("hundred-command execution failed", err, settled)
	}
	return runtime, exec, func() {
		cancel()
		<-runResult
	}
}
