package flow

import (
	"context"
	"fmt"
	"testing"
	"time"

	"github.com/google/uuid"
	"github.com/goware/flow/internal/pgschema"
	"github.com/goware/flow/internal/store"
	"github.com/goware/flow/internal/testpg"
)

// BenchmarkExecutionIngressNotification measures the commit-path cost of the
// optional transactional hint. It intentionally does not run a listener: the
// comparison isolates pg_notify generation/commit from handler scheduling.
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

// BenchmarkCoordinatorSparseOutcomeScan10K records the adversarial idle cost
// of an outcome-only coordinator above a long prefix of unrelated application
// events. The execution ceiling bounds commands, not facts, so this remains a
// useful regression workload even for ordinary M1 executions.
func BenchmarkCoordinatorSparseOutcomeScan10K(b *testing.B) {
	database := testpg.Open(b)
	ctx := context.Background()
	if err := Migrate(ctx, database.DB, WithSchema(database.Schema)); err != nil {
		b.Fatal(err)
	}
	runtime, err := New(database.DB, WithSchema(database.Schema), WithNotifications(false))
	if err != nil {
		b.Fatal(err)
	}
	coordinator := DefineCoordinator[None]("benchmark.coordinator.sparse", 1)
	handle, err := coordinator.With(runtime).Execute(ctx, "sparse", None{}, WithoutExecutionDeadline())
	if err != nil {
		b.Fatal(err)
	}
	executionID, err := uuid.Parse(string(handle.ID))
	if err != nil {
		b.Fatal(err)
	}
	var coordinatorID uuid.UUID
	if err := database.DB.Conn.QueryRow(ctx, `UPDATE `+pgschema.Table(database.Schema, "flow_coordinators")+`
		SET start_pending=false,scan_position=0,delivery_state='idle',delivery_key=NULL,delivery_position=NULL
		WHERE execution_id=$1 RETURNING coordinator_id`, executionID).Scan(&coordinatorID); err != nil {
		b.Fatal(err)
	}
	if _, err := database.DB.Conn.Exec(ctx, `INSERT INTO `+pgschema.Table(database.Schema, "flow_journal")+` (
		execution_id,position,entry_id,entry_kind,recorded_at,event_id,event_namespace,event_name,
		event_key,event_class,body,body_hash)
		SELECT $1::uuid,1+n,md5(($1::uuid)::text || '/entry/' || n)::uuid,'event_recorded',clock_timestamp(),
		       md5(($1::uuid)::text || '/event/' || n)::uuid,'application','benchmark.unrelated','event/' || n,
		       'application','{}'::text::bytea,decode(repeat('00',32),'hex')
		FROM generate_series(1,10000) rows(n)`, executionID); err != nil {
		b.Fatal(err)
	}
	if _, err := database.DB.Conn.Exec(ctx, `UPDATE `+pgschema.Table(database.Schema, "flow_executions")+`
		SET next_journal_position=10002 WHERE execution_id=$1`, executionID); err != nil {
		b.Fatal(err)
	}
	for _, table := range []string{"flow_journal", "flow_commands"} {
		if _, err := database.DB.Conn.Exec(ctx, `ANALYZE `+pgschema.Table(database.Schema, table)); err != nil {
			b.Fatal(err)
		}
	}
	candidate := store.CoordinatorCandidate{CoordinatorID: coordinatorID, ExecutionID: executionID,
		Name: coordinator.Name(), Version: coordinator.Version()}
	selectors := []store.CoordinatorSelector{{Name: "benchmark.never", Version: 1, Outcome: true}}

	b.ResetTimer()
	for range b.N {
		b.StopTimer()
		if _, err := database.DB.Conn.Exec(ctx, `UPDATE `+pgschema.Table(database.Schema, "flow_coordinators")+`
			SET scan_position=0 WHERE coordinator_id=$1`, coordinatorID); err != nil {
			b.Fatal(err)
		}
		b.StartTimer()
		result, err := runtime.store.ClaimCoordinator(ctx, candidate, selectors, time.Minute, "benchmark", nil)
		if err != nil || !result.Progressed || result.Coordinator != nil {
			b.Fatalf("ClaimCoordinator() = %#v, %v", result, err)
		}
	}
}

// BenchmarkInspection100Commands measures the bounded public diagnostic paths
// against one execution with a non-trivial graph-shaped command population.
func BenchmarkInspection100Commands(b *testing.B) {
	for _, operation := range []string{"history", "trace"} {
		b.Run(operation, func(b *testing.B) {
			database := testpg.Open(b)
			ctx := context.Background()
			if err := Migrate(ctx, database.DB, WithSchema(database.Schema)); err != nil {
				b.Fatal(err)
			}
			runtime, err := New(database.DB, WithSchema(database.Schema), WithNotifications(false), WithMaxCommandsPerExecution(0))
			if err != nil {
				b.Fatal(err)
			}
			command := DefineCommand[None, None]("benchmark.inspection.command", 1)
			coordinator := DefineCoordinator[None]("benchmark.inspection.coordinator", 1,
				OnStart(func(_ context.Context, coordination *Coordination[None]) error {
					for index := range 100 {
						Execute(coordination, fmt.Sprintf("work/%03d", index), command, None{}).Optional()
					}
					return nil
				}),
			)
			if err := runtime.Register(coordinator); err != nil {
				b.Fatal(err)
			}
			runCtx, cancel := context.WithCancel(ctx)
			runResult := make(chan error, 1)
			go func() { runResult <- runtime.Run(runCtx) }()
			defer func() {
				cancel()
				<-runResult
			}()
			handle, err := coordinator.With(runtime).Execute(ctx, "inspection", None{}, WithoutExecutionDeadline())
			if err != nil {
				b.Fatal(err)
			}
			deadline := time.Now().Add(3 * time.Second)
			for time.Now().Before(deadline) {
				var count int
				if err := database.DB.Conn.QueryRow(ctx, `SELECT command_count FROM `+
					pgschema.Table(database.Schema, "flow_executions")+` WHERE execution_id=$1`, handle.ID).Scan(&count); err == nil && count == 100 {
					break
				}
				time.Sleep(5 * time.Millisecond)
			}
			b.ResetTimer()
			for range b.N {
				switch operation {
				case "history":
					if _, err := History(ctx, runtime, handle.ID, HistoryLimit(1_000)); err != nil {
						b.Fatal(err)
					}
				case "trace":
					if _, err := Trace(ctx, runtime, handle.ID); err != nil {
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
	runtime, err := New(database.DB, WithSchema(database.Schema), WithNotifications(false),
		WithMaxCommandsPerExecution(0), WithPollInterval(5*time.Millisecond))
	if err != nil {
		t.Fatal(err)
	}
	command := DefineCommand[None, None]("measure.journal.command", 1)
	coordinator := DefineCoordinator[None]("measure.journal.coordinator", 1,
		OnStart(func(_ context.Context, coordination *Coordination[None]) error {
			for index := range 100 {
				Execute(coordination, fmt.Sprintf("work/%03d", index), command, None{}).Optional()
			}
			return nil
		}),
	)
	if err := runtime.Register(coordinator); err != nil {
		t.Fatal(err)
	}
	cancel, result := startRuntime(t, runtime)
	handle, err := coordinator.With(runtime).Execute(ctx, "journal-growth", None{}, WithoutExecutionDeadline())
	if err != nil {
		t.Fatal(err)
	}
	deadline := time.Now().Add(3 * time.Second)
	for time.Now().Before(deadline) {
		var count int
		if err := database.DB.Conn.QueryRow(ctx, `SELECT command_count FROM `+
			pgschema.Table(database.Schema, "flow_executions")+` WHERE execution_id=$1`, handle.ID).Scan(&count); err == nil && count == 100 {
			break
		}
		time.Sleep(5 * time.Millisecond)
	}
	stopRuntime(t, cancel, result)
	var rows int
	var tupleBytes, bodyBytes int64
	if err := database.DB.Conn.QueryRow(ctx, `SELECT count(*),COALESCE(sum(pg_column_size(j)),0),COALESCE(sum(octet_length(body)),0)
		FROM `+pgschema.Table(database.Schema, "flow_journal")+` j WHERE execution_id=$1`, handle.ID).
		Scan(&rows, &tupleBytes, &bodyBytes); err != nil {
		t.Fatal(err)
	}
	if rows != 104 {
		t.Fatalf("journal rows=%d want 104", rows)
	}
	t.Logf("100-command declaration journal: rows=%d tuple_bytes=%d body_bytes=%d tuple_bytes_per_command=%.1f",
		rows, tupleBytes, bodyBytes, float64(tupleBytes)/100)
}
