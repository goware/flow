package flow

import (
	"context"
	"fmt"
	"os"
	"testing"
	"time"

	"github.com/google/uuid"
	"github.com/goware/flow/internal/pgschema"
	"github.com/goware/flow/internal/testpg"
	"github.com/jackc/pgx/v5"
)

func BenchmarkQueueDepth(b *testing.B) {
	rowCounts := []int{1_000, 100_000}
	if os.Getenv("FLOW_BENCHMARK_STRESS") == "1" {
		rowCounts = append(rowCounts, 1_000_000)
	}
	for _, rowCount := range rowCounts {
		for _, selectivity := range []int{1, 10, 100} {
			for _, indexShape := range []string{"none", "queue", "queue_state_time"} {
				name := fmt.Sprintf("rows_%d/selectivity_%d/index_%s", rowCount, selectivity, indexShape)
				b.Run(name, func(b *testing.B) {
					runtime, expected, indexBytes := setupQueueDepthBenchmark(b, rowCount, selectivity, indexShape)
					ctx := context.Background()
					b.ReportAllocs()
					b.ResetTimer()
					for range b.N {
						depth, err := runtime.store.QueueDepthInTx(ctx, nil, "target")
						if err != nil {
							b.Fatal(err)
						}
						if depth.Ready+depth.Delayed+depth.Running != int64(expected) {
							b.Fatalf("queue depth total=%d, want %d", depth.Ready+depth.Delayed+depth.Running, expected)
						}
					}
					b.StopTimer()
					b.ReportMetric(float64(rowCount), "table_rows")
					if indexBytes > 0 {
						b.ReportMetric(float64(indexBytes), "index_bytes")
					}
				})
			}
		}
	}
}

func setupQueueDepthBenchmark(b *testing.B, rowCount int, selectivity int, indexShape string) (*Runtime, int, int64) {
	b.Helper()
	database := testpg.Open(b)
	ctx := context.Background()
	if err := Migrate(ctx, database.DB, WithSchema(database.Schema)); err != nil {
		b.Fatal(err)
	}
	runtime, err := New(database.DB, WithSchema(database.Schema), WithNotifications(false))
	if err != nil {
		b.Fatal(err)
	}
	commandIDs := make([]uuid.UUID, rowCount)
	for index := range commandIDs {
		commandIDs[index] = uuid.New()
	}
	executionID := uuid.New()
	now := time.Now().UTC().Truncate(time.Millisecond)
	targetRows := rowCount * selectivity / 100
	if targetRows == 0 {
		targetRows = 1
	}
	tx, err := database.DB.Conn.Begin(ctx)
	if err != nil {
		b.Fatal(err)
	}
	defer func() { _ = tx.Rollback(context.Background()) }()
	zeroDigest := make([]byte, 32)
	if _, err := tx.Exec(ctx, `INSERT INTO `+pgschema.Table(database.Schema, "flow_executions")+`
		(execution_id,definition_name,definition_version,execution_key,status,fail_fast,
		 start_fingerprint,input,metadata,metadata_canonical,deadline_at,max_commands,
		 command_count,open_commands,next_journal_position,root_command_id,
		 created_at,updated_at,status_at)
		VALUES ($1,'benchmark.queue_depth',1,'','running',true,$2,$3,$4::jsonb,$5,NULL,0,$6,$6,1,$7,$8,$8,$8)`,
		executionID, zeroDigest, []byte(`{}`), `{}`, []byte(`{}`), rowCount, commandIDs[0], now); err != nil {
		b.Fatal(err)
	}
	commandColumns := []string{
		"command_id", "execution_id", "command_key", "name", "version", "parent_command_id", "required",
		"args", "declaration_fingerprint", "state", "unsatisfied_waits", "queue", "retry_policy",
		"budget_started_at", "next_attempt_at", "attempt_ordinal", "consumed_attempts", "created_position",
		"created_at", "updated_at", "status_at",
	}
	commandRows, err := tx.CopyFrom(ctx, pgx.Identifier{database.Schema, "flow_commands"}, commandColumns,
		pgx.CopyFromSlice(rowCount, func(index int) ([]any, error) {
			queue := "other"
			if index < targetRows {
				queue = "target"
			}
			state := "ready"
			var budgetStartedAt, nextAttemptAt any
			attempt := 0
			switch index % 3 {
			case 1:
				state = "retry_wait"
				budgetStartedAt, nextAttemptAt = now, now.Add(time.Hour)
			case 2:
				state, attempt = "running", 1
			}
			return []any{
				commandIDs[index], executionID, fmt.Sprintf("command/%09d", index), "benchmark.queue_depth", 1,
				nil, true, []byte(`{}`), zeroDigest, state, 0, queue, []byte(`{}`), budgetStartedAt,
				nextAttemptAt, attempt, 0, int64(index + 1), now, now, now,
			}, nil
		}))
	if err != nil || commandRows != int64(rowCount) {
		b.Fatalf("copy commands rows=%d, err=%v", commandRows, err)
	}
	queueColumns := []string{
		"command_id", "execution_id", "queue", "name", "version", "state", "next_run_at",
		"active_attempt_id", "lease_token", "lease_owner", "lease_started_at", "lease_expires_at",
	}
	queueRows, err := tx.CopyFrom(ctx, pgx.Identifier{database.Schema, "flow_command_queue"}, queueColumns,
		pgx.CopyFromSlice(rowCount, func(index int) ([]any, error) {
			queue := "other"
			if index < targetRows {
				queue = "target"
			}
			state := "ready"
			nextRunAt := now.Add(-time.Minute)
			var attemptID, token, owner, leaseStartedAt, leaseExpiresAt any
			switch index % 3 {
			case 1:
				state, nextRunAt = "retry_wait", now.Add(time.Hour)
			case 2:
				state = "running"
				attemptID, token, owner = uuid.New(), uuid.New(), "benchmark"
				leaseStartedAt, leaseExpiresAt = now, now.Add(time.Hour)
			}
			return []any{
				commandIDs[index], executionID, queue, "benchmark.queue_depth", 1, state, nextRunAt,
				attemptID, token, owner, leaseStartedAt, leaseExpiresAt,
			}, nil
		}))
	if err != nil || queueRows != int64(rowCount) {
		b.Fatalf("copy queue rows=%d, err=%v", queueRows, err)
	}
	if err := tx.Commit(ctx); err != nil {
		b.Fatal(err)
	}
	var indexColumns string
	switch indexShape {
	case "none":
		return runtime, targetRows, 0
	case "queue":
		indexColumns = "(queue)"
	case "queue_state_time":
		indexColumns = "(queue,state,next_run_at)"
	default:
		b.Fatalf("unknown queue-depth index shape %q", indexShape)
	}
	indexName := pgx.Identifier{"plan6_queue_depth_idx"}.Sanitize()
	if _, err := database.DB.Conn.Exec(ctx, `CREATE INDEX `+indexName+` ON `+
		pgschema.Table(database.Schema, "flow_command_queue")+` `+indexColumns); err != nil {
		b.Fatal(err)
	}
	var indexBytes int64
	if err := database.DB.Conn.QueryRow(ctx, `SELECT pg_relation_size($1::regclass)`,
		database.Schema+".plan6_queue_depth_idx").Scan(&indexBytes); err != nil {
		b.Fatal(err)
	}
	return runtime, targetRows, indexBytes
}

// BenchmarkQueueDepthIndexClaimCost measures the write-side cost of candidate
// queue-depth indexes on the ordinary ready-to-running batch claim transition.
func BenchmarkQueueDepthIndexClaimCost(b *testing.B) {
	const commandCount = 16
	for _, indexShape := range []string{"none", "queue", "queue_state_time"} {
		b.Run(indexShape, func(b *testing.B) {
			runtime, candidates := setupBenchmarkReadyCommands(b, commandCount, "queue_depth_claim_"+indexShape)
			if indexShape != "none" {
				columns := "(queue)"
				if indexShape == "queue_state_time" {
					columns = "(queue,state,next_run_at)"
				}
				if _, err := runtime.db.Conn.Exec(context.Background(), `CREATE INDEX plan6_queue_depth_write_idx ON `+
					pgschema.Table(runtime.schema, "flow_command_queue")+` `+columns); err != nil {
					b.Fatal(err)
				}
			}
			b.ReportAllocs()
			b.ResetTimer()
			b.StopTimer()
			for range b.N {
				b.StartTimer()
				result, err := runtime.store.ClaimCommands(context.Background(), candidates, time.Minute, "benchmark", nil)
				b.StopTimer()
				if err != nil || len(result.Commands) != commandCount {
					b.Fatalf("ClaimCommands() commands=%d, err=%v", len(result.Commands), err)
				}
				resetBenchmarkClaimBatch(b, context.Background(), runtime, result.Commands)
			}
			b.ReportMetric(commandCount, "commands/op")
		})
	}
}
