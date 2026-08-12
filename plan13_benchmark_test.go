package flow

import (
	"context"
	"crypto/sha256"
	"fmt"
	"testing"
	"time"

	"github.com/goware/flow/internal/pgschema"
	"github.com/goware/flow/internal/testpg"
	"github.com/goware/pgkit/v2"
)

func BenchmarkPermanentKeyRediscovery(b *testing.B) {
	database := testpg.Open(b)
	ctx := context.Background()
	if err := Migrate(ctx, database.DB, WithSchema(database.Schema)); err != nil {
		b.Fatal(err)
	}
	runtime, err := New(database.DB, WithSchema(database.Schema), WithNotifications(false))
	if err != nil {
		b.Fatal(err)
	}
	command := DefineCommand[None, None]("benchmark.rediscovery", 1)
	original, err := command.Enqueue(ctx, runtime, "stable", None{}, WithoutRunDeadline())
	if err != nil {
		b.Fatal(err)
	}

	b.ReportAllocs()
	b.ResetTimer()
	for range b.N {
		result, rediscoverErr := command.Enqueue(ctx, runtime, "stable", None{}, WithoutRunDeadline())
		if rediscoverErr != nil || result.Created || result.RunID != original.RunID {
			b.Fatalf("rediscovery = %#v, %v; want existing %s", result, rediscoverErr, original.RunID)
		}
	}
}

func BenchmarkQueueStats16(b *testing.B) {
	database := testpg.Open(b)
	ctx := context.Background()
	if err := Migrate(ctx, database.DB, WithSchema(database.Schema)); err != nil {
		b.Fatal(err)
	}
	runtime, err := New(database.DB, WithSchema(database.Schema), WithNotifications(false))
	if err != nil {
		b.Fatal(err)
	}
	queues := make([]string, 16)
	for index := range queues {
		queues[index] = fmt.Sprintf("benchmark.stats.%02d", index)
		command := DefineCommand[None, None](fmt.Sprintf("benchmark.stats.command.%02d", index), 1,
			WithQueue(queues[index]))
		if _, err := command.Enqueue(ctx, runtime, fmt.Sprintf("queue/%02d", index), None{}, WithoutRunDeadline()); err != nil {
			b.Fatal(err)
		}
	}

	b.Run("batched", func(b *testing.B) {
		b.ReportAllocs()
		b.ReportMetric(1, "round_trips/op")
		for range b.N {
			stats, err := GetQueueStats(ctx, runtime, queues...)
			if err != nil || len(stats) != len(queues) {
				b.Fatalf("GetQueueStats() lanes=%d, err=%v", len(stats), err)
			}
			for _, queue := range queues {
				if stats[queue].Ready != 1 {
					b.Fatalf("queue %s ready=%d, want 1", queue, stats[queue].Ready)
				}
			}
		}
	})
	b.Run("single_lane_calls", func(b *testing.B) {
		b.ReportAllocs()
		b.ReportMetric(float64(len(queues)), "round_trips/op")
		for range b.N {
			for _, queue := range queues {
				stats, err := GetQueueStats(ctx, runtime, queue)
				if err != nil || stats[queue].Ready != 1 {
					b.Fatalf("GetQueueStats(%s) = %#v, %v", queue, stats[queue], err)
				}
			}
		}
	})
}

func BenchmarkPruneTerminalRuns(b *testing.B) {
	for _, batchSize := range []int{100, 1000} {
		b.Run(fmt.Sprintf("runs_%d", batchSize), func(b *testing.B) {
			database := testpg.Open(b)
			ctx := context.Background()
			if err := Migrate(ctx, database.DB, WithSchema(database.Schema)); err != nil {
				b.Fatal(err)
			}
			runtime, err := New(database.DB, WithSchema(database.Schema), WithNotifications(false))
			if err != nil {
				b.Fatal(err)
			}
			cutoff := time.Now().UTC()

			b.ReportAllocs()
			b.ReportMetric(float64(batchSize), "runs/op")
			b.ResetTimer()
			b.StopTimer()
			for iteration := range b.N {
				seedBenchmarkPruneRuns(b, database.DB, database.Schema, batchSize, iteration, cutoff.Add(-time.Hour))
				b.StartTimer()
				result, pruneErr := PruneTerminalRuns(ctx, runtime, cutoff, batchSize)
				b.StopTimer()
				want := PruneResult{Runs: int64(batchSize), Commands: int64(batchSize), JournalEntries: int64(batchSize * 2)}
				if pruneErr != nil || result != want {
					b.Fatalf("PruneTerminalRuns() = %#v, %v; want %#v", result, pruneErr, want)
				}
			}
		})
	}
}

func seedBenchmarkPruneRuns(
	b *testing.B,
	db *pgkit.DB,
	schema string,
	count int,
	iteration int,
	finishedAt time.Time,
) {
	b.Helper()
	ctx := context.Background()
	tx, err := db.Conn.Begin(ctx)
	if err != nil {
		b.Fatal(err)
	}
	defer func() { _ = tx.Rollback(context.WithoutCancel(ctx)) }()

	seed := fmt.Sprintf("plan13-prune:%d", iteration)
	runs := pgschema.Table(schema, "flow_runs")
	commands := pgschema.Table(schema, "flow_commands")
	journal := pgschema.Table(schema, "flow_journal")
	body := []byte(`{}`)
	bodyHash := sha256.Sum256(body)
	createdAt := finishedAt.Add(-time.Hour)

	if _, err := tx.Exec(ctx, `INSERT INTO `+runs+` (
		run_id,definition_name,definition_version,run_key,key_scope,status,start_fingerprint,
		max_commands,command_count,open_commands,next_journal_position,root_command_id,
		created_at,updated_at,status_at,finished_at
	) SELECT md5($1||':run:'||g)::uuid,'benchmark.prune',1,'','permanent','succeeded',$2,
		1,1,0,3,md5($1||':command:'||g)::uuid,$3,$4,$4,$4
	FROM generate_series(1,$5) AS g`, seed, make([]byte, 32), createdAt, finishedAt, count); err != nil {
		b.Fatal(err)
	}
	if _, err := tx.Exec(ctx, `INSERT INTO `+commands+` (
		command_id,run_id,command_key,name,version,args,declaration_fingerprint,state,
		queue,retry_policy,result,terminal_position,created_position,created_at,updated_at,status_at,finished_at
	) SELECT root_command_id,run_id,'root','benchmark.prune',1,$1,$2,'succeeded',
		'default',$1,$1,2,1,created_at,finished_at,finished_at,finished_at
	FROM `+runs+` WHERE definition_name='benchmark.prune'`, body, make([]byte, 32)); err != nil {
		b.Fatal(err)
	}
	if _, err := tx.Exec(ctx, `INSERT INTO `+journal+` (
		run_id,position,entry_id,entry_kind,recorded_at,command_id,body,body_hash
	) SELECT r.run_id,p.position,md5($1||':entry:'||r.run_id::text||':'||p.position)::uuid,
		p.entry_kind,r.finished_at,CASE WHEN p.position=2 THEN r.root_command_id END,$2,$3
	FROM `+runs+` r
	CROSS JOIN (VALUES (1::bigint,'run_started'),(2::bigint,'command_created')) AS p(position,entry_kind)
	WHERE r.definition_name='benchmark.prune'`, seed, body, bodyHash[:]); err != nil {
		b.Fatal(err)
	}
	if err := tx.Commit(ctx); err != nil {
		b.Fatal(err)
	}
}
