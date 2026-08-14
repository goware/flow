package store_test

import (
	"context"
	"crypto/sha256"
	"fmt"
	"strings"
	"testing"
	"time"

	flow "github.com/goware/flow"
	"github.com/goware/flow/internal/pgschema"
	"github.com/goware/flow/internal/store"
	"github.com/goware/flow/internal/testpg"
)

func TestEventWatchSparsePostCursorPlan(t *testing.T) {
	database := testpg.Open(t)
	ctx := context.Background()
	if err := flow.Migrate(ctx, database.DB, flow.WithSchema(database.Schema)); err != nil {
		t.Fatal(err)
	}
	runtime, err := flow.New(database.DB, flow.WithSchema(database.Schema), flow.WithNotifications(false))
	if err != nil {
		t.Fatal(err)
	}
	repository, err := store.New(database.DB, database.Schema, false)
	if err != nil {
		t.Fatal(err)
	}
	root := flow.DefineCommand[flow.None, flow.None]("watch.plan.root", 1)
	body := []byte(`{"payload":{},"v":1}`)
	digest := sha256.Sum256(body)
	for _, count := range []int{100, 1000, 10000} {
		t.Run(fmt.Sprint(count), func(t *testing.T) {
			run, enqueueErr := root.Enqueue(ctx, runtime, fmt.Sprintf("watch/plan/%d", count), flow.None{}, flow.WithStartDelay(time.Hour))
			if enqueueErr != nil {
				t.Fatal(enqueueErr)
			}
			if _, insertErr := database.DB.Conn.Exec(ctx, `INSERT INTO `+pgschema.Table(database.Schema, "flow_journal")+` (
				run_id,position,entry_id,entry_kind,recorded_at,event_id,event_namespace,event_name,event_key,event_class,body,body_hash
			) SELECT $1::uuid,series+2,md5(($1::uuid)::text||'/entry/'||series)::uuid,'event_recorded',clock_timestamp(),
				md5(($1::uuid)::text||'/event/'||series)::uuid,'application',
				CASE WHEN series=$2::integer THEN 'watch.plan.target' ELSE 'watch.plan.noise' END,
				'event/'||series,'application',$3,$4
			FROM generate_series(1,$2::integer) AS series`, run.RunID, count, body, digest[:]); insertErr != nil {
				t.Fatal(insertErr)
			}
			if _, updateErr := database.DB.Conn.Exec(ctx, `UPDATE `+pgschema.Table(database.Schema, "flow_runs")+`
				SET next_journal_position=$2::bigint+3 WHERE run_id=$1`, run.RunID, count); updateErr != nil {
				t.Fatal(updateErr)
			}
			rows, explainErr := database.DB.Conn.Query(ctx, `EXPLAIN (ANALYZE,BUFFERS,COSTS OFF) `+
				store.EventWatchReadQueryForTest(repository), run.RunID, int64(2), "watch.plan.target")
			if explainErr != nil {
				t.Fatal(explainErr)
			}
			var lines []string
			for rows.Next() {
				var line string
				if err := rows.Scan(&line); err != nil {
					rows.Close()
					t.Fatal(err)
				}
				lines = append(lines, line)
			}
			if err := rows.Err(); err != nil {
				rows.Close()
				t.Fatal(err)
			}
			rows.Close()
			plan := strings.Join(lines, "\n")
			if !strings.Contains(plan, "flow_journal_pkey") || strings.Contains(plan, "Seq Scan on flow_journal") {
				t.Fatalf("event-watch plan is not cursor-indexed:\n%s", plan)
			}
			t.Logf("%d post-cursor rows:\n%s", count, plan)
		})
	}
}
