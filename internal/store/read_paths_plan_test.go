package store_test

import (
	"context"
	"fmt"
	"strings"
	"testing"

	"github.com/goware/flow"
	"github.com/goware/flow/internal/pgschema"
	"github.com/goware/flow/internal/store"
	"github.com/jackc/pgx/v5"
)

func TestReleaseReadPathProductionQueriesUsePlannedIndexes(t *testing.T) {
	t.Parallel()

	db, schema, repository := setupStore(t)
	ctx := context.Background()
	runtime, err := flow.New(db, flow.WithSchema(schema), flow.WithNotifications(false))
	if err != nil {
		t.Fatal(err)
	}
	target := flow.DefineCommand[flow.None, flow.None]("store.release_read_target", 1, flow.WithQueue("release.rare"))
	filler := flow.DefineCommand[flow.None, flow.None]("store.release_read_filler", 1)
	for index := range 40 {
		prefix := "other"
		if index < 10 {
			prefix = "needle"
		}
		_, err := target.Enqueue(ctx, runtime, fmt.Sprintf("release/%s/%03d", prefix, index), flow.None{}, flow.WithoutRunDeadline())
		if err != nil {
			t.Fatalf("create target %d: %v", index, err)
		}
	}
	for index := range 400 {
		if _, err := filler.Enqueue(ctx, runtime, fmt.Sprintf("release/filler/%03d", index), flow.None{}, flow.WithoutRunDeadline()); err != nil {
			t.Fatalf("create filler %d: %v", index, err)
		}
	}
	if _, err := db.Conn.Exec(ctx, `ANALYZE `+pgschema.Table(schema, "flow_runs")+`, `+
		pgschema.Table(schema, "flow_commands")+`, `+pgschema.Table(schema, "flow_command_queue")+`, `+
		pgschema.Table(schema, "flow_journal")); err != nil {
		t.Fatal(err)
	}

	type planTest struct {
		name      string
		query     string
		args      []any
		wantIndex string
	}
	var tests []planTest
	query, args := store.LiveWorkListQueryForTest(repository, store.LiveWorkListFilter{
		Keys: []string{"release/needle/000"}, Limit: 11,
	})
	tests = append(tests, planTest{"live_work", query, args, "flow_runs_key_lookup_idx"})
	query, args = store.KeyedHistoryListQueryForTest(repository, store.KeyedHistoryListFilter{
		Keys: []string{"release/needle/000"}, Limit: 11,
	})
	tests = append(tests, planTest{"keyed_history", query, args, "flow_runs_key_lookup_idx"})
	query, args = store.RunListQueryForTest(repository, store.RunListFilter{Limit: 11})
	tests = append(tests, planTest{"default_run_list", query, args, "flow_runs_created_idx"})
	tests = append(tests, planTest{
		name: "queue_depth", query: store.QueueDepthQueryForTest(repository),
		args: []any{"release.rare"}, wantIndex: "flow_command_queue_depth_idx",
	})
	query, args = store.RunListQueryForTest(repository, store.RunListFilter{
		DefinitionName: target.Name(), KeyPrefix: "release/needle/", Limit: 11,
	})
	tests = append(tests, planTest{"typed_prefix", query, args, "flow_runs_key_prefix_idx"})
	for _, test := range tests {
		rows, err := db.Conn.Query(ctx, `EXPLAIN (ANALYZE,BUFFERS,FORMAT TEXT) `+test.query, test.args...)
		if err != nil {
			t.Fatalf("explain %s: %v", test.name, err)
		}
		lines, err := pgx.CollectRows(rows, pgx.RowTo[string])
		if err != nil {
			t.Fatalf("collect %s plan: %v", test.name, err)
		}
		plan := strings.Join(lines, "\n")
		if !strings.Contains(plan, test.wantIndex) {
			t.Fatalf("%s did not use %s:\n%s", test.name, test.wantIndex, plan)
		}
		t.Logf("%s plan:\n%s", test.name, plan)
	}
}

func TestTraceWaitProductionQueryAvoidsUnrelatedSatisfiedWaits(t *testing.T) {
	t.Parallel()

	db, schema, repository := setupStore(t)
	ctx := context.Background()
	runtime, err := flow.New(db, flow.WithSchema(schema), flow.WithNotifications(false))
	if err != nil {
		t.Fatal(err)
	}
	command := flow.DefineCommand[flow.None, flow.None]("store.trace_wait_target", 1)
	target, err := command.Enqueue(ctx, runtime, "trace/target", flow.None{}, flow.WithoutRunDeadline())
	if err != nil {
		t.Fatal(err)
	}
	filler, err := command.Enqueue(ctx, runtime, "trace/filler", flow.None{}, flow.WithoutRunDeadline())
	if err != nil {
		t.Fatal(err)
	}
	commands := pgschema.Table(schema, "flow_commands")
	waits := pgschema.Table(schema, "flow_command_event_waits")
	if _, err := db.Conn.Exec(ctx, `INSERT INTO `+waits+`
		(command_id,run_id,event_name,event_key,satisfied_position)
		VALUES ($1,$2,'store.trace.target','target',1)`, target.RootCommandID, target.ID); err != nil {
		t.Fatal(err)
	}
	if _, err := db.Conn.Exec(ctx, `INSERT INTO `+commands+` (
		command_id,run_id,command_key,name,version,parent_command_id,required,args,declaration_fingerprint,
		state,unsatisfied_waits,queue,retry_policy,created_position,created_at,updated_at,status_at)
		SELECT md5($1::text||':'||g::text)::uuid,$1::uuid,'trace/sparse/'||g::text,'store.trace.synthetic',1,$2::uuid,true,
		       convert_to('{}','UTF8'),decode(repeat('00',32),'hex'),'pending',1,'default',convert_to('{}','UTF8'),
		       1,clock_timestamp(),clock_timestamp(),clock_timestamp()
		FROM generate_series(1,10000) AS g`, filler.ID, filler.RootCommandID); err != nil {
		t.Fatalf("seed unrelated commands: %v", err)
	}
	if _, err := db.Conn.Exec(ctx, `INSERT INTO `+waits+`
		(command_id,run_id,event_name,event_key,satisfied_position)
		SELECT command_id,run_id,'store.trace.unrelated',command_key,1
		FROM `+commands+` WHERE run_id=$1 AND parent_command_id IS NOT NULL`, filler.ID); err != nil {
		t.Fatalf("seed unrelated waits: %v", err)
	}
	if _, err := db.Conn.Exec(ctx, `ANALYZE `+commands+`, `+waits); err != nil {
		t.Fatal(err)
	}
	rows, err := db.Conn.Query(ctx, `EXPLAIN (ANALYZE,BUFFERS,FORMAT TEXT) `+
		store.TraceWaitsQueryForTest(repository), target.ID)
	if err != nil {
		t.Fatal(err)
	}
	lines, err := pgx.CollectRows(rows, pgx.RowTo[string])
	if err != nil {
		t.Fatal(err)
	}
	plan := strings.Join(lines, "\n")
	for _, index := range []string{"flow_commands_run_command_uq", "flow_command_event_waits_pkey"} {
		if !strings.Contains(plan, index) {
			t.Fatalf("trace waits did not use %s:\n%s", index, plan)
		}
	}
	if !strings.Contains(plan, "actual time=") || !strings.Contains(plan, "rows=1") {
		t.Fatalf("trace waits plan did not return the one target row:\n%s", plan)
	}
	t.Logf("trace waits plan:\n%s", plan)
}
