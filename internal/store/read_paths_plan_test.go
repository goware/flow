package store_test

import (
	"context"
	"fmt"
	"regexp"
	"strings"
	"testing"
	"time"

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
	target := flow.DefineCommand[flow.None, flow.None]("store.release_read_target", 1)
	statsTarget := flow.DefineCommand[flow.None, flow.None]("store.release_read_stats_target", 1, flow.WithQueue("release.rare"))
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
	if _, err := statsTarget.Enqueue(ctx, runtime, "release/stats", flow.None{}, flow.WithoutRunDeadline()); err != nil {
		t.Fatalf("create queue statistics target: %v", err)
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
	query, args := store.ActiveCommandListQueryForTest(repository, store.ActiveCommandListFilter{
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
		name: "queue_stats", query: store.QueueStatsQueryForTest(repository),
		args: []any{[]string{"release.rare"}}, wantIndex: "flow_command_queue_stats_idx",
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

func TestPruneCandidateProductionQueryUsesPartialIndexAndBoundsRows(t *testing.T) {
	t.Parallel()

	db, schema, repository := setupStore(t)
	ctx := context.Background()
	tx, err := db.Conn.Begin(ctx)
	if err != nil {
		t.Fatal(err)
	}
	defer tx.Rollback(ctx)
	runs := pgschema.Table(schema, "flow_runs")
	commands := pgschema.Table(schema, "flow_commands")
	if _, err := tx.Exec(ctx, `INSERT INTO `+runs+` (
		run_id,definition_name,definition_version,run_key,key_scope,status,start_fingerprint,
		max_commands,command_count,open_commands,root_command_id,created_at,updated_at,status_at,finished_at
	) SELECT md5('prune-run:'||g)::uuid,'store.prune',1,'permanent/'||g,'permanent','succeeded',
		decode(repeat('00',32),'hex'),1,1,0,md5('prune-command:'||g)::uuid,
		clock_timestamp()-interval '3 hours',clock_timestamp(),clock_timestamp(),clock_timestamp()-interval '2 hours'
	FROM generate_series(1,10000) g`); err != nil {
		t.Fatal(err)
	}
	if _, err := tx.Exec(ctx, `INSERT INTO `+commands+` (
		command_id,run_id,command_key,name,version,args,declaration_fingerprint,state,
		queue,retry_policy,result,terminal_position,created_position,created_at,updated_at,status_at,finished_at
	) SELECT root_command_id,run_id,'root','store.prune',1,'{}'::text::bytea,
		decode(repeat('00',32),'hex'),'succeeded','default','{}'::text::bytea,'{}'::text::bytea,
		1,1,created_at,updated_at,status_at,finished_at FROM `+runs+` WHERE definition_name='store.prune'`); err != nil {
		t.Fatal(err)
	}
	for index := range 3 {
		runID := fmt.Sprintf("00000000-0000-7000-8000-%012d", index+1)
		commandID := fmt.Sprintf("00000000-0000-7000-9000-%012d", index+1)
		if _, err := tx.Exec(ctx, `INSERT INTO `+runs+` (
			run_id,definition_name,definition_version,run_key,key_scope,status,start_fingerprint,
			max_commands,command_count,open_commands,root_command_id,created_at,updated_at,status_at,finished_at
		) VALUES ($1,'store.prune.eligible',1,'','permanent','succeeded',decode(repeat('00',32),'hex'),
			1,1,0,$2,clock_timestamp()-interval '3 hours',clock_timestamp(),clock_timestamp(),
			clock_timestamp()-interval '2 hours'+$3::int*interval '1 second')`, runID, commandID, index); err != nil {
			t.Fatal(err)
		}
		if _, err := tx.Exec(ctx, `INSERT INTO `+commands+` (
			command_id,run_id,command_key,name,version,args,declaration_fingerprint,state,
			queue,retry_policy,result,terminal_position,created_position,created_at,updated_at,status_at,finished_at
		) VALUES ($1,$2,'root','store.prune.eligible',1,'{}'::text::bytea,decode(repeat('00',32),'hex'),
			'succeeded','default','{}'::text::bytea,'{}'::text::bytea,1,1,clock_timestamp(),clock_timestamp(),
			clock_timestamp(),clock_timestamp())`, commandID, runID); err != nil {
			t.Fatal(err)
		}
	}
	if err := tx.Commit(ctx); err != nil {
		t.Fatal(err)
	}
	if _, err := db.Conn.Exec(ctx, `ANALYZE `+runs+`,`+commands); err != nil {
		t.Fatal(err)
	}

	query := store.PruneCandidatesQueryForTest(repository)
	rows, err := db.Conn.Query(ctx, `EXPLAIN (ANALYZE,BUFFERS,FORMAT TEXT) `+query, time.Now().UTC(), 2)
	if err != nil {
		t.Fatal(err)
	}
	lines, err := pgx.CollectRows(rows, pgx.RowTo[string])
	if err != nil {
		t.Fatal(err)
	}
	plan := strings.Join(lines, "\n")
	if !strings.Contains(plan, "flow_runs_prune_idx") ||
		!regexp.MustCompile(`(?m)^Limit .*actual .*rows=2(?:\.00)? loops=1`).MatchString(plan) {
		t.Fatalf("prune candidate plan did not use the partial index for a two-row batch:\n%s", plan)
	}
	result, err := repository.PruneTerminalRuns(ctx, time.Now().UTC(), 2)
	if err != nil || result.Runs != 2 || result.Commands != 2 {
		t.Fatalf("bounded prune result = %#v, %v", result, err)
	}
	var eligible int
	if err := db.Conn.QueryRow(ctx, `SELECT count(*) FROM `+runs+`
		WHERE definition_name='store.prune.eligible'`).Scan(&eligible); err != nil {
		t.Fatal(err)
	}
	if eligible != 1 {
		t.Fatalf("eligible rows after bounded prune = %d, want 1", eligible)
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
	targetRun, err := flow.GetRun(ctx, runtime, target.RunID)
	if err != nil {
		t.Fatal(err)
	}
	filler, err := command.Enqueue(ctx, runtime, "trace/filler", flow.None{}, flow.WithoutRunDeadline())
	if err != nil {
		t.Fatal(err)
	}
	fillerRun, err := flow.GetRun(ctx, runtime, filler.RunID)
	if err != nil {
		t.Fatal(err)
	}
	commands := pgschema.Table(schema, "flow_commands")
	waits := pgschema.Table(schema, "flow_command_event_waits")
	if _, err := db.Conn.Exec(ctx, `INSERT INTO `+waits+`
		(command_id,run_id,event_name,event_key,satisfied_position)
		VALUES ($1,$2,'store.trace.target','target',1)`, targetRun.RootCommandID, target.RunID); err != nil {
		t.Fatal(err)
	}
	if _, err := db.Conn.Exec(ctx, `INSERT INTO `+commands+` (
		command_id,run_id,command_key,name,version,parent_command_id,args,declaration_fingerprint,
		state,unsatisfied_waits,queue,retry_policy,created_position,created_at,updated_at,status_at)
		SELECT md5($1::text||':'||g::text)::uuid,$1::uuid,'trace/sparse/'||g::text,'store.trace.synthetic',1,$2::uuid,
		       convert_to('{}','UTF8'),decode(repeat('00',32),'hex'),'pending',1,'default',convert_to('{}','UTF8'),
		       1,clock_timestamp(),clock_timestamp(),clock_timestamp()
		FROM generate_series(1,10000) AS g`, filler.RunID, fillerRun.RootCommandID); err != nil {
		t.Fatalf("seed unrelated commands: %v", err)
	}
	if _, err := db.Conn.Exec(ctx, `INSERT INTO `+waits+`
		(command_id,run_id,event_name,event_key,satisfied_position)
		SELECT command_id,run_id,'store.trace.unrelated',command_key,1
		FROM `+commands+` WHERE run_id=$1 AND parent_command_id IS NOT NULL`, filler.RunID); err != nil {
		t.Fatalf("seed unrelated waits: %v", err)
	}
	if _, err := db.Conn.Exec(ctx, `ANALYZE `+commands+`, `+waits); err != nil {
		t.Fatal(err)
	}
	rows, err := db.Conn.Query(ctx, `EXPLAIN (ANALYZE,BUFFERS,FORMAT TEXT) `+
		store.TraceWaitsQueryForTest(repository), target.RunID)
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
