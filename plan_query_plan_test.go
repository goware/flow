package flow

import (
	"context"
	"os"
	"strconv"
	"strings"
	"testing"

	"github.com/google/uuid"
	"github.com/goware/flow/internal/pgschema"
	"github.com/goware/flow/internal/testpg"
	"github.com/jackc/pgx/v5/pgxpool"
)

func TestDirtyPlanAndEventSnapshotQueryPlans(t *testing.T) {
	if testing.Short() {
		t.Skip("query-plan fixture is an integration benchmark")
	}
	scale := 10_000
	if configured := os.Getenv("FLOW_PLAN_QUERY_SCALE"); configured != "" {
		value, err := strconv.Atoi(configured)
		if err != nil || value < 10_000 {
			t.Fatalf("FLOW_PLAN_QUERY_SCALE must be an integer >= 10000")
		}
		scale = value
	}
	database := testpg.Open(t)
	ctx := context.Background()
	if err := Migrate(ctx, database.DB, WithSchema(database.Schema)); err != nil {
		t.Fatalf("Migrate() error = %v", err)
	}
	runtime, err := New(database.DB, WithSchema(database.Schema), WithMaxCommandsPerExecution(0))
	if err != nil {
		t.Fatal(err)
	}
	plan := DefinePlan[None]("plan.query.fixture", 1, func(*Plan, None) {})
	handle, err := plan.With(runtime).Execute(ctx, "query/base", None{})
	if err != nil {
		t.Fatal(err)
	}
	executionID, err := uuid.Parse(string(handle.ID))
	if err != nil {
		t.Fatal(err)
	}
	seedDirtyPlanExecutions(t, database.Schema, database.DB.Conn, executionID, scale)
	seedPlanEventRows(t, database.Schema, database.DB.Conn, executionID, scale)
	for _, table := range []string{"flow_executions", "flow_journal"} {
		if _, err := database.DB.Conn.Exec(ctx, `ANALYZE `+pgschema.Table(database.Schema, table)); err != nil {
			t.Fatalf("ANALYZE %s: %v", table, err)
		}
	}

	dirtyPlan := explainText(t, database.DB.Conn, `EXPLAIN (ANALYZE,BUFFERS,WAL,FORMAT TEXT)
		WITH handled(name,version) AS (SELECT * FROM unnest($1::text[],$2::integer[]))
		SELECT candidate.execution_id,candidate.definition_name,candidate.definition_version,candidate.plan_dirty_since
		FROM handled h CROSS JOIN LATERAL (
			SELECT e.execution_id,e.definition_name,e.definition_version,e.plan_dirty_since
			FROM `+pgschema.Table(database.Schema, "flow_executions")+` e
			WHERE e.definition_name=h.name AND e.definition_version=h.version
			  AND e.driver_mode='plan' AND e.status IN ('running','failing') AND e.plan_dirty
			ORDER BY e.plan_dirty_since,e.execution_id LIMIT 64
		) candidate
		ORDER BY candidate.plan_dirty_since,candidate.execution_id LIMIT 64`, []string{"plan.query.fixture"}, []int32{1})
	t.Logf("dirty-plan probe at %d rows:\n%s", scale, dirtyPlan)
	if !strings.Contains(dirtyPlan, "flow_executions_plan_queue_idx") {
		t.Fatalf("dirty-plan probe did not use flow_executions_plan_queue_idx:\n%s", dirtyPlan)
	}

	eventPlan := explainText(t, database.DB.Conn, `EXPLAIN (ANALYZE,BUFFERS,WAL,FORMAT TEXT)
		SELECT position,event_namespace,event_name,event_version,COALESCE(event_key,''),body
		FROM `+pgschema.Table(database.Schema, "flow_journal")+`
		WHERE execution_id=$1 AND position<=$2 AND entry_kind='event_recorded'
		  AND event_namespace=$3 AND event_name=$4 AND event_version=$5
		ORDER BY position`, executionID, int64(scale+1), "application", "plan.query.wanted", int32(1))
	t.Logf("exact event snapshot at %d rows:\n%s", scale, eventPlan)
	if !strings.Contains(eventPlan, "flow_journal_event_lookup_idx") {
		t.Fatalf("exact event snapshot did not use flow_journal_event_lookup_idx:\n%s", eventPlan)
	}
}

func explainText(t testing.TB, db *pgxpool.Pool, query string, args ...any) string {
	t.Helper()
	rows, err := db.Query(context.Background(), query, args...)
	if err != nil {
		t.Fatalf("EXPLAIN query: %v", err)
	}
	defer rows.Close()
	var lines []string
	for rows.Next() {
		var line string
		if err := rows.Scan(&line); err != nil {
			t.Fatalf("scan EXPLAIN: %v", err)
		}
		lines = append(lines, line)
	}
	if err := rows.Err(); err != nil {
		t.Fatalf("read EXPLAIN: %v", err)
	}
	return strings.Join(lines, "\n")
}

func seedDirtyPlanExecutions(t testing.TB, schema string, db *pgxpool.Pool, base uuid.UUID, count int) {
	t.Helper()
	if count <= 1 {
		return
	}
	table := pgschema.Table(schema, "flow_executions")
	tag, err := db.Exec(context.Background(), `INSERT INTO `+table+` (
		execution_id,driver_mode,definition_name,definition_version,execution_key,status,fail_fast,
		start_fingerprint,input,input_hash,metadata,metadata_canonical,metadata_hash,deadline_at,max_commands,
		command_count,open_commands,plan_dirty,plan_dirty_since,plan_quiescent,plan_revision,plan_waiting_count,
		plan_waiting_on,next_journal_position,root_command_id,outcome_ref,failure,created_at,updated_at,status_at,finished_at)
	SELECT md5(source.execution_id::text || '/plan/' || n::text)::uuid,source.driver_mode,
		CASE WHEN n <= GREATEST(0,$2::integer-10) THEN 'plan.query.unhandled' ELSE source.definition_name END,
		source.definition_version,'query/' || n::text,source.status,source.fail_fast,source.start_fingerprint,source.input,
		source.input_hash,source.metadata,source.metadata_canonical,source.metadata_hash,source.deadline_at,source.max_commands,
		source.command_count,source.open_commands,true,source.plan_dirty_since + n * interval '1 microsecond',false,
		source.plan_revision,source.plan_waiting_count,source.plan_waiting_on,source.next_journal_position,NULL,
		source.outcome_ref,source.failure,source.created_at,source.updated_at,source.status_at,source.finished_at
	FROM `+table+` source CROSS JOIN generate_series(1,$2::integer-1) rows(n) WHERE source.execution_id=$1`, base, count)
	if err != nil {
		t.Fatalf("seed dirty plans: %v", err)
	}
	if tag.RowsAffected() != int64(count-1) {
		t.Fatalf("seed dirty plans inserted=%d want=%d", tag.RowsAffected(), count-1)
	}
}

func seedPlanEventRows(t testing.TB, schema string, db *pgxpool.Pool, executionID uuid.UUID, count int) {
	t.Helper()
	table := pgschema.Table(schema, "flow_journal")
	tag, err := db.Exec(context.Background(), `INSERT INTO `+table+` (
		execution_id,position,entry_id,entry_kind,recorded_at,event_id,event_namespace,event_name,event_version,
		event_key,event_class,body,body_hash)
	SELECT $1::uuid,n+1,md5($1::text || '/entry/' || n::text)::uuid,'event_recorded',clock_timestamp(),
		md5($1::text || '/event/' || n::text)::uuid,'application',
		CASE WHEN n % 100 = 0 THEN 'plan.query.wanted' ELSE 'plan.query.unrelated' END,1,
		'event/' || n::text,'application','{}'::bytea,decode(repeat('00',32),'hex')
	FROM generate_series(1,$2::integer) rows(n)`, executionID, count)
	if err != nil {
		t.Fatalf("seed plan events: %v", err)
	}
	if tag.RowsAffected() != int64(count) {
		t.Fatalf("seed plan events inserted=%d want=%d", tag.RowsAffected(), count)
	}
	if _, err := db.Exec(context.Background(), `UPDATE `+pgschema.Table(schema, "flow_executions")+`
		SET next_journal_position=$2+2 WHERE execution_id=$1`, executionID, count); err != nil {
		t.Fatalf("advance fixture journal allocator: %v", err)
	}
}
