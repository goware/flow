package flow

import (
	"context"
	"fmt"
	"os"
	"strconv"
	"strings"
	"testing"

	"github.com/google/uuid"
	"github.com/goware/flow/internal/pgschema"
	"github.com/goware/flow/internal/testpg"
	"github.com/jackc/pgx/v5/pgxpool"
)

func TestClaimProbeQueryPlan(t *testing.T) {
	if testing.Short() {
		t.Skip("query-plan fixture is an integration benchmark")
	}
	scale := 10_000
	if configured := os.Getenv("FLOW_CLAIM_PLAN_SCALE"); configured != "" {
		value, err := strconv.Atoi(configured)
		if err != nil || value < 10_000 {
			t.Fatalf("FLOW_CLAIM_PLAN_SCALE must be an integer >= 10000")
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
		t.Fatalf("New() error = %v", err)
	}
	plan := DefinePlan[runtimeArgs]("claim.plan.fixture", 1, func(*Plan, runtimeArgs) {})
	handle, err := plan.With(runtime).Execute(ctx, fmt.Sprintf("scale/%d", scale), runtimeArgs{})
	if err != nil {
		t.Fatalf("Plan.Execute() error = %v", err)
	}
	executionID, err := uuid.Parse(string(handle.ID))
	if err != nil {
		t.Fatalf("parse execution ID: %v", err)
	}
	seedClaimPlanRows(t, database.Schema, database.DB.Conn, executionID, scale)
	if _, err := database.DB.Conn.Exec(ctx, `ANALYZE `+pgschema.Table(database.Schema, "flow_command_queue")); err != nil {
		t.Fatalf("ANALYZE queue: %v", err)
	}
	if _, err := database.DB.Conn.Exec(ctx, `ANALYZE `+pgschema.Table(database.Schema, "flow_executions")); err != nil {
		t.Fatalf("ANALYZE executions: %v", err)
	}
	rows, err := database.DB.Conn.Query(ctx, `EXPLAIN (ANALYZE,BUFFERS,WAL,FORMAT TEXT)
		WITH handled(name,version) AS (SELECT * FROM unnest($1::text[],$2::integer[]))
		SELECT q.command_id,q.execution_id,q.queue,q.name,q.version,q.next_run_at
		FROM handled h
		CROSS JOIN LATERAL (
			SELECT candidate.command_id,candidate.execution_id,candidate.queue,candidate.name,candidate.version,candidate.next_run_at
			FROM `+pgschema.Table(database.Schema, "flow_command_queue")+` candidate
			WHERE candidate.name=h.name AND candidate.version=h.version
			  AND candidate.state IN ('ready','retry_wait') AND candidate.next_run_at<=clock_timestamp()
			ORDER BY candidate.next_run_at,candidate.queue,candidate.command_id LIMIT 32
		) q
		JOIN `+pgschema.Table(database.Schema, "flow_executions")+` e ON e.execution_id=q.execution_id
		WHERE e.status IN ('running','failing')
		ORDER BY q.next_run_at,q.queue,q.command_id LIMIT 32`, []string{"claim.plan.handled"}, []int32{1})
	if err != nil {
		t.Fatalf("EXPLAIN claim probe: %v", err)
	}
	defer rows.Close()
	var planLines []string
	for rows.Next() {
		var line string
		if err := rows.Scan(&line); err != nil {
			t.Fatalf("scan EXPLAIN: %v", err)
		}
		planLines = append(planLines, line)
	}
	if err := rows.Err(); err != nil {
		t.Fatalf("read EXPLAIN: %v", err)
	}
	text := strings.Join(planLines, "\n")
	t.Logf("claim probe at %d rows:\n%s", scale, text)
	if !strings.Contains(text, "flow_command_queue_claim_idx") {
		t.Fatalf("claim probe did not use flow_command_queue_claim_idx:\n%s", text)
	}
}

func seedClaimPlanRows(t testing.TB, schema string, db *pgxpool.Pool, executionID uuid.UUID, count int) {
	t.Helper()
	ctx := context.Background()
	queue := pgschema.Table(schema, "flow_command_queue")
	// This is a planner fixture, not a semantic-store fixture. It needs the
	// production queue shape and indexes but not ten million wide command
	// projections. The test schema is private and disposable, so remove the
	// command FK and WAL cost before PostgreSQL generates the hot rows itself.
	if _, err := db.Exec(ctx, `ALTER TABLE `+queue+` SET UNLOGGED`); err != nil {
		t.Fatalf("make queue fixture unlogged: %v", err)
	}
	if _, err := db.Exec(ctx, `ALTER TABLE `+queue+` DROP CONSTRAINT flow_command_queue_command_id_fkey`); err != nil {
		t.Fatalf("drop queue fixture command FK: %v", err)
	}
	commandTag, err := db.Exec(ctx, `INSERT INTO `+queue+`
		(command_id,execution_id,queue,name,version,state,next_run_at,updated_at)
		SELECT md5($1::text || '/' || n::text)::uuid,
		       $1::uuid,
		       'default',
		       CASE WHEN n <= ($2::bigint * 9 / 10)
		            THEN 'claim.plan.unhandled' ELSE 'claim.plan.handled' END,
		       1,
		       'ready',
		       clock_timestamp() - CASE WHEN n <= ($2::bigint * 9 / 10)
		                                THEN interval '2 hours' ELSE interval '1 hour' END,
		       clock_timestamp()
		FROM generate_series(1, $2::bigint) AS rows(n)`, executionID, count)
	if err != nil {
		t.Fatalf("generate queue fixture: %v", err)
	}
	if commandTag.RowsAffected() != int64(count) {
		t.Fatalf("generate queue fixture: inserted=%d want=%d", commandTag.RowsAffected(), count)
	}
	if _, err := db.Exec(ctx, `UPDATE `+pgschema.Table(schema, "flow_executions")+`
		SET command_count=$2,open_commands=$2 WHERE execution_id=$1`, executionID, count); err != nil {
		t.Fatalf("update fixture execution counters: %v", err)
	}
}
