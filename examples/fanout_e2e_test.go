package examples_test

import (
	"bytes"
	"context"
	"strings"
	"testing"

	"github.com/goware/flow"
	flowscenarios "github.com/goware/flow/internal/examples"
	"github.com/goware/flow/internal/pgschema"
	"github.com/goware/flow/internal/testpg"
)

func TestFanOutExampleEndToEnd(t *testing.T) {
	t.Parallel()
	database := testpg.Open(t)
	ctx := context.Background()
	if err := flow.Migrate(ctx, database.DB, flow.WithSchema(database.Schema)); err != nil {
		t.Fatalf("Migrate() error = %v", err)
	}
	var output bytes.Buffer
	result, err := flowscenarios.RunFanOut(ctx, database.DB, database.Schema, &output)
	if err != nil {
		t.Fatalf("RunFanOut() error = %v", err)
	}
	if !strings.Contains(output.String(), "generated report with score 6") {
		t.Fatalf("example output = %q", output.String())
	}
	final, err := flow.ResultOf(result.Trace, "generate", flowscenarios.GenerateReport)
	if err != nil || final.Total != 6 {
		t.Fatalf("final report = %#v, %v", final, err)
	}
	if result.Trace.Execution.Status != "succeeded" || len(result.Trace.Commands) != 5 {
		t.Fatalf("example trace = %#v", result.Trace)
	}
	var queueRows, createdRows, terminalRows int
	if err := database.DB.Conn.QueryRow(ctx, `SELECT count(*) FROM `+pgschema.Table(database.Schema, "flow_command_queue")+`
		WHERE execution_id=$1`, result.Handle.ID).Scan(&queueRows); err != nil {
		t.Fatal(err)
	}
	if err := database.DB.Conn.QueryRow(ctx, `SELECT count(*) FILTER (WHERE entry_kind='command_created'),
		count(*) FILTER (WHERE entry_kind='event_recorded' AND event_class='command_terminal')
		FROM `+pgschema.Table(database.Schema, "flow_journal")+` WHERE execution_id=$1`, result.Handle.ID).
		Scan(&createdRows, &terminalRows); err != nil {
		t.Fatal(err)
	}
	if queueRows != 0 || createdRows != 5 || terminalRows != 5 {
		t.Fatalf("database rows queue=%d created=%d terminal=%d", queueRows, createdRows, terminalRows)
	}
}
