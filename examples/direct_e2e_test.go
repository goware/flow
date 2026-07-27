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

func TestDirectExampleEndToEnd(t *testing.T) {
	t.Parallel()

	database := testpg.Open(t)
	ctx := context.Background()
	if err := flow.Migrate(ctx, database.DB, flow.WithSchema(database.Schema)); err != nil {
		t.Fatalf("Migrate() error = %v", err)
	}
	var output bytes.Buffer
	result, err := flowscenarios.RunDirect(ctx, database.DB, database.Schema, &output)
	if err != nil {
		t.Fatalf("RunDirect() error = %v", err)
	}
	if !strings.Contains(output.String(), "receipt sent: stub-example-order") {
		t.Fatalf("example output = %q", output.String())
	}
	if result.Trace.Execution.Status != "succeeded" || len(result.Trace.Commands) != 1 ||
		string(result.Trace.Commands[0].Result) != `{"provider_message_id":"stub-example-order"}` {
		t.Fatalf("example trace = %#v", result.Trace)
	}
	var queueRows, journalRows int
	if err := database.DB.Conn.QueryRow(ctx, `SELECT count(*) FROM `+pgschema.Table(database.Schema, "flow_command_queue")+`
		WHERE execution_id=$1`, result.Handle.ID).Scan(&queueRows); err != nil {
		t.Fatalf("count queue rows: %v", err)
	}
	if err := database.DB.Conn.QueryRow(ctx, `SELECT count(*) FROM `+pgschema.Table(database.Schema, "flow_journal")+`
		WHERE execution_id=$1`, result.Handle.ID).Scan(&journalRows); err != nil {
		t.Fatalf("count journal rows: %v", err)
	}
	if queueRows != 0 || journalRows != 6 || len(result.Trace.History) != journalRows {
		t.Fatalf("database rows queue=%d journal=%d trace=%d", queueRows, journalRows, len(result.Trace.History))
	}
}
