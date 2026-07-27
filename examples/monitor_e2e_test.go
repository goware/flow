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

func TestExternalMonitorExampleEndToEnd(t *testing.T) {
	t.Parallel()
	database := testpg.Open(t)
	ctx := context.Background()
	if err := flow.Migrate(ctx, database.DB, flow.WithSchema(database.Schema)); err != nil {
		t.Fatalf("Migrate() error = %v", err)
	}
	var output bytes.Buffer
	result, err := flowscenarios.RunMonitor(ctx, database.DB, database.Schema, &output)
	if err != nil {
		t.Fatalf("RunMonitor() error = %v", err)
	}
	if !strings.Contains(output.String(), "external monitor observed bridge delivery") ||
		!strings.Contains(output.String(), "bridge delivery confirmed") {
		t.Fatalf("example output = %q", output.String())
	}
	confirmed, err := flow.ResultOf(result.Trace, "confirm", flowscenarios.ConfirmBridge)
	if err != nil || !confirmed.Confirmed {
		t.Fatalf("confirmation = %#v, %v", confirmed, err)
	}
	if len(result.Trace.Commands) != 1 || len(result.Trace.Commands[0].Waits) != 1 {
		t.Fatalf("wait trace = %#v", result.Trace.Commands)
	}
	var factRows, queueRows, satisfiedWaits int
	if err := database.DB.Conn.QueryRow(ctx, `SELECT count(*) FROM `+pgschema.Table(database.Schema, "flow_journal")+`
		WHERE execution_id=$1 AND event_namespace='application' AND event_name=$2`,
		result.Handle.ID, flowscenarios.BridgeDeliveredName).Scan(&factRows); err != nil {
		t.Fatal(err)
	}
	if err := database.DB.Conn.QueryRow(ctx, `SELECT count(*) FROM `+pgschema.Table(database.Schema, "flow_command_event_waits")+`
		WHERE execution_id=$1 AND satisfied_position IS NOT NULL`, result.Handle.ID).Scan(&satisfiedWaits); err != nil {
		t.Fatal(err)
	}
	if err := database.DB.Conn.QueryRow(ctx, `SELECT count(*) FROM `+pgschema.Table(database.Schema, "flow_command_queue")+`
		WHERE execution_id=$1`, result.Handle.ID).Scan(&queueRows); err != nil {
		t.Fatal(err)
	}
	if factRows != 1 || satisfiedWaits != 1 || queueRows != 0 {
		t.Fatalf("database rows facts=%d satisfied_waits=%d queue=%d", factRows, satisfiedWaits, queueRows)
	}
}
