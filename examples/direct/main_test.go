package main

import (
	"bytes"
	"context"
	"strings"
	"testing"

	"github.com/goware/flow"
	"github.com/goware/flow/flowtest/replaytest"
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
	runtime, err := newFlowRuntime(database.DB, database.Schema, &output)
	if err != nil {
		t.Fatalf("newFlowRuntime() error = %v", err)
	}
	stopFlowRuntime := runFlowRuntime(runtime)
	defer stopFlowRuntime()

	handle, trace, err := runExampleCommand(ctx, runtime)
	if err != nil {
		t.Fatalf("runExampleCommand() error = %v", err)
	}
	if !strings.Contains(output.String(), "receipt sent: stub-example-order") {
		t.Fatalf("example output = %q", output.String())
	}
	if trace.Execution.Status != "succeeded" || len(trace.Commands) != 1 ||
		string(trace.Commands[0].Result) != `{"provider_message_id":"stub-example-order"}` {
		t.Fatalf("example trace = %#v", trace)
	}
	var queueRows, journalRows int
	if err := database.DB.Conn.QueryRow(ctx, `SELECT count(*) FROM `+pgschema.Table(database.Schema, "flow_command_queue")+`
		WHERE execution_id=$1`, handle.ID).Scan(&queueRows); err != nil {
		t.Fatalf("count queue rows: %v", err)
	}
	if err := database.DB.Conn.QueryRow(ctx, `SELECT count(*) FROM `+pgschema.Table(database.Schema, "flow_journal")+`
		WHERE execution_id=$1`, handle.ID).Scan(&journalRows); err != nil {
		t.Fatalf("count journal rows: %v", err)
	}
	if queueRows != 0 || journalRows != 6 || len(trace.History) != journalRows {
		t.Fatalf("database rows queue=%d journal=%d trace=%d", queueRows, journalRows, len(trace.History))
	}
	replaytest.AssertMatchesLive(t, database.DB, database.Schema, handle.ID)
}
