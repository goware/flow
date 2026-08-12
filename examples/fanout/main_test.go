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

func TestFanOutExampleEndToEnd(t *testing.T) {
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

	run, trace, err := runExampleCommand(ctx, runtime)
	if err != nil {
		t.Fatalf("runExampleCommand() error = %v", err)
	}
	if !strings.Contains(output.String(), "generated report with score 6") {
		t.Fatalf("example output = %q", output.String())
	}
	final, err := flow.ResultOf(trace, "generate", generateReport)
	if err != nil || final.Total != 6 {
		t.Fatalf("final report = %#v, %v", final, err)
	}
	if trace.Run.Status != "succeeded" || len(trace.Commands) != 10 {
		t.Fatalf("example trace = %#v", trace)
	}
	var queueRows, createdRows, terminalRows int
	if err := database.DB.Conn.QueryRow(ctx, `SELECT count(*) FROM `+pgschema.Table(database.Schema, "flow_command_queue")+`
		WHERE run_id=$1`, run.ID).Scan(&queueRows); err != nil {
		t.Fatal(err)
	}
	if err := database.DB.Conn.QueryRow(ctx, `SELECT count(*) FILTER (WHERE entry_kind='command_created'),
		count(*) FILTER (WHERE entry_kind='event_recorded' AND event_class='command_terminal')
		FROM `+pgschema.Table(database.Schema, "flow_journal")+` WHERE run_id=$1`, run.ID).
		Scan(&createdRows, &terminalRows); err != nil {
		t.Fatal(err)
	}
	if queueRows != 0 || createdRows != 10 || terminalRows != 10 {
		t.Fatalf("database rows queue=%d created=%d terminal=%d", queueRows, createdRows, terminalRows)
	}
	replaytest.AssertMatchesLive(t, database.DB, database.Schema, run.ID)
}
