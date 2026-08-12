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

func TestDurableAdaptiveAgentExampleEndToEnd(t *testing.T) {
	t.Parallel()
	database := testpg.Open(t)
	ctx := context.Background()
	if err := flow.Migrate(ctx, database.DB, flow.WithSchema(database.Schema)); err != nil {
		t.Fatalf("Migrate: %v", err)
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
	if !strings.Contains(output.String(), "agent thinking on turn 2") ||
		!strings.Contains(output.String(), "running tool archive") {
		t.Fatalf("output=%q", output.String())
	}
	if trace.Run.Status != "succeeded" || len(trace.Commands) != 4 {
		t.Fatalf("trace=%+v", trace)
	}
	statuses := map[string]flow.CommandStatus{}
	for _, command := range trace.Commands {
		statuses[command.Key] = command.Status
	}
	if statuses["turn/1/tool/archive"] != flow.CommandStatusSucceeded || statuses["turn/2/think"] != flow.CommandStatusSucceeded {
		t.Fatalf("statuses=%v", statuses)
	}
	completedEvent := false
	for _, event := range trace.Events {
		if event.Name == agentCompleted.Name() && event.Key == "final" && event.Class == "application" {
			completedEvent = true
		}
	}
	if !completedEvent {
		t.Fatalf("completed event missing from trace: %+v", trace.Events)
	}
	var outcomes, applicationEvents, queueRows int
	if err = database.DB.Conn.QueryRow(ctx, `SELECT count(*) FILTER (WHERE event_class='command_terminal'),
		count(*) FILTER (WHERE event_class='application') FROM `+pgschema.Table(database.Schema, "flow_journal")+` WHERE run_id=$1`, run.ID).Scan(&outcomes, &applicationEvents); err != nil {
		t.Fatal(err)
	}
	if err = database.DB.Conn.QueryRow(ctx, `SELECT count(*) FROM `+pgschema.Table(database.Schema, "flow_command_queue")+` WHERE run_id=$1`, run.ID).Scan(&queueRows); err != nil {
		t.Fatal(err)
	}
	if outcomes != 4 || applicationEvents != 4 || queueRows != 0 {
		t.Fatalf("outcomes=%d application_events=%d queue=%d", outcomes, applicationEvents, queueRows)
	}
	replaytest.AssertMatchesLive(t, database.DB, database.Schema, run.ID)
}
