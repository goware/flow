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

	handle, trace, err := runExampleCommand(ctx, runtime)
	if err != nil {
		t.Fatalf("runExampleCommand() error = %v", err)
	}
	if !strings.Contains(output.String(), "agent thinking on turn 2") ||
		!strings.Contains(output.String(), "running tool broken") {
		t.Fatalf("output=%q", output.String())
	}
	if trace.Execution.Status != "succeeded" ||
		len(trace.Commands) != 4 || trace.Coordinator == nil || trace.Coordinator.Status != "completed" ||
		trace.Coordinator.StateRevision != 6 || len(trace.Coordinator.Attempts) != 6 ||
		trace.Coordinator.InboxPosition == 0 {
		t.Fatalf("trace=%+v", trace)
	}
	statuses := map[string]string{}
	for _, command := range trace.Commands {
		statuses[command.Key] = command.State
	}
	if statuses["turn/1/tool/broken"] != "failed" || statuses["turn/2/think"] != "succeeded" {
		t.Fatalf("statuses=%v", statuses)
	}
	completedEvent := false
	for _, event := range trace.Events {
		if event.Name == agentCompleted.Name() && event.Key == "final" &&
			event.Class == "application" && event.CoordinatorID != "" {
			completedEvent = true
		}
	}
	if !completedEvent {
		t.Fatalf("completed event missing from trace: %+v", trace.Events)
	}
	var transitions, outcomes, applicationEvents, queueRows int
	if err = database.DB.Conn.QueryRow(ctx, `SELECT count(*) FILTER (WHERE entry_kind='coordinator_transition'),
		count(*) FILTER (WHERE event_class='command_terminal'),
		count(*) FILTER (WHERE event_class='application') FROM `+pgschema.Table(database.Schema, "flow_journal")+` WHERE execution_id=$1`, handle.ID).Scan(&transitions, &outcomes, &applicationEvents); err != nil {
		t.Fatal(err)
	}
	if err = database.DB.Conn.QueryRow(ctx, `SELECT count(*) FROM `+pgschema.Table(database.Schema, "flow_command_queue")+` WHERE execution_id=$1`, handle.ID).Scan(&queueRows); err != nil {
		t.Fatal(err)
	}
	if transitions != 6 || outcomes != 4 || applicationEvents != 2 || queueRows != 0 {
		t.Fatalf("transitions=%d outcomes=%d application_events=%d queue=%d", transitions, outcomes, applicationEvents, queueRows)
	}
	replaytest.AssertMatchesLive(t, database.DB, database.Schema, handle.ID)
}
