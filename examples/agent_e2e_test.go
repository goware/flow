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

func TestDurableAdaptiveAgentExampleEndToEnd(t *testing.T) {
	t.Parallel()
	database := testpg.Open(t)
	ctx := context.Background()
	if err := flow.Migrate(ctx, database.DB, flow.WithSchema(database.Schema)); err != nil {
		t.Fatalf("Migrate: %v", err)
	}
	var output bytes.Buffer
	result, err := flowscenarios.RunAgent(ctx, database.DB, database.Schema, &output)
	if err != nil {
		t.Fatalf("RunAgent: %v", err)
	}
	if !strings.Contains(output.String(), "agent thinking on turn 2") || !strings.Contains(output.String(), "running tool broken") {
		t.Fatalf("output=%q", output.String())
	}
	if result.Trace.Execution.Status != "succeeded" || result.Trace.Execution.OutcomeRef != "result/final-report" ||
		len(result.Trace.Commands) != 4 || result.Trace.Coordinator == nil || result.Trace.Coordinator.Status != "completed" ||
		result.Trace.Coordinator.StateRevision != 6 || len(result.Trace.Coordinator.Attempts) != 6 ||
		result.Trace.Coordinator.InboxPosition == 0 {
		t.Fatalf("trace=%+v", result.Trace)
	}
	statuses := map[string]string{}
	for _, command := range result.Trace.Commands {
		statuses[command.Key] = command.State
	}
	if statuses["turn/1/tool/broken"] != "failed" || statuses["turn/2/think"] != "succeeded" {
		t.Fatalf("statuses=%v", statuses)
	}
	var transitions, outcomes, queueRows int
	if err = database.DB.Conn.QueryRow(ctx, `SELECT count(*) FILTER (WHERE entry_kind='coordinator_transition'),
		count(*) FILTER (WHERE event_class='command_terminal') FROM `+pgschema.Table(database.Schema, "flow_journal")+` WHERE execution_id=$1`, result.Handle.ID).Scan(&transitions, &outcomes); err != nil {
		t.Fatal(err)
	}
	if err = database.DB.Conn.QueryRow(ctx, `SELECT count(*) FROM `+pgschema.Table(database.Schema, "flow_command_queue")+` WHERE execution_id=$1`, result.Handle.ID).Scan(&queueRows); err != nil {
		t.Fatal(err)
	}
	if transitions != 6 || outcomes != 4 || queueRows != 0 {
		t.Fatalf("transitions=%d outcomes=%d queue=%d", transitions, outcomes, queueRows)
	}
	assertReplayMatchesLive(t, database.DB, database.Schema, result.Handle.ID)
}
