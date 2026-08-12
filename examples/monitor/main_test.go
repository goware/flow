package main

import (
	"bytes"
	"context"
	"strings"
	"testing"
	"time"

	"github.com/goware/flow"
	"github.com/goware/flow/flowtest/replaytest"
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
	runtime, alerts, err := newFlowRuntime(database.DB, database.Schema, &output)
	if err != nil {
		t.Fatalf("newFlowRuntime() error = %v", err)
	}
	monitor, err := newExternalMonitor(database.DB, database.Schema, &output)
	if err != nil {
		t.Fatalf("newExternalMonitor() error = %v", err)
	}
	stopFlowRuntime := runFlowRuntime(runtime)
	defer stopFlowRuntime()

	exec, trace, err := runExampleCommand(ctx, runtime, monitor)
	if err != nil {
		t.Fatalf("runExampleCommand() error = %v", err)
	}
	if !strings.Contains(output.String(), "external monitor observed bridge delivery") ||
		!strings.Contains(output.String(), "bridge delivery 0xexample confirmed") {
		t.Fatalf("example output = %q", output.String())
	}
	deadline := time.Now().Add(2 * time.Second)
	dutyCycle, pages := alerts.summary()
	for dutyCycle == 0 && time.Now().Before(deadline) {
		time.Sleep(time.Millisecond)
		dutyCycle, pages = alerts.summary()
	}
	if dutyCycle == 0 || pages != 0 {
		t.Fatalf("observer summary = %d duty-cycle facts, %d pages", dutyCycle, pages)
	}
	confirmed, err := flow.ResultOf(trace, "root", confirmBridge)
	if err != nil || !confirmed.Confirmed {
		t.Fatalf("confirmation = %#v, %v", confirmed, err)
	}
	if len(trace.Commands) != 1 || len(trace.Commands[0].Waits) != 1 {
		t.Fatalf("wait trace = %#v", trace.Commands)
	}
	var factRows, queueRows, satisfiedWaits int
	if err := database.DB.Conn.QueryRow(ctx, `SELECT count(*) FROM `+pgschema.Table(database.Schema, "flow_journal")+`
		WHERE run_id=$1 AND event_namespace='application' AND event_name=$2`,
		exec.ID, bridgeDeliveredName).Scan(&factRows); err != nil {
		t.Fatal(err)
	}
	if err := database.DB.Conn.QueryRow(ctx, `SELECT count(*) FROM `+pgschema.Table(database.Schema, "flow_command_event_waits")+`
		WHERE run_id=$1 AND satisfied_position IS NOT NULL`, exec.ID).Scan(&satisfiedWaits); err != nil {
		t.Fatal(err)
	}
	if err := database.DB.Conn.QueryRow(ctx, `SELECT count(*) FROM `+pgschema.Table(database.Schema, "flow_command_queue")+`
		WHERE run_id=$1`, exec.ID).Scan(&queueRows); err != nil {
		t.Fatal(err)
	}
	if factRows != 1 || satisfiedWaits != 1 || queueRows != 0 {
		t.Fatalf("database rows facts=%d satisfied_waits=%d queue=%d", factRows, satisfiedWaits, queueRows)
	}
	replaytest.AssertMatchesLive(t, database.DB, database.Schema, exec.ID)
}
