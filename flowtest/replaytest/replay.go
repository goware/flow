// Package replaytest provides PostgreSQL-backed assertions for verifying that
// Flow's journal replay agrees with its live projections.
package replaytest

import (
	"bytes"
	"context"
	"testing"

	"github.com/google/uuid"
	"github.com/goware/flow"
	"github.com/goware/flow/internal/pgschema"
	"github.com/goware/flow/internal/replay"
	"github.com/goware/flow/internal/store"
	"github.com/goware/pgkit/v2"
)

// AssertMatchesLive checks that folding an execution's journal produces
// the same execution, command, and coordinator state as the live projections.
func AssertMatchesLive(t testing.TB, db *pgkit.DB, schema string, id flow.ExecutionID) {
	t.Helper()
	ctx := context.Background()
	runtime, err := flow.New(db, flow.WithSchema(schema))
	if err != nil {
		t.Fatalf("New() for replay conformance error = %v", err)
	}
	live, err := flow.GetExecution(ctx, runtime, id)
	if err != nil {
		t.Fatalf("GetExecution() for replay conformance error = %v", err)
	}
	repository, err := store.New(db, schema, false)
	if err != nil {
		t.Fatalf("store.New() error = %v", err)
	}
	executionID, err := uuid.Parse(string(id))
	if err != nil {
		t.Fatalf("parse execution ID: %v", err)
	}
	var rows []store.JournalRow
	var after uint64
	for {
		page, err := repository.History(ctx, executionID, after, store.MaxHistoryLimit)
		if err != nil {
			t.Fatalf("History() replay rows error = %v", err)
		}
		rows = append(rows, page...)
		if len(page) < store.MaxHistoryLimit {
			break
		}
		after = uint64(page[len(page)-1].Position)
	}
	projected, err := replay.Fold(rows)
	if err != nil {
		t.Fatalf("replay.Fold() error = %v", err)
	}
	if projected.ID.String() != string(live.ID) || string(projected.DriverMode) != live.Mode ||
		projected.DefinitionName != live.Type || projected.DefinitionVersion != live.Version ||
		projected.ExecutionKey != live.Key || projected.Status != live.Status || projected.CommandCount != live.CommandCount ||
		projected.OpenCommands != live.OpenCommands || projected.PlanDirty != live.PlanDirty ||
		projected.PlanQuiescent != live.PlanQuiescent || projected.PlanRevision != int64(live.PlanRevision) ||
		projected.PlanWaitingCount != live.PlanWaitingCount ||
		projected.FailureCode != live.FailureCode || projected.FailureMessage != live.FailureMessage {
		t.Fatalf("replay/live execution mismatch:\nreplay=%#v\nlive=%#v", projected, live)
	}

	commandRows, err := db.Conn.Query(ctx, `SELECT command_id,state,result,terminal_position FROM `+
		pgschema.Table(schema, "flow_commands")+` WHERE execution_id=$1`, executionID)
	if err != nil {
		t.Fatalf("query live commands: %v", err)
	}
	defer commandRows.Close()
	seen := 0
	for commandRows.Next() {
		var commandID uuid.UUID
		var state string
		var result []byte
		var terminalPosition *int64
		if err := commandRows.Scan(&commandID, &state, &result, &terminalPosition); err != nil {
			t.Fatalf("scan live command: %v", err)
		}
		command, ok := projected.Commands[commandID]
		if !ok || command.State != state || !bytes.Equal(command.Result, result) || !equalInt64(command.TerminalPosition, terminalPosition) {
			t.Fatalf("replay/live command mismatch id=%s replay=%#v state=%s result=%s terminal=%v",
				commandID, command, state, result, terminalPosition)
		}
		seen++
	}
	if err := commandRows.Err(); err != nil {
		t.Fatalf("read live commands: %v", err)
	}
	if seen != len(projected.Commands) {
		t.Fatalf("replay/live command count = %d/%d", len(projected.Commands), seen)
	}

	if projected.Coordinator != nil {
		var status string
		var state []byte
		var revision, statePosition, inbox int64
		if err := db.Conn.QueryRow(ctx, `SELECT status,state,state_revision,state_position,inbox_position FROM `+
			pgschema.Table(schema, "flow_coordinators")+` WHERE execution_id=$1`, executionID).
			Scan(&status, &state, &revision, &statePosition, &inbox); err != nil {
			t.Fatalf("read live coordinator: %v", err)
		}
		if projected.Coordinator.Status != status || !bytes.Equal(projected.Coordinator.State, state) ||
			projected.Coordinator.StateRevision != revision || projected.Coordinator.StatePosition != statePosition ||
			projected.Coordinator.InboxPosition != inbox {
			t.Fatalf("replay/live coordinator mismatch replay=%#v live=%s/%s/%d/%d/%d",
				projected.Coordinator, status, state, revision, statePosition, inbox)
		}
	}
}

func equalInt64(left, right *int64) bool {
	if left == nil || right == nil {
		return left == nil && right == nil
	}
	return *left == *right
}
