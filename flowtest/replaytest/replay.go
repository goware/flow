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

// AssertMatchesLive checks that folding a run's journal produces
// the same run and command state as the live projections.
func AssertMatchesLive(t testing.TB, db *pgkit.DB, schema string, id flow.RunID) {
	t.Helper()
	ctx := context.Background()
	runtime, err := flow.New(db, flow.WithSchema(schema))
	if err != nil {
		t.Fatalf("New() for replay conformance error = %v", err)
	}
	live, err := flow.GetRun(ctx, runtime, id)
	if err != nil {
		t.Fatalf("GetRun() for replay conformance error = %v", err)
	}
	repository, err := store.New(db, schema, false)
	if err != nil {
		t.Fatalf("store.New() error = %v", err)
	}
	runID, err := uuid.Parse(string(id))
	if err != nil {
		t.Fatalf("parse run ID: %v", err)
	}
	var rows []store.JournalRow
	var after uint64
	for {
		page, err := repository.History(ctx, runID, after, store.MaxHistoryLimit)
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
	if projected.ID.String() != string(live.ID) || projected.DefinitionName != live.RootCommandName || projected.DefinitionVersion != live.RootCommandVersion ||
		projected.RunKey != live.RunKey || projected.Status != string(live.Status) || projected.CommandCount != live.CommandCount ||
		projected.OpenCommands != live.OpenCommands || !equalFailure(projected.Failure, live.Failure) {
		t.Fatalf("replay/live run mismatch:\nreplay=%#v\nlive=%#v", projected, live)
	}

	commandRows, err := db.Conn.Query(ctx, `SELECT command_id,state,result,terminal_position,retry_policy,declaration_fingerprint FROM `+
		pgschema.Table(schema, "flow_commands")+` WHERE run_id=$1`, runID)
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
		var retryPolicy, declarationFingerprint []byte
		if err := commandRows.Scan(&commandID, &state, &result, &terminalPosition, &retryPolicy, &declarationFingerprint); err != nil {
			t.Fatalf("scan live command: %v", err)
		}
		command, ok := projected.Commands[commandID]
		if !ok || command.State != state || !bytes.Equal(command.Result, result) ||
			!equalInt64(command.TerminalPosition, terminalPosition) || !bytes.Equal(command.RetryPolicy, retryPolicy) ||
			!bytes.Equal(command.DeclarationFingerprint[:], declarationFingerprint) {
			t.Fatalf("replay/live command mismatch id=%s replay=%#v state=%s result=%s terminal=%v retry=%x fingerprint=%x",
				commandID, command, state, result, terminalPosition, retryPolicy, declarationFingerprint)
		}
		seen++
	}
	if err := commandRows.Err(); err != nil {
		t.Fatalf("read live commands: %v", err)
	}
	if seen != len(projected.Commands) {
		t.Fatalf("replay/live command count = %d/%d", len(projected.Commands), seen)
	}
}

func equalFailure(left, right *flow.Failure) bool {
	if left == nil || right == nil {
		return left == nil && right == nil
	}
	return *left == *right
}

func equalInt64(left, right *int64) bool {
	if left == nil || right == nil {
		return left == nil && right == nil
	}
	return *left == *right
}
