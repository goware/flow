package replay

import (
	"crypto/sha256"
	"encoding/json"
	"testing"

	"github.com/google/uuid"
	"github.com/goware/flow/internal/store"
	"github.com/goware/flow/internal/store/journalcodec"
)

func TestFoldInitialProjectionAndValidation(t *testing.T) {
	t.Parallel()

	executionID := uuid.New()
	commandID := uuid.New()
	start := row(t, executionID, 1, store.ExecutionStarted, nil, journalcodec.ExecutionStartedBody{
		V: 1, ExecutionID: executionID.String(), DriverMode: "direct", DefinitionName: "work",
		DefinitionVersion: 1, ExecutionKey: "key", Input: json.RawMessage(`{"x":1}`),
		FailFast: true, DeadlineMode: "none", MaxCommands: 5, Metadata: json.RawMessage(`{}`),
	})
	created := row(t, executionID, 2, store.CommandCreated, &commandID, journalcodec.CommandCreatedBody{
		V: 1, CommandID: commandID.String(), CommandKey: "root", Name: "work", Version: 1,
		Args: json.RawMessage(`{"x":1}`), Origin: "direct_root", Required: true,
		InitialState: "ready", Queue: "default", RetryPolicy: json.RawMessage(`{"backoff":[1],"jitter":0,"max_attempts":1}`),
		DeclarationFingerprint: "0000000000000000000000000000000000000000000000000000000000000000",
	})
	state, err := Fold([]store.JournalRow{start, created})
	if err != nil {
		t.Fatalf("Fold() error = %v", err)
	}
	if state.ID != executionID || state.CommandCount != 1 || state.OpenCommands != 1 ||
		state.RootCommandID == nil || *state.RootCommandID != commandID || state.Commands[commandID].State != "ready" {
		t.Fatalf("Fold() = %#v", state)
	}

	gap := created
	gap.Position = 3
	if _, err := Fold([]store.JournalRow{start, gap}); err == nil {
		t.Fatal("Fold() accepted a journal gap")
	}
	badHash := start
	badHash.BodyHash[0]++
	if _, err := Fold([]store.JournalRow{badHash}); err == nil {
		t.Fatal("Fold() accepted a bad body hash")
	}
}

func row(t *testing.T, executionID uuid.UUID, position int64, kind store.EntryKind, commandID *uuid.UUID, body any) store.JournalRow {
	t.Helper()
	encoded, err := journalcodec.Encode(body)
	if err != nil {
		t.Fatalf("journalcodec.Encode() error = %v", err)
	}
	return store.JournalRow{
		ExecutionID: executionID, Position: position, EntryID: uuid.New(), Kind: kind,
		CommandID: commandID, Body: encoded.BytesCopy(), BodyHash: sha256.Sum256(encoded.Bytes),
	}
}
