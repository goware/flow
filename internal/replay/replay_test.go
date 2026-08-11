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

	runID := uuid.New()
	commandID := uuid.New()
	start := row(t, runID, 1, store.RunStarted, nil, journalcodec.RunStartedBody{
		V: 1, RunID: runID.String(), DefinitionName: "work",
		DefinitionVersion: 1, RunKey: "key", Input: json.RawMessage(`{"x":1}`),
		FailFast: true, DeadlineMode: "none", MaxCommands: 5, Metadata: json.RawMessage(`{}`),
	})
	created := row(t, runID, 2, store.CommandCreated, &commandID, journalcodec.CommandCreatedBody{
		V: 1, CommandID: commandID.String(), CommandKey: "root", Name: "work", Version: 1,
		Args: json.RawMessage(`{"x":1}`), Required: true,
		InitialState: "ready", Queue: "default", RetryPolicy: json.RawMessage(`{"backoff":[1],"jitter":0,"max_attempts":1}`),
		DeclarationFingerprint: "0000000000000000000000000000000000000000000000000000000000000000",
	})
	state, err := Fold([]store.JournalRow{start, created})
	if err != nil {
		t.Fatalf("Fold() error = %v", err)
	}
	if state.ID != runID || state.CommandCount != 1 || state.OpenCommands != 1 ||
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
	noncanonical := start
	noncanonical.Body = append([]byte(" \n"), start.Body...)
	noncanonical.BodyHash = sha256.Sum256(noncanonical.Body)
	if _, err := Fold([]store.JournalRow{noncanonical}); err == nil {
		t.Fatal("Fold() accepted a matching-hash noncanonical body")
	}
	duplicateKey := start
	duplicateKey.Body = []byte(`{"v":1,"v":1}`)
	duplicateKey.BodyHash = sha256.Sum256(duplicateKey.Body)
	if _, err := Fold([]store.JournalRow{duplicateKey}); err == nil {
		t.Fatal("Fold() accepted a matching-hash duplicate-key body")
	}
}

func TestFoldValidatesApplicationEventBodies(t *testing.T) {
	t.Parallel()

	runID := uuid.New()
	start := row(t, runID, 1, store.RunStarted, nil, journalcodec.RunStartedBody{
		V: 1, RunID: runID.String(), DefinitionName: "work",
		DefinitionVersion: 1, RunKey: "key", Input: json.RawMessage(`{}`),
		FailFast: true, DeadlineMode: "none", MaxCommands: 5, Metadata: json.RawMessage(`{}`),
	})
	application := row(t, runID, 2, store.EventRecorded, nil, journalcodec.ApplicationEventBody{
		V: 2, Payload: json.RawMessage(`{"value":"future"}`),
	})
	eventID := uuid.New()
	namespace, name, key, class := "application", "event", "key", "application"
	application.EventID = &eventID
	application.EventNamespace = &namespace
	application.EventName = &name
	application.EventKey = &key
	application.EventClass = &class
	if _, err := Fold([]store.JournalRow{start, application}); err == nil {
		t.Fatal("Fold() accepted an unknown application-event body version")
	}

	application.Body = []byte(`{"payload":{"duplicate":1,"duplicate":2},"v":1}`)
	application.BodyHash = sha256.Sum256(application.Body)
	if _, err := Fold([]store.JournalRow{start, application}); err == nil {
		t.Fatal("Fold() accepted a matching-hash duplicate-key application payload")
	}
}

func row(t *testing.T, runID uuid.UUID, position int64, kind store.EntryKind, commandID *uuid.UUID, body any) store.JournalRow {
	t.Helper()
	encoded, err := journalcodec.Encode(body)
	if err != nil {
		t.Fatalf("journalcodec.Encode() error = %v", err)
	}
	return store.JournalRow{
		RunID: runID, Position: position, EntryID: uuid.New(), Kind: kind,
		CommandID: commandID, Body: encoded.BytesCopy(), BodyHash: sha256.Sum256(encoded.Bytes),
	}
}
