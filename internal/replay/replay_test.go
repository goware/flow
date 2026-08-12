package replay

import (
	"crypto/sha256"
	"encoding/json"
	"testing"
	"time"

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
		DefinitionVersion: 1, RunKey: "key", DeadlineMode: "none", MaxCommands: 5,
	})
	created := row(t, runID, 2, store.CommandCreated, &commandID, journalcodec.CommandCreatedBody{
		V: 1, CommandID: commandID.String(), CommandKey: "root", Name: "work", Version: 1,
		Args:         json.RawMessage(`{"x":1}`),
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
	unknownVersion := row(t, runID, 1, store.RunStarted, nil, journalcodec.RunStartedBody{
		V: 2, RunID: runID.String(), DefinitionName: "work",
		DefinitionVersion: 1, RunKey: "key", DeadlineMode: "none", MaxCommands: 5,
	})
	if _, err := Fold([]store.JournalRow{unknownVersion}); err == nil {
		t.Fatal("Fold() accepted an unknown run-start body version")
	}
	unknownFieldCommand := row(t, runID, 2, store.CommandCreated, &commandID, struct {
		journalcodec.CommandCreatedBody
		Unexpected bool `json:"unexpected"`
	}{CommandCreatedBody: journalcodec.CommandCreatedBody{
		V: 1, CommandID: commandID.String(), CommandKey: "root", Name: "work", Version: 1,
		Args: json.RawMessage(`{"x":1}`), InitialState: "ready", Queue: "default",
		RetryPolicy:            json.RawMessage(`{"backoff":[1],"jitter":0,"max_attempts":1}`),
		DeclarationFingerprint: "0000000000000000000000000000000000000000000000000000000000000000",
	}, Unexpected: true})
	if _, err := Fold([]store.JournalRow{start, unknownFieldCommand}); err == nil {
		t.Fatal("Fold() accepted an unknown command-created field")
	}
}

func TestFoldValidatesApplicationEventBodies(t *testing.T) {
	t.Parallel()

	runID := uuid.New()
	start := row(t, runID, 1, store.RunStarted, nil, journalcodec.RunStartedBody{
		V: 1, RunID: runID.String(), DefinitionName: "work",
		DefinitionVersion: 1, RunKey: "key", DeadlineMode: "none", MaxCommands: 5,
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

func TestFoldValidatesRecoveryLeaseDeclarationsAndAttempts(t *testing.T) {
	t.Parallel()

	runID, commandID, attemptID := uuid.New(), uuid.New(), uuid.New()
	start := row(t, runID, 1, store.RunStarted, nil, journalcodec.RunStartedBody{
		V: 1, RunID: runID.String(), DefinitionName: "work",
		DefinitionVersion: 1, RunKey: "key", DeadlineMode: "none", MaxCommands: 1,
	})
	lease := int64(120)
	createdBody := journalcodec.CommandCreatedBody{
		V: 1, CommandID: commandID.String(), CommandKey: "root", Name: "work", Version: 1,
		Args: json.RawMessage(`{}`), InitialState: "ready", Queue: "default",
		RetryPolicy:            json.RawMessage(`{"backoff":[1],"jitter":0,"max_attempts":1}`),
		RecoveryLeaseMS:        &lease,
		DeclarationFingerprint: "0000000000000000000000000000000000000000000000000000000000000000",
	}
	created := row(t, runID, 2, store.CommandCreated, &commandID, createdBody)
	started := row(t, runID, 3, store.AttemptStarted, &commandID, journalcodec.AttemptStartedBody{
		V: 1, AttemptID: attemptID.String(), CommandID: commandID.String(), CommandKey: "root",
		Attempt: 1, StartedAt: time.Now().UTC(), Worker: "worker", LeaseDurationMS: lease,
		BudgetStartedAt: time.Now().UTC(),
	})
	started.AttemptID = &attemptID
	if _, err := Fold([]store.JournalRow{start, created, started}); err != nil {
		t.Fatalf("Fold(valid recovery lease) error = %v", err)
	}

	tooShort := int64(29)
	createdBody.RecoveryLeaseMS = &tooShort
	if _, err := Fold([]store.JournalRow{start, row(t, runID, 2, store.CommandCreated, &commandID, createdBody)}); err == nil {
		t.Fatal("Fold() accepted a sub-floor recovery lease")
	}
	createdBody.RecoveryLeaseMS = &lease
	zeroStarted := started
	zeroStarted.Body = row(t, runID, 3, store.AttemptStarted, &commandID, journalcodec.AttemptStartedBody{
		V: 1, AttemptID: attemptID.String(), CommandID: commandID.String(), CommandKey: "root",
		Attempt: 1, StartedAt: time.Now().UTC(), Worker: "worker", LeaseDurationMS: 0,
		BudgetStartedAt: time.Now().UTC(),
	}).Body
	zeroStarted.BodyHash = sha256.Sum256(zeroStarted.Body)
	if _, err := Fold([]store.JournalRow{start, created, zeroStarted}); err == nil {
		t.Fatal("Fold() accepted a zero attempt lease")
	}
	mismatched := row(t, runID, 3, store.AttemptStarted, &commandID, journalcodec.AttemptStartedBody{
		V: 1, AttemptID: attemptID.String(), CommandID: commandID.String(), CommandKey: "root",
		Attempt: 1, StartedAt: time.Now().UTC(), Worker: "worker", LeaseDurationMS: 121,
		BudgetStartedAt: time.Now().UTC(),
	})
	mismatched.AttemptID = &attemptID
	if _, err := Fold([]store.JournalRow{start, created, mismatched}); err == nil {
		t.Fatal("Fold() accepted an attempt lease that differs from its declaration")
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
