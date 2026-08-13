package flow

import (
	"context"
	"errors"
	"testing"
	"time"

	"github.com/goware/flow/internal/store"
	"github.com/goware/flow/internal/testpg"
)

func waitForTerminalObservation(
	t *testing.T,
	observer *recordingObserver,
	kind ObservationKind,
	operation string,
	outcome string,
	timeout time.Duration,
) Observation {
	t.Helper()
	deadline := time.Now().Add(timeout)
	for time.Now().Before(deadline) {
		for _, observation := range observer.snapshot() {
			if observation.Kind == kind && observation.Operation == operation && observation.Outcome == outcome {
				return observation
			}
		}
		time.Sleep(time.Millisecond)
	}
	t.Fatalf("did not observe %s/%s/%s: %#v", kind, operation, outcome, observer.snapshot())
	return Observation{}
}

func assertRunIdentity(t *testing.T, observation Observation, runKey, definition string) {
	t.Helper()
	if observation.RunKey != runKey || observation.RootCommandName != definition {
		t.Fatalf("observation identity RunKey=%q RootCommandName=%q, want %q/%q",
			observation.RunKey, observation.RootCommandName, runKey, definition)
	}
	if observation.OccurredAt.IsZero() {
		t.Fatal("observation OccurredAt is zero")
	}
}

func TestExhaustedConclusionEmitsTerminalObservations(t *testing.T) {
	t.Parallel()

	database := testpg.Open(t)
	ctx := context.Background()
	if err := Migrate(ctx, database.DB, WithSchema(database.Schema)); err != nil {
		t.Fatal(err)
	}
	observer := &recordingObserver{}
	command := DefineCommand[None, None]("observe.exhausted", 1, WithRetry(Attempts(1)))
	runtime, err := New(database.DB, WithSchema(database.Schema), WithNotifications(false),
		WithWorkerConcurrency(1), WithPollInterval(5*time.Millisecond), WithObserver(observer))
	if err != nil {
		t.Fatal(err)
	}
	if err := runtime.Register(Handle(command, func(context.Context, *Work[None]) (None, error) {
		return None{}, errors.New("expected failure")
	})); err != nil {
		t.Fatal(err)
	}
	cancel, runResult := startRuntime(t, runtime)
	defer stopRuntime(t, cancel, runResult)
	run, err := command.Enqueue(ctx, runtime, "observe/exhausted", None{})
	if err != nil {
		t.Fatal(err)
	}

	concluded := waitForTerminalObservation(t, observer, ObservationAttempt,
		ObservationOpConcludeExhausted, ObservationOutcomeFailed, 5*time.Second)
	assertRunIdentity(t, concluded, "observe/exhausted", command.Name())
	terminal := waitForTerminalObservation(t, observer, ObservationRun,
		ObservationOpTerminal, ObservationOutcomeFailed, 5*time.Second)
	assertRunIdentity(t, terminal, "observe/exhausted", command.Name())
	if terminal.RunID != run.RunID {
		t.Fatalf("run terminal RunID = %s, want %s", terminal.RunID, run.RunID)
	}
}

func TestCancellationEmitsTerminalObservations(t *testing.T) {
	t.Parallel()

	database := testpg.Open(t)
	ctx := context.Background()
	if err := Migrate(ctx, database.DB, WithSchema(database.Schema)); err != nil {
		t.Fatal(err)
	}
	observer := &recordingObserver{}
	command := DefineCommand[None, None]("observe.cancel", 1)
	runtime, err := New(database.DB, WithSchema(database.Schema), WithNotifications(false),
		WithPollInterval(time.Hour), WithObserver(observer))
	if err != nil {
		t.Fatal(err)
	}
	cancel, runResult := startRuntime(t, runtime)
	defer stopRuntime(t, cancel, runResult)

	cancelled, err := command.Enqueue(ctx, runtime, "observe/cancel-run", None{}, WithStartDelay(time.Hour))
	if err != nil {
		t.Fatal(err)
	}
	if err := CancelRun(ctx, runtime, cancelled.RunID, "operator cancellation"); err != nil {
		t.Fatal(err)
	}
	runCancel := waitForTerminalObservation(t, observer, ObservationRun,
		ObservationOpCancel, ObservationOutcomeCancelled, 5*time.Second)
	assertRunIdentity(t, runCancel, "observe/cancel-run", command.Name())

	commandRun, err := command.Enqueue(ctx, runtime, "observe/cancel-command", None{}, WithStartDelay(time.Hour))
	if err != nil {
		t.Fatal(err)
	}
	target := mustGetRun(t, runtime, commandRun.RunID)
	if err := CancelCommand(ctx, runtime, target.RootCommandID, "operator cancellation"); err != nil {
		t.Fatal(err)
	}
	commandCancel := waitForTerminalObservation(t, observer, ObservationCommand,
		ObservationOpCancel, ObservationOutcomeCancelled, 5*time.Second)
	assertRunIdentity(t, commandCancel, "observe/cancel-command", command.Name())
	if commandCancel.CommandKey != "root" {
		t.Fatalf("command cancel CommandKey = %q, want root", commandCancel.CommandKey)
	}
	// Cancelling the run's only command terminalizes the run. Deliveries are
	// FIFO on the adapter queue in this unflooded test, so once this later
	// observation lands, any earlier run/terminal for cancelled.RunID is
	// already in the snapshot.
	terminal := waitForTerminalObservation(t, observer, ObservationRun,
		ObservationOpTerminal, ObservationOutcomeFailed, 5*time.Second)
	assertRunIdentity(t, terminal, "observe/cancel-command", command.Name())

	// Direct run cancellation's run/cancel tuple is the terminal fact; it
	// must not also emit a run/terminal observation for the same run.
	cancelRunCancelCount := 0
	for _, observation := range observer.snapshot() {
		if observation.RunID != cancelled.RunID {
			continue
		}
		if observation.Kind == ObservationRun && observation.Operation == ObservationOpCancel &&
			observation.Outcome == ObservationOutcomeCancelled {
			cancelRunCancelCount++
		}
		if observation.Kind == ObservationRun && observation.Operation == ObservationOpTerminal {
			t.Fatalf("unexpected run/terminal observation for direct run cancellation: %#v", observation)
		}
	}
	if cancelRunCancelCount != 1 {
		t.Fatalf("run/cancel/cancelled observations for %s = %d, want 1", cancelled.RunID, cancelRunCancelCount)
	}
}

func TestMaintenanceEmitsTerminalObservations(t *testing.T) {
	t.Parallel()

	database := testpg.Open(t)
	ctx := context.Background()
	if err := Migrate(ctx, database.DB, WithSchema(database.Schema)); err != nil {
		t.Fatal(err)
	}
	observer := &recordingObserver{}
	gate := DefineEvent[None]("observe.maintenance.gate")
	waiting := DefineCommand[None, None]("observe.wait_expiry", 1)
	expiring := DefineCommand[None, None]("observe.run_expiry", 1)
	runtime, err := New(database.DB, WithSchema(database.Schema), WithNotifications(false),
		WithWorkerConcurrency(1), WithPollInterval(5*time.Millisecond), WithObserver(observer))
	if err != nil {
		t.Fatal(err)
	}
	if err := runtime.Register(
		Handle(waiting, func(context.Context, *Work[None]) (None, error) { return None{}, nil }),
		Handle(expiring, func(context.Context, *Work[None]) (None, error) { return None{}, nil }),
	); err != nil {
		t.Fatal(err)
	}
	cancel, runResult := startRuntime(t, runtime)
	defer stopRuntime(t, cancel, runResult)

	waitRun, err := waiting.Enqueue(ctx, runtime, "observe/wait-expiry", None{},
		WaitFor(gate, "never"), Within(50*time.Millisecond))
	if err != nil {
		t.Fatal(err)
	}
	expired := waitForTerminalObservation(t, observer, ObservationWait,
		ObservationOpExpire, ObservationOutcomeExpired, 5*time.Second)
	assertRunIdentity(t, expired, "observe/wait-expiry", waiting.Name())
	if expired.RunID != waitRun.RunID || expired.CommandKey != "root" {
		t.Fatalf("wait expiry observation = %#v", expired)
	}
	terminal := waitForTerminalObservation(t, observer, ObservationRun,
		ObservationOpTerminal, ObservationOutcomeFailed, 5*time.Second)
	assertRunIdentity(t, terminal, "observe/wait-expiry", waiting.Name())

	expiringRun, err := expiring.Enqueue(ctx, runtime, "observe/run-expiry", None{},
		WithStartDelay(time.Hour), WithRunDeadline(50*time.Millisecond))
	if err != nil {
		t.Fatal(err)
	}
	runExpired := waitForTerminalObservation(t, observer, ObservationRun,
		ObservationOpTerminal, ObservationOutcomeExpired, 5*time.Second)
	assertRunIdentity(t, runExpired, "observe/run-expiry", expiring.Name())
	if runExpired.RunID != expiringRun.RunID {
		t.Fatalf("run expiry RunID = %s, want %s", runExpired.RunID, expiringRun.RunID)
	}
}

func TestLeaseRecoveryEmitsPerCommandObservation(t *testing.T) {
	t.Parallel()

	database := testpg.Open(t)
	ctx := context.Background()
	if err := Migrate(ctx, database.DB, WithSchema(database.Schema)); err != nil {
		t.Fatal(err)
	}
	command := DefineCommand[None, None]("observe.lease_recovery", 1)
	staging, _ := stageClaimFixture(t, database, "observe_recovery", 1, func(work *Work[None]) {
		Enqueue(work, "recover", command, None{})
	})
	candidates := probeClaimCandidates(t, staging,
		[]store.CommandKind{{Name: command.Name(), Version: command.Version()}}, 1)
	claimed, err := staging.store.ClaimCommands(ctx, candidates, 50*time.Millisecond, "observe-recovery", nil)
	if err != nil || len(claimed.Commands) != 1 {
		t.Fatalf("ClaimCommands() commands=%d, err=%v", len(claimed.Commands), err)
	}
	time.Sleep(100 * time.Millisecond)

	// A separate observing runtime recovers the expired lease through its
	// maintenance loop and emits the per-command fact.
	observer := &recordingObserver{}
	runtime, err := New(database.DB, WithSchema(database.Schema), WithNotifications(false),
		WithPollInterval(5*time.Millisecond), WithObserver(observer))
	if err != nil {
		t.Fatal(err)
	}
	cancel, runResult := startRuntime(t, runtime)
	defer stopRuntime(t, cancel, runResult)

	recovered := waitForTerminalObservation(t, observer, ObservationLease,
		ObservationOpRecover, ObservationOutcomeRecovered, 5*time.Second)
	assertRunIdentity(t, recovered, "claim/fixture/observe_recovery", "claim.fixture_parent_observe_recovery")
	if recovered.CommandKey != "recover" ||
		recovered.CommandID != CommandID(claimed.Commands[0].CommandID.String()) {
		t.Fatalf("lease recovery observation = %#v", recovered)
	}
}
