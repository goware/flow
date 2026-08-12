package flow

import (
	"context"
	"errors"
	"sync/atomic"
	"testing"
	"time"

	"github.com/goware/flow/internal/fault"
	"github.com/goware/flow/internal/pgschema"
	"github.com/goware/flow/internal/store"
	"github.com/goware/flow/internal/testpg"
)

func countObservations(observations []Observation, kind ObservationKind, operation, outcome string, id RunID) int {
	matched := 0
	for _, observation := range observations {
		if observation.Kind == kind && observation.Operation == operation &&
			observation.Outcome == outcome && observation.RunID == id {
			matched++
		}
	}
	return matched
}

func TestSettlementEmitsRunTerminalFactOncePerRun(t *testing.T) {
	t.Parallel()

	database := testpg.Open(t)
	ctx := context.Background()
	if err := Migrate(ctx, database.DB, WithSchema(database.Schema)); err != nil {
		t.Fatal(err)
	}
	succeeding := DefineCommand[None, None]("observe.succeeding", 1)
	exhausting := DefineCommand[None, None]("observe.exhausting", 1, WithRetry(Attempts(1)))
	permanent := DefineCommand[None, None]("observe.permanent", 1)
	observer := &recordingObserver{}
	runtime, err := New(database.DB, WithSchema(database.Schema), WithNotifications(false),
		WithPollInterval(5*time.Millisecond), WithObserver(observer))
	if err != nil {
		t.Fatal(err)
	}
	if err := runtime.Register(
		Handle(succeeding, func(context.Context, *Work[None]) (None, error) { return None{}, nil }),
		Handle(exhausting, func(context.Context, *Work[None]) (None, error) {
			return None{}, errors.New("retryable failure")
		}),
		Handle(permanent, func(context.Context, *Work[None]) (None, error) {
			return None{}, Permanent(errors.New("permanent failure"))
		}),
	); err != nil {
		t.Fatal(err)
	}
	cancelRun, runResult := startRuntime(t, runtime)
	defer stopRuntime(t, cancelRun, runResult)

	good, err := succeeding.Enqueue(ctx, runtime, "observe/succeeding", None{})
	if err != nil {
		t.Fatal(err)
	}
	spent, err := exhausting.Enqueue(ctx, runtime, "observe/exhausting", None{})
	if err != nil {
		t.Fatal(err)
	}
	broken, err := permanent.Enqueue(ctx, runtime, "observe/permanent", None{})
	if err != nil {
		t.Fatal(err)
	}
	waitForRunStatus(t, database.Schema, database.DB.Conn, good.ID, "succeeded", 5*time.Second)
	waitForRunStatus(t, database.Schema, database.DB.Conn, spent.ID, "failed", 5*time.Second)
	waitForRunStatus(t, database.Schema, database.DB.Conn, broken.ID, "failed", 5*time.Second)
	waitForObservation(t, observer, ObservationOpTerminal, ObservationOutcomeFailed, 2, time.Second)
	observations := observer.snapshot()

	if got := countObservations(observations, ObservationRun, ObservationOpTerminal, ObservationOutcomeSucceeded, good.ID); got != 1 {
		t.Fatalf("succeeded run terminal observations = %d, want 1", got)
	}
	for _, id := range []RunID{spent.ID, broken.ID} {
		if got := countObservations(observations, ObservationRun, ObservationOpTerminal, ObservationOutcomeFailed, id); got != 1 {
			t.Fatalf("failed run %s terminal observations = %d, want 1", id, got)
		}
	}
	// Retry-budget exhaustion is distinguishable from a permanent failure.
	if got := countObservations(observations, ObservationAttempt, ObservationOpConcludeExhausted, ObservationOutcomeFailed, spent.ID); got != 1 {
		t.Fatalf("exhausted conclude observations = %d, want 1", got)
	}
	if got := countObservations(observations, ObservationAttempt, ObservationOpConclude, ObservationOutcomeFailed, broken.ID); got != 1 {
		t.Fatalf("permanent conclude observations = %d, want 1", got)
	}
	if got := countObservations(observations, ObservationAttempt, ObservationOpConcludeExhausted, ObservationOutcomeFailed, broken.ID); got != 0 {
		t.Fatalf("permanent failure reported budget exhaustion %d times", got)
	}
	for _, observation := range observations {
		if observation.RunID != good.ID || observation.Kind != ObservationRun {
			continue
		}
		if observation.RunKey != "observe/succeeding" || observation.Definition != succeeding.Name() {
			t.Fatalf("run observation identity = %#v", observation)
		}
	}
}

func TestCancelCommandReportsTheRunItTerminalized(t *testing.T) {
	t.Parallel()

	database := testpg.Open(t)
	ctx := context.Background()
	if err := Migrate(ctx, database.DB, WithSchema(database.Schema)); err != nil {
		t.Fatal(err)
	}
	command := DefineCommand[None, None]("observe.cancelled", 1)
	observer := &recordingObserver{}
	runtime, err := New(database.DB, WithSchema(database.Schema), WithNotifications(false), WithObserver(observer))
	if err != nil {
		t.Fatal(err)
	}
	runtime.observations.run()
	defer runtime.observations.close()
	run, err := command.Enqueue(ctx, runtime, "observe/cancelled", None{})
	if err != nil {
		t.Fatal(err)
	}
	if err := CancelCommand(ctx, runtime, run.RootCommandID, "cancelled by test"); err != nil {
		t.Fatal(err)
	}
	waitForObservation(t, observer, ObservationOpTerminal, ObservationOutcomeFailed, 1, time.Second)
	observations := observer.snapshot()
	if got := countObservations(observations, ObservationCommand, ObservationOpCancel, ObservationOutcomeCancelled, run.ID); got != 1 {
		t.Fatalf("command cancel observations = %d, want 1", got)
	}
	if got := countObservations(observations, ObservationRun, ObservationOpTerminal, ObservationOutcomeFailed, run.ID); got != 1 {
		t.Fatalf("run terminal observations = %d, want 1", got)
	}
}

func TestMaintenancePagesEmitBoundedTerminalFacts(t *testing.T) {
	t.Parallel()

	database := testpg.Open(t)
	ctx := context.Background()
	if err := Migrate(ctx, database.DB, WithSchema(database.Schema)); err != nil {
		t.Fatal(err)
	}
	event := DefineEvent[string]("observe.maintenance_event")
	deadline := DefineCommand[None, None]("observe.maintenance_deadline", 1)
	waiting := DefineCommand[None, None]("observe.maintenance_wait", 1)
	leased := DefineCommand[None, None]("observe.maintenance_lease", 1)
	observer := &recordingObserver{}
	runtime, err := New(database.DB, WithSchema(database.Schema), WithNotifications(false), WithObserver(observer))
	if err != nil {
		t.Fatal(err)
	}
	runtime.observations.run()
	defer runtime.observations.close()

	expiring, err := deadline.Enqueue(ctx, runtime, "observe/maintenance/deadline", None{},
		WithRunDeadline(20*time.Millisecond))
	if err != nil {
		t.Fatal(err)
	}
	stalled, err := waiting.Enqueue(ctx, runtime, "observe/maintenance/wait", None{},
		WaitFor(event, "never"), Within(20*time.Millisecond))
	if err != nil {
		t.Fatal(err)
	}
	recovering, err := leased.Enqueue(ctx, runtime, "observe/maintenance/lease", None{})
	if err != nil {
		t.Fatal(err)
	}
	candidates := probeClaimCandidates(t, runtime,
		[]store.CommandKind{{Name: leased.Name(), Version: leased.Version()}}, 1)
	claimed, err := runtime.store.ClaimCommand(ctx, candidates[0], time.Minute, "maintenance-observation", fault.None{})
	if err != nil || claimed.Command == nil {
		t.Fatalf("ClaimCommand() = %+v, %v", claimed, err)
	}
	if _, err := database.DB.Conn.Exec(ctx, `UPDATE `+pgschema.Table(database.Schema, "flow_command_queue")+`
		SET lease_expires_at=clock_timestamp()-interval '1 second' WHERE command_id=$1`, claimed.Command.CommandID); err != nil {
		t.Fatal(err)
	}
	time.Sleep(30 * time.Millisecond)

	first := runtime.runMaintenancePass(ctx)
	if !first.progressed {
		t.Fatalf("maintenance pass = %+v", first)
	}
	second := runtime.runMaintenancePass(ctx)
	if second.progressed {
		t.Fatalf("second maintenance pass repeated work = %+v", second)
	}
	waitForObservation(t, observer, ObservationOpRecover, ObservationOutcomeRecovered, 1, time.Second)
	observations := observer.snapshot()

	if got := countObservations(observations, ObservationRun, ObservationOpTerminal, ObservationOutcomeExpired, expiring.ID); got != 1 {
		t.Fatalf("expired run terminal observations = %d, want 1", got)
	}
	if got := countObservations(observations, ObservationWait, ObservationOpExpire, ObservationOutcomeExpired, stalled.ID); got != 1 {
		t.Fatalf("wait expiry observations = %d, want 1", got)
	}
	if got := countObservations(observations, ObservationRun, ObservationOpTerminal, ObservationOutcomeFailed, stalled.ID); got != 1 {
		t.Fatalf("wait expiry run terminal observations = %d, want 1", got)
	}
	if got := countObservations(observations, ObservationLease, ObservationOpRecover, ObservationOutcomeRecovered, recovering.ID); got != 1 {
		t.Fatalf("lease recovery observations = %d, want 1", got)
	}
}

func TestConclusionAfterRunDeadlineReportsExpiry(t *testing.T) {
	t.Parallel()

	database := testpg.Open(t)
	ctx := context.Background()
	if err := Migrate(ctx, database.DB, WithSchema(database.Schema)); err != nil {
		t.Fatal(err)
	}
	command := DefineCommand[None, None]("observe.deadline_conclusion", 1)
	observer := &recordingObserver{}
	// Maintenance polls at the poll interval, so an hour of it leaves the
	// expiry to the conclusion path under test.
	runtime, err := New(database.DB, WithSchema(database.Schema), WithNotifications(false),
		WithPollInterval(time.Hour), WithObserver(observer))
	if err != nil {
		t.Fatal(err)
	}
	if err := runtime.Register(Handle(command, func(workerCtx context.Context, _ *Work[None]) (None, error) {
		<-workerCtx.Done()
		return None{}, errors.New("attempt outlived the run deadline")
	})); err != nil {
		t.Fatal(err)
	}
	cancelRun, runResult := startRuntime(t, runtime)
	defer stopRuntime(t, cancelRun, runResult)
	run, err := command.Enqueue(ctx, runtime, "observe/deadline_conclusion", None{},
		WithRunDeadline(200*time.Millisecond))
	if err != nil {
		t.Fatal(err)
	}
	waitForRunStatus(t, database.Schema, database.DB.Conn, run.ID, "expired", 5*time.Second)
	waitForObservation(t, observer, ObservationOpTerminal, ObservationOutcomeExpired, 1, time.Second)
	observations := observer.snapshot()
	if got := countObservations(observations, ObservationAttempt, ObservationOpConclude, ObservationOutcomeExpired, run.ID); got != 1 {
		t.Fatalf("expired conclude observations = %d, want 1", got)
	}
	if got := countObservations(observations, ObservationRun, ObservationOpTerminal, ObservationOutcomeExpired, run.ID); got != 1 {
		t.Fatalf("expired run terminal observations = %d, want 1", got)
	}
}

func TestNotifyListenerConnectFailureIsObserved(t *testing.T) {
	t.Parallel()

	database := testpg.Open(t)
	ctx := context.Background()
	if err := Migrate(ctx, database.DB, WithSchema(database.Schema)); err != nil {
		t.Fatal(err)
	}
	observer := &recordingObserver{}
	runtime, err := New(database.DB, WithSchema(database.Schema), WithNotifications(true),
		WithPollInterval(time.Hour), WithObserver(observer))
	if err != nil {
		t.Fatal(err)
	}
	runtime.faults = fault.Func(func(_ context.Context, point fault.Point) error {
		if point == fault.NotifyConnect {
			return fault.Injected(point)
		}
		return nil
	})
	cancelRun, runResult := startRuntime(t, runtime)
	waitForObservation(t, observer, ObservationOpNotifyListener, ObservationOutcomeConnectError, 1, 2*time.Second)
	stopRuntime(t, cancelRun, runResult)
}

func TestMaintenanceTransitionOutcomesUseTheVocabulary(t *testing.T) {
	t.Parallel()

	observer := &recordingObserver{}
	runtime := &Runtime{observations: newObserverAdapter(observer)}
	runtime.observations.run()
	pageErr := errors.New("maintenance page error")
	cases := []struct {
		operation          string
		attempted, changed int
		err                error
		want               string
	}{
		{ObservationOpDeadline, 2, 2, nil, ObservationOutcomeOK},
		{ObservationOpDeadline, 2, 1, pageErr, ObservationOutcomePartial},
		{ObservationOpWaitExpiry, 2, 0, nil, ObservationOutcomeNoop},
		{ObservationOpWaitExpiry, 2, 1, pageErr, ObservationOutcomePartial},
		{ObservationOpLeaseRecovery, 2, 0, nil, ObservationOutcomeNoop},
		{ObservationOpLeaseRecovery, 2, 0, pageErr, ObservationOutcomeError},
		{ObservationOpLeaseRecovery, 2, 1, pageErr, ObservationOutcomePartial},
	}
	for _, testCase := range cases {
		runtime.observeMaintenanceTransition(context.Background(), testCase.operation,
			testCase.attempted, testCase.changed, testCase.err, time.Now())
	}
	runtime.observations.close()
	select {
	case <-runtime.observations.done:
	case <-time.After(time.Second):
		t.Fatal("observer adapter did not drain")
	}
	observations := observer.snapshot()
	if len(observations) != len(cases) {
		t.Fatalf("maintenance transition observations = %d, want %d", len(observations), len(cases))
	}
	for index, testCase := range cases {
		got := observations[index]
		if got.Kind != ObservationRuntime || got.Operation != testCase.operation || got.Outcome != testCase.want {
			t.Fatalf("transition %d = %s/%s/%s, want runtime/%s/%s", index,
				got.Kind, got.Operation, got.Outcome, testCase.operation, testCase.want)
		}
	}
}

func TestAmbiguousSettlementDoesNotMultiplyTerminalObservations(t *testing.T) {
	t.Parallel()

	database := testpg.Open(t)
	ctx := context.Background()
	if err := Migrate(ctx, database.DB, WithSchema(database.Schema)); err != nil {
		t.Fatal(err)
	}
	command := DefineCommand[None, None]("observe.ambiguous_settlement", 1)
	observer := &recordingObserver{}
	runtime, err := New(database.DB, WithSchema(database.Schema), WithNotifications(false),
		WithPollInterval(5*time.Millisecond), WithObserver(observer))
	if err != nil {
		t.Fatal(err)
	}
	if err := runtime.Register(
		Handle(command, func(context.Context, *Work[None]) (None, error) { return None{}, nil }),
	); err != nil {
		t.Fatal(err)
	}
	var injected atomic.Bool
	runtime.faults = fault.Func(func(_ context.Context, point fault.Point) error {
		if point == fault.SettleCommitAmbiguous && injected.CompareAndSwap(false, true) {
			return fault.Injected(point)
		}
		return nil
	})
	cancelRun, runResult := startRuntime(t, runtime)
	defer stopRuntime(t, cancelRun, runResult)
	run, err := command.Enqueue(ctx, runtime, "observe/ambiguous", None{})
	if err != nil {
		t.Fatal(err)
	}
	waitForRunStatus(t, database.Schema, database.DB.Conn, run.ID, "succeeded", 5*time.Second)

	// The settlement retry may repeat the terminal fact, but the stream stays
	// bounded by the settlement attempt limit: at-least-zero, at-most-few.
	deadline := time.Now().Add(time.Second)
	for time.Now().Before(deadline) {
		if got := countObservations(observer.snapshot(), ObservationRun, ObservationOpTerminal,
			ObservationOutcomeSucceeded, run.ID); got > settlementAttempts {
			t.Fatalf("run terminal observations = %d, want at most %d", got, settlementAttempts)
		}
		time.Sleep(time.Millisecond)
	}
}
