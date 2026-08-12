package flow

import (
	"context"
	"testing"
	"time"

	"github.com/google/uuid"
	"github.com/goware/flow/internal/fault"
	"github.com/goware/flow/internal/testpg"
)

func TestWakeHubBroadcastsOneGenerationToEveryScheduler(t *testing.T) {
	hub := newWakeHub()
	seen := hub.snapshot()
	const waiters = 8
	done := make(chan struct{}, waiters)
	for range waiters {
		go func() {
			hub.wait(context.Background(), seen, time.Minute)
			done <- struct{}{}
		}()
	}
	hub.signal()
	for range waiters {
		select {
		case <-done:
		case <-time.After(time.Second):
			t.Fatal("one wake generation did not reach every scheduler")
		}
	}
}

func TestLeaseRenewalResultCannotChangeWorkOutsideItsSnapshot(t *testing.T) {
	active := newActiveCommands()
	oldID, newID, replacedID := uuid.New(), uuid.New(), uuid.New()
	oldAttempt, newAttempt, replacedAttempt, replacementAttempt := uuid.New(), uuid.New(), uuid.New(), uuid.New()
	oldCancelled := make(chan struct{}, 1)
	newCancelled := make(chan struct{}, 1)
	replacementCancelled := make(chan struct{}, 1)
	active.register(activeCommand{commandID: oldID, attemptID: oldAttempt, cancel: func(error) { oldCancelled <- struct{}{} }})
	active.register(activeCommand{commandID: newID, attemptID: newAttempt, cancel: func(error) { newCancelled <- struct{}{} }})
	// A newer retry of the same logical command may replace the snapshotted
	// attempt while its renewal query is in flight.
	active.register(activeCommand{commandID: replacedID, attemptID: replacementAttempt, cancel: func(error) { replacementCancelled <- struct{}{} }})
	active.renewed(replacedID, replacedAttempt, time.Now().Add(time.Hour))
	foundReplacement := false
	for _, value := range active.snapshot() {
		if value.commandID == replacedID {
			foundReplacement = true
			if !value.localExpiry.IsZero() {
				t.Fatal("an older renewal changed a newer command attempt's local expiry")
			}
		}
	}
	if !foundReplacement {
		t.Fatal("replacement command attempt missing")
	}

	if !active.cancelLost(oldID, oldAttempt) {
		t.Fatal("the definitely lost snapshotted attempt was not cancelled")
	}
	if active.cancelLost(replacedID, replacedAttempt) {
		t.Fatal("an older result cancelled a newer command attempt")
	}
	for _, command := range active.snapshot() {
		if command.commandID == oldID {
			t.Fatal("a locally cancelled attempt remained eligible for renewal")
		}
	}
	select {
	case <-oldCancelled:
	default:
		t.Fatal("a definitely lost lease was not cancelled")
	}
	select {
	case <-newCancelled:
		t.Fatal("a command registered after the renewal snapshot was cancelled")
	default:
	}
	select {
	case <-replacementCancelled:
		t.Fatal("a newer attempt for a snapshotted command was cancelled")
	default:
	}
}

func TestActiveCommandUncertainRenewalKeepsOldDeadline(t *testing.T) {
	active := newActiveCommands()
	commandID, attemptID := uuid.New(), uuid.New()
	cancelled := make(chan struct{}, 1)
	oldDeadline := time.Now().Add(time.Minute)
	active.register(activeCommand{
		commandID: commandID, attemptID: attemptID, token: uuid.New(), localExpiry: oldDeadline,
		cancel: func(error) { cancelled <- struct{}{} },
	})

	values := active.snapshot()
	if len(values) != 1 || !values[0].localExpiry.Equal(oldDeadline) {
		t.Fatalf("uncertain renewal deadline = %v, want %v", values, oldDeadline)
	}
	if got := active.cancelExpired(); got != 0 {
		t.Fatalf("cancelExpired() before deadline = %d", got)
	}
	select {
	case <-cancelled:
		t.Fatal("uncertain attempt cancelled before its prior deadline")
	default:
	}
	expired := values[0]
	expired.localExpiry = time.Now().Add(-time.Millisecond)
	active.register(expired)
	if got := active.cancelExpired(); got != 1 {
		t.Fatalf("cancelExpired() at deadline = %d, want 1", got)
	}
	if got := active.cancelExpired(); got != 0 {
		t.Fatalf("cancelExpired() repeated cancellation = %d", got)
	}
	if len(active.snapshot()) != 0 {
		t.Fatal("expired local attempt remained eligible for renewal")
	}
}

func TestActiveCommandShutdownCancellationExcludesRenewal(t *testing.T) {
	active := newActiveCommands()
	commandID, attemptID := uuid.New(), uuid.New()
	cancelled := make(chan error, 1)
	active.register(activeCommand{
		commandID: commandID, attemptID: attemptID, token: uuid.New(), localExpiry: time.Now().Add(time.Minute),
		cancel: func(cause error) { cancelled <- cause },
	})
	if !active.cancelAttempt(commandID, attemptID, errRuntimeShutdown) {
		t.Fatal("shutdown did not cancel the registered attempt")
	}
	if len(active.snapshot()) != 0 {
		t.Fatal("shutdown-cancelled attempt remained eligible for renewal")
	}
	select {
	case cause := <-cancelled:
		if cause != errRuntimeShutdown {
			t.Fatalf("shutdown cancellation cause = %v", cause)
		}
	default:
		t.Fatal("shutdown cancellation did not reach the attempt context")
	}
}

func TestLeaseServiceIntervalsStayInsideLeaseWindow(t *testing.T) {
	for _, lease := range []time.Duration{30 * time.Millisecond, 120 * time.Millisecond, 60 * time.Second} {
		if timeout := commandRenewalTimeout(lease); timeout <= 0 || timeout >= lease {
			t.Fatalf("commandRenewalTimeout(%s) = %s", lease, timeout)
		}
		if interval := leaseWatchdogInterval(lease); interval <= 0 || interval >= lease {
			t.Fatalf("leaseWatchdogInterval(%s) = %s", lease, interval)
		}
	}
}

func TestLeaseRenewalErrorIsObservedWithoutFalseSuccess(t *testing.T) {
	t.Parallel()

	database := testpg.Open(t)
	ctx := context.Background()
	if err := Migrate(ctx, database.DB, WithSchema(database.Schema)); err != nil {
		t.Fatal(err)
	}
	observer := &recordingObserver{}
	command := DefineCommand[None, None]("runtime.renewal_observation", 1)
	runtime, err := New(database.DB, WithSchema(database.Schema), WithNotifications(false),
		WithWorkerConcurrency(1), WithPollInterval(5*time.Millisecond), withCommandLeaseForTest(120*time.Millisecond),
		WithShutdownGrace(time.Second), WithObserver(observer))
	if err != nil {
		t.Fatal(err)
	}
	runtime.faults = fault.Func(func(_ context.Context, point fault.Point) error {
		if point == fault.RenewBeforeResult {
			return fault.Injected(point)
		}
		return nil
	})
	started := make(chan struct{}, 1)
	if err := runtime.Register(Handle(command, func(ctx context.Context, _ *Work[None]) (None, error) {
		select {
		case started <- struct{}{}:
		default:
		}
		<-ctx.Done()
		return None{}, ctx.Err()
	})); err != nil {
		t.Fatal(err)
	}
	cancel, runResult := startRuntime(t, runtime)
	if _, err := command.Enqueue(ctx, runtime, "renewal/observation", None{}); err != nil {
		cancel()
		t.Fatal(err)
	}
	select {
	case <-started:
	case <-time.After(3 * time.Second):
		cancel()
		t.Fatal("handler did not start")
	}
	waitForObservation(t, observer, "renew", "error", 1, 2*time.Second)
	waitForObservation(t, observer, "local_cancel", "expired", 1, 2*time.Second)
	for _, observation := range observer.snapshot() {
		if observation.Operation == "renew" && observation.Outcome == "ok" {
			cancel()
			t.Fatalf("renewal error was followed by success: %#v", observer.snapshot())
		}
	}
	stopRuntime(t, cancel, runResult)
}

func TestLockedSettlementIsUncertainWhileUnrelatedLeaseRenews(t *testing.T) {
	t.Parallel()

	database := testpg.Open(t)
	ctx := context.Background()
	if err := Migrate(ctx, database.DB, WithSchema(database.Schema)); err != nil {
		t.Fatal(err)
	}
	observer := &recordingObserver{}
	settling := DefineCommand[None, None]("runtime.locked_renewal_settling", 1)
	unrelated := DefineCommand[None, None]("runtime.locked_renewal_unrelated", 1)
	runtime, err := New(database.DB, WithSchema(database.Schema), WithNotifications(false),
		WithWorkerConcurrency(2), WithPollInterval(5*time.Millisecond), withCommandLeaseForTest(3*time.Second),
		WithObserver(observer))
	if err != nil {
		t.Fatal(err)
	}
	commitStarted := make(chan struct{}, 1)
	releaseCommit := make(chan struct{})
	commitCancelled := make(chan struct{}, 1)
	unrelatedStarted := make(chan struct{}, 1)
	releaseUnrelated := make(chan struct{})
	if err := runtime.Register(
		Handle(settling, func(context.Context, *Work[None]) (None, error) {
			return None{}, nil
		}, WithCommit(func(ctx context.Context, _ Tx, _ Commit[None, None]) error {
			select {
			case commitStarted <- struct{}{}:
			default:
			}
			select {
			case <-releaseCommit:
				return nil
			case <-ctx.Done():
				commitCancelled <- struct{}{}
				return ctx.Err()
			}
		})),
		Handle(unrelated, func(context.Context, *Work[None]) (None, error) {
			select {
			case unrelatedStarted <- struct{}{}:
			default:
			}
			<-releaseUnrelated
			return None{}, nil
		}),
	); err != nil {
		t.Fatal(err)
	}
	cancel, runResult := startRuntime(t, runtime)
	defer func() {
		select {
		case <-releaseCommit:
		default:
			close(releaseCommit)
		}
		select {
		case <-releaseUnrelated:
		default:
			close(releaseUnrelated)
		}
		stopRuntime(t, cancel, runResult)
	}()
	settlingRun, err := settling.Enqueue(ctx, runtime, "renewal/locked-settlement", None{})
	if err != nil {
		t.Fatal(err)
	}
	unrelatedRun, err := unrelated.Enqueue(ctx, runtime, "renewal/unrelated", None{})
	if err != nil {
		t.Fatal(err)
	}
	select {
	case <-commitStarted:
	case <-time.After(3 * time.Second):
		t.Fatal("settlement callback did not start")
	}
	select {
	case <-unrelatedStarted:
	case <-time.After(3 * time.Second):
		t.Fatal("unrelated handler did not start")
	}
	waitForObservation(t, observer, "renew_result", "uncertain", 1, 2*time.Second)
	waitForObservation(t, observer, "renew_result", "renewed", 1, 2*time.Second)
	select {
	case <-commitCancelled:
		t.Fatal("locked settlement was cancelled merely because renewal skipped its row")
	default:
	}
	close(releaseCommit)
	close(releaseUnrelated)
	waitForRunStatus(t, database.Schema, database.DB.Conn, settlingRun.ID, "succeeded", 3*time.Second)
	waitForRunStatus(t, database.Schema, database.DB.Conn, unrelatedRun.ID, "succeeded", 3*time.Second)
	for _, runID := range []RunID{settlingRun.ID, unrelatedRun.ID} {
		trace, err := Trace(ctx, runtime, runID)
		if err != nil || len(trace.Commands) != 1 || len(trace.Commands[0].Attempts) != 1 {
			t.Fatalf("trace %s = %#v, %v", runID, trace, err)
		}
	}
}

func TestLeaseWatchdogCancelsAfterPoolStarvationAndAllowsTakeover(t *testing.T) {
	t.Parallel()

	database := testpg.OpenWithMaxConns(t, 2)
	ctx := context.Background()
	if err := Migrate(ctx, database.DB, WithSchema(database.Schema)); err != nil {
		t.Fatal(err)
	}
	observer := &recordingObserver{}
	command := DefineCommand[None, None]("runtime.renewal_pool_starvation", 1)
	first, err := New(database.DB, WithSchema(database.Schema), WithNotifications(false),
		WithWorkerConcurrency(1), WithPollInterval(5*time.Millisecond), withCommandLeaseForTest(120*time.Millisecond),
		WithShutdownGrace(time.Second), WithObserver(observer))
	if err != nil {
		t.Fatal(err)
	}
	started := make(chan struct{}, 1)
	if err := first.Register(Handle(command, func(ctx context.Context, _ *Work[None]) (None, error) {
		started <- struct{}{}
		<-ctx.Done()
		return None{}, ctx.Err()
	})); err != nil {
		t.Fatal(err)
	}
	cancelFirst, firstResult := startRuntime(t, first)
	run, err := command.Enqueue(ctx, first, "renewal/pool-starvation", None{})
	if err != nil {
		cancelFirst()
		t.Fatal(err)
	}
	select {
	case <-started:
	case <-time.After(3 * time.Second):
		cancelFirst()
		t.Fatal("first handler did not start")
	}
	connectionOne, err := database.DB.Conn.Acquire(ctx)
	if err != nil {
		cancelFirst()
		t.Fatal(err)
	}
	connectionTwo, err := database.DB.Conn.Acquire(ctx)
	if err != nil {
		connectionOne.Release()
		cancelFirst()
		t.Fatal(err)
	}
	released := false
	defer func() {
		if !released {
			connectionOne.Release()
			connectionTwo.Release()
		}
	}()
	waitForObservation(t, observer, "renew", "error", 1, 2*time.Second)
	waitForObservation(t, observer, "local_cancel", "expired", 1, 2*time.Second)
	cancelFirst()
	connectionOne.Release()
	connectionTwo.Release()
	released = true
	select {
	case err := <-firstResult:
		if err != nil {
			t.Fatalf("first Run() error = %v", err)
		}
	case <-time.After(3 * time.Second):
		t.Fatal("first runtime did not drain after pool capacity returned")
	}

	second, err := New(database.DB, WithSchema(database.Schema), WithNotifications(false),
		WithWorkerConcurrency(1), WithPollInterval(5*time.Millisecond), withCommandLeaseForTest(120*time.Millisecond))
	if err != nil {
		t.Fatal(err)
	}
	if err := second.Register(Handle(command, func(context.Context, *Work[None]) (None, error) {
		return None{}, nil
	})); err != nil {
		t.Fatal(err)
	}
	cancelSecond, secondResult := startRuntime(t, second)
	waitForRunStatus(t, database.Schema, database.DB.Conn, run.ID, "succeeded", 5*time.Second)
	stopRuntime(t, cancelSecond, secondResult)
	trace, err := Trace(ctx, mustReader(t, database), run.ID)
	if err != nil || len(trace.Commands) != 1 || len(trace.Commands[0].Attempts) != 2 ||
		trace.Commands[0].Attempts[1].Classification != "succeeded" {
		t.Fatalf("pool-starvation trace = %#v, %v", trace, err)
	}
}
