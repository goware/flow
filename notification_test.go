package flow

import (
	"context"
	"errors"
	"testing"
	"time"

	"github.com/goware/flow/internal/canonical"
	"github.com/goware/flow/internal/failure"
	"github.com/goware/flow/internal/fault"
	"github.com/goware/flow/internal/pgschema"
	retrypolicy "github.com/goware/flow/internal/retry"
	"github.com/goware/flow/internal/store"
	"github.com/goware/flow/internal/testpg"
	"github.com/jackc/pgx/v5"
)

func TestNotificationHintsCommitButDoNotRollback(t *testing.T) {
	t.Parallel()

	database := testpg.Open(t)
	ctx := context.Background()
	if err := Migrate(ctx, database.DB, WithSchema(database.Schema)); err != nil {
		t.Fatalf("Migrate() error = %v", err)
	}
	runtime, err := New(database.DB, WithSchema(database.Schema), WithNotifications(true))
	if err != nil {
		t.Fatalf("New() error = %v", err)
	}
	command := DefineCommand[runtimeArgs, runtimeResult]("notify.commit", 1)

	listener, err := pgx.ConnectConfig(ctx, database.DB.Conn.Config().ConnConfig)
	if err != nil {
		t.Fatalf("connect listener: %v", err)
	}
	defer listener.Close(context.Background())
	channel := runtime.store.NotificationChannel()
	if _, err := listener.Exec(ctx, "LISTEN "+pgx.Identifier{channel}.Sanitize()); err != nil {
		t.Fatalf("LISTEN: %v", err)
	}

	tx, err := database.DB.Conn.Begin(ctx)
	if err != nil {
		t.Fatalf("Begin(commit) error = %v", err)
	}
	exec, err := command.Enqueue(ctx, runtime.InTx(tx), "notify/commit", runtimeArgs{})
	if err != nil {
		_ = tx.Rollback(ctx)
		t.Fatalf("Enqueue(commit) error = %v", err)
	}
	assertNoNotification(t, listener, 150*time.Millisecond)
	if err := tx.Commit(ctx); err != nil {
		t.Fatalf("Commit() error = %v", err)
	}
	notifyCtx, cancelNotify := context.WithTimeout(ctx, 2*time.Second)
	notification, err := listener.WaitForNotification(notifyCtx)
	cancelNotify()
	if err != nil {
		t.Fatalf("WaitForNotification(commit) error = %v", err)
	}
	id, valid := store.ParseNotificationHint(notification.Payload)
	if !valid || id.String() != string(exec.ID) || notification.Channel != channel {
		t.Fatalf("notification = %#v, parsed=%s/%t", notification, id, valid)
	}
	if len(notification.Payload) > 128 {
		t.Fatalf("notification payload contains more than a bounded identity hint: %q", notification.Payload)
	}

	tx, err = database.DB.Conn.Begin(ctx)
	if err != nil {
		t.Fatalf("Begin() error = %v", err)
	}
	if _, err := command.Enqueue(ctx, runtime.InTx(tx), "notify/rollback", runtimeArgs{}); err != nil {
		_ = tx.Rollback(ctx)
		t.Fatalf("Enqueue(rollback) error = %v", err)
	}
	if err := tx.Rollback(ctx); err != nil {
		t.Fatalf("Rollback() error = %v", err)
	}
	assertNoNotification(t, listener, 150*time.Millisecond)
}

func TestReplaceCurrentRunNotifiesOnlyForCommittedRunnableSuccessor(t *testing.T) {
	t.Parallel()
	database := testpg.Open(t)
	ctx := context.Background()
	if err := Migrate(ctx, database.DB, WithSchema(database.Schema)); err != nil {
		t.Fatal(err)
	}
	runtime, err := New(database.DB, WithSchema(database.Schema), WithNotifications(true))
	if err != nil {
		t.Fatal(err)
	}
	command := DefineCommand[None, None]("notify.replace", 1)
	listener := openNotificationListener(t, database, runtime)
	original, err := command.Enqueue(ctx, runtime, "notify/replace", None{}, WithLiveKey(), WithStartDelay(time.Hour))
	if err != nil {
		t.Fatal(err)
	}
	assertNoNotification(t, listener, 150*time.Millisecond)
	replaced, err := command.ReplaceCurrentRun(ctx, runtime, original.ID, "notify/replace", None{}, "runnable replacement", WithLiveKey())
	if err != nil || !replaced.Replaced {
		t.Fatalf("replacement = %#v, %v", replaced, err)
	}
	waitForNotificationHint(t, listener, replaced.Run.ID, 2*time.Second)

	rolledBackOriginal, err := command.Enqueue(ctx, runtime, "notify/replace-rollback", None{}, WithLiveKey(), WithStartDelay(time.Hour))
	if err != nil {
		t.Fatal(err)
	}
	assertNoNotification(t, listener, 150*time.Millisecond)
	tx, err := database.DB.Conn.Begin(ctx)
	if err != nil {
		t.Fatal(err)
	}
	if _, err := command.ReplaceCurrentRun(ctx, runtime.InTx(tx), rolledBackOriginal.ID,
		"notify/replace-rollback", None{}, "rolled back replacement", WithLiveKey()); err != nil {
		_ = tx.Rollback(ctx)
		t.Fatal(err)
	}
	assertNoNotification(t, listener, 150*time.Millisecond)
	if err := tx.Rollback(ctx); err != nil {
		t.Fatal(err)
	}
	assertNoNotification(t, listener, 150*time.Millisecond)
}

func TestNotificationHintsOnlyForRunnableTransitions(t *testing.T) {
	t.Parallel()

	database := testpg.Open(t)
	ctx := context.Background()
	if err := Migrate(ctx, database.DB, WithSchema(database.Schema)); err != nil {
		t.Fatalf("Migrate() error = %v", err)
	}
	event := DefineEvent[None]("notify.release")
	command := DefineCommand[None, None]("notify.gated", 1)
	api, err := New(database.DB, WithSchema(database.Schema), WithNotifications(true))
	if err != nil {
		t.Fatalf("New(api) error = %v", err)
	}
	worker, err := New(database.DB, WithSchema(database.Schema), WithNotifications(true),
		WithPollInterval(5*time.Second), WithWorkerConcurrency(1))
	if err != nil {
		t.Fatalf("New(worker) error = %v", err)
	}
	if err := worker.Register(Handle(command, func(context.Context, *Work[None]) (None, error) {
		return None{}, nil
	})); err != nil {
		t.Fatalf("Register() error = %v", err)
	}
	listener := openNotificationListener(t, database, api)
	cancelRun, runResult := startRuntime(t, worker)
	defer stopRuntime(t, cancelRun, runResult)

	exec, err := command.Enqueue(ctx, api, "notify/gated", None{}, WaitFor(event, "ready"), Within(time.Second))
	if err != nil {
		t.Fatalf("Enqueue(gated) error = %v", err)
	}
	assertNoNotification(t, listener, 150*time.Millisecond)
	if err := event.Deliver(ctx, api, exec.ID, "unrelated", None{}); err != nil {
		t.Fatalf("Emit(unrelated) error = %v", err)
	}
	assertNoNotification(t, listener, 150*time.Millisecond)

	started := time.Now()
	if err := event.Deliver(ctx, api, exec.ID, "ready", None{}); err != nil {
		t.Fatalf("Emit(ready) error = %v", err)
	}
	waitForNotificationHint(t, listener, exec.ID, 2*time.Second)
	waitForRunStatus(t, database.Schema, database.DB.Conn, exec.ID, "succeeded", 2*time.Second)
	if elapsed := time.Since(started); elapsed >= 2*time.Second {
		t.Fatalf("event-release notification wake took %s with five-second polling", elapsed)
	}
}

func TestClaimAndTerminalSettlementDoNotNotify(t *testing.T) {
	t.Parallel()

	database := testpg.Open(t)
	ctx := context.Background()
	if err := Migrate(ctx, database.DB, WithSchema(database.Schema)); err != nil {
		t.Fatalf("Migrate() error = %v", err)
	}
	runtime, err := New(database.DB, WithSchema(database.Schema), WithNotifications(true))
	if err != nil {
		t.Fatalf("New() error = %v", err)
	}
	command := DefineCommand[None, None]("notify.journal_only", 1)
	listener := openNotificationListener(t, database, runtime)
	exec, err := command.Enqueue(ctx, runtime, "notify/journal-only", None{})
	if err != nil {
		t.Fatalf("Enqueue() error = %v", err)
	}
	waitForNotificationHint(t, listener, exec.ID, 2*time.Second)

	candidates, err := runtime.store.ProbeCommands(ctx, []store.CommandKind{{Name: command.Name(), Version: command.Version()}}, 1)
	if err != nil || len(candidates) != 1 {
		t.Fatalf("ProbeCommands() = %+v, %v", candidates, err)
	}
	claimed, err := runtime.store.ClaimCommand(ctx, candidates[0], time.Minute, "notification-test", fault.None{})
	if err != nil || claimed.Command == nil {
		t.Fatalf("ClaimCommand() = %+v, %v", claimed, err)
	}
	assertNoNotification(t, listener, 150*time.Millisecond)
	result, err := canonical.Marshal(None{}, 0)
	if err != nil {
		t.Fatalf("encode result: %v", err)
	}
	if _, err := runtime.store.SettleCommandSuccess(ctx, store.CommandSuccess{Claim: *claimed.Command, Result: result}, fault.None{}); err != nil {
		t.Fatalf("SettleCommandSuccess() error = %v", err)
	}
	assertNoNotification(t, listener, 150*time.Millisecond)
}

func TestImmediateRetryAndLeaseRecoveryNotify(t *testing.T) {
	t.Parallel()

	database := testpg.Open(t)
	ctx := context.Background()
	if err := Migrate(ctx, database.DB, WithSchema(database.Schema)); err != nil {
		t.Fatalf("Migrate() error = %v", err)
	}
	runtime, err := New(database.DB, WithSchema(database.Schema), WithNotifications(true))
	if err != nil {
		t.Fatalf("New() error = %v", err)
	}
	command := DefineCommand[None, None]("notify.retry", 1, WithRetry(Attempts(3)))
	listener := openNotificationListener(t, database, runtime)
	claim := startAndClaimForNotification(t, ctx, runtime, listener, command, "notify/immediate-retry")
	if _, err := runtime.store.SettleCommandConclusion(ctx, store.CommandConclusion{
		Claim: claim, Classification: retrypolicy.ClassInterrupted,
		Failure: failure.Value{Code: "interrupted", Message: "test interruption"},
	}, fault.None{}); err != nil {
		t.Fatalf("SettleCommandConclusion(interrupted) error = %v", err)
	}
	waitForNotificationHint(t, listener, RunID(claim.RunID.String()), 2*time.Second)
	candidates, err := runtime.store.ProbeCommands(ctx,
		[]store.CommandKind{{Name: command.Name(), Version: command.Version()}}, 10)
	if err != nil {
		t.Fatalf("ProbeCommands(future retry) error = %v", err)
	}
	var retryCandidate *store.CommandCandidate
	for index := range candidates {
		if candidates[index].RunID == claim.RunID {
			retryCandidate = &candidates[index]
			break
		}
	}
	if retryCandidate == nil {
		t.Fatal("immediate retry was not claimable")
	}
	retryClaim, err := runtime.store.ClaimCommand(ctx, *retryCandidate, time.Minute, "notification-test", fault.None{})
	if err != nil || retryClaim.Command == nil {
		t.Fatalf("ClaimCommand(future retry) = %+v, %v", retryClaim, err)
	}
	assertNoNotification(t, listener, 150*time.Millisecond)
	if _, err := runtime.store.SettleCommandConclusion(ctx, store.CommandConclusion{
		Claim: *retryClaim.Command, Classification: retrypolicy.ClassRetryable,
		Failure: failure.Value{Code: "retryable", Message: "test retry"},
	}, fault.None{}); err != nil {
		t.Fatalf("SettleCommandConclusion(future retry) error = %v", err)
	}
	assertNoNotification(t, listener, 150*time.Millisecond)

	recoveryClaim := startAndClaimForNotification(t, ctx, runtime, listener, command, "notify/lease-recovery")
	if _, err := database.DB.Conn.Exec(ctx, `UPDATE `+pgschema.Table(database.Schema, "flow_command_queue")+`
		SET lease_expires_at=clock_timestamp()-interval '1 second' WHERE command_id=$1`, recoveryClaim.CommandID); err != nil {
		t.Fatalf("expire lease: %v", err)
	}
	changed, err := runtime.store.RecoverExpiredCommandLease(ctx, store.ExpiredLeaseCandidate{
		CommandID: recoveryClaim.CommandID, RunID: recoveryClaim.RunID,
	})
	if err != nil || !changed {
		t.Fatalf("RecoverExpiredCommandLease() = %t, %v", changed, err)
	}
	waitForNotificationHint(t, listener, RunID(recoveryClaim.RunID.String()), 2*time.Second)
}

func openNotificationListener(t *testing.T, database testpg.Database, runtime *Runtime) *pgx.Conn {
	t.Helper()
	ctx := context.Background()
	listener, err := pgx.ConnectConfig(ctx, database.DB.Conn.Config().ConnConfig)
	if err != nil {
		t.Fatalf("connect notification listener: %v", err)
	}
	t.Cleanup(func() { _ = listener.Close(context.Background()) })
	channel := runtime.store.NotificationChannel()
	if _, err := listener.Exec(ctx, "LISTEN "+pgx.Identifier{channel}.Sanitize()); err != nil {
		t.Fatalf("LISTEN: %v", err)
	}
	return listener
}

func waitForNotificationHint(t *testing.T, listener *pgx.Conn, runID RunID, timeout time.Duration) {
	t.Helper()
	ctx, cancel := context.WithTimeout(context.Background(), timeout)
	defer cancel()
	notification, err := listener.WaitForNotification(ctx)
	if err != nil {
		t.Fatalf("WaitForNotification() error = %v", err)
	}
	id, valid := store.ParseNotificationHint(notification.Payload)
	if !valid || id.String() != string(runID) {
		t.Fatalf("notification = %#v, parsed=%s/%t, want run %s", notification, id, valid, runID)
	}
}

func assertNoNotification(t *testing.T, listener *pgx.Conn, timeout time.Duration) {
	t.Helper()
	ctx, cancel := context.WithTimeout(context.Background(), timeout)
	defer cancel()
	_, err := listener.WaitForNotification(ctx)
	if !errors.Is(err, context.DeadlineExceeded) {
		t.Fatalf("notification error = %v, want deadline", err)
	}
}

func startAndClaimForNotification(
	t *testing.T,
	ctx context.Context,
	runtime *Runtime,
	listener *pgx.Conn,
	command Command[None, None],
	key string,
) store.ClaimedCommand {
	t.Helper()
	exec, err := command.Enqueue(ctx, runtime, key, None{})
	if err != nil {
		t.Fatalf("Enqueue(%s) error = %v", key, err)
	}
	waitForNotificationHint(t, listener, exec.ID, 2*time.Second)
	candidates, err := runtime.store.ProbeCommands(ctx,
		[]store.CommandKind{{Name: command.Name(), Version: command.Version()}}, 10)
	if err != nil {
		t.Fatalf("ProbeCommands(%s) error = %v", key, err)
	}
	for _, candidate := range candidates {
		if candidate.RunID.String() != string(exec.ID) {
			continue
		}
		claimed, err := runtime.store.ClaimCommand(ctx, candidate, time.Minute, "notification-test", fault.None{})
		if err != nil || claimed.Command == nil {
			t.Fatalf("ClaimCommand(%s) = %+v, %v", key, claimed, err)
		}
		assertNoNotification(t, listener, 150*time.Millisecond)
		return *claimed.Command
	}
	t.Fatalf("no command candidate found for %s", exec.ID)
	return store.ClaimedCommand{}
}

func TestDistributedNotificationAndReconnectCatchUp(t *testing.T) {
	t.Parallel()

	database := testpg.Open(t)
	ctx := context.Background()
	if err := Migrate(ctx, database.DB, WithSchema(database.Schema)); err != nil {
		t.Fatalf("Migrate() error = %v", err)
	}
	command := DefineCommand[runtimeArgs, runtimeResult]("notify.distributed", 1)
	api, err := New(database.DB, WithSchema(database.Schema), WithNotifications(true))
	if err != nil {
		t.Fatalf("New(api) error = %v", err)
	}
	observer := &recordingObserver{}
	worker, err := New(database.DB, WithSchema(database.Schema), WithNotifications(true),
		WithObserver(observer), WithPollInterval(5*time.Second), WithWorkerConcurrency(1))
	if err != nil {
		t.Fatalf("New(worker) error = %v", err)
	}
	if err := worker.Register(Handle(command, func(_ context.Context, work *Work[runtimeArgs]) (runtimeResult, error) {
		return runtimeResult{Value: work.Info().CommandKey}, nil
	})); err != nil {
		t.Fatalf("Register() error = %v", err)
	}
	cancelRun, runResult := startRuntime(t, worker)
	waitForObservation(t, observer, "notify_listener", "listening", 1, 2*time.Second)

	started := time.Now()
	first, err := command.Enqueue(ctx, api, "notify/remote", runtimeArgs{})
	if err != nil {
		t.Fatalf("Enqueue(remote) error = %v", err)
	}
	waitForRunStatus(t, database.Schema, database.DB.Conn, first.ID, "succeeded", 2*time.Second)
	if elapsed := time.Since(started); elapsed >= 2*time.Second {
		t.Fatalf("notification wake took %s with five-second polling", elapsed)
	}
	if _, err := database.DB.Conn.Exec(ctx, `SELECT pg_notify($1,$2)`, worker.store.NotificationChannel(), `{"v":99}`); err != nil {
		t.Fatalf("publish future notification hint: %v", err)
	}
	waitForObservation(t, observer, "notify_hint", "broad_wake", 1, 2*time.Second)

	var terminated bool
	applicationName := "flow-listener-" + worker.instanceID.String()
	if err := database.DB.Conn.QueryRow(ctx, `SELECT pg_terminate_backend(pid) FROM pg_stat_activity
		WHERE application_name=$1 AND pid <> pg_backend_pid() LIMIT 1`, applicationName).Scan(&terminated); err != nil {
		t.Fatalf("terminate listener: %v", err)
	}
	if !terminated {
		t.Fatal("listener backend was not terminated")
	}
	waitForObservation(t, observer, "notify_listener", "reconnecting", 1, 2*time.Second)

	// This commit races the disconnected window. Either a reconnected listener
	// receives its hint or the mandatory post-LISTEN catch-up generation finds
	// it; the five-second correctness poll is intentionally too slow to help.
	second, err := command.Enqueue(ctx, api, "notify/reconnect", runtimeArgs{})
	if err != nil {
		t.Fatalf("Enqueue(reconnect) error = %v", err)
	}
	waitForRunStatus(t, database.Schema, database.DB.Conn, second.ID, "succeeded", 2*time.Second)
	waitForObservation(t, observer, "notify_listener", "listening", 2, 2*time.Second)
	stopRuntime(t, cancelRun, runResult)
}

func waitForObservation(t *testing.T, observer *recordingObserver, operation, outcome string, count int, timeout time.Duration) {
	t.Helper()
	deadline := time.Now().Add(timeout)
	for time.Now().Before(deadline) {
		matched := 0
		for _, observation := range observer.snapshot() {
			if observation.Operation == operation && observation.Outcome == outcome {
				matched++
			}
		}
		if matched >= count {
			return
		}
		time.Sleep(time.Millisecond)
	}
	t.Fatalf("did not observe %s/%s %d times: %#v", operation, outcome, count, observer.snapshot())
}
