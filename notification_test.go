package flow

import (
	"context"
	"errors"
	"testing"
	"time"

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

	exec, err := command.With(runtime).Execute(ctx, "notify/commit", runtimeArgs{})
	if err != nil {
		t.Fatalf("Execute(commit) error = %v", err)
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

	tx, err := database.DB.Conn.Begin(ctx)
	if err != nil {
		t.Fatalf("Begin() error = %v", err)
	}
	if _, err := command.With(runtime.InTx(tx)).Execute(ctx, "notify/rollback", runtimeArgs{}); err != nil {
		_ = tx.Rollback(ctx)
		t.Fatalf("Execute(rollback) error = %v", err)
	}
	if err := tx.Rollback(ctx); err != nil {
		t.Fatalf("Rollback() error = %v", err)
	}
	quietCtx, cancelQuiet := context.WithTimeout(ctx, 150*time.Millisecond)
	_, err = listener.WaitForNotification(quietCtx)
	cancelQuiet()
	if !errors.Is(err, context.DeadlineExceeded) {
		t.Fatalf("rollback notification error = %v, want deadline", err)
	}
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
	first, err := command.With(api).Execute(ctx, "notify/remote", runtimeArgs{})
	if err != nil {
		t.Fatalf("Execute(remote) error = %v", err)
	}
	waitForExecutionStatus(t, database.Schema, database.DB.Conn, first.ID, "succeeded", 2*time.Second)
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
	second, err := command.With(api).Execute(ctx, "notify/reconnect", runtimeArgs{})
	if err != nil {
		t.Fatalf("Execute(reconnect) error = %v", err)
	}
	waitForExecutionStatus(t, database.Schema, database.DB.Conn, second.ID, "succeeded", 2*time.Second)
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
