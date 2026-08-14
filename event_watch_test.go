package flow

import (
	"context"
	"crypto/sha256"
	"errors"
	goruntime "runtime"
	"strings"
	"sync"
	"sync/atomic"
	"testing"
	"time"

	"github.com/goware/flow/internal/fault"
	"github.com/goware/flow/internal/pgschema"
	"github.com/goware/flow/internal/store"
	"github.com/goware/flow/internal/testpg"
	"github.com/jackc/pgx/v5"
	"github.com/jackc/pgx/v5/pgxpool"
)

type watchedPayload struct {
	Value string `json:"value"`
}

type eventWatchReadContextKey struct{}

type eventWatchReadTracer struct {
	mu    sync.Mutex
	once  sync.Once
	done  chan struct{}
	reads int
}

func (tracer *eventWatchReadTracer) TraceQueryStart(
	ctx context.Context,
	_ *pgx.Conn,
	data pgx.TraceQueryStartData,
) context.Context {
	if strings.Contains(data.SQL, "LEFT JOIN LATERAL") && strings.Contains(data.SQL, "position>$2") {
		return context.WithValue(ctx, eventWatchReadContextKey{}, true)
	}
	return ctx
}

func (tracer *eventWatchReadTracer) TraceQueryEnd(ctx context.Context, _ *pgx.Conn, data pgx.TraceQueryEndData) {
	if data.Err == nil && ctx.Value(eventWatchReadContextKey{}) == true {
		tracer.mu.Lock()
		tracer.reads++
		tracer.mu.Unlock()
		tracer.once.Do(func() { close(tracer.done) })
	}
}

func (tracer *eventWatchReadTracer) readCount() int {
	tracer.mu.Lock()
	defer tracer.mu.Unlock()
	return tracer.reads
}

func TestEventWatchCrossRuntimeOrderAndTerminal(t *testing.T) {
	t.Parallel()
	database := testpg.Open(t)
	ctx := context.Background()
	if err := Migrate(ctx, database.DB, WithSchema(database.Schema)); err != nil {
		t.Fatal(err)
	}
	event := DefineEvent[watchedPayload]("watch.changed")
	otherEvent := DefineEvent[None]("watch.other")
	root := DefineCommand[None, None]("watch.root", 1)
	writer, err := New(database.DB, WithSchema(database.Schema), WithNotifications(true))
	if err != nil {
		t.Fatal(err)
	}
	observer := &recordingObserver{}
	reader, err := New(database.DB, WithSchema(database.Schema), WithNotifications(true),
		WithObserver(observer), WithPollInterval(5*time.Second))
	if err != nil {
		t.Fatal(err)
	}
	run, err := root.Enqueue(ctx, writer, "watch/order", None{}, WithStartDelay(time.Hour))
	if err != nil {
		t.Fatal(err)
	}
	if err := event.Deliver(ctx, writer, run.RunID, "historical", watchedPayload{Value: "old"}); err != nil {
		t.Fatal(err)
	}

	watch, err := event.Watch(ctx, reader, run.RunID)
	if err != nil {
		t.Fatal(err)
	}
	defer watch.Close()
	cancel, runResult := startRuntime(t, reader)
	defer stopRuntime(t, cancel, runResult)
	waitForObservation(t, observer, "notify_listener", "listening", 1, 2*time.Second)

	if err := otherEvent.Deliver(ctx, writer, run.RunID, "ignored", None{}); err != nil {
		t.Fatal(err)
	}
	if err := event.Deliver(ctx, writer, run.RunID, "second", watchedPayload{Value: "two"}); err != nil {
		t.Fatal(err)
	}
	if err := event.Deliver(ctx, writer, run.RunID, "third", watchedPayload{Value: "three"}); err != nil {
		t.Fatal(err)
	}
	for _, want := range []struct {
		key   string
		value string
	}{{"second", "two"}, {"third", "three"}} {
		nextCtx, cancelNext := context.WithTimeout(ctx, 2*time.Second)
		key, payload, nextErr := watch.Next(nextCtx)
		cancelNext()
		if nextErr != nil || key != want.key || payload.Value != want.value {
			t.Fatalf("Next() = %q, %#v, %v; want %q/%q", key, payload, nextErr, want.key, want.value)
		}
	}

	tx, err := database.DB.Conn.Begin(ctx)
	if err != nil {
		t.Fatal(err)
	}
	txClient := writer.InTx(tx)
	if err := event.Deliver(ctx, txClient, run.RunID, "final", watchedPayload{Value: "last"}); err != nil {
		_ = tx.Rollback(ctx)
		t.Fatal(err)
	}
	if err := CancelRun(ctx, txClient, run.RunID, "watch complete"); err != nil {
		_ = tx.Rollback(ctx)
		t.Fatal(err)
	}
	if err := tx.Commit(ctx); err != nil {
		t.Fatal(err)
	}
	finalCtx, cancelFinal := context.WithTimeout(ctx, 2*time.Second)
	key, payload, err := watch.Next(finalCtx)
	cancelFinal()
	if err != nil || key != "final" || payload.Value != "last" {
		t.Fatalf("Next(event before terminal) = %q, %#v, %v", key, payload, err)
	}
	terminalCtx, cancelTerminal := context.WithTimeout(ctx, 2*time.Second)
	_, _, err = watch.Next(terminalCtx)
	cancelTerminal()
	if !errors.Is(err, ErrTerminal) {
		t.Fatalf("Next(terminal) error = %v", err)
	}
}

func TestEventWatchWorkerStagedEmitAndRejectedCommit(t *testing.T) {
	t.Parallel()
	database := testpg.Open(t)
	ctx := context.Background()
	if err := Migrate(ctx, database.DB, WithSchema(database.Schema)); err != nil {
		t.Fatal(err)
	}
	event := DefineEvent[watchedPayload]("watch.staged")
	success := DefineCommand[None, None]("watch.staged.success", 1, WithRetry(Attempts(1)))
	rejected := DefineCommand[None, None]("watch.staged.rejected", 1, WithRetry(Attempts(1)))
	worker, err := New(database.DB, WithSchema(database.Schema), WithWorkerConcurrency(2), WithPollInterval(5*time.Second))
	if err != nil {
		t.Fatal(err)
	}
	if err := worker.Register(
		Handle(success, func(_ context.Context, work *Work[None]) (None, error) {
			if err := Emit(work, event, "success", watchedPayload{Value: "committed"}); err != nil {
				return None{}, err
			}
			return None{}, nil
		}),
		Handle(rejected, func(_ context.Context, work *Work[None]) (None, error) {
			if err := Emit(work, event, "rejected", watchedPayload{Value: "rolled-back"}); err != nil {
				return None{}, err
			}
			return None{}, nil
		}, WithCommit(func(context.Context, Tx, Commit[None, None]) error {
			return NoRetry(errors.New("reject application commit"))
		})),
	); err != nil {
		t.Fatal(err)
	}
	observer := &recordingObserver{}
	reader, err := New(database.DB, WithSchema(database.Schema), WithObserver(observer))
	if err != nil {
		t.Fatal(err)
	}
	cancelReader, readerResult := startRuntime(t, reader)
	defer stopRuntime(t, cancelReader, readerResult)
	waitForObservation(t, observer, "notify_listener", "listening", 1, 2*time.Second)

	successRun, err := success.Enqueue(ctx, worker, "watch/staged/success", None{})
	if err != nil {
		t.Fatal(err)
	}
	rejectedRun, err := rejected.Enqueue(ctx, worker, "watch/staged/rejected", None{})
	if err != nil {
		t.Fatal(err)
	}
	successWatch, err := event.Watch(ctx, reader, successRun.RunID)
	if err != nil {
		t.Fatal(err)
	}
	defer successWatch.Close()
	rejectedWatch, err := event.Watch(ctx, reader, rejectedRun.RunID)
	if err != nil {
		t.Fatal(err)
	}
	defer rejectedWatch.Close()
	cancelWorker, workerResult := startRuntime(t, worker)
	defer stopRuntime(t, cancelWorker, workerResult)

	nextCtx, cancelNext := context.WithTimeout(ctx, 3*time.Second)
	key, payload, err := successWatch.Next(nextCtx)
	cancelNext()
	if err != nil || key != "success" || payload.Value != "committed" {
		t.Fatalf("successful staged Next() = %q, %#v, %v", key, payload, err)
	}
	rejectedCtx, cancelRejected := context.WithTimeout(ctx, 3*time.Second)
	_, _, err = rejectedWatch.Next(rejectedCtx)
	cancelRejected()
	if !errors.Is(err, ErrTerminal) {
		t.Fatalf("rejected staged Next error = %v", err)
	}
	var rejectedEvents int
	if err := database.DB.Conn.QueryRow(ctx, `SELECT count(*) FROM `+pgschema.Table(database.Schema, "flow_journal")+`
		WHERE run_id=$1 AND event_class='application'`, rejectedRun.RunID).Scan(&rejectedEvents); err != nil {
		t.Fatal(err)
	}
	if rejectedEvents != 0 {
		t.Fatalf("rejected WithCommit retained %d application events", rejectedEvents)
	}
}

func TestEventWatchHasNoPeriodicOrUnrelatedRunReads(t *testing.T) {
	t.Parallel()
	tracer := &eventWatchReadTracer{done: make(chan struct{})}
	database := testpg.OpenWithQueryTracer(t, tracer)
	ctx := context.Background()
	if err := Migrate(ctx, database.DB, WithSchema(database.Schema)); err != nil {
		t.Fatal(err)
	}
	event := DefineEvent[None]("watch.idle")
	root := DefineCommand[None, None]("watch.idle.root", 1)
	observer := &recordingObserver{}
	runtime, err := New(database.DB, WithSchema(database.Schema), WithPollInterval(time.Millisecond), WithObserver(observer))
	if err != nil {
		t.Fatal(err)
	}
	run, err := root.Enqueue(ctx, runtime, "watch/idle", None{}, WithStartDelay(time.Hour))
	if err != nil {
		t.Fatal(err)
	}
	other, err := root.Enqueue(ctx, runtime, "watch/idle/other", None{}, WithStartDelay(time.Hour))
	if err != nil {
		t.Fatal(err)
	}
	watch, err := event.Watch(ctx, runtime, run.RunID)
	if err != nil {
		t.Fatal(err)
	}
	defer watch.Close()
	cancelRun, runResult := startRuntime(t, runtime)
	defer stopRuntime(t, cancelRun, runResult)
	waitForObservation(t, observer, "notify_listener", "listening", 1, 2*time.Second)
	waitCtx, cancelWait := context.WithCancel(ctx)
	result := make(chan error, 1)
	go func() {
		_, _, nextErr := watch.Next(waitCtx)
		result <- nextErr
	}()
	select {
	case <-tracer.done:
	case <-time.After(time.Second):
		t.Fatal("Next did not perform its initial read")
	}
	initial := tracer.readCount()
	time.Sleep(75 * time.Millisecond)
	if got := tracer.readCount(); got != initial {
		t.Fatalf("idle Next reads = %d, want %d", got, initial)
	}
	if err := event.Deliver(ctx, runtime, other.RunID, "other", None{}); err != nil {
		t.Fatal(err)
	}
	time.Sleep(50 * time.Millisecond)
	if got := tracer.readCount(); got != initial {
		t.Fatalf("unrelated run caused %d reads, want %d", got, initial)
	}
	if err := event.Deliver(ctx, runtime, run.RunID, "target", None{}); err != nil {
		t.Fatal(err)
	}
	deadline := time.Now().Add(time.Second)
	for tracer.readCount() == initial && time.Now().Before(deadline) {
		time.Sleep(time.Millisecond)
	}
	if got := tracer.readCount(); got != initial+1 {
		t.Fatalf("targeted signal reads = %d, want %d", got, initial+1)
	}
	if err := <-result; err != nil {
		t.Fatalf("same-runtime Next error = %v", err)
	}
	cancelWait()
}

func TestEventWatchEventOnlyCommitEmitsOneNotificationStatement(t *testing.T) {
	recorder := &queryRecorder{}
	database := testpg.OpenWithQueryTracer(t, recorder)
	ctx := context.Background()
	if err := Migrate(ctx, database.DB, WithSchema(database.Schema)); err != nil {
		t.Fatal(err)
	}
	event := DefineEvent[None]("watch.protocol")
	root := DefineCommand[None, None]("watch.protocol.root", 1)
	runtime, err := New(database.DB, WithSchema(database.Schema), WithNotifications(true))
	if err != nil {
		t.Fatal(err)
	}
	run, err := root.Enqueue(ctx, runtime, "watch/protocol", None{}, WithStartDelay(time.Hour))
	if err != nil {
		t.Fatal(err)
	}
	recorder.reset()
	if err := event.Deliver(ctx, runtime, run.RunID, "event", None{}); err != nil {
		t.Fatal(err)
	}
	queries := recorder.snapshot()
	notifications := 0
	for _, query := range queries {
		if strings.Contains(query, "pg_notify") {
			notifications++
		}
	}
	if notifications != 1 {
		t.Fatalf("pg_notify statements = %d, want 1: %#v", notifications, queries)
	}
	listener := openNotificationListener(t, database, runtime)
	gated, err := root.Enqueue(ctx, runtime, "watch/protocol/gated", None{}, WaitFor(event, "release"), Within(time.Hour))
	if err != nil {
		t.Fatal(err)
	}
	recorder.reset()
	if err := event.Deliver(ctx, runtime, gated.RunID, "release", None{}); err != nil {
		t.Fatal(err)
	}
	readyQueries := recorder.snapshot()
	readyNotifications := 0
	for _, query := range readyQueries {
		if strings.Contains(query, "pg_notify") {
			readyNotifications++
		}
	}
	if readyNotifications != 1 {
		t.Fatalf("ready-event pg_notify statements = %d, want 1: %#v", readyNotifications, readyQueries)
	}
	waitForNotificationHintKind(t, listener, gated.RunID, store.NotificationRun, 2*time.Second)
	assertNoNotification(t, listener, 150*time.Millisecond)
	foldedRun, err := root.Enqueue(ctx, runtime, "watch/protocol/folded", None{}, WithStartDelay(time.Hour))
	if err != nil {
		t.Fatal(err)
	}
	tx, err := database.DB.Conn.Begin(ctx)
	if err != nil {
		t.Fatal(err)
	}
	txClient := runtime.InTx(tx)
	if err := event.Deliver(ctx, txClient, foldedRun.RunID, "event", None{}); err != nil {
		_ = tx.Rollback(ctx)
		t.Fatal(err)
	}
	if err := CancelRun(ctx, txClient, foldedRun.RunID, "fold notification hints"); err != nil {
		_ = tx.Rollback(ctx)
		t.Fatal(err)
	}
	if err := tx.Commit(ctx); err != nil {
		t.Fatal(err)
	}
	waitForNotificationHintKind(t, listener, foldedRun.RunID, store.NotificationEvent, 2*time.Second)
	assertNoNotification(t, listener, 150*time.Millisecond)

	withoutHints, err := New(database.DB, WithSchema(database.Schema), WithNotifications(false))
	if err != nil {
		t.Fatal(err)
	}
	withoutHintsRun, err := root.Enqueue(ctx, withoutHints, "watch/protocol/without-hints", None{}, WithStartDelay(time.Hour))
	if err != nil {
		t.Fatal(err)
	}
	recorder.reset()
	if err := event.Deliver(ctx, withoutHints, withoutHintsRun.RunID, "event", None{}); err != nil {
		t.Fatal(err)
	}
	withoutHintQueries := recorder.snapshot()
	if len(queries) != len(withoutHintQueries)+1 {
		t.Fatalf("event hint statement delta = %d enabled/%d disabled; want exactly one", len(queries), len(withoutHintQueries))
	}
	for _, query := range withoutHintQueries {
		if strings.Contains(query, "pg_notify") {
			t.Fatalf("notification-disabled delivery issued pg_notify: %#v", withoutHintQueries)
		}
	}
	t.Logf("event-only delivery query statements=%d enabled/%d disabled", len(queries), len(withoutHintQueries))
}

func TestEventWatchDisabledWriterDoesNotPromiseWake(t *testing.T) {
	t.Parallel()
	tracer := &eventWatchReadTracer{done: make(chan struct{})}
	database := testpg.OpenWithQueryTracer(t, tracer)
	ctx := context.Background()
	if err := Migrate(ctx, database.DB, WithSchema(database.Schema)); err != nil {
		t.Fatal(err)
	}
	event := DefineEvent[None]("watch.disabled_writer")
	root := DefineCommand[None, None]("watch.disabled_writer.root", 1)
	writer, err := New(database.DB, WithSchema(database.Schema), WithNotifications(false))
	if err != nil {
		t.Fatal(err)
	}
	observer := &recordingObserver{}
	reader, err := New(database.DB, WithSchema(database.Schema), WithObserver(observer))
	if err != nil {
		t.Fatal(err)
	}
	run, err := root.Enqueue(ctx, writer, "watch/disabled-writer", None{}, WithStartDelay(time.Hour))
	if err != nil {
		t.Fatal(err)
	}
	watch, err := event.Watch(ctx, reader, run.RunID)
	if err != nil {
		t.Fatal(err)
	}
	defer watch.Close()
	cancelRun, runResult := startRuntime(t, reader)
	defer stopRuntime(t, cancelRun, runResult)
	waitForObservation(t, observer, "notify_listener", "listening", 1, 2*time.Second)

	nextCtx, cancelNext := context.WithTimeout(ctx, 150*time.Millisecond)
	result := make(chan error, 1)
	go func() {
		_, _, nextErr := watch.Next(nextCtx)
		result <- nextErr
	}()
	select {
	case <-tracer.done:
	case <-time.After(time.Second):
		t.Fatal("Next did not perform its initial read")
	}
	if err := event.Deliver(ctx, writer, run.RunID, "persisted-without-hint", None{}); err != nil {
		t.Fatal(err)
	}
	if err := <-result; !errors.Is(err, context.DeadlineExceeded) {
		t.Fatalf("disabled-writer Next error = %v", err)
	}
	cancelNext()
	runID, err := parseRunID(run.RunID)
	if err != nil {
		t.Fatal(err)
	}
	reader.eventWakes.signal(runID)
	retryCtx, cancelRetry := context.WithTimeout(ctx, time.Second)
	key, _, err := watch.Next(retryCtx)
	cancelRetry()
	if err != nil || key != "persisted-without-hint" {
		t.Fatalf("Next(after explicit catch-up) = %q, %v", key, err)
	}
}

func TestEventWatchRejectsCorruptBodyAndTreatsPruningAsTerminal(t *testing.T) {
	t.Parallel()
	database := testpg.Open(t)
	ctx := context.Background()
	if err := Migrate(ctx, database.DB, WithSchema(database.Schema)); err != nil {
		t.Fatal(err)
	}
	event := DefineEvent[None]("watch.corruption")
	root := DefineCommand[None, None]("watch.corruption.root", 1)
	runtime, err := New(database.DB, WithSchema(database.Schema))
	if err != nil {
		t.Fatal(err)
	}
	corruptRun, err := root.Enqueue(ctx, runtime, "watch/corrupt", None{}, WithLiveKey(), WithStartDelay(time.Hour))
	if err != nil {
		t.Fatal(err)
	}
	corruptWatch, err := event.Watch(ctx, runtime, corruptRun.RunID)
	if err != nil {
		t.Fatal(err)
	}
	defer corruptWatch.Close()
	if err := event.Deliver(ctx, runtime, corruptRun.RunID, "bad", None{}); err != nil {
		t.Fatal(err)
	}
	badBody := []byte(`{"payload":{},"v":99}`)
	digest := sha256.Sum256(badBody)
	if _, err := database.DB.Conn.Exec(ctx, `UPDATE `+pgschema.Table(database.Schema, "flow_journal")+`
		SET body=$3,body_hash=$4 WHERE run_id=$1 AND event_key=$2 AND event_class='application'`,
		corruptRun.RunID, "bad", badBody, digest[:]); err != nil {
		t.Fatal(err)
	}
	if _, _, err := corruptWatch.Next(ctx); !errors.Is(err, ErrInvalidState) {
		t.Fatalf("Next(corrupt body) error = %v", err)
	}

	prunedRun, err := root.Enqueue(ctx, runtime, "watch/pruned", None{}, WithLiveKey(), WithStartDelay(time.Hour))
	if err != nil {
		t.Fatal(err)
	}
	prunedWatch, err := event.Watch(ctx, runtime, prunedRun.RunID)
	if err != nil {
		t.Fatal(err)
	}
	defer prunedWatch.Close()
	if err := CancelRun(ctx, runtime, prunedRun.RunID, "prune watch test"); err != nil {
		t.Fatal(err)
	}
	pruned, err := PruneTerminalRuns(ctx, runtime, time.Now().Add(time.Second), 10)
	if err != nil || pruned.Runs < 1 {
		t.Fatalf("PruneTerminalRuns() = %#v, %v", pruned, err)
	}
	if _, _, err := prunedWatch.Next(ctx); !errors.Is(err, ErrTerminal) {
		t.Fatalf("Next(pruned run) error = %v", err)
	}
}

func TestEventWatchReconnectCatchUpFindsDisconnectedCommit(t *testing.T) {
	t.Parallel()
	tracer := &eventWatchReadTracer{done: make(chan struct{})}
	database := testpg.OpenWithQueryTracer(t, tracer)
	ctx := context.Background()
	if err := Migrate(ctx, database.DB, WithSchema(database.Schema)); err != nil {
		t.Fatal(err)
	}
	event := DefineEvent[None]("watch.reconnect")
	root := DefineCommand[None, None]("watch.reconnect.root", 1)
	writer, err := New(database.DB, WithSchema(database.Schema))
	if err != nil {
		t.Fatal(err)
	}
	observer := &recordingObserver{}
	reader, err := New(database.DB, WithSchema(database.Schema), WithObserver(observer))
	if err != nil {
		t.Fatal(err)
	}
	var disconnect atomic.Bool
	releaseReconnect := make(chan struct{})
	reader.faults = fault.Func(func(hookCtx context.Context, point fault.Point) error {
		if point != fault.NotifyConnect || !disconnect.Load() {
			return nil
		}
		select {
		case <-releaseReconnect:
			return nil
		case <-hookCtx.Done():
			return hookCtx.Err()
		}
	})
	run, err := root.Enqueue(ctx, writer, "watch/reconnect", None{}, WithStartDelay(time.Hour))
	if err != nil {
		t.Fatal(err)
	}
	watch, err := event.Watch(ctx, reader, run.RunID)
	if err != nil {
		t.Fatal(err)
	}
	defer watch.Close()
	cancelRun, runResult := startRuntime(t, reader)
	defer stopRuntime(t, cancelRun, runResult)
	waitForObservation(t, observer, "notify_listener", "listening", 1, 2*time.Second)

	nextCtx, cancelNext := context.WithTimeout(ctx, 300*time.Millisecond)
	defer cancelNext()
	next := make(chan struct {
		key string
		err error
	}, 1)
	go func() {
		key, _, nextErr := watch.Next(nextCtx)
		next <- struct {
			key string
			err error
		}{key: key, err: nextErr}
	}()
	select {
	case <-tracer.done:
	case <-time.After(time.Second):
		t.Fatal("Next did not complete its initial read")
	}
	disconnect.Store(true)
	var terminated bool
	if err := database.DB.Conn.QueryRow(ctx, `SELECT pg_terminate_backend(pid) FROM pg_stat_activity
		WHERE application_name=$1 AND pid<>pg_backend_pid() LIMIT 1`,
		"flow-listener-"+reader.instanceID.String()).Scan(&terminated); err != nil {
		t.Fatal(err)
	}
	if !terminated {
		t.Fatal("listener backend was not terminated")
	}
	waitForObservation(t, observer, "notify_listener", "reconnecting", 1, 2*time.Second)
	if err := event.Deliver(ctx, writer, run.RunID, "during-disconnect", None{}); err != nil {
		t.Fatal(err)
	}
	select {
	case got := <-next:
		if !errors.Is(got.err, context.DeadlineExceeded) {
			t.Fatalf("Next(during outage) = %q, %v", got.key, got.err)
		}
	case <-time.After(time.Second):
		t.Fatal("Next did not reach its caller deadline during listener outage")
	}
	close(releaseReconnect)
	waitForObservation(t, observer, "notify_listener", "listening", 2, 2*time.Second)
	retryCtx, cancelRetry := context.WithTimeout(ctx, 2*time.Second)
	key, _, err := watch.Next(retryCtx)
	cancelRetry()
	if err != nil || key != "during-disconnect" {
		t.Fatalf("Next(after reconnect) = %q, %v", key, err)
	}
}

func TestEventWatchRunReplacementWakesPredecessor(t *testing.T) {
	t.Parallel()
	database := testpg.Open(t)
	ctx := context.Background()
	if err := Migrate(ctx, database.DB, WithSchema(database.Schema)); err != nil {
		t.Fatal(err)
	}
	event := DefineEvent[None]("watch.replacement")
	root := DefineCommand[None, None]("watch.replacement.root", 1)
	writer, err := New(database.DB, WithSchema(database.Schema))
	if err != nil {
		t.Fatal(err)
	}
	observer := &recordingObserver{}
	reader, err := New(database.DB, WithSchema(database.Schema), WithObserver(observer))
	if err != nil {
		t.Fatal(err)
	}
	original, err := root.Enqueue(ctx, writer, "watch/replacement", None{}, WithLiveKey(), WithStartDelay(time.Hour))
	if err != nil {
		t.Fatal(err)
	}
	watch, err := event.Watch(ctx, reader, original.RunID)
	if err != nil {
		t.Fatal(err)
	}
	defer watch.Close()
	cancelRun, runResult := startRuntime(t, reader)
	defer stopRuntime(t, cancelRun, runResult)
	waitForObservation(t, observer, "notify_listener", "listening", 1, 2*time.Second)

	next := make(chan error, 1)
	go func() {
		_, _, nextErr := watch.Next(ctx)
		next <- nextErr
	}()
	replacement, err := root.ReplaceCurrentRun(ctx, writer, original.RunID, "watch/replacement", None{},
		"new generation", WithLiveKey(), WithStartDelay(time.Hour))
	if err != nil || !replacement.Replaced || replacement.RunID == original.RunID {
		t.Fatalf("ReplaceCurrentRun() = %#v, %v", replacement, err)
	}
	select {
	case err := <-next:
		if !errors.Is(err, ErrTerminal) {
			t.Fatalf("predecessor Next error = %v", err)
		}
	case <-time.After(2 * time.Second):
		t.Fatal("replacement did not wake predecessor watch")
	}
	current, found, err := GetCurrentRun(ctx, writer, root.Name(), "watch/replacement")
	if err != nil || !found || current.ID != replacement.RunID {
		t.Fatalf("GetCurrentRun() = %#v, %v, %v", current, found, err)
	}
}

func TestEventWatchBroadcastsAcrossRuntimesAndWatchers(t *testing.T) {
	t.Parallel()
	tracer := &eventWatchReadTracer{done: make(chan struct{})}
	database := testpg.OpenWithQueryTracer(t, tracer)
	ctx := context.Background()
	if err := Migrate(ctx, database.DB, WithSchema(database.Schema)); err != nil {
		t.Fatal(err)
	}
	event := DefineEvent[None]("watch.broadcast")
	root := DefineCommand[None, None]("watch.broadcast.root", 1)
	writer, err := New(database.DB, WithSchema(database.Schema))
	if err != nil {
		t.Fatal(err)
	}
	run, err := root.Enqueue(ctx, writer, "watch/broadcast", None{}, WithStartDelay(time.Hour))
	if err != nil {
		t.Fatal(err)
	}
	type runningRuntime struct {
		cancel context.CancelFunc
		done   <-chan error
	}
	readers := make([]*Runtime, 2)
	running := make([]runningRuntime, len(readers))
	watches := make([]*EventWatch[None], 0, 4)
	for index := range readers {
		readers[index], err = New(database.DB, WithSchema(database.Schema))
		if err != nil {
			t.Fatal(err)
		}
		running[index].cancel, running[index].done = startRuntime(t, readers[index])
		defer stopRuntime(t, running[index].cancel, running[index].done)
		for range 2 {
			watch, watchErr := event.Watch(ctx, readers[index], run.RunID)
			if watchErr != nil {
				t.Fatal(watchErr)
			}
			watches = append(watches, watch)
			defer watch.Close()
		}
	}
	initialReads := tracer.readCount()
	results := make(chan struct {
		key string
		err error
	}, len(watches))
	for _, watch := range watches {
		go func() {
			key, _, nextErr := watch.Next(ctx)
			results <- struct {
				key string
				err error
			}{key: key, err: nextErr}
		}()
	}
	deadline := time.Now().Add(2 * time.Second)
	for tracer.readCount() < initialReads+len(watches) && time.Now().Before(deadline) {
		time.Sleep(time.Millisecond)
	}
	if got := tracer.readCount(); got < initialReads+len(watches) {
		t.Fatalf("initial watch reads = %d, want at least %d", got, initialReads+len(watches))
	}
	if err := event.Deliver(ctx, writer, run.RunID, "shared", None{}); err != nil {
		t.Fatal(err)
	}
	for range watches {
		select {
		case got := <-results:
			if got.err != nil || got.key != "shared" {
				t.Fatalf("broadcast Next() = %q, %v", got.key, got.err)
			}
		case <-time.After(2 * time.Second):
			t.Fatal("broadcast watch did not wake")
		}
	}
}

func TestEventWatchMalformedHintPerformsBroadCatchUp(t *testing.T) {
	t.Parallel()
	tracer := &eventWatchReadTracer{done: make(chan struct{})}
	database := testpg.OpenWithQueryTracer(t, tracer)
	ctx := context.Background()
	if err := Migrate(ctx, database.DB, WithSchema(database.Schema)); err != nil {
		t.Fatal(err)
	}
	event := DefineEvent[None]("watch.broad")
	root := DefineCommand[None, None]("watch.broad.root", 1)
	writer, err := New(database.DB, WithSchema(database.Schema), WithNotifications(false))
	if err != nil {
		t.Fatal(err)
	}
	reader, err := New(database.DB, WithSchema(database.Schema))
	if err != nil {
		t.Fatal(err)
	}
	run, err := root.Enqueue(ctx, writer, "watch/broad", None{}, WithStartDelay(time.Hour))
	if err != nil {
		t.Fatal(err)
	}
	watch, err := event.Watch(ctx, reader, run.RunID)
	if err != nil {
		t.Fatal(err)
	}
	defer watch.Close()
	cancelRun, runResult := startRuntime(t, reader)
	defer stopRuntime(t, cancelRun, runResult)
	result := make(chan error, 1)
	go func() {
		key, _, nextErr := watch.Next(ctx)
		if nextErr == nil && key != "recovered" {
			nextErr = errors.New("unexpected recovered event key")
		}
		result <- nextErr
	}()
	select {
	case <-tracer.done:
	case <-time.After(time.Second):
		t.Fatal("Next did not complete its initial read")
	}
	if err := event.Deliver(ctx, writer, run.RunID, "recovered", None{}); err != nil {
		t.Fatal(err)
	}
	if _, err := database.DB.Conn.Exec(ctx, `SELECT pg_notify($1,$2)`, reader.store.NotificationChannel(), `{"v":99}`); err != nil {
		t.Fatal(err)
	}
	select {
	case err := <-result:
		if err != nil {
			t.Fatal(err)
		}
	case <-time.After(2 * time.Second):
		t.Fatal("malformed hint did not trigger broad catch-up")
	}
}

func TestEventWatchThousandIdleWatchersDoNotPoll(t *testing.T) {
	tracer := &eventWatchReadTracer{done: make(chan struct{})}
	database := testpg.OpenWithQueryTracer(t, tracer)
	ctx := context.Background()
	if err := Migrate(ctx, database.DB, WithSchema(database.Schema)); err != nil {
		t.Fatal(err)
	}
	event := DefineEvent[None]("watch.scale")
	root := DefineCommand[None, None]("watch.scale.root", 1)
	observer := &recordingObserver{}
	runtime, err := New(database.DB, WithSchema(database.Schema), WithObserver(observer))
	if err != nil {
		t.Fatal(err)
	}
	run, err := root.Enqueue(ctx, runtime, "watch/scale", None{}, WithStartDelay(time.Hour))
	if err != nil {
		t.Fatal(err)
	}
	readDurableWork := func() (commands, queueRows, leases int) {
		t.Helper()
		if err := database.DB.Conn.QueryRow(ctx, `SELECT
			(SELECT count(*) FROM `+pgschema.Table(database.Schema, "flow_commands")+` WHERE run_id=$1),
			(SELECT count(*) FROM `+pgschema.Table(database.Schema, "flow_command_queue")+` WHERE run_id=$1),
			(SELECT count(*) FROM `+pgschema.Table(database.Schema, "flow_command_queue")+`
			 WHERE run_id=$1 AND lease_token IS NOT NULL)`, run.RunID).Scan(&commands, &queueRows, &leases); err != nil {
			t.Fatal(err)
		}
		return commands, queueRows, leases
	}
	beforeCommands, beforeQueueRows, beforeLeases := readDurableWork()
	const count = 1000
	watches := make([]*EventWatch[None], count)
	beforeRegistration := goruntime.NumGoroutine()
	for index := range watches {
		watches[index], err = event.Watch(ctx, runtime, run.RunID)
		if err != nil {
			t.Fatalf("Watch(%d) error = %v", index, err)
		}
	}
	if added := goruntime.NumGoroutine() - beforeRegistration; added > 4 {
		t.Fatalf("watch registration added %d goroutines", added)
	}
	cancelRun, runResult := startRuntime(t, runtime)
	defer stopRuntime(t, cancelRun, runResult)
	waitForObservation(t, observer, "notify_listener", "listening", 1, 2*time.Second)
	listenerDeadline := time.Now().Add(2 * time.Second)
	for {
		var listeners int
		if err := database.DB.Conn.QueryRow(ctx, `SELECT count(*) FROM pg_stat_activity WHERE application_name=$1`,
			"flow-listener-"+runtime.instanceID.String()).Scan(&listeners); err != nil {
			t.Fatal(err)
		}
		if listeners == 1 {
			break
		}
		if time.Now().After(listenerDeadline) {
			t.Fatalf("dedicated listener connections = %d, want 1", listeners)
		}
		time.Sleep(time.Millisecond)
	}

	initialReads := tracer.readCount()
	waitCtx, cancelWait := context.WithCancel(ctx)
	results := make(chan error, count)
	for _, watch := range watches {
		go func() {
			_, _, nextErr := watch.Next(waitCtx)
			results <- nextErr
		}()
	}
	deadline := time.Now().Add(5 * time.Second)
	for tracer.readCount() < initialReads+count && time.Now().Before(deadline) {
		time.Sleep(time.Millisecond)
	}
	if got := tracer.readCount(); got != initialReads+count {
		cancelWait()
		t.Fatalf("initial Next reads = %d, want %d", got, initialReads+count)
	}
	connectionDeadline := time.Now().Add(time.Second)
	for database.DB.Conn.Stat().AcquiredConns() != 0 && time.Now().Before(connectionDeadline) {
		time.Sleep(time.Millisecond)
	}
	if acquired := database.DB.Conn.Stat().AcquiredConns(); acquired != 0 {
		cancelWait()
		t.Fatalf("idle watches retain %d application connections", acquired)
	}
	readCount := tracer.readCount()
	time.Sleep(75 * time.Millisecond)
	if got := tracer.readCount(); got != readCount {
		cancelWait()
		t.Fatalf("idle watch reads grew from %d to %d", readCount, got)
	}
	afterCommands, afterQueueRows, afterLeases := readDurableWork()
	if afterCommands != beforeCommands || afterQueueRows != beforeQueueRows || afterLeases != beforeLeases {
		cancelWait()
		t.Fatalf("watchers changed durable work: commands %d/%d queue %d/%d leases %d/%d",
			beforeCommands, afterCommands, beforeQueueRows, afterQueueRows, beforeLeases, afterLeases)
	}
	cancelWait()
	for range count {
		if err := <-results; !errors.Is(err, context.Canceled) {
			t.Fatalf("Next cancellation error = %v", err)
		}
	}
	for _, watch := range watches {
		watch.Close()
	}
	runtime.eventWakes.mu.Lock()
	entries := len(runtime.eventWakes.entries)
	runtime.eventWakes.mu.Unlock()
	if entries != 0 {
		t.Fatalf("event wake entries after Close = %d", entries)
	}
}

func TestEventWatchValidationCancellationAndClose(t *testing.T) {
	t.Parallel()
	database := testpg.Open(t)
	ctx := context.Background()
	if err := Migrate(ctx, database.DB, WithSchema(database.Schema)); err != nil {
		t.Fatal(err)
	}
	event := DefineEvent[None]("watch.validation")
	root := DefineCommand[None, None]("watch.validation.root", 1)
	runtime, err := New(database.DB, WithSchema(database.Schema), WithNotifications(true))
	if err != nil {
		t.Fatal(err)
	}
	run, err := root.Enqueue(ctx, runtime, "watch/validation", None{}, WithStartDelay(time.Hour))
	if err != nil {
		t.Fatal(err)
	}
	if _, err := event.Watch(ctx, nil, run.RunID); !errors.Is(err, ErrInvalid) {
		t.Fatalf("Watch(nil) error = %v", err)
	}
	var invalidEvent Event[None]
	if _, err := invalidEvent.Watch(ctx, runtime, run.RunID); !errors.Is(err, ErrInvalid) {
		t.Fatalf("Watch(invalid event) error = %v", err)
	}
	disabled, err := New(database.DB, WithSchema(database.Schema), WithNotifications(false))
	if err != nil {
		t.Fatal(err)
	}
	if _, err := event.Watch(ctx, disabled, run.RunID); !errors.Is(err, ErrInvalid) {
		t.Fatalf("Watch(notifications disabled) error = %v", err)
	}
	if _, err := event.Watch(ctx, runtime, RunID("bad")); !errors.Is(err, ErrInvalid) {
		t.Fatalf("Watch(invalid run ID) error = %v", err)
	}
	if _, err := event.Watch(ctx, runtime, RunID("00000000-0000-0000-0000-000000000001")); !errors.Is(err, ErrNotFound) {
		t.Fatalf("Watch(missing run) error = %v", err)
	}
	constructorCtx, cancelConstructor := context.WithCancel(ctx)
	cancelConstructor()
	if _, err := event.Watch(constructorCtx, runtime, run.RunID); !errors.Is(err, context.Canceled) {
		t.Fatalf("Watch(cancelled construction) error = %v", err)
	}
	runtime.eventWakes.mu.Lock()
	failedEntries := len(runtime.eventWakes.entries)
	runtime.eventWakes.mu.Unlock()
	if failedEntries != 0 {
		t.Fatalf("failed watch construction retained %d wake entries", failedEntries)
	}

	watch, err := event.Watch(ctx, runtime, run.RunID)
	if err != nil {
		t.Fatal(err)
	}
	cancelled, cancel := context.WithCancel(ctx)
	cancel()
	if _, _, err := watch.Next(cancelled); !errors.Is(err, context.Canceled) {
		t.Fatalf("Next(cancelled) error = %v", err)
	}
	if err := event.Deliver(ctx, runtime, run.RunID, "after-cancel", None{}); err != nil {
		t.Fatal(err)
	}
	nextCtx, cancelNext := context.WithTimeout(ctx, time.Second)
	key, _, err := watch.Next(nextCtx)
	cancelNext()
	if err != nil || key != "after-cancel" {
		t.Fatalf("reused Next() = %q, %v", key, err)
	}

	waiting := make(chan error, 1)
	connections := make([]*pgxpool.Conn, 0, runtime.db.Conn.Config().MaxConns)
	for range runtime.db.Conn.Config().MaxConns {
		connection, acquireErr := runtime.db.Conn.Acquire(ctx)
		if acquireErr != nil {
			t.Fatal(acquireErr)
		}
		connections = append(connections, connection)
	}
	defer func() {
		for _, connection := range connections {
			connection.Release()
		}
	}()
	go func() {
		_, _, waitErr := watch.Next(context.Background())
		waiting <- waitErr
	}()
	deadline := time.Now().Add(time.Second)
	for !watch.inNext.Load() && time.Now().Before(deadline) {
		time.Sleep(time.Millisecond)
	}
	if !watch.inNext.Load() {
		t.Fatal("Next did not begin")
	}
	if _, _, err := watch.Next(ctx); !errors.Is(err, ErrInvalidState) {
		t.Fatalf("concurrent Next error = %v", err)
	}
	watch.Close()
	watch.Close()
	select {
	case err := <-waiting:
		if !errors.Is(err, ErrClosed) {
			t.Fatalf("waiting Next error = %v", err)
		}
	case <-time.After(time.Second):
		t.Fatal("Close did not cancel a Next blocked on database acquisition")
	}
	for _, connection := range connections {
		connection.Release()
	}
	connections = nil
	if _, _, err := watch.Next(ctx); !errors.Is(err, ErrClosed) {
		t.Fatalf("Next(closed) error = %v", err)
	}
	if err := CancelRun(ctx, runtime, run.RunID, "validation complete"); err != nil {
		t.Fatal(err)
	}
	if _, err := event.Watch(ctx, runtime, run.RunID); !errors.Is(err, ErrTerminal) {
		t.Fatalf("Watch(terminal run) error = %v", err)
	}
}

func TestEventWatchRuntimeStopCancelsConstruction(t *testing.T) {
	t.Parallel()
	database := testpg.Open(t)
	ctx := context.Background()
	if err := Migrate(ctx, database.DB, WithSchema(database.Schema)); err != nil {
		t.Fatal(err)
	}
	event := DefineEvent[None]("watch.construction_stop")
	root := DefineCommand[None, None]("watch.construction_stop.root", 1)
	writer, err := New(database.DB, WithSchema(database.Schema))
	if err != nil {
		t.Fatal(err)
	}
	run, err := root.Enqueue(ctx, writer, "watch/construction-stop", None{}, WithStartDelay(time.Hour))
	if err != nil {
		t.Fatal(err)
	}
	reader, err := New(database.DB, WithSchema(database.Schema))
	if err != nil {
		t.Fatal(err)
	}
	connections := make([]*pgxpool.Conn, 0, database.DB.Conn.Config().MaxConns)
	for range database.DB.Conn.Config().MaxConns {
		connection, acquireErr := database.DB.Conn.Acquire(ctx)
		if acquireErr != nil {
			t.Fatal(acquireErr)
		}
		connections = append(connections, connection)
	}
	defer func() {
		for _, connection := range connections {
			connection.Release()
		}
	}()

	constructed := make(chan error, 1)
	go func() {
		_, watchErr := event.Watch(context.Background(), reader, run.RunID)
		constructed <- watchErr
	}()
	deadline := time.Now().Add(time.Second)
	for {
		reader.eventWakes.mu.Lock()
		registered := len(reader.eventWakes.entries) == 1
		reader.eventWakes.mu.Unlock()
		if registered {
			break
		}
		if time.Now().After(deadline) {
			t.Fatal("Watch did not register before its initial database read")
		}
		time.Sleep(time.Millisecond)
	}
	if err := reader.Stop(ctx); err != nil {
		t.Fatal(err)
	}
	select {
	case err := <-constructed:
		if !errors.Is(err, ErrClosed) {
			t.Fatalf("Watch(runtime stopped during construction) error = %v", err)
		}
	case <-time.After(time.Second):
		t.Fatal("runtime stop did not cancel watch construction")
	}
}

func TestEventWatchPreRunCatchUpAndShutdown(t *testing.T) {
	t.Parallel()
	tracer := &eventWatchReadTracer{done: make(chan struct{})}
	database := testpg.OpenWithQueryTracer(t, tracer)
	ctx := context.Background()
	if err := Migrate(ctx, database.DB, WithSchema(database.Schema)); err != nil {
		t.Fatal(err)
	}
	event := DefineEvent[None]("watch.startup")
	root := DefineCommand[None, None]("watch.startup.root", 1)
	writer, err := New(database.DB, WithSchema(database.Schema))
	if err != nil {
		t.Fatal(err)
	}
	observer := &recordingObserver{}
	reader, err := New(database.DB, WithSchema(database.Schema), WithPollInterval(5*time.Second), WithObserver(observer))
	if err != nil {
		t.Fatal(err)
	}
	var connectAttempts atomic.Int32
	reader.faults = fault.Func(func(_ context.Context, point fault.Point) error {
		if point == fault.NotifyConnect && connectAttempts.Add(1) == 1 {
			return fault.Injected(point)
		}
		return nil
	})
	run, err := root.Enqueue(ctx, writer, "watch/startup", None{}, WithStartDelay(time.Hour))
	if err != nil {
		t.Fatal(err)
	}
	watch, err := event.Watch(ctx, reader, run.RunID)
	if err != nil {
		t.Fatal(err)
	}
	defer watch.Close()
	result := make(chan struct {
		key string
		err error
	}, 1)
	go func() {
		key, _, nextErr := watch.Next(ctx)
		result <- struct {
			key string
			err error
		}{key: key, err: nextErr}
	}()
	select {
	case <-tracer.done:
	case <-time.After(time.Second):
		t.Fatal("Next did not complete its initial durable read")
	}
	if err := event.Deliver(ctx, writer, run.RunID, "during-startup", None{}); err != nil {
		t.Fatal(err)
	}
	select {
	case got := <-result:
		t.Fatalf("pre-Run Next returned without a listener: %#v", got)
	case <-time.After(75 * time.Millisecond):
	}
	cancelRun, runResult := startRuntime(t, reader)
	waitForObservation(t, observer, "notify_listener", "connect_error", 1, 2*time.Second)
	waitForObservation(t, observer, "notify_listener", "listening", 1, 2*time.Second)
	select {
	case got := <-result:
		if got.err != nil || got.key != "during-startup" {
			t.Fatalf("startup catch-up Next() = %q, %v", got.key, got.err)
		}
	case <-time.After(2 * time.Second):
		t.Fatal("listener startup did not catch up the watch")
	}
	stopRuntime(t, cancelRun, runResult)
	if _, _, err := watch.Next(ctx); !errors.Is(err, ErrClosed) {
		t.Fatalf("Next(stopped runtime) error = %v", err)
	}
	if _, err := event.Watch(ctx, reader, run.RunID); !errors.Is(err, ErrClosed) {
		t.Fatalf("Watch(stopped runtime) error = %v", err)
	}
}
