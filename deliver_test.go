package flow

import (
	"context"
	"errors"
	"testing"
	"time"

	"github.com/goware/flow/internal/pgschema"
	"github.com/goware/flow/internal/testpg"
	"github.com/jackc/pgx/v5"
)

type deliveredPayload struct {
	Value string `json:"value"`
}

type deliveredPart struct {
	Target RunID  `json:"target"`
	Key    string `json:"key"`
	Value  string `json:"value"`
}

func TestDeliverFromActiveWorker(t *testing.T) {
	t.Parallel()
	database := testpg.Open(t)
	ctx := context.Background()
	if err := Migrate(ctx, database.DB, WithSchema(database.Schema)); err != nil {
		t.Fatal(err)
	}

	event := DefineEvent[deliveredPayload]("delivery.worker_event")
	target := DefineCommand[None, None]("delivery.target", 1)
	producer := DefineCommand[RunID, None]("delivery.producer", 1,
		WithRetry(RetryFor(time.Second).Attempts(2).Backoff(5*time.Millisecond)))
	runtime, err := New(database.DB, WithSchema(database.Schema), WithWorkerConcurrency(2),
		WithPollInterval(5*time.Millisecond), WithNotifications(false))
	if err != nil {
		t.Fatal(err)
	}
	received := make(chan deliveredPayload, 1)
	detachedErr := make(chan error, 1)
	if err := runtime.Register(
		Handle(target, func(_ context.Context, work *Work[None]) (None, error) {
			payload, found, err := GetEventValue(work, event, "done")
			if err == nil && found {
				received <- payload
			} else if err == nil {
				err = errors.New("required delivered event is absent")
			}
			return None{}, err
		}),
		Handle(producer, func(ctx context.Context, work *Work[RunID]) (None, error) {
			payload := deliveredPayload{Value: "complete"}
			if err := event.Deliver(ctx, runtime, work.Args, "done", payload); err != nil {
				return None{}, err
			}
			if work.Info().Attempt == 1 {
				return None{}, errors.New("retry after committed delivery")
			}
			detachedErr <- event.Deliver(ctx, runtime, work.Args, "guarded", payload)
			return None{}, nil
		}),
	); err != nil {
		t.Fatal(err)
	}

	targetHandle, err := target.Enqueue(ctx, runtime, "target", None{}, WaitFor(event, "done"))
	if err != nil {
		t.Fatal(err)
	}
	producerHandle, err := producer.Enqueue(ctx, runtime, "producer", targetHandle.RunID)
	if err != nil {
		t.Fatal(err)
	}
	cancel, runResult := startRuntime(t, runtime)
	defer stopRuntime(t, cancel, runResult)

	waitForRunStatus(t, database.Schema, database.DB.Conn, targetHandle.RunID, "succeeded", 5*time.Second)
	waitForRunStatus(t, database.Schema, database.DB.Conn, producerHandle.RunID, "succeeded", 5*time.Second)
	select {
	case payload := <-received:
		if payload.Value != "complete" {
			t.Fatalf("delivered payload = %#v", payload)
		}
	default:
		t.Fatal("target worker did not read the delivered event")
	}
	select {
	case err := <-detachedErr:
		if !errors.Is(err, ErrTerminal) {
			t.Fatalf("late Event.Deliver inside attempt error = %v, want ErrTerminal", err)
		}
	default:
		t.Fatal("producer did not exercise detached Event.Deliver")
	}
	var eventCount int
	if err := database.DB.Conn.QueryRow(ctx, `SELECT count(*) FROM `+pgschema.Table(database.Schema, "flow_journal")+`
		WHERE run_id=$1 AND event_class='application' AND event_name=$2`, targetHandle.RunID, event.Name()).Scan(&eventCount); err != nil {
		t.Fatal(err)
	}
	if eventCount != 1 {
		t.Fatalf("delivered event count = %d, want 1", eventCount)
	}
}

func TestDeliverInCallerTransaction(t *testing.T) {
	t.Parallel()
	database := testpg.Open(t)
	ctx := context.Background()
	if err := Migrate(ctx, database.DB, WithSchema(database.Schema)); err != nil {
		t.Fatal(err)
	}
	if _, err := database.DB.Conn.Exec(ctx, `CREATE TABLE `+pgschema.Table(database.Schema, "delivery_records")+`
		(id text PRIMARY KEY)`); err != nil {
		t.Fatal(err)
	}

	event := DefineEvent[deliveredPayload]("delivery.tx_event")
	target := DefineCommand[None, None]("delivery.tx_target", 1)
	runtime, err := New(database.DB, WithSchema(database.Schema), WithPollInterval(5*time.Millisecond), WithNotifications(false))
	if err != nil {
		t.Fatal(err)
	}
	runs := make(chan RunID, 2)
	if err := runtime.Register(Handle(target, func(_ context.Context, work *Work[None]) (None, error) {
		if _, found, err := GetEventValue(work, event, "done"); err != nil {
			return None{}, err
		} else if !found {
			return None{}, errors.New("required delivered event is absent")
		}
		runs <- work.Info().RunID
		return None{}, nil
	})); err != nil {
		t.Fatal(err)
	}
	committed, err := target.Enqueue(ctx, runtime, "committed", None{}, WaitFor(event, "done"))
	if err != nil {
		t.Fatal(err)
	}
	rolledBack, err := target.Enqueue(ctx, runtime, "rolled-back", None{}, WaitFor(event, "done"))
	if err != nil {
		t.Fatal(err)
	}
	cancel, runResult := startRuntime(t, runtime)
	defer stopRuntime(t, cancel, runResult)
	observer, err := database.DB.Conn.Acquire(ctx)
	if err != nil {
		t.Fatal(err)
	}
	defer observer.Release()

	tx, err := database.DB.Conn.BeginTx(ctx, pgx.TxOptions{})
	if err != nil {
		t.Fatal(err)
	}
	flowTx := runtime.InTx(tx)
	if err := event.Deliver(ctx, flowTx, committed.RunID, "done", deliveredPayload{Value: "committed"}); err != nil {
		t.Fatal(err)
	}
	if err := flowTx.BeginApplicationWrites(); err != nil {
		t.Fatal(err)
	}
	committedRun := mustGetRun(t, flowTx, committed.RunID)
	if _, err := tx.Exec(ctx, `INSERT INTO `+pgschema.Table(database.Schema, "delivery_records")+` (id) VALUES ('committed')`); err != nil {
		t.Fatal(err)
	}
	var applicationRows, committedEvents int
	var commandState string
	if err := observer.QueryRow(ctx, `SELECT count(*) FROM `+pgschema.Table(database.Schema, "delivery_records")+`
		WHERE id='committed'`).Scan(&applicationRows); err != nil {
		t.Fatal(err)
	}
	if err := observer.QueryRow(ctx, `SELECT count(*) FROM `+pgschema.Table(database.Schema, "flow_journal")+`
		WHERE run_id=$1 AND event_class='application' AND event_name=$2`, committed.RunID, event.Name()).Scan(&committedEvents); err != nil {
		t.Fatal(err)
	}
	if err := observer.QueryRow(ctx, `SELECT state FROM `+pgschema.Table(database.Schema, "flow_commands")+`
		WHERE command_id=$1`, committedRun.RootCommandID).Scan(&commandState); err != nil {
		t.Fatal(err)
	}
	if applicationRows != 0 || committedEvents != 0 || commandState != "pending" {
		t.Fatalf("before commit: application rows=%d events=%d command=%s", applicationRows, committedEvents, commandState)
	}
	if err := tx.Commit(ctx); err != nil {
		t.Fatal(err)
	}
	waitForRunStatus(t, database.Schema, database.DB.Conn, committed.RunID, "succeeded", 5*time.Second)
	if err := observer.QueryRow(ctx, `SELECT count(*) FROM `+pgschema.Table(database.Schema, "delivery_records")+`
		WHERE id='committed'`).Scan(&applicationRows); err != nil {
		t.Fatal(err)
	}
	if err := observer.QueryRow(ctx, `SELECT count(*) FROM `+pgschema.Table(database.Schema, "flow_journal")+`
		WHERE run_id=$1 AND event_class='application' AND event_name=$2`, committed.RunID, event.Name()).Scan(&committedEvents); err != nil {
		t.Fatal(err)
	}
	if applicationRows != 1 || committedEvents != 1 {
		t.Fatalf("after commit: application rows=%d events=%d", applicationRows, committedEvents)
	}

	tx, err = database.DB.Conn.BeginTx(ctx, pgx.TxOptions{})
	if err != nil {
		t.Fatal(err)
	}
	flowTx = runtime.InTx(tx)
	if err := event.Deliver(ctx, flowTx, rolledBack.RunID, "done", deliveredPayload{Value: "rolled-back"}); err != nil {
		t.Fatal(err)
	}
	if err := flowTx.BeginApplicationWrites(); err != nil {
		t.Fatal(err)
	}
	if _, err := tx.Exec(ctx, `INSERT INTO `+pgschema.Table(database.Schema, "delivery_records")+` (id) VALUES ('rolled-back')`); err != nil {
		t.Fatal(err)
	}
	if err := tx.Rollback(ctx); err != nil {
		t.Fatal(err)
	}

	var rolledBackEvents int
	if err := observer.QueryRow(ctx, `SELECT count(*) FROM `+pgschema.Table(database.Schema, "delivery_records")).Scan(&applicationRows); err != nil {
		t.Fatal(err)
	}
	if err := observer.QueryRow(ctx, `SELECT count(*) FROM `+pgschema.Table(database.Schema, "flow_journal")+`
		WHERE run_id=$1 AND event_class='application' AND event_name=$2`, rolledBack.RunID, event.Name()).Scan(&rolledBackEvents); err != nil {
		t.Fatal(err)
	}
	if applicationRows != 1 || rolledBackEvents != 0 {
		t.Fatalf("committed application rows = %d, rolled-back events = %d", applicationRows, rolledBackEvents)
	}
	select {
	case id := <-runs:
		if id != committed.RunID {
			t.Fatalf("rolled-back target %s ran", id)
		}
	default:
		t.Fatal("committed target did not run")
	}
}

func TestDeliverIdentityLifecycleAndGateParity(t *testing.T) {
	t.Parallel()
	database := testpg.Open(t)
	ctx := context.Background()
	if err := Migrate(ctx, database.DB, WithSchema(database.Schema)); err != nil {
		t.Fatal(err)
	}

	event := DefineEvent[deliveredPayload]("delivery.identity_event")
	target := DefineCommand[None, None]("delivery.identity_target", 1)
	runtime, err := New(database.DB, WithSchema(database.Schema), WithPollInterval(5*time.Millisecond), WithNotifications(false))
	if err != nil {
		t.Fatal(err)
	}
	received := make(chan struct {
		ID      RunID
		Payload deliveredPayload
	}, 2)
	if err := runtime.Register(Handle(target, func(_ context.Context, work *Work[None]) (None, error) {
		payload, found, err := GetEventValue(work, event, "ready")
		if err == nil && found {
			received <- struct {
				ID      RunID
				Payload deliveredPayload
			}{ID: work.Info().RunID, Payload: payload}
		} else if err == nil {
			err = errors.New("required delivered event is absent")
		}
		return None{}, err
	})); err != nil {
		t.Fatal(err)
	}
	payload := deliveredPayload{Value: "complete"}
	missing := RunID("00000000-0000-0000-0000-000000000001")
	if err := event.Deliver(ctx, runtime, missing, "ready", payload); !errors.Is(err, ErrNotFound) {
		t.Fatalf("Deliver(missing) error = %v, want ErrNotFound", err)
	}

	first, err := target.Enqueue(ctx, runtime, "stage", None{}, WithLiveKey(), WaitFor(event, "ready"))
	if err != nil {
		t.Fatal(err)
	}
	if err := event.Deliver(ctx, runtime, first.RunID, "ready", payload); err != nil {
		t.Fatal(err)
	}
	cancel, runResult := startRuntime(t, runtime)
	defer stopRuntime(t, cancel, runResult)
	waitForRunStatus(t, database.Schema, database.DB.Conn, first.RunID, "succeeded", 5*time.Second)
	if err := event.Deliver(ctx, runtime, first.RunID, "late", payload); !errors.Is(err, ErrTerminal) {
		t.Fatalf("Deliver(terminal) error = %v, want ErrTerminal", err)
	}

	second, err := target.Enqueue(ctx, runtime, "stage", None{}, WithLiveKey(), WaitFor(event, "ready"))
	if err != nil {
		t.Fatal(err)
	}
	if !second.Created || second.RunID == first.RunID {
		t.Fatalf("new stage exec = %#v, first = %#v", second, first)
	}
	if err := event.Deliver(ctx, runtime, second.RunID, "ready", payload); err != nil {
		t.Fatal(err)
	}
	if err := event.Deliver(ctx, runtime, second.RunID, "ready", payload); err != nil {
		t.Fatalf("idempotent Deliver() error = %v", err)
	}
	if err := event.Deliver(ctx, runtime, second.RunID, "ready", deliveredPayload{Value: "changed"}); !errors.Is(err, ErrConflict) {
		t.Fatalf("conflicting Deliver() error = %v, want ErrConflict", err)
	}
	waitForRunStatus(t, database.Schema, database.DB.Conn, second.RunID, "succeeded", 5*time.Second)

	seen := make(map[RunID]deliveredPayload, 2)
	for range 2 {
		value := <-received
		seen[value.ID] = value.Payload
	}
	if seen[first.RunID] != payload || seen[second.RunID] != payload {
		t.Fatalf("Emit/Deliver gate payloads = %#v", seen)
	}
}

func TestDeliverMultiProducerFanIn(t *testing.T) {
	t.Parallel()
	database := testpg.Open(t)
	ctx := context.Background()
	if err := Migrate(ctx, database.DB, WithSchema(database.Schema)); err != nil {
		t.Fatal(err)
	}

	event := DefineEvent[deliveredPayload]("delivery.fanin_event")
	join := DefineCommand[None, None]("delivery.fanin_join", 1)
	producer := DefineCommand[deliveredPart, None]("delivery.fanin_producer", 1)
	runtime, err := New(database.DB, WithSchema(database.Schema), WithWorkerConcurrency(4),
		WithPollInterval(5*time.Millisecond), WithNotifications(false))
	if err != nil {
		t.Fatal(err)
	}
	joined := make(chan []string, 1)
	keys := []string{"part-a", "part-b", "part-c"}
	if err := runtime.Register(
		Handle(join, func(_ context.Context, work *Work[None]) (None, error) {
			values := make([]string, 0, len(keys))
			for _, key := range keys {
				payload, found, err := GetEventValue(work, event, key)
				if err != nil || !found {
					if err == nil {
						err = errors.New("required delivered event is absent")
					}
					return None{}, err
				}
				values = append(values, payload.Value)
			}
			joined <- values
			return None{}, nil
		}),
		Handle(producer, func(ctx context.Context, work *Work[deliveredPart]) (None, error) {
			return None{}, event.Deliver(ctx, runtime, work.Args.Target, work.Args.Key, deliveredPayload{Value: work.Args.Value})
		}),
	); err != nil {
		t.Fatal(err)
	}
	joinHandle, err := join.Enqueue(ctx, runtime, "join", None{},
		WaitFor(event, keys[0]), WaitFor(event, keys[1]), WaitFor(event, keys[2]))
	if err != nil {
		t.Fatal(err)
	}
	producers := make([]EnqueueResult, 0, len(keys))
	for index, key := range keys {
		exec, err := producer.Enqueue(ctx, runtime, key, deliveredPart{
			Target: joinHandle.RunID, Key: key, Value: string(rune('A' + index)),
		})
		if err != nil {
			t.Fatal(err)
		}
		producers = append(producers, exec)
	}
	cancel, runResult := startRuntime(t, runtime)
	defer stopRuntime(t, cancel, runResult)
	for _, exec := range producers {
		waitForRunStatus(t, database.Schema, database.DB.Conn, exec.RunID, "succeeded", 5*time.Second)
	}
	waitForRunStatus(t, database.Schema, database.DB.Conn, joinHandle.RunID, "succeeded", 5*time.Second)
	if values := <-joined; len(values) != 3 || values[0] != "A" || values[1] != "B" || values[2] != "C" {
		t.Fatalf("joined payloads = %#v", values)
	}
	trace, err := Trace(ctx, runtime, joinHandle.RunID)
	if err != nil {
		t.Fatal(err)
	}
	if len(trace.Commands) != 1 || len(trace.Commands[0].Waits) != 3 {
		t.Fatalf("join trace = %#v", trace.Commands)
	}
	for _, wait := range trace.Commands[0].Waits {
		if wait.SatisfiedPosition == nil {
			t.Fatalf("unsatisfied join wait = %#v", wait)
		}
	}
}
