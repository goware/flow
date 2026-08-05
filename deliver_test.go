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
	Target ExecutionID `json:"target"`
	Key    string      `json:"key"`
	Value  string      `json:"value"`
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
	producer := DefineCommand[ExecutionID, None]("delivery.producer", 1,
		WithRetry(RetryFor(time.Second).Attempts(2).Backoff(5*time.Millisecond)))
	runtime, err := New(database.DB, WithSchema(database.Schema), WithWorkerConcurrency(2),
		WithPollInterval(5*time.Millisecond), WithNotifications(false))
	if err != nil {
		t.Fatal(err)
	}
	received := make(chan deliveredPayload, 1)
	guardErr := make(chan error, 1)
	if err := runtime.Register(
		Handle(target, func(_ context.Context, work *Work[None]) (None, error) {
			payload, err := GetEventValue(work, event, "done")
			if err == nil {
				received <- payload
			}
			return None{}, err
		}),
		Handle(producer, func(ctx context.Context, work *Work[ExecutionID]) (None, error) {
			payload := deliveredPayload{Value: "complete"}
			if err := event.Deliver(ctx, runtime, work.Args, "done", payload); err != nil {
				return None{}, err
			}
			if work.Info().Attempt == 1 {
				return None{}, errors.New("retry after committed delivery")
			}
			guardErr <- event.Emit(ctx, runtime, work.Args, "guarded", payload)
			return None{}, nil
		}),
	); err != nil {
		t.Fatal(err)
	}

	targetHandle, err := target.With(runtime).Execute(ctx, "target", None{}, WaitFor(event, "done"))
	if err != nil {
		t.Fatal(err)
	}
	producerHandle, err := producer.With(runtime).Execute(ctx, "producer", targetHandle.ID)
	if err != nil {
		t.Fatal(err)
	}
	cancel, runResult := startRuntime(t, runtime)
	defer stopRuntime(t, cancel, runResult)

	waitForExecutionStatus(t, database.Schema, database.DB.Conn, targetHandle.ID, "succeeded", 5*time.Second)
	waitForExecutionStatus(t, database.Schema, database.DB.Conn, producerHandle.ID, "failed", 5*time.Second)
	select {
	case payload := <-received:
		if payload.Value != "complete" {
			t.Fatalf("delivered payload = %#v", payload)
		}
	default:
		t.Fatal("target worker did not read the delivered event")
	}
	select {
	case err := <-guardErr:
		if !errors.Is(err, ErrInvalidState) {
			t.Fatalf("Event.Emit inside attempt error = %v, want ErrInvalidState", err)
		}
	default:
		t.Fatal("producer did not exercise the Event.Emit attempt guard")
	}
	var eventCount int
	if err := database.DB.Conn.QueryRow(ctx, `SELECT count(*) FROM `+pgschema.Table(database.Schema, "flow_journal")+`
		WHERE execution_id=$1 AND event_class='application' AND event_name=$2`, targetHandle.ID, event.Name()).Scan(&eventCount); err != nil {
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
	runs := make(chan ExecutionID, 2)
	if err := runtime.Register(Handle(target, func(_ context.Context, work *Work[None]) (None, error) {
		if _, err := GetEventValue(work, event, "done"); err != nil {
			return None{}, err
		}
		runs <- work.Info().ExecutionID
		return None{}, nil
	})); err != nil {
		t.Fatal(err)
	}
	committed, err := target.With(runtime).Execute(ctx, "committed", None{}, WaitFor(event, "done"))
	if err != nil {
		t.Fatal(err)
	}
	rolledBack, err := target.With(runtime).Execute(ctx, "rolled-back", None{}, WaitFor(event, "done"))
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
	if _, err := tx.Exec(ctx, `INSERT INTO `+pgschema.Table(database.Schema, "delivery_records")+` (id) VALUES ('committed')`); err != nil {
		t.Fatal(err)
	}
	if err := event.Deliver(ctx, runtime.InTx(tx), committed.ID, "done", deliveredPayload{Value: "committed"}); err != nil {
		t.Fatal(err)
	}
	var applicationRows, committedEvents int
	var commandState string
	if err := observer.QueryRow(ctx, `SELECT count(*) FROM `+pgschema.Table(database.Schema, "delivery_records")+`
		WHERE id='committed'`).Scan(&applicationRows); err != nil {
		t.Fatal(err)
	}
	if err := observer.QueryRow(ctx, `SELECT count(*) FROM `+pgschema.Table(database.Schema, "flow_journal")+`
		WHERE execution_id=$1 AND event_class='application' AND event_name=$2`, committed.ID, event.Name()).Scan(&committedEvents); err != nil {
		t.Fatal(err)
	}
	if err := observer.QueryRow(ctx, `SELECT state FROM `+pgschema.Table(database.Schema, "flow_commands")+`
		WHERE command_id=$1`, committed.RootCommandID).Scan(&commandState); err != nil {
		t.Fatal(err)
	}
	if applicationRows != 0 || committedEvents != 0 || commandState != "pending" {
		t.Fatalf("before commit: application rows=%d events=%d command=%s", applicationRows, committedEvents, commandState)
	}
	if err := tx.Commit(ctx); err != nil {
		t.Fatal(err)
	}
	waitForExecutionStatus(t, database.Schema, database.DB.Conn, committed.ID, "succeeded", 5*time.Second)
	if err := observer.QueryRow(ctx, `SELECT count(*) FROM `+pgschema.Table(database.Schema, "delivery_records")+`
		WHERE id='committed'`).Scan(&applicationRows); err != nil {
		t.Fatal(err)
	}
	if err := observer.QueryRow(ctx, `SELECT count(*) FROM `+pgschema.Table(database.Schema, "flow_journal")+`
		WHERE execution_id=$1 AND event_class='application' AND event_name=$2`, committed.ID, event.Name()).Scan(&committedEvents); err != nil {
		t.Fatal(err)
	}
	if applicationRows != 1 || committedEvents != 1 {
		t.Fatalf("after commit: application rows=%d events=%d", applicationRows, committedEvents)
	}

	tx, err = database.DB.Conn.BeginTx(ctx, pgx.TxOptions{})
	if err != nil {
		t.Fatal(err)
	}
	if _, err := tx.Exec(ctx, `INSERT INTO `+pgschema.Table(database.Schema, "delivery_records")+` (id) VALUES ('rolled-back')`); err != nil {
		t.Fatal(err)
	}
	if err := event.Deliver(ctx, runtime.InTx(tx), rolledBack.ID, "done", deliveredPayload{Value: "rolled-back"}); err != nil {
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
		WHERE execution_id=$1 AND event_class='application' AND event_name=$2`, rolledBack.ID, event.Name()).Scan(&rolledBackEvents); err != nil {
		t.Fatal(err)
	}
	if applicationRows != 1 || rolledBackEvents != 0 {
		t.Fatalf("committed application rows = %d, rolled-back events = %d", applicationRows, rolledBackEvents)
	}
	select {
	case id := <-runs:
		if id != committed.ID {
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
		ID      ExecutionID
		Payload deliveredPayload
	}, 2)
	if err := runtime.Register(Handle(target, func(_ context.Context, work *Work[None]) (None, error) {
		payload, err := GetEventValue(work, event, "ready")
		if err == nil {
			received <- struct {
				ID      ExecutionID
				Payload deliveredPayload
			}{ID: work.Info().ExecutionID, Payload: payload}
		}
		return None{}, err
	})); err != nil {
		t.Fatal(err)
	}
	payload := deliveredPayload{Value: "complete"}
	missing := ExecutionID("00000000-0000-0000-0000-000000000001")
	if err := event.Deliver(ctx, runtime, missing, "ready", payload); !errors.Is(err, ErrNotFound) {
		t.Fatalf("Deliver(missing) error = %v, want ErrNotFound", err)
	}

	first, err := target.With(runtime).Execute(ctx, "stage", None{}, WithLiveKey(), WaitFor(event, "ready"))
	if err != nil {
		t.Fatal(err)
	}
	if err := event.Emit(ctx, runtime, first.ID, "ready", payload); err != nil {
		t.Fatal(err)
	}
	cancel, runResult := startRuntime(t, runtime)
	defer stopRuntime(t, cancel, runResult)
	waitForExecutionStatus(t, database.Schema, database.DB.Conn, first.ID, "succeeded", 5*time.Second)
	if err := event.Deliver(ctx, runtime, first.ID, "late", payload); !errors.Is(err, ErrTerminal) {
		t.Fatalf("Deliver(terminal) error = %v, want ErrTerminal", err)
	}

	second, err := target.With(runtime).Execute(ctx, "stage", None{}, WithLiveKey(), WaitFor(event, "ready"))
	if err != nil {
		t.Fatal(err)
	}
	if !second.Created || second.ID == first.ID {
		t.Fatalf("new stage exec = %#v, first = %#v", second, first)
	}
	if err := event.Deliver(ctx, runtime, second.ID, "ready", payload); err != nil {
		t.Fatal(err)
	}
	if err := event.Deliver(ctx, runtime, second.ID, "ready", payload); err != nil {
		t.Fatalf("idempotent Deliver() error = %v", err)
	}
	if err := event.Deliver(ctx, runtime, second.ID, "ready", deliveredPayload{Value: "changed"}); !errors.Is(err, ErrConflict) {
		t.Fatalf("conflicting Deliver() error = %v, want ErrConflict", err)
	}
	waitForExecutionStatus(t, database.Schema, database.DB.Conn, second.ID, "succeeded", 5*time.Second)

	seen := make(map[ExecutionID]deliveredPayload, 2)
	for range 2 {
		value := <-received
		seen[value.ID] = value.Payload
	}
	if seen[first.ID] != payload || seen[second.ID] != payload {
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
				payload, err := GetEventValue(work, event, key)
				if err != nil {
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
	joinHandle, err := join.With(runtime).Execute(ctx, "join", None{},
		WaitFor(event, keys[0]), WaitFor(event, keys[1]), WaitFor(event, keys[2]))
	if err != nil {
		t.Fatal(err)
	}
	producers := make([]Execution, 0, len(keys))
	for index, key := range keys {
		exec, err := producer.With(runtime).Execute(ctx, key, deliveredPart{
			Target: joinHandle.ID, Key: key, Value: string(rune('A' + index)),
		})
		if err != nil {
			t.Fatal(err)
		}
		producers = append(producers, exec)
	}
	cancel, runResult := startRuntime(t, runtime)
	defer stopRuntime(t, cancel, runResult)
	for _, exec := range producers {
		waitForExecutionStatus(t, database.Schema, database.DB.Conn, exec.ID, "succeeded", 5*time.Second)
	}
	waitForExecutionStatus(t, database.Schema, database.DB.Conn, joinHandle.ID, "succeeded", 5*time.Second)
	if values := <-joined; len(values) != 3 || values[0] != "A" || values[1] != "B" || values[2] != "C" {
		t.Fatalf("joined payloads = %#v", values)
	}
	trace, err := Trace(ctx, runtime, joinHandle.ID)
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
