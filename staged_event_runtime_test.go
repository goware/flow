package flow

import (
	"context"
	"errors"
	"testing"
	"time"

	"github.com/goware/flow/internal/pgschema"
	"github.com/goware/flow/internal/testpg"
)

type stagedEventPayload struct {
	Value string `json:"value"`
}

func TestWorkerStagedEventsSettleAtomicallyWithChildrenAndCommit(t *testing.T) {
	t.Parallel()
	database := testpg.Open(t)
	ctx := context.Background()
	if err := Migrate(ctx, database.DB, WithSchema(database.Schema)); err != nil {
		t.Fatal(err)
	}
	if _, err := database.DB.Conn.Exec(ctx, `CREATE TABLE `+pgschema.Table(database.Schema, "staged_event_commits")+`
		(execution_id text PRIMARY KEY)`); err != nil {
		t.Fatal(err)
	}

	event := DefineEvent[stagedEventPayload]("staged.worker_event")
	child := DefineCommand[None, None]("staged.child", 1, WithRetry(Attempts(1)))
	success := DefineCommand[None, None]("staged.success", 1, WithRetry(Attempts(1)))
	failure := DefineCommand[None, None]("staged.failure", 1, WithRetry(Attempts(1)))
	commitFailure := DefineCommand[None, None]("staged.commit_failure", 1, WithRetry(Attempts(1)))
	commitIngress := DefineCommand[None, None]("staged.commit_ingress", 1, WithRetry(Attempts(1)))
	observer := &recordingObserver{}
	runtime, err := New(database.DB, WithSchema(database.Schema), WithWorkerConcurrency(4),
		WithPollInterval(5*time.Millisecond), WithNotifications(false), WithObserver(observer))
	if err != nil {
		t.Fatal(err)
	}
	stage := func(work *Work[None]) {
		_ = Emit(work, event, "z", stagedEventPayload{Value: "last"})
		_ = Emit(work, event, "a", stagedEventPayload{Value: "first"})
		_ = Emit(work, event, "a", stagedEventPayload{Value: "first"})
		Execute(work, "child", child, None{})
	}
	if err := runtime.Register(
		Handle(child, func(context.Context, *Work[None]) (None, error) { return None{}, nil }),
		Handle(success, func(_ context.Context, work *Work[None]) (None, error) {
			stage(work)
			return None{}, nil
		}, WithCommit(func(ctx context.Context, tx Tx, commit Commit[None, None]) error {
			_, err := tx.Exec(ctx, `INSERT INTO `+pgschema.Table(database.Schema, "staged_event_commits")+`
				(execution_id) VALUES ($1)`, commit.Info.ExecutionID)
			return err
		})),
		Handle(failure, func(_ context.Context, work *Work[None]) (None, error) {
			stage(work)
			return None{}, Permanent(errors.New("worker rejected"))
		}),
		Handle(commitFailure, func(_ context.Context, work *Work[None]) (None, error) {
			stage(work)
			return None{}, nil
		}, WithCommit(func(context.Context, Tx, Commit[None, None]) error {
			return Permanent(errors.New("commit rejected"))
		})),
		Handle(commitIngress, func(_ context.Context, work *Work[None]) (None, error) {
			stage(work)
			return None{}, nil
		}, WithCommit(func(ctx context.Context, tx Tx, commit Commit[None, None]) error {
			if _, err := tx.Exec(ctx, `INSERT INTO `+pgschema.Table(database.Schema, "staged_event_commits")+`
				(execution_id) VALUES ($1)`, commit.Info.ExecutionID); err != nil {
				return err
			}
			_ = event.Emit(ctx, runtime, commit.Info.ExecutionID, "forbidden-ingress", stagedEventPayload{Value: "must-not-commit"})
			return nil
		})),
	); err != nil {
		t.Fatal(err)
	}
	cancel, runResult := startRuntime(t, runtime)
	defer stopRuntime(t, cancel, runResult)

	successHandle, err := success.With(runtime).Execute(ctx, "success", None{})
	if err != nil {
		t.Fatal(err)
	}
	failureHandle, err := failure.With(runtime).Execute(ctx, "failure", None{})
	if err != nil {
		t.Fatal(err)
	}
	commitFailureHandle, err := commitFailure.With(runtime).Execute(ctx, "commit-failure", None{})
	if err != nil {
		t.Fatal(err)
	}
	commitIngressHandle, err := commitIngress.With(runtime).Execute(ctx, "commit-ingress", None{})
	if err != nil {
		t.Fatal(err)
	}
	waitForExecutionStatus(t, database.Schema, database.DB.Conn, successHandle.ID, "succeeded", 5*time.Second)
	waitForExecutionStatus(t, database.Schema, database.DB.Conn, failureHandle.ID, "failed", 5*time.Second)
	waitForExecutionStatus(t, database.Schema, database.DB.Conn, commitFailureHandle.ID, "failed", 5*time.Second)
	waitForExecutionStatus(t, database.Schema, database.DB.Conn, commitIngressHandle.ID, "failed", 5*time.Second)
	poisonedTrace, err := Trace(ctx, runtime, commitIngressHandle.ID)
	if err != nil {
		t.Fatal(err)
	}
	if len(poisonedTrace.Commands) != 1 || poisonedTrace.Commands[0].FailureCode != "invalid_decision" {
		t.Fatalf("poisoned commit trace=%+v", poisonedTrace)
	}

	trace, err := Trace(ctx, runtime, successHandle.ID)
	if err != nil {
		t.Fatal(err)
	}
	var applicationEvents []TraceEvent
	for _, recorded := range trace.Events {
		if recorded.Class == "application" {
			applicationEvents = append(applicationEvents, recorded)
		}
	}
	if len(applicationEvents) != 2 || applicationEvents[0].Key != "a" || applicationEvents[1].Key != "z" ||
		applicationEvents[0].CommandID != successHandle.RootCommandID {
		t.Fatalf("application events=%+v", applicationEvents)
	}
	var committed int
	if err := database.DB.Conn.QueryRow(ctx, `SELECT count(*) FROM `+pgschema.Table(database.Schema, "staged_event_commits")+`
		WHERE execution_id=$1`, successHandle.ID).Scan(&committed); err != nil || committed != 1 {
		t.Fatalf("commit rows=%d err=%v", committed, err)
	}
	for _, handle := range []ExecutionHandle{failureHandle, commitFailureHandle, commitIngressHandle} {
		var eventCount, childCount int
		if err := database.DB.Conn.QueryRow(ctx, `SELECT
			count(*) FILTER (WHERE event_class='application'),
			(SELECT count(*) FROM `+pgschema.Table(database.Schema, "flow_commands")+`
			 WHERE execution_id=$1 AND parent_command_id IS NOT NULL)
			FROM `+pgschema.Table(database.Schema, "flow_journal")+` WHERE execution_id=$1`, handle.ID).
			Scan(&eventCount, &childCount); err != nil {
			t.Fatal(err)
		}
		if eventCount != 0 || childCount != 0 {
			t.Fatalf("rolled-back decision %s exposed events=%d children=%d", handle.ID, eventCount, childCount)
		}
	}
	var poisonedCommitRows int
	if err := database.DB.Conn.QueryRow(ctx, `SELECT count(*) FROM `+pgschema.Table(database.Schema, "staged_event_commits")+`
		WHERE execution_id=$1`, commitIngressHandle.ID).Scan(&poisonedCommitRows); err != nil || poisonedCommitRows != 0 {
		t.Fatalf("poisoned commit rows=%d err=%v", poisonedCommitRows, err)
	}
	settled := waitForMatchingObservations(t, observer, 3, func(observation Observation) bool {
		return observation.ExecutionID == successHandle.ID && observation.Operation == "settle" &&
			(observation.Kind == ObservationEvent ||
				(observation.Kind == ObservationAttempt && observation.CommandID == successHandle.RootCommandID))
	})
	var eventObservations, attemptObservations int
	for _, observation := range settled {
		switch observation.Kind {
		case ObservationEvent:
			eventObservations++
			if observation.Outcome != "accepted" || observation.CommandID != successHandle.RootCommandID ||
				observation.CommandKey != "root" || observation.Name != event.Name() {
				t.Fatalf("worker event observation=%+v", observation)
			}
		case ObservationAttempt:
			attemptObservations++
			if observation.Outcome != "succeeded" || observation.Count != 2 || observation.Name != success.Name() {
				t.Fatalf("worker settle observation=%+v", observation)
			}
		}
	}
	if eventObservations != 2 || attemptObservations != 1 {
		t.Fatalf("worker settle observations=%+v", settled)
	}
	rolledBack := map[ExecutionID]bool{
		failureHandle.ID: true, commitFailureHandle.ID: true, commitIngressHandle.ID: true,
	}
	waitForMatchingObservations(t, observer, len(rolledBack), func(observation Observation) bool {
		return rolledBack[observation.ExecutionID] && observation.Kind == ObservationAttempt && observation.Operation == "conclude"
	})
	for _, observation := range observer.snapshot() {
		if rolledBack[observation.ExecutionID] && observation.Kind == ObservationEvent && observation.Operation == "settle" {
			t.Fatalf("rolled-back worker decision emitted settlement observation=%+v", observation)
		}
	}
	assertReplayMatches(t, runtime, successHandle.ID)
}

func waitForMatchingObservations(t *testing.T, observer *recordingObserver, count int,
	match func(Observation) bool) []Observation {
	t.Helper()
	deadline := time.Now().Add(time.Second)
	for time.Now().Before(deadline) {
		var matching []Observation
		for _, observation := range observer.snapshot() {
			if match(observation) {
				matching = append(matching, observation)
			}
		}
		if len(matching) >= count {
			return matching
		}
		time.Sleep(time.Millisecond)
	}
	var matching []Observation
	for _, observation := range observer.snapshot() {
		if match(observation) {
			matching = append(matching, observation)
		}
	}
	t.Fatalf("matching observations=%d want at least %d: %+v", len(matching), count, observer.snapshot())
	return nil
}

func TestWorkerStagedEventCoalescesOrConflictsWithDurableIdentity(t *testing.T) {
	t.Parallel()
	database := testpg.Open(t)
	ctx := context.Background()
	if err := Migrate(ctx, database.DB, WithSchema(database.Schema)); err != nil {
		t.Fatal(err)
	}
	event := DefineEvent[stagedEventPayload]("staged.durable_identity")
	command := DefineCommand[runtimeArgs, None]("staged.durable_identity_worker", 1, WithRetry(Attempts(1)))
	runtime, err := New(database.DB, WithSchema(database.Schema), WithPollInterval(5*time.Millisecond), WithNotifications(false))
	if err != nil {
		t.Fatal(err)
	}
	if err := runtime.Register(Handle(command, func(_ context.Context, work *Work[runtimeArgs]) (None, error) {
		return None{}, Emit(work, event, "same", stagedEventPayload{Value: work.Args.Value})
	})); err != nil {
		t.Fatal(err)
	}
	equivalent, err := command.With(runtime).Execute(ctx, "equivalent", runtimeArgs{Value: "same"}, WithStartDelay(100*time.Millisecond))
	if err != nil {
		t.Fatal(err)
	}
	conflicting, err := command.With(runtime).Execute(ctx, "conflicting", runtimeArgs{Value: "new"}, WithStartDelay(100*time.Millisecond))
	if err != nil {
		t.Fatal(err)
	}
	if err := event.Emit(ctx, runtime, equivalent.ID, "same", stagedEventPayload{Value: "same"}); err != nil {
		t.Fatal(err)
	}
	if err := event.Emit(ctx, runtime, conflicting.ID, "same", stagedEventPayload{Value: "old"}); err != nil {
		t.Fatal(err)
	}
	cancel, runResult := startRuntime(t, runtime)
	defer stopRuntime(t, cancel, runResult)
	waitForExecutionStatus(t, database.Schema, database.DB.Conn, equivalent.ID, "succeeded", 5*time.Second)
	waitForExecutionStatus(t, database.Schema, database.DB.Conn, conflicting.ID, "failed", 5*time.Second)
	for _, handle := range []ExecutionHandle{equivalent, conflicting} {
		var count int
		if err := database.DB.Conn.QueryRow(ctx, `SELECT count(*) FROM `+pgschema.Table(database.Schema, "flow_journal")+`
			WHERE execution_id=$1 AND event_class='application'`, handle.ID).Scan(&count); err != nil || count != 1 {
			t.Fatalf("execution=%s application events=%d err=%v", handle.ID, count, err)
		}
	}
}
