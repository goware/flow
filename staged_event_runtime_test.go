package flow

import (
	"context"
	"errors"
	"fmt"
	"testing"
	"time"

	"github.com/goware/flow/internal/pgschema"
	"github.com/goware/flow/internal/testpg"
)

type stagedEventPayload struct {
	Value string `json:"value"`
}

func TestRuntimeStagesOneHundredChildrenAsOnePersistenceBatch(t *testing.T) {
	t.Parallel()
	database := testpg.Open(t)
	ctx := context.Background()
	if err := Migrate(ctx, database.DB, WithSchema(database.Schema)); err != nil {
		t.Fatal(err)
	}
	parent := DefineCommand[None, None]("staged.batch_100_parent", 1)
	child := DefineCommand[None, None]("staged.batch_100_child", 1)
	runtime, err := New(database.DB, WithSchema(database.Schema), WithMaxCommandsPerExecution(0),
		WithWorkerConcurrency(1), WithPollInterval(5*time.Millisecond), WithNotifications(false))
	if err != nil {
		t.Fatal(err)
	}
	if err := runtime.Register(Handle(parent, func(_ context.Context, work *Work[None]) (None, error) {
		for index := range 100 {
			Execute(work, fmt.Sprintf("child/%03d", index), child, None{})
		}
		return None{}, nil
	})); err != nil {
		t.Fatal(err)
	}
	cancel, runResult := startRuntime(t, runtime)
	defer stopRuntime(t, cancel, runResult)
	exec, err := parent.With(runtime).Execute(ctx, "batch/100", None{})
	if err != nil {
		t.Fatal(err)
	}

	deadline := time.Now().Add(5 * time.Second)
	for {
		var commandCount, openCommands, children, queued, waits int
		err := database.DB.Conn.QueryRow(ctx, `SELECT e.command_count,e.open_commands,
			(SELECT count(*) FROM `+pgschema.Table(database.Schema, "flow_commands")+` c
			 WHERE c.execution_id=e.execution_id AND c.parent_command_id=$2),
			(SELECT count(*) FROM `+pgschema.Table(database.Schema, "flow_command_queue")+` q
			 WHERE q.execution_id=e.execution_id AND q.command_id<>$2),
			(SELECT count(*) FROM `+pgschema.Table(database.Schema, "flow_command_event_waits")+` w
			 WHERE w.execution_id=e.execution_id)
			FROM `+pgschema.Table(database.Schema, "flow_executions")+` e WHERE e.execution_id=$1`,
			exec.ID, exec.RootCommandID).Scan(&commandCount, &openCommands, &children, &queued, &waits)
		if err == nil && commandCount == 101 && openCommands == 100 && children == 100 && queued == 100 && waits == 0 {
			break
		}
		if time.Now().After(deadline) {
			t.Fatalf("batched children shape=%d/%d children=%d queued=%d waits=%d err=%v",
				commandCount, openCommands, children, queued, waits, err)
		}
		time.Sleep(5 * time.Millisecond)
	}
	var mapped int
	if err := database.DB.Conn.QueryRow(ctx, `SELECT count(*)
		FROM `+pgschema.Table(database.Schema, "flow_commands")+` c
		JOIN `+pgschema.Table(database.Schema, "flow_journal")+` j
		  ON j.execution_id=c.execution_id AND j.position=c.created_position
		 AND j.entry_kind='command_created' AND j.command_id=c.command_id
		WHERE c.execution_id=$1 AND c.parent_command_id=$2`, exec.ID, exec.RootCommandID).Scan(&mapped); err != nil {
		t.Fatal(err)
	}
	if mapped != 100 {
		t.Fatalf("journal-created child mappings=%d, want 100", mapped)
	}
	assertReplayMatches(t, runtime, exec.ID)
	if err := CancelExecution(ctx, runtime, exec.ID, "batch test complete"); err != nil {
		t.Fatal(err)
	}
}

func TestRuntimeStagesMixedRetainedAndNewEventWaitBatch(t *testing.T) {
	t.Parallel()
	database := testpg.Open(t)
	ctx := context.Background()
	if err := Migrate(ctx, database.DB, WithSchema(database.Schema)); err != nil {
		t.Fatal(err)
	}
	if _, err := database.DB.Conn.Exec(ctx, `CREATE TABLE `+pgschema.Table(database.Schema, "batched_decision_commits")+`
		(execution_id text PRIMARY KEY)`); err != nil {
		t.Fatal(err)
	}
	fact := DefineEvent[None]("staged.batch_wait_fact")
	parent := DefineCommand[None, None]("staged.batch_wait_parent", 1)
	child := DefineCommand[None, None]("staged.batch_wait_child", 1)
	runtime, err := New(database.DB, WithSchema(database.Schema), WithMaxCommandsPerExecution(0),
		WithWorkerConcurrency(1), WithPollInterval(5*time.Millisecond), WithNotifications(false))
	if err != nil {
		t.Fatal(err)
	}
	if err := runtime.Register(Handle(parent, func(_ context.Context, work *Work[None]) (None, error) {
		for index := 40; index < 60; index++ {
			if err := Emit(work, fact, fmt.Sprintf("staged/%03d", index), None{}); err != nil {
				return None{}, err
			}
		}
		for index := range 100 {
			node := Execute(work, fmt.Sprintf("child/%03d", index), child, None{})
			switch {
			case index < 20:
			case index < 40:
				node.WaitFor(fact, "retained/shared")
			case index < 60:
				node.WaitFor(fact, fmt.Sprintf("staged/%03d", index))
			case index < 80:
				node.WaitFor(fact, "retained/shared").WaitFor(fact, "missing/shared").Within(time.Minute)
			default:
				node.WaitFor(fact, "missing/shared").Within(time.Minute)
			}
		}
		return None{}, nil
	}, WithCommit(func(ctx context.Context, tx Tx, commit Commit[None, None]) error {
		_, err := tx.Exec(ctx, `INSERT INTO `+pgschema.Table(database.Schema, "batched_decision_commits")+`
			(execution_id) VALUES ($1)`, commit.Info.ExecutionID)
		return err
	}))); err != nil {
		t.Fatal(err)
	}
	exec, err := parent.With(runtime).Execute(ctx, "batch/mixed", None{}, WithStartDelay(100*time.Millisecond))
	if err != nil {
		t.Fatal(err)
	}
	if err := fact.Emit(ctx, runtime, exec.ID, "retained/shared", None{}); err != nil {
		t.Fatal(err)
	}
	cancel, runResult := startRuntime(t, runtime)
	defer stopRuntime(t, cancel, runResult)

	deadline := time.Now().Add(5 * time.Second)
	for {
		var commandCount int
		if err := database.DB.Conn.QueryRow(ctx, `SELECT command_count FROM `+
			pgschema.Table(database.Schema, "flow_executions")+` WHERE execution_id=$1`, exec.ID).Scan(&commandCount); err != nil {
			t.Fatal(err)
		}
		if commandCount == 101 {
			break
		}
		if time.Now().After(deadline) {
			t.Fatalf("mixed batch command count=%d, want 101", commandCount)
		}
		time.Sleep(5 * time.Millisecond)
	}

	var ready, pending, unsatisfied, queued, waits, satisfied, mapped, commitRows int
	if err := database.DB.Conn.QueryRow(ctx, `SELECT
			count(*) FILTER (WHERE c.state='ready'),
			count(*) FILTER (WHERE c.state='pending'),
			coalesce(sum(c.unsatisfied_waits),0),
			(SELECT count(*) FROM `+pgschema.Table(database.Schema, "flow_command_queue")+` q
			 WHERE q.execution_id=$1 AND q.command_id<>$2),
			(SELECT count(*) FROM `+pgschema.Table(database.Schema, "flow_command_event_waits")+` w
			 WHERE w.execution_id=$1),
			(SELECT count(*) FROM `+pgschema.Table(database.Schema, "flow_command_event_waits")+` w
			 WHERE w.execution_id=$1 AND w.satisfied_position IS NOT NULL),
			(SELECT count(*) FROM `+pgschema.Table(database.Schema, "flow_commands")+` mapped_command
			 JOIN `+pgschema.Table(database.Schema, "flow_journal")+` j
			   ON j.execution_id=mapped_command.execution_id AND j.position=mapped_command.created_position
			  AND j.entry_kind='command_created' AND j.command_id=mapped_command.command_id
			 WHERE mapped_command.execution_id=$1 AND mapped_command.parent_command_id=$2),
			(SELECT count(*) FROM `+pgschema.Table(database.Schema, "batched_decision_commits")+`
			 WHERE execution_id=$1::text)
		FROM `+pgschema.Table(database.Schema, "flow_commands")+` c
		WHERE c.execution_id=$1 AND c.parent_command_id=$2`, exec.ID, exec.RootCommandID).
		Scan(&ready, &pending, &unsatisfied, &queued, &waits, &satisfied, &mapped, &commitRows); err != nil {
		t.Fatal(err)
	}
	if ready != 60 || pending != 40 || unsatisfied != 40 || queued != 60 ||
		waits != 100 || satisfied != 60 || mapped != 100 || commitRows != 1 {
		t.Fatalf("mixed batch ready=%d pending=%d unsatisfied=%d queued=%d waits=%d satisfied=%d mapped=%d commits=%d",
			ready, pending, unsatisfied, queued, waits, satisfied, mapped, commitRows)
	}
	var retainedPosition int64
	var retainedMatches, stagedMatches, missingMatches int
	if err := database.DB.Conn.QueryRow(ctx, `SELECT
			(SELECT position FROM `+pgschema.Table(database.Schema, "flow_journal")+`
			 WHERE execution_id=$1 AND event_class='application' AND event_name=$2 AND event_key='retained/shared'),
			count(*) FILTER (WHERE w.event_key='retained/shared' AND w.satisfied_position IS NOT NULL),
			count(*) FILTER (WHERE w.event_key LIKE 'staged/%' AND j.position IS NOT NULL
			 AND j.event_name=w.event_name AND j.event_key=w.event_key),
			count(*) FILTER (WHERE w.event_key='missing/shared' AND w.satisfied_position IS NULL)
		FROM `+pgschema.Table(database.Schema, "flow_command_event_waits")+` w
		LEFT JOIN `+pgschema.Table(database.Schema, "flow_journal")+` j
		  ON j.execution_id=w.execution_id AND j.position=w.satisfied_position
		WHERE w.execution_id=$1`, exec.ID, fact.Name()).
		Scan(&retainedPosition, &retainedMatches, &stagedMatches, &missingMatches); err != nil {
		t.Fatal(err)
	}
	var exactRetainedMatches int
	if err := database.DB.Conn.QueryRow(ctx, `SELECT count(*) FROM `+
		pgschema.Table(database.Schema, "flow_command_event_waits")+`
		WHERE execution_id=$1 AND event_key='retained/shared' AND satisfied_position=$2`,
		exec.ID, retainedPosition).Scan(&exactRetainedMatches); err != nil {
		t.Fatal(err)
	}
	if retainedMatches != 40 || exactRetainedMatches != 40 || stagedMatches != 20 || missingMatches != 40 {
		t.Fatalf("mixed wait positions retained=%d exact=%d staged=%d missing=%d",
			retainedMatches, exactRetainedMatches, stagedMatches, missingMatches)
	}
	trace, err := Trace(ctx, runtime, exec.ID)
	if err != nil {
		t.Fatal(err)
	}
	if len(trace.Commands) != 101 {
		t.Fatalf("mixed batch trace commands=%d, want 101", len(trace.Commands))
	}
	assertReplayMatches(t, runtime, exec.ID)
	if err := CancelExecution(ctx, runtime, exec.ID, "mixed batch test complete"); err != nil {
		t.Fatal(err)
	}
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
	if len(poisonedTrace.Commands) != 1 || poisonedTrace.Commands[0].Failure == nil || poisonedTrace.Commands[0].Failure.Code != "invalid_decision" {
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
	for _, exec := range []Execution{failureHandle, commitFailureHandle, commitIngressHandle} {
		var eventCount, childCount int
		if err := database.DB.Conn.QueryRow(ctx, `SELECT
			count(*) FILTER (WHERE event_class='application'),
			(SELECT count(*) FROM `+pgschema.Table(database.Schema, "flow_commands")+`
			 WHERE execution_id=$1 AND parent_command_id IS NOT NULL)
			FROM `+pgschema.Table(database.Schema, "flow_journal")+` WHERE execution_id=$1`, exec.ID).
			Scan(&eventCount, &childCount); err != nil {
			t.Fatal(err)
		}
		if eventCount != 0 || childCount != 0 {
			t.Fatalf("rolled-back decision %s exposed events=%d children=%d", exec.ID, eventCount, childCount)
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
	for _, exec := range []Execution{equivalent, conflicting} {
		var count int
		if err := database.DB.Conn.QueryRow(ctx, `SELECT count(*) FROM `+pgschema.Table(database.Schema, "flow_journal")+`
			WHERE execution_id=$1 AND event_class='application'`, exec.ID).Scan(&count); err != nil || count != 1 {
			t.Fatalf("execution=%s application events=%d err=%v", exec.ID, count, err)
		}
	}
}
