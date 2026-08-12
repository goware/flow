package flow

import (
	"context"
	"errors"
	"strings"
	"sync"
	"sync/atomic"
	"testing"
	"time"

	"github.com/goware/flow/internal/fault"
	"github.com/goware/flow/internal/pgschema"
	"github.com/goware/flow/internal/testpg"
)

func TestCommandFaultBoundariesRecoverWithoutDuplicateProgress(t *testing.T) {
	t.Parallel()

	for _, point := range []fault.Point{
		fault.ClaimRunLock,
		fault.ClaimBeforeJournal,
		fault.ClaimBeforeCommit,
		fault.ClaimCommitAmbiguous,
		fault.SettleAfterFence,
		fault.SettleAfterAttempt,
		fault.SettleAfterChildren,
		fault.SettleAfterEvents,
		fault.SettleBeforeCommitFunction,
		fault.SettleAfterCommitFunction,
		fault.SettleBeforeCommit,
		fault.SettleCommitAmbiguous,
	} {
		point := point
		t.Run(string(point), func(t *testing.T) {
			t.Parallel()
			database := testpg.Open(t)
			ctx := context.Background()
			if err := Migrate(ctx, database.DB, WithSchema(database.Schema)); err != nil {
				t.Fatalf("Migrate() error = %v", err)
			}
			if _, err := database.DB.Conn.Exec(ctx, `CREATE TABLE `+pgschema.Table(database.Schema, "fault_commit")+`
				(command_id text PRIMARY KEY)`); err != nil {
				t.Fatalf("create commit table: %v", err)
			}
			command := DefineCommand[runtimeArgs, runtimeResult]("fault.command."+string(point), 1)
			child := DefineCommand[None, None]("fault.child."+string(point), 1)
			event := DefineEvent[runtimeResult]("fault.event." + string(point))
			var calls atomic.Int32
			runtime, err := New(database.DB, WithSchema(database.Schema), WithWorkerConcurrency(1),
				WithPollInterval(5*time.Millisecond), withCommandLeaseForTest(300*time.Millisecond))
			if err != nil {
				t.Fatalf("New() error = %v", err)
			}
			if err := runtime.Register(Handle(command, func(_ context.Context, work *Work[runtimeArgs]) (runtimeResult, error) {
				calls.Add(1)
				if err := Emit(work, event, "accepted", runtimeResult{Value: "ok"}); err != nil {
					return runtimeResult{}, err
				}
				Enqueue(work, "child", child, None{})
				return runtimeResult{Value: "ok"}, nil
			}, WithCommit(func(ctx context.Context, tx Tx, commit Commit[runtimeArgs, runtimeResult]) error {
				_, err := tx.Exec(ctx, `INSERT INTO `+pgschema.Table(database.Schema, "fault_commit")+` (command_id) VALUES ($1)`, commit.Info.CommandID)
				return err
			})), Handle(child, func(context.Context, *Work[None]) (None, error) { return None{}, nil })); err != nil {
				t.Fatalf("Register() error = %v", err)
			}
			var hookMu sync.Mutex
			injected := false
			runtime.faults = fault.Func(func(_ context.Context, got fault.Point) error {
				hookMu.Lock()
				defer hookMu.Unlock()
				if got == point && !injected {
					injected = true
					return fault.Injected(got)
				}
				return nil
			})
			cancelRun, runResult := startRuntime(t, runtime)
			exec, err := command.Enqueue(ctx, runtime, "fault", runtimeArgs{})
			if err != nil {
				t.Fatalf("Enqueue() error = %v", err)
			}
			waitForRunStatus(t, database.Schema, database.DB.Conn, exec.ID, "succeeded", 5*time.Second)
			stopRuntime(t, cancelRun, runResult)
			if calls.Load() != 1 {
				t.Fatalf("worker calls = %d", calls.Load())
			}
			var starts, conclusions, terminalEvents, applicationEvents int
			if err := database.DB.Conn.QueryRow(ctx, `SELECT
				count(*) FILTER (WHERE entry_kind='attempt_started'),
				count(*) FILTER (WHERE entry_kind='attempt_concluded'),
				count(*) FILTER (WHERE entry_kind='event_recorded' AND event_class='command_terminal'),
				count(*) FILTER (WHERE entry_kind='event_recorded' AND event_class='application')
				FROM `+pgschema.Table(database.Schema, "flow_journal")+` WHERE run_id=$1`, exec.ID).
				Scan(&starts, &conclusions, &terminalEvents, &applicationEvents); err != nil {
				t.Fatalf("count history: %v", err)
			}
			if starts != 2 || conclusions != 2 || terminalEvents != 2 || applicationEvents != 1 {
				t.Fatalf("history starts=%d conclusions=%d terminal=%d application=%d", starts, conclusions, terminalEvents, applicationEvents)
			}
			var commitRows int
			if err := database.DB.Conn.QueryRow(ctx, `SELECT count(*) FROM `+pgschema.Table(database.Schema, "fault_commit")+`
				WHERE command_id=$1`, exec.RootCommandID).Scan(&commitRows); err != nil || commitRows != 1 {
				t.Fatalf("commit rows=%d error=%v", commitRows, err)
			}
		})
	}
}

func TestSettlementOutageRecoversByLeaseExpiry(t *testing.T) {
	t.Parallel()

	database := testpg.Open(t)
	ctx := context.Background()
	if err := Migrate(ctx, database.DB, WithSchema(database.Schema)); err != nil {
		t.Fatalf("Migrate() error = %v", err)
	}
	command := DefineCommand[runtimeArgs, runtimeResult]("fault.settlement_outage", 1, WithRetry(Attempts(1)))
	event := DefineEvent[runtimeResult]("fault.settlement_outage_event")
	child := DefineCommand[runtimeArgs, runtimeResult]("fault.settlement_outage_child", 1)
	first, err := New(database.DB, WithSchema(database.Schema), WithWorkerConcurrency(1),
		WithPollInterval(5*time.Millisecond), withCommandLeaseForTest(90*time.Millisecond))
	if err != nil {
		t.Fatalf("New(first) error = %v", err)
	}
	firstReturned := make(chan struct{}, 1)
	if err := first.Register(Handle(command, func(_ context.Context, work *Work[runtimeArgs]) (runtimeResult, error) {
		firstReturned <- struct{}{}
		if err := Emit(work, event, "settled", runtimeResult{Value: "unsettled"}); err != nil {
			return runtimeResult{}, err
		}
		Enqueue(work, "discarded-child", child, runtimeArgs{Value: "unsettled"})
		return runtimeResult{Value: "unsettled"}, nil
	})); err != nil {
		t.Fatalf("Register(first) error = %v", err)
	}
	first.faults = fault.Func(func(_ context.Context, point fault.Point) error {
		if point == fault.SettleAfterFence {
			return fault.Injected(point)
		}
		return nil
	})
	cancelFirst, firstResult := startRuntime(t, first)
	exec, err := command.Enqueue(ctx, first, "settlement-outage", runtimeArgs{})
	if err != nil {
		t.Fatalf("Enqueue() error = %v", err)
	}
	select {
	case <-firstReturned:
	case <-time.After(3 * time.Second):
		t.Fatal("first worker did not return")
	}
	// Let all bounded settlement attempts fail before stopping renewal.
	time.Sleep(75 * time.Millisecond)
	stopRuntime(t, cancelFirst, firstResult)

	second, err := New(database.DB, WithSchema(database.Schema), WithWorkerConcurrency(1),
		WithPollInterval(5*time.Millisecond), withCommandLeaseForTest(90*time.Millisecond))
	if err != nil {
		t.Fatalf("New(second) error = %v", err)
	}
	if err := second.Register(Handle(command, func(_ context.Context, work *Work[runtimeArgs]) (runtimeResult, error) {
		if err := Emit(work, event, "settled", runtimeResult{Value: "recovered"}); err != nil {
			return runtimeResult{}, err
		}
		return runtimeResult{Value: "recovered"}, nil
	})); err != nil {
		t.Fatalf("Register(second) error = %v", err)
	}
	cancelSecond, secondResult := startRuntime(t, second)
	waitForRunStatus(t, database.Schema, database.DB.Conn, exec.ID, "succeeded", 5*time.Second)
	trace, traceErr := Trace(ctx, second, exec.ID)
	stopRuntime(t, cancelSecond, secondResult)
	if traceErr != nil {
		t.Fatalf("Trace() error = %v", traceErr)
	}
	if len(trace.Commands) != 1 || len(trace.Commands[0].Attempts) != 2 ||
		trace.Commands[0].Attempts[0].Classification != "lease_lost" ||
		trace.Commands[0].Attempts[0].ConsumedBudget ||
		trace.Commands[0].Attempts[1].Classification != "succeeded" ||
		trace.Commands[0].Failure != nil {
		t.Fatalf("outage Trace = %#v", trace)
	}
	applicationEvents := 0
	for _, recorded := range trace.Events {
		if recorded.Class == "application" {
			applicationEvents++
			if !strings.Contains(string(recorded.Body), "recovered") || strings.Contains(string(recorded.Body), "unsettled") {
				t.Fatalf("lost lease retained stale event body %s", recorded.Body)
			}
		}
	}
	if applicationEvents != 1 {
		t.Fatalf("outage application events=%d trace=%#v", applicationEvents, trace.Events)
	}
	var childCount int
	if err := database.DB.Conn.QueryRow(ctx, `SELECT count(*) FROM `+pgschema.Table(database.Schema, "flow_commands")+`
		WHERE run_id=$1 AND parent_command_id IS NOT NULL`, exec.ID).Scan(&childCount); err != nil {
		t.Fatal(err)
	}
	if childCount != 0 {
		t.Fatalf("lost lease exposed %d staged children", childCount)
	}
}

func TestExhaustedSettlementLoopsAreObserved(t *testing.T) {
	t.Parallel()

	database := testpg.Open(t)
	ctx := context.Background()
	if err := Migrate(ctx, database.DB, WithSchema(database.Schema)); err != nil {
		t.Fatal(err)
	}
	observer := &recordingObserver{}
	succeeds := DefineCommand[None, None]("fault.exhausted_success_settlement", 1)
	fails := DefineCommand[None, None]("fault.exhausted_conclusion", 1)
	runtime, err := New(database.DB, WithSchema(database.Schema), WithNotifications(false),
		WithWorkerConcurrency(2), WithPollInterval(5*time.Millisecond), WithObserver(observer))
	if err != nil {
		t.Fatal(err)
	}
	if err := runtime.Register(
		Handle(succeeds, func(context.Context, *Work[None]) (None, error) { return None{}, nil }),
		Handle(fails, func(context.Context, *Work[None]) (None, error) { return None{}, errors.New("expected failure") }),
	); err != nil {
		t.Fatal(err)
	}
	runtime.faults = fault.Func(func(_ context.Context, point fault.Point) error {
		if point == fault.SettleAfterFence {
			return fault.Injected(point)
		}
		return nil
	})
	cancel, runResult := startRuntime(t, runtime)
	defer stopRuntime(t, cancel, runResult)
	if _, err := succeeds.Enqueue(ctx, runtime, "settle/error", None{}); err != nil {
		t.Fatal(err)
	}
	if _, err := fails.Enqueue(ctx, runtime, "conclude/error", None{}); err != nil {
		t.Fatal(err)
	}
	waitForObservation(t, observer, "settle", "error", 1, 2*time.Second)
	waitForObservation(t, observer, "conclude", "error", 1, 2*time.Second)
}
