package flow

import (
	"context"
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
		fault.ClaimExecutionLock,
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
			var calls atomic.Int32
			runtime, err := New(database.DB, WithSchema(database.Schema), WithWorkerConcurrency(1),
				WithPollInterval(5*time.Millisecond), WithCommandLease(300*time.Millisecond))
			if err != nil {
				t.Fatalf("New() error = %v", err)
			}
			if err := runtime.Register(Handle(command, func(context.Context, *Work[runtimeArgs]) (runtimeResult, error) {
				calls.Add(1)
				return runtimeResult{Value: "ok"}, nil
			}, WithCommit(func(ctx context.Context, tx Tx, commit Commit[runtimeArgs, runtimeResult]) error {
				_, err := tx.Exec(ctx, `INSERT INTO `+pgschema.Table(database.Schema, "fault_commit")+` (command_id) VALUES ($1)`, commit.Info.CommandID)
				return err
			}))); err != nil {
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
			handle, err := command.With(runtime).Execute(ctx, "fault", runtimeArgs{})
			if err != nil {
				t.Fatalf("Execute() error = %v", err)
			}
			waitForExecutionStatus(t, database.Schema, database.DB.Conn, handle.ID, "succeeded", 5*time.Second)
			stopRuntime(t, cancelRun, runResult)
			if calls.Load() != 1 {
				t.Fatalf("worker calls = %d", calls.Load())
			}
			var starts, conclusions, terminalEvents int
			if err := database.DB.Conn.QueryRow(ctx, `SELECT
				count(*) FILTER (WHERE entry_kind='attempt_started'),
				count(*) FILTER (WHERE entry_kind='attempt_concluded'),
				count(*) FILTER (WHERE entry_kind='event_recorded' AND event_class='command_terminal')
				FROM `+pgschema.Table(database.Schema, "flow_journal")+` WHERE execution_id=$1`, handle.ID).
				Scan(&starts, &conclusions, &terminalEvents); err != nil {
				t.Fatalf("count history: %v", err)
			}
			if starts != 1 || conclusions != 1 || terminalEvents != 1 {
				t.Fatalf("history starts=%d conclusions=%d terminal=%d", starts, conclusions, terminalEvents)
			}
			var commitRows int
			if err := database.DB.Conn.QueryRow(ctx, `SELECT count(*) FROM `+pgschema.Table(database.Schema, "fault_commit")+`
				WHERE command_id=$1`, handle.RootCommandID).Scan(&commitRows); err != nil || commitRows != 1 {
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
	command := DefineCommand[runtimeArgs, runtimeResult]("fault.settlement_outage", 1, WithMaxAttempts(1))
	first, err := New(database.DB, WithSchema(database.Schema), WithWorkerConcurrency(1),
		WithPollInterval(5*time.Millisecond), WithCommandLease(90*time.Millisecond))
	if err != nil {
		t.Fatalf("New(first) error = %v", err)
	}
	firstReturned := make(chan struct{}, 1)
	if err := first.Register(Handle(command, func(context.Context, *Work[runtimeArgs]) (runtimeResult, error) {
		firstReturned <- struct{}{}
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
	handle, err := command.With(first).Execute(ctx, "settlement-outage", runtimeArgs{})
	if err != nil {
		t.Fatalf("Execute() error = %v", err)
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
		WithPollInterval(5*time.Millisecond), WithCommandLease(90*time.Millisecond))
	if err != nil {
		t.Fatalf("New(second) error = %v", err)
	}
	if err := second.Register(Handle(command, func(context.Context, *Work[runtimeArgs]) (runtimeResult, error) {
		return runtimeResult{Value: "recovered"}, nil
	})); err != nil {
		t.Fatalf("Register(second) error = %v", err)
	}
	cancelSecond, secondResult := startRuntime(t, second)
	waitForExecutionStatus(t, database.Schema, database.DB.Conn, handle.ID, "succeeded", 5*time.Second)
	trace, traceErr := Trace(ctx, second, handle.ID)
	stopRuntime(t, cancelSecond, secondResult)
	if traceErr != nil {
		t.Fatalf("Trace() error = %v", traceErr)
	}
	if len(trace.Commands) != 1 || len(trace.Commands[0].Attempts) != 2 ||
		trace.Commands[0].Attempts[0].Classification != "lease_lost" ||
		trace.Commands[0].Attempts[0].ConsumedBudget ||
		trace.Commands[0].Attempts[1].Classification != "succeeded" ||
		trace.Commands[0].FailureCode != "" {
		t.Fatalf("outage Trace = %#v", trace)
	}
}
