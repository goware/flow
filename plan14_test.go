package flow

import (
	"context"
	"errors"
	"fmt"
	"strings"
	"sync"
	"testing"
	"time"

	"github.com/goware/flow/internal/fault"
	"github.com/goware/flow/internal/pgschema"
	"github.com/goware/flow/internal/store"
	"github.com/goware/flow/internal/testpg"
	"github.com/goware/flow/internal/uuid"
	"github.com/jackc/pgx/v5"
)

type plan14ProtocolCounts struct {
	Queries  int
	Batches  int
	CopyFrom int
}

type plan14ProtocolTracer struct {
	mu     sync.Mutex
	counts plan14ProtocolCounts
}

type plan14ProbeObserver struct {
	once     sync.Once
	observed chan struct{}
}

type plan14AwaitRunReadContextKey struct{}

type plan14AwaitRunReadBarrier struct {
	done chan struct{}
	once sync.Once
}

type plan14AwaitRunTracer struct {
	queryRecorder
	barrierMu sync.Mutex
	getRunSQL string
	barrier   *plan14AwaitRunReadBarrier
}

func (tracer *plan14AwaitRunTracer) arm(getRunSQL string) <-chan struct{} {
	tracer.barrierMu.Lock()
	defer tracer.barrierMu.Unlock()
	tracer.getRunSQL = getRunSQL
	tracer.barrier = &plan14AwaitRunReadBarrier{done: make(chan struct{})}
	return tracer.barrier.done
}

func (tracer *plan14AwaitRunTracer) TraceQueryStart(
	ctx context.Context,
	conn *pgx.Conn,
	data pgx.TraceQueryStartData,
) context.Context {
	ctx = tracer.queryRecorder.TraceQueryStart(ctx, conn, data)
	tracer.barrierMu.Lock()
	barrier := tracer.barrier
	matches := barrier != nil && data.SQL == tracer.getRunSQL
	tracer.barrierMu.Unlock()
	if matches {
		ctx = context.WithValue(ctx, plan14AwaitRunReadContextKey{}, barrier)
	}
	return ctx
}

func (tracer *plan14AwaitRunTracer) TraceQueryEnd(
	ctx context.Context,
	conn *pgx.Conn,
	data pgx.TraceQueryEndData,
) {
	tracer.queryRecorder.TraceQueryEnd(ctx, conn, data)
	if barrier, ok := ctx.Value(plan14AwaitRunReadContextKey{}).(*plan14AwaitRunReadBarrier); data.Err == nil && ok {
		barrier.once.Do(func() { close(barrier.done) })
	}
}

func plan14GetRunSQL(schema string) string {
	return `SELECT run_id,definition_name,definition_version,run_key,status,
	max_commands,command_count,open_commands,deadline_at,failure,created_at,updated_at,status_at,
	finished_at,root_command_id FROM ` + pgschema.Table(schema, "flow_runs") + ` WHERE run_id=$1`
}

func (observer *plan14ProbeObserver) Observe(_ context.Context, observation Observation) {
	if observation.Kind == ObservationClaim && observation.Operation == "probe" && observation.Outcome == "ok" {
		observer.once.Do(func() { close(observer.observed) })
	}
}

func (tracer *plan14ProtocolTracer) TraceQueryStart(ctx context.Context, _ *pgx.Conn, _ pgx.TraceQueryStartData) context.Context {
	tracer.mu.Lock()
	tracer.counts.Queries++
	tracer.mu.Unlock()
	return ctx
}

func (*plan14ProtocolTracer) TraceQueryEnd(context.Context, *pgx.Conn, pgx.TraceQueryEndData) {}

func (tracer *plan14ProtocolTracer) TraceBatchStart(ctx context.Context, _ *pgx.Conn, _ pgx.TraceBatchStartData) context.Context {
	tracer.mu.Lock()
	tracer.counts.Batches++
	tracer.mu.Unlock()
	return ctx
}

func (*plan14ProtocolTracer) TraceBatchQuery(context.Context, *pgx.Conn, pgx.TraceBatchQueryData) {}
func (*plan14ProtocolTracer) TraceBatchEnd(context.Context, *pgx.Conn, pgx.TraceBatchEndData)     {}

func (tracer *plan14ProtocolTracer) TraceCopyFromStart(ctx context.Context, _ *pgx.Conn, _ pgx.TraceCopyFromStartData) context.Context {
	tracer.mu.Lock()
	tracer.counts.CopyFrom++
	tracer.mu.Unlock()
	return ctx
}

func (*plan14ProtocolTracer) TraceCopyFromEnd(context.Context, *pgx.Conn, pgx.TraceCopyFromEndData) {}

func (tracer *plan14ProtocolTracer) reset() {
	tracer.mu.Lock()
	tracer.counts = plan14ProtocolCounts{}
	tracer.mu.Unlock()
}

func (tracer *plan14ProtocolTracer) snapshot() plan14ProtocolCounts {
	tracer.mu.Lock()
	defer tracer.mu.Unlock()
	return tracer.counts
}

func TestPlan14CommandProbeReturnsGlobalFutureDelay(t *testing.T) {
	database := testpg.Open(t)
	ctx := context.Background()
	if err := Migrate(ctx, database.DB, WithSchema(database.Schema)); err != nil {
		t.Fatal(err)
	}
	runtime, err := New(database.DB, WithSchema(database.Schema), WithNotifications(false))
	if err != nil {
		t.Fatal(err)
	}
	handled := DefineCommand[None, None]("plan14.probe.handled", 1)
	unhandled := DefineCommand[None, None]("plan14.probe.unhandled", 1)
	due, err := handled.Enqueue(ctx, runtime, "due", None{}, WithoutRunDeadline())
	if err != nil {
		t.Fatal(err)
	}
	future, err := handled.Enqueue(ctx, runtime, "future", None{}, WithStartDelay(10*time.Second), WithoutRunDeadline())
	if err != nil {
		t.Fatal(err)
	}
	if _, err := unhandled.Enqueue(ctx, runtime, "unregistered-earlier", None{}, WithStartDelay(250*time.Millisecond), WithoutRunDeadline()); err != nil {
		t.Fatal(err)
	}

	kinds := []store.CommandKind{{Name: handled.Name(), Version: handled.Version()}}
	probe, err := runtime.store.ProbeCommandsExcluding(ctx, kinds, 10, nil, nil, nil)
	if err != nil {
		t.Fatal(err)
	}
	if len(probe.Candidates) != 1 || probe.Candidates[0].RunID.String() != string(due.RunID) {
		t.Fatalf("due candidates = %#v", probe.Candidates)
	}
	if probe.FutureDelay == nil || *probe.FutureDelay <= 5*time.Second || *probe.FutureDelay > 10*time.Second {
		t.Fatalf("future delay = %v, want handled future row and not earlier unregistered row", probe.FutureDelay)
	}

	cursor := commandProbeCursor(probe.Candidates[0])
	excluded, err := runtime.store.ProbeCommandsExcluding(ctx, kinds, 10,
		[]uuid.UUID{mustStoreUUID(t, string(future.RunID))}, []string{"default"}, cursor)
	if err != nil {
		t.Fatal(err)
	}
	if len(excluded.Candidates) != 0 || excluded.FutureDelay == nil || *excluded.FutureDelay <= 5*time.Second {
		t.Fatalf("excluded probe = %#v, horizon must ignore cursor/run/queue exclusions", excluded)
	}

	if err := CancelRun(ctx, runtime, future.RunID, "terminal rows do not set a horizon"); err != nil {
		t.Fatal(err)
	}
	terminal, err := runtime.store.ProbeCommandsExcluding(ctx, kinds, 10, nil, []string{"default"}, nil)
	if err != nil {
		t.Fatal(err)
	}
	if terminal.FutureDelay != nil {
		t.Fatalf("terminal run set future horizon %s", *terminal.FutureDelay)
	}
}

func TestPlan14CommandProbeClampsFarFutureDelay(t *testing.T) {
	database := testpg.Open(t)
	ctx := context.Background()
	if err := Migrate(ctx, database.DB, WithSchema(database.Schema)); err != nil {
		t.Fatal(err)
	}
	runtime, err := New(database.DB, WithSchema(database.Schema), WithNotifications(false))
	if err != nil {
		t.Fatal(err)
	}
	command := DefineCommand[None, None]("plan14.probe.far_future", 1)
	run, err := command.Enqueue(ctx, runtime, "far-future", None{}, WithStartDelay(time.Hour), WithoutRunDeadline())
	if err != nil {
		t.Fatal(err)
	}
	if _, err := database.DB.Conn.Exec(ctx, `UPDATE `+pgschema.Table(database.Schema, "flow_command_queue")+`
		SET next_run_at=clock_timestamp()+interval '400 years' WHERE run_id=$1`, run.RunID); err != nil {
		t.Fatal(err)
	}

	probe, err := runtime.store.ProbeCommandsExcluding(ctx,
		[]store.CommandKind{{Name: command.Name(), Version: command.Version()}}, 1, nil, nil, nil)
	if err != nil {
		t.Fatal(err)
	}
	if probe.FutureDelay == nil || *probe.FutureDelay != time.Duration(1<<63-1) {
		t.Fatalf("far-future delay = %v, want the maximum positive duration", probe.FutureDelay)
	}
}

func TestPlan14SchedulerDelayHasPositiveFloor(t *testing.T) {
	now := time.Now()
	for _, test := range []struct {
		name         string
		pollInterval time.Duration
		futureWake   time.Time
		want         time.Duration
	}{
		{name: "no horizon", pollInterval: 2 * time.Second, want: 2 * time.Second},
		{name: "elapsed horizon", pollInterval: 2 * time.Second, futureWake: now.Add(-time.Second), want: time.Millisecond},
		{name: "near-zero horizon", pollInterval: 2 * time.Second, futureWake: now.Add(time.Nanosecond), want: time.Millisecond},
		{name: "future horizon", pollInterval: 2 * time.Second, futureWake: now.Add(100 * time.Millisecond), want: 100 * time.Millisecond},
		{name: "poll cap", pollInterval: 2 * time.Second, futureWake: now.Add(3 * time.Second), want: 2 * time.Second},
		{name: "sub-millisecond poll", pollInterval: 500 * time.Microsecond, futureWake: now, want: 500 * time.Microsecond},
	} {
		t.Run(test.name, func(t *testing.T) {
			if got := commandSchedulerDelay(test.pollInterval, test.futureWake, now); got != test.want {
				t.Fatalf("commandSchedulerDelay() = %s, want %s", got, test.want)
			}
		})
	}
}

func TestPlan14ClaimAndSettlementProtocolCensusIsRepeatable(t *testing.T) {
	tracer := &plan14ProtocolTracer{}
	database := testpg.OpenWithQueryTracer(t, tracer)
	ctx := context.Background()
	if err := Migrate(ctx, database.DB, WithSchema(database.Schema)); err != nil {
		t.Fatal(err)
	}
	command := DefineCommand[None, None]("plan14.protocol.census", 1)
	runtime, err := New(database.DB, WithSchema(database.Schema), WithNotifications(false))
	if err != nil {
		t.Fatal(err)
	}
	result, err := command.def.Result.Encode(None{}, maxCommandResultBytes)
	if err != nil {
		t.Fatal(err)
	}
	var claimCounts, settleCounts []plan14ProtocolCounts
	for sample := range 3 {
		run, err := command.Enqueue(ctx, runtime, fmt.Sprintf("sample/%d", sample), None{}, WithoutRunDeadline())
		if err != nil {
			t.Fatal(err)
		}
		candidates, err := runtime.store.ProbeCommands(ctx,
			[]store.CommandKind{{Name: command.Name(), Version: command.Version()}}, 1)
		if err != nil || len(candidates) != 1 {
			t.Fatalf("ProbeCommands() candidates=%d, err=%v", len(candidates), err)
		}
		tracer.reset()
		claimed, err := runtime.store.ClaimCommand(ctx, candidates[0], time.Minute, "plan14-census", fault.None{})
		if err != nil || claimed.Command == nil {
			t.Fatalf("ClaimCommand() = %#v, %v", claimed, err)
		}
		claimCounts = append(claimCounts, tracer.snapshot())
		tracer.reset()
		settled, err := runtime.store.SettleCommandSuccess(ctx, store.CommandSuccess{
			Claim: *claimed.Command, Result: result,
		}, fault.None{})
		if err != nil || !settled.Terminal || settled.Status != "succeeded" {
			t.Fatalf("SettleCommandSuccess() = %#v, %v", settled, err)
		}
		settleCounts = append(settleCounts, tracer.snapshot())
		got, err := GetRun(ctx, runtime, run.RunID)
		if err != nil || got.Status != RunStatusSucceeded {
			t.Fatalf("GetRun() status=%s, err=%v", got.Status, err)
		}
	}
	for index := 1; index < 3; index++ {
		if claimCounts[index] != claimCounts[0] || settleCounts[index] != settleCounts[0] {
			t.Fatalf("protocol census was not repeatable: claims=%v settlements=%v", claimCounts, settleCounts)
		}
	}
	if claimCounts[0].Batches != 1 || settleCounts[0].Batches != 1 {
		t.Fatalf("batched protocol counts: claim=%+v settlement=%+v", claimCounts[0], settleCounts[0])
	}
	t.Logf("claim protocol operations: %+v; simple-success settlement: %+v", claimCounts[0], settleCounts[0])
}

func TestPlan14BatchedClaimFailureRollsBackEveryProjection(t *testing.T) {
	database := testpg.Open(t)
	ctx := context.Background()
	if err := Migrate(ctx, database.DB, WithSchema(database.Schema)); err != nil {
		t.Fatal(err)
	}
	command := DefineCommand[None, None]("plan14.batch.rollback", 1)
	runtime, err := New(database.DB, WithSchema(database.Schema), WithNotifications(false))
	if err != nil {
		t.Fatal(err)
	}
	run, err := command.Enqueue(ctx, runtime, "rollback", None{}, WithoutRunDeadline())
	if err != nil {
		t.Fatal(err)
	}
	candidates, err := runtime.store.ProbeCommands(ctx,
		[]store.CommandKind{{Name: command.Name(), Version: command.Version()}}, 1)
	if err != nil || len(candidates) != 1 {
		t.Fatalf("ProbeCommands() candidates=%d, err=%v", len(candidates), err)
	}
	failFunction := pgschema.Table(database.Schema, "plan14_fail_running_command")
	if _, err := database.DB.Conn.Exec(ctx, `CREATE FUNCTION `+failFunction+`() RETURNS trigger
		LANGUAGE plpgsql AS $$ BEGIN
			IF NEW.state='running' THEN RAISE EXCEPTION 'plan14 injected command projection failure'; END IF;
			RETURN NEW;
		END $$`); err != nil {
		t.Fatal(err)
	}
	if _, err := database.DB.Conn.Exec(ctx, `CREATE TRIGGER plan14_fail_running_command
		BEFORE UPDATE ON `+pgschema.Table(database.Schema, "flow_commands")+`
		FOR EACH ROW EXECUTE FUNCTION `+failFunction+`() `); err != nil {
		t.Fatal(err)
	}
	if _, err := runtime.store.ClaimCommand(ctx, candidates[0], time.Minute, "plan14-batch-failure", fault.None{}); err == nil {
		t.Fatal("ClaimCommand() succeeded despite the injected second-statement failure")
	}
	var commandState, queueState string
	var attemptStarts int
	if err := database.DB.Conn.QueryRow(ctx, `SELECT c.state,q.state,
		(SELECT count(*) FROM `+pgschema.Table(database.Schema, "flow_journal")+`
		 WHERE run_id=c.run_id AND entry_kind='attempt_started')
		FROM `+pgschema.Table(database.Schema, "flow_commands")+` c
		JOIN `+pgschema.Table(database.Schema, "flow_command_queue")+` q ON q.command_id=c.command_id
		WHERE c.run_id=$1 AND c.command_key='root'`, run.RunID).
		Scan(&commandState, &queueState, &attemptStarts); err != nil {
		t.Fatal(err)
	}
	if commandState != "ready" || queueState != "ready" || attemptStarts != 0 {
		t.Fatalf("failed batch persisted command=%s queue=%s attempt_starts=%d",
			commandState, queueState, attemptStarts)
	}
}

func mustStoreUUID(t *testing.T, value string) uuid.UUID {
	t.Helper()
	parsed, err := parseRunID(RunID(value))
	if err != nil {
		t.Fatal(err)
	}
	return parsed
}

func TestPlan14ScheduledCommandUsesDatabaseHorizon(t *testing.T) {
	database := testpg.Open(t)
	ctx := context.Background()
	if err := Migrate(ctx, database.DB, WithSchema(database.Schema)); err != nil {
		t.Fatal(err)
	}
	started := make(chan time.Time, 1)
	command := DefineCommand[None, None]("plan14.scheduler.horizon", 1)
	runtime, err := New(database.DB, WithSchema(database.Schema), WithNotifications(false),
		WithPollInterval(2*time.Second), WithWorkerConcurrency(1))
	if err != nil {
		t.Fatal(err)
	}
	if err := runtime.Register(Handle(command, func(context.Context, *Work[None]) (None, error) {
		started <- time.Now()
		return None{}, nil
	})); err != nil {
		t.Fatal(err)
	}
	run, err := command.Enqueue(ctx, runtime, "delayed", None{}, WithStartDelay(150*time.Millisecond), WithoutRunDeadline())
	if err != nil {
		t.Fatal(err)
	}
	var durableNext time.Time
	if err := database.DB.Conn.QueryRow(ctx, `SELECT next_run_at FROM `+
		pgschema.Table(database.Schema, "flow_command_queue")+` WHERE run_id=$1`, run.RunID).Scan(&durableNext); err != nil {
		t.Fatal(err)
	}
	cancel, result := startRuntime(t, runtime)
	defer stopRuntime(t, cancel, result)
	select {
	case observed := <-started:
		if observed.Before(durableNext) {
			t.Fatalf("handler started at %s before durable next_run_at %s", observed, durableNext)
		}
		if lateness := observed.Sub(durableNext); lateness > time.Second {
			t.Fatalf("handler started %s after durable next_run_at; fallback poll likely won", lateness)
		}
	case <-time.After(1500 * time.Millisecond):
		t.Fatal("delayed command did not beat the two-second fallback poll")
	}
}

func TestPlan14SchedulerWakesForEarlierWorkInsertedAfterCompletedProbe(t *testing.T) {
	database := testpg.Open(t)
	ctx := context.Background()
	if err := Migrate(ctx, database.DB, WithSchema(database.Schema)); err != nil {
		t.Fatal(err)
	}
	observer := &plan14ProbeObserver{observed: make(chan struct{})}
	started := make(chan struct{}, 1)
	command := DefineCommand[None, None]("plan14.scheduler.earlier_after_probe", 1)
	runtime, err := New(database.DB, WithSchema(database.Schema), WithNotifications(false),
		WithPollInterval(2*time.Second), WithWorkerConcurrency(1), WithObserver(observer))
	if err != nil {
		t.Fatal(err)
	}
	if err := runtime.Register(Handle(command, func(context.Context, *Work[None]) (None, error) {
		started <- struct{}{}
		return None{}, nil
	})); err != nil {
		t.Fatal(err)
	}
	if _, err := command.Enqueue(ctx, runtime, "future", None{}, WithStartDelay(time.Hour), WithoutRunDeadline()); err != nil {
		t.Fatal(err)
	}
	cancel, result := startRuntime(t, runtime)
	defer stopRuntime(t, cancel, result)
	select {
	case <-observer.observed:
	case <-time.After(time.Second):
		t.Fatal("scheduler did not complete the initial future-work probe")
	}

	insertedAt := time.Now()
	if _, err := command.Enqueue(ctx, runtime, "earlier", None{}, WithoutRunDeadline()); err != nil {
		t.Fatal(err)
	}
	select {
	case <-started:
		if elapsed := time.Since(insertedAt); elapsed > time.Second {
			t.Fatalf("earlier work started after %s; completed-probe sleep was not interrupted", elapsed)
		}
	case <-time.After(1500 * time.Millisecond):
		t.Fatal("earlier work inserted after a completed probe did not interrupt scheduler sleep")
	}
}

func TestPlan14AwaitRunLocalWakeAndTimerFallback(t *testing.T) {
	t.Run("already terminal performs one durable read", func(t *testing.T) {
		recorder := &queryRecorder{}
		database := testpg.OpenWithQueryTracer(t, recorder)
		ctx := context.Background()
		if err := Migrate(ctx, database.DB, WithSchema(database.Schema)); err != nil {
			t.Fatal(err)
		}
		command := DefineCommand[None, None]("plan14.await.terminal_read", 1)
		runtime, err := New(database.DB, WithSchema(database.Schema), WithNotifications(false))
		if err != nil {
			t.Fatal(err)
		}
		run, err := command.Enqueue(ctx, runtime, "terminal", None{}, WithStartDelay(time.Hour), WithoutRunDeadline())
		if err != nil {
			t.Fatal(err)
		}
		if err := CancelRun(ctx, runtime, run.RunID, "terminal before await"); err != nil {
			t.Fatal(err)
		}
		recorder.reset()
		settled, err := AwaitRun(ctx, runtime, run.RunID)
		if err != nil || settled.Status != RunStatusCancelled {
			t.Fatalf("AwaitRun() status=%s, err=%v", settled.Status, err)
		}
		queries := recorder.snapshot()
		if len(queries) != 1 || !strings.Contains(queries[0], "WHERE run_id=$1") {
			t.Fatalf("already-terminal AwaitRun durable reads = %q, want exactly one run read", queries)
		}
	})

	t.Run("same runtime wake", func(t *testing.T) {
		database := testpg.Open(t)
		ctx := context.Background()
		if err := Migrate(ctx, database.DB, WithSchema(database.Schema)); err != nil {
			t.Fatal(err)
		}
		entered, release := make(chan struct{}), make(chan struct{})
		command := DefineCommand[None, None]("plan14.await.local", 1)
		runtime, err := New(database.DB, WithSchema(database.Schema), WithNotifications(false),
			WithPollInterval(2*time.Second), WithWorkerConcurrency(1))
		if err != nil {
			t.Fatal(err)
		}
		if err := runtime.Register(Handle(command, func(context.Context, *Work[None]) (None, error) {
			close(entered)
			<-release
			return None{}, nil
		})); err != nil {
			t.Fatal(err)
		}
		cancel, result := startRuntime(t, runtime)
		defer stopRuntime(t, cancel, result)
		run, err := command.Enqueue(ctx, runtime, "local", None{}, WithoutRunDeadline())
		if err != nil {
			t.Fatal(err)
		}
		<-entered
		awaited := make(chan error, 1)
		go func() {
			settled, awaitErr := AwaitRun(ctx, runtime, run.RunID)
			if awaitErr == nil && settled.Status != RunStatusSucceeded {
				awaitErr = fmt.Errorf("status %s", settled.Status)
			}
			awaited <- awaitErr
		}()
		time.Sleep(25 * time.Millisecond)
		releasedAt := time.Now()
		close(release)
		select {
		case err := <-awaited:
			if err != nil {
				t.Fatal(err)
			}
			if elapsed := time.Since(releasedAt); elapsed > 150*time.Millisecond {
				t.Fatalf("local wake took %s", elapsed)
			}
		case <-time.After(175 * time.Millisecond):
			t.Fatal("local wake did not beat fallback timer")
		}
	})

	t.Run("remote timer fallback", func(t *testing.T) {
		database := testpg.Open(t)
		ctx := context.Background()
		if err := Migrate(ctx, database.DB, WithSchema(database.Schema)); err != nil {
			t.Fatal(err)
		}
		command := DefineCommand[None, None]("plan14.await.remote", 1)
		reader, err := New(database.DB, WithSchema(database.Schema), WithNotifications(false), WithPollInterval(100*time.Millisecond))
		if err != nil {
			t.Fatal(err)
		}
		worker, err := New(database.DB, WithSchema(database.Schema), WithNotifications(false), WithPollInterval(5*time.Millisecond))
		if err != nil {
			t.Fatal(err)
		}
		entered, release := make(chan struct{}), make(chan struct{})
		if err := worker.Register(Handle(command, func(context.Context, *Work[None]) (None, error) {
			close(entered)
			<-release
			return None{}, nil
		})); err != nil {
			t.Fatal(err)
		}
		cancel, result := startRuntime(t, worker)
		defer stopRuntime(t, cancel, result)
		run, err := command.Enqueue(ctx, reader, "remote", None{}, WithoutRunDeadline())
		if err != nil {
			t.Fatal(err)
		}
		<-entered
		awaited := make(chan error, 1)
		started := time.Now()
		go func() {
			_, awaitErr := AwaitRun(ctx, reader, run.RunID)
			awaited <- awaitErr
		}()
		time.Sleep(20 * time.Millisecond)
		close(release)
		select {
		case err := <-awaited:
			if err != nil {
				t.Fatal(err)
			}
			if elapsed := time.Since(started); elapsed < 50*time.Millisecond || elapsed > 500*time.Millisecond {
				t.Fatalf("remote timer fallback took %s", elapsed)
			}
		case <-time.After(time.Second):
			t.Fatal("timer fallback did not observe remote completion")
		}
	})

	t.Run("unrelated wake and cancellation", func(t *testing.T) {
		database := testpg.Open(t)
		ctx := context.Background()
		if err := Migrate(ctx, database.DB, WithSchema(database.Schema)); err != nil {
			t.Fatal(err)
		}
		command := DefineCommand[None, None]("plan14.await.unrelated", 1)
		runtime, err := New(database.DB, WithSchema(database.Schema), WithNotifications(false), WithPollInterval(time.Hour))
		if err != nil {
			t.Fatal(err)
		}
		run, err := command.Enqueue(ctx, runtime, "pending", None{}, WithStartDelay(time.Hour), WithoutRunDeadline())
		if err != nil {
			t.Fatal(err)
		}
		waitCtx, cancel := context.WithCancel(ctx)
		const waiterCount = 16
		awaited := make(chan error, waiterCount)
		go func() {
			_, awaitErr := AwaitRun(waitCtx, runtime, run.RunID)
			awaited <- awaitErr
		}()
		time.Sleep(20 * time.Millisecond)
		runtime.wake.signal()
		select {
		case err := <-awaited:
			t.Fatalf("unrelated wake returned: %v", err)
		case <-time.After(50 * time.Millisecond):
		}
		for range waiterCount - 1 {
			go func() {
				_, awaitErr := AwaitRun(waitCtx, runtime, run.RunID)
				awaited <- awaitErr
			}()
		}
		cancel()
		for index := range waiterCount {
			select {
			case err := <-awaited:
				if !errors.Is(err, context.Canceled) {
					t.Fatalf("cancellation error[%d] = %v", index, err)
				}
			case <-time.After(time.Second):
				t.Fatalf("AwaitRun waiter %d did not exit after cancellation", index)
			}
		}
	})

	t.Run("unrelated wake traffic is rate limited", func(t *testing.T) {
		recorder := &plan14AwaitRunTracer{}
		database := testpg.OpenWithQueryTracer(t, recorder)
		ctx := context.Background()
		if err := Migrate(ctx, database.DB, WithSchema(database.Schema)); err != nil {
			t.Fatal(err)
		}
		command := DefineCommand[None, None]("plan14.await.unrelated_traffic", 1)
		runtime, err := New(database.DB, WithSchema(database.Schema), WithNotifications(false), WithPollInterval(time.Hour))
		if err != nil {
			t.Fatal(err)
		}
		run, err := command.Enqueue(ctx, runtime, "pending", None{}, WithStartDelay(time.Hour), WithoutRunDeadline())
		if err != nil {
			t.Fatal(err)
		}
		recorder.reset()
		getRunSQL := plan14GetRunSQL(database.Schema)
		initialReadDone := recorder.arm(getRunSQL)
		waitCtx, cancel := context.WithCancel(ctx)
		defer cancel()
		awaited := make(chan error, 1)
		go func() {
			_, awaitErr := AwaitRun(waitCtx, runtime, run.RunID)
			awaited <- awaitErr
		}()
		select {
		case <-initialReadDone:
		case <-time.After(time.Second):
			cancel()
			t.Fatal("AwaitRun did not perform its initial durable read")
		}
		recorder.reset()

		trafficDeadline := time.Now().Add(125 * time.Millisecond)
		for time.Now().Before(trafficDeadline) {
			runtime.wake.signal()
			time.Sleep(time.Millisecond)
		}
		cancel()
		if err := <-awaited; !errors.Is(err, context.Canceled) {
			t.Fatalf("AwaitRun cancellation error = %v", err)
		}
		reads := 0
		for _, query := range recorder.snapshot() {
			if query == getRunSQL {
				reads++
			}
		}
		if reads == 0 || reads > 8 {
			t.Fatalf("AwaitRun performed %d durable reads during unrelated wake traffic, want 1..8", reads)
		}
	})
}

func TestPlan14RunExpiryBulkLoadsRunningDeliveries(t *testing.T) {
	recorder := &queryRecorder{}
	database := testpg.OpenWithQueryTracer(t, recorder)
	ctx := context.Background()
	if err := Migrate(ctx, database.DB, WithSchema(database.Schema)); err != nil {
		t.Fatal(err)
	}
	child := DefineCommand[None, None]("plan14.expiry.child", 1)
	runtime, run := stageClaimFixture(t, database, "plan14_expiry", 100, func(work *Work[None]) {
		for index := range 100 {
			Enqueue(work, fmt.Sprintf("child/%03d", index), child, None{})
		}
	})
	candidates := probeClaimCandidates(t, runtime,
		[]store.CommandKind{{Name: child.Name(), Version: child.Version()}}, 100)
	claimed, err := runtime.store.ClaimCommands(ctx, candidates, time.Minute, "plan14-expiry", fault.None{})
	if err != nil || len(claimed.Commands) != 100 {
		t.Fatalf("ClaimCommands() commands=%d, err=%v", len(claimed.Commands), err)
	}
	if _, err := database.DB.Conn.Exec(ctx, `UPDATE `+pgschema.Table(database.Schema, "flow_runs")+`
		SET deadline_at=clock_timestamp()-interval '1 second' WHERE run_id=$1`, run.ID); err != nil {
		t.Fatal(err)
	}
	recorder.reset()
	expired, err := runtime.store.ExpireRun(ctx, mustStoreUUID(t, string(run.ID)), "plan14 bulk expiry")
	if err != nil || !expired {
		t.Fatalf("ExpireRun() = %t, %v", expired, err)
	}
	bulkQueries := 0
	for _, query := range recorder.snapshot() {
		if strings.Contains(query, "q.command_id=ANY($1::uuid[])") {
			bulkQueries++
		}
		if strings.Contains(query, "WHERE command_id=$1 FOR UPDATE") {
			t.Fatalf("found per-command expiry delivery query: %s", query)
		}
	}
	if bulkQueries != 1 {
		t.Fatalf("bulk delivery queries = %d, want 1", bulkQueries)
	}
}

func markPlan14RunDeadlineExpired(t *testing.T, database testpg.Database, runID RunID) {
	t.Helper()
	if _, err := database.DB.Conn.Exec(context.Background(), `UPDATE `+
		pgschema.Table(database.Schema, "flow_runs")+`
		SET deadline_at=clock_timestamp()-interval '1 second' WHERE run_id=$1`, runID); err != nil {
		t.Fatal(err)
	}
}

func assertPlan14ExpiryBulkQueries(t *testing.T, recorder *queryRecorder, want int) {
	t.Helper()
	bulkQueries := 0
	for _, query := range recorder.snapshot() {
		if strings.Contains(query, "q.command_id=ANY($1::uuid[])") {
			bulkQueries++
		}
		if strings.Contains(query, "WHERE command_id=$1 FOR UPDATE") {
			t.Fatalf("found per-command expiry delivery query: %s", query)
		}
	}
	if bulkQueries != want {
		t.Fatalf("bulk delivery queries = %d, want %d", bulkQueries, want)
	}
}

func TestPlan14RunExpiryZeroAndOneRunningCommand(t *testing.T) {
	t.Run("zero running", func(t *testing.T) {
		recorder := &queryRecorder{}
		database := testpg.OpenWithQueryTracer(t, recorder)
		ctx := context.Background()
		if err := Migrate(ctx, database.DB, WithSchema(database.Schema)); err != nil {
			t.Fatal(err)
		}
		child := DefineCommand[None, None]("plan14.expiry.zero.child", 1)
		runtime, run := stageClaimFixture(t, database, "plan14_expiry_zero", 3, func(work *Work[None]) {
			for index := range 3 {
				Enqueue(work, fmt.Sprintf("child/%d", index), child, None{})
			}
		})
		markPlan14RunDeadlineExpired(t, database, run.ID)
		recorder.reset()
		expired, err := runtime.store.ExpireRun(ctx, mustStoreUUID(t, string(run.ID)), "plan14 zero-running expiry")
		if err != nil || !expired {
			t.Fatalf("ExpireRun() = %t, %v", expired, err)
		}
		assertPlan14ExpiryBulkQueries(t, recorder, 0)
	})

	t.Run("one running", func(t *testing.T) {
		recorder := &queryRecorder{}
		database := testpg.OpenWithQueryTracer(t, recorder)
		ctx := context.Background()
		if err := Migrate(ctx, database.DB, WithSchema(database.Schema)); err != nil {
			t.Fatal(err)
		}
		child := DefineCommand[None, None]("plan14.expiry.one.child", 1)
		runtime, run := stageClaimFixture(t, database, "plan14_expiry_one", 2, func(work *Work[None]) {
			Enqueue(work, "running", child, None{})
			Enqueue(work, "ready", child, None{})
		})
		candidates := probeClaimCandidates(t, runtime,
			[]store.CommandKind{{Name: child.Name(), Version: child.Version()}}, 2)
		claimed, err := runtime.store.ClaimCommands(ctx, candidates[:1], time.Minute, "plan14-expiry-one", fault.None{})
		if err != nil || len(claimed.Commands) != 1 {
			t.Fatalf("ClaimCommands() commands=%d, err=%v", len(claimed.Commands), err)
		}
		markPlan14RunDeadlineExpired(t, database, run.ID)
		recorder.reset()
		expired, err := runtime.store.ExpireRun(ctx, mustStoreUUID(t, string(run.ID)), "plan14 one-running expiry")
		if err != nil || !expired {
			t.Fatalf("ExpireRun() = %t, %v", expired, err)
		}
		assertPlan14ExpiryBulkQueries(t, recorder, 1)
	})
}

func TestPlan14RunExpiryBulkLoadsOneHundredMixedCommands(t *testing.T) {
	recorder := &queryRecorder{}
	database := testpg.OpenWithQueryTracer(t, recorder)
	ctx := context.Background()
	if err := Migrate(ctx, database.DB, WithSchema(database.Schema)); err != nil {
		t.Fatal(err)
	}
	gate := DefineEvent[None]("plan14.expiry.mixed.gate")
	child := DefineCommand[None, None]("plan14.expiry.mixed.child", 1)
	runtime, run := stageClaimFixture(t, database, "plan14_expiry_mixed", 100, func(work *Work[None]) {
		for index := range 100 {
			node := Enqueue(work, fmt.Sprintf("child/%03d", index), child, None{})
			if index >= 50 {
				node.WaitFor(gate, fmt.Sprintf("gate/%03d", index))
			}
		}
	})
	candidates := probeClaimCandidates(t, runtime,
		[]store.CommandKind{{Name: child.Name(), Version: child.Version()}}, 50)
	claimed, err := runtime.store.ClaimCommands(ctx, candidates, time.Minute, "plan14-expiry-mixed", fault.None{})
	if err != nil || len(claimed.Commands) != 50 {
		t.Fatalf("ClaimCommands() commands=%d, err=%v", len(claimed.Commands), err)
	}
	markPlan14RunDeadlineExpired(t, database, run.ID)
	recorder.reset()
	expired, err := runtime.store.ExpireRun(ctx, mustStoreUUID(t, string(run.ID)), "plan14 mixed expiry")
	if err != nil || !expired {
		t.Fatalf("ExpireRun() = %t, %v", expired, err)
	}
	assertPlan14ExpiryBulkQueries(t, recorder, 1)
	var running, waiting int
	if err := database.DB.Conn.QueryRow(ctx, `SELECT
		count(*) FILTER (WHERE state='running'),count(*) FILTER (WHERE state='pending')
		FROM `+pgschema.Table(database.Schema, "flow_commands")+` WHERE run_id=$1`, run.ID).
		Scan(&running, &waiting); err != nil {
		t.Fatal(err)
	}
	if running != 0 || waiting != 0 {
		t.Fatalf("expired mixed projections still running=%d waiting=%d", running, waiting)
	}
}

func TestPlan14RunExpiryRejectsMissingDeliveryAndAttemptJournal(t *testing.T) {
	tests := []struct {
		name    string
		corrupt func(*testing.T, testpg.Database, store.ClaimedCommand)
	}{
		{
			name: "missing queue projection",
			corrupt: func(t *testing.T, database testpg.Database, claim store.ClaimedCommand) {
				t.Helper()
				if _, err := database.DB.Conn.Exec(context.Background(), `DELETE FROM `+
					pgschema.Table(database.Schema, "flow_command_queue")+` WHERE command_id=$1`, claim.CommandID); err != nil {
					t.Fatal(err)
				}
			},
		},
		{
			name: "missing attempt journal fence",
			corrupt: func(t *testing.T, database testpg.Database, claim store.ClaimedCommand) {
				t.Helper()
				if _, err := database.DB.Conn.Exec(context.Background(), `DELETE FROM `+
					pgschema.Table(database.Schema, "flow_journal")+`
					WHERE run_id=$1 AND attempt_id=$2 AND entry_kind='attempt_started'`, claim.RunID, claim.AttemptID); err != nil {
					t.Fatal(err)
				}
			},
		},
	}
	for _, test := range tests {
		t.Run(test.name, func(t *testing.T) {
			database := testpg.Open(t)
			ctx := context.Background()
			if err := Migrate(ctx, database.DB, WithSchema(database.Schema)); err != nil {
				t.Fatal(err)
			}
			child := DefineCommand[None, None]("plan14.expiry.corrupt.child", 1)
			runtime, run := stageClaimFixture(t, database, "plan14_expiry_corrupt", 1, func(work *Work[None]) {
				Enqueue(work, "child", child, None{})
			})
			candidates := probeClaimCandidates(t, runtime,
				[]store.CommandKind{{Name: child.Name(), Version: child.Version()}}, 1)
			claimed, err := runtime.store.ClaimCommands(ctx, candidates, time.Minute, "plan14-expiry-corrupt", fault.None{})
			if err != nil || len(claimed.Commands) != 1 {
				t.Fatalf("ClaimCommands() commands=%d, err=%v", len(claimed.Commands), err)
			}
			test.corrupt(t, database, claimed.Commands[0])
			markPlan14RunDeadlineExpired(t, database, run.ID)
			expired, err := runtime.store.ExpireRun(ctx, mustStoreUUID(t, string(run.ID)), "plan14 corrupt expiry")
			if expired || !errors.Is(err, ErrInvalidState) {
				t.Fatalf("ExpireRun() = %t, %v; want invalid-state fence", expired, err)
			}
			var status string
			if err := database.DB.Conn.QueryRow(ctx, `SELECT status FROM `+
				pgschema.Table(database.Schema, "flow_runs")+` WHERE run_id=$1`, run.ID).Scan(&status); err != nil {
				t.Fatal(err)
			}
			if status != "running" {
				t.Fatalf("rejected corrupt expiry changed run status to %s", status)
			}
		})
	}
}

func TestPlan14RunExpiryRollsBackAfterBulkRead(t *testing.T) {
	recorder := &queryRecorder{}
	database := testpg.OpenWithQueryTracer(t, recorder)
	ctx := context.Background()
	if err := Migrate(ctx, database.DB, WithSchema(database.Schema)); err != nil {
		t.Fatal(err)
	}
	child := DefineCommand[None, None]("plan14.expiry.rollback.child", 1)
	runtime, run := stageClaimFixture(t, database, "plan14_expiry_rollback", 1, func(work *Work[None]) {
		Enqueue(work, "child", child, None{})
	})
	candidates := probeClaimCandidates(t, runtime,
		[]store.CommandKind{{Name: child.Name(), Version: child.Version()}}, 1)
	claimed, err := runtime.store.ClaimCommands(ctx, candidates, time.Minute, "plan14-expiry-rollback", fault.None{})
	if err != nil || len(claimed.Commands) != 1 {
		t.Fatalf("ClaimCommands() commands=%d, err=%v", len(claimed.Commands), err)
	}
	failFunction := pgschema.Table(database.Schema, "plan14_fail_expiry_projection")
	if _, err := database.DB.Conn.Exec(ctx, `CREATE FUNCTION `+failFunction+`() RETURNS trigger
		LANGUAGE plpgsql AS $$ BEGIN
			IF NEW.state='cancelled' THEN RAISE EXCEPTION 'plan14 injected expiry projection failure'; END IF;
			RETURN NEW;
		END $$`); err != nil {
		t.Fatal(err)
	}
	if _, err := database.DB.Conn.Exec(ctx, `CREATE TRIGGER plan14_fail_expiry_projection
		BEFORE UPDATE ON `+pgschema.Table(database.Schema, "flow_commands")+`
		FOR EACH ROW EXECUTE FUNCTION `+failFunction+`() `); err != nil {
		t.Fatal(err)
	}
	markPlan14RunDeadlineExpired(t, database, run.ID)
	recorder.reset()
	expired, err := runtime.store.ExpireRun(ctx, mustStoreUUID(t, string(run.ID)), "plan14 rollback expiry")
	if err == nil || expired {
		t.Fatalf("ExpireRun() = %t, %v; want injected failure", expired, err)
	}
	assertPlan14ExpiryBulkQueries(t, recorder, 1)
	var runStatus, commandState, queueState string
	var attemptStarts, attemptConcluded int
	if err := database.DB.Conn.QueryRow(ctx, `SELECT r.status,c.state,q.state,
		count(*) FILTER (WHERE j.entry_kind='attempt_started'),
		count(*) FILTER (WHERE j.entry_kind='attempt_concluded')
		FROM `+pgschema.Table(database.Schema, "flow_runs")+` r
		JOIN `+pgschema.Table(database.Schema, "flow_commands")+` c ON c.run_id=r.run_id AND c.command_id=$2
		JOIN `+pgschema.Table(database.Schema, "flow_command_queue")+` q ON q.command_id=c.command_id
		LEFT JOIN `+pgschema.Table(database.Schema, "flow_journal")+` j ON j.run_id=r.run_id AND j.attempt_id=$3
		WHERE r.run_id=$1 GROUP BY r.status,c.state,q.state`, run.ID,
		claimed.Commands[0].CommandID, claimed.Commands[0].AttemptID).
		Scan(&runStatus, &commandState, &queueState, &attemptStarts, &attemptConcluded); err != nil {
		t.Fatal(err)
	}
	if runStatus != "running" || commandState != "running" || queueState != "running" || attemptStarts != 1 || attemptConcluded != 0 {
		t.Fatalf("rollback left run=%s command=%s queue=%s starts=%d conclusions=%d",
			runStatus, commandState, queueState, attemptStarts, attemptConcluded)
	}
}

func TestPlan14RunExpirySerializesWithConcurrentSettlement(t *testing.T) {
	database := testpg.Open(t)
	ctx := context.Background()
	if err := Migrate(ctx, database.DB, WithSchema(database.Schema)); err != nil {
		t.Fatal(err)
	}
	child := DefineCommand[None, None]("plan14.expiry.concurrent.child", 1)
	runtime, run := stageClaimFixture(t, database, "plan14_expiry_concurrent", 1, func(work *Work[None]) {
		Enqueue(work, "child", child, None{})
	})
	candidates := probeClaimCandidates(t, runtime,
		[]store.CommandKind{{Name: child.Name(), Version: child.Version()}}, 1)
	claimed, err := runtime.store.ClaimCommands(ctx, candidates, time.Minute, "plan14-expiry-concurrent", fault.None{})
	if err != nil || len(claimed.Commands) != 1 {
		t.Fatalf("ClaimCommands() commands=%d, err=%v", len(claimed.Commands), err)
	}
	encoded, err := child.def.Result.Encode(None{}, maxCommandResultBytes)
	if err != nil {
		t.Fatal(err)
	}
	runID := mustStoreUUID(t, string(run.ID))
	markPlan14RunDeadlineExpired(t, database, run.ID)
	type expiryResult struct {
		changed bool
		err     error
	}
	start := make(chan struct{})
	expiryDone := make(chan expiryResult, 1)
	settlementDone := make(chan error, 1)
	go func() {
		<-start
		changed, expireErr := runtime.store.ExpireRun(ctx, runID, "plan14 concurrent expiry")
		expiryDone <- expiryResult{changed: changed, err: expireErr}
	}()
	go func() {
		<-start
		_, settleErr := runtime.store.SettleCommandSuccess(ctx, store.CommandSuccess{
			Claim: claimed.Commands[0], Result: encoded,
		}, fault.None{})
		settlementDone <- settleErr
	}()
	close(start)
	var expiry expiryResult
	select {
	case expiry = <-expiryDone:
	case <-time.After(3 * time.Second):
		t.Fatal("ExpireRun did not finish during concurrent settlement")
	}
	var settleErr error
	select {
	case settleErr = <-settlementDone:
	case <-time.After(3 * time.Second):
		t.Fatal("SettleCommandSuccess did not finish during concurrent expiry")
	}
	if expiry.err != nil {
		t.Fatalf("ExpireRun() changed=%t, err=%v", expiry.changed, expiry.err)
	}
	if settleErr != nil && !errors.Is(settleErr, ErrTerminal) {
		t.Fatalf("SettleCommandSuccess() err=%v", settleErr)
	}
	var status string
	var attemptConcluded, cancelled int
	if err := database.DB.Conn.QueryRow(ctx, `SELECT r.status,
		count(*) FILTER (WHERE j.entry_kind='attempt_concluded' AND j.attempt_id=$2),
		count(*) FILTER (WHERE j.event_class='command_terminal' AND j.command_id=$3 AND j.terminal_status='cancelled')
		FROM `+pgschema.Table(database.Schema, "flow_runs")+` r
		LEFT JOIN `+pgschema.Table(database.Schema, "flow_journal")+` j ON j.run_id=r.run_id
		WHERE r.run_id=$1 GROUP BY r.status`, run.ID, claimed.Commands[0].AttemptID, claimed.Commands[0].CommandID).
		Scan(&status, &attemptConcluded, &cancelled); err != nil {
		t.Fatal(err)
	}
	if status != "expired" || attemptConcluded != 1 || cancelled != 1 {
		t.Fatalf("concurrent expiry status=%s conclusions=%d cancellations=%d",
			status, attemptConcluded, cancelled)
	}
}
