package flow

import (
	"context"
	"fmt"
	"os"
	"strings"
	"sync"
	"testing"
	"time"

	"github.com/google/uuid"
	"github.com/goware/flow/internal/canonical"
	"github.com/goware/flow/internal/pgschema"
	"github.com/goware/flow/internal/store"
	"github.com/goware/flow/internal/testpg"
)

const (
	benchmarkPollInterval        = 5 * time.Millisecond
	benchmarkLifecycleBatchSize  = 64
	benchmarkExternalRunDeadline = 30 * time.Minute
	benchmarkExternalHoldKey     = "benchmark/hold"
)

func BenchmarkRunIngressNotification(b *testing.B) {
	for _, notifications := range []bool{false, true} {
		name := map[bool]string{false: "poll_only", true: "notify"}[notifications]
		b.Run(name, func(b *testing.B) {
			database := testpg.Open(b)
			ctx := context.Background()
			if err := Migrate(ctx, database.DB, WithSchema(database.Schema)); err != nil {
				b.Fatal(err)
			}
			runtime, err := New(database.DB, WithSchema(database.Schema), WithNotifications(notifications))
			if err != nil {
				b.Fatal(err)
			}
			command := DefineCommand[None, None]("benchmark.notification", 1)
			b.ResetTimer()
			for index := range b.N {
				if _, err := command.Enqueue(ctx, runtime, fmt.Sprintf("ingress/%d", index), None{}, WithoutRunDeadline()); err != nil {
					b.Fatal(err)
				}
			}
		})
	}
}

// BenchmarkIndependentCommandLifecycle measures complete no-op command
// lifecycles. Migration, registration, and runtime startup are excluded; each
// timed iteration starts and awaits a fresh batch so scheduler polling does not
// dominate a single command.
func BenchmarkIndependentCommandLifecycle(b *testing.B) {
	for _, producers := range []int{1, 4, 16} {
		b.Run(fmt.Sprintf("producers_%d", producers), func(b *testing.B) {
			database := testpg.Open(b)
			ctx := context.Background()
			if err := Migrate(ctx, database.DB, WithSchema(database.Schema)); err != nil {
				b.Fatal(err)
			}
			command := DefineCommand[None, None]("benchmark.lifecycle", 1)
			runtime, err := New(database.DB, WithSchema(database.Schema), WithNotifications(false),
				WithWorkerConcurrency(16), WithPollInterval(benchmarkPollInterval))
			if err != nil {
				b.Fatal(err)
			}
			if err := runtime.Register(Handle(command, func(context.Context, *Work[None]) (None, error) {
				return None{}, nil
			})); err != nil {
				b.Fatal(err)
			}
			stop := startBenchmarkRuntime(b, runtime)
			defer func() {
				b.StopTimer()
				stop()
			}()

			b.ReportAllocs()
			b.ResetTimer()
			for iteration := range b.N {
				runs := make([]EnqueueResult, benchmarkLifecycleBatchSize)
				err := runBenchmarkProducers(producers, benchmarkLifecycleBatchSize, func(index int) error {
					var executeErr error
					runs[index], executeErr = command.Enqueue(ctx, runtime,
						fmt.Sprintf("lifecycle/%d/%d", iteration, index), None{}, WithoutRunDeadline())
					return executeErr
				})
				if err != nil {
					b.Fatal(err)
				}
				waitCtx, cancel := context.WithTimeout(ctx, 30*time.Second)
				for _, run := range runs {
					settled, awaitErr := AwaitRun(waitCtx, runtime, run.RunID)
					if awaitErr != nil || settled.Status != RunStatusSucceeded || settled.CommandCount != 1 {
						cancel()
						b.Fatalf("AwaitRun() = status %q commands %d, err %v",
							settled.Status, settled.CommandCount, awaitErr)
					}
				}
				cancel()
			}
			b.StopTimer()
			reportBenchmarkRate(b, float64(b.N*benchmarkLifecycleBatchSize), "commands")
		})
	}
}

// BenchmarkSameRunFanout measures a complete fan-out run. The
// size is the total command count, including the root command. Notifications
// are disabled so the workload uses deterministic local polling.
func BenchmarkSameRunFanout(b *testing.B) {
	for _, commandCount := range []int{10, 100} {
		b.Run(fmt.Sprintf("commands_%d", commandCount), func(b *testing.B) {
			benchmarkSameRunFanout(b, commandCount)
		})
	}
}

// BenchmarkSameRunFanoutStress1000 is deliberately opt-in and one-shot.
// Run it with FLOW_BENCHMARK_STRESS=1 and -benchtime=1x.
func BenchmarkSameRunFanoutStress1000(b *testing.B) {
	if os.Getenv("FLOW_BENCHMARK_STRESS") != "1" {
		b.Skip("set FLOW_BENCHMARK_STRESS=1 and use -benchtime=1x for the 1,000-command stress workload")
	}
	if b.N != 1 {
		b.Fatalf("1,000-command stress workload requires -benchtime=1x (b.N=%d)", b.N)
	}
	benchmarkSameRunFanout(b, 1000)
}

func benchmarkSameRunFanout(b *testing.B, commandCount int) {
	b.Helper()
	database := testpg.Open(b)
	ctx := context.Background()
	if err := Migrate(ctx, database.DB, WithSchema(database.Schema)); err != nil {
		b.Fatal(err)
	}
	child := DefineCommand[None, None]("benchmark.fanout.child", 1)
	root := DefineCommand[None, None]("benchmark.fanout.root", 1)
	runtime, err := New(database.DB, WithSchema(database.Schema), WithNotifications(false),
		WithMaxCommandsPerRun(0), WithWorkerConcurrency(16), WithPollInterval(benchmarkPollInterval))
	if err != nil {
		b.Fatal(err)
	}
	if err := runtime.Register(
		Handle(root, func(_ context.Context, work *Work[None]) (None, error) {
			for index := 1; index < commandCount; index++ {
				Enqueue(work, fmt.Sprintf("work/%04d", index), child, None{})
			}
			return None{}, nil
		}),
		Handle(child, func(context.Context, *Work[None]) (None, error) { return None{}, nil }),
	); err != nil {
		b.Fatal(err)
	}
	stop := startBenchmarkRuntime(b, runtime)
	defer func() {
		b.StopTimer()
		stop()
	}()

	b.ReportAllocs()
	b.ResetTimer()
	for iteration := range b.N {
		run, executeErr := root.Enqueue(ctx, runtime,
			fmt.Sprintf("fanout/%d/%d", commandCount, iteration), None{}, WithoutRunDeadline())
		if executeErr != nil {
			b.Fatal(executeErr)
		}
		waitCtx, cancel := context.WithTimeout(ctx, 2*time.Minute)
		settled, awaitErr := AwaitRun(waitCtx, runtime, run.RunID)
		cancel()
		if awaitErr != nil || settled.Status != RunStatusSucceeded || settled.CommandCount != commandCount {
			b.Fatalf("AwaitRun() = status %q commands %d, err %v",
				settled.Status, settled.CommandCount, awaitErr)
		}
	}
	b.StopTimer()
	b.ReportMetric(float64(b.Elapsed())/float64(time.Millisecond)/float64(b.N), "completion_ms/op")
	reportBenchmarkRate(b, float64(b.N*commandCount), "commands")
}

// BenchmarkSameRunClaimBatch measures one ordinary multi-command claim
// transaction for ready siblings in the same run. Fixture creation,
// candidate probing, and projection reset are excluded from the timed region.
func BenchmarkSameRunClaimBatch(b *testing.B) {
	const commandCount = 16

	database := testpg.Open(b)
	ctx := context.Background()
	if err := Migrate(ctx, database.DB, WithSchema(database.Schema)); err != nil {
		b.Fatal(err)
	}
	parent := DefineCommand[None, None]("benchmark.claim_batch.parent", 1)
	child := DefineCommand[None, None]("benchmark.claim_batch.child", 1)
	runtime, err := New(database.DB, WithSchema(database.Schema), WithNotifications(false),
		WithMaxCommandsPerRun(0), WithWorkerConcurrency(1), WithPollInterval(benchmarkPollInterval))
	if err != nil {
		b.Fatal(err)
	}
	if err := runtime.Register(Handle(parent, func(_ context.Context, work *Work[None]) (None, error) {
		for index := range commandCount {
			Enqueue(work, fmt.Sprintf("child/%02d", index), child, None{})
		}
		return None{}, nil
	})); err != nil {
		b.Fatal(err)
	}
	stop := startBenchmarkRuntime(b, runtime)
	run, err := parent.Enqueue(ctx, runtime, "claim-batch", None{}, WithoutRunDeadline())
	if err != nil {
		stop()
		b.Fatal(err)
	}
	deadline := time.Now().Add(5 * time.Second)
	for {
		var persistedCommands int
		var parentState string
		if err := database.DB.Conn.QueryRow(ctx, `SELECT e.command_count,c.state
			FROM `+pgschema.Table(database.Schema, "flow_runs")+` e
			JOIN `+pgschema.Table(database.Schema, "flow_commands")+` c
			  ON c.run_id=e.run_id AND c.command_id=e.root_command_id
			WHERE e.run_id=$1`, run.RunID).Scan(&persistedCommands, &parentState); err != nil {
			stop()
			b.Fatal(err)
		}
		if persistedCommands == commandCount+1 && parentState == "succeeded" {
			break
		}
		if time.Now().After(deadline) {
			stop()
			b.Fatalf("claim fixture commands=%d parent=%s", persistedCommands, parentState)
		}
		time.Sleep(benchmarkPollInterval)
	}
	stop()
	candidates, err := runtime.store.ProbeCommands(ctx,
		[]store.CommandKind{{Name: child.Name(), Version: child.Version()}}, commandCount)
	if err != nil || len(candidates) != commandCount {
		b.Fatalf("ProbeCommands() candidates=%d, err=%v", len(candidates), err)
	}

	b.ReportAllocs()
	b.ResetTimer()
	b.StopTimer()
	for range b.N {
		b.StartTimer()
		result, claimErr := runtime.store.ClaimCommands(ctx, candidates, time.Minute, "benchmark", nil)
		b.StopTimer()
		if claimErr != nil || !result.Progressed || len(result.Commands) != commandCount {
			b.Fatalf("ClaimCommands() progressed=%t commands=%d, err=%v",
				result.Progressed, len(result.Commands), claimErr)
		}
		resetBenchmarkClaimBatch(b, ctx, runtime, result.Commands)
	}
	b.ReportMetric(commandCount, "commands/op")
	reportBenchmarkRate(b, float64(b.N*commandCount), "commands")
}

func resetBenchmarkClaimBatch(b *testing.B, ctx context.Context, runtime *Runtime, claims []store.ClaimedCommand) {
	b.Helper()
	commandIDs := make([]uuid.UUID, len(claims))
	attemptIDs := make([]uuid.UUID, len(claims))
	firstPosition := claims[0].AttemptStartedPosition
	runID := claims[0].RunID
	for index, claim := range claims {
		if claim.RunID != runID || claim.AttemptStartedPosition != firstPosition+int64(index) {
			b.Fatalf("claim[%d] run=%s position=%d, want %s/%d", index,
				claim.RunID, claim.AttemptStartedPosition, runID, firstPosition+int64(index))
		}
		commandIDs[index], attemptIDs[index] = claim.CommandID, claim.AttemptID
	}
	tx, err := runtime.db.Conn.Begin(ctx)
	if err != nil {
		b.Fatal(err)
	}
	defer func() { _ = tx.Rollback(context.WithoutCancel(ctx)) }()
	removed, err := tx.Exec(ctx, `DELETE FROM `+pgschema.Table(runtime.schema, "flow_journal")+`
		WHERE run_id=$1 AND attempt_id=ANY($2::uuid[]) AND entry_kind='attempt_started'`,
		runID, attemptIDs)
	if err != nil || removed.RowsAffected() != int64(len(claims)) {
		b.Fatalf("reset claim journal rows=%d, err=%v", removed.RowsAffected(), err)
	}
	resetQueue, err := tx.Exec(ctx, `WITH reset(command_id,attempt_id) AS (
		SELECT * FROM unnest($1::uuid[],$2::uuid[])
	)
	UPDATE `+pgschema.Table(runtime.schema, "flow_command_queue")+` q
	SET state='ready',active_attempt_id=NULL,lease_token=NULL,lease_owner=NULL,
	    lease_started_at=NULL,lease_expires_at=NULL
	FROM reset
	WHERE q.command_id=reset.command_id AND q.run_id=$3 AND q.active_attempt_id=reset.attempt_id`,
		commandIDs, attemptIDs, runID)
	if err != nil || resetQueue.RowsAffected() != int64(len(claims)) {
		b.Fatalf("reset claim queue rows=%d, err=%v", resetQueue.RowsAffected(), err)
	}
	resetCommand, err := tx.Exec(ctx, `UPDATE `+pgschema.Table(runtime.schema, "flow_commands")+`
		SET state='ready',attempt_ordinal=attempt_ordinal-1
		WHERE run_id=$1 AND command_id=ANY($2::uuid[]) AND state='running' AND attempt_ordinal>0`,
		runID, commandIDs)
	if err != nil || resetCommand.RowsAffected() != int64(len(claims)) {
		b.Fatalf("reset claim command rows=%d, err=%v", resetCommand.RowsAffected(), err)
	}
	resetRun, err := tx.Exec(ctx, `UPDATE `+pgschema.Table(runtime.schema, "flow_runs")+`
		SET next_journal_position=$2
		WHERE run_id=$1 AND next_journal_position=$2::bigint+$3::bigint`, runID, firstPosition, len(claims))
	if err != nil || resetRun.RowsAffected() != 1 {
		b.Fatalf("reset claim run rows=%d, err=%v", resetRun.RowsAffected(), err)
	}
	if err := tx.Commit(ctx); err != nil {
		b.Fatal(err)
	}
}

// BenchmarkStagedDecisionBatch isolates the successful settlement transaction
// from run ingress, claiming, handler work, and later child run.
// Child shapes rotate through no waits, one wait, and three waits.
func BenchmarkStagedDecisionBatch(b *testing.B) {
	for _, childCount := range []int{1, 10, 100} {
		for _, eventCount := range []int{0, 10, 100} {
			name := fmt.Sprintf("children_%d/events_%d", childCount, eventCount)
			b.Run(name, func(b *testing.B) {
				database := testpg.Open(b)
				ctx := context.Background()
				if err := Migrate(ctx, database.DB, WithSchema(database.Schema)); err != nil {
					b.Fatal(err)
				}
				runtime, err := New(database.DB, WithSchema(database.Schema), WithNotifications(false),
					WithMaxCommandsPerRun(0))
				if err != nil {
					b.Fatal(err)
				}
				root := DefineCommand[None, None]("benchmark.decision.root", 1)
				child := DefineCommand[None, None]("benchmark.decision.child", 1)
				event := DefineEvent[None]("benchmark.decision.event")
				result, err := root.def.Result.Encode(None{}, maxCommandResultBytes)
				if err != nil {
					b.Fatal(err)
				}

				b.ReportAllocs()
				b.ResetTimer()
				for iteration := range b.N {
					b.StopTimer()
					claim := claimBenchmarkRoot(b, ctx, runtime, root,
						fmt.Sprintf("decision/%d/%d/%d", childCount, eventCount, iteration))
					events, children := prepareBenchmarkDecision(b, claim, child, event, childCount, eventCount)
					b.StartTimer()
					if _, settleErr := runtime.store.SettleCommandSuccess(ctx, store.CommandSuccess{
						Claim: claim, Result: result, Events: events, Children: children,
					}, nil); settleErr != nil {
						b.Fatal(settleErr)
					}
				}
				b.StopTimer()
				reportBenchmarkRate(b, float64(b.N), "settlements")
			})
		}
	}
}

func BenchmarkExternalEventIngress(b *testing.B) {
	b.Run("distinct_live/no_match", func(b *testing.B) {
		runtime, root, event := setupExternalEventBenchmark(b)
		targets := make([]benchmarkEventTarget, b.N)
		for index := range b.N {
			run, err := root.Enqueue(context.Background(), runtime,
				fmt.Sprintf("external/distinct/%d", index), None{},
				WithRunDeadline(benchmarkExternalRunDeadline), WaitFor(event, benchmarkExternalHoldKey))
			if err != nil {
				b.Fatal(err)
			}
			targets[index] = benchmarkEventTarget{runID: run.RunID, key: fmt.Sprintf("event/%d", index)}
		}
		benchmarkEventTargets(b, runtime, event, targets)
	})

	b.Run("hot_live/no_match", func(b *testing.B) {
		runtime, root, event := setupExternalEventBenchmark(b)
		run, err := root.Enqueue(context.Background(), runtime, "external/hot", None{},
			WithRunDeadline(benchmarkExternalRunDeadline), WaitFor(event, benchmarkExternalHoldKey))
		if err != nil {
			b.Fatal(err)
		}
		targets := make([]benchmarkEventTarget, b.N)
		for index := range b.N {
			targets[index] = benchmarkEventTarget{runID: run.RunID, key: fmt.Sprintf("event/%d", index)}
		}
		benchmarkEventTargets(b, runtime, event, targets)
	})

	b.Run("retained_100/no_match", func(b *testing.B) {
		runtime, root, event := setupExternalEventBenchmark(b)
		child := DefineCommand[None, None]("benchmark.external.child", 1)
		runID, _ := createRetainedWaitRun(b, runtime, root, child, event,
			"external/retained/no-match", func(index int) string { return fmt.Sprintf("wait/%03d", index) })
		targets := make([]benchmarkEventTarget, b.N)
		for index := range b.N {
			targets[index] = benchmarkEventTarget{runID: runID, key: fmt.Sprintf("unmatched/%d", index)}
		}
		benchmarkEventTargets(b, runtime, event, targets)
	})

	b.Run("retained_100/match_one", func(b *testing.B) {
		runtime, root, event := setupExternalEventBenchmark(b)
		child := DefineCommand[None, None]("benchmark.external.child", 1)
		targets := make([]benchmarkEventTarget, 0, b.N*99)
		for iteration := range b.N {
			runID, keys := createRetainedWaitRun(b, runtime, root, child, event,
				fmt.Sprintf("external/retained/match-one/%d", iteration),
				func(index int) string { return fmt.Sprintf("wait/%03d", index) })
			for _, key := range keys {
				targets = append(targets, benchmarkEventTarget{runID: runID, key: key})
			}
		}
		benchmarkEventTargets(b, runtime, event, targets)
	})

	b.Run("retained_100/match_several", func(b *testing.B) {
		runtime, root, event := setupExternalEventBenchmark(b)
		child := DefineCommand[None, None]("benchmark.external.child", 1)
		targets := make([]benchmarkEventTarget, 0, b.N*11)
		for iteration := range b.N {
			runID, keys := createRetainedWaitRun(b, runtime, root, child, event,
				fmt.Sprintf("external/retained/match-several/%d", iteration),
				func(index int) string { return fmt.Sprintf("group/%02d", index/9) })
			for _, key := range keys {
				targets = append(targets, benchmarkEventTarget{runID: runID, key: key})
			}
		}
		benchmarkEventTargets(b, runtime, event, targets)
	})
}

type benchmarkEventTarget struct {
	runID RunID
	key   string
}

func setupExternalEventBenchmark(b *testing.B) (*Runtime, Command[None, None], Event[None]) {
	b.Helper()
	database := testpg.Open(b)
	ctx := context.Background()
	if err := Migrate(ctx, database.DB, WithSchema(database.Schema)); err != nil {
		b.Fatal(err)
	}
	runtime, err := New(database.DB, WithSchema(database.Schema), WithNotifications(true),
		WithMaxCommandsPerRun(0))
	if err != nil {
		b.Fatal(err)
	}
	return runtime,
		DefineCommand[None, None]("benchmark.external.root", 1),
		DefineEvent[None]("benchmark.external.event")
}

func benchmarkEventTargets(b *testing.B, runtime *Runtime, event Event[None], targets []benchmarkEventTarget) {
	b.Helper()
	ctx := context.Background()
	b.ReportAllocs()
	b.ResetTimer()
	for _, target := range targets {
		if err := event.Deliver(ctx, runtime, target.runID, target.key, None{}); err != nil {
			b.Fatal(err)
		}
	}
	b.StopTimer()
	assertBenchmarkTargetsHeld(b, ctx, runtime, event, targets)
	reportBenchmarkRate(b, float64(len(targets)), "events")
	b.ReportMetric(float64(b.Elapsed())/float64(len(targets)), "ns/event")
}

func assertBenchmarkTargetsHeld(
	b *testing.B,
	ctx context.Context,
	runtime *Runtime,
	event Event[None],
	targets []benchmarkEventTarget,
) {
	b.Helper()
	seen := make(map[RunID]struct{}, len(targets))
	for _, target := range targets {
		if _, checked := seen[target.runID]; checked {
			continue
		}
		seen[target.runID] = struct{}{}
		run, err := GetRun(ctx, runtime, target.runID)
		if err != nil || run.Status != RunStatusRunning || run.DeadlineAt == nil {
			b.Fatalf("external target %s status=%q deadline=%v, err=%v",
				target.runID, run.Status, run.DeadlineAt, err)
		}
		var unresolvedHold int
		if err := runtime.db.Conn.QueryRow(ctx, `SELECT count(*) FROM `+
			pgschema.Table(runtime.schema, "flow_command_event_waits")+`
			WHERE run_id=$1 AND event_name=$2 AND event_key=$3 AND satisfied_position IS NULL`,
			target.runID, event.Name(), benchmarkExternalHoldKey).Scan(&unresolvedHold); err != nil {
			b.Fatal(err)
		}
		if unresolvedHold != 1 {
			b.Fatalf("external target %s unresolved hold waits=%d, want 1", target.runID, unresolvedHold)
		}
	}
}

// createRetainedWaitRun settles one root with 99 waiting children. The
// resulting finite run contains exactly 100 retained commands. One
// child has an additional hold wait that is never included in the returned
// event keys, so every measured shape remains deliberately live.
func createRetainedWaitRun(
	b *testing.B,
	runtime *Runtime,
	root Command[None, None],
	child Command[None, None],
	event Event[None],
	runKey string,
	waitKey func(int) string,
) (RunID, []string) {
	b.Helper()
	ctx := context.Background()
	claim := claimBenchmarkRoot(b, ctx, runtime, root, runKey,
		WithRunDeadline(benchmarkExternalRunDeadline))
	scope := &workScope{args: None{}}
	work := &Work[None]{Args: None{}, scope: &scope.state}
	keys := make([]string, 0, 99)
	seen := make(map[string]struct{}, 99)
	for index := range 99 {
		key := waitKey(index)
		node := Enqueue(work, fmt.Sprintf("waiting/%03d", index), child, None{}).WaitFor(event, key)
		if index == 0 {
			node.WaitFor(event, benchmarkExternalHoldKey)
		}
		if _, exists := seen[key]; !exists {
			seen[key] = struct{}{}
			keys = append(keys, key)
		}
	}
	events, children, err := prepareWorkerDecision(scope, claim)
	if err != nil {
		b.Fatal(err)
	}
	result, err := root.def.Result.Encode(None{}, maxCommandResultBytes)
	if err != nil {
		b.Fatal(err)
	}
	if _, err := runtime.store.SettleCommandSuccess(ctx, store.CommandSuccess{
		Claim: claim, Result: result, Events: events, Children: children,
	}, nil); err != nil {
		b.Fatal(err)
	}
	var commandCount, waitCount int
	if err := runtime.db.Conn.QueryRow(ctx, `SELECT
		(SELECT count(*) FROM `+pgschema.Table(runtime.schema, "flow_commands")+` WHERE run_id=$1),
		(SELECT count(*) FROM `+pgschema.Table(runtime.schema, "flow_command_event_waits")+` WHERE run_id=$1)`,
		claim.RunID).Scan(&commandCount, &waitCount); err != nil {
		b.Fatal(err)
	}
	if commandCount != 100 || waitCount != 100 {
		b.Fatalf("retained fixture has commands=%d waits=%d, want 100 and 100", commandCount, waitCount)
	}
	return RunID(claim.RunID.String()), keys
}

func claimBenchmarkRoot(
	b *testing.B,
	ctx context.Context,
	runtime *Runtime,
	root Command[None, None],
	runKey string,
	opts ...RunOption,
) store.ClaimedCommand {
	b.Helper()
	if len(opts) == 0 {
		opts = []RunOption{WithoutRunDeadline()}
	}
	run, err := root.Enqueue(ctx, runtime, runKey, None{}, opts...)
	if err != nil {
		b.Fatal(err)
	}
	runSnapshot := mustGetRun(b, runtime, run.RunID)
	commandID, err := uuid.Parse(string(runSnapshot.RootCommandID))
	if err != nil {
		b.Fatal(err)
	}
	runID, err := uuid.Parse(string(run.RunID))
	if err != nil {
		b.Fatal(err)
	}
	claimed, err := runtime.store.ClaimCommand(ctx, store.CommandCandidate{
		CommandID: commandID, RunID: runID, Queue: defaultQueue,
		Name: root.Name(), Version: root.Version(),
	}, time.Minute, "benchmark", nil)
	if err != nil || claimed.Command == nil {
		b.Fatalf("ClaimCommand() command=%v, err=%v", claimed.Command != nil, err)
	}
	return *claimed.Command
}

func prepareBenchmarkDecision(
	b *testing.B,
	claim store.ClaimedCommand,
	child Command[None, None],
	event Event[None],
	childCount int,
	eventCount int,
) ([]store.ApplicationEvent, []store.CommandCreate) {
	b.Helper()
	scope := &workScope{args: None{}}
	work := &Work[None]{Args: None{}, scope: &scope.state}
	for index := range eventCount {
		if err := Emit(work, event, fmt.Sprintf("event/%03d", index), None{}); err != nil {
			b.Fatal(err)
		}
	}
	for index := range childCount {
		node := Enqueue(work, fmt.Sprintf("child/%03d", index), child, None{})
		waitCount := index % 3
		if waitCount == 2 {
			waitCount = 3
		}
		for wait := range waitCount {
			key := fmt.Sprintf("missing/%03d/%d", index, wait)
			if eventCount > 0 {
				key = fmt.Sprintf("event/%03d", (index+wait)%eventCount)
			}
			node.WaitFor(event, key)
		}
	}
	events, children, err := prepareWorkerDecision(scope, claim)
	if err != nil {
		b.Fatal(err)
	}
	return events, children
}

func runBenchmarkProducers(concurrency int, count int, produce func(int) error) error {
	jobs := make(chan int)
	var group sync.WaitGroup
	var firstErr error
	var firstErrOnce sync.Once
	group.Add(concurrency)
	for range concurrency {
		go func() {
			defer group.Done()
			for index := range jobs {
				if err := produce(index); err != nil {
					firstErrOnce.Do(func() { firstErr = err })
				}
			}
		}()
	}
	for index := range count {
		jobs <- index
	}
	close(jobs)
	group.Wait()
	return firstErr
}

func startBenchmarkRuntime(b *testing.B, runtime *Runtime) func() {
	b.Helper()
	ctx, cancel := context.WithCancel(context.Background())
	result := make(chan error, 1)
	go func() { result <- runtime.Run(ctx) }()
	deadline := time.Now().Add(2 * time.Second)
	for time.Now().Before(deadline) {
		runtime.mu.RLock()
		running := runtime.lifecycle == runtimeRunning
		runtime.mu.RUnlock()
		if running {
			return func() {
				cancel()
				select {
				case err := <-result:
					if err != nil {
						b.Fatal(err)
					}
				case <-time.After(5 * time.Second):
					b.Fatal("benchmark runtime did not stop")
				}
			}
		}
		time.Sleep(time.Millisecond)
	}
	cancel()
	b.Fatal("benchmark runtime did not enter running state")
	return func() {}
}

func reportBenchmarkRate(b *testing.B, completed float64, noun string) {
	b.Helper()
	if elapsed := b.Elapsed().Seconds(); elapsed > 0 {
		b.ReportMetric(completed/elapsed, noun+"/s")
	}
}

// BenchmarkGetEventValueLookup256 measures the worker-time O(1) lookup at the
// maximum declared-input count. No database work occurs in the benchmark.
func BenchmarkGetEventValueLookup256(b *testing.B) {
	event := DefineEvent[int]("benchmark.read_event")
	inputs := make(map[string]eventInputSnapshot, maxCommandEventWaits)
	for index := range maxCommandEventWaits {
		payload, err := canonical.Marshal(index, maxApplicationEventBytes)
		if err != nil {
			b.Fatal(err)
		}
		key := fmt.Sprintf("input/%03d", index)
		inputs[event.Name()+"\x00"+key] = eventInputSnapshot{position: int64(index + 1), payload: payload.BytesCopy()}
	}
	work := &Work[None]{scope: &scopeState{eventInputs: inputs}}
	b.ReportAllocs()
	b.ResetTimer()
	for range b.N {
		value, found, err := GetEventValue(work, event, "input/255")
		if err != nil || !found || value != 255 {
			b.Fatalf("GetEventValue() = %d, %v, %v", value, found, err)
		}
	}
}

// BenchmarkEventSnapshotMaterialization covers ordinary event-input shapes.
// The separately named 256-input maximum-payload benchmark remains the
// adversarial allocation guard.
func BenchmarkEventSnapshotMaterialization(b *testing.B) {
	for _, shape := range []struct {
		name         string
		inputCount   int
		payloadBytes int
	}{
		{name: "inputs_1/payload_1KiB", inputCount: 1, payloadBytes: 1 << 10},
		{name: "inputs_32/payload_1KiB", inputCount: 32, payloadBytes: 1 << 10},
		{name: "inputs_256/payload_1KiB", inputCount: 256, payloadBytes: 1 << 10},
	} {
		b.Run(shape.name, func(b *testing.B) {
			benchmarkEventSnapshotMaterialization(b, shape.inputCount, shape.payloadBytes)
		})
	}
}

// BenchmarkEventSnapshotMaterialization256 measures a command claim that
// batches 256 maximum-size event payloads into one immutable worker input
// snapshot. Migration, run ingress, event ingress, and candidate setup
// are excluded from the timed region.
func BenchmarkEventSnapshotMaterialization256(b *testing.B) {
	benchmarkEventSnapshotMaterialization(b, maxCommandEventWaits, maxApplicationEventBytes)
}

func benchmarkEventSnapshotMaterialization(b *testing.B, inputCount int, payloadBytes int) {
	b.Helper()
	database := testpg.Open(b)
	ctx := context.Background()
	if err := Migrate(ctx, database.DB, WithSchema(database.Schema)); err != nil {
		b.Fatal(err)
	}
	runtime, err := New(database.DB, WithSchema(database.Schema), WithNotifications(false))
	if err != nil {
		b.Fatal(err)
	}
	event := DefineEvent[string]("benchmark.snapshot_event")
	command := DefineCommand[None, None]("benchmark.snapshot_command", 1)
	payload := strings.Repeat("x", payloadBytes-2)
	opts := make([]RunOption, 0, inputCount+1)
	opts = append(opts, WithoutRunDeadline())
	for wait := range inputCount {
		opts = append(opts, WaitFor(event, fmt.Sprintf("input/%03d", wait)))
	}
	exec, err := command.Enqueue(ctx, runtime,
		fmt.Sprintf("snapshot/%d/%d", inputCount, payloadBytes), None{}, opts...)
	if err != nil {
		b.Fatal(err)
	}
	execRun := mustGetRun(b, runtime, exec.RunID)
	for wait := range inputCount {
		if err := event.Deliver(ctx, runtime, exec.RunID, fmt.Sprintf("input/%03d", wait), payload); err != nil {
			b.Fatal(err)
		}
	}
	commandID, err := uuid.Parse(string(execRun.RootCommandID))
	if err != nil {
		b.Fatal(err)
	}
	runID, err := uuid.Parse(string(exec.RunID))
	if err != nil {
		b.Fatal(err)
	}
	candidate := store.CommandCandidate{CommandID: commandID, RunID: runID,
		Queue: defaultQueue, Name: command.Name(), Version: command.Version()}

	b.ReportAllocs()
	b.SetBytes(int64(inputCount * payloadBytes))
	b.ResetTimer()
	b.StopTimer()
	for range b.N {
		b.StartTimer()
		result, err := runtime.store.ClaimCommand(ctx, candidate, time.Minute, "benchmark", nil)
		b.StopTimer()
		if err != nil || result.Command == nil || len(result.Command.EventInputs) != inputCount {
			count := 0
			if result.Command != nil {
				count = len(result.Command.EventInputs)
			}
			b.Fatalf("ClaimCommand() inputs=%d, err=%v", count, err)
		}
		resetBenchmarkClaim(b, ctx, runtime, *result.Command)
	}
	b.ReportMetric(float64(inputCount), "inputs/op")
}

// resetBenchmarkClaim restores the isolated fixture to its pre-claim shape.
// It runs with the timer stopped and removes only the attempt-started row and
// projection fields written by the immediately preceding claim. Reusing the
// immutable event fixture keeps excluded setup from dominating wall time.
func resetBenchmarkClaim(b *testing.B, ctx context.Context, runtime *Runtime, claim store.ClaimedCommand) {
	b.Helper()
	tx, err := runtime.db.Conn.Begin(ctx)
	if err != nil {
		b.Fatal(err)
	}
	defer func() { _ = tx.Rollback(context.WithoutCancel(ctx)) }()
	removed, err := tx.Exec(ctx, `DELETE FROM `+pgschema.Table(runtime.schema, "flow_journal")+`
		WHERE run_id=$1 AND position=$2 AND attempt_id=$3 AND entry_kind='attempt_started'`,
		claim.RunID, claim.AttemptStartedPosition, claim.AttemptID)
	if err != nil || removed.RowsAffected() != 1 {
		b.Fatalf("reset claim journal rows=%d, err=%v", removed.RowsAffected(), err)
	}
	resetQueue, err := tx.Exec(ctx, `UPDATE `+pgschema.Table(runtime.schema, "flow_command_queue")+`
		SET state='ready',active_attempt_id=NULL,lease_token=NULL,lease_owner=NULL,
		    lease_started_at=NULL,lease_expires_at=NULL
		WHERE command_id=$1 AND run_id=$2 AND active_attempt_id=$3`,
		claim.CommandID, claim.RunID, claim.AttemptID)
	if err != nil || resetQueue.RowsAffected() != 1 {
		b.Fatalf("reset claim queue rows=%d, err=%v", resetQueue.RowsAffected(), err)
	}
	resetCommand, err := tx.Exec(ctx, `UPDATE `+pgschema.Table(runtime.schema, "flow_commands")+`
		SET state='ready',attempt_ordinal=attempt_ordinal-1
		WHERE command_id=$1 AND run_id=$2 AND state='running' AND attempt_ordinal>0`,
		claim.CommandID, claim.RunID)
	if err != nil || resetCommand.RowsAffected() != 1 {
		b.Fatalf("reset claim command rows=%d, err=%v", resetCommand.RowsAffected(), err)
	}
	resetRun, err := tx.Exec(ctx, `UPDATE `+pgschema.Table(runtime.schema, "flow_runs")+`
		SET next_journal_position=$2
		WHERE run_id=$1 AND next_journal_position=$2+1`,
		claim.RunID, claim.AttemptStartedPosition)
	if err != nil || resetRun.RowsAffected() != 1 {
		b.Fatalf("reset claim run rows=%d, err=%v", resetRun.RowsAffected(), err)
	}
	if err := tx.Commit(ctx); err != nil {
		b.Fatal(err)
	}
}

func BenchmarkInspection100Commands(b *testing.B) {
	for _, operation := range []string{"history", "trace"} {
		b.Run(operation, func(b *testing.B) {
			database := testpg.Open(b)
			ctx := context.Background()
			if err := Migrate(ctx, database.DB, WithSchema(database.Schema)); err != nil {
				b.Fatal(err)
			}
			runtime, exec, stop := startHundredCommandRun(b, database, ctx, "inspection")
			defer stop()
			b.ResetTimer()
			for range b.N {
				switch operation {
				case "history":
					if _, err := History(ctx, runtime, exec.ID, HistoryLimit(1_000)); err != nil {
						b.Fatal(err)
					}
				case "trace":
					if _, err := Trace(ctx, runtime, exec.ID); err != nil {
						b.Fatal(err)
					}
				}
			}
		})
	}
}

func TestJournalGrowthMeasurement100Commands(t *testing.T) {
	if testing.Short() {
		t.Skip("journal storage measurement uses PostgreSQL")
	}
	database := testpg.Open(t)
	ctx := context.Background()
	if err := Migrate(ctx, database.DB, WithSchema(database.Schema)); err != nil {
		t.Fatal(err)
	}
	_, exec, stop := startHundredCommandRun(t, database, ctx, "journal-growth")
	defer stop()
	var rows int
	var tupleBytes, bodyBytes int64
	if err := database.DB.Conn.QueryRow(ctx, `SELECT count(*),COALESCE(sum(pg_column_size(j)),0),COALESCE(sum(octet_length(body)),0)
		FROM `+pgschema.Table(database.Schema, "flow_journal")+` j WHERE run_id=$1`, exec.ID).
		Scan(&rows, &tupleBytes, &bodyBytes); err != nil {
		t.Fatal(err)
	}
	if rows != 402 {
		t.Fatalf("journal rows=%d want 402", rows)
	}
	t.Logf("100-command journal: rows=%d tuple_bytes=%d body_bytes=%d tuple_bytes_per_command=%.1f",
		rows, tupleBytes, bodyBytes, float64(tupleBytes)/100)
}

type benchmarkTB interface {
	Helper()
	Fatal(...any)
}

func startHundredCommandRun(tb benchmarkTB, database testpg.Database, ctx context.Context, key string) (*Runtime, Run, func()) {
	tb.Helper()
	child := DefineCommand[None, None]("benchmark.inspection.child", 1)
	root := DefineCommand[None, None]("benchmark.inspection.root", 1)
	runtime, err := New(database.DB, WithSchema(database.Schema), WithNotifications(false),
		WithMaxCommandsPerRun(0), WithWorkerConcurrency(16), WithPollInterval(5*time.Millisecond))
	if err != nil {
		tb.Fatal(err)
	}
	if err := runtime.Register(
		Handle(root, func(_ context.Context, work *Work[None]) (None, error) {
			for index := range 99 {
				Enqueue(work, fmt.Sprintf("work/%03d", index), child, None{})
			}
			return None{}, nil
		}),
		Handle(child, func(context.Context, *Work[None]) (None, error) { return None{}, nil }),
	); err != nil {
		tb.Fatal(err)
	}
	runCtx, cancel := context.WithCancel(ctx)
	runResult := make(chan error, 1)
	go func() { runResult <- runtime.Run(runCtx) }()
	exec, err := root.Enqueue(ctx, runtime, key, None{}, WithoutRunDeadline())
	if err != nil {
		cancel()
		tb.Fatal(err)
	}
	deadlineCtx, deadlineCancel := context.WithTimeout(ctx, 10*time.Second)
	settled, err := AwaitRun(deadlineCtx, runtime, exec.RunID)
	deadlineCancel()
	if err != nil || settled.Status != "succeeded" || settled.CommandCount != 100 {
		cancel()
		<-runResult
		tb.Fatal("hundred-command run failed", err, settled)
	}
	return runtime, settled, func() {
		cancel()
		<-runResult
	}
}
