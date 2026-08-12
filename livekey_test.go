package flow

import (
	"context"
	"errors"
	"sync/atomic"
	"testing"
	"time"

	"github.com/google/uuid"
	"github.com/goware/flow/internal/testpg"
)

type liveKeyArgs struct {
	Value string `json:"value"`
}

func TestRenamedStoreGetContracts(t *testing.T) {
	t.Parallel()
	database := testpg.Open(t)
	ctx := context.Background()
	if err := Migrate(ctx, database.DB, WithSchema(database.Schema)); err != nil {
		t.Fatal(err)
	}
	runtime, err := New(database.DB, WithSchema(database.Schema))
	if err != nil {
		t.Fatal(err)
	}
	command := DefineCommand[None, None]("store.get_contracts", 1)
	event := DefineEvent[string]("store.get_contracts_event")
	run, err := command.Enqueue(ctx, runtime, "entity/gets", None{}, WithLiveKey(), WithStartDelay(time.Hour))
	if err != nil {
		t.Fatal(err)
	}
	runID, err := parseRunID(run.RunID)
	if err != nil {
		t.Fatal(err)
	}
	runSnapshot := mustGetRun(t, runtime, run.RunID)
	commandID, err := parseCommandID(runSnapshot.RootCommandID)
	if err != nil {
		t.Fatal(err)
	}

	if _, found, err := runtime.store.GetEvent(ctx, nil, runID, event.Name(), "ready"); err != nil || found {
		t.Fatalf("GetEvent(absent) = found %v, %v", found, err)
	}
	owner, err := runtime.store.GetCommandRunID(ctx, nil, commandID)
	if err != nil || owner != runID {
		t.Fatalf("GetCommandRunID() = %s, %v; want %s", owner, err, runID)
	}
	if _, err := runtime.store.GetCommandRunID(ctx, nil, uuid.New()); !errors.Is(err, ErrNotFound) {
		t.Fatalf("GetCommandRunID(missing) error = %v, want ErrNotFound", err)
	}

	row, found, err := runtime.store.GetCurrentRun(ctx, nil, command.Name(), "entity/gets")
	if err != nil || !found || row.ID != runID {
		t.Fatalf("GetCurrentRun(no tx) = %#v, %v, %v", row, found, err)
	}
	tx, err := database.DB.Conn.Begin(ctx)
	if err != nil {
		t.Fatal(err)
	}
	defer tx.Rollback(ctx)
	row, found, err = runtime.store.GetCurrentRun(ctx, tx, command.Name(), "entity/gets")
	if err != nil || !found || row.ID != runID {
		t.Fatalf("GetCurrentRun(tx) = %#v, %v, %v", row, found, err)
	}
	if err := tx.Rollback(ctx); err != nil {
		t.Fatal(err)
	}

	if err := event.Deliver(ctx, runtime, run.RunID, "ready", "value"); err != nil {
		t.Fatal(err)
	}
	record, found, err := runtime.store.GetEvent(ctx, nil, runID, event.Name(), "ready")
	if err != nil || !found || record.ID == uuid.Nil || len(record.Body) == 0 {
		t.Fatalf("GetEvent(present) = %#v, %v, %v", record, found, err)
	}
	if err := CancelRun(ctx, runtime, run.RunID, "store get contract complete"); err != nil {
		t.Fatal(err)
	}
	if _, found, err := runtime.store.GetCurrentRun(ctx, nil, command.Name(), "entity/gets"); err != nil || found {
		t.Fatalf("GetCurrentRun(terminal) = found %v, %v", found, err)
	}
}

type liveKeyResult struct {
	Value string `json:"value"`
}

func TestCommandGetCurrentRunUsesDefinitionNameAndCallerTransaction(t *testing.T) {
	t.Parallel()
	database := testpg.Open(t)
	ctx := context.Background()
	if err := Migrate(ctx, database.DB, WithSchema(database.Schema)); err != nil {
		t.Fatal(err)
	}
	runtime, err := New(database.DB, WithSchema(database.Schema))
	if err != nil {
		t.Fatal(err)
	}
	v1 := DefineCommand[None, None]("current_run.command", 1, WithQueue("current_run.queue"))
	v2 := DefineCommand[None, None](v1.Name(), 2, WithQueue("current_run.queue"))
	started, err := v1.Enqueue(ctx, runtime, "current/visible", None{}, WithLiveKey(), WithStartDelay(time.Hour))
	if err != nil {
		t.Fatal(err)
	}
	fromMethod, found, err := v1.GetCurrentRun(ctx, runtime, "current/visible")
	if err != nil || !found || fromMethod.ID != started.RunID || fromMethod.RootCommandVersion != 1 {
		t.Fatalf("Command.GetCurrentRun(v1) = %#v, %t, %v", fromMethod, found, err)
	}
	fromNewVersion, found, err := v2.GetCurrentRun(ctx, runtime, "current/visible")
	if err != nil || !found || fromNewVersion.ID != started.RunID || fromNewVersion.RootCommandVersion != 1 {
		t.Fatalf("Command.GetCurrentRun(v2) = %#v, %t, %v", fromNewVersion, found, err)
	}
	fromTopLevel, found, err := GetCurrentRun(ctx, runtime, v1.Name(), "current/visible")
	if err != nil || !found || fromTopLevel.ID != started.RunID {
		t.Fatalf("GetCurrentRun() = %#v, %t, %v", fromTopLevel, found, err)
	}
	if _, found, err := GetCurrentRun(ctx, runtime, v1.Queue(), "current/visible"); err != nil || found {
		t.Fatalf("GetCurrentRun(queue) found=%t error=%v", found, err)
	}
	if _, _, err := (Command[None, None]{}).GetCurrentRun(ctx, runtime, "current/visible"); !errors.Is(err, ErrInvalid) {
		t.Fatalf("zero Command.GetCurrentRun() error=%v", err)
	}

	tx, err := database.DB.Conn.Begin(ctx)
	if err != nil {
		t.Fatal(err)
	}
	defer tx.Rollback(ctx)
	txClient := runtime.InTx(tx)
	uncommitted, err := v1.Enqueue(ctx, txClient, "current/uncommitted", None{}, WithLiveKey(), WithStartDelay(time.Hour))
	if err != nil {
		t.Fatal(err)
	}
	inside, found, err := v1.GetCurrentRun(ctx, txClient, "current/uncommitted")
	if err != nil || !found || inside.ID != uncommitted.RunID {
		t.Fatalf("transaction Command.GetCurrentRun() = %#v, %t, %v", inside, found, err)
	}
	if _, found, err := v1.GetCurrentRun(ctx, runtime, "current/uncommitted"); err != nil || found {
		t.Fatalf("outside Command.GetCurrentRun(uncommitted) found=%t error=%v", found, err)
	}
}

// A live-scoped key is held while its run is non-terminal — repeated
// starts rediscover the live run with no identity comparison — and is
// released at settlement, so a later start with the same key creates a new
// run.
func TestLiveKeyReleasesOnSettlement(t *testing.T) {
	t.Parallel()

	database := testpg.Open(t)
	ctx := context.Background()
	if err := Migrate(ctx, database.DB, WithSchema(database.Schema)); err != nil {
		t.Fatalf("Migrate() error = %v", err)
	}
	runtime, err := New(database.DB, WithSchema(database.Schema), WithPollInterval(20*time.Millisecond))
	if err != nil {
		t.Fatalf("New() error = %v", err)
	}

	release := make(chan struct{})
	var invocations atomic.Int64
	command := DefineCommand[liveKeyArgs, liveKeyResult]("livekey.work", 1)
	if err := runtime.Register(Handle(command, func(ctx context.Context, work *Work[liveKeyArgs]) (liveKeyResult, error) {
		invocations.Add(1)
		select {
		case <-release:
		case <-ctx.Done():
			return liveKeyResult{}, ctx.Err()
		}
		return liveKeyResult{Value: work.Args.Value}, nil
	})); err != nil {
		t.Fatalf("Register() error = %v", err)
	}

	runCtx, stop := context.WithCancel(ctx)
	defer stop()
	go runtime.Run(runCtx) //nolint:errcheck // returns nil on cancel

	first, err := command.Enqueue(ctx, runtime, "entity/1", liveKeyArgs{Value: "a"}, WithLiveKey())
	if err != nil || !first.Created {
		t.Fatalf("first live start = %#v, %v", first, err)
	}

	// While the first run is live, an equivalent start and a start with
	// different arguments both rediscover it silently: live keys are a dedupe
	// on the entity, not an identity assertion.
	same, err := command.Enqueue(ctx, runtime, "entity/1", liveKeyArgs{Value: "a"}, WithLiveKey())
	if err != nil || same.Created || same.RunID != first.RunID {
		t.Fatalf("equivalent live start = %#v, %v", same, err)
	}
	different, err := command.Enqueue(ctx, runtime, "entity/1", liveKeyArgs{Value: "different"}, WithLiveKey())
	if err != nil || different.Created || different.RunID != first.RunID {
		t.Fatalf("differing live start = %#v, %v", different, err)
	}

	live, found, err := GetCurrentRun(ctx, runtime, command.Name(), "entity/1")
	if err != nil || !found || live.ID != first.RunID {
		t.Fatalf("GetCurrentRun(live) = %#v, %v, %v", live, found, err)
	}

	close(release)
	if _, err := AwaitRun(ctx, runtime, first.RunID); err != nil {
		t.Fatalf("AwaitRun() error = %v", err)
	}

	if _, found, err := GetCurrentRun(ctx, runtime, command.Name(), "entity/1"); err != nil || found {
		t.Fatalf("GetCurrentRun(settled) = %v, %v", found, err)
	}

	// The settled run released the key: the same key starts fresh work.
	second, err := command.Enqueue(ctx, runtime, "entity/1", liveKeyArgs{Value: "b"}, WithLiveKey())
	if err != nil || !second.Created || second.RunID == first.RunID {
		t.Fatalf("post-settlement live start = %#v, %v", second, err)
	}
	if _, err := AwaitRun(ctx, runtime, second.RunID); err != nil {
		t.Fatalf("AwaitRun(second) error = %v", err)
	}
	if got := invocations.Load(); got != 2 {
		t.Fatalf("handler invocations = %d, want 2", got)
	}

	// Permanent-scope semantics are untouched: the same key under the default
	// scope is its own identity space and conflicts on differing input.
	permanent, err := command.Enqueue(ctx, runtime, "entity/1", liveKeyArgs{Value: "perm"})
	if err != nil || !permanent.Created {
		t.Fatalf("permanent start = %#v, %v", permanent, err)
	}
	if _, err := command.Enqueue(ctx, runtime, "entity/1", liveKeyArgs{Value: "other"}); !errors.Is(err, ErrConflict) {
		t.Fatalf("permanent conflicting start error = %v", err)
	}
}

func TestLiveKeyRequiresRunKey(t *testing.T) {
	t.Parallel()

	database := testpg.Open(t)
	ctx := context.Background()
	if err := Migrate(ctx, database.DB, WithSchema(database.Schema)); err != nil {
		t.Fatalf("Migrate() error = %v", err)
	}
	runtime, err := New(database.DB, WithSchema(database.Schema))
	if err != nil {
		t.Fatalf("New() error = %v", err)
	}
	command := DefineCommand[liveKeyArgs, liveKeyResult]("livekey.unkeyed", 1)
	if _, err := command.Enqueue(ctx, runtime, "", liveKeyArgs{}, WithLiveKey()); !errors.Is(err, ErrInvalid) {
		t.Fatalf("unkeyed live start error = %v", err)
	}
}

// WithStartDelay parks the root command as delayed work: not deliverable
// before the delay, visible in the queue depth as Delayed, and executed once
// the delay elapses.
func TestStartDelayDefersRootDelivery(t *testing.T) {
	t.Parallel()

	database := testpg.Open(t)
	ctx := context.Background()
	if err := Migrate(ctx, database.DB, WithSchema(database.Schema)); err != nil {
		t.Fatalf("Migrate() error = %v", err)
	}
	runtime, err := New(database.DB, WithSchema(database.Schema), WithPollInterval(20*time.Millisecond))
	if err != nil {
		t.Fatalf("New() error = %v", err)
	}

	command := DefineCommand[liveKeyArgs, liveKeyResult]("livekey.delayed", 1, WithQueue("livekey.lane"))
	if err := runtime.Register(Handle(command, func(ctx context.Context, work *Work[liveKeyArgs]) (liveKeyResult, error) {
		return liveKeyResult{Value: work.Args.Value}, nil
	})); err != nil {
		t.Fatalf("Register() error = %v", err)
	}

	const delay = 400 * time.Millisecond
	startedAt := time.Now()
	exec, err := command.Enqueue(ctx, runtime, "delayed/1", liveKeyArgs{Value: "later"}, WithLiveKey(), WithStartDelay(delay))
	if err != nil || !exec.Created {
		t.Fatalf("delayed start = %#v, %v", exec, err)
	}

	depth, err := GetQueueDepth(ctx, runtime, "livekey.lane")
	if err != nil {
		t.Fatalf("GetQueueDepth() error = %v", err)
	}
	if depth.Ready != 0 || depth.Delayed != 1 || depth.Running != 0 {
		t.Fatalf("pre-delay depth = %#v", depth)
	}

	runCtx, stop := context.WithCancel(ctx)
	defer stop()
	go runtime.Run(runCtx) //nolint:errcheck // returns nil on cancel

	run, err := AwaitRun(ctx, runtime, exec.RunID)
	if err != nil {
		t.Fatalf("AwaitRun() error = %v", err)
	}
	if run.Status != "succeeded" {
		t.Fatalf("delayed run status = %s", run.Status)
	}
	if elapsed := time.Since(startedAt); elapsed < delay {
		t.Fatalf("delayed root ran after %s, before its %s delay", elapsed, delay)
	}

}

// GetQueueDepth counts one lane's deliverable, scheduled, and leased commands
// without a running runtime.
func TestGetQueueDepthCountsLane(t *testing.T) {
	t.Parallel()

	database := testpg.Open(t)
	ctx := context.Background()
	if err := Migrate(ctx, database.DB, WithSchema(database.Schema)); err != nil {
		t.Fatalf("Migrate() error = %v", err)
	}
	runtime, err := New(database.DB, WithSchema(database.Schema))
	if err != nil {
		t.Fatalf("New() error = %v", err)
	}

	command := DefineCommand[liveKeyArgs, liveKeyResult]("livekey.depth", 1, WithQueue("livekey.depth.lane"))
	if _, err := command.Enqueue(ctx, runtime, "depth/ready", liveKeyArgs{Value: "r"}); err != nil {
		t.Fatalf("ready start error = %v", err)
	}
	if _, err := command.Enqueue(ctx, runtime, "depth/delayed", liveKeyArgs{Value: "d"}, WithStartDelay(time.Hour)); err != nil {
		t.Fatalf("delayed start error = %v", err)
	}

	depth, err := GetQueueDepth(ctx, runtime, "livekey.depth.lane")
	if err != nil {
		t.Fatalf("GetQueueDepth() error = %v", err)
	}
	if depth.Ready != 1 || depth.Delayed != 1 || depth.Running != 0 || depth.OldestReadyFor < 0 {
		t.Fatalf("queue depth = %#v", depth)
	}

	empty, err := GetQueueDepth(ctx, runtime, "livekey.empty.lane")
	if err != nil {
		t.Fatalf("GetQueueDepth(empty) error = %v", err)
	}
	if empty.Ready != 0 || empty.Delayed != 0 || empty.Running != 0 || empty.OldestReadyFor != 0 {
		t.Fatalf("empty queue depth = %#v", empty)
	}

	if _, err := GetQueueDepth(ctx, runtime, ""); !errors.Is(err, ErrInvalid) {
		t.Fatalf("unnamed queue depth error = %v", err)
	}
}
