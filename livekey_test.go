package flow

import (
	"context"
	"errors"
	"sync/atomic"
	"testing"
	"time"

	"github.com/goware/flow/internal/testpg"
)

type liveKeyArgs struct {
	Value string `json:"value"`
}

type liveKeyResult struct {
	Value string `json:"value"`
}

// A live-scoped key is held while its execution is non-terminal — repeated
// starts rediscover the live execution with no identity comparison — and is
// released at settlement, so a later start with the same key creates a new
// execution.
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

	first, err := command.With(runtime).Execute(ctx, "entity/1", liveKeyArgs{Value: "a"}, WithLiveKey())
	if err != nil || !first.Created {
		t.Fatalf("first live start = %#v, %v", first, err)
	}

	// While the first execution is live, an equivalent start and a start with
	// different arguments both rediscover it silently: live keys are a dedupe
	// on the entity, not an identity assertion.
	same, err := command.With(runtime).Execute(ctx, "entity/1", liveKeyArgs{Value: "a"}, WithLiveKey())
	if err != nil || same.Created || same.ID != first.ID {
		t.Fatalf("equivalent live start = %#v, %v", same, err)
	}
	different, err := command.With(runtime).Execute(ctx, "entity/1", liveKeyArgs{Value: "different"}, WithLiveKey())
	if err != nil || different.Created || different.ID != first.ID {
		t.Fatalf("differing live start = %#v, %v", different, err)
	}

	live, found, err := LookupLiveExecution(ctx, runtime, command.Name(), "entity/1")
	if err != nil || !found || live.ID != first.ID {
		t.Fatalf("LookupLiveExecution(live) = %#v, %v, %v", live, found, err)
	}

	close(release)
	if _, err := AwaitExecution(ctx, runtime, first.ID); err != nil {
		t.Fatalf("AwaitExecution() error = %v", err)
	}

	if _, found, err := LookupLiveExecution(ctx, runtime, command.Name(), "entity/1"); err != nil || found {
		t.Fatalf("LookupLiveExecution(settled) = %v, %v", found, err)
	}

	// The settled execution released the key: the same key starts fresh work.
	second, err := command.With(runtime).Execute(ctx, "entity/1", liveKeyArgs{Value: "b"}, WithLiveKey())
	if err != nil || !second.Created || second.ID == first.ID {
		t.Fatalf("post-settlement live start = %#v, %v", second, err)
	}
	if _, err := AwaitExecution(ctx, runtime, second.ID); err != nil {
		t.Fatalf("AwaitExecution(second) error = %v", err)
	}
	if got := invocations.Load(); got != 2 {
		t.Fatalf("handler invocations = %d, want 2", got)
	}

	// Permanent-scope semantics are untouched: the same key under the default
	// scope is its own identity space and conflicts on differing input.
	permanent, err := command.With(runtime).Execute(ctx, "entity/1", liveKeyArgs{Value: "perm"})
	if err != nil || !permanent.Created {
		t.Fatalf("permanent start = %#v, %v", permanent, err)
	}
	if _, err := command.With(runtime).Execute(ctx, "entity/1", liveKeyArgs{Value: "other"}); !errors.Is(err, ErrConflict) {
		t.Fatalf("permanent conflicting start error = %v", err)
	}
}

func TestLiveKeyRequiresExecutionKey(t *testing.T) {
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
	if _, err := command.With(runtime).Execute(ctx, "", liveKeyArgs{}, WithLiveKey()); !errors.Is(err, ErrInvalid) {
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
	handle, err := command.With(runtime).Execute(ctx, "delayed/1", liveKeyArgs{Value: "later"}, WithLiveKey(), WithStartDelay(delay))
	if err != nil || !handle.Created {
		t.Fatalf("delayed start = %#v, %v", handle, err)
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

	execution, err := AwaitExecution(ctx, runtime, handle.ID)
	if err != nil {
		t.Fatalf("AwaitExecution() error = %v", err)
	}
	if execution.Status != "succeeded" {
		t.Fatalf("delayed execution status = %s", execution.Status)
	}
	if elapsed := time.Since(startedAt); elapsed < delay {
		t.Fatalf("delayed root ran after %s, before its %s delay", elapsed, delay)
	}

	plan := DefinePlan[liveKeyArgs]("livekey.plan", 1, func(*Plan, liveKeyArgs) {})
	if _, err := plan.With(runtime).Execute(ctx, "plan/delayed", liveKeyArgs{}, WithStartDelay(delay)); !errors.Is(err, ErrInvalid) {
		t.Fatalf("plan start delay error = %v", err)
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
	if _, err := command.With(runtime).Execute(ctx, "depth/ready", liveKeyArgs{Value: "r"}); err != nil {
		t.Fatalf("ready start error = %v", err)
	}
	if _, err := command.With(runtime).Execute(ctx, "depth/delayed", liveKeyArgs{Value: "d"}, WithStartDelay(time.Hour)); err != nil {
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
