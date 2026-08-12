package flow

import (
	"context"
	"errors"
	"math"
	"strconv"
	"testing"
	"time"

	"github.com/google/uuid"
	"github.com/goware/flow/internal/canonical"
	"github.com/goware/flow/internal/failure"
	"github.com/goware/flow/internal/fault"
	retrypolicy "github.com/goware/flow/internal/retry"
	"github.com/goware/flow/internal/store"
	"github.com/goware/flow/internal/testpg"
)

func TestPublicDurableDurationsRoundUpToMilliseconds(t *testing.T) {
	t.Parallel()

	if command := DefineCommand[None, None]("duration.timeout", 1, WithTimeout(time.Nanosecond)); command.err != nil || command.defaults.attemptTimeout != time.Millisecond {
		t.Fatalf("nanosecond timeout = %s, %v", command.defaults.attemptTimeout, command.err)
	}
	if policy := RetryFor(time.Millisecond + time.Nanosecond); validateRetryPolicy(policy) != nil || *retrypolicy.ValueOf(policy).MaxElapsed != 2*time.Millisecond {
		t.Fatalf("fractional retry elapsed = %#v, %v", retrypolicy.ValueOf(policy), validateRetryPolicy(policy))
	}
	if policy := RetryFor(time.Second).Backoff(time.Nanosecond, time.Millisecond+time.Nanosecond); validateRetryPolicy(policy) != nil ||
		!retrypolicy.ValueOf(policy).Equal(retrypolicy.Policy{MaxElapsed: durationPointerForTest(time.Second), Backoff: []time.Duration{time.Millisecond, 2 * time.Millisecond}, Jitter: retrypolicy.DefaultJitter}) {
		t.Fatalf("fractional retry backoff = %#v, %v", retrypolicy.ValueOf(policy), validateRetryPolicy(policy))
	}
	input, err := canonical.Marshal(None{}, 0)
	if err != nil {
		t.Fatal(err)
	}
	options, firstFingerprint, err := prepareStartOptions("duration.start", 1, "key", input, 0,
		WithRunDeadline(time.Nanosecond), WithStartDelay(time.Nanosecond), WaitFor(DefineEvent[None]("duration.gate"), "ready"), Within(time.Nanosecond))
	if err != nil || options.deadline.Duration != time.Millisecond || options.startDelay != time.Millisecond || options.within != time.Millisecond {
		t.Fatalf("normalized start options = %#v, %v", options, err)
	}
	_, secondFingerprint, err := prepareStartOptions("duration.start", 1, "key", input, 0,
		WithRunDeadline(time.Millisecond), WithStartDelay(time.Millisecond), WaitFor(DefineEvent[None]("duration.gate"), "ready"), Within(time.Millisecond))
	if err != nil || firstFingerprint != secondFingerprint {
		t.Fatalf("equivalent normalized starts differ: %x != %x, %v", firstFingerprint, secondFingerprint, err)
	}
	event := DefineEvent[None]("duration.gate")
	nodeDelay := &Work[None]{scope: &scopeState{}}
	Enqueue(nodeDelay, "child", DefineCommand[None, None]("duration.child.delay", 1), None{}).Delay(time.Nanosecond)
	if nodeDelay.scope.firstError != nil || nodeDelay.scope.decision.commands["child"].startAfter != time.Millisecond {
		t.Fatalf("normalized node delay = %#v, %v", nodeDelay.scope.decision.commands["child"], nodeDelay.scope.firstError)
	}
	nodeWithin := &Work[None]{scope: &scopeState{}}
	Enqueue(nodeWithin, "child", DefineCommand[None, None]("duration.child.within", 1), None{}).
		WaitFor(event, "ready").Within(time.Nanosecond)
	if nodeWithin.scope.firstError != nil || nodeWithin.scope.decision.commands["child"].within != time.Millisecond {
		t.Fatalf("normalized node within = %#v, %v", nodeWithin.scope.decision.commands["child"], nodeWithin.scope.firstError)
	}
	retryAfter := RetryAfter(time.Nanosecond, errors.New("later"))
	if delay, ok := failure.RetryDelay(retryAfter); !ok || delay != time.Millisecond {
		t.Fatalf("RetryAfter delay = %s, %v", delay, ok)
	}
	maximumExact := time.Duration(math.MaxInt64/int64(time.Millisecond)) * time.Millisecond
	if command := DefineCommand[None, None]("duration.maximum", 1, WithTimeout(maximumExact)); command.err != nil {
		t.Fatalf("maximum exact duration rejected: %v", command.err)
	}
	for name, command := range map[string]Command[None, None]{
		"zero":     DefineCommand[None, None]("duration.zero", 1, WithTimeout(0)),
		"negative": DefineCommand[None, None]("duration.negative", 1, WithTimeout(-time.Nanosecond)),
		"overflow": DefineCommand[None, None]("duration.overflow", 1, WithTimeout(maximumExact+time.Nanosecond)),
	} {
		if command.err == nil {
			t.Fatalf("%s timeout was accepted", name)
		}
	}
}

func TestEveryPublicDurableDurationBoundary(t *testing.T) {
	t.Parallel()

	maximumExact := time.Duration(math.MaxInt64/int64(time.Millisecond)) * time.Millisecond
	tests := []struct {
		name  string
		value time.Duration
		want  time.Duration
		valid bool
	}{
		{name: "nanosecond", value: time.Nanosecond, want: time.Millisecond, valid: true},
		{name: "sub millisecond", value: 999*time.Microsecond + 999*time.Nanosecond, want: time.Millisecond, valid: true},
		{name: "exact millisecond", value: time.Millisecond, want: time.Millisecond, valid: true},
		{name: "fractional millisecond", value: time.Millisecond + time.Nanosecond, want: 2 * time.Millisecond, valid: true},
		{name: "maximum exact", value: maximumExact, want: maximumExact, valid: true},
		{name: "ceiling overflow", value: maximumExact + time.Nanosecond},
		{name: "zero", value: 0},
		{name: "negative", value: -time.Nanosecond},
	}
	event := DefineEvent[None]("duration.boundary_gate")
	type feature struct {
		name  string
		apply func(time.Duration) (time.Duration, error)
	}
	features := []feature{
		{name: "attempt timeout", apply: func(value time.Duration) (time.Duration, error) {
			command := DefineCommand[None, None]("duration.boundary_timeout", 1, WithTimeout(value))
			return command.defaults.attemptTimeout, command.err
		}},
		{name: "run deadline", apply: func(value time.Duration) (time.Duration, error) {
			options := runOptions{}
			WithRunDeadline(value).applyRun(&options)
			return options.deadline.Duration, errors.Join(options.errs...)
		}},
		{name: "start delay", apply: func(value time.Duration) (time.Duration, error) {
			options := runOptions{}
			WithStartDelay(value).applyRun(&options)
			return options.startDelay, errors.Join(options.errs...)
		}},
		{name: "root within", apply: func(value time.Duration) (time.Duration, error) {
			options := runOptions{}
			Within(value).applyRun(&options)
			return options.within, errors.Join(options.errs...)
		}},
		{name: "child delay", apply: func(value time.Duration) (time.Duration, error) {
			work := &Work[None]{scope: &scopeState{}}
			Enqueue(work, "child", DefineCommand[None, None]("duration.boundary_child_delay", 1), None{}).Delay(value)
			return work.scope.decision.commands["child"].startAfter, work.scope.firstError
		}},
		{name: "child within", apply: func(value time.Duration) (time.Duration, error) {
			work := &Work[None]{scope: &scopeState{}}
			Enqueue(work, "child", DefineCommand[None, None]("duration.boundary_child_within", 1), None{}).
				WaitFor(event, "ready").Within(value)
			return work.scope.decision.commands["child"].within, work.scope.firstError
		}},
		{name: "retry elapsed", apply: func(value time.Duration) (time.Duration, error) {
			policy := RetryFor(value)
			if err := validateRetryPolicy(policy); err != nil {
				return 0, err
			}
			return *retrypolicy.ValueOf(policy).MaxElapsed, nil
		}},
		{name: "retry backoff", apply: func(value time.Duration) (time.Duration, error) {
			policy := Attempts(2).Backoff(value)
			if err := validateRetryPolicy(policy); err != nil {
				return 0, err
			}
			return retrypolicy.ValueOf(policy).Backoff[0], nil
		}},
		{name: "retry after", apply: func(value time.Duration) (time.Duration, error) {
			retry := RetryAfter(value, errors.New("retry"))
			if err := failure.ValidateRetryAfter(retry); err != nil {
				return 0, err
			}
			delay, ok := failure.RetryDelay(retry)
			if !ok {
				return 0, errors.New("retry-after delay is absent")
			}
			return delay, nil
		}},
	}
	for _, feature := range features {
		feature := feature
		t.Run(feature.name, func(t *testing.T) {
			for _, test := range tests {
				test := test
				t.Run(test.name, func(t *testing.T) {
					got, err := feature.apply(test.value)
					if test.valid {
						if err != nil || got != test.want {
							t.Fatalf("duration %s normalized to %s, %v; want %s", test.value, got, err, test.want)
						}
						return
					}
					if err == nil {
						t.Fatalf("duration %s was accepted as %s", test.value, got)
					}
				})
			}
		})
	}
}

func TestNormalizedDurationsRediscoverEquivalentDurableDeclarations(t *testing.T) {
	t.Parallel()

	database := testpg.Open(t)
	ctx := context.Background()
	if err := Migrate(ctx, database.DB, WithSchema(database.Schema)); err != nil {
		t.Fatal(err)
	}
	runtime, err := New(database.DB, WithSchema(database.Schema), WithNotifications(false))
	if err != nil {
		t.Fatal(err)
	}
	event := DefineEvent[None]("duration.rediscovery_gate")
	firstCommand := DefineCommand[None, None]("duration.rediscovery_root", 1,
		WithTimeout(time.Nanosecond), WithRetry(RetryFor(time.Nanosecond).Backoff(time.Nanosecond)))
	first, err := firstCommand.Enqueue(ctx, runtime, "same", None{},
		WithRunDeadline(time.Nanosecond), WithStartDelay(time.Nanosecond),
		WaitFor(event, "ready"), Within(time.Nanosecond))
	if err != nil {
		t.Fatal(err)
	}
	secondCommand := DefineCommand[None, None]("duration.rediscovery_root", 1,
		WithTimeout(time.Millisecond), WithRetry(RetryFor(time.Millisecond).Backoff(time.Millisecond)))
	second, err := secondCommand.Enqueue(ctx, runtime, "same", None{},
		WithRunDeadline(time.Millisecond), WithStartDelay(time.Millisecond),
		WaitFor(event, "ready"), Within(time.Millisecond))
	if err != nil || second.RunID != first.RunID || second.Created {
		t.Fatalf("equivalent normalized start = %#v, %v; first=%#v", second, err, first)
	}

	childEvent := DefineEvent[None]("duration.rediscovery_child_gate")
	equivalent := &Work[None]{scope: &scopeState{}}
	Enqueue(equivalent, "child", DefineCommand[None, None]("duration.rediscovery_child", 1,
		WithTimeout(time.Nanosecond), WithRetry(RetryFor(time.Nanosecond).Backoff(time.Nanosecond))), None{}).
		Delay(time.Nanosecond).WaitFor(childEvent, "ready").Within(time.Nanosecond)
	Enqueue(equivalent, "child", DefineCommand[None, None]("duration.rediscovery_child", 1,
		WithTimeout(time.Millisecond), WithRetry(RetryFor(time.Millisecond).Backoff(time.Millisecond))), None{}).
		Delay(time.Millisecond).WaitFor(childEvent, "ready").Within(time.Millisecond)
	if equivalent.scope.firstError != nil || len(equivalent.scope.decision.commands) != 1 {
		t.Fatalf("equivalent child declarations = %#v, %v", equivalent.scope.decision.commands, equivalent.scope.firstError)
	}

	conflicting := &Work[None]{scope: &scopeState{}}
	Enqueue(conflicting, "child", DefineCommand[None, None]("duration.rediscovery_conflict", 1), None{}).Delay(time.Nanosecond)
	Enqueue(conflicting, "child", DefineCommand[None, None]("duration.rediscovery_conflict", 1), None{}).Delay(time.Millisecond + time.Nanosecond)
	if !errors.Is(conflicting.scope.firstError, ErrInvalid) {
		t.Fatalf("different normalized child declarations error = %v, want ErrInvalid", conflicting.scope.firstError)
	}
}

func durationPointerForTest(value time.Duration) *time.Duration { return &value }

func TestPostgresIntegerPublicBoundaries(t *testing.T) {
	t.Parallel()

	if command := DefineCommand[None, None]("integer.maximum", math.MaxInt32); command.err != nil {
		t.Fatalf("maximum PostgreSQL command version rejected: %v", command.err)
	}
	if strconv.IntSize > 32 {
		tooLargeValue := int64(math.MaxInt32) + 1
		tooLarge := int(tooLargeValue)
		if command := DefineCommand[None, None]("integer.too_large", tooLarge); !errors.Is(command.err, ErrInvalid) {
			t.Fatalf("oversized command version error = %v, want ErrInvalid", command.err)
		}
		options := runtimeOptions{}
		WithMaxCommandsPerRun(tooLarge).applyRuntime(&options)
		if err := errors.Join(options.errs...); !errors.Is(err, ErrInvalid) {
			t.Fatalf("oversized maximum commands error = %v, want ErrInvalid", err)
		}
	}
}

func TestCommandDeclarationFingerprintIncludesDurableSettings(t *testing.T) {
	t.Parallel()

	args, err := canonical.Marshal(None{}, 0)
	if err != nil {
		t.Fatal(err)
	}
	policy, err := retrypolicy.CanonicalPublic(retrypolicy.DefaultPublic())
	if err != nil {
		t.Fatal(err)
	}
	alternatePolicy, err := retrypolicy.CanonicalPublic(retrypolicy.NewPublicAttempts(2))
	if err != nil {
		t.Fatal(err)
	}
	base := store.CommandCreate{
		ID: uuid.New(), Key: "child", Name: "fingerprint.command", Version: 1,
		Args: args, Queue: "default", RetryPolicy: policy,
	}
	want, err := commandDeclarationFingerprint(base)
	if err != nil {
		t.Fatal(err)
	}
	for name, mutate := range map[string]func(*store.CommandCreate){
		"queue":           func(command *store.CommandCreate) { command.Queue = "priority" },
		"attempt timeout": func(command *store.CommandCreate) { command.AttemptTimeout = time.Millisecond },
		"retry policy":    func(command *store.CommandCreate) { command.RetryPolicy = alternatePolicy },
	} {
		t.Run(name, func(t *testing.T) {
			changed := base
			mutate(&changed)
			got, err := commandDeclarationFingerprint(changed)
			if err != nil {
				t.Fatal(err)
			}
			if got == want {
				t.Fatalf("%s did not change declaration fingerprint", name)
			}
		})
	}
}

func TestLargestPostgresIntegerPersists(t *testing.T) {
	t.Parallel()

	database := testpg.Open(t)
	ctx := context.Background()
	if err := Migrate(ctx, database.DB, WithSchema(database.Schema)); err != nil {
		t.Fatal(err)
	}
	runtime, err := New(database.DB, WithSchema(database.Schema), WithMaxCommandsPerRun(math.MaxInt32))
	if err != nil {
		t.Fatal(err)
	}
	command := DefineCommand[None, None]("integer.persisted", math.MaxInt32)
	run, err := command.Enqueue(ctx, runtime, "maximum", None{})
	if err != nil {
		t.Fatal(err)
	}
	runSnapshot := mustGetRun(t, runtime, run.RunID)
	if runSnapshot.RootCommandVersion != math.MaxInt32 || runSnapshot.MaxCommands != math.MaxInt32 {
		t.Fatalf("persisted integer boundaries = version %d max %d", runSnapshot.RootCommandVersion, runSnapshot.MaxCommands)
	}
}

func TestMalformedRetryPolicyBytesFailOnRead(t *testing.T) {
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
	command := DefineCommand[None, None]("retry.malformed", 1)
	run, err := command.Enqueue(ctx, runtime, "malformed", None{})
	if err != nil {
		t.Fatal(err)
	}
	candidates, err := runtime.store.ProbeCommands(ctx, []store.CommandKind{{Name: command.Name(), Version: command.Version()}}, 1)
	if err != nil || len(candidates) != 1 {
		t.Fatalf("probe command = %#v, %v", candidates, err)
	}
	if _, err := database.DB.Conn.Exec(ctx, `UPDATE `+quoteIdentifier(database.Schema)+`.flow_commands
		SET retry_policy=$2 WHERE run_id=$1`, run.RunID, []byte("not canonical JSON")); err != nil {
		t.Fatal(err)
	}
	if _, err := runtime.store.ClaimCommand(ctx, candidates[0], time.Second, "worker", fault.None{}); !errors.Is(err, ErrInvalidState) {
		t.Fatalf("malformed retry policy claim error = %v, want ErrInvalidState", err)
	}
}
