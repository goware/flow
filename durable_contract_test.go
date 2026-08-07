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
	"github.com/goware/flow/internal/fault"
	retrypolicy "github.com/goware/flow/internal/retry"
	"github.com/goware/flow/internal/store"
	"github.com/goware/flow/internal/testpg"
)

func TestPublicDurableDurationsRequireExactMilliseconds(t *testing.T) {
	t.Parallel()

	fractional := time.Millisecond + time.Nanosecond
	if command := DefineCommand[None, None]("duration.timeout", 1, WithTimeout(fractional)); !errors.Is(command.err, ErrInvalid) {
		t.Fatalf("fractional timeout error = %v, want ErrInvalid", command.err)
	}
	if policy := RetryFor(fractional); !errors.Is(validateRetryPolicy(policy), ErrInvalid) {
		t.Fatalf("fractional retry elapsed error = %v, want ErrInvalid", validateRetryPolicy(policy))
	}
	if policy := RetryFor(time.Second).Backoff(fractional); !errors.Is(validateRetryPolicy(policy), ErrInvalid) {
		t.Fatalf("fractional retry backoff error = %v, want ErrInvalid", validateRetryPolicy(policy))
	}
	input, err := canonical.Marshal(None{}, 0)
	if err != nil {
		t.Fatal(err)
	}
	for name, option := range map[string]ExecutionOption{
		"deadline": WithExecutionDeadline(fractional),
		"delay":    WithStartDelay(fractional),
	} {
		if _, _, _, err := prepareStartOptions("duration.start", 1, "key", input, option); !errors.Is(err, ErrInvalid) {
			t.Fatalf("fractional %s error = %v, want ErrInvalid", name, err)
		}
	}
	event := DefineEvent[None]("duration.gate")
	if _, _, _, err := prepareStartOptions("duration.start", 1, "key", input,
		WaitFor(event, "ready"), Within(fractional)); !errors.Is(err, ErrInvalid) {
		t.Fatalf("fractional within error = %v, want ErrInvalid", err)
	}
	nodeDelay := &Work[None]{scope: &scopeState{}}
	Execute(nodeDelay, "child", DefineCommand[None, None]("duration.child.delay", 1), None{}).Delay(fractional)
	if !errors.Is(nodeDelay.scope.firstError, ErrInvalid) {
		t.Fatalf("fractional node delay error = %v, want ErrInvalid", nodeDelay.scope.firstError)
	}
	nodeWithin := &Work[None]{scope: &scopeState{}}
	Execute(nodeWithin, "child", DefineCommand[None, None]("duration.child.within", 1), None{}).
		WaitFor(event, "ready").Within(fractional)
	if !errors.Is(nodeWithin.scope.firstError, ErrInvalid) {
		t.Fatalf("fractional node within error = %v, want ErrInvalid", nodeWithin.scope.firstError)
	}
	maximumExact := time.Duration(math.MaxInt64/int64(time.Millisecond)) * time.Millisecond
	if command := DefineCommand[None, None]("duration.maximum", 1, WithTimeout(maximumExact)); command.err != nil {
		t.Fatalf("maximum exact duration rejected: %v", command.err)
	}
}

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
		WithMaxCommandsPerExecution(tooLarge).applyRuntime(&options)
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
		Args: args, Required: true, Queue: "default", RetryPolicy: policy,
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
	runtime, err := New(database.DB, WithSchema(database.Schema), WithMaxCommandsPerExecution(math.MaxInt32))
	if err != nil {
		t.Fatal(err)
	}
	command := DefineCommand[None, None]("integer.persisted", math.MaxInt32)
	execution, err := command.With(runtime).Execute(ctx, "maximum", None{})
	if err != nil {
		t.Fatal(err)
	}
	if execution.Version != math.MaxInt32 || execution.MaxCommands != math.MaxInt32 {
		t.Fatalf("persisted integer boundaries = version %d max %d", execution.Version, execution.MaxCommands)
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
	execution, err := command.With(runtime).Execute(ctx, "malformed", None{})
	if err != nil {
		t.Fatal(err)
	}
	candidates, err := runtime.store.ProbeCommands(ctx, []store.CommandKind{{Name: command.Name(), Version: command.Version()}}, 1)
	if err != nil || len(candidates) != 1 {
		t.Fatalf("probe command = %#v, %v", candidates, err)
	}
	if _, err := database.DB.Conn.Exec(ctx, `UPDATE `+quoteIdentifier(database.Schema)+`.flow_commands
		SET retry_policy=$2 WHERE execution_id=$1`, execution.ID, []byte("not canonical JSON")); err != nil {
		t.Fatal(err)
	}
	if _, err := runtime.store.ClaimCommand(ctx, candidates[0], time.Second, "worker", fault.None{}); !errors.Is(err, ErrInvalidState) {
		t.Fatalf("malformed retry policy claim error = %v, want ErrInvalidState", err)
	}
}
