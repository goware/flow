package flowtest

import (
	"context"
	"encoding/json"
	"errors"
	"fmt"
	"reflect"
	"time"

	"github.com/google/uuid"
	"github.com/goware/flow"
	"github.com/goware/flow/internal/canonical"
	"github.com/goware/flow/internal/testengine"
)

type StagedEvent struct {
	Name    string
	Version int
	Key     string
	Payload json.RawMessage
}

type StagedCommand struct {
	Key        string
	Name       string
	Version    int
	Args       json.RawMessage
	Required   bool
	StartAfter time.Duration
}

type Dependency struct {
	value testengine.Dependency
	err   error
}

func Succeeded[A, R any](key string, command flow.Command[A, R], result R) Dependency {
	encoded, err := canonical.Marshal(result, 256<<10)
	return Dependency{value: testengine.Dependency{Key: key, Name: command.Name(), Version: command.Version(),
		Status: string(flow.StatusSucceeded), Result: encoded.BytesCopy()}, err: err}
}

func Failed[A, R any](key string, command flow.Command[A, R], code, message string) Dependency {
	return Dependency{value: testengine.Dependency{Key: key, Name: command.Name(), Version: command.Version(),
		Status: string(flow.StatusFailed), FailureCode: code, FailureMessage: message}}
}

type WorkerOption interface{ applyWorker(*workerOptions) }

type workerOptions struct {
	info         flow.CommandInfo
	dependencies []Dependency
}

type workerOptionFunc func(*workerOptions)

func (f workerOptionFunc) applyWorker(options *workerOptions) { f(options) }

func WithCommandInfo(info flow.CommandInfo) WorkerOption {
	return workerOptionFunc(func(options *workerOptions) { options.info = info })
}

func WithDependencies(values ...Dependency) WorkerOption {
	copy := append([]Dependency(nil), values...)
	return workerOptionFunc(func(options *workerOptions) { options.dependencies = copy })
}

type WorkerResult[R any] struct {
	Result   R
	Err      error
	Panicked bool
	Events   []StagedEvent
	Commands []StagedCommand
}

// RunWorker invokes the registered production worker and production staged
// decision recorder without PostgreSQL.
func RunWorker[A, R any](ctx context.Context, registration flow.Registration, args A, opts ...WorkerOption) (WorkerResult[R], error) {
	options := workerOptions{}
	for _, option := range opts {
		if option == nil {
			return WorkerResult[R]{}, errors.New("flowtest: nil worker option")
		}
		option.applyWorker(&options)
	}
	encoded, err := canonical.Marshal(args, 256<<10)
	if err != nil {
		return WorkerResult[R]{}, fmt.Errorf("flowtest: encode worker arguments: %w", err)
	}
	dependencies := make([]testengine.Dependency, len(options.dependencies))
	for index, dependency := range options.dependencies {
		if dependency.err != nil {
			return WorkerResult[R]{}, fmt.Errorf("flowtest: encode dependency: %w", dependency.err)
		}
		dependencies[index] = dependency.value
	}
	result, err := testengine.Invoke(registration, testengine.Request{Operation: testengine.Worker, Context: ctx,
		Args: encoded.BytesCopy(), Info: bridgeInfo(options.info), Dependencies: dependencies})
	if err != nil {
		return WorkerResult[R]{}, err
	}
	output := WorkerResult[R]{Err: result.HandlerError, Panicked: result.Panicked,
		Events: publicEvents(result.Events), Commands: publicCommands(result.Commands)}
	if result.HandlerError == nil && !result.Panicked && len(result.Value) != 0 {
		if err := canonical.Decode(result.Value, &output.Result); err != nil {
			return WorkerResult[R]{}, fmt.Errorf("flowtest: decode worker result: %w", err)
		}
	}
	return output, nil
}

// RunCommit invokes the production declared commit function with a caller's
// transaction double. A registration without a commit function is a no-op.
func RunCommit[A, R any](ctx context.Context, registration flow.Registration, tx flow.Tx,
	args A, result R, info flow.CommandInfo) error {
	argsBytes, err := canonical.Marshal(args, 256<<10)
	if err != nil {
		return err
	}
	resultBytes, err := canonical.Marshal(result, 256<<10)
	if err != nil {
		return err
	}
	_, err = testengine.Invoke(registration, testengine.Request{Operation: testengine.Commit, Context: ctx,
		Args: argsBytes.BytesCopy(), Result: resultBytes.BytesCopy(), Info: bridgeInfo(info), Tx: tx})
	return err
}

type PlanCommand struct {
	value testengine.PlanCommand
	err   error
}

func SucceededCommand[A, R any](key string, command flow.Command[A, R], result R) PlanCommand {
	encoded, err := canonical.Marshal(result, 256<<10)
	return PlanCommand{value: testengine.PlanCommand{ID: uuid.NewString(), Key: key, Name: command.Name(),
		Version: command.Version(), Origin: "plan", State: string(flow.StatusSucceeded), Result: encoded.BytesCopy(),
	}, err: err}
}

func FailedCommand[A, R any](key string, command flow.Command[A, R], code, message string) PlanCommand {
	return PlanCommand{value: testengine.PlanCommand{ID: uuid.NewString(), Key: key, Name: command.Name(),
		Version: command.Version(), Origin: "plan", State: string(flow.StatusFailed), FailureCode: code, FailureMessage: message}}
}

type EventRef struct {
	Namespace string
	Name      string
	Version   int
}

func EventReference[T any](event flow.Event[T]) EventRef {
	return EventRef{Namespace: "application", Name: event.Name(), Version: event.Version()}
}

type PlanEvent struct {
	value testengine.PlanEvent
	err   error
}

func Fact[T any](position int64, event flow.Event[T], key string, payload T) PlanEvent {
	encoded, err := canonical.Marshal(payload, 64<<10)
	return PlanEvent{value: testengine.PlanEvent{ID: uuid.NewString(), Position: position, Namespace: "application",
		Name: event.Name(), Version: event.Version(), Key: key, Payload: encoded.BytesCopy()}, err: err}
}

type PlanWorld struct {
	ExecutionID    flow.ExecutionID
	Status         string
	MaxCommands    int
	JournalThrough int64
	Commands       []PlanCommand
	Events         []PlanEvent
	KnownEvents    []EventRef
}

type Declaration struct {
	Key          string
	Name         string
	Version      int
	Args         json.RawMessage
	Required     bool
	Dependencies [][]string
	Waits        []string
	Within       time.Duration
	Delay        time.Duration
}

type Read struct {
	Kind         string
	Identity     string
	Availability string
}

type PlanResult struct {
	Declarations []Declaration
	Reads        []Read
	WaitingReads int
}

// RunPlan evaluates the production pure plan recorder over one synthetic
// immutable snapshot.
func RunPlan[A any](plan flow.PlanDef[A], args A, world PlanWorld) (PlanResult, error) {
	encoded, err := canonical.Marshal(args, 256<<10)
	if err != nil {
		return PlanResult{}, err
	}
	commands := make([]testengine.PlanCommand, len(world.Commands))
	for i, command := range world.Commands {
		if command.err != nil {
			return PlanResult{}, fmt.Errorf("flowtest: encode plan command: %w", command.err)
		}
		commands[i] = command.value
	}
	events := make([]testengine.PlanEvent, len(world.Events))
	for i, event := range world.Events {
		if event.err != nil {
			return PlanResult{}, fmt.Errorf("flowtest: encode plan event: %w", event.err)
		}
		events[i] = event.value
	}
	knownEvents := make([]testengine.EventSelector, len(world.KnownEvents))
	for i, event := range world.KnownEvents {
		knownEvents[i] = testengine.EventSelector{Namespace: event.Namespace, Name: event.Name, Version: event.Version}
	}
	result, err := testengine.Invoke(plan, testengine.Request{Operation: testengine.Plan, Args: encoded.BytesCopy(),
		ExecutionID: string(world.ExecutionID), Status: world.Status, MaxCommands: world.MaxCommands,
		JournalThrough: world.JournalThrough, Commands: commands, Events: events, KnownEvents: knownEvents})
	if err != nil {
		return PlanResult{}, err
	}
	output := PlanResult{WaitingReads: result.WaitingReads, Declarations: make([]Declaration, len(result.Declarations)), Reads: make([]Read, len(result.Reads))}
	for i, declaration := range result.Declarations {
		output.Declarations[i] = Declaration{Key: declaration.Key, Name: declaration.Name, Version: declaration.Version,
			Args: declaration.Args, Required: declaration.Required, Dependencies: declaration.Dependencies,
			Waits: declaration.Waits, Within: declaration.Within, Delay: declaration.Delay}
	}
	for i, read := range result.Reads {
		output.Reads[i] = Read(read)
	}
	return output, nil
}

// Simulate returns the declaration/read projection after every supplied
// immutable history prefix.
func Simulate[A any](plan flow.PlanDef[A], args A, worlds ...PlanWorld) ([]PlanResult, error) {
	result := make([]PlanResult, len(worlds))
	for index, world := range worlds {
		value, err := RunPlan(plan, args, world)
		if err != nil {
			return nil, fmt.Errorf("flowtest: simulate prefix %d: %w", index, err)
		}
		result[index] = value
	}
	return result, nil
}

func AssertPlanDeterministic[A any](t TestingT, plan flow.PlanDef[A], args A, world PlanWorld) PlanResult {
	t.Helper()
	first, err := RunPlan(plan, args, world)
	if err != nil {
		t.Fatalf("first plan evaluation: %v", err)
	}
	second, err := RunPlan(plan, args, world)
	if err != nil {
		t.Fatalf("second plan evaluation: %v", err)
	}
	if !reflect.DeepEqual(first, second) {
		t.Fatalf("plan evaluation is nondeterministic:\nfirst: %#v\nsecond: %#v", first, second)
	}
	return first
}

type CoordinatorDelivery struct {
	kind           string
	namespace      string
	name           string
	version        int
	key            string
	position       int64
	recordedAt     time.Time
	payload        json.RawMessage
	status         string
	result         json.RawMessage
	failureCode    string
	failureMessage string
}

func Start() CoordinatorDelivery { return CoordinatorDelivery{kind: "start"} }

func DeliverEvent[T any](position int64, event flow.Event[T], key string, recordedAt time.Time, payload T) CoordinatorDelivery {
	encoded, _ := canonical.Marshal(payload, 64<<10)
	return CoordinatorDelivery{kind: "event", namespace: "application", name: event.Name(), version: event.Version(),
		key: key, position: position, recordedAt: recordedAt, payload: encoded.BytesCopy()}
}

func DeliverOutcome[A, R any](position int64, command flow.Command[A, R], key string, recordedAt time.Time,
	outcome flow.CommandOutcome[R]) CoordinatorDelivery {
	result, _ := canonical.Marshal(outcome.Result, 256<<10)
	delivery := CoordinatorDelivery{kind: "outcome", namespace: "command_terminal", name: command.Name(),
		version: command.Version(), key: key, position: position, recordedAt: recordedAt,
		status: string(outcome.Status), result: result.BytesCopy()}
	if outcome.Failure != nil {
		delivery.failureCode, delivery.failureMessage = outcome.Failure.Code, outcome.Failure.Message
	}
	return delivery
}

type CoordinatorResult[S any] struct {
	State          S
	Err            error
	Panicked       bool
	Events         []StagedEvent
	Commands       []StagedCommand
	Terminal       string
	ResultRef      string
	TerminalReason string
}

func RunCoordinator[S any](ctx context.Context, coordinator flow.Coordinator[S], state S,
	delivery CoordinatorDelivery) (CoordinatorResult[S], error) {
	stateBytes, err := canonical.Marshal(state, 256<<10)
	if err != nil {
		return CoordinatorResult[S]{}, err
	}
	result, err := testengine.Invoke(coordinator, testengine.Request{Operation: testengine.Coordinator, Context: ctx,
		State: stateBytes.BytesCopy(), DeliveryKind: delivery.kind, DeliveryNamespace: delivery.namespace,
		DeliveryName: delivery.name, DeliveryVersion: delivery.version, DeliveryKey: delivery.key,
		DeliveryPosition: delivery.position, DeliveryRecordedAt: delivery.recordedAt, DeliveryPayload: delivery.payload,
		DeliveryStatus: delivery.status, DeliveryResult: delivery.result,
		DeliveryFailureCode: delivery.failureCode, DeliveryFailureMessage: delivery.failureMessage})
	if err != nil {
		return CoordinatorResult[S]{}, err
	}
	output := CoordinatorResult[S]{Err: result.HandlerError, Panicked: result.Panicked,
		Events: publicEvents(result.Events), Commands: publicCommands(result.Commands), Terminal: result.Terminal,
		ResultRef: result.ResultRef, TerminalReason: result.TerminalReason}
	if err := canonical.Decode(result.State, &output.State); err != nil {
		return CoordinatorResult[S]{}, err
	}
	return output, nil
}

type DirectResult[R any] struct {
	Result   R
	Commands map[string]json.RawMessage
	Events   []StagedEvent
}

// RunDirect recursively executes a closed tree of successful worker
// decisions. Resolver supplies registrations for staged child name/version.
func RunDirect[A, R any](ctx context.Context, root flow.Registration, args A, maxCommands int,
	resolver func(name string, version int) (flow.Registration, bool)) (DirectResult[R], error) {
	if maxCommands == 0 {
		maxCommands = 1000
	}
	rootArgs, err := canonical.Marshal(args, 256<<10)
	if err != nil {
		return DirectResult[R]{}, err
	}
	type item struct {
		key  string
		reg  flow.Registration
		args json.RawMessage
		root bool
	}
	queue := []item{{key: "root", reg: root, args: rootArgs.BytesCopy(), root: true}}
	seen := map[string]struct{}{"root": {}}
	output := DirectResult[R]{Commands: make(map[string]json.RawMessage)}
	for len(queue) != 0 {
		current := queue[0]
		queue = queue[1:]
		decision, err := testengine.Invoke(current.reg, testengine.Request{Operation: testengine.Worker, Context: ctx,
			Args: current.args, Info: testengine.Info{CommandKey: current.key}})
		if err != nil {
			return DirectResult[R]{}, err
		}
		if decision.HandlerError != nil {
			return DirectResult[R]{}, decision.HandlerError
		}
		if decision.Panicked {
			return DirectResult[R]{}, errors.New("flowtest: worker panicked")
		}
		output.Commands[current.key] = append([]byte(nil), decision.Value...)
		output.Events = append(output.Events, publicEvents(decision.Events)...)
		if current.root {
			if err := canonical.Decode(decision.Value, &output.Result); err != nil {
				return DirectResult[R]{}, err
			}
		}
		for _, child := range decision.Commands {
			if _, exists := seen[child.Key]; exists {
				return DirectResult[R]{}, fmt.Errorf("flowtest: duplicate command key %q", child.Key)
			}
			if len(seen) >= maxCommands {
				return DirectResult[R]{}, errors.New("flowtest: command ceiling exceeded")
			}
			registration, ok := resolver(child.Name, child.Version)
			if !ok {
				return DirectResult[R]{}, fmt.Errorf("flowtest: no worker for %s/%d", child.Name, child.Version)
			}
			seen[child.Key] = struct{}{}
			queue = append(queue, item{key: child.Key, reg: registration, args: child.Args})
		}
	}
	return output, nil
}

func bridgeInfo(info flow.CommandInfo) testengine.Info {
	return testengine.Info{ExecutionID: string(info.ExecutionID), CommandID: string(info.CommandID), CommandKey: info.CommandKey,
		Name: info.Name, Version: info.Version, CreatedAt: info.CreatedAt, BudgetStartedAt: info.BudgetStartedAt,
		Attempt: info.Attempt, AttemptStartedAt: info.AttemptStartedAt}
}

func publicEvents(values []testengine.StagedEvent) []StagedEvent {
	result := make([]StagedEvent, len(values))
	for i, value := range values {
		result[i] = StagedEvent{Name: value.Name, Version: value.Version, Key: value.Key, Payload: value.Payload}
	}
	return result
}

func publicCommands(values []testengine.StagedCommand) []StagedCommand {
	result := make([]StagedCommand, len(values))
	for i, value := range values {
		result[i] = StagedCommand{Key: value.Key, Name: value.Name, Version: value.Version,
			Args: value.Args, Required: value.Required, StartAfter: value.StartAfter}
	}
	return result
}
