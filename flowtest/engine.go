package flowtest

import (
	"bytes"
	"context"
	"encoding/json"
	"errors"
	"fmt"
	"time"

	"github.com/goware/flow"
	"github.com/goware/flow/internal/canonical"
	"github.com/goware/flow/internal/testengine"
)

type StagedCommand struct {
	Key        string
	Name       string
	Version    int
	Args       json.RawMessage
	Required   bool
	StartAfter time.Duration
	Waits      []EventWait
	Within     time.Duration
}

type EventWait struct {
	Name string
	Key  string
}

type StagedEvent struct {
	Name    string
	Key     string
	Payload json.RawMessage
}

type WorkerOption interface{ applyWorker(*workerOptions) }

type workerOptions struct {
	info flow.CommandInfo
}

type workerOptionFunc func(*workerOptions)

func (f workerOptionFunc) applyWorker(options *workerOptions) { f(options) }

func WithCommandInfo(info flow.CommandInfo) WorkerOption {
	return workerOptionFunc(func(options *workerOptions) { options.info = info })
}

type WorkerResult[R any] struct {
	Result   R
	Err      error
	Panicked bool
	Commands []StagedCommand
	Events   []StagedEvent
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
	result, err := testengine.Invoke(registration, testengine.Request{Operation: testengine.Worker, Context: ctx,
		Args: encoded.BytesCopy(), Info: bridgeInfo(options.info)})
	if err != nil {
		return WorkerResult[R]{}, err
	}
	output := WorkerResult[R]{Err: result.HandlerError, Panicked: result.Panicked,
		Commands: publicCommands(result.Commands), Events: publicEvents(result.Events)}
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
	return CoordinatorDelivery{kind: "event", namespace: "application", name: event.Name(),
		key: key, position: position, recordedAt: recordedAt, payload: encoded.BytesCopy()}
}

func DeliverOutcome[A, R any](position int64, command flow.Command[A, R], key string, recordedAt time.Time,
	outcome flow.Outcome[R]) CoordinatorDelivery {
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
	Commands       []StagedCommand
	Events         []StagedEvent
	Terminal       string
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
		DeliveryName: delivery.name, DeliveryCommandVersion: delivery.version, DeliveryKey: delivery.key,
		DeliveryPosition: delivery.position, DeliveryRecordedAt: delivery.recordedAt, DeliveryPayload: delivery.payload,
		DeliveryStatus: delivery.status, DeliveryResult: delivery.result,
		DeliveryFailureCode: delivery.failureCode, DeliveryFailureMessage: delivery.failureMessage})
	if err != nil {
		return CoordinatorResult[S]{}, err
	}
	output := CoordinatorResult[S]{Err: result.HandlerError, Panicked: result.Panicked,
		Commands: publicCommands(result.Commands), Events: publicEvents(result.Events), Terminal: result.Terminal,
		TerminalReason: result.TerminalReason}
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
	seenEvents := make(map[string]StagedEvent)
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
		for _, event := range publicEvents(decision.Events) {
			identity := event.Name + "\x00" + event.Key
			if prior, exists := seenEvents[identity]; exists {
				if bytes.Equal(prior.Payload, event.Payload) {
					continue
				}
				return DirectResult[R]{}, fmt.Errorf("flowtest: conflicting event identity %s/%s", event.Name, event.Key)
			}
			seenEvents[identity] = event
			output.Events = append(output.Events, event)
		}
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

func publicCommands(values []testengine.StagedCommand) []StagedCommand {
	result := make([]StagedCommand, len(values))
	for i, value := range values {
		result[i] = StagedCommand{Key: value.Key, Name: value.Name, Version: value.Version,
			Args: value.Args, Required: value.Required, StartAfter: value.StartAfter,
			Within: value.Within, Waits: make([]EventWait, len(value.Waits))}
		for j, wait := range value.Waits {
			result[i].Waits[j] = EventWait{Name: wait.Name, Key: wait.Key}
		}
	}
	return result
}

func publicEvents(values []testengine.StagedEvent) []StagedEvent {
	result := make([]StagedEvent, len(values))
	for i, value := range values {
		result[i] = StagedEvent{Name: value.Name, Key: value.Key,
			Payload: append(json.RawMessage(nil), value.Payload...)}
	}
	return result
}
