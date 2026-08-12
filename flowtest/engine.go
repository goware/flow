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
	info   flow.CommandInfo
	events []testengine.EventInput
	errs   []error
}

type workerOptionFunc func(*workerOptions)

func (f workerOptionFunc) applyWorker(options *workerOptions) { f(options) }

func WithCommandInfo(info flow.CommandInfo) WorkerOption {
	return workerOptionFunc(func(options *workerOptions) { options.info = info })
}

// WithEvent supplies one exact declared event input to a database-free worker
// decision. It uses Flow's production canonical payload encoding.
func WithEvent[T any](event flow.Event[T], key string, payload T) WorkerOption {
	return workerOptionFunc(func(options *workerOptions) {
		encoded, err := canonical.Marshal(payload, 64<<10)
		if err != nil {
			options.errs = append(options.errs, fmt.Errorf("encode event %s/%s: %w", event.Name(), key, err))
			return
		}
		if event.Name() == "" || key == "" {
			options.errs = append(options.errs, errors.New("event input requires a valid event and key"))
			return
		}
		identity := event.Name() + "\x00" + key
		for _, prior := range options.events {
			if prior.Name+"\x00"+prior.Key != identity {
				continue
			}
			if bytes.Equal(prior.Payload, encoded.Bytes) {
				return
			}
			options.errs = append(options.errs, fmt.Errorf("conflicting event input %s/%s", event.Name(), key))
			return
		}
		if len(options.events) >= 256 {
			options.errs = append(options.errs, errors.New("event input limit exceeded"))
			return
		}
		options.events = append(options.events, testengine.EventInput{
			Name: event.Name(), Key: key, Position: int64(len(options.events) + 1), Payload: encoded.BytesCopy(),
		})
	})
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
	if err := errors.Join(options.errs...); err != nil {
		return WorkerResult[R]{}, fmt.Errorf("flowtest: worker options: %w", err)
	}
	encoded, err := canonical.Marshal(args, 256<<10)
	if err != nil {
		return WorkerResult[R]{}, fmt.Errorf("flowtest: encode worker arguments: %w", err)
	}
	result, err := testengine.Invoke(registration, testengine.Request{Operation: testengine.Worker, Context: ctx,
		Args: encoded.BytesCopy(), Info: bridgeInfo(options.info), EventInputs: options.events})
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

type DirectResult[R any] struct {
	Result   R
	Commands map[string]json.RawMessage
	Events   []StagedEvent
}

// RunDirect recursively executes a closed tree of successful worker
// decisions. Resolver supplies registrations for staged sub-command name/version.
func RunDirect[A, R any](ctx context.Context, root flow.Registration, args A, maxCommands int,
	resolver func(name string, version int) (flow.Registration, bool)) (DirectResult[R], error) {
	if maxCommands < 0 {
		return DirectResult[R]{}, errors.New("flowtest: command ceiling must not be negative")
	}
	if maxCommands == 0 {
		maxCommands = 1000
	}
	rootArgs, err := canonical.Marshal(args, 256<<10)
	if err != nil {
		return DirectResult[R]{}, err
	}
	type item struct {
		key   string
		reg   flow.Registration
		args  json.RawMessage
		waits []testengine.EventWait
		root  bool
	}
	queue := []item{{key: "root", reg: root, args: rootArgs.BytesCopy(), root: true}}
	seen := map[string]struct{}{"root": {}}
	output := DirectResult[R]{Commands: make(map[string]json.RawMessage)}
	seenEvents := make(map[string]testengine.EventInput)
	for len(queue) != 0 {
		readyIndex := -1
		for index, candidate := range queue {
			ready := true
			for _, wait := range candidate.waits {
				if _, exists := seenEvents[wait.Name+"\x00"+wait.Key]; !exists {
					ready = false
					break
				}
			}
			if ready {
				readyIndex = index
				break
			}
		}
		if readyIndex < 0 {
			return DirectResult[R]{}, errors.New("flowtest: command tree is waiting for an event that was not emitted")
		}
		current := queue[readyIndex]
		queue = append(queue[:readyIndex], queue[readyIndex+1:]...)
		inputs := make([]testengine.EventInput, 0, len(current.waits))
		for _, wait := range current.waits {
			inputs = append(inputs, seenEvents[wait.Name+"\x00"+wait.Key])
		}
		decision, err := testengine.Invoke(current.reg, testengine.Request{Operation: testengine.Worker, Context: ctx,
			Args: current.args, Info: testengine.Info{CommandKey: current.key}, EventInputs: inputs})
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
			seenEvents[identity] = testengine.EventInput{
				Name: event.Name, Key: event.Key, Position: int64(len(output.Events) + 1), Payload: append([]byte(nil), event.Payload...),
			}
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
			queue = append(queue, item{key: child.Key, reg: registration, args: child.Args, waits: child.Waits})
		}
	}
	return output, nil
}

func bridgeInfo(info flow.CommandInfo) testengine.Info {
	return testengine.Info{RunID: string(info.RunID), RunKey: info.RunKey, Definition: info.Definition,
		CommandID: string(info.CommandID), CommandKey: info.CommandKey,
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
