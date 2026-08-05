package flow

import (
	"errors"
	"fmt"
	"time"

	"github.com/goware/flow/internal/definition"
)

const defaultQueue = "default"

type Command[A, R any] struct {
	def      *definition.Command
	defaults commandDefaults
	client   Client
	err      error
}

type Event[T any] struct {
	def *definition.Event
	err error
}

type Coordinator[S any] struct {
	def      *definition.Coordinator
	handlers []coordinatorHandler[S]
	client   Client
	err      error
}

type EventRef interface {
	flowEventRef() eventReference
}

type CommandOption interface {
	applyCommand(*commandOptionState)
}

type commandDefaults struct {
	retryPolicy    RetryPolicy
	attemptTimeout time.Duration
	queue          string
}

type commandOptionState struct {
	defaults        commandDefaults
	retryConfigured bool
	timeoutSet      bool
	queueSet        bool
	errs            []error
}

type commandOptionFunc func(*commandOptionState)

func (f commandOptionFunc) applyCommand(state *commandOptionState) { f(state) }

func DefineCommand[A, R any](name string, version int, opts ...CommandOption) Command[A, R] {
	base := definition.Base{Kind: definition.CommandKind, Name: name, Version: version}
	argsCodec := definition.NewCodec[A]()
	resultCodec := definition.NewCodec[R]()
	command := Command[A, R]{
		def: &definition.Command{Base: base, Args: argsCodec, Result: resultCodec},
	}
	state := commandOptionState{defaults: commandDefaults{
		retryPolicy: defaultRetryPolicy(),
		queue:       defaultQueue,
	}}
	for _, option := range opts {
		if option == nil {
			state.errs = append(state.errs, errors.New("nil command option"))
			continue
		}
		option.applyCommand(&state)
	}
	command.defaults = state.defaults
	command.err = errors.Join(definition.ValidateBase(base), errors.Join(state.errs...))
	return command
}

func DefineEvent[T any](name string) Event[T] {
	event := Event[T]{def: &definition.Event{
		Name:      name,
		Namespace: "application",
		Payload:   definition.NewCodec[T](),
	}}
	event.err = definition.ValidateName(name)
	return event
}

func DefineCoordinator[S any](name string, version int, handlers ...Handler[S]) Coordinator[S] {
	base := definition.Base{Kind: definition.CoordinatorKind, Name: name, Version: version}
	coordinator := Coordinator[S]{def: &definition.Coordinator{
		Base:  base,
		State: definition.NewCodec[S](),
	}}
	coordinator.err = definition.ValidateBase(base)

	seen := make(map[string]struct{}, len(handlers))
	for _, handler := range handlers {
		if handler == nil {
			coordinator.err = errors.Join(coordinator.err, errors.New("nil coordinator handler"))
			continue
		}
		value := handler.flowCoordinatorHandler()
		coordinator.handlers = append(coordinator.handlers, value)
		coordinator.err = errors.Join(coordinator.err, value.err)
		key := value.selector.key()
		if _, exists := seen[key]; exists {
			coordinator.err = errors.Join(coordinator.err, fmt.Errorf("duplicate coordinator handler %s", key))
		}
		seen[key] = struct{}{}
	}
	return coordinator
}

func (c Command[A, R]) Name() string {
	if c.def == nil {
		return ""
	}
	return c.def.Name
}

func (c Command[A, R]) Version() int {
	if c.def == nil {
		return 0
	}
	return c.def.Version
}

func (c Command[A, R]) With(client Client) Command[A, R] {
	copy := c
	copy.client = client
	return copy
}

func (e Event[T]) flowEventRef() eventReference {
	if e.def == nil {
		return eventReference{}
	}
	return eventReference{namespace: e.def.Namespace, name: e.def.Name}
}

func (e Event[T]) Name() string {
	if e.def == nil {
		return ""
	}
	return e.def.Name
}

func (c Coordinator[S]) Name() string {
	if c.def == nil {
		return ""
	}
	return c.def.Name
}

func (c Coordinator[S]) Version() int {
	if c.def == nil {
		return 0
	}
	return c.def.Version
}

func (c Coordinator[S]) With(client Client) Coordinator[S] {
	copy := c
	copy.client = client
	copy.handlers = append([]coordinatorHandler[S](nil), c.handlers...)
	return copy
}

func WithRetry(policy RetryPolicy) CommandOption {
	return commandOptionFunc(func(state *commandOptionState) {
		if state.retryConfigured {
			state.errs = append(state.errs, errors.New("retry policy configured more than once"))
			return
		}
		state.retryConfigured = true
		state.defaults.retryPolicy = cloneRetryPolicy(policy)
		state.errs = appendValidation(state.errs, validateRetryPolicy(policy))
	})
}

func WithTimeout(timeout time.Duration) CommandOption {
	return commandOptionFunc(func(state *commandOptionState) {
		if state.timeoutSet {
			state.errs = append(state.errs, errors.New("attempt timeout configured more than once"))
			return
		}
		state.timeoutSet = true
		if timeout < time.Millisecond {
			state.errs = append(state.errs, errors.New("attempt timeout must be at least one millisecond"))
			return
		}
		state.defaults.attemptTimeout = timeout
	})
}

func WithQueue(queue string) CommandOption {
	return commandOptionFunc(func(state *commandOptionState) {
		if state.queueSet {
			state.errs = append(state.errs, errors.New("queue configured more than once"))
			return
		}
		state.queueSet = true
		if err := definition.ValidateName(queue); err != nil {
			state.errs = append(state.errs, fmt.Errorf("invalid queue: %w", err))
			return
		}
		state.defaults.queue = queue
	})
}

type eventReference struct {
	namespace string
	name      string
}

func appendValidation(errs []error, err error) []error {
	if err != nil {
		return append(errs, err)
	}
	return errs
}
