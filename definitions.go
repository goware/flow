package flow

import (
	"errors"
	"fmt"
	"time"

	"github.com/goware/flow/internal/definition"
	"github.com/goware/flow/internal/durable"
)

const (
	defaultQueue         = "default"
	minimumRecoveryLease = 30 * time.Millisecond
)

type Command[A, R any] struct {
	def      *definition.Command
	defaults commandDefaults
	err      error
}

type Event[T any] struct {
	def *definition.Event
	err error
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
	recoveryLease  time.Duration
	queue          string
}

type commandOptionState struct {
	defaults         commandDefaults
	retryConfigured  bool
	timeoutSet       bool
	recoveryLeaseSet bool
	queueSet         bool
	errs             []error
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
	command.err = errors.Join(definition.ValidateBase(base),
		durable.PostgresInteger("command version", version, 1, durable.PostgresIntegerMax), errors.Join(state.errs...))
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

// Queue returns the command's normalized delivery queue. It returns an empty
// string for a zero or invalid command definition.
func (c Command[A, R]) Queue() string {
	if c.def == nil || c.err != nil {
		return ""
	}
	return c.defaults.queue
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
		if timeout <= 0 {
			state.errs = append(state.errs, errors.New("attempt timeout must be positive"))
			return
		}
		normalized, _, err := durable.CeilMilliseconds("attempt timeout", timeout)
		if err != nil {
			state.errs = append(state.errs, err)
			return
		}
		state.defaults.attemptTimeout = normalized
	})
}

// WithRecoveryLease controls how soon another worker may retry this command
// after lease renewal stops. A shorter lease can permit concurrent duplicate
// handler execution, so use it only when repeating the worker is safe. Attempt
// fencing still permits only the current owner to durably settle. This setting
// is durable command identity; change the command version when changing it.
// It is independent of WithTimeout, which bounds one handler invocation.
func WithRecoveryLease(lease time.Duration) CommandOption {
	return commandOptionFunc(func(state *commandOptionState) {
		if state.recoveryLeaseSet {
			state.errs = append(state.errs, errors.New("recovery lease configured more than once"))
			return
		}
		state.recoveryLeaseSet = true
		if lease <= 0 {
			state.errs = append(state.errs, errors.New("recovery lease must be positive"))
			return
		}
		normalized, _, err := durable.CeilMilliseconds("recovery lease", lease)
		if err != nil {
			state.errs = append(state.errs, err)
			return
		}
		if normalized < minimumRecoveryLease {
			state.errs = append(state.errs, fmt.Errorf("recovery lease must be at least %s", minimumRecoveryLease))
			return
		}
		state.defaults.recoveryLease = normalized
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
