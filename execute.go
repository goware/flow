package flow

import (
	"bytes"
	"context"
	"encoding/json"
	"errors"
	"sort"
	"strings"
	"time"
	"unicode/utf8"

	"github.com/goware/flow/internal/canonical"
	"github.com/goware/flow/internal/definition"
	"github.com/goware/flow/internal/durable"
	"github.com/goware/flow/internal/fault"
	retrypolicy "github.com/goware/flow/internal/retry"
	"github.com/goware/flow/internal/store"
	"github.com/goware/flow/internal/store/journalcodec"
	"github.com/goware/flow/internal/uuid"
	"github.com/jackc/pgx/v5"
)

const (
	maxCommandArgumentBytes   = 256 << 10
	maxApplicationEventBytes  = journalcodec.MaxApplicationEventPayloadBytes
	maxCommandEventWaits      = 256
	maxExecutionMetadataBytes = 16 << 10
	maxExecutionKeyBytes      = 1024
	maxCommandKeyBytes        = 1024
	defaultExecutionDeadline  = 30 * 24 * time.Hour
)

// ExecutionOption is a sealed command execution option.
type ExecutionOption interface {
	applyExecution(*executionOptions)
}

type executionOptions struct {
	deadline      store.DeadlineSpec
	deadlineSet   bool
	failFast      bool
	failFastSet   bool
	metadata      map[string]string
	metadataSet   bool
	keyScope      string
	keyScopeSet   bool
	startDelay    time.Duration
	startDelaySet bool
	waits         []commandEventWait
	within        time.Duration
	withinSet     bool
	errs          []error
}

type executionOptionFunc func(*executionOptions)

func (f executionOptionFunc) applyExecution(options *executionOptions) { f(options) }

func WithExecutionDeadline(deadline time.Duration) ExecutionOption {
	return executionOptionFunc(func(options *executionOptions) {
		if options.deadlineSet {
			options.errs = append(options.errs, errors.New("execution deadline configured more than once"))
			return
		}
		options.deadlineSet = true
		if deadline <= 0 {
			options.errs = append(options.errs, errors.New("execution deadline must be positive"))
			return
		}
		if _, err := durable.ExactMilliseconds("execution deadline", deadline); err != nil {
			options.errs = append(options.errs, err)
			return
		}
		options.deadline = store.DeadlineSpec{Mode: "duration", Duration: deadline}
	})
}

func WithoutExecutionDeadline() ExecutionOption {
	return executionOptionFunc(func(options *executionOptions) {
		if options.deadlineSet {
			options.errs = append(options.errs, errors.New("execution deadline configured more than once"))
			return
		}
		options.deadlineSet = true
		options.deadline = store.DeadlineSpec{Mode: "none"}
	})
}

func WithMetadata(metadata map[string]string) ExecutionOption {
	return executionOptionFunc(func(options *executionOptions) {
		if options.metadataSet {
			options.errs = append(options.errs, errors.New("metadata configured more than once"))
			return
		}
		options.metadataSet = true
		options.metadata = cloneStringMap(metadata)
	})
}

func WithFailFast(enabled bool) ExecutionOption {
	return executionOptionFunc(func(options *executionOptions) {
		if options.failFastSet {
			options.errs = append(options.errs, errors.New("fail-fast configured more than once"))
			return
		}
		options.failFastSet = true
		options.failFast = enabled
	})
}

// WithLiveKey scopes the execution key to live executions: while a running or
// failing execution holds the key, Execute rediscovers it without comparing
// start identity — a silent, queue-style dedupe no-op — and once that
// execution reaches a terminal status the key is released for a new start.
// Live-keyed starts therefore give at-most-one live execution per key, not
// at-most-one execution ever. Requires a non-empty key.
func WithLiveKey() ExecutionOption {
	return executionOptionFunc(func(options *executionOptions) {
		if options.keyScopeSet {
			options.errs = append(options.errs, errors.New("key scope configured more than once"))
			return
		}
		options.keyScopeSet = true
		options.keyScope = store.KeyScopeLive
	})
}

// WithStartDelay schedules an execution's root command to become deliverable
// after the delay instead of immediately, mirroring Delay for sub-commands.
func WithStartDelay(delay time.Duration) ExecutionOption {
	return executionOptionFunc(func(options *executionOptions) {
		if options.startDelaySet {
			options.errs = append(options.errs, errors.New("start delay configured more than once"))
			return
		}
		options.startDelaySet = true
		if delay < time.Millisecond {
			options.errs = append(options.errs, errors.New("start delay must be at least one millisecond"))
			return
		}
		if _, err := durable.ExactMilliseconds("start delay", delay); err != nil {
			options.errs = append(options.errs, err)
			return
		}
		options.startDelay = delay
	})
}

// WaitFor gates a root command on one exact application event inside the
// execution it creates. Multiple waits are AND conditions. Worker decisions
// use the matching Node.WaitFor method.
func WaitFor(event EventRef, key string) ExecutionOption {
	return executionOptionFunc(func(options *executionOptions) {
		wait, err := makeCommandEventWait(event, key)
		if err != nil {
			options.errs = append(options.errs, err)
			return
		}
		options.waits = addCommandEventWait(options.waits, wait)
	})
}

// Within bounds how long a direct root command waits for its exact events.
// It is valid only when the same start declares at least one WaitFor option.
func Within(duration time.Duration) ExecutionOption {
	return executionOptionFunc(func(options *executionOptions) {
		if duration < time.Millisecond {
			options.errs = append(options.errs, errors.New("within must be at least one millisecond"))
			return
		}
		if _, err := durable.ExactMilliseconds("within", duration); err != nil {
			options.errs = append(options.errs, err)
			return
		}
		if options.withinSet && options.within == duration {
			return
		}
		if options.withinSet {
			options.errs = append(options.errs, errors.New("within configured with different values"))
			return
		}
		options.withinSet, options.within = true, duration
	})
}

func (cmd Command[A, R]) Execute(ctx context.Context, key string, args A, opts ...ExecutionOption) (Execution, error) {
	var definitionError error
	if cmd.def == nil {
		definitionError = errors.New("zero definition")
	}
	if err := errors.Join(cmd.err, definitionError, validateBoundClient(cmd.client)); err != nil {
		return Execution{}, newError(ErrInvalid, "execute", "command", cmd.Name(), err.Error())
	}
	input, err := encodeDefinitionValue(cmd.def.Args, args, maxCommandArgumentBytes, "command arguments")
	if err != nil {
		return Execution{}, err
	}
	client, err := resolveClient(cmd.client)
	if err != nil {
		return Execution{}, err
	}
	options, metadata, fingerprint, err := prepareStartOptions(cmd.Name(), cmd.Version(), key, input, opts...)
	if err != nil {
		return Execution{}, err
	}
	root, err := prepareCommand(uuid.New(), "root", cmd.def, cmd.defaults, input)
	if err != nil {
		return Execution{}, err
	}
	if options.startDelay > 0 {
		root.InitialDelay = options.startDelay
	}
	for _, wait := range options.waits {
		root.Waits = append(root.Waits, store.EventWaitCreate{Name: wait.name, Key: wait.key})
	}
	root.Within = options.within
	root.DeclarationFingerprint, err = commandDeclarationFingerprint(root)
	if err != nil {
		return Execution{}, err
	}
	request := store.StartRequest{
		ID: uuid.New(), DefinitionName: cmd.Name(), DefinitionVersion: cmd.Version(), Key: key,
		KeyScope: options.keyScope, StartFingerprint: fingerprint, Input: input, Metadata: metadata,
		FailFast: options.failFast, Deadline: options.deadline, MaxCommands: client.runtime.maxCommands, Root: &root,
	}
	return executeStart(ctx, client, request)
}

func executeStart(ctx context.Context, client resolvedClient, request store.StartRequest) (Execution, error) {
	var result store.StartResult
	err := client.inTransaction(ctx, func(tx pgx.Tx) error {
		if err := client.runtime.faults.Hit(ctx, fault.IngressBeforeJournal); err != nil {
			return err
		}
		var err error
		result, err = client.runtime.store.StartInTx(ctx, tx, request, client.order)
		return err
	})
	if err != nil {
		return Execution{}, err
	}
	exec, err := executionFromStore(result.Row)
	if err != nil {
		return Execution{}, err
	}
	exec.Created = result.Created
	if client.tx == nil && result.Created {
		client.runtime.wakeCommands()
		client.runtime.observe(ctx, Observation{
			Kind: ObservationExecution, Operation: "start", Outcome: "created",
			ExecutionID: exec.ID, Name: request.DefinitionName, Version: request.DefinitionVersion,
		})
	}
	return exec, nil
}

func (event Event[T]) Emit(ctx context.Context, c Client, id ExecutionID, key string, payload T) error {
	if state := attemptScope(ctx); state != nil {
		err := newError(ErrInvalidState, "emit", "event", key, "external event ingress is unavailable inside an attempt")
		state.poison(err)
		return err
	}
	return event.emitExternal(ctx, c, id, key, payload)
}

// Deliver records an event in a known execution, including from inside an
// active worker attempt. Delivery is detached from that attempt: pass a
// Runtime.InTx client to join a caller-owned transaction. Once committed, the
// event is not retracted if the source attempt fails or retries; equivalent
// repeats retain ordinary event idempotency. Use Emit(work, ...) for
// same-execution events that must settle atomically with the worker decision.
func (event Event[T]) Deliver(ctx context.Context, client Client, target ExecutionID, key string, payload T) error {
	return event.emitExternal(ctx, client, target, key, payload)
}

func (event Event[T]) emitExternal(ctx context.Context, c Client, id ExecutionID, key string, payload T) error {
	if event.err != nil || event.def == nil || event.def.Namespace != "application" {
		return newError(ErrInvalid, "emit", "event", eventName(event.def), "invalid event definition")
	}
	executionID, err := parseExecutionID(id)
	if err != nil {
		return err
	}
	if err := validateStableKey(key, maxCommandKeyBytes, "event"); err != nil {
		return err
	}
	encoded, err := encodeDefinitionValue(event.def.Payload, payload, maxApplicationEventBytes, "event payload")
	if err != nil {
		return err
	}
	body, err := canonical.Marshal(journalcodec.ApplicationEventBody{
		V: journalcodec.ApplicationEventBodyVersion, Payload: json.RawMessage(encoded.BytesCopy()),
	}, 0)
	if err != nil {
		return newError(ErrInvalid, "emit", "event", event.def.Name, "payload cannot be journaled")
	}
	client, err := resolveClient(c)
	if err != nil {
		return err
	}
	existing, err := client.runtime.store.LookupApplicationEvent(ctx, client.tx, executionID, event.def.Name, key)
	if err != nil {
		return err
	}
	if existing.Found {
		if bytes.Equal(existing.Body, body.Bytes) {
			return nil
		}
		return newError(ErrConflict, "emit", "event", key, "event identity differs")
	}
	created := false
	err = client.semantic(ctx, executionID, func(semantic *store.SemanticTx) error {
		if err := client.runtime.faults.Hit(ctx, fault.IngressBeforeJournal); err != nil {
			return err
		}
		var err error
		created, err = client.runtime.store.EmitLocked(ctx, semantic, store.ApplicationEvent{
			ID: uuid.New(), Name: event.def.Name, Key: key, Body: body,
		})
		return err
	})
	if err != nil {
		return err
	}
	if client.tx == nil && created {
		client.runtime.observe(ctx, Observation{
			Kind: ObservationEvent, Operation: "emit", Outcome: "created", ExecutionID: id,
			Name: event.def.Name,
		})
	}
	return nil
}

func CancelCommand(ctx context.Context, c Client, id CommandID, reason string) error {
	if err := validateCancellationReason(reason); err != nil {
		return err
	}
	commandID, err := parseCommandID(id)
	if err != nil {
		return err
	}
	client, err := resolveClient(c)
	if err != nil {
		return err
	}
	executionID, err := client.runtime.store.LookupCommandExecution(ctx, client.tx, commandID)
	if err != nil {
		return err
	}
	var result store.CancelResult
	err = client.semantic(ctx, executionID, func(semantic *store.SemanticTx) error {
		if err := client.runtime.faults.Hit(ctx, fault.IngressBeforeJournal); err != nil {
			return err
		}
		var err error
		result, err = client.runtime.store.CancelCommandLocked(ctx, semantic, commandID, reason)
		return err
	})
	if err != nil {
		return err
	}
	if client.tx == nil && result.Created {
		client.runtime.observe(ctx, Observation{
			Kind: ObservationCommand, Operation: "cancel", Outcome: "cancelled",
			ExecutionID: ExecutionID(executionID.String()), CommandID: id,
		})
	}
	return nil
}

func CancelExecution(ctx context.Context, c Client, id ExecutionID, reason string) error {
	if err := validateCancellationReason(reason); err != nil {
		return err
	}
	executionID, err := parseExecutionID(id)
	if err != nil {
		return err
	}
	client, err := resolveClient(c)
	if err != nil {
		return err
	}
	var result store.CancelResult
	err = client.semantic(ctx, executionID, func(semantic *store.SemanticTx) error {
		if err := client.runtime.faults.Hit(ctx, fault.IngressBeforeJournal); err != nil {
			return err
		}
		var err error
		result, err = client.runtime.store.CancelExecutionLocked(ctx, semantic, reason)
		return err
	})
	if err != nil {
		return err
	}
	if client.tx == nil && result.Created {
		client.runtime.observe(ctx, Observation{
			Kind: ObservationExecution, Operation: "cancel", Outcome: "cancelled", ExecutionID: id,
		})
	}
	return nil
}

func prepareStartOptions(name string, version int, key string, input canonical.Value, supplied ...ExecutionOption) (executionOptions, canonical.Value, [32]byte, error) {
	if err := durable.PostgresInteger("definition version", version, 1, durable.PostgresIntegerMax); err != nil {
		return executionOptions{}, canonical.Value{}, [32]byte{}, newError(ErrInvalid, "execute", "version", "", err.Error())
	}
	if len(key) > maxExecutionKeyBytes || !utf8.ValidString(key) {
		return executionOptions{}, canonical.Value{}, [32]byte{}, newError(ErrInvalid, "execute", "key", "", "execution key is invalid or too long")
	}
	options := executionOptions{
		deadline: store.DeadlineSpec{Mode: "duration", Duration: defaultExecutionDeadline},
		failFast: true, metadata: map[string]string{},
	}
	for _, option := range supplied {
		if option == nil {
			options.errs = append(options.errs, errors.New("nil execution option"))
			continue
		}
		option.applyExecution(&options)
	}
	if err := errors.Join(options.errs...); err != nil {
		return executionOptions{}, canonical.Value{}, [32]byte{}, newError(ErrInvalid, "execute", "options", "", err.Error())
	}
	if options.keyScope == store.KeyScopeLive && key == "" {
		return executionOptions{}, canonical.Value{}, [32]byte{}, newError(ErrInvalid, "execute", "key", "", "live key scope requires a non-empty execution key")
	}
	if options.withinSet && len(options.waits) == 0 {
		return executionOptions{}, canonical.Value{}, [32]byte{}, newError(ErrInvalid, "execute", "within", "", "Within requires WaitFor")
	}
	if len(options.waits) > maxCommandEventWaits {
		return executionOptions{}, canonical.Value{}, [32]byte{}, newError(ErrInvalid, "execute", "wait", "", "command exceeds the 256 event-wait limit")
	}
	if err := validateMetadata(options.metadata); err != nil {
		return executionOptions{}, canonical.Value{}, [32]byte{}, err
	}
	metadata, err := canonical.Marshal(options.metadata, maxExecutionMetadataBytes)
	if err != nil {
		return executionOptions{}, canonical.Value{}, [32]byte{}, mapCanonicalError("execute", "metadata", err)
	}
	// key_scope and start_delay_ms are omitted when zero so fingerprints of
	// starts that predate these options remain rediscoverable.
	deadlineMilliseconds, err := durable.ExactMilliseconds("execution deadline", options.deadline.Duration)
	if err != nil {
		return executionOptions{}, canonical.Value{}, [32]byte{}, err
	}
	startDelayMilliseconds, err := durable.ExactMilliseconds("start delay", options.startDelay)
	if err != nil {
		return executionOptions{}, canonical.Value{}, [32]byte{}, err
	}
	withinMilliseconds, err := durable.ExactMilliseconds("within", options.within)
	if err != nil {
		return executionOptions{}, canonical.Value{}, [32]byte{}, err
	}
	fingerprintRecord := struct {
		V                 int                      `json:"v"`
		DefinitionName    string                   `json:"definition_name"`
		DefinitionVersion int                      `json:"definition_version"`
		ExecutionKey      string                   `json:"execution_key"`
		KeyScope          string                   `json:"key_scope,omitempty"`
		Input             json.RawMessage          `json:"input"`
		DeadlineMode      string                   `json:"deadline_mode"`
		DeadlineDuration  int64                    `json:"deadline_duration_ms"`
		FailFast          bool                     `json:"fail_fast"`
		StartDelayMS      int64                    `json:"start_delay_ms,omitempty"`
		Waits             []commandWaitFingerprint `json:"waits,omitempty"`
		WithinMS          int64                    `json:"within_ms,omitempty"`
		Metadata          json.RawMessage          `json:"metadata"`
	}{
		V: 1, DefinitionName: name, DefinitionVersion: version,
		ExecutionKey: key, KeyScope: options.keyScope, Input: json.RawMessage(input.BytesCopy()),
		DeadlineMode: options.deadline.Mode, DeadlineDuration: deadlineMilliseconds,
		FailFast: options.failFast, StartDelayMS: startDelayMilliseconds,
		Waits: commandWaitFingerprints(options.waits), WithinMS: withinMilliseconds,
		Metadata: json.RawMessage(metadata.BytesCopy()),
	}
	fingerprint, err := canonical.Marshal(fingerprintRecord, 0)
	if err != nil {
		return executionOptions{}, canonical.Value{}, [32]byte{}, newError(ErrInvalid, "execute", "identity", "", "cannot canonicalize start identity")
	}
	return options, metadata, fingerprint.Digest, nil
}

func prepareCommand(id uuid.UUID, key string, command *definition.Command, defaults commandDefaults, args canonical.Value) (store.CommandCreate, error) {
	if id == uuid.Nil || command == nil {
		return store.CommandCreate{}, newError(ErrInvalid, "create", "command", key, "incomplete command definition")
	}
	if err := validateStableKey(key, maxCommandKeyBytes, "command"); err != nil {
		return store.CommandCreate{}, err
	}
	if err := durable.PostgresInteger("command version", command.Version, 1, durable.PostgresIntegerMax); err != nil {
		return store.CommandCreate{}, newError(ErrInvalid, "create", "command", key, err.Error())
	}
	policy, err := retrypolicy.CanonicalPublic(defaults.retryPolicy)
	if err != nil {
		return store.CommandCreate{}, newError(ErrInvalid, "create", "command", key, "invalid retry policy")
	}
	declaration, err := canonical.Marshal(struct {
		V        int             `json:"v"`
		Key      string          `json:"key"`
		Name     string          `json:"name"`
		Version  int             `json:"version"`
		Args     json.RawMessage `json:"args"`
		Required bool            `json:"required"`
	}{V: 1, Key: key, Name: command.Name, Version: command.Version, Args: json.RawMessage(args.BytesCopy()), Required: true}, 0)
	if err != nil {
		return store.CommandCreate{}, newError(ErrInvalid, "create", "command", key, "cannot canonicalize declaration")
	}
	return store.CommandCreate{
		ID: id, Key: key, Name: command.Name, Version: command.Version, Args: args,
		DeclarationFingerprint: declaration.Digest, Required: true,
		Queue: defaults.queue, AttemptTimeout: defaults.attemptTimeout, RetryPolicy: policy,
	}, nil
}

type commandWaitFingerprint struct {
	Namespace string `json:"namespace"`
	Name      string `json:"name"`
	Key       string `json:"key"`
}

func commandWaitFingerprints(waits []commandEventWait) []commandWaitFingerprint {
	result := make([]commandWaitFingerprint, len(waits))
	for index, wait := range waits {
		result[index] = commandWaitFingerprint{Namespace: wait.namespace, Name: wait.name, Key: wait.key}
	}
	sort.Slice(result, func(i, j int) bool {
		if result[i].Namespace != result[j].Namespace {
			return result[i].Namespace < result[j].Namespace
		}
		if result[i].Name != result[j].Name {
			return result[i].Name < result[j].Name
		}
		return result[i].Key < result[j].Key
	})
	return result
}

func makeCommandEventWait(event EventRef, key string) (commandEventWait, error) {
	if event == nil {
		return commandEventWait{}, newError(ErrInvalid, "execute", "wait", key, "nil event selector")
	}
	if err := validateStableKey(key, maxCommandKeyBytes, "event"); err != nil {
		return commandEventWait{}, err
	}
	selector := event.flowEventRef()
	if selector.name == "" || selector.namespace != "application" {
		return commandEventWait{}, newError(ErrInvalid, "execute", "wait", key, "invalid event selector")
	}
	return commandEventWait{eventReference: selector, key: key}, nil
}

func commandDeclarationFingerprint(command store.CommandCreate) ([32]byte, error) {
	parent := ""
	if command.ParentCommandID != nil {
		parent = command.ParentCommandID.String()
	}
	waits := make([]commandWaitFingerprint, len(command.Waits))
	for index, wait := range command.Waits {
		waits[index] = commandWaitFingerprint{Namespace: "application", Name: wait.Name, Key: wait.Key}
	}
	sort.Slice(waits, func(i, j int) bool {
		if waits[i].Name != waits[j].Name {
			return waits[i].Name < waits[j].Name
		}
		return waits[i].Key < waits[j].Key
	})
	startAfterMilliseconds, err := durable.ExactMilliseconds("initial delay", command.InitialDelay)
	if err != nil {
		return [32]byte{}, err
	}
	attemptTimeoutMilliseconds, err := durable.ExactMilliseconds("attempt timeout", command.AttemptTimeout)
	if err != nil {
		return [32]byte{}, err
	}
	withinMilliseconds, err := durable.ExactMilliseconds("within", command.Within)
	if err != nil {
		return [32]byte{}, err
	}
	declaration, err := canonical.Marshal(struct {
		V            int                      `json:"v"`
		Key          string                   `json:"key"`
		Name         string                   `json:"name"`
		Version      int                      `json:"version"`
		Args         json.RawMessage          `json:"args"`
		Parent       string                   `json:"parent,omitempty"`
		Required     bool                     `json:"required"`
		Queue        string                   `json:"queue"`
		RetryPolicy  json.RawMessage          `json:"retry_policy"`
		AttemptMS    int64                    `json:"attempt_timeout_ms,omitempty"`
		StartAfterMS int64                    `json:"start_after_ms,omitempty"`
		Waits        []commandWaitFingerprint `json:"waits,omitempty"`
		WithinMS     int64                    `json:"within_ms,omitempty"`
	}{
		V: 1, Key: command.Key, Name: command.Name, Version: command.Version,
		Args: json.RawMessage(command.Args.BytesCopy()), Parent: parent,
		Required: command.Required, Queue: command.Queue,
		RetryPolicy: json.RawMessage(command.RetryPolicy.BytesCopy()), AttemptMS: attemptTimeoutMilliseconds,
		StartAfterMS: startAfterMilliseconds,
		Waits:        waits, WithinMS: withinMilliseconds,
	}, 0)
	if err != nil {
		return [32]byte{}, newError(ErrInvalid, "create", "command", command.Key, "declaration cannot be canonicalized")
	}
	return declaration.Digest, nil
}

func encodeDefinitionValue(codec definition.Codec, value any, maxBytes int, resource string) (canonical.Value, error) {
	encoded, err := codec.Encode(value, maxBytes)
	if err != nil {
		return canonical.Value{}, mapCanonicalError("encode", resource, err)
	}
	return encoded, nil
}

func mapCanonicalError(operation, resource string, err error) error {
	if errors.Is(err, canonical.ErrTooLarge) {
		return newError(ErrPayloadTooLarge, operation, resource, "", "canonical value exceeds its limit")
	}
	return newError(ErrInvalid, operation, resource, "", "value is not canonical JSON")
}

func validateStableKey(key string, maxBytes int, resource string) error {
	if key == "" || len(key) > maxBytes || !utf8.ValidString(key) || strings.TrimSpace(key) != key {
		return newError(ErrInvalid, "validate", resource+" key", "", "key is empty, malformed, or too long")
	}
	return nil
}

func validateMetadata(metadata map[string]string) error {
	for key, value := range metadata {
		if key == "" || len(key) > 128 || len(value) > 1024 || !utf8.ValidString(key) || !utf8.ValidString(value) {
			return newError(ErrInvalid, "execute", "metadata", "", "metadata key or value is invalid")
		}
	}
	return nil
}

func validateCancellationReason(reason string) error {
	if reason == "" || strings.TrimSpace(reason) != reason || len(reason) > 1024 || !utf8.ValidString(reason) {
		return newError(ErrInvalid, "cancel", "reason", "", "reason is empty, malformed, or too long")
	}
	return nil
}

func validateBoundClient(client Client) error {
	if client == nil {
		return errors.New("definition is not bound to a client")
	}
	return nil
}

func cloneStringMap(input map[string]string) map[string]string {
	result := make(map[string]string, len(input))
	for key, value := range input {
		result[key] = value
	}
	return result
}

func eventName(event *definition.Event) string {
	if event == nil {
		return ""
	}
	return event.Name
}
