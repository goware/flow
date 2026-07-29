package flow

import (
	"bytes"
	"context"
	"encoding/json"
	"errors"
	"strings"
	"time"
	"unicode/utf8"

	"github.com/google/uuid"
	"github.com/goware/flow/internal/canonical"
	"github.com/goware/flow/internal/definition"
	"github.com/goware/flow/internal/fault"
	retrypolicy "github.com/goware/flow/internal/retry"
	"github.com/goware/flow/internal/store"
	"github.com/goware/flow/internal/store/journalcodec"
	"github.com/jackc/pgx/v5"
)

const (
	maxCommandArgumentBytes   = 256 << 10
	maxCoordinatorStateBytes  = 256 << 10
	maxApplicationEventBytes  = 64 << 10
	maxExecutionMetadataBytes = 16 << 10
	maxExecutionKeyBytes      = 1024
	maxCommandKeyBytes        = 1024
	defaultExecutionDeadline  = 30 * 24 * time.Hour
)

// ExecutionOption is a sealed start option shared by all execution modes.
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

// WithStartDelay schedules a direct execution's root command to become
// deliverable after the delay instead of immediately, mirroring StartAfter
// for spawned children. Plan and coordinator starts reject it.
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
		options.startDelay = delay
	})
}

func (cmd Command[A, R]) Execute(ctx context.Context, key string, args A, opts ...ExecutionOption) (ExecutionHandle, error) {
	var definitionError error
	if cmd.def == nil {
		definitionError = errors.New("zero definition")
	}
	if err := errors.Join(cmd.err, definitionError, validateBoundClient(cmd.client)); err != nil {
		return ExecutionHandle{}, newError(ErrInvalid, "execute", "command", cmd.Name(), err.Error())
	}
	input, err := encodeDefinitionValue(cmd.def.Args, args, maxCommandArgumentBytes, "command arguments")
	if err != nil {
		return ExecutionHandle{}, err
	}
	client, err := resolveClient(cmd.client)
	if err != nil {
		return ExecutionHandle{}, err
	}
	options, metadata, fingerprint, err := prepareStartOptions(store.DriverDirect, cmd.Name(), cmd.Version(), key, input, opts...)
	if err != nil {
		return ExecutionHandle{}, err
	}
	root, err := prepareCommand(uuid.New(), "root", cmd.def, cmd.defaults, input, "direct_root")
	if err != nil {
		return ExecutionHandle{}, err
	}
	if options.startDelay > 0 {
		root.ScheduleKind = "execute_delay"
		root.InitialDelay = options.startDelay
	}
	request := store.StartRequest{
		ID: uuid.New(), Mode: store.DriverDirect, DefinitionName: cmd.Name(), DefinitionVersion: cmd.Version(), Key: key,
		KeyScope: options.keyScope, StartFingerprint: fingerprint, Input: input, Metadata: metadata,
		FailFast: options.failFast, Deadline: options.deadline, MaxCommands: client.runtime.maxCommands, Root: &root,
	}
	return executeStart(ctx, client, request)
}

func (plan PlanDef[A]) Execute(ctx context.Context, key string, args A, opts ...ExecutionOption) (ExecutionHandle, error) {
	var definitionError error
	if plan.def == nil {
		definitionError = errors.New("zero definition")
	}
	if err := errors.Join(plan.err, definitionError, validateBoundClient(plan.client)); err != nil {
		return ExecutionHandle{}, newError(ErrInvalid, "execute", "plan", planDefinitionName(plan.def), err.Error())
	}
	input, err := encodeDefinitionValue(plan.def.Args, args, maxCommandArgumentBytes, "plan arguments")
	if err != nil {
		return ExecutionHandle{}, err
	}
	client, err := resolveClient(plan.client)
	if err != nil {
		return ExecutionHandle{}, err
	}
	options, metadata, fingerprint, err := prepareStartOptions(store.DriverPlan, plan.def.Name, plan.def.Version, key, input, opts...)
	if err != nil {
		return ExecutionHandle{}, err
	}
	if options.startDelay > 0 {
		return ExecutionHandle{}, newError(ErrInvalid, "execute", "plan", plan.def.Name, "plan starts do not accept a start delay")
	}
	return executeStart(ctx, client, store.StartRequest{
		ID: uuid.New(), Mode: store.DriverPlan, DefinitionName: plan.def.Name, DefinitionVersion: plan.def.Version, Key: key,
		KeyScope: options.keyScope, StartFingerprint: fingerprint, Input: input, Metadata: metadata,
		FailFast: options.failFast, Deadline: options.deadline, MaxCommands: client.runtime.maxCommands,
	})
}

func (coordinator Coordinator[S]) Execute(ctx context.Context, key string, initial S, opts ...ExecutionOption) (ExecutionHandle, error) {
	var definitionError error
	if coordinator.def == nil {
		definitionError = errors.New("zero definition")
	}
	if err := errors.Join(coordinator.err, definitionError, validateBoundClient(coordinator.client)); err != nil {
		return ExecutionHandle{}, newError(ErrInvalid, "execute", "coordinator", coordinatorDefinitionName(coordinator.def), err.Error())
	}
	input, err := encodeDefinitionValue(coordinator.def.State, initial, maxCoordinatorStateBytes, "coordinator state")
	if err != nil {
		return ExecutionHandle{}, err
	}
	client, err := resolveClient(coordinator.client)
	if err != nil {
		return ExecutionHandle{}, err
	}
	options, metadata, fingerprint, err := prepareStartOptions(store.DriverCoordinator, coordinator.def.Name, coordinator.def.Version, key, input, opts...)
	if err != nil {
		return ExecutionHandle{}, err
	}
	if options.startDelay > 0 {
		return ExecutionHandle{}, newError(ErrInvalid, "execute", "coordinator", coordinator.def.Name, "coordinator starts do not accept a start delay")
	}
	policy, err := retrypolicy.CanonicalPublic(defaultRetryPolicy())
	if err != nil {
		return ExecutionHandle{}, newError(ErrInvalid, "execute", "coordinator", coordinator.def.Name, "invalid default retry policy")
	}
	request := store.StartRequest{
		ID: uuid.New(), Mode: store.DriverCoordinator, DefinitionName: coordinator.def.Name,
		DefinitionVersion: coordinator.def.Version, Key: key, KeyScope: options.keyScope, StartFingerprint: fingerprint,
		Input: input, Metadata: metadata, FailFast: options.failFast, Deadline: options.deadline,
		MaxCommands: client.runtime.maxCommands,
		Coordinator: &store.CoordinatorCreate{ID: uuid.New(), State: input, RetryPolicy: policy},
	}
	return executeStart(ctx, client, request)
}

func executeStart(ctx context.Context, client resolvedClient, request store.StartRequest) (ExecutionHandle, error) {
	var result store.StartResult
	err := client.inTransaction(ctx, func(tx pgx.Tx) error {
		if err := client.runtime.faults.Hit(ctx, fault.IngressBeforeJournal); err != nil {
			return err
		}
		var err error
		result, err = client.runtime.store.StartInTx(ctx, tx, request)
		return err
	})
	if err != nil {
		return ExecutionHandle{}, err
	}
	handle := ExecutionHandle{
		ID: ExecutionID(result.ExecutionID.String()), Type: request.DefinitionName, Key: request.Key, Created: result.Created,
	}
	if result.RootCommandID != nil {
		handle.RootCommandID = CommandID(result.RootCommandID.String())
	}
	if client.tx == nil && result.Created {
		client.runtime.wakeCommands()
		client.runtime.observe(ctx, Observation{
			Kind: ObservationExecution, Operation: "start", Outcome: "created",
			ExecutionID: handle.ID, Name: request.DefinitionName, Version: request.DefinitionVersion,
		})
	}
	return handle, nil
}

func (event Event[T]) Emit(ctx context.Context, c Client, id ExecutionID, key string, payload T) error {
	if state := attemptScope(ctx); state != nil {
		err := newError(ErrInvalidState, "emit", "event", key, "external event ingress is unavailable inside an attempt")
		state.poison(err)
		return err
	}
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
	body, err := canonical.Marshal(journalcodec.ApplicationEventBody{V: 1, Payload: json.RawMessage(encoded.BytesCopy())}, 0)
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

func prepareStartOptions(mode store.DriverMode, name string, version int, key string, input canonical.Value, supplied ...ExecutionOption) (executionOptions, canonical.Value, [32]byte, error) {
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
	if err := validateMetadata(options.metadata); err != nil {
		return executionOptions{}, canonical.Value{}, [32]byte{}, err
	}
	metadata, err := canonical.Marshal(options.metadata, maxExecutionMetadataBytes)
	if err != nil {
		return executionOptions{}, canonical.Value{}, [32]byte{}, mapCanonicalError("execute", "metadata", err)
	}
	// key_scope and start_delay_ms are omitted when zero so fingerprints of
	// starts that predate these options remain rediscoverable.
	fingerprintRecord := struct {
		V                 int             `json:"v"`
		DriverMode        string          `json:"driver_mode"`
		DefinitionName    string          `json:"definition_name"`
		DefinitionVersion int             `json:"definition_version"`
		ExecutionKey      string          `json:"execution_key"`
		KeyScope          string          `json:"key_scope,omitempty"`
		Input             json.RawMessage `json:"input"`
		DeadlineMode      string          `json:"deadline_mode"`
		DeadlineDuration  int64           `json:"deadline_duration"`
		FailFast          bool            `json:"fail_fast"`
		StartDelayMS      int64           `json:"start_delay_ms,omitempty"`
		Metadata          json.RawMessage `json:"metadata"`
	}{
		V: 1, DriverMode: string(mode), DefinitionName: name, DefinitionVersion: version,
		ExecutionKey: key, KeyScope: options.keyScope, Input: json.RawMessage(input.BytesCopy()),
		DeadlineMode: options.deadline.Mode, DeadlineDuration: int64(options.deadline.Duration),
		FailFast: options.failFast, StartDelayMS: options.startDelay.Milliseconds(),
		Metadata: json.RawMessage(metadata.BytesCopy()),
	}
	fingerprint, err := canonical.Marshal(fingerprintRecord, 0)
	if err != nil {
		return executionOptions{}, canonical.Value{}, [32]byte{}, newError(ErrInvalid, "execute", "identity", "", "cannot canonicalize start identity")
	}
	return options, metadata, fingerprint.Digest, nil
}

func prepareCommand(id uuid.UUID, key string, command *definition.Command, defaults commandDefaults, args canonical.Value, origin string) (store.CommandCreate, error) {
	if id == uuid.Nil || command == nil {
		return store.CommandCreate{}, newError(ErrInvalid, "create", "command", key, "incomplete command definition")
	}
	if err := validateStableKey(key, maxCommandKeyBytes, "command"); err != nil {
		return store.CommandCreate{}, err
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
		Origin   string          `json:"origin"`
		Required bool            `json:"required"`
	}{V: 1, Key: key, Name: command.Name, Version: command.Version, Args: json.RawMessage(args.BytesCopy()), Origin: origin, Required: true}, 0)
	if err != nil {
		return store.CommandCreate{}, newError(ErrInvalid, "create", "command", key, "cannot canonicalize declaration")
	}
	return store.CommandCreate{
		ID: id, Key: key, Name: command.Name, Version: command.Version, Args: args,
		DeclarationFingerprint: declaration.Digest, Origin: origin, Required: true,
		Queue: defaults.queue, AttemptTimeout: defaults.attemptTimeout, RetryPolicy: policy, ScheduleKind: "none",
	}, nil
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

func planDefinitionName(plan *definition.Plan) string {
	if plan == nil {
		return ""
	}
	return plan.Name
}

func coordinatorDefinitionName(coordinator *definition.Coordinator) string {
	if coordinator == nil {
		return ""
	}
	return coordinator.Name
}

func eventName(event *definition.Event) string {
	if event == nil {
		return ""
	}
	return event.Name
}
