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
	maxCommandArgumentBytes  = 256 << 10
	maxApplicationEventBytes = journalcodec.MaxApplicationEventPayloadBytes
	maxCommandEventWaits     = 256
	maxRunMetadataBytes      = 16 << 10
	maxRunKeyBytes           = 1024
	maxCommandKeyBytes       = 1024
	defaultRunDeadline       = 30 * 24 * time.Hour
)

// RunOption is a sealed command run option.
type RunOption interface {
	applyRun(*runOptions)
}

type runOptions struct {
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

type runOptionFunc func(*runOptions)

func (f runOptionFunc) applyRun(options *runOptions) { f(options) }

func WithRunDeadline(deadline time.Duration) RunOption {
	return runOptionFunc(func(options *runOptions) {
		if options.deadlineSet {
			options.errs = append(options.errs, errors.New("run deadline configured more than once"))
			return
		}
		options.deadlineSet = true
		if deadline <= 0 {
			options.errs = append(options.errs, errors.New("run deadline must be positive"))
			return
		}
		normalized, _, err := durable.CeilMilliseconds("run deadline", deadline)
		if err != nil {
			options.errs = append(options.errs, err)
			return
		}
		options.deadline = store.DeadlineSpec{Mode: "duration", Duration: normalized}
	})
}

func WithoutRunDeadline() RunOption {
	return runOptionFunc(func(options *runOptions) {
		if options.deadlineSet {
			options.errs = append(options.errs, errors.New("run deadline configured more than once"))
			return
		}
		options.deadlineSet = true
		options.deadline = store.DeadlineSpec{Mode: "none"}
	})
}

func WithMetadata(metadata map[string]string) RunOption {
	return runOptionFunc(func(options *runOptions) {
		if options.metadataSet {
			options.errs = append(options.errs, errors.New("metadata configured more than once"))
			return
		}
		options.metadataSet = true
		options.metadata = cloneStringMap(metadata)
	})
}

func WithFailFast(enabled bool) RunOption {
	return runOptionFunc(func(options *runOptions) {
		if options.failFastSet {
			options.errs = append(options.errs, errors.New("fail-fast configured more than once"))
			return
		}
		options.failFastSet = true
		options.failFast = enabled
	})
}

// WithLiveKey scopes the run key to live runs: while a running or
// failing run holds the key, Enqueue rediscovers it without comparing
// start identity — a silent, queue-style dedupe no-op — and once that
// run reaches a terminal status the key is released for a new start.
// Live-keyed starts therefore give at-most-one live run per key, not
// at-most-one run ever. Requires a non-empty key.
func WithLiveKey() RunOption {
	return runOptionFunc(func(options *runOptions) {
		if options.keyScopeSet {
			options.errs = append(options.errs, errors.New("key scope configured more than once"))
			return
		}
		options.keyScopeSet = true
		options.keyScope = store.KeyScopeLive
	})
}

// WithStartDelay schedules a run's root command to become deliverable
// after the delay instead of immediately, mirroring Delay for sub-commands.
func WithStartDelay(delay time.Duration) RunOption {
	return runOptionFunc(func(options *runOptions) {
		if options.startDelaySet {
			options.errs = append(options.errs, errors.New("start delay configured more than once"))
			return
		}
		options.startDelaySet = true
		if delay <= 0 {
			options.errs = append(options.errs, errors.New("start delay must be positive"))
			return
		}
		normalized, _, err := durable.CeilMilliseconds("start delay", delay)
		if err != nil {
			options.errs = append(options.errs, err)
			return
		}
		options.startDelay = normalized
	})
}

// WaitFor gates a root command on one exact application event inside the
// run it creates. Multiple waits are AND conditions. Worker decisions
// use the matching Node.WaitFor method.
func WaitFor(event EventRef, key string) RunOption {
	return runOptionFunc(func(options *runOptions) {
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
func Within(duration time.Duration) RunOption {
	return runOptionFunc(func(options *runOptions) {
		if duration <= 0 {
			options.errs = append(options.errs, errors.New("within must be positive"))
			return
		}
		normalized, _, err := durable.CeilMilliseconds("within", duration)
		if err != nil {
			options.errs = append(options.errs, err)
			return
		}
		if options.withinSet && options.within == normalized {
			return
		}
		if options.withinSet {
			options.errs = append(options.errs, errors.New("within configured with different values"))
			return
		}
		options.withinSet, options.within = true, normalized
	})
}

func (cmd Command[A, R]) Enqueue(ctx context.Context, client Client, key string, args A, opts ...RunOption) (Run, error) {
	var definitionError error
	if cmd.def == nil {
		definitionError = errors.New("zero definition")
	}
	if err := errors.Join(cmd.err, definitionError); err != nil {
		return Run{}, newError(ErrInvalid, "enqueue", "command", cmd.Name(), err.Error())
	}
	input, err := encodeDefinitionValue(cmd.def.Args, args, maxCommandArgumentBytes, "command arguments")
	if err != nil {
		return Run{}, err
	}
	resolved, err := resolveClient(client)
	if err != nil {
		return Run{}, err
	}
	if err := resolved.beforeFlowWrite(); err != nil {
		return Run{}, err
	}
	request, err := cmd.prepareStartRequest(key, input, resolved.runtime.maxCommands, opts...)
	if err != nil {
		return Run{}, err
	}
	return enqueueStart(ctx, resolved, request)
}

func (cmd Command[A, R]) prepareStartRequest(
	key string,
	input canonical.Value,
	maxCommands int,
	opts ...RunOption,
) (store.StartRequest, error) {
	options, metadata, fingerprint, err := prepareStartOptions(cmd.Name(), cmd.Version(), key, input, opts...)
	if err != nil {
		return store.StartRequest{}, err
	}
	root, err := prepareCommand(uuid.New(), "root", cmd.def, cmd.defaults, input)
	if err != nil {
		return store.StartRequest{}, err
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
		return store.StartRequest{}, err
	}
	return store.StartRequest{
		ID: uuid.New(), DefinitionName: cmd.Name(), DefinitionVersion: cmd.Version(), Key: key,
		KeyScope: options.keyScope, StartFingerprint: fingerprint, Input: input, Metadata: metadata,
		FailFast: options.failFast, Deadline: options.deadline, MaxCommands: maxCommands, Root: &root,
	}, nil
}

// ReplaceCurrentRun atomically cancels expected and creates a distinct
// live-key successor. If expected is stale, an equivalent already-committed
// successor is returned with Replaced=false; a different current declaration
// conflicts and no state is changed.
func (cmd Command[A, R]) ReplaceCurrentRun(
	ctx context.Context,
	client Client,
	expected RunID,
	key string,
	args A,
	reason string,
	opts ...RunOption,
) (ReplaceRunResult, error) {
	var definitionError error
	if cmd.def == nil {
		definitionError = errors.New("zero definition")
	}
	if err := errors.Join(cmd.err, definitionError); err != nil {
		return ReplaceRunResult{}, newError(ErrInvalid, "replace", "command", cmd.Name(), err.Error())
	}
	expectedID, err := parseRunID(expected)
	if err != nil {
		return ReplaceRunResult{}, err
	}
	if err := validateCancellationReason(reason); err != nil {
		return ReplaceRunResult{}, err
	}
	input, err := encodeDefinitionValue(cmd.def.Args, args, maxCommandArgumentBytes, "command arguments")
	if err != nil {
		return ReplaceRunResult{}, err
	}
	resolved, err := resolveClient(client)
	if err != nil {
		return ReplaceRunResult{}, err
	}
	if err := resolved.beforeFlowWrite(); err != nil {
		return ReplaceRunResult{}, err
	}
	start, err := cmd.prepareStartRequest(key, input, resolved.runtime.maxCommands, opts...)
	if err != nil {
		return ReplaceRunResult{}, err
	}
	if key == "" || start.KeyScope != store.KeyScopeLive {
		return ReplaceRunResult{}, newError(ErrInvalid, "replace", "run", key, "replacement requires a non-empty live key and WithLiveKey")
	}

	var result store.ReplaceRunResult
	err = resolved.inTransaction(ctx, func(tx pgx.Tx) error {
		if err := resolved.runtime.faults.Hit(ctx, fault.IngressBeforeJournal); err != nil {
			return err
		}
		var err error
		result, err = resolved.runtime.store.ReplaceCurrentRunInTx(ctx, tx, store.ReplaceRunRequest{
			Expected: expectedID, Start: start, Reason: reason,
		}, resolved.order)
		return err
	})
	if err != nil {
		return ReplaceRunResult{}, err
	}
	run, err := runFromStore(result.Start.Row)
	if err != nil {
		return ReplaceRunResult{}, err
	}
	run.Created = result.Start.Created
	if resolved.tx == nil && result.Replaced {
		resolved.runtime.wakeCommands()
		resolved.runtime.observe(ctx, Observation{
			Kind: ObservationRun, Operation: ObservationOpCancel, Outcome: ObservationOutcomeCancelled,
			RunID: expected, RunKey: key,
		})
		resolved.runtime.observe(ctx, Observation{
			Kind: ObservationRun, Operation: ObservationOpStart, Outcome: ObservationOutcomeCreated,
			RunID: run.ID, RunKey: key, Definition: start.DefinitionName,
			Name: start.DefinitionName, Version: start.DefinitionVersion,
		})
	}
	return ReplaceRunResult{Run: run, Replaced: result.Replaced}, nil
}

func enqueueStart(ctx context.Context, client resolvedClient, request store.StartRequest) (Run, error) {
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
		return Run{}, err
	}
	run, err := runFromStore(result.Row)
	if err != nil {
		return Run{}, err
	}
	run.Created = result.Created
	if client.tx == nil && result.Created {
		client.runtime.wakeCommands()
		client.runtime.observe(ctx, Observation{
			Kind: ObservationRun, Operation: ObservationOpStart, Outcome: ObservationOutcomeCreated,
			RunID: run.ID, RunKey: request.Key, Definition: request.DefinitionName,
			Name: request.DefinitionName, Version: request.DefinitionVersion,
		})
	}
	return run, nil
}

// Deliver records an event in a known run, including from inside an
// active worker attempt. Delivery is detached from that attempt: pass a
// Runtime.InTx client to join a caller-owned transaction. Once committed, the
// event is not retracted if the source attempt fails or retries; equivalent
// repeats retain ordinary event idempotency. Use Emit(work, ...) for
// same-run events that must settle atomically with the worker decision.
func (event Event[T]) Deliver(ctx context.Context, client Client, target RunID, key string, payload T) error {
	return event.deliverExternal(ctx, client, target, key, payload)
}

func (event Event[T]) deliverExternal(ctx context.Context, c Client, id RunID, key string, payload T) error {
	if event.err != nil || event.def == nil || event.def.Namespace != "application" {
		return newError(ErrInvalid, "deliver", "event", eventName(event.def), "invalid event definition")
	}
	runID, err := parseRunID(id)
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
		return newError(ErrInvalid, "deliver", "event", event.def.Name, "payload cannot be journaled")
	}
	client, err := resolveClient(c)
	if err != nil {
		return err
	}
	if err := client.beforeFlowWrite(); err != nil {
		return err
	}
	existing, found, err := client.runtime.store.GetEvent(ctx, client.tx, runID, event.def.Name, key)
	if err != nil {
		return err
	}
	if found {
		if bytes.Equal(existing.Body, body.Bytes) {
			return nil
		}
		return newError(ErrConflict, "deliver", "event", key, "event identity differs")
	}
	created := false
	err = client.semantic(ctx, runID, func(semantic *store.SemanticTx) error {
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
			Kind: ObservationEvent, Operation: ObservationOpDeliver, Outcome: ObservationOutcomeCreated, RunID: id,
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
	if err := client.beforeFlowWrite(); err != nil {
		return err
	}
	runID, err := client.runtime.store.GetCommandRunID(ctx, client.tx, commandID)
	if err != nil {
		return err
	}
	var result store.CancelResult
	err = client.semantic(ctx, runID, func(semantic *store.SemanticTx) error {
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
			Kind: ObservationCommand, Operation: ObservationOpCancel, Outcome: ObservationOutcomeCancelled,
			RunID: RunID(runID.String()), CommandID: id,
		})
		client.runtime.observeRunTerminal(ctx, result.RunTerminalStatus, RunID(runID.String()), "", "")
	}
	return nil
}

func CancelRun(ctx context.Context, c Client, id RunID, reason string) error {
	if err := validateCancellationReason(reason); err != nil {
		return err
	}
	runID, err := parseRunID(id)
	if err != nil {
		return err
	}
	client, err := resolveClient(c)
	if err != nil {
		return err
	}
	if err := client.beforeFlowWrite(); err != nil {
		return err
	}
	var result store.CancelResult
	err = client.semantic(ctx, runID, func(semantic *store.SemanticTx) error {
		if err := client.runtime.faults.Hit(ctx, fault.IngressBeforeJournal); err != nil {
			return err
		}
		var err error
		result, err = client.runtime.store.CancelRunLocked(ctx, semantic, reason)
		return err
	})
	if err != nil {
		return err
	}
	if client.tx == nil && result.Created {
		client.runtime.observe(ctx, Observation{
			Kind: ObservationRun, Operation: ObservationOpCancel, Outcome: ObservationOutcomeCancelled, RunID: id,
		})
	}
	return nil
}

func prepareStartOptions(name string, version int, key string, input canonical.Value, supplied ...RunOption) (runOptions, canonical.Value, [32]byte, error) {
	if err := durable.PostgresInteger("definition version", version, 1, durable.PostgresIntegerMax); err != nil {
		return runOptions{}, canonical.Value{}, [32]byte{}, newError(ErrInvalid, "enqueue", "version", "", err.Error())
	}
	if len(key) > maxRunKeyBytes || !utf8.ValidString(key) {
		return runOptions{}, canonical.Value{}, [32]byte{}, newError(ErrInvalid, "enqueue", "key", "", "run key is invalid or too long")
	}
	options := runOptions{
		deadline: store.DeadlineSpec{Mode: "duration", Duration: defaultRunDeadline},
		failFast: true, metadata: map[string]string{},
	}
	for _, option := range supplied {
		if option == nil {
			options.errs = append(options.errs, errors.New("nil run option"))
			continue
		}
		option.applyRun(&options)
	}
	if err := errors.Join(options.errs...); err != nil {
		return runOptions{}, canonical.Value{}, [32]byte{}, newError(ErrInvalid, "enqueue", "options", "", err.Error())
	}
	if options.keyScope == store.KeyScopeLive && key == "" {
		return runOptions{}, canonical.Value{}, [32]byte{}, newError(ErrInvalid, "enqueue", "key", "", "live key scope requires a non-empty run key")
	}
	if options.withinSet && len(options.waits) == 0 {
		return runOptions{}, canonical.Value{}, [32]byte{}, newError(ErrInvalid, "enqueue", "within", "", "Within requires WaitFor")
	}
	if len(options.waits) > maxCommandEventWaits {
		return runOptions{}, canonical.Value{}, [32]byte{}, newError(ErrInvalid, "enqueue", "wait", "", "command exceeds the 256 event-wait limit")
	}
	if err := validateMetadata(options.metadata); err != nil {
		return runOptions{}, canonical.Value{}, [32]byte{}, err
	}
	metadata, err := canonical.Marshal(options.metadata, maxRunMetadataBytes)
	if err != nil {
		return runOptions{}, canonical.Value{}, [32]byte{}, mapCanonicalError("enqueue", "metadata", err)
	}
	// key_scope and start_delay_ms are omitted when zero so fingerprints of
	// starts that predate these options remain rediscoverable.
	deadlineMilliseconds, err := durable.ExactMilliseconds("run deadline", options.deadline.Duration)
	if err != nil {
		return runOptions{}, canonical.Value{}, [32]byte{}, err
	}
	startDelayMilliseconds, err := durable.ExactMilliseconds("start delay", options.startDelay)
	if err != nil {
		return runOptions{}, canonical.Value{}, [32]byte{}, err
	}
	withinMilliseconds, err := durable.ExactMilliseconds("within", options.within)
	if err != nil {
		return runOptions{}, canonical.Value{}, [32]byte{}, err
	}
	fingerprintRecord := struct {
		V                 int                      `json:"v"`
		DefinitionName    string                   `json:"definition_name"`
		DefinitionVersion int                      `json:"definition_version"`
		RunKey            string                   `json:"execution_key"`
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
		RunKey: key, KeyScope: options.keyScope, Input: json.RawMessage(input.BytesCopy()),
		DeadlineMode: options.deadline.Mode, DeadlineDuration: deadlineMilliseconds,
		FailFast: options.failFast, StartDelayMS: startDelayMilliseconds,
		Waits: commandWaitFingerprints(options.waits), WithinMS: withinMilliseconds,
		Metadata: json.RawMessage(metadata.BytesCopy()),
	}
	fingerprint, err := canonical.Marshal(fingerprintRecord, 0)
	if err != nil {
		return runOptions{}, canonical.Value{}, [32]byte{}, newError(ErrInvalid, "enqueue", "identity", "", "cannot canonicalize start identity")
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
		return commandEventWait{}, newError(ErrInvalid, "enqueue", "wait", key, "nil event selector")
	}
	if err := validateStableKey(key, maxCommandKeyBytes, "event"); err != nil {
		return commandEventWait{}, err
	}
	selector := event.flowEventRef()
	if selector.name == "" || selector.namespace != "application" {
		return commandEventWait{}, newError(ErrInvalid, "enqueue", "wait", key, "invalid event selector")
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
			return newError(ErrInvalid, "enqueue", "metadata", "", "metadata key or value is invalid")
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
