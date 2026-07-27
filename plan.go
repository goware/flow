package flow

import (
	"bytes"
	"encoding/json"
	"errors"
	"fmt"
	"sort"
	"strconv"
	"time"

	"github.com/google/uuid"
	"github.com/goware/flow/internal/canonical"
	"github.com/goware/flow/internal/definition"
	retrypolicy "github.com/goware/flow/internal/retry"
	"github.com/goware/flow/internal/store"
)

type Plan struct {
	snapshot        planSnapshot
	declarations    map[string]*planDeclaration
	order           []string
	readKeys        map[string]struct{}
	readSelectors   map[eventReference]struct{}
	selectorMisses  map[eventReference]struct{}
	valueMisses     map[uuid.UUID]struct{}
	waitingReadKeys map[string]struct{}
	waitingReads    int
	firstError      error
}

type Node struct {
	plan *Plan
	decl *planDeclaration
}

type planSnapshot struct {
	commands     map[string]store.PlanCommandSnapshot
	events       map[eventReference][]store.PlanEventSnapshot
	loadedEvents map[eventReference]bool
}

type planDeclaration struct {
	key            string
	command        *definition.Command
	defaults       commandDefaults
	args           canonical.Value
	required       bool
	groups         []planDependency
	waits          []eventReference
	within         time.Duration
	delay          time.Duration
	optionalSet    bool
	withinSet      bool
	delaySet       bool
	retryOverride  *RetryPolicy
	maxAttempts    *int
	retrySet       bool
	maxAttemptsSet bool
}

type planDependency struct {
	kind      string
	threshold *int
	keys      []string
}

func newPlan(snapshot store.PlanSnapshot) *Plan {
	plan := &Plan{
		declarations: make(map[string]*planDeclaration), readKeys: make(map[string]struct{}), readSelectors: make(map[eventReference]struct{}),
		selectorMisses: make(map[eventReference]struct{}), valueMisses: make(map[uuid.UUID]struct{}),
		waitingReadKeys: make(map[string]struct{}),
		snapshot: planSnapshot{commands: make(map[string]store.PlanCommandSnapshot),
			events: make(map[eventReference][]store.PlanEventSnapshot), loadedEvents: make(map[eventReference]bool)},
	}
	for _, command := range snapshot.Commands {
		plan.snapshot.commands[command.Key] = command
	}
	for _, event := range snapshot.Events {
		selector := eventReference{namespace: event.Namespace, name: event.Name, version: event.Version}
		plan.snapshot.events[selector] = append(plan.snapshot.events[selector], event)
	}
	for _, selector := range snapshot.LoadedSelectors {
		plan.snapshot.loadedEvents[eventReference{namespace: selector.Namespace, name: selector.Name, version: selector.Version}] = true
	}
	return plan
}

func Do[A, R any](plan *Plan, key string, cmd Command[A, R], args A) *Node {
	if plan == nil {
		return &Node{}
	}
	if plan.firstError != nil {
		return &Node{plan: plan}
	}
	if cmd.def == nil || cmd.err != nil {
		plan.poison(newError(ErrInvalid, "plan", "command", key, "invalid command definition"))
		return &Node{plan: plan}
	}
	if err := validateStableKey(key, maxCommandKeyBytes, "command"); err != nil {
		plan.poison(err)
		return &Node{plan: plan}
	}
	encoded, err := encodeDefinitionValue(cmd.def.Args, args, maxCommandArgumentBytes, "command arguments")
	if err != nil {
		plan.poison(err)
		return &Node{plan: plan}
	}
	if prior, exists := plan.declarations[key]; exists {
		if prior.command.Name != cmd.def.Name || prior.command.Version != cmd.def.Version ||
			!bytes.Equal(prior.args.Bytes, encoded.Bytes) || !equivalentCommandDefaults(prior.defaults, cmd.defaults) {
			plan.poison(newError(ErrConflict, "plan", "command", key, "duplicate declaration differs"))
		}
		return &Node{plan: plan, decl: prior}
	}
	decl := &planDeclaration{key: key, command: cmd.def, defaults: cmd.defaults, args: encoded, required: true}
	plan.declarations[key] = decl
	plan.order = append(plan.order, key)
	return &Node{plan: plan, decl: decl}
}

func Fact[T any](plan *Plan, event Event[T]) (T, bool) {
	var zero T
	values := factValues(plan, event)
	if len(values) == 0 {
		return zero, false
	}
	decoded, err := event.def.Payload.Decode(values[0].Payload)
	if err != nil {
		plan.poison(newError(ErrInvalidState, "plan", "event", eventName(event.def), "retained event cannot be decoded"))
		return zero, false
	}
	result, ok := decoded.(T)
	if !ok {
		plan.poison(newError(ErrInvalidState, "plan", "event", eventName(event.def), "retained event has incompatible type"))
		return zero, false
	}
	return result, true
}

func Facts[T any](plan *Plan, event Event[T]) []T {
	values := factValues(plan, event)
	result := make([]T, 0, len(values))
	for _, value := range values {
		decoded, err := event.def.Payload.Decode(value.Payload)
		if err != nil {
			plan.poison(newError(ErrInvalidState, "plan", "event", eventName(event.def), "retained event cannot be decoded"))
			return nil
		}
		result = append(result, decoded.(T))
	}
	return result
}

func factValues[T any](plan *Plan, event Event[T]) []store.PlanEventSnapshot {
	if plan == nil {
		return nil
	}
	if event.def == nil || event.err != nil {
		plan.poison(newError(ErrInvalid, "plan", "event", eventName(event.def), "invalid event definition"))
		return nil
	}
	selector := eventReference{namespace: event.def.Namespace, name: event.def.Name, version: event.def.Version}
	plan.readSelectors[selector] = struct{}{}
	if !plan.snapshot.loadedEvents[selector] {
		plan.selectorMisses[selector] = struct{}{}
		return nil
	}
	values := plan.snapshot.events[selector]
	if len(values) == 0 {
		plan.markWaiting("event:" + selector.namespace + ":" + selector.name + ":" + strconv.Itoa(selector.version))
	}
	return values
}

func Children(plan *Plan, parentKey string) ([]string, bool) {
	command, known := planCommandForRead(plan, parentKey)
	if !known {
		return nil, false
	}
	if command.State == "succeeded" && command.ChildMembershipClosed {
		return append([]string(nil), command.Children...), true
	}
	if !isPublicTerminal(command.State) {
		plan.markWaiting("command:" + parentKey)
	}
	return nil, false
}

func Result[A, R any](plan *Plan, key string, cmd Command[A, R]) (R, bool) {
	var zero R
	command, known := matchingPlanCommand(plan, key, cmd.def)
	if !known {
		return zero, false
	}
	if command.State != "succeeded" {
		if !isPublicTerminal(command.State) {
			plan.markWaiting("command:" + key)
		}
		return zero, false
	}
	if !command.ResultLoaded {
		plan.valueMisses[command.ID] = struct{}{}
		return zero, false
	}
	decoded, err := cmd.def.Result.Decode(command.Result)
	if err != nil {
		plan.poison(newError(ErrInvalidState, "plan", "command", key, "retained result cannot be decoded"))
		return zero, false
	}
	return decoded.(R), true
}

func Outcome[A, R any](plan *Plan, key string, cmd Command[A, R]) (CommandOutcome[R], bool) {
	var result CommandOutcome[R]
	command, known := matchingPlanCommand(plan, key, cmd.def)
	if !known {
		return result, false
	}
	if !isPublicTerminal(command.State) {
		plan.markWaiting("command:" + key)
		return result, false
	}
	result.Status = CommandStatus(command.State)
	if result.Status == StatusSucceeded {
		if !command.ResultLoaded {
			plan.valueMisses[command.ID] = struct{}{}
			return CommandOutcome[R]{}, false
		}
		decoded, err := cmd.def.Result.Decode(command.Result)
		if err != nil {
			plan.poison(newError(ErrInvalidState, "plan", "command", key, "retained result cannot be decoded"))
			return CommandOutcome[R]{}, false
		}
		result.Result = decoded.(R)
	} else {
		result.Failure = &CommandFailure{Code: command.FailureCode, Message: command.FailureMessage}
		if result.Failure.Code == "" {
			result.Failure.Code = command.State
		}
	}
	return result, true
}

func planCommandForRead(plan *Plan, key string) (store.PlanCommandSnapshot, bool) {
	if plan == nil {
		return store.PlanCommandSnapshot{}, false
	}
	plan.readKeys[key] = struct{}{}
	if command, exists := plan.snapshot.commands[key]; exists {
		return command, true
	}
	if declaration, exists := plan.declarations[key]; exists {
		return store.PlanCommandSnapshot{Key: key, Name: declaration.command.Name, Version: declaration.command.Version, State: "pending"}, true
	}
	return store.PlanCommandSnapshot{}, false
}

func matchingPlanCommand(plan *Plan, key string, def *definition.Command) (store.PlanCommandSnapshot, bool) {
	command, known := planCommandForRead(plan, key)
	if !known {
		return command, false
	}
	if def == nil || command.Name != def.Name || command.Version != def.Version {
		plan.poison(newError(ErrConflict, "plan", "command", key, "read definition differs from durable command"))
		return store.PlanCommandSnapshot{}, false
	}
	return command, true
}

func (node *Node) After(keys ...string) *Node {
	return node.addGroup("all_succeeded", nil, keys)
}

func (node *Node) AfterSettled(keys ...string) *Node {
	return node.addGroup("all_settled", nil, keys)
}

func (node *Node) AfterFailed(keys ...string) *Node {
	return node.addGroup("all_failed", nil, keys)
}

func (node *Node) AfterAny(count int, keys ...string) *Node {
	return node.addGroup("at_least", &count, keys)
}

func (node *Node) addGroup(kind string, threshold *int, keys []string) *Node {
	if !node.usable("dependency") {
		return node
	}
	normalized := append([]string(nil), keys...)
	sort.Strings(normalized)
	if len(normalized) == 0 || threshold != nil && (*threshold <= 0 || *threshold > len(normalized)) {
		node.plan.poison(newError(ErrInvalid, "plan", "dependency", node.decl.key, "dependency group is empty or has an invalid threshold"))
		return node
	}
	for index, key := range normalized {
		if key == "" || index > 0 && key == normalized[index-1] || key == node.decl.key {
			node.plan.poison(newError(ErrInvalid, "plan", "dependency", node.decl.key, "dependency keys are invalid, duplicated, or self-referential"))
			return node
		}
	}
	node.decl.groups = append(node.decl.groups, planDependency{kind: kind, threshold: cloneIntPointer(threshold), keys: normalized})
	return node
}

func (node *Node) Await(events ...EventName) *Node {
	if !node.usable("await") {
		return node
	}
	seen := make(map[eventReference]struct{}, len(node.decl.waits)+len(events))
	for _, prior := range node.decl.waits {
		seen[prior] = struct{}{}
	}
	for _, event := range events {
		if event == nil {
			node.plan.poison(newError(ErrInvalid, "plan", "await", node.decl.key, "nil event selector"))
			return node
		}
		selector := event.flowEventName()
		if selector.name == "" || selector.version <= 0 || (selector.namespace != "application" && selector.namespace != "command_success") {
			node.plan.poison(newError(ErrInvalid, "plan", "await", node.decl.key, "invalid event selector"))
			return node
		}
		if _, duplicate := seen[selector]; duplicate {
			continue
		}
		seen[selector] = struct{}{}
		node.decl.waits = append(node.decl.waits, selector)
	}
	return node
}

func (node *Node) Within(duration time.Duration) *Node {
	if !node.usable("within") {
		return node
	}
	if node.decl.withinSet || duration < time.Millisecond {
		node.plan.poison(newError(ErrInvalid, "plan", "within", node.decl.key, "within must be configured once with a positive duration"))
		return node
	}
	node.decl.withinSet, node.decl.within = true, duration
	return node
}

func (node *Node) Delay(duration time.Duration) *Node {
	if !node.usable("delay") {
		return node
	}
	if node.decl.delaySet || duration < time.Millisecond {
		node.plan.poison(newError(ErrInvalid, "plan", "delay", node.decl.key, "delay must be configured once with a positive duration"))
		return node
	}
	node.decl.delaySet, node.decl.delay = true, duration
	return node
}

func (node *Node) Optional() *Node {
	if !node.usable("optional") {
		return node
	}
	if node.decl.optionalSet {
		node.plan.poison(newError(ErrInvalid, "plan", "optional", node.decl.key, "optional configured more than once"))
		return node
	}
	node.decl.optionalSet, node.decl.required = true, false
	return node
}

func (node *Node) MaxAttempts(max int) *Node {
	if !node.usable("max attempts") {
		return node
	}
	if node.decl.maxAttemptsSet || node.decl.retrySet || max <= 0 {
		node.plan.poison(newError(ErrInvalid, "plan", "retry policy", node.decl.key, "max attempts is invalid or duplicated"))
		return node
	}
	node.decl.maxAttemptsSet = true
	node.decl.maxAttempts = &max
	return node
}

func (node *Node) RetryPolicy(policy RetryPolicy) *Node {
	if !node.usable("retry policy") {
		return node
	}
	if node.decl.retrySet || node.decl.maxAttemptsSet || validateRetryPolicy(policy) != nil {
		node.plan.poison(newError(ErrInvalid, "plan", "retry policy", node.decl.key, "retry policy is invalid or duplicated"))
		return node
	}
	copy := cloneRetryPolicy(policy)
	node.decl.retrySet, node.decl.retryOverride = true, &copy
	return node
}

func (node *Node) usable(operation string) bool {
	if node == nil || node.plan == nil || node.decl == nil {
		return false
	}
	if node.plan.firstError != nil {
		return false
	}
	_ = operation
	return true
}

func (plan *Plan) poison(err error) {
	if plan != nil && plan.firstError == nil {
		plan.firstError = err
	}
}

func (plan *Plan) markWaiting(key string) {
	if plan == nil {
		return
	}
	if _, exists := plan.waitingReadKeys[key]; exists {
		return
	}
	plan.waitingReadKeys[key] = struct{}{}
	plan.waitingReads++
}

func (plan *Plan) waitingDiagnostics() []string {
	result := make([]string, 0, min(32, len(plan.waitingReadKeys)))
	for key := range plan.waitingReadKeys {
		result = append(result, key)
	}
	sort.Strings(result)
	if len(result) > 32 {
		result = result[:32]
	}
	return result
}

func (plan *Plan) missingSelectors() []store.PlanEventSelector {
	result := make([]store.PlanEventSelector, 0, len(plan.selectorMisses))
	for selector := range plan.selectorMisses {
		result = append(result, store.PlanEventSelector{Namespace: selector.namespace, Name: selector.name, Version: selector.version})
	}
	sort.Slice(result, func(i, j int) bool {
		if result[i].Namespace != result[j].Namespace {
			return result[i].Namespace < result[j].Namespace
		}
		if result[i].Name != result[j].Name {
			return result[i].Name < result[j].Name
		}
		return result[i].Version < result[j].Version
	})
	return result
}

func (plan *Plan) missingValues() []uuid.UUID {
	result := make([]uuid.UUID, 0, len(plan.valueMisses))
	for id := range plan.valueMisses {
		result = append(result, id)
	}
	sort.Slice(result, func(i, j int) bool { return bytes.Compare(result[i][:], result[j][:]) < 0 })
	return result
}

func (plan *Plan) complete() bool { return len(plan.selectorMisses) == 0 && len(plan.valueMisses) == 0 }

func equivalentCommandDefaults(a, b commandDefaults) bool {
	if a.queue != b.queue || a.attemptTimeout != b.attemptTimeout {
		return false
	}
	left, leftErr := retrypolicy.CanonicalPublic(a.retryPolicy)
	right, rightErr := retrypolicy.CanonicalPublic(b.retryPolicy)
	return leftErr == nil && rightErr == nil && bytes.Equal(left.Bytes, right.Bytes)
}

func isPublicTerminal(state string) bool {
	switch CommandStatus(state) {
	case StatusSucceeded, StatusFailed, StatusCancelled, StatusExpired, StatusSkipped:
		return true
	default:
		return false
	}
}

func (plan *Plan) validate() error {
	if plan.firstError != nil {
		return plan.firstError
	}
	for key := range plan.readKeys {
		if _, durable := plan.snapshot.commands[key]; !durable {
			if _, declared := plan.declarations[key]; !declared {
				return newError(ErrInvalid, "plan", "read", key, "command key is neither durable nor declared")
			}
		}
	}
	known := make(map[string]struct{}, len(plan.snapshot.commands)+len(plan.declarations))
	for key := range plan.snapshot.commands {
		known[key] = struct{}{}
	}
	for key := range plan.declarations {
		known[key] = struct{}{}
	}
	for _, decl := range plan.declarations {
		if decl.withinSet && len(decl.waits) == 0 {
			return newError(ErrInvalid, "plan", "within", decl.key, "Within requires Await")
		}
		for _, group := range decl.groups {
			for _, key := range group.keys {
				if _, exists := known[key]; !exists {
					return newError(ErrInvalid, "plan", "dependency", key, "dependency key does not exist")
				}
			}
		}
	}
	if cycle := planCycle(plan.declarations); cycle != "" {
		return newError(ErrInvalid, "plan", "dependency cycle", cycle, "plan dependency graph contains a cycle")
	}
	return nil
}

func planCycle(declarations map[string]*planDeclaration) string {
	state := make(map[string]uint8, len(declarations))
	var visit func(string) string
	visit = func(key string) string {
		if state[key] == 1 {
			return key
		}
		if state[key] == 2 {
			return ""
		}
		state[key] = 1
		for _, group := range declarations[key].groups {
			for _, dependency := range group.keys {
				if _, newCommand := declarations[dependency]; newCommand {
					if cycle := visit(dependency); cycle != "" {
						return cycle
					}
				}
			}
		}
		state[key] = 2
		return ""
	}
	for key := range declarations {
		if cycle := visit(key); cycle != "" {
			return cycle
		}
	}
	return ""
}

type planDecisionBody struct {
	V               int                   `json:"v"`
	Revision        int64                 `json:"revision"`
	ConsumedThrough int64                 `json:"consumed_through"`
	WaitingReads    int                   `json:"waiting_reads"`
	Quiescent       bool                  `json:"quiescent"`
	Declarations    []planDecisionCommand `json:"declarations,omitempty"`
}

type planDecisionCommand struct {
	Key         string `json:"key"`
	CommandID   string `json:"command_id"`
	Fingerprint string `json:"fingerprint"`
}

func canonicalPlanDecision(value planDecisionBody) (canonical.Value, error) {
	return canonical.Marshal(value, 0)
}

func planWaitingDiagnostics(plan *Plan) canonical.Value {
	value, _ := canonical.Marshal([]string{}, 0)
	return value
}

func buildPlanReconciliation(snapshot store.PlanSnapshot, plan *Plan) (store.PlanReconciliation, error) {
	if err := plan.validate(); err != nil {
		return store.PlanReconciliation{}, err
	}
	ids := make(map[string]store.DependencyMemberCreate, len(snapshot.Commands)+len(plan.declarations))
	for _, command := range snapshot.Commands {
		ids[command.Key] = store.DependencyMemberCreate{CommandID: command.ID, Key: command.Key}
	}
	for _, key := range plan.order {
		if _, exists := ids[key]; !exists {
			ids[key] = store.DependencyMemberCreate{CommandID: uuid.New(), Key: key}
		}
	}
	request := store.PlanReconciliation{
		ExpectedRevision: snapshot.Revision, ConsumedThrough: snapshot.JournalThrough,
		WaitingReads: plan.waitingReads, WaitingOn: plan.waitingDiagnostics(),
	}
	for _, key := range plan.order {
		decl := plan.declarations[key]
		fingerprint, err := planDeclarationFingerprint(decl)
		if err != nil {
			return store.PlanReconciliation{}, err
		}
		if existing, exists := plan.snapshot.commands[key]; exists {
			if existing.Origin != "plan" {
				return store.PlanReconciliation{}, newError(ErrConflict, "plan", "command", key, "plan cannot own a command created by another source")
			}
			if existing.Name != decl.command.Name || existing.Version != decl.command.Version ||
				!bytes.Equal(existing.Args, decl.args.Bytes) || !bytes.Equal(existing.DeclarationFingerprint, fingerprint[:]) {
				return store.PlanReconciliation{}, newError(ErrConflict, "plan", "command", key, "durable declaration differs")
			}
			continue
		}
		defaults := decl.defaults
		if decl.maxAttempts != nil {
			defaults.retryPolicy = attemptRetryPolicy(*decl.maxAttempts)
		} else if decl.retryOverride != nil {
			defaults.retryPolicy = cloneRetryPolicy(*decl.retryOverride)
		}
		command, err := prepareCommand(ids[key].CommandID, key, decl.command, defaults, decl.args, "plan")
		if err != nil {
			return store.PlanReconciliation{}, err
		}
		command.DeclarationFingerprint = fingerprint
		command.Required = decl.required
		// Once an execution is failing, every genuinely new plan declaration is
		// part of the newly selected recovery decision. Existing unrelated work
		// was already handled by fail-fast; treating the new delta as one scope
		// avoids trying to infer Go control-flow provenance from payloads.
		command.FailureScope = snapshot.Status == "failing"
		if decl.delay > 0 {
			command.ScheduleKind, command.InitialDelay = "plan_delay", decl.delay
		}
		command.Within = decl.within
		for _, group := range decl.groups {
			value := store.DependencyGroupCreate{ID: uuid.New(), Kind: group.kind, Threshold: cloneIntPointer(group.threshold)}
			for _, dependencyKey := range group.keys {
				value.Members = append(value.Members, ids[dependencyKey])
			}
			command.Dependencies = append(command.Dependencies, value)
		}
		for _, wait := range decl.waits {
			command.Waits = append(command.Waits, store.EventWaitCreate{
				Namespace: wait.namespace, Name: wait.name, Version: wait.version,
			})
		}
		request.Commands = append(request.Commands, command)
	}
	return request, nil
}

func planDeclarationFingerprint(decl *planDeclaration) ([32]byte, error) {
	type group struct {
		Kind      string   `json:"kind"`
		Threshold *int     `json:"threshold,omitempty"`
		Keys      []string `json:"keys"`
	}
	type wait struct {
		Namespace string `json:"namespace"`
		Name      string `json:"name"`
		Version   int    `json:"version"`
	}
	value := struct {
		V             int             `json:"v"`
		Key           string          `json:"key"`
		Name          string          `json:"name"`
		Version       int             `json:"version"`
		Args          json.RawMessage `json:"args"`
		Required      bool            `json:"required"`
		Groups        []group         `json:"groups,omitempty"`
		Waits         []wait          `json:"waits,omitempty"`
		WithinMS      int64           `json:"within_ms,omitempty"`
		DelayMS       int64           `json:"delay_ms,omitempty"`
		MaxAttempts   *int            `json:"max_attempts,omitempty"`
		RetryOverride json.RawMessage `json:"retry_override,omitempty"`
	}{
		V: 1, Key: decl.key, Name: decl.command.Name, Version: decl.command.Version,
		Args: json.RawMessage(decl.args.BytesCopy()), Required: decl.required,
		WithinMS: decl.within.Milliseconds(), DelayMS: decl.delay.Milliseconds(),
		MaxAttempts: cloneIntPointer(decl.maxAttempts),
	}
	for _, dependency := range decl.groups {
		value.Groups = append(value.Groups, group{Kind: dependency.kind, Threshold: cloneIntPointer(dependency.threshold), Keys: append([]string(nil), dependency.keys...)})
	}
	sort.Slice(value.Groups, func(i, j int) bool {
		left, _ := canonical.Marshal(value.Groups[i], 0)
		right, _ := canonical.Marshal(value.Groups[j], 0)
		return bytes.Compare(left.Bytes, right.Bytes) < 0
	})
	for _, selector := range decl.waits {
		value.Waits = append(value.Waits, wait{Namespace: selector.namespace, Name: selector.name, Version: selector.version})
	}
	sort.Slice(value.Waits, func(i, j int) bool {
		if value.Waits[i].Namespace != value.Waits[j].Namespace {
			return value.Waits[i].Namespace < value.Waits[j].Namespace
		}
		if value.Waits[i].Name != value.Waits[j].Name {
			return value.Waits[i].Name < value.Waits[j].Name
		}
		return value.Waits[i].Version < value.Waits[j].Version
	})
	if decl.retryOverride != nil {
		policy, err := retrypolicy.CanonicalPublic(*decl.retryOverride)
		if err != nil {
			return [32]byte{}, err
		}
		value.RetryOverride = json.RawMessage(policy.BytesCopy())
	}
	encoded, err := canonical.Marshal(value, 0)
	if err != nil {
		return [32]byte{}, newError(ErrInvalid, "plan", "command", decl.key, "declaration cannot be canonicalized")
	}
	return encoded.Digest, nil
}

func planEvaluationFingerprint(plan *Plan) (canonical.Value, error) {
	if err := plan.validate(); err != nil {
		return canonical.Value{}, err
	}
	type declaration struct {
		Key         string `json:"key"`
		Fingerprint string `json:"fingerprint"`
	}
	type read struct {
		Kind         string `json:"kind"`
		Identity     string `json:"identity"`
		Availability string `json:"availability"`
	}
	value := struct {
		V            int           `json:"v"`
		Declarations []declaration `json:"declarations"`
		Reads        []read        `json:"reads"`
		Waiting      int           `json:"waiting"`
	}{V: 1, Waiting: plan.waitingReads}
	keys := append([]string(nil), plan.order...)
	sort.Strings(keys)
	for _, key := range keys {
		fingerprint, err := planDeclarationFingerprint(plan.declarations[key])
		if err != nil {
			return canonical.Value{}, err
		}
		value.Declarations = append(value.Declarations, declaration{Key: key, Fingerprint: fmt.Sprintf("%x", fingerprint)})
	}
	for key := range plan.readKeys {
		availability := "temporary"
		if command, exists := plan.snapshot.commands[key]; exists {
			switch {
			case command.State == "succeeded":
				availability = "available"
			case isPublicTerminal(command.State):
				availability = "permanent"
			}
		}
		value.Reads = append(value.Reads, read{Kind: "command", Identity: key, Availability: availability})
	}
	for selector := range plan.readSelectors {
		availability := "temporary"
		if len(plan.snapshot.events[selector]) > 0 {
			availability = "available"
		}
		value.Reads = append(value.Reads, read{
			Kind: "event", Identity: selector.namespace + ":" + selector.name + ":" + strconv.Itoa(selector.version),
			Availability: availability,
		})
	}
	sort.Slice(value.Reads, func(i, j int) bool {
		if value.Reads[i].Kind != value.Reads[j].Kind {
			return value.Reads[i].Kind < value.Reads[j].Kind
		}
		return value.Reads[i].Identity < value.Reads[j].Identity
	})
	return canonical.Marshal(value, 0)
}

type erasedPlan struct {
	def    *definition.Plan
	invoke func(*Plan, any) error
}

func (p PlanDef[A]) flowRegistration() registrationData {
	name, version := "", 0
	if p.def != nil {
		name, version = p.def.Name, p.def.Version
	}
	erased := erasedPlan{def: p.def}
	if p.invoke != nil {
		erased.invoke = func(plan *Plan, args any) error {
			typed, ok := args.(A)
			if !ok {
				return newError(ErrInvalid, "plan", "execution", name, "argument type mismatch")
			}
			p.invoke(plan, typed)
			return nil
		}
	}
	validation := p.err
	if p.def == nil {
		validation = errors.Join(validation, errors.New("zero plan definition"))
	}
	return registrationData{
		kind: planRegistrationKind, name: name, version: version,
		value: erased, validation: validation,
	}
}
