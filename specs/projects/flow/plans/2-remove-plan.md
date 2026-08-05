---
status: complete
completed_at: 2026-08-04
---

# Plan: remove plans and make event-gated commands first-class

## 1. Purpose

Remove Flow's plan feature before the public API and durable formats are frozen. Replace plan-driven orchestration with a smaller model built from:

```text
commands + workers + coordinators + execution-scoped events
```

The target is not merely to hide plans behind a different name. `Plan`, plan reconciliation, declarative dependencies, plan snapshots, fact reads, and plan-specific storage are deleted. The use cases currently demonstrated by plans move to one of three explicit shapes:

1. a command starts directly;
2. a worker stages a continuation or bounded fan-out as part of its successful decision;
3. a coordinator consumes events and command outcomes, keeps bounded durable state, and stages the next commands.

Exact event waits remain valuable independently of plans. They become a first-class command readiness feature available to direct root commands and to commands staged by workers or coordinators.

This is a pre-release breaking refactor. Removed names receive no compatibility aliases, old plan histories are not migrated, and obsolete behavior is removed from code, schema, replay, inspection, testing helpers, examples, benchmarks, and documentation.

## 2. Controlling decision

This plan is a follow-up amendment to the current Flow project artifacts and to `plans/1-smaller.md`. Where those documents retain plans, declarative dependencies, plan reads, or plan reconciliation, this plan supersedes them.

The architectural decision is:

> Flow does not provide a declarative orchestration language in its first release. Commands perform work, coordinators make durable event-driven decisions, and exact event gates control when already-created commands become eligible to run.

Plans would earn their cost only if declarative dependencies, dynamic joins, and fact-driven branching were core product features. They are not part of the target product. Coordinators are the single mechanism for joins, branching, races, and open-ended event-driven behavior.

## 3. Why this is smaller

Plans currently introduce a second decision model beside coordinators:

- pure functions that are repeatedly evaluated over retained snapshots;
- consulted-read tracking and lazy snapshot expansion;
- dirty, quiescent, revision, and waiting-read execution state;
- a reconciliation scheduler, leases, fixed-point evaluation, and optional double evaluation;
- declarative dependency groups and reverse dependency resolution;
- separate plan failure and defect semantics;
- plan-specific journal entries, replay state, inspection fields, tests, fault points, benchmarks, configuration, and documentation.

Commands, coordinators, and events already provide an expressive durable model:

- workers stage follow-up commands and facts atomically with success;
- coordinators receive typed command outcomes and application events;
- coordinator state durably represents pending membership, joins, branches, and accumulated results;
- exact event gates keep commands off the worker queue until their facts exist;
- the ordered execution journal preserves recovery, observation, and replay.

The complexity of a dynamic join becomes visible application coordinator code. In exchange, Flow loses an entire scheduler and evaluation model, two dependency tables, plan projection state, and a large set of public concepts.

## 4. Target developer model

### 4.1 Choose the smallest orchestration shape

| Need | Use |
|---|---|
| One independent operation | `Command.Execute` |
| A successful worker always continues with known work | `flow.Execute(work, ...)` |
| A successful worker discovers bounded parallel work and no join is required | stage children from the worker |
| A command must not run until exact execution-scoped facts exist | `WaitFor` and optional `Within` |
| A fact's payload determines command arguments or branching | coordinator `OnEvent` |
| Later work depends on one or more command outcomes | coordinator `OnOutcome` |
| Dynamic fan-out, join, quorum, race, or loop | coordinator state plus `OnOutcome`/`OnEvent` |

There is no separate plan-versus-coordinator choice.

### 4.2 Continuations

A worker that has all information needed for the next step stages it before returning:

```go
func handlePrepare(ctx context.Context, work *flow.Work[PrepareArgs]) (PrepareResult, error) {
    result := prepare(work.Args)
    flow.Execute(work, "publish", Publish, PublishArgs{Report: result.Report})
    return result, nil
}
```

The child and parent success remain one atomic settlement. A failed, panicked, cancelled, or fenced parent exposes no child.

### 4.3 Event-gated direct command

A direct root command may be created in a pending state and released by exact facts emitted into that execution:

```go
handle, err := ConfirmBridge.Execute(
    ctx,
    "bridge/example",
    flow.None{},
    flow.WaitFor(BridgeDelivered, "delivery/example"),
    flow.Within(2*time.Minute),
)
```

After `Execute` returns the handle, another process can call:

```go
err := BridgeDelivered.Emit(
    ctx,
    publisher,
    handle.ID,
    "delivery/example",
    delivery,
)
```

The command consumes no worker slot, command lease, or long-held connection while it waits.

### 4.4 Event-gated staged command

Workers and coordinators use the same command-node vocabulary:

```go
flow.Execute(coordination, "confirm", ConfirmBridge, args).
    WaitFor(BridgeDelivered, "delivery/example").
    Within(2 * time.Minute)
```

This stages the command, exact waits, and coordinator state transition in one transaction.

### 4.5 Payload-driven events

`WaitFor` is a readiness gate. It proves that an exact fact exists, but it does not inject the event payload into command arguments. When the payload is needed, a coordinator handles the event and explicitly passes typed data to a command:

```go
flow.OnEvent(BridgeDelivered, func(
    ctx context.Context,
    coordination *flow.Coordination[BridgeState],
    delivery flow.Received[BridgeDelivery],
) error {
    flow.Execute(coordination, "confirm/"+delivery.Key, ConfirmBridge, ConfirmArgs{
        TransactionHash: delivery.Payload.TransactionHash,
    })
    return nil
})
```

This keeps command inputs explicit and prevents event lookup semantics from becoming a second implicit data-flow system.

## 5. Public API target

### 5.1 Remove plan definitions and execution

Delete:

```go
type Plan
type PlanDef[A any]
func DefinePlan[A any](...)
func (PlanDef[A]) Execute(...)
func (PlanDef[A]) With(...)
func (PlanDef[A]) Name() string
func (PlanDef[A]) Version() int
```

Remove plans from `Registration`, registry validation, runtime startup, execution discovery, and all public documentation. The only execution driver modes are `direct` and `coordinator`.

### 5.2 Remove plan reads and declarative dependencies

Delete:

```go
func Fact[T any](*Plan, Event[T], string) (T, bool)
func Facts[T any](*Plan, Event[T]) []T

func (*Node[R]) Outcome() (Outcome[R], bool)
func (*Node[R]) Children() ([]string, bool)
func (*Node[R]) After(...string) *Node[R]
func (*Node[R]) AfterSettled(...string) *Node[R]
func (*Node[R]) AfterFailed(...string) *Node[R]
```

There are no forward references, dependency predicates, consulted reads, dynamic child-membership reads, or fact-driven re-evaluations after this refactor.

The orchestration replacements are deliberate:

- a continuation receives data explicitly in its command arguments;
- a coordinator receives a typed `Outcome[R]` through `OnOutcome`;
- a coordinator tracks dynamic membership in bounded state;
- a coordinator consumes application facts through `OnEvent`;
- operator and test code reads retained results from `ExecutionTrace`.

### 5.3 Retain typed outcomes for coordinators and traces

Retain:

```go
type Outcome[R any] struct { ... }
func OnOutcome[S, A, R any](Command[A, R], handler) Handler[S]
```

`Outcome[R]` remains the typed terminal value delivered to coordinators.

Retain `ResultOf` and `OutcomeOf` as typed trace-reading conveniences. `ResultSource` remains sealed and is implemented only by `ExecutionTrace`; `Work` stops implementing it. Remove worker dependency snapshots, `flowtest.WithDependencies`, and all documentation that presents prior-command lookup as worker orchestration.

### 5.4 Simplify `Node`

After typed plan reads disappear, a node no longer uses its result type. Replace the generic plan/decision union with one sealed decision builder:

```go
type Node struct { /* unexported decision scope and key */ }

func Execute[A, R any](scope Scope, key string, command Command[A, R], args A) *Node
func (n *Node) Key() string
func (n *Node) Optional() *Node
func (n *Node) Delay(time.Duration) *Node
func (n *Node) WaitFor(EventRef, string) *Node
func (n *Node) Within(time.Duration) *Node
```

`Scope` is implemented only by `*Work[A]` and `*Coordination[S]`. A node is ephemeral, valid only during the enclosing handler decision, and must not be retained or shared between goroutines.

Every modifier updates only the staged decision. It performs no database work while a handler runs. Validation errors poison the whole decision so no partial commands, events, coordinator state, or worker success can commit.

### 5.5 Add direct-root gate options

Add execution options with the same vocabulary as staged nodes:

```go
func WaitFor(event EventRef, key string) ExecutionOption
func Within(duration time.Duration) ExecutionOption
```

These options are accepted only by `Command.Execute`. `Coordinator.Execute` rejects them because a coordinator can stage a gated command from `OnStart`. `Within` requires at least one `WaitFor` in the same command declaration.

`WaitFor` and `Within` options are part of the execution's start identity. Re-executing an existing execution key with different gate configuration is a durable conflict, matching the staged-command rule. Under `WithLiveKey`, a live execution is rediscovered without comparing start identity, per existing live-key semantics.

`WithStartDelay` remains direct-only. When used with event gates, delay and waits are independent readiness requirements.

### 5.6 Remove dependency-only result plumbing

Remove from worker execution:

```go
func (*Work[A]) flowResultSource() ...
```

Remove the internal dependency snapshot passed to a worker attempt. A worker receives only its typed arguments, command information, context, and decision scope. Results needed by later work must be passed as command arguments or handled by a coordinator.

### 5.7 Remove plan-only statuses and types

Delete:

- `PlanRevision`;
- `StatusSkipped`, because unsatisfiable dependencies were its only source;
- plan registration kinds and definition kinds;
- plan-specific errors, observations, and fault points.

Retain `StatusSucceeded`, `StatusFailed`, `StatusCancelled`, and `StatusExpired` for commands.

## 6. Exact event-gate semantics

### 6.1 Identity and scope

A gate matches exactly:

```text
(execution ID, application event name, non-empty event key)
```

Events are retained facts inside one execution. There are no global subscriptions, wildcard selectors, prefix selectors, cross-execution matching, or event-created executions.

Only application events are valid `WaitFor` operands. Command terminal events continue to feed coordinator `OnOutcome`; they are not generic event-gate selectors.

### 6.2 Declaration rules

- One command may declare multiple waits; all must be satisfied.
- Repeating an identical `(event,key)` wait coalesces.
- Empty, invalid, or oversized keys poison the declaration.
- Repeating an identical `Within` coalesces; configuring a different value poisons the declaration. `Within` requires at least one wait.
- `Within` must be at least one millisecond.
- Within one worker/coordinator decision, repeated `Execute` calls with the same base definition, arguments, defaults, and key address one staged command. Their distinct waits merge additively as AND gates; identical waits coalesce.
- Singleton modifiers retain conflict semantics inside one decision: identical `Within` or `Delay` values coalesce, different values poison the decision, and `Optional` is idempotent.
- When a complete staged declaration is compared with an already-durable command under the same key, requiredness, delay, waits, and wait timeout must all match. A different durable declaration is a conflict.
- Waits are normalized deterministically before command fingerprints and journal bodies are produced.

### 6.3 Readiness and timing

Command acceptance and gate creation are atomic.

At acceptance:

1. matching retained events satisfy their waits immediately;
2. unmatched waits remain pending;
3. when at least one wait remains unmatched, `Within` starts once at acceptance using PostgreSQL time and is capped by the execution deadline;
4. an initial delay runs independently from the waits;
5. retry budget starts only when every wait is satisfied and the initial delay has elapsed.

The resulting eligible time is the later of the delay boundary and the time the last wait is satisfied. Waiting time does not consume retry budget.

Because `Within` starts at acceptance and the initial delay is an independent requirement, a wait may expire while the initial delay is still pending; the command then terminalizes as `expired` without ever having been eligible.

An application event staged in the same successful worker/coordinator decision may satisfy the new command in that transaction. Journal ordering and command insertion must make this result deterministic.

### 6.4 Deadline races

The persisted deadline decides an event-versus-timeout race:

- an event committed at or before the deadline wins, even if timeout maintenance observes it later;
- an event committed after the deadline cannot resurrect the command;
- the execution row lock and PostgreSQL time serialize the semantic winner;
- timeout is recorded as command status `expired` with failure code `wait_expired`;
- expiry of a required command drives normal required-failure handling;
- expiry of an optional command is observable but does not independently fail the execution.

### 6.5 Resource behavior

A pending gate owns no worker delivery, worker semaphore, command lease, or dedicated database connection. PostgreSQL rows plus notification hints and bounded polling are the durable wait mechanism.

## 7. Coordinator replacements for plan behavior

### 7.1 Branching

Plans previously branched by repeatedly reading facts and outcomes. A coordinator branches once when it receives the relevant durable event:

- `OnEvent(event)` decodes the typed application payload;
- `OnOutcome(command)` decodes every terminal command state;
- the handler updates bounded state and stages commands atomically;
- the inbox position prevents accepted history from being silently replayed as a new decision.

No pure re-evaluation or consulted-read tracking is needed.

### 7.2 Dynamic join

A dynamic join is explicit coordinator state. For the fan-out example, use this sequence:

1. `OnStart` stages `prepareReport`.
2. `prepareReport` performs discovery and returns a bounded typed list of parts or durable references; it does not spawn analysis children.
3. `OnOutcome(prepareReport)` stores a pending key set and stages one optional `analyzePart` command per part.
4. `OnOutcome(analyzePart)` removes the delivered key, records or aggregates its typed result, and records failures according to application policy.
5. When the pending set is empty, the same coordinator decision stages `generateReport` with explicit aggregate data or references.
6. `OnOutcome(generateReport)` calls `Succeed` on success or `Fail` on failure.

Discovery must produce unique stable part identities. Duplicate identities are a coordinator decision defect rather than silently reducing the fan-out. The zero-part case stages `generateReport` directly from `OnOutcome(prepareReport)` so it cannot wait for an outcome that will never exist. `OnOutcome(analyzePart)` must reject an unknown key and coalesce an already-consumed delivery through the coordinator inbox's durable position semantics.

Analysis commands are optional when the coordinator, rather than Flow's required-command policy, owns partial-failure behavior.

Large inputs and results must not be copied into coordinator state. Workers persist large values in application storage and return stable references. The existing 256 KiB coordinator-state limit remains.

### 7.3 Static fan-out without a join

A worker may still stage a bounded set of children. Their creation and parent success commit atomically. Because plan `Children()` is removed, child membership is not an application read API. Stable keys, trace, and journal history remain available for inspection.

### 7.4 Failure handling

Required commands retain Flow's execution failure semantics. Coordinators that need to prevent a command failure from automatically moving the execution into `failing` should stage that command as optional, consume its `OnOutcome`, and explicitly call `Fail` or continue. A required command outcome is still delivered to an active coordinator; using `Optional` is about ownership of failure policy, not visibility.

This avoids preserving dependency-specific failure scopes or `AfterFailed` branches.

The reduced fail-fast policy is explicit:

- when a required command fails with fail-fast enabled, pending, ready, and retry-wait commands are cancelled;
- attempts already running may settle because their external side effects cannot be assumed abortable;
- their result and staged application events may commit, but commands they stage after the execution entered `failing` are created and cancelled in the same settlement transaction and never become runnable;
- the coordinator remains active to consume terminal outcomes and make an explicit terminal decision;
- with fail-fast disabled, already accepted work and descendants staged by its successful settlements continue normally until the execution reaches its mode-specific terminal condition.

These rules replace `failure_scope`; no durable failure-branch marker remains.

## 8. Runtime and engine deletion

Delete the plan runtime as a subsystem:

- `plan.go` and `plan_runtime.go`;
- plan erasure, definition registration, and registry maps;
- dirty-plan probes, claims, leases, renewals, snapshot loading, reconciliation, and failure paths;
- fixed-point declaration evaluation and deterministic double evaluation;
- consulted-read and snapshot expansion logic;
- plan reconciliation semaphores and goroutines;
- `WithPlanConcurrency` and `WithPlanVerification`;
- plan observer operations and internal fault points;
- plan wakeups on command settlement and application event ingress.

`Runtime.Run` retains only:

- command delivery and retry scheduling;
- coordinator inbox delivery;
- exact wait expiry;
- execution deadline processing;
- command/coordinator lease renewal and takeover;
- transactional notification hints plus correctness-preserving polling.

The command readiness engine becomes wait-and-delay focused. Remove dependency graph loading, group evaluation, unsatisfiable propagation, skipped-command creation, and failure-scope closure. Rename internal graph code so retained names describe waits/readiness rather than a graph that no longer exists.

## 9. Storage target

Because Flow is pre-release, edit the baseline migrations in place and recreate development schemas. Do not add compatibility columns, tombstone tables, or data conversion for plan executions.

### 9.1 Retained tables

The target schema has seven Flow tables:

1. `flow_executions`
2. `flow_commands`
3. `flow_command_queue`
4. `flow_command_event_waits`
5. `flow_coordinators`
6. `flow_journal`
7. `flow_schema_migrations`

### 9.2 Delete dependency storage

Delete:

- `flow_command_dependency_groups`;
- `flow_command_dependency_members`;
- every dependency index, foreign key, row codec, snapshot, query, and resolution path.

From `flow_commands`, delete:

- `unsatisfied_groups`;
- `failure_scope`; section 7.4 defines the simpler behavior for children staged by attempts that survive fail-fast;
- `child_membership_closed`, which existed as a plan-readable projection;
- dependency fields from the declaration fingerprint and `CommandCreated` body.

Retain `unsatisfied_waits`, exact wait rows, and wait timing fields.

Remove `skipped` from command status constraints and journal terminal-status constraints.

### 9.3 Delete plan execution projection

From `flow_executions`, delete:

- `plan_dirty`;
- `plan_dirty_since`;
- `plan_quiescent`;
- `plan_revision`;
- `plan_waiting_count`;
- `plan_waiting_on`;
- plan-field constraints and the dirty-plan queue index.

Restrict `driver_mode` to `direct|coordinator`.

### 9.4 Simplify command scheduling projection

Remove the plan-specific command origin and schedule terminology:

- remove command origin `plan`;
- retain `direct_root`, `worker_child`, and `coordinator_command`;
- replace `plan_delay`/`execute_delay` distinctions with one nullable initial-delay representation;
- remove `schedule_kind`; once `plan_delay` is gone, `initial_delay_ms IS NULL` fully represents no delay.

The public trace should expose `InitialDelay`, not an obsolete internal schedule kind.

### 9.5 Delete plan journal shape

Delete:

- journal entry kind `plan_reconciled`;
- journal column `plan_revision`;
- event class `plan_terminal`;
- `flow.plan_failed` and other plan terminal events;
- plan revision/terminal uniqueness indexes and shape constraints;
- plan reconciliation bodies and codecs.

Retain command creation, attempts, application events, command/execution terminal events, and coordinator transitions. `CommandCreated` continues to record exact waits, optional `within_ms`, and initial delay so replay can reconstruct readiness.

## 10. Replay, inspection, and observability

Replay must project only the retained execution model.

Remove from replay and live/projection conformance:

- dirty, quiescent, revision, waiting-read, and plan-mode state;
- plan reconciliation application;
- dependency groups, dependency resolution, and skipped terminality;
- child-membership closure as a public projection;
- plan terminal events and failure codes.

Retain and verify:

- direct and coordinator execution state;
- command origin, arguments, delay, waits, readiness, attempts, and terminal result;
- application-event identity and payload;
- coordinator transitions and inbox progress;
- command/execution terminal events and causation.

Remove from `Execution`, `ExecutionTrace`, `TraceCommand`, and history:

- every `Plan*` field;
- `TraceDependencyGroup` and dependency collections;
- `UnsatisfiedGroups`;
- `ChildMembershipClosed`;
- plan revisions on history entries;
- obsolete schedule-kind values.

Retain wait descriptors and wait start/deadline fields because they explain why a command is pending.

Observers lose plan claim/evaluate/reconcile operations. Retained observations must distinguish command work, coordinator decisions, wait expiry, execution expiry, lease recovery, and administrative actions.

## 11. `flowtest` target

Delete plan simulation support:

- `RunPlan`;
- `Simulate`;
- `AssertPlanDeterministic`;
- `PlanWorld`, `PlanResult`, `PlanCommand`, `PlanEvent`, declaration/read types, and plan fixtures;
- the plan operation from the internal testing bridge.

Delete worker dependency fixtures:

- `Dependency`;
- `Succeeded`/`Failed` dependency constructors;
- `WithDependencies`;
- dependency snapshots in test-engine requests.

Retain `RunWorker`, `RunCommit`, and `RunCoordinator`. Extend `StagedCommand` with normalized exact waits and `Within` so unit tests can assert event-gated decisions without PostgreSQL.

Retain `flowtest/replaytest`. It is the right home for reusable replay/live conformance checks; remove only its plan fields and plan-history assumptions.

## 12. Example migrations

### 12.1 `examples/direct`

Keep the direct example as the smallest one-command execution. It should not introduce a coordinator or event gate.

### 12.2 `examples/agent`

Keep the coordinator-based agent. Remove only plan-related runtime configuration or commentary if present.

### 12.3 `examples/monitor`

Replace `bridgePlan` with a direct event-gated `confirmBridge` execution:

```go
handle, err := confirmBridge.With(runtime).Execute(
    ctx,
    "bridge/example",
    flow.None{},
    flow.WaitFor(bridgeDelivered, "delivery/example"),
    flow.Within(2*time.Second),
)
```

The external monitor still receives the execution ID and emits the exact fact through a lightweight publisher runtime. Register only the worker. Update the example comments to say the publisher and worker runtime can be deployed independently; do not refer to a plan processor.

### 12.4 `examples/fanout`

Replace `reportPlan` with a `reportCoordinator` following the dynamic-join sequence in section 7.2. The example should make the coordinator state and handlers self-documenting in `examples/fanout`; no core example logic moves into `internal/examples`.

The final worker receives the accumulated total or stable result references explicitly. It must not use `ResultOf(work, ...)`.

Use the same construction and lifecycle conventions as the direct, agent, and monitor examples:

- one `newFlowRuntime` constructor;
- one `runFlowRuntime` helper called from `main`;
- one clearly named `runExampleCommand` entry point;
- handler functions declared separately rather than anonymous workers in setup.

## 13. Documentation synchronization

Update all controlling project artifacts in the same implementation series:

- `project_overview.md` presents direct commands and coordinators as the complete model;
- `functional_spec.md` removes plan graph semantics and specifies event-gated commands;
- `architecture.md` removes reconciliation and dependency components;
- `components/engine.md` specifies wait/delay readiness and two-mode completion;
- `components/runtime.md` removes plan pools, claims, leases, and configuration;
- `components/schema.md` describes the seven-table target;
- `implementation_plan.md`, phase plans, acceptance evidence, and benchmark evidence remove obsolete plan deliverables;
- package documentation, README material, examples, and Go comments use the new vocabulary.

Historical review files under `specs/projects/flow/reviews` are source material and must not be rewritten as part of implementation unless a separate task explicitly requests it.

At implementation start, mark affected completed project artifacts `draft` so they do not continue to present the superseded plan model as an approved design. This plan remains the controlling amendment until the documents are synchronized. Mark them `complete` again only after their content and acceptance evidence describe the retained model.

Searches for removed vocabulary must distinguish historical prose intentionally quoting the old model from active product documentation. Active documentation must not suggest that plans are supported.

## 14. Implementation sequence

### Phase 1: introduce event-gated commands

- [x] Mark the superseded active project artifacts `draft`; keep this plan as the controlling implementation amendment.
- [x] Add direct `WaitFor` and `Within` execution options.
- [x] Extend staged worker/coordinator commands with exact waits and a wait timeout.
- [x] Extend the staged node builder's `WaitFor`/`Within` to worker/coordinator decision scopes; the builder stays generic while plan reads exist.
- [x] Include waits and timeout in normalized staged identity and command fingerprints.
- [x] Reuse the existing retained-event lookup, wait rows, reverse index, timeout maintenance, and release logic for every command origin.
- [x] Define delay-plus-wait eligibility and retry-budget timing exactly as section 6 specifies.
- [x] Extend `flowtest.StagedCommand` and worker/coordinator unit tests.
- [x] Add integration tests for direct-root, worker-staged, and coordinator-staged gates before removing plans.

### Phase 2: migrate product examples and application tests

- [x] Rewrite monitor as a direct event-gated command.
- [x] Rewrite fan-out as a coordinator-owned dynamic join.
- [x] Remove worker `ResultOf` use from examples and pass explicit data/references.
- [x] Update example tests to assert behavior rather than plan internals.
- [x] Verify every example with its package tests and the repository integration database.

### Phase 3: remove the complete plan vertical slice

- [x] Delete plan definitions, registration, execution, scopes, facts, reads, and plan-only modifiers.
- [x] Delete the plan scheduler, evaluator, reconciliation store, snapshots, verification, wakeups, observations, and faults in the same buildable cut.
- [x] Move the retained generic node builder out of `plan.go`; simplify its type after dependency plumbing is removed in Phase 4.
- [x] Delete plan simulation, plan runtime tests, and plan query-plan/benchmark coverage.
- [x] Remove `PlanRevision` and `StatusSkipped`.
- [x] Update compile-contract tests so removed APIs cannot reappear accidentally.

### Phase 4: remove dependency and failure-scope internals

- [x] Delete dependency APIs, graph resolution, failure scopes, skip propagation, and worker dependency snapshots.
- [x] Stop making `Work` a `ResultSource`; retain typed trace reads.
- [x] Remove generic typing from `Node`.
- [x] Implement the reduced fail-fast settlement rules from section 7.4.
- [x] Reduce runtime lifecycle, registry, wakeups, maintenance loops, and configuration to workers and coordinators.
- [x] Preserve exact wait races, fail-fast behavior, command ceilings, and atomic decision settlement.

### Phase 5: rewrite storage and replay

- [x] Rewrite baseline migrations to the seven-table schema.
- [x] Remove plan execution fields, indexes, constraints, journal kinds, and event classes.
- [x] Remove dependency tables and dependency command columns.
- [x] Simplify command origin and delay storage.
- [x] Remove plan/dependency projections from replay, trace, history, inspection, and observers.
- [x] Update `flowtest/replaytest` and retain live-versus-replay conformance for direct and coordinator executions.
- [x] Recreate development test schemas rather than migrating pre-release plan data.

### Phase 6: synchronize specifications and harden

- [x] Apply section 13 to all active specifications and docs.
- [x] Remove plan query-plan and reconciliation benchmarks.
- [x] Retain schema-index verification plus representative command-claim, exact-wait, deadline, coordinator-scan, inspection, and journal-growth workloads. Avoid brittle planner-choice assertions in the ordinary suite.
- [x] Run formatting, static analysis, package tests, integration tests, race-sensitive tests where supported, and example tests.
- [x] Record final acceptance and benchmark evidence against the reduced architecture.

## 15. Required test matrix

### 15.1 Event gates

- event recorded before staged command creation satisfies immediately;
- event recorded after command creation releases the pending command;
- direct root returns a handle while its command is pending;
- multiple waits use AND semantics;
- an event with a non-matching key or event name does not release the gate;
- identical duplicate waits coalesce;
- distinct waits added to the same staged command within one decision merge as AND gates;
- a complete staged declaration that differs from an already-durable command under the same key conflicts;
- re-executing a direct root with identical gate options rediscovers idempotently;
- re-executing a direct root with different gate configuration conflicts;
- `WithLiveKey` rediscovers a live execution regardless of gate configuration;
- empty key, invalid event, conflicting `Within` values, and `Within` without a wait fail the whole declaration;
- worker/coordinator settlement atomically commits commands, waits, events, result/state, and journal history;
- failed, panicked, cancelled, rolled-back, or fenced decisions expose none of their staged gates;
- an event staged in the same decision satisfies deterministically;
- fully retained waits make the command ready without starting an active wait deadline;
- waiting consumes no worker delivery or lease;
- delay and wait are independent prerequisites;
- retry budget begins only after both prerequisites clear;
- wait expiry during an initial delay terminalizes the command as `expired`;
- an on-deadline event wins and an after-deadline event does not resurrect the command;
- required and optional wait expiry drive the documented outcomes;
- restart, notification loss, and maintenance takeover preserve behavior.

### 15.2 Coordinators and joins

- prepare success creates exactly the discovered fan-out set;
- zero discovered parts schedules the final command immediately;
- duplicate discovered part identities fail the coordinator decision;
- unknown analysis outcome keys fail the coordinator decision;
- duplicate coordinator delivery cannot create conflicting duplicate commands;
- concurrent analysis outcomes serialize into one bounded state progression;
- the final command is staged exactly once when the pending set becomes empty;
- analysis failure follows explicit coordinator policy;
- final success/failure terminalizes the coordinator execution correctly;
- coordinator retry, lease loss, and process takeover preserve inbox and state revisions;
- large fan-out tests enforce command and coordinator-state limits.

### 15.3 Failure reduction

- fail-fast cancels pending, ready, and retry-wait commands after a required failure;
- attempts already running may settle without losing their result or staged events;
- children staged by a surviving attempt are created terminal-cancelled and never enter the worker queue;
- fail-fast-disabled execution accepts descendants staged by successful surviving work;
- coordinator executions remain able to consume terminal outcomes and explicitly complete while `failing`;
- direct executions terminalize after surviving running attempts settle.

### 15.4 Removal contracts

- active Go source exports no `Plan`, `PlanDef`, `DefinePlan`, `Fact`, or `Facts`;
- active Go source contains no plan registration or driver mode;
- `Node` has no `After*`, `Outcome`, or `Children` methods;
- workers cannot read dependency snapshots;
- migrations create exactly seven Flow tables;
- no dependency table, plan execution column, plan journal kind, plan event class, or skipped status remains;
- trace, history, replay, and observers expose no plan fields;
- runtime exposes no plan concurrency or verification option;
- active docs and examples contain no supported plan workflow.

### 15.5 Regression guarantees

- direct and coordinator execution idempotency remains unchanged;
- worker/coordinator decisions remain atomic and fenced;
- retries and persisted jitter remain deterministic across restart;
- command and coordinator lease takeover remains safe;
- cancellation, execution deadlines, fail-fast, and command ceilings remain correct;
- journal positions remain gap-free and commit-ordered per execution;
- application event identity and conflicting-payload rules remain unchanged;
- live inspection agrees with replay for retained fields.

## 16. Performance acceptance

The refactor must demonstrate the expected reduction rather than only deleting names:

- no dirty-plan update is performed on command settlement or application-event ingress;
- no background plan probe, snapshot load, or reconciliation transaction exists;
- command terminality does not scan dependency groups or members;
- exact event emission uses the retained reverse wait index;
- idle runtimes perform no plan polling work;
- direct command and coordinator throughput do not regress outside agreed benchmark noise;
- dynamic fan-out coordinator throughput and coordinator-row contention are measured at representative fan-out sizes.

Any coordinator hot-row limit found by the fan-out benchmark must be documented. It is not a reason to restore plans silently; a future declarative join feature requires a separate proposal with evidence.

## 17. Non-goals

This refactor does not add:

- global event subscriptions;
- wildcard or prefix event matching;
- events that create executions;
- cross-execution joins or event gates;
- arbitrary external command injection into an existing execution;
- a replacement DAG DSL under another name;
- declarative quorum, race, or threshold operators;
- implicit loading of event payloads into workers;
- child executions;
- compatibility aliases or migration of pre-release plan history.

It also does not remove coordinators, staged application events, exact command waits, worker child commands, the ordered journal, PostgreSQL durability, retries, leases, fencing, replay, or inspection.

## 18. Completion criteria

This plan is complete only when:

1. the public model has commands, workers, coordinators, and events but no plans;
2. direct and staged commands can be durably gated on exact execution-scoped events;
3. event gates have retained-fact, timeout-race, delay, retry-budget, and resource semantics covered by integration tests;
4. fan-out uses a coordinator-owned join and monitor uses a direct event-gated command;
5. declarative dependencies and worker dependency reads are gone;
6. runtime plan scheduling and reconciliation are gone;
7. the schema has the seven retained tables and no plan/dependency projection;
8. replay, trace, history, flowtest, observers, faults, benchmarks, and active docs describe only the retained model;
9. all unit, integration, replay-conformance, migration, query-plan, and example tests pass against PostgreSQL;
10. the active project specifications are synchronized and this artifact is marked `complete` with acceptance evidence linked.
