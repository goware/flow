---
status: complete
completed_at: 2026-08-04
---

# Plan: remove coordinators and make commands fully composable

## 1. Purpose

Remove Flow's coordinator and coordination features before the public API and durable formats are frozen. Reduce the product to one execution model:

```text
command -> worker -> result + events + child commands
                       |
                       v
             exact event-gated commands
```

Commands are the only durable units of orchestration. A worker performs one command, may atomically emit immutable application events, and may stage bounded child commands. A command may declare exact application events that must exist before its worker can run. A worker may read only the event payloads declared as gates for its own command.

This supports sequences, fan-out, fan-in, repeated fan-out/join phases, delayed work, external signals, and bounded command loops without a second durable state-machine abstraction.

The target is intentionally minimal:

```text
commands + workers + execution-scoped events
```

There is no coordinator definition, coordinator state row, event-handler registry, coordinator scheduler, serialized coordinator inbox, or coordinator-specific terminal decision. Execution completion is derived from the command tree.

This is a pre-release breaking refactor. Removed names receive no compatibility aliases. Existing pre-release coordinator histories are not migrated. Baseline migrations, replay, inspection, testing helpers, examples, benchmarks, and active documentation are rewritten to the smaller model.

## 2. Relationship to existing project artifacts

This plan follows `plans/2-remove-plan.md`. Where the active specifications or the completed removal plan retain coordinators, coordinator state, coordinator handlers, coordinator scheduling, or coordinator storage, this plan supersedes them.

This explicitly reverses plan 2's controlling decision that coordinators are Flow's mechanism for joins, branching, races, and open-ended event-driven behavior. The new controlling decision is narrower: Flow supports deterministic all-of composition through commands and exact application events. It intentionally gives up in-execution reactions to unsuccessful command outcomes, first-of-N races, quorum/threshold gates, and open-ended handler-driven state machines. The command-only agent rewrite in Phase 1 is the required product proof that this smaller boundary serves the target applications; coordinator deletion must stop if that proof exposes a near-term need for any foreclosed behavior.

The completed command/event work remains foundational:

- direct command execution;
- worker-staged child commands;
- exact `WaitFor` gates;
- `Within` wait budgets;
- initial delays;
- staged and external application events;
- retries, leases, fencing, deadlines, cancellation, fail-fast, history, replay, and trace.

Historical review files under `specs/projects/flow/reviews` remain untouched.

## 3. Product thesis

### 3.1 One unit of composition

A Flow application should not choose between a command and a coordinator as the root of durable work. It always starts a command.

A command can:

1. execute immediately, after a delay, or after exact events exist;
2. perform typed work;
3. return a typed result for durable inspection;
4. emit typed immutable events;
5. stage more commands with explicit arguments and gates.

Those capabilities are sufficient for Flow's target use cases when commands are composable and event payloads declared as inputs are available to their workers.

### 3.2 Events are the only synchronization primitive

Flow does not add a separate command-dependency language. A command waits on exact application events:

```go
flow.Execute(work, "report/join", JoinReport, JoinArgs{Parts: parts}).
    WaitFor(PartAnalyzed, "part/0").
    WaitFor(PartAnalyzed, "part/1")
```

Each predecessor emits its application event as part of successful settlement. The waiting worker reads those declared inputs:

```go
part, err := flow.ReadEvent(work, PartAnalyzed, "part/0")
```

There are no hidden graph edges, dependency groups, forward references, result snapshots from arbitrary commands, or re-evaluation scheduler. The wait rows already required for exact events remain the complete readiness mechanism.

### 3.3 Command trees provide lifecycle and provenance

Every execution begins with one root command. A worker may stage child commands. Those children may stage their own children, producing a durable provenance tree.

Event gates may synchronize siblings or commands across different tree branches. They do not change ownership: each staged command still has one parent command and one stable execution-local key.

An execution remains live while any command is non-terminal. If future external input is required, a worker must stage the gated continuation before its own settlement. An event cannot reopen a terminal execution.

### 3.4 No application callback runs merely because an event exists

An event only releases commands that were already declared with exact matching gates. Events do not create executions, inject arbitrary commands, select handlers, or run application code directly.

This keeps control flow visible in command declarations:

```text
declare continuation -> wait -> exact event exists -> worker runs
```

### 3.5 Coordinator complexity is not retained under another name

This refactor must not replace coordinators with a differently named state-machine object. In particular, it does not add:

- event-handler registrations;
- durable mutable workflow state outside command arguments/application storage;
- an inbox cursor or serialized delivery loop;
- explicit workflow `Succeed`/`Fail` calls;
- command-outcome subscriptions;
- wildcard, OR, threshold, race, or quorum gates;
- an event-driven reconciliation scheduler.

If those features become product requirements later, they require a separate proposal with evidence that command composition cannot serve the use case.

### 3.6 The expressiveness loss is accepted

The smaller model does not react to every possible terminal condition:

- a failed, cancelled, or expired worker emits no application event from that unsuccessful decision;
- a command waiting for that event therefore never becomes ready from the predecessor's failure;
- required predecessor failure is handled only by execution failure/fail-fast;
- optional predecessor failure requires an explicit wait budget or policy outside Flow;
- there is no first event wins, first command finishes, quorum, threshold, or race primitive;
- there is no serialized mutable state reacting to an unknown sequence of facts.

This is a deliberate product boundary, not an accidental omission. Expected business alternatives should be represented by successful typed results/events. Infrastructure failure remains execution failure. Applications needing first-of-N or reactive failure handling are outside this milestone; the implementation must not quietly rebuild coordinator semantics to accommodate them.

## 4. Target public model

### 4.1 Definitions

Retain:

```go
type Command[A, R any]
type Event[T any]

func DefineCommand[A, R any](name string, version int, opts ...CommandOption) Command[A, R]
func DefineEvent[T any](name string) Event[T]
```

Remove:

```go
type Coordinator[S any]
type Coordination[S any]
type Handler[S any]
type Received[T any]
type CoordinatorID string

func DefineCoordinator[S any](...)
func OnStart[S any](...)
func OnEvent[S, T any](...)
func OnOutcome[S, A, R any](...)
```

There is no compatibility alias for any removed symbol.

### 4.2 Starting work

The only execution start is:

```go
handle, err := command.With(client).Execute(ctx, executionKey, args, opts...)
```

Execution options remain:

- `WithExecutionDeadline` / `WithoutExecutionDeadline`;
- `WithMetadata`;
- `WithFailFast`;
- `WithLiveKey`;
- `WithStartDelay`;
- one or more `WaitFor` gates;
- optional `Within` wait budget.

All options now describe one root command execution. Documentation must stop referring to modes or shared execution modes.

### 4.3 Worker registration

Retain:

```go
func Handle[A, R any](
    command Command[A, R],
    worker func(context.Context, *Work[A]) (R, error),
    options ...WorkerOption[A, R],
) Registration
```

`Runtime.Register` accepts worker registrations only. The registry contains one exact command name/version map. Registration kind tags and coordinator erasure are removed.

### 4.4 Worker decisions without `Scope`

Once `Coordination` is gone, `Scope` no longer abstracts multiple decision owners. Remove the public sealed `Scope` interface and make worker-only signatures explicit:

```go
func Execute[W, A, R any](
    work *Work[W],
    key string,
    command Command[A, R],
    args A,
) *Node

func Emit[W, T any](
    work *Work[W],
    event Event[T],
    key string,
    payload T,
) error
```

Type inference keeps ordinary calls unchanged:

```go
flow.Execute(work, "notify", Notify, args)
flow.Emit(work, NotificationRequested, "notify", payload)
```

`Work[A]` retains typed `Args` and `Info()`. Its private decision recorder owns staged events, staged children, declared event inputs, and the first defect.

### 4.5 Staged command builder

Retain the non-generic ephemeral `Node`:

```go
type Node

func (*Node) Key() string
func (*Node) Optional() *Node
func (*Node) Delay(time.Duration) *Node
func (*Node) WaitFor(EventRef, string) *Node
func (*Node) Within(time.Duration) *Node
```

`Node` remains valid only during its worker decision. Repeated equivalent declarations coalesce. Repeated declarations of the same command key within that one worker decision merge distinct exact waits additively; singleton disagreement poisons the complete decision.

Once a command declaration is durable, its fingerprint is immutable. A later worker decision that redeclares the key with any added, removed, or changed wait conflicts exactly as it does today. Flow does not support durable declaration amendment, and there is no race in which waits can be added after a command becomes ready, claimed, or terminal.

### 4.6 Read declared event inputs

Add one narrow typed read:

```go
func ReadEvent[W, T any](
    work *Work[W],
    event Event[T],
    key string,
) (T, error)
```

`ReadEvent` is not an execution-wide event query. It may read only an exact `(event name,event key)` gate declared on the currently executing command.

Rules:

1. Every declared wait is satisfied before worker invocation.
2. The runtime loads the canonical event payload at the recorded satisfying journal position before releasing its database connection.
3. `ReadEvent` performs an in-memory typed lookup and no SQL.
4. Calling it for an undeclared selector, invalid event, or missing snapshot returns `ErrInvalid`/`ErrInvalidState` and poisons the worker decision.
5. Repeated reads return the same immutable value.
6. Retries and lease takeover receive the same event identities and canonical payloads.
7. Event arrival order does not affect the normalized input snapshot.
8. The event may have been recorded before command declaration, after declaration, or in the same accepted parent decision that created the command.

This is the only durable cross-command input read. There is no command result lookup inside a worker.

### 4.7 Results and terminal inspection

Remove the coordinator-oriented terminal wrapper and generic source abstraction:

```go
type Outcome[R any]
type ResultSource

func OutcomeOf(...)
```

Retain typed successful-result inspection, but bind it directly to a trace:

```go
func ResultOf[A, R any](
    trace ExecutionTrace,
    key string,
    command Command[A, R],
) (R, error)
```

Unsuccessful status and structured failure remain available on `TraceCommand`. `CommandStatus` and `CommandFailure` remain public inspection types.

Command results are not automatic inputs to other commands. A worker passes its own computed values into children it stages. Sibling or external data moves through declared event payloads, stable references, or application storage.

## 5. Command composition semantics

### 5.1 Sequence

A successful worker stages its continuation with explicit arguments:

```go
func reserve(_ context.Context, work *flow.Work[ReserveArgs]) (Reservation, error) {
    reservation := performReservation(work.Args)
    flow.Execute(work, "capture", Capture, CaptureArgs{
        ReservationID: reservation.ID,
    })
    return reservation, nil
}
```

The child does not exist unless the parent decision settles successfully. Parent result, staged events, staged children, application commit, journal entries, and readiness changes remain atomic.

### 5.2 Fan-out

A worker stages a bounded set of children with stable keys:

```go
for _, part := range parts {
    flow.Execute(work, "analysis/"+part.ID, AnalyzePart, part)
}
```

The execution command ceiling remains the hard durable bound. Conflicting duplicate keys fail the complete parent decision.

### 5.3 Fan-in

The same parent stages the fan-out and one join command. The join waits on one exact event from each child:

```go
join := flow.Execute(work, "analysis/join", JoinAnalysis, JoinArgs{
    PartKeys: partKeys,
})

for _, part := range parts {
    key := "analysis/" + part.ID
    flow.Execute(work, key, AnalyzePart, part)
    join.WaitFor(PartAnalyzed, key)
}
```

Each analysis worker emits `PartAnalyzed` with the same stable key. The join worker uses `ReadEvent` for the keys listed in its arguments and performs the aggregation.

The command tree records provenance; exact waits provide the cross-sibling synchronization. No command-dependency rows are introduced.

### 5.4 Repeated fan-out and join

A join is an ordinary command. Its worker may stage another fan-out plus another gated join:

```text
prepare
  |-- analyze/0 --emit--> analyzed/0 --|
  |-- analyze/1 --emit--> analyzed/1 --|--> join-analysis
                                              |-- enrich/0 --emit--> enriched/0 --|
                                              |-- enrich/1 --emit--> enriched/1 --|--> join-enrichment
                                                                                         |--> generate
```

There is no limit on the number of phases other than the execution deadline, command ceiling, exact-wait bounds, and application-controlled command arguments.

### 5.5 Bounded loops

A worker may stage another version of the same command definition under a new stable key:

```go
flow.Execute(work, fmt.Sprintf("turn/%d", nextTurn), Think, nextArgs)
```

This creates a bounded durable command chain, not recursive inline execution. Execution deadlines and the command ceiling protect against accidental unbounded loops.

An open-ended workflow engine is not a Flow goal. Long-lived interactions must always keep an explicit gated command open and remain within configured bounds.

### 5.6 External signals

A root or child command can wait for an event published by another process:

```go
handle, err := Confirm.With(runtime).Execute(ctx, "bridge/42", args,
    flow.WaitFor(BridgeDelivered, "delivery/42"),
    flow.Within(30*time.Minute),
)
```

The worker may call `ReadEvent` to decode `BridgeDelivered`. External publishers need only an event definition and a client; they register no handlers and do not call `Run`.

### 5.7 Event identity and AND gates

Application-event identity remains:

```text
(execution ID, event name, event key)
```

Matching is exact. Multiple gates are AND conditions. Identical gates coalesce. Events are immutable; equivalent repeated emission is idempotent and different canonical content conflicts.

`Within` begins at command creation and is independent of initial delay. A command is claimable only when every exact gate is satisfied and its delay has elapsed. Expiry is terminal and late events cannot resurrect it.

### 5.8 Failure behavior

Worker errors do not commit staged application events or child commands. Retry classification runs as today.

Required terminal failure enters reduced fail-fast, cancels work without active attempts, preserves valid running settlements, and ultimately fails the execution. A gated required continuation is cancelled rather than waiting forever after required predecessor failure.

Optional commands remain suitable for non-critical side work. If an optional command is expected to feed a required join, the application must define an explicit policy, such as:

- represent expected domain failure in a successfully emitted typed event;
- give the join a `Within` budget and handle expiry at execution policy level;
- avoid marking a required data producer optional.

Flow does not provide failure-branch subscriptions. Infrastructure failure is a failed command; expected business alternatives should be typed command results/events rather than worker errors.

### 5.9 Completion

There is no explicit workflow `Succeed` or `Fail` operation.

An execution:

- succeeds when every command is terminal and no required command failed/expired;
- fails when required failure semantics finish draining valid running attempts;
- becomes cancelled or expired through the existing execution operations;
- remains running while any pending, ready, retrying, or running command exists.

Application events alone do not keep an execution open. A declared gated command does.

Optionality affects failure contribution, not liveness. Execution success still requires optional commands to become terminal. An optional gated command without `Within` can therefore hold the execution open until its event arrives or the execution deadline expires. Flow has no "finish now and abandon optional stragglers" transition after coordinator terminal methods are removed. Applications should give every optional externally gated command a deliberate `Within` budget or avoid staging it when the execution should not wait.

## 6. Event input bounds

Command delivery must remain bounded and must not introduce N+1 reads during worker code.

Add these limits:

- at most 256 exact event waits per command;
- existing 64 KiB canonical payload maximum per application event;
- therefore at most 16 MiB of encoded event input payload per command before decoding overhead;
- one bounded batched store query loads all satisfied wait positions and bodies during claim materialization.

The wait-count limit is validated when a direct root or staged command is declared. Applications with larger joins build a tree of join commands or place aggregate data behind stable references.

The runtime must not hold a PostgreSQL connection while the handler processes the snapshot. `ReadEvent` is O(1) over a normalized in-memory map.

## 7. Remove the coordinator public surface

Delete coordinator definitions and handlers from:

- `definitions.go`;
- `coordinator.go`;
- registration types and validation;
- command/event/coordinator examples;
- package documentation and public API compile fixtures.

Remove all public coordinator names, including:

- `Coordinator`;
- `Coordination`;
- `Handler`;
- `DefineCoordinator`;
- `OnStart`;
- `OnEvent`;
- `OnOutcome`;
- `Received`;
- `CoordinatorID`;
- `WithCoordinatorConcurrency`.

Remove `Outcome`, `OutcomeOf`, and `ResultSource` as described in section 4.7. Do not rename coordinator concepts or preserve them in `flowtest`.

## 8. Runtime simplification

Delete the coordinator runtime as a subsystem:

- `coordinator_runtime.go`;
- coordinator registry maps and erasure;
- coordinator concurrency semaphore;
- coordinator probe, scan, claim, lease, renewal, retry, settlement, and recovery loops;
- coordinator wake routing;
- active coordinator invocation tracking;
- coordinator observer operations;
- coordinator fault points;
- coordinator shutdown handling;
- coordinator-specific retry state.

`Runtime.Run` owns only:

- the command scheduler;
- command lease renewal;
- exact-wait, execution-deadline, and expired-command recovery maintenance;
- optional notification listening;
- observer delivery and graceful shutdown.

Runtime connection/capacity documentation must be recalculated. One running runtime has worker capacity plus the optional notification session; there is no coordinator pool.

Event ingress performs exact-wait resolution and command wakeup only. It does not scan or wake an event-handler inbox.

## 9. Six-table PostgreSQL schema

Because Flow is pre-release, rewrite the baseline migrations and recreate development/test Flow schemas. Do not add compatibility columns or migrate coordinator histories.

The target inventory is exactly six tables:

1. `flow_executions`;
2. `flow_commands`;
3. `flow_command_queue`;
4. `flow_command_event_waits`;
5. `flow_journal`;
6. `flow_schema_migrations`.

### 9.1 Delete coordinator storage

Delete:

- `flow_coordinators`;
- coordinator claim, idle-scan, and lease indexes;
- coordinator store row/request/result types;
- coordinator selectors, inbox snapshots, delivery attempts, and retry SQL;
- all coordinator migration constraints and validation tests.

### 9.2 Remove execution modes

There is only one kind of execution. Remove:

- `flow_executions.driver_mode`;
- the driver-mode check constraint;
- `DriverMode`, `DriverDirect`, and `DriverCoordinator` internal values;
- public `Execution.Mode`;
- driver mode from start requests, start fingerprints, journal bodies, replay, trace, list/lookup queries, and tests.

Permanent execution-key uniqueness becomes:

```text
(definition_name, execution_key)
```

for non-empty permanent keys. Live-key uniqueness uses the same identity restricted to non-terminal rows. `definition_name` and `definition_version` describe the root command and remain useful indexed inspection fields.

### 9.3 Remove command origin

With a single root model, command origin is derivable:

- `parent_command_id IS NULL` identifies the root;
- non-null `parent_command_id` identifies a worker-staged child.

Remove:

- `flow_commands.origin`;
- origin constraints and index includes;
- origin from command declaration fingerprints and `CommandCreated` bodies;
- public `TraceCommand.Origin`;
- internal origin constants and branches.

Keep `flow_executions.root_command_id` and `flow_commands.parent_command_id` for root identity and provenance. Keep the parent index for trace and cancellation inspection.

### 9.4 Simplify journal schema

Remove:

- `flow_journal.coordinator_id`;
- `coordinator_transition` entry kind;
- `coordinator_terminal` event class;
- coordinator terminal unique index;
- coordinator subject-shape branches;
- coordinator/delivery fields from `ExecutionStarted`, `AttemptStarted`, and `AttemptConcluded` bodies;
- `CoordinatorTransitionBody`.

Retain journal kinds:

- `execution_started`;
- `execution_failing`;
- `command_created`;
- `attempt_started`;
- `attempt_concluded`;
- `event_recorded`.

Retain event classes:

- `application`;
- `command_terminal`;
- `execution_terminal`.

### 9.5 Event-input snapshot query

`flow_command_event_waits.satisfied_position` remains the authoritative link from a declared gate to its immutable journal event.

When materializing a claimed command, load all wait rows and join their satisfying `(execution_id,position)` journal entries in one bounded query. Validate:

- every wait is satisfied;
- every referenced row is an application event;
- event name/key match the wait row;
- the journal body/hash is valid;
- total rows do not exceed 256;
- payload codecs match only when application code calls `ReadEvent`.

No new event-input or event-payload table is added.

## 10. Replay, history, trace, and observers

### 10.1 Replay

Remove coordinator semantic state, transitions, attempts, terminality, and conformance overlays from `internal/replay`.

Replay continues to fold:

- one execution start;
- command creation/provenance;
- command attempt start/conclusion;
- application events;
- command terminal events;
- execution failing/terminal events.

The reducer rejects any removed journal kind or field shape in the clean pre-release schema.

### 10.2 History

Remove `CoordinatorID` from `HistoryEntry`. History kinds no longer include `HistoryCoordinatorTransition`.

### 10.3 Trace

Remove:

- `ExecutionTrace.Coordinator`;
- `TraceCoordinator`;
- `TraceEvent.CoordinatorID`;
- `TraceCommand.Origin`;
- coordinator operational overlay queries;
- coordinator result-source population.

Add declared event-input visibility to each command trace. `TraceEventWait` should include the satisfying journal position when available so operators can connect a worker input to the exact event row.

Keep typed successful `ResultOf(trace, key, command)` as a direct trace helper.

### 10.4 Observers and faults

Delete coordinator observation operations and fault hooks. Keep command, execution, event ingress, maintenance, notification, and shutdown observations/fault boundaries.

## 11. Testing helpers

Remove coordinator testing from:

- `flowtest`;
- `flowtest/replaytest`;
- the private testing bridge;
- `internal/testengine`.

Retain worker decision tests and add declared event snapshots:

```go
flowtest.RunWorker(...,
    flowtest.WithEvent(PartAnalyzed, "part/0", payload),
)
```

The helper must use production canonical codecs and `ReadEvent` behavior. It validates exact selector identity, duplicate/conflicting fixture values, wait bounds, typed decode errors, scope poisoning, and deterministic repeated reads.

`flowtest.StagedCommand` continues to expose key, arguments, optionality, delay, waits, and `Within` so multi-phase command composition can be asserted without PostgreSQL.

## 12. Example migrations

Every example retains its complete application logic inside its own `examples/*` package and follows the established `newFlowRuntime`, `runFlowRuntime`, and `runExampleCommand` conventions.

### 12.1 `examples/direct`

No behavioral change. It remains the smallest command/worker example.

### 12.2 `examples/monitor`

Keep the direct root gated on an externally emitted exact event. Update its worker to call `ReadEvent` and demonstrate that a gated command can consume the typed payload, not only wake on it.

### 12.3 `examples/fanout`

Remove `reportCoordinator` and all handler/state types.

Rewrite it as a command-only two-stage join:

```text
prepare
  -> analyze fan-out
  -> join-analysis waits for PartAnalyzed events
       -> enrichment fan-out
       -> join-enrichment waits for PartEnriched events
            -> generate report
```

Requirements:

- `prepare` discovers a dynamic stable part list;
- it stages every analysis command and the first join atomically;
- analysis workers emit typed scores/references;
- the first join reads all declared analysis events and stages the second fan-out/join;
- enrichment workers emit typed results;
- the second join reads those events, computes final command arguments, and stages generation;
- zero-part fan-outs continue immediately with a join that has no waits or directly stage the next phase;
- duplicate part identities poison the declaring worker decision;
- required child failure fails the execution and cancels gated continuations;
- stable keys make retry/redelivery declarations idempotent.

This example is the primary proof that repeated fan-out/join does not require coordinator state.

### 12.4 `examples/agent`

Remove `researchAgent`, `agentState`, and coordinator handlers.

Use self-composing commands:

1. a root `think` command receives turn state in its arguments;
2. if tools are required, it stages tool commands and the next `think` command;
3. each tool emits a typed `ToolCompleted` event with a stable output reference;
4. the next `think` command waits for those exact tool events and reads them with `ReadEvent`;
5. an external user message is another exact gated event when required;
6. a final turn emits `AgentCompleted` and stages no continuation.

The example must remain bounded by execution deadline and command ceiling. Tool output bodies should use stable references where realistic rather than placing large model content in Flow events.

## 13. Documentation synchronization

At implementation start, mark affected active artifacts `draft`. After code and acceptance evidence agree, mark them `complete` again.

Update:

- `README.md` to present command -> worker -> events/children as the complete model;
- `project_overview.md` to remove the two-mode distinction;
- `functional_spec.md` to define composable commands and declared event inputs;
- `architecture.md` to remove coordinator scheduling/state/delivery;
- `components/engine.md` to describe worker-only decisions and `ReadEvent`;
- `components/runtime.md` to describe one command scheduler;
- `components/schema.md` to describe six tables;
- `implementation_plan.md`, phase plans, acceptance evidence, and benchmark evidence;
- package documentation, exported comments, examples, and test documentation.

Historical review files remain source material. Historical completed plans may retain old terminology only when clearly marked superseded. Active documentation must not suggest that coordinators are supported.

## 14. Implementation sequence

### Phase 1: prove event payload inputs and command-only examples

- [ ] Add bounded declared event snapshots to command claims and worker scopes.
- [ ] Add `ReadEvent` with exact declared-gate enforcement and deterministic retry behavior.
- [ ] Add wait-count validation and trace satisfaction positions.
- [ ] Extend `flowtest` worker fixtures for declared event inputs.
- [ ] Rewrite fan-out as two command-owned fan-out/join phases.
- [ ] Rewrite agent as a bounded self-composing command loop.
- [ ] Update monitor to consume its exact event payload.
- [ ] Run database-free and PostgreSQL example tests before deleting coordinators.
- [ ] Confirm the rewritten agent and target applications require no failure subscription, first-of-N, quorum, race, or open-ended handler state; stop before Phase 2 if they do.

### Phase 2: remove the complete coordinator public/runtime vertical

- [ ] Delete coordinator definitions, handlers, state, outcomes, received envelopes, and IDs.
- [ ] Remove `Scope` and make worker-only `Execute`/`Emit` signatures explicit.
- [ ] Bind `ResultOf` directly to `ExecutionTrace`; remove `Outcome`, `OutcomeOf`, and `ResultSource`.
- [ ] Delete coordinator registration, scheduler, scan, claim, lease, retry, settlement, recovery, wake, observation, and fault code.
- [ ] Delete coordinator runtime/unit/integration tests and benchmarks.
- [ ] Reduce runtime lifecycle and capacity to command work plus maintenance.

### Phase 3: rewrite storage and replay

- [ ] Rewrite baseline migrations to six tables.
- [ ] Delete coordinator storage/indexes and journal representation.
- [ ] Remove execution driver mode and command origin columns/types.
- [ ] Simplify key uniqueness, start fingerprints, journal bodies, inspection queries, and constraints.
- [ ] Remove coordinator replay/history/trace/observer/test-helper projections.
- [ ] Retain live-versus-replay conformance for direct command executions.
- [ ] Recreate development/test Flow schemas rather than migrating coordinator data.

### Phase 4: synchronize and harden

- [ ] Apply section 13 to all active documentation.
- [ ] Replace coordinator sparse-scan benchmarks with event-gated fan-in workloads.
- [ ] Run formatting, static analysis, package tests, PostgreSQL tests, race tests, examples, and migration checks.
- [ ] Run public API and removed-symbol scans.
- [ ] Record six-table acceptance and retained benchmark evidence.
- [ ] Mark this plan and synchronized active artifacts `complete`.

## 15. Required test matrix

### 15.1 Declared event inputs

- event-before-command and command-before-event produce the same decoded input;
- a parent-staged event satisfies and supplies a child gate in the same decision;
- multiple waits use AND semantics and normalize independently of declaration order;
- repeated declarations of one command key within a single decision merge distinct waits as AND gates;
- repeated declarations within one decision reject different arguments, delay, optionality, or `Within` values;
- a later decision cannot add, remove, or change waits on an already-durable command key;
- an attempted durable redeclaration conflicts whether the original command is pending, ready, running, or terminal;
- `ReadEvent` returns the correct typed payload for each exact name/key;
- repeated reads return identical values;
- undeclared selector reads poison the worker decision;
- wrong event definition/type fails deterministically;
- invalid or empty gate identities fail atomically, while identical duplicate waits coalesce;
- retries and lease takeover receive identical satisfying positions/payloads;
- failed, panicked, cancelled, rolled-back, or fenced decisions expose no staged events or children;
- maximum 256 waits is accepted and 257 is rejected atomically;
- command claim materialization uses bounded queries and releases the database connection before handler invocation;
- wait expiry and late-event races remain correct;
- delay and wait remain independent prerequisites.

### 15.2 Composition

- sequential child staging passes parent-computed arguments;
- dynamic fan-out creates exactly the discovered stable keys;
- zero fan-out continues without a permanent wait;
- fan-in runs only after every exact success event;
- fan-in reads all typed event payloads and computes deterministic output;
- a command in one provenance branch can emit an event that releases a command in a different branch;
- two fan-out/join phases complete without coordinator state;
- duplicate part identities fail the declaring decision;
- required child failure cancels the gated join and fails execution;
- worker retry does not duplicate events, children, or joins;
- bounded self-staging loops respect command ceilings and deadlines;
- a predeclared externally gated continuation keeps execution live;
- an optional gated command without `Within` prevents success until it receives its event or the execution deadline terminalizes it;
- an optional gated command with `Within` reaches terminal expiry and allows quiescent completion according to optional-failure policy;
- an event cannot reopen a terminal execution.

### 15.3 Removal contracts

- active Go source exports no `Coordinator`, `Coordination`, `Handler`, `DefineCoordinator`, `OnStart`, `OnEvent`, `OnOutcome`, `Received`, `CoordinatorID`, `Outcome`, `OutcomeOf`, or `ResultSource`;
- `Scope` no longer exists and worker decision functions accept `*Work[...]`;
- runtime exposes no coordinator concurrency option;
- registry accepts worker registrations only;
- runtime starts no coordinator goroutine, probe, scan, renewer, or maintenance path;
- migrations create exactly six Flow tables;
- `flow_coordinators`, `driver_mode`, command `origin`, `coordinator_id`, coordinator journal kinds/classes, and their indexes are absent;
- replay, history, trace, observers, faults, flowtest, and replaytest expose no coordinator fields;
- active documentation and examples present no coordinator workflow.

### 15.4 Regression guarantees

- start idempotency and live-key behavior remain correct without driver mode;
- worker decisions remain atomic and fenced;
- reduced fail-fast and surviving running settlements remain correct;
- retries, persisted jitter, attempt timeouts, and lease takeover remain deterministic;
- cancellation, deadlines, wait expiry, command ceilings, and shutdown remain correct;
- journal positions remain gap-free and commit-ordered;
- staged/external event identity and conflicting-payload behavior remain unchanged;
- history and live trace agree with replay;
- caller-owned transactions preserve execution-first lock ordering;
- notifications remain hints and polling alone progresses all work;
- all four examples pass against real PostgreSQL.

## 16. Performance acceptance

The refactor must demonstrate structural simplification:

- idle runtimes perform no coordinator probe or journal inbox scan;
- there is no coordinator hot row serializing wide fan-out outcomes;
- event ingress performs only idempotent append, exact reverse-wait lookup, readiness update, and wake hint;
- event-gated join delivery loads declared inputs in bounded queries without N+1 handler reads;
- one runtime requires no coordinator semaphore, active-invocation map, lease renewal batch, or dedicated store scans;
- the migrated schema contains one fewer table and removes all coordinator indexes/columns;
- command ingress, claim, settlement, and exact-wait performance do not regress outside agreed benchmark noise;
- event-gated joins are measured at 1, 10, 100, and 256 inputs;
- claim materialization at 256 maximum-size 64 KiB payloads is measured under representative worker concurrency, including peak heap and allocation volume;
- repeated fan-out/join is measured without a single serialized workflow-state row.

Remove `BenchmarkCoordinatorSparseOutcomeScan10K`. Add retained benchmarks for event snapshot materialization, join readiness, and `ReadEvent` lookup.

## 17. Non-goals

This refactor does not add:

- a renamed coordinator or workflow state machine;
- declarative command dependencies or a DAG DSL;
- arbitrary command-result reads inside workers;
- event handlers that execute without a declared command;
- events that create or reopen executions;
- wildcard, prefix, OR, quorum, race, or threshold event gates;
- cross-execution gates or joins;
- implicit unbounded event collections;
- arbitrary external command injection;
- child executions;
- exactly-once external side effects;
- compatibility aliases or migration of pre-release coordinator history.

It also does not remove typed commands/results, worker child commands, exact event gates, typed application events, external event publication, retries, delays, timeouts, leases, fencing, caller transactions, the ordered journal, replay, inspection, fail-fast, or command ceilings.

## 18. Completion criteria

This plan is complete only when:

1. the complete public model is commands, workers, and execution-scoped events;
2. every execution begins with one root command and completes from command terminality;
3. workers can stage child commands/events and read only their declared exact event inputs;
4. sequences, fan-out, fan-in, repeated fan-out/join, bounded loops, and external signals are demonstrated without coordinator state;
5. coordinator APIs, runtime scheduling, storage, journal formats, replay, inspection, tests, and documentation are gone;
6. `Outcome`, `OutcomeOf`, `ResultSource`, and worker/coordinator `Scope` abstractions are gone;
7. the schema contains exactly the six retained tables and no execution mode or command origin column;
8. event input loading is bounded, deterministic across retry/takeover, and performs no handler-time SQL;
9. database-free, PostgreSQL, race, migration, replay-conformance, example, and static checks pass;
10. active specifications and acceptance/benchmark evidence describe only the minimal command -> worker -> event model;
11. this artifact is reviewed, approved, and marked `complete`.
