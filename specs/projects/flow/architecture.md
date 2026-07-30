---
status: complete
---

# Architecture: flow

## 1. Architectural objective

Flow presents a small typed Go API over a PostgreSQL-backed durable execution engine. The library keeps the public grammar small while retaining separate internal mechanisms for direct execution, pure plan reconciliation, worker child settlement, coordinator decisions, exact event waits, and distributed claiming.

`1-smaller.md` is the controlling amendment for this revision. The functional specification defines behavior; this document defines the implementation that satisfies it.

## 2. Package layout

```text
flow/
├── definitions.go          typed definitions, binding, command options
├── execute.go              root Execute, Event.Emit, cancellation
├── worker.go               Work, staged Emit, Commit, ResultOf/OutcomeOf
├── plan.go                 Plan, Node[R], Fact/Facts, reconciliation values
├── coordinator.go          handlers, Coordination, On/OnOutcome
├── runtime*.go             schedulers, invocation, leases, shutdown
├── inspection*.go          Get, List, Trace, History, AwaitExecution
├── flowtest/               deterministic decision and simulation harness
├── internal/definition/    erased typed descriptors and codecs
├── internal/retry/         canonical immutable retry data and decisions
├── internal/store/         SQL operations and semantic transactions
├── internal/replay/        journal reducer and conformance
├── internal/fault/         named internal fault hooks
├── internal/testpg/        real PostgreSQL test support
└── migrations/             embedded flow_-prefixed schema
```

Public files organize by developer concept. Internal packages may retain more types than the public API when those types enforce lock order, indexing, fencing, or deterministic replay.

## 3. Core data flow

```text
root Execute / external Event.Emit
          │
          ▼
ordered semantic PostgreSQL transaction
          │
          ├─ append journal entries
          ├─ update projections and graph rows
          ├─ materialize queue eligibility
          └─ emit NOTIFY hint
                       │
                       ▼
bounded runtime scheduler ── SKIP LOCKED claim
                       │
                       ▼
worker outside transaction/connection
        │ stages events and children in memory
                       │
                       ▼
fenced semantic settlement transaction
```

Plans and coordinators use their own capacity-bounded schedulers but share the ordered semantic transaction executor and journal builder.

## 4. Public definitions and erased descriptors

`Command[A,R]`, `Event[T]`, `PlanDef[A]`, and `Coordinator[S]` store erased internal definitions with canonical codecs. Generic values never cross the storage boundary without their definition codec.

Commands, plans, and coordinators retain positive versions. Events store only a name, namespace `application`, and payload codec. Internally generated terminal events use runtime journal kinds rather than derived public event descriptors.

`EventRef` is a sealed non-generic interface implemented only by `Event[T]`. It exists solely because Go methods cannot introduce a new type parameter for `Node.WaitFor`.

Bindings copy a definition and attach a `Client`; no wrapper types are introduced.

## 5. One `Execute` verb, separate internal paths

Root methods perform immediate PostgreSQL ingress and return an `ExecutionHandle`:

- `Command.Execute` creates direct execution plus root command;
- `PlanDef.Execute` creates a dirty plan execution;
- `Coordinator.Execute` creates coordinator state and initial activation.

Package `flow.Execute(scope,key,cmd,args)` performs no SQL. It type-switches over sealed library-owned scopes:

- `*Plan`: records a typed plan declaration and returns `Node[R]` bound to that declaration;
- `*Work[A]`: stages a worker child in the current decision buffer;
- `*Coordination[S]`: stages a coordinator command in its decision buffer.

The internal records remain distinct because their acceptance rules differ. The public verb is unified because every path means asynchronous durable command execution.

`Node[R]` holds an ephemeral owner, key, definition, and either a plan declaration or staged command reference. `Optional` and `Delay` mutate both supported record forms. Plan-only methods validate owner kind; invalid use stores the first scope defect. Settlement/reconciliation rejects a poisoned scope even when the node is ignored.

## 6. Canonical values and declaration identity

All durable typed inputs use canonical JSON and SHA-256 fingerprints. Equality compares canonical bytes and all semantic metadata, never Go pointers or interface identity.

Command creation identity includes:

- execution-local key;
- definition name/version;
- canonical arguments;
- origin and worker parent where applicable;
- required/optional classification;
- normalized dependency groups and exact event waits;
- accepted absolute initial schedule;
- accepted retry, attempt-timeout, and queue settings.

Equivalent repetition coalesces. Disagreement returns/records a conflict appropriate to the scope. Definition defaults are creation-time values: a later redeploy does not reinterpret an existing command.

## 7. Retry policy

The public `RetryPolicy` is immutable data, not an application callback. Its canonical representation contains optional attempt and elapsed bounds, ordered backoff values, and the effective fixed 20% jitter.

`WithRetry` is the only command retry option. `RetryFor` and `Attempts` create policies; both share immutable builder methods. The store persists canonical policy and hash in each command. The retry engine receives persisted database times and classification, chooses once, and persists the absolute next attempt.

Coordinator creation similarly persists the runtime's accepted canonical default retry policy and hash. It remains internal configuration in M1 but cannot be recomputed from a later replica's default.

## 8. Worker decision engine

`Work[A]` contains decoded immutable arguments, command metadata, preloaded dependency outcomes, and a private decision buffer. It has no client, transaction, or external event ingress capability.

The retained worker outputs are only:

- typed result/error from the handler;
- application events staged by `flow.Emit`;
- child commands staged by `flow.Execute`;
- required/optional and delay modifiers;
- result/outcome reads of explicit dependencies;
- the optional statically registered commit function.

The staged event map is keyed by `(event name,key)` and stores canonical bounded payloads. Stable ordering sorts by name and key. Identical content coalesces; conflicting content records the first decision defect. Plans reject this capability because they are pure.

On successful handler return, the runtime canonicalizes `R` and settles staged events, children, the result/terminal event, and any commit-function write under the command fence. A registered commit function receives durable `Args`, `Result`, and `CommandInfo` plus a narrow transaction handle after Flow state locks and writes. Every output remains invisible until the shared transaction commits.

## 9. Plans and typed nodes

### 9.1 Snapshot

A plan snapshot contains execution arguments, existing commands keyed by command key, terminal results/failures, authoritative closed child membership, and retained application events keyed by `(name,key)`. Event payload sets are loaded lazily by definition.

There is no `flow_plan_reads` table. Read tracking is an in-memory evaluation/test diagnostic. Dirty reconciliation does not need a durable subscription set because facts/outcomes are immutable and re-readable.

### 9.2 Evaluation

The plan function receives only `*Plan` plus decoded start arguments. `flow.Execute` declares commands. `Fact`, `Facts`, `Node.Outcome`, and `Node.Children` consult the snapshot and record availability.

At evaluation end the engine validates:

- no scope defect, panic, or conflicting duplicate declaration;
- keys and payload limits;
- every dependency key exists durably or in the same evaluation;
- graph is acyclic;
- only `all_succeeded`, `all_settled`, and `all_failed` dependency groups exist;
- `Within` accompanies a wait;
- exact event keys are non-empty;
- plan ownership does not conflict with existing worker/coordinator commands;
- command ceiling is not crossed.

Plan declarations are monotonic. Existing keys must remain equivalent; newly accepted keys append `CommandCreated`. `PlanReconciled` records revision, snapshot boundary, quiescence, and new declaration keys/fingerprints without repeating command payloads.

### 9.3 Dirty scheduler

Plan-visible transitions set `plan_dirty=true` while holding the execution row. A scheduler probes dirty rows with `SKIP LOCKED`, claims one plan lease, loads a snapshot, invokes the exact plan version, and commits reconciliation.

The reconciler clears dirty only while still holding the execution lock. Therefore all earlier committed triggers are in its snapshot, and a later trigger re-dirties. Initial evaluation is deferred through the same path to keep caller transactions free of plan CPU and defects.

## 10. Graph and wait engine

Dependency groups are normalized into group and member tables. The engine maintains reverse lookup from predecessor to dependents and derives readiness or permanent unsatisfiability when a predecessor terminalizes.

Supported groups are:

| Kind | Satisfied | Permanently unsatisfied |
|---|---|---|
| all_succeeded | every member succeeded | any member ends unsuccessfully |
| all_settled | every member terminal | never before all terminal |
| all_failed | every member unsuccessful | any member succeeds |

Exact event waits store `(execution_id, event_name, event_key, command_id)`. Event acceptance resolves matching rows. `Within` begins once all command dependencies satisfy; retained matching facts resolve before a deadline is created. Expiry uses the persisted deadline so sweep timing cannot change a fact/deadline race.

Worker child membership closes in the same settlement that creates all children and terminalizes the parent successfully. `Node.Children` reads that authoritative set sorted by key.

## 11. Coordinator engine

Coordinator definitions erase typed handlers into selectors and codecs:

- `OnStart` selects the synthetic initial activation;
- `On(Event[T])` selects exact application event name;
- `OnOutcome(Command[A,R])` selects terminal command rows by command name/version and decodes `Outcome[R]`.

There is no command-success-only event subscription and no overlap rule.

`Coordination[S]` owns decoded state and a private staged decision. `flow.Execute` stages commands and `flow.Emit` stages application events. `Succeed` or `Fail` stage terminality without returning an error. Invalid repetition, mixed terminal decisions, or mutation after terminality stores a deterministic scope defect.

One fenced coordinator lease serializes a delivery. Selection finds the earliest matching position above the durable inbox. Handler error discards the decision and retries the same stable delivery with persisted coordinator policy. Nil return atomically writes state, events, commands, transition history, inbox advance, and terminal decision.

## 12. Journal and replay

The execution row owns `next_journal_position`. Every semantic transaction locks the execution first, builds a deterministic ordered batch, reserves contiguous positions, appends rows, and updates projections before commit.

The required journal kinds cover execution start, command creation, attempt lifecycle, application facts from staged decisions or external ingress, terminal outcome, plan reconciliation/defect, coordinator transition/failure, and execution outcome.

`CommandCreated` plus exactly one terminal event per command reconstructs graph existence and settled command state. Attempt rows reconstruct operational invocation history. Current projections remain the efficient query/claim source; the reducer checks that retained history produces equivalent settled control state.

Running/started is projection plus attempt history, never a permanent application event.

## 13. PostgreSQL schema

The nine prefixed tables are:

1. `flow_executions`
2. `flow_commands`
3. `flow_command_queue`
4. `flow_command_dependency_groups`
5. `flow_command_dependency_members`
6. `flow_command_event_waits`
7. `flow_coordinators`
8. `flow_journal`
9. `flow_schema_migrations`

The queue is an operational projection separated from immutable command identity and results. Dependency and wait tables remain separate because their reverse indexes and resolution rules differ. Journal and projection rows remain separate because they have different mutation, retention, and query workloads.

Event-version and execution `outcome_ref` columns do not exist. Dependency groups have no threshold. Coordinator retry policy/hash remain stored. All definitions, constraints, partial indexes, and SQL predicates use the reduced durable shapes.

## 14. Semantic transactions and lock order

The shared ordered executor captures PostgreSQL time after obtaining the execution lock, allocates journal positions, and exposes category-specific methods to ingress, settlement, plan, coordinator, and maintenance paths.

Blocking lock order:

```text
ascending execution IDs
  → commands/coordinator/plan-owned execution fields
  → dependency and wait rows
  → queue and journal/projections
  → application rows through commit function/caller code
```

Claims take only skip-locked locks and never wait, so they are exempt. Renewals are fenced single statements.

Caller-owned transactions record the greatest Flow execution ID already locked and reject a later lower ID. Flow operations precede application table locks. The library never commits or rolls back a caller transaction.

## 15. Command scheduler and leases

The runtime owns global and per-queue semaphores. It computes free slots before querying. The probe joins eligible queue rows with commands and filters exact registered name/version pairs. It uses `FOR UPDATE SKIP LOCKED`, a bounded candidate limit, and no blocking lock order.

Claim commit creates `AttemptStarted`, stores a random lease token and expiry, increments invocation ordinal, and returns decoded work. The connection is returned before handler invocation.

The production lease is a fixed 60 seconds, renewed near one third of its duration with bounded internal jitter. An unexported runtime config is available only to in-package tests. Expired-lease recovery is fenced, records interruption/loss, and restores the prior ready/retry schedule without consuming attempt budget or moving `BudgetStartedAt`.

## 16. Notifications, polling, and connections

One runtime listener owns one PostgreSQL notification connection and fans wake hints into in-process scheduler channels. Worker goroutines do not each listen or continuously poll. Each scheduler coalesces hints and also performs bounded polling, so dropped notifications or listener reconnects cannot lose work.

Application handlers do not hold pool connections. Connections are used for short claims, renewals, settlements, reconciliation, coordinator transitions, maintenance, ingress, and inspection.

## 17. Event ingress

`Event.Emit` validates the event definition, exact key, payload, execution ID, and canonical 64 KiB body. Under the execution lock it first checks idempotency by `(execution,name,key)`. Equivalent history returns success even after terminality; disagreement conflicts; a genuinely new terminal write is rejected.

New acceptance appends the application event, resolves exact waits, marks plan mode dirty, and leaves the position available to coordinator selection. The method may execute through `Runtime.InTx`.

Handler scopes stage application events only through `flow.Emit`, which performs no SQL. `Event.Emit` remains external ingress and is rejected through the attempt context because it would escape fencing; creating an unrelated context to bypass that guard remains prohibited application misuse.

## 18. Completion and failure

Execution counters are updated incrementally under the execution lock. The store does not scan the whole graph to decide completion.

- direct succeeds when its required closed tree succeeds and no command remains open;
- plan succeeds when no command/wait remains open, dirty is clear, and last reconciliation is quiescent;
- coordinator succeeds only on staged `Succeed` with no required open command;
- required unsuccessful command enters failing according to fail-fast policy;
- staged coordinator `Succeed` cancels optional outstanding or newly staged work; staged `Fail`, cancellation, or execution deadline cancels all outstanding or newly staged work before terminalizing.

Plan read absence blocks success, not failure. Explicit failure-handling commands determine when failing may terminalize; impossible success values never keep a doomed execution alive.

## 19. Inspection, observation, and faults

`GetExecution` is exact by ID. `ListExecutions` uses bounded indexed filters. `Trace` batches command, attempt, dependency, wait, coordinator, and cause queries. `History` scans one execution by position with a page limit. `AwaitExecution` combines notification hints and polling.

Observer callbacks receive immutable bounded values and cannot affect correctness. Required observations cover starts, claims, attempts, retries, terminal outcomes, plan dirty age/evaluation, coordinator delivery, notification health, maintenance, long attempts, and interruption ratios.

Named fault hooks surround journal append, projection writes, claim commit, handler return, fence validation, commit function, plan/coordinator settlement, maintenance, notification, and ambiguous commit boundaries. Hooks remain internal/test-only.

## 20. Testing and performance

Tests are layered:

1. canonical codecs, retry data, definitions, and decision properties without PostgreSQL;
2. migration, constraint, SQL ordering, idempotency, and replay/store tests with real PostgreSQL;
3. distributed runtime, lease, failure, cancellation, plan, coordinator, and poll-only integration tests;
4. the four public examples as E2E tests against real PostgreSQL;
5. race, fault-injection, ambiguous-commit, and rolling-version tests;
6. query-plan and workload benchmarks for claims, dirty plans, waits, coordinator selection, and journal growth.

Claim-path benchmarks include head-of-lane unhandled command kinds and same-execution bursts. Plan benchmarks include dirty probe, snapshot loading, and 10/100/1,000 declarations. Coordinator benchmarks include sparse `OnOutcome` selection.

## 21. Security and data handling

Flow stores application-provided command/event/state bodies. Logs and observer values must use IDs, names, sizes, classifications, and hashes rather than payload bodies by default. Cancellation/failure messages are bounded and safe diagnostics. Schema names and queue names are validated and never interpolated from untrusted values without quoting/allow-listing.

## 22. Retention

M1 retains terminal executions and complete journals indefinitely. Retention is a near-term operational follow-on because journal rows dominate growth. A future two-stage policy may remove bulky command/event payload bodies before retaining causal skeletons, but doing so explicitly narrows historical decode/simulation guarantees. Command/current-state projection cleanup must never precede journal policy in a way that leaves active work stranded.

## 23. Responsibility matrix

| Concern | Engine | Store | Runtime |
|---|---:|---:|---:|
| typed definitions/codecs | owner | consumes erased values | registry |
| declaration equality | owner | enforces accepted uniqueness | — |
| retry decision | owner | persists policy/times | schedules |
| journal ordering | builds batch | owns lock/allocation/append | invokes path |
| dependencies/waits | derives transitions | owns rows/indexes | maintenance wake |
| queue eligibility | derives | materializes | claims |
| worker decision | validates | settles fenced batch | invokes handler |
| plan reconciliation | evaluates/validates | snapshots/commits | schedules |
| coordinator decision | validates | serializes/commits | invokes handler |
| notification | — | transactional hint | listener/coalescing |
| replay | reducer semantics | row codec/scan | conformance tests |

The boundaries are deliberate: pure semantics remain testable without PostgreSQL, durable ordering and indexing remain centralized, and runtime goroutines orchestrate rather than reinterpret rules.
