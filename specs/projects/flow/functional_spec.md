---
status: complete
---

# Functional specification: flow

## 1. Purpose

Flow provides event-driven, durable, distributed execution for typed Go commands on PostgreSQL. Its public orchestration grammar is intentionally limited to commands, workers, coordinators, and execution-scoped events.

The runtime supplies durable queues, retries, attempt timeouts, leases, fencing, exact event waits, cancellation, deadlines, inspection, replay, and crash takeover. Application code remains ordinary typed Go.

## 2. Product scope

Flow supports:

- direct background commands;
- bounded worker-created child commands;
- exact keyed event gates;
- coordinator-owned joins, branches, races, and loops;
- typed application events from accepted decisions or external publishers;
- durable execution and command idempotency;
- caller-owned PostgreSQL transactions;
- independently deployed API, worker, and coordinator processes;
- ordered history, replay, and operational trace.

Flow does not support a declarative workflow graph, command dependencies, automatic result dataflow, arbitrary external command injection, a global event bus, or exactly-once external side effects.

## 3. Definitions

### 3.1 Commands

`DefineCommand[A,R](name, version, options...)` creates an immutable typed definition. Names are stable and versions are positive integers. Definition options configure retry policy, attempt timeout, and queue.

`Handle(command, worker, options...)` registers the exact name/version with a runtime. A worker receives `*Work[A]`, whose public inputs are typed arguments and immutable `CommandInfo`. It returns `R` or an error.

### 3.2 Events

`DefineEvent[T](name)` creates an unversioned typed application-event definition. An application event is uniquely identified by execution ID, event name, and non-empty event key.

### 3.3 Coordinators

`DefineCoordinator[S](name, version, handlers...)` creates a durable state machine with typed state. Handler selectors are:

- `OnStart` for its initial delivery;
- `On(event, handler)` for application events;
- `OnOutcome(command, handler)` for terminal command outcomes.

Duplicate selectors are invalid. A coordinator is also its registration value.

## 4. Starting executions

Calling `command.With(client).Execute` starts direct mode. Calling `coordinator.With(client).Execute` starts coordinator mode. Both operations are durable and asynchronous.

An execution start accepts:

- a stable key or live-scoped key;
- metadata;
- an execution deadline or explicit absence of one;
- fail-fast selection;
- for a direct root, an initial delay and exact event gates.

A non-empty permanent key identifies at most one execution for `(mode, definition name, key)`. Repeating an equivalent start returns the existing handle; conflicting canonical start content is rejected. A live key identifies at most one non-terminal execution and can be reused after settlement.

A direct execution contains a root command. A coordinator execution contains one coordinator state machine and initially no command.

## 5. Worker decisions

A worker may call:

```go
flow.Emit(work, event, key, payload)
flow.Execute(work, commandKey, command, args)
```

These calls record an in-memory decision. They perform no SQL during handler execution. The result, optional `WithCommit` callback, staged events, staged child commands, parent terminal event, readiness changes, and execution progression commit in one fenced transaction.

Command staging returns a non-generic ephemeral `*Node`. Its modifiers are:

- `Optional()` marks the command non-required;
- `Delay(d)` adds an initial eligibility delay;
- `WaitFor(event,key)` adds an exact event gate;
- `Within(d)` bounds its event wait.

The node's `Key` is available for logging and local bookkeeping. Nodes do not expose outcome or child-membership reads.

Repeated events or commands with the same decision identity and canonical content coalesce. Additive repeated command declarations merge exact waits. Singleton disagreement, type/size violations, or invalid scope use poisons the complete decision.

A worker cannot read another command's outcome. It receives required data in arguments. Bounded children discovered by the worker are created atomically with its success.

## 6. Coordinator decisions

A coordinator consumes one retained input per accepted delivery. `Coordination[S].State` is mutable only inside that handler. It uses the same `flow.Execute`, `flow.Emit`, and `Node` grammar as workers.

`OnOutcome` receives `Received[Outcome[R]]`. The outcome always exists for a terminal command and contains one of `succeeded`, `failed`, `cancelled`, or `expired`; only success contains `R`.

The coordinator must call `Succeed()` or `Fail(error)` to become terminal. Staging after terminal selection is a decision defect. If a handler returns a retryable error, the delivery and prior state remain unaccepted and retry according to coordinator policy. Equivalent redelivery is safe because accepted inbox position and staged keys are durable.

Coordinator command outcomes and matching application events are retained until a compatible coordinator replica accepts them. Irrelevant journal entries are skipped by bounded scanning without application invocation.

## 7. Application events

Inside a worker or coordinator, `flow.Emit` stages an event atomically with the decision. Outside a handler, `Event.Emit` appends through a `Client`, optionally participating in `Runtime.InTx`.

Equivalent repeated publication is idempotent. A different canonical payload for the same identity is a conflict. A new event is rejected after execution terminality, while an equivalent already-recorded event remains idempotently discoverable.

Events are immutable journal facts. Publishing an event may satisfy command waits and make a coordinator delivery eligible in the same semantic transaction.

## 8. Exact event gates

A direct start declares gates with `flow.WaitFor(event,key)` and optionally `flow.Within(duration)`. A staged command declares the same values on its node.

Rules:

1. Matching is exact on execution, application event name, and event key.
2. Multiple waits are AND conditions.
3. Identical waits deduplicate; different waits accumulate.
4. A retained earlier event satisfies a later declaration.
5. A later event resolves the corresponding unresolved row atomically.
6. `Within` is legal only when at least one wait exists.
7. The wait budget begins at command creation, not after delay eligibility.
8. Delay and wait readiness must both be satisfied before claiming.
9. Expiry terminalizes the command; a late event remains history and cannot resurrect it.

## 9. Attempts, retries, and leases

Ready commands are claimed with `FOR UPDATE SKIP LOCKED` only by runtimes that registered the exact command name/version and have lane capacity. Claims create an attempt identity and renewable lease.

The handler executes without holding a PostgreSQL connection. Settlement verifies execution state, command state, attempt ID, lease token, and ownership. A stale or superseded attempt cannot commit progress.

Retry policies are created by `Attempts(n)` or `RetryFor(duration)` and installed with `WithRetry`. `Permanent(err)` bypasses retry. Attempt timeouts cancel handler context and are classified by runtime policy. Durable jitter is fixed by Flow rather than configured per command.

Application handlers remain at-least-once because a process can lose its lease after an external effect and before durable settlement. External effects therefore use stable idempotency keys. `WithCommit` is the mechanism for application database writes that must share Flow's fenced settlement transaction.

## 10. Failure semantics

Command statuses are `succeeded`, `failed`, `cancelled`, and `expired`. Execution statuses are `running`, `failing`, `succeeded`, `failed`, `cancelled`, and `expired`.

With fail-fast enabled, a required failure:

- records the failure and moves the execution to `failing`;
- cancels commands that are pending, ready, or waiting to retry;
- does not revoke already-running attempt leases;
- accepts settlement from a valid surviving attempt;
- records any children staged by that attempt but immediately terminalizes them as cancelled;
- preserves the settling attempt's result, application events, and application commit;
- fails the execution after all surviving open attempts settle.

With fail-fast disabled, other commands continue. Once all commands close, any required failure causes execution failure. Optional unsuccessful commands do not fail direct execution.

Coordinator code normally marks staged orchestration commands optional and converts received outcomes into explicit coordinator success/failure policy.

## 11. Cancellation and deadlines

Cancellation is idempotent and durably terminalizes pending work. Running attempts are fenced from later settlement according to cancellation semantics. Execution deadlines are derived from PostgreSQL time and expire non-terminal executions through maintenance. The default deadline is 30 days unless disabled.

## 12. Completion

A direct execution succeeds when all commands are terminal and no required command failed. It fails under the rules above and may terminate cancelled or expired.

A coordinator execution completes only after the coordinator explicitly becomes terminal and all command progression required by its accepted decisions has been accounted for. Coordinator failure determines execution failure.

Every execution terminal state has one runtime terminal event in history.

## 13. Durable storage and journal

Flow owns seven prefixed tables in the selected application schema: executions, commands, command queue, command event waits, coordinators, journal, and schema migrations.

Every semantic mutation locks its execution first. Journal positions are allocated under that lock and are gap-free and commit-ordered within one execution. Immutable journal rows and mutable projections commit together.

Journal kinds are:

- `execution_started`;
- `execution_failing`;
- `command_created`;
- `attempt_started`;
- `attempt_concluded`;
- `event_recorded`;
- `coordinator_transition`.

Runtime terminal events distinguish command, execution, and coordinator outcomes. Event and command declaration bodies are canonical and hashed.

## 14. Runtime and deployment

`Migrate` is an explicit deployment operation. `New` validates exact migration checksums and compatibility and starts no background work. `Register` is allowed before `Run`; registry mutation then freezes. One runtime runs at most once.

`Run` starts bounded command and coordinator schedulers, renewers, readiness/deadline/recovery maintenance, optional notification listening, and observer delivery. Notifications are hints; polling is always sufficient for correctness.

Configuration includes maximum commands per execution, global and per-queue worker concurrency, coordinator concurrency, poll interval, notification enablement, shutdown grace, observer, and schema.

API-only publishers do not call `Run`. Worker and coordinator roles can be combined or separately scaled. Missing exact registrations leave durable work unclaimed rather than failing it.

## 15. Inspection, replay, and testing

Public inspection includes `GetExecution`, `LookupLiveExecution`, `ListExecutions`, `AwaitExecution`, paginated `History`, and `Trace`. Trace exposes commands, parent identity, readiness waits, attempts, events, coordinator state, causation, and complete bounded history.

`ResultOf` and `OutcomeOf` are typed conveniences over `ExecutionTrace` only.

Replay is a pure fold over journal rows. Projection conformance tests compare replayed semantic state with live materializations. `flowtest` invokes production decision recorders without PostgreSQL and reports staged commands/events and coordinator transitions.

## 16. Limits

- command arguments: 256 KiB canonical JSON;
- command result: definition codec bound enforced by settlement;
- coordinator state: 256 KiB;
- application event payload: 64 KiB;
- execution metadata: 16 KiB;
- execution and command keys: 1024 bytes;
- default maximum commands per execution: 1000, configurable and optionally disabled;
- bounded history pages and a bounded initial trace fold.

## 17. Acceptance criteria

1. Public documentation and `go doc` expose no declarative graph API or compatibility alias.
2. Direct, worker-child, and coordinator-created commands share one durable command model.
3. Exact event waits pass event-before-command, command-before-event, multi-wait, delayed, expiry, and late-event tests.
4. Workers have no dependency-result input; later work receives explicit arguments or coordinator state.
5. Dynamic fan-out/fan-in is demonstrated by coordinator state with duplicate, zero-work, mixed-outcome, and exactly-once continuation handling.
6. Reduced fail-fast preserves running settlements and events while cancelling their newly staged children.
7. Migrations create exactly seven `flow_` tables and accept only `direct` and `coordinator` execution modes.
8. Replay, trace, history, observers, fault hooks, tests, examples, and benchmarks contain no removed runtime state.
9. Database-free and real-PostgreSQL suites, vet, formatting, and repository scans pass.
