---
status: complete
completed_at: 2026-08-05
---

# flow

## Summary

`flow` is a typed Go library for durable, distributed work execution on PostgreSQL. It is intended for applications that need background work to survive process crashes, retries, deploys, and temporary worker outages without introducing a separate workflow service.

Its complete orchestration model is deliberately small:

```text
commands + workers + execution-scoped events
```

- A command is a durable, typed instruction identified by a stable name and positive version.
- A worker handles one exact command name/version, returns a typed result, and may atomically emit events or stage bounded sub-commands.
- A command may wait for exact application events and read the values of only those declared inputs.
- An execution owns one root command and the command tree created from it.
- PostgreSQL is the queue, coordination authority, journal, and current-state store.

There is one execution kind and one command-delivery scheduler. Flow has no declarative workflow graph, coordinator/state-machine API, command-outcome subscription, or automatic result dataflow.

## The problem Flow solves

Ordinary goroutines and job queues make it easy to run one function later, but applications often need stronger behavior:

- a request and its background work must commit together;
- a worker may create follow-up work without exposing half of its decision;
- several independently completed branches may need to join;
- an external fact may need to release durable work created earlier;
- a crashed or partitioned worker must be retried without accepting stale completion;
- operators need durable history rather than process-local logs;
- multiple replicas must safely share the same work.

Flow supplies those properties while keeping orchestration inside ordinary typed Go workers. It does not attempt to be a general-purpose BPM system or an event broker.

## Mental model

Starting a command creates an asynchronous execution. `Execute` never invokes a worker inline. It returns the `Execution` snapshot as of durable acceptance, whose `Created` field tells the caller whether this call created the execution or rediscovered it. `GetExecution` and `AwaitExecution` return the same `Execution` type with the current or final durable state.

Each execution begins with one root command. A successful worker may stage children, and those children may stage more work. Every child has exactly one parent, so the resulting tree records ownership and provenance even when events synchronize work across branches.

Worker decisions are all-or-nothing. A result, `WithCommit` database writes, staged events, staged children, terminal command history, readiness changes, and execution progression commit in one fenced PostgreSQL transaction. If the transaction cannot commit, none of that progress is accepted.

Worker processing is nevertheless at-least-once rather than exactly-once. A lease may expire after an external side effect but before settlement becomes durable, so side effects outside Flow's settlement transaction still need stable application idempotency keys.

## Composition model

Flow builds larger behavior from command-owned composition rather than a stored graph definition:

- **Sequence:** a worker stages the next command after completing its own work.
- **Fan-out:** a worker stages a bounded set of children with stable execution-local keys.
- **Fan-in:** a join command declares exact event inputs from every required branch.
- **Repeated stages:** a join worker may stage the next fan-out/join phase.
- **Bounded loops:** a worker stages a later command carrying the next explicit state.
- **Delay:** a root or child becomes eligible after a durable initial delay.
- **External gate:** a command is created first and waits for an exact event emitted later.
- **Cross-execution delivery:** application code deliberately delivers a target-local event to a known execution.

Stable command keys make worker retries deterministic. Repeating an equivalent declaration coalesces; trying to reuse a key for different durable work rejects the entire decision.

Multiple waits on one command are exact AND conditions. Flow deliberately does not infer dependencies from results or observe arbitrary command outcomes. Parent-computed data is passed directly in child arguments. Sibling or external data moves through declared event payloads, stable application references, or application tables.

Command boundaries should correspond to independent retry, side-effect,
isolation, timeout, queue-ownership, or useful parallelism boundaries. Small
deterministic transformations remain inside the worker that owns them, and
several small same-database writes may share one `WithCommit` callback. A
microstep whose durable lifecycle costs more than its work is not a useful
command boundary.

An execution is one serialized semantic aggregate. Causally related work
belongs together, while independent bulk items or shards should use separate
executions rather than one tenant-wide or global container. The default
1,000-command ceiling is a safety limit, not a target or a new recommended hard
maximum. Ordinary executions should normally remain in the tens or low
hundreds. Very large fan-outs can be chunked behind bounded batch commands, and
large all-of inputs can be combined through hierarchical join commands.

Related events and children should be staged in one decision when they form one
atomic change. Large or sensitive documents remain in application storage and
move through Flow as stable references in arguments or event payloads.

## Event model

Application events are immutable facts local to one execution. Their identity is:

```text
(execution ID, event name, event key)
```

The event key is a correlation identity both producer and waiting command can know in advance. Provider-generated values normally belong in the payload rather than in the key.

Flow exposes three intentionally different ingress paths:

| API | Transaction boundary | Intended use |
|---|---|---|
| `flow.Emit(work, event, key, value)` | staged with the current worker decision | same-execution facts that must disappear if the attempt fails |
| `event.Emit(ctx, client, executionID, key, value)` | immediate external ingress | application or operator code outside a worker attempt |
| `event.Deliver(ctx, client, executionID, key, value)` | immediate, deliberately detached ingress | targeted delivery to a known execution, including from an active worker |

`Event.Deliver` is not publish/subscribe. It does not discover targets, store source identity, or make target workers exactly once. A committed delivery survives source failure and retry; stable keys and deterministic payloads make repeated delivery idempotent.

A waiting worker reads a declared value with `GetEventValue`. The value was loaded before invocation and is already in memory; the call neither blocks nor queries PostgreSQL. Retries and lease takeovers receive the same immutable event snapshot and satisfying journal position.

## Identity and idempotency

A non-empty execution key is permanently idempotent by default. Repeating an equivalent start for the same command name and key returns the original execution with `Created=false`. Reusing that identity with different start inputs or options returns a conflict. An empty permanent key creates a new execution on every call.

`WithLiveKey` changes the contract to queue-style deduplication. At most one non-terminal execution for a command name/key may exist. While it is live, another start silently rediscovers it without comparing arguments or options; after it becomes terminal, the key is available for a new execution.

Command and event identities are similarly durable. Equivalent repeated declarations or publications are no-ops, while conflicting content is rejected. The first accepted declaration fixes its arguments, definition version, queue, retry policy, timeout, delay, waits, and optionality.

## Durability guarantees

Every semantic mutation is scoped to one execution and locks its execution row first. Within that transaction Flow:

1. validates the current execution and command state;
2. allocates consecutive journal positions;
3. appends immutable semantic entries;
4. updates current-state, readiness, and delivery projections;
5. emits an optional transactional notification hint only when the mutation
   creates immediately runnable work; and
6. commits all changes together.

The journal is gap-free and commit-ordered within each execution. It records execution start/failing, command creation, attempt start/conclusion, application events, and command/execution terminal events. Current projections make claims and inspection efficient; replay verifies that retained semantic history reconstructs the same outcome.

Event readiness is delta-based: accepted events use the reverse-wait index to
update matching unresolved waits and decrement only their commands'
`unsatisfied_waits` counters. Normalized child/event decisions and
same-execution claims are persisted in bounded sets. A runtime may claim groups
from independent executions concurrently within its worker and database-pool
capacity, but mutations within one execution remain serialized.

Journal integrity has three deliberate boundaries. Accepted writes canonicalize
and hash every body. The claim hot path verifies the retained hash and decodes
the bounded application-event envelope without redundant reconstruction. Full
replay re-canonicalizes history for stronger conformance diagnostics.

Claims install an attempt ID, lease token, owner, and expiry. Only the currently fenced attempt may settle. A stale worker may finish locally after lease loss, but its result, events, children, and commit callback cannot become durable.

## Failure and time

Each command definition owns an immutable retry policy, optional attempt timeout, and queue. Retriable errors, requested retry delays, panics, timeouts, shutdown interruption, and lease loss are classified separately. Shutdown interruption and lease loss do not consume the application attempt budget.

Executions have a 30-day deadline by default unless configured otherwise. `Within` is a separate lifetime for commands waiting on exact events and begins when the command is created; `Delay` does not postpone it. An event committed on or before the persisted wait deadline wins, even if expiry maintenance observes it later.

A required command that exhausts retry or is cancelled/expired makes the execution fail. Reduced fail-fast is enabled by default: Flow cancels open work that has no active attempt while allowing already running attempts to settle through their fences. Optional command failure is observable but does not by itself fail the execution; optional work still keeps the execution open until it becomes terminal.

## Transactions and application state

`runtime.InTx(tx)` lets starts, event ingress, cancellation, and inspection participate in a caller-owned PostgreSQL transaction. Flow never commits or rolls back that transaction. Callers must reuse one transaction client, perform Flow operations before application writes, and acquire multiple existing executions in ascending execution-ID order.

`WithCommit` is the completion-side transaction boundary. It receives the worker arguments, result, command information, and Flow's fenced PostgreSQL transaction. It is suitable for application-table writes that must commit exactly with command success. If it returns an error, the success settlement rolls back and the error follows normal permanent/retryable classification.

Neither mechanism makes remote calls or other non-transactional effects exactly once.

Both transaction forms should be short. `WithCommit` is for bounded
same-database writes, not remote calls, and a caller-owned transaction retains
each acquired execution lock until the caller commits or rolls back.

## Storage and operations

Flow owns exactly six tables in a configurable PostgreSQL schema:

| Table | Role |
|---|---|
| `flow_executions` | aggregate identity, status, counters, deadlines, and the execution lock |
| `flow_commands` | durable declarations, provenance, semantic state, result, and failures |
| `flow_command_queue` | hot readiness, retry, active-attempt, and lease projection |
| `flow_command_event_waits` | exact wait selectors and their satisfying journal positions |
| `flow_journal` | immutable ordered history and application-event bodies |
| `flow_schema_migrations` | checksummed schema version and compatibility ledger |

`Migrate` applies embedded migrations explicitly. `New` verifies schema compatibility and starts nothing. `Run` owns a bounded scheduler, lease renewal, wait/deadline/recovery maintenance, optional notification listening, observers, and graceful shutdown. Polling is always sufficient for correctness.

Worker registration matches exact command name/version pairs. Unhandled versions remain durable until a compatible replica is deployed, they are cancelled, or a deadline expires. A process may host every worker, a selected pool of workers, or only API publishers that never call `Run`.

## Inspection and testing

`GetExecution`, `LookupLiveExecution`, `ListExecutions`, `AwaitExecution`, `GetQueueDepth`, `History`, and `Trace` expose durable state without invoking application code. Trace includes command provenance, exact waits and satisfying positions, attempts, results/failures, events, operational lease state, and ordered history. `ResultOf` decodes a successful command result from a trace snapshot; workers do not use it as implicit dataflow.

`flowtest` exercises the production decision recorder, codecs, retry calculation, and commit callback without PostgreSQL. PostgreSQL integration tests cover migrations, claims, fencing, retries, exact event inputs, fail-fast, cancellation, replay conformance, transaction ownership, notification loss, multi-replica behavior, and all examples.

## Fit and deliberate limits

Flow is a good fit when workflows are naturally expressed as durable commands, bounded command trees, and exact event gates, and when the application already relies on PostgreSQL.

It is not intended for workflows that require first-of-N races, quorum gates, reactions to unsuccessful command outcomes, arbitrary mutable workflow state, global event subscriptions, dynamically injected commands, unbounded histories without an application retention plan, or exactly-once non-transactional effects. Those needs require another coordination model rather than hidden complexity in this one.
