---
status: complete
completed_at: 2026-08-05
---

# flow

## Summary

`flow` is a typed Go library for durable, distributed work on PostgreSQL. It is intended for applications that need background work to survive process crashes, retries, deploys, and temporary worker outages without introducing a separate workflow service.

Its complete orchestration model is deliberately small:

```text
commands + workers + run-scoped events
```

- A command is a durable, typed instruction identified by a stable name and positive version.
- A worker handles one exact command name/version, returns a typed result, and may atomically emit events or stage bounded sub-commands.
- A command may wait for exact application events and read the values of only those declared inputs.
- A run owns one root command and the command tree created from it.
- PostgreSQL is the queue, coordination authority, journal, and current-state store.

There is one run kind and one command-delivery scheduler. Flow has no declarative workflow graph, coordinator/state-machine API, command-outcome subscription, or automatic result dataflow.

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

Starting a command creates an asynchronous run. `Enqueue` never invokes a
worker inline. It returns compact `EnqueueResult{RunID, Created}` operation
data. `GetRun` and `AwaitRun` return the full current or final durable `Run`
snapshot only when it is needed.

Each run begins with one root command. A successful worker may stage children, and those children may stage more work. Every child has exactly one parent, so the resulting tree records ownership and provenance even when events synchronize work across branches.

Worker decisions are all-or-nothing. A result, `WithCommit` database writes, staged events, staged children, terminal command history, readiness changes, and run progression commit in one fenced PostgreSQL transaction. If the transaction cannot commit, none of that progress is accepted.

Worker processing is nevertheless at-least-once rather than exactly-once. A lease may expire after an external side effect but before settlement becomes durable, so side effects outside Flow's settlement transaction still need stable application idempotency keys.

## Composition model

Flow builds larger behavior from command-owned composition rather than a stored graph definition:

- **Sequence:** a worker stages the next command after completing its own work.
- **Fan-out:** a worker stages a bounded set of children with stable run-local keys.
- **Fan-in:** a join command declares exact event inputs from every required branch.
- **Repeated stages:** a join worker may stage the next fan-out/join phase.
- **Bounded loops:** a worker stages a later command carrying the next explicit state.
- **Delay:** a root or child becomes eligible after a durable initial delay.
- **External gate:** a command is created first and waits for an exact event emitted later.
- **Cross-run delivery:** application code deliberately delivers a target-local event to a known run.

Stable command keys make worker retries deterministic. Repeating an equivalent declaration coalesces; trying to reuse a key for different durable work rejects the entire decision.

Multiple waits on one command are exact AND conditions. Flow deliberately does not infer dependencies from results or observe arbitrary command outcomes. Parent-computed data is passed directly in child arguments. Sibling or external data moves through declared event payloads, stable application references, or application tables.

Command boundaries should correspond to independent retry, side-effect,
isolation, timeout, queue-ownership, or useful parallelism boundaries. Small
deterministic transformations remain inside the worker that owns them, and
several small same-database writes may share one `WithCommit` callback. A
microstep whose durable lifecycle costs more than its work is not a useful
command boundary.

A run is one serialized semantic aggregate. Causally related work
belongs together, while independent bulk items or shards should use separate
runs rather than one tenant-wide or global container. The default
1,000-command ceiling is a safety limit, not a target or a new recommended hard
maximum. Ordinary runs should normally remain in the tens or low
hundreds. Very large fan-outs can be chunked behind bounded batch commands, and
large all-of inputs can be combined through hierarchical join commands.

Related events and children should be staged in one decision when they form one
atomic change. Large or sensitive documents remain in application storage and
move through Flow as stable references in arguments or event payloads.
One command may wait for at most 256 exact events, and one worker decision may
stage at most 256 distinct application events.

## Event model

Application events are immutable facts local to one run. Their identity is:

```text
(run ID, event name, event key)
```

The event key is a correlation identity both producer and waiting command can know in advance. Provider-generated values normally belong in the payload rather than in the key.

Flow exposes two intentionally different event paths:

| API | Transaction boundary | Intended use |
|---|---|---|
| `flow.Emit(work, event, key, value)` | staged with the current worker decision | same-run facts that must disappear if the attempt fails |
| `event.Deliver(ctx, client, runID, key, value)` | immediate, deliberately detached ingress | targeted delivery to a known run, including from an active worker |

`Event.Deliver` is not publish/subscribe. It does not discover targets, store source identity, or make target workers exactly once. A committed delivery survives source failure and retry; stable keys and deterministic payloads make repeated delivery idempotent.

A waiting worker reads an input with `GetEventValue`, whose `found` result
distinguishes ordinary absence from a present typed value. The immutable
attempt snapshot was loaded before invocation; the call neither blocks nor
queries PostgreSQL. Retries and lease takeovers receive the same payload and
satisfying journal position.

Event definitions name stable semantic fact kinds. Applications use one
deterministic event-key helper to carry entity and generation identity through
`WaitFor`, `Deliver`, and `GetEventValue`. `GetCurrentRun` followed by
`Deliver` is an explicit two-operation composition: the selected run may settle
between them, in which case new ingress returns `ErrTerminal`.

## Identity and idempotency

A non-empty run key is permanently idempotent by default. Repeating an
equivalent start for the same command name and key returns the original
`RunID` with `Created=false`. Reusing that identity with different start inputs
or options returns a conflict. An empty permanent key creates a new run on
every call.

`WithLiveKey` changes the contract to queue-style deduplication. At most one non-terminal run for a command name/key may exist. While it is live, another start silently rediscovers it without comparing arguments or options; after it becomes terminal, the key is available for a new run.

Command and event identities are similarly durable. Equivalent repeated
declarations or publications are no-ops, while conflicting content is
rejected. The first accepted declaration fixes its arguments, definition
version, queue, retry policy, timeout, delay, and waits.

## Durability guarantees

Every semantic mutation is scoped to one run and locks its run row first. Within that transaction Flow:

1. validates the current run and command state;
2. allocates consecutive journal positions;
3. appends immutable semantic entries;
4. updates current-state, readiness, and delivery projections;
5. emits a transactional run-identity hint when the mutation creates
   immediately runnable work, records an application event, or terminalizes
   the run and notifications are enabled; and
6. commits all changes together.

The journal is gap-free and commit-ordered within each run. It records run start/failing, command creation, attempt start/conclusion, application events, and command/run terminal events. Current projections make claims and inspection efficient; replay verifies that retained semantic history reconstructs the same outcome.

Event readiness is delta-based: accepted events use the reverse-wait index to
update matching unresolved waits and decrement only their commands'
`unsatisfied_waits` counters. Normalized child/event decisions and
same-run claims are persisted in bounded sets. A runtime may claim groups
from independent runs concurrently within its worker and database-pool
capacity, but mutations within one run remain serialized.

Journal integrity has three deliberate boundaries. Accepted writes canonicalize
and hash every body. The claim hot path verifies the retained hash and decodes
the bounded application-event envelope without redundant reconstruction. Full
replay re-canonicalizes history for stronger conformance diagnostics.

Claims install an attempt ID, lease token, owner, resolved lease duration, and expiry. Only the currently fenced attempt may settle. A stale worker may finish locally after lease loss, but its result, events, children, and commit callback cannot become durable.

## Failure and time

Each command definition owns an immutable retry policy, optional attempt timeout, optional recovery-lease override, and queue. The recovery lease controls dead-worker takeover while the timeout bounds one invocation; unset commands retain the conservative 60-second lease. Retriable errors, requested retry delays, panics, timeouts, shutdown interruption, and lease loss are classified separately. Shutdown interruption and lease loss do not consume the application attempt budget.

Runs have a 30-day deadline by default unless configured otherwise. `Within` is a separate lifetime for commands waiting on exact events and begins when the command is created; `Delay` does not postpone it. An event committed on or before the persisted wait deadline wins, even if expiry maintenance observes it later.

Positive public durations that become durable configuration are rounded upward
once to the next whole millisecond before fingerprinting and persistence.
Strict store and decode boundaries continue to require exact millisecond values.

Any command that exhausts retry or is cancelled/expired makes the run fail.
Flow cancels open work that has no active attempt while allowing already
running attempts to settle through their fences.

## Transactions and application state

`runtime.InTx(tx)` returns a named `TransactionClient` for starts, event
ingress, cancellation, replacement, and inspection in a caller-owned
PostgreSQL transaction. Flow never commits or rolls back that transaction.
Applications create the client exactly once per `pgx.Tx`, do not use it
concurrently or after the transaction ends, perform Flow operations first,
call `BeginApplicationWrites`, and then lock or write application rows. Every
later Flow write/locking operation through that client fails before SQL.
Multiple pre-existing runs are acquired in ascending run-ID order; rows first
inserted by the transaction are transaction-owned and create no cross-
transaction lock edge.

`Command.ReplaceCurrentRun` is the atomic live-key supersession operation. It
locks the expected predecessor, cancels it, and starts one distinct successor
in the same transaction. Equality with the expected ID always means replace,
even for an identical declaration. Declaration equivalence is consulted only
after the current ID differs, allowing retry after an ambiguous commit to
rediscover the committed successor without mutating it.

`WithCommit` is the completion-side transaction boundary. It receives the worker arguments, result, command information, and Flow's fenced PostgreSQL transaction. It is suitable for application-table writes that must commit exactly with command success. If it returns an error, the success settlement rolls back and the error follows normal permanent/retryable classification.

Neither mechanism makes remote calls or other non-transactional effects exactly once.

Both transaction forms should be short. `WithCommit` is for bounded
same-database writes, not remote calls, and a caller-owned transaction retains
each acquired run lock until the caller commits or rolls back.

## Storage and operations

Flow owns exactly six tables in a configurable PostgreSQL schema:

| Table | Role |
|---|---|
| `flow_runs` | aggregate identity, status, counters, deadlines, and the run lock |
| `flow_commands` | durable declarations, provenance, semantic state, result, and failures |
| `flow_command_queue` | hot readiness, retry, active-attempt, and lease projection |
| `flow_command_event_waits` | exact wait selectors and their satisfying journal positions |
| `flow_journal` | immutable ordered history and application-event bodies |
| `flow_schema_migrations` | checksummed schema version and compatibility ledger |

`Migrate` installs the one clean baseline migration explicitly. An older
multi-migration development schema is not upgraded; operators drop and
recreate the Flow schema first. `New` verifies schema compatibility and starts
nothing. `Run` owns a bounded scheduler, lease renewal,
wait/deadline/recovery maintenance, optional notification listening, observers,
and graceful shutdown. Polling remains sufficient for command processing;
EventWatch is a notification-backed inspection API with listener
startup/reconnect catch-up and no periodic polling.

`PruneTerminalRuns` deletes one bounded batch of old terminal unkeyed or
live-key aggregates in a Flow-owned transaction. Permanent non-empty keys and
all application tables remain outside pruning. There is no automatic TTL or
archival service.

Worker registration matches exact command name/version pairs. Unhandled versions remain durable until a compatible replica is deployed, they are cancelled, or a deadline expires. A process may host every worker, a selected pool of workers, or only API publishers that never call `Run`.

## Inspection and testing

`GetRun`, both definition-bound and top-level `GetCurrentRun`, `ListRuns`,
`AwaitRun`, both forms of `GetResult`, `GetQueueStats`, `History`, and `Trace`
expose durable state without invoking application code. `GetResult` decodes one successful command result directly
from its `(run ID, command key)` projection without replay. Trace includes
command provenance, exact waits and satisfying positions, attempts,
results/failures, events, operational lease state, and ordered history.
`ResultOf` decodes a successful command result already held in a trace snapshot;
workers use neither helper as implicit dataflow.

`flowtest` exercises the production decision recorder, codecs, retry calculation, and commit callback without PostgreSQL. PostgreSQL integration tests cover migrations, claims, fencing, retries, exact event inputs, single failure propagation, cancellation, pruning, replay conformance, transaction ownership, notification loss, multi-replica behavior, and all examples.

## Fit and deliberate limits

Flow is a good fit when workflows are naturally expressed as durable commands, bounded command trees, and exact event gates, and when the application already relies on PostgreSQL.

It is not intended for workflows that require first-of-N races, quorum gates, reactions to unsuccessful command outcomes, arbitrary mutable workflow state, global event subscriptions, dynamically injected commands, unbounded histories without an application retention plan, or exactly-once non-transactional effects. Those needs require another coordination model rather than hidden complexity in this one.
