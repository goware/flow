---
status: complete
completed_at: 2026-08-05
---

# Functional specification: flow

## 1. Purpose and scope

Flow provides event-driven, durable, distributed work for typed Go commands on PostgreSQL. Its public orchestration grammar is limited to commands, workers, run-scoped application events, and exact event gates.

Flow supports direct background work, worker-created command trees, sequence, dynamic fan-out, all-of fan-in, repeated joins, bounded loops, initial delays, external signals, retries, attempt timeouts, leases, fencing, cancellation, run deadlines, inspection, and replay.

Flow intentionally omits declarative graphs, mutable coordinator state, event handlers, command-outcome subscriptions, OR/quorum/race gates, arbitrary external command injection, global events, implicit result dataflow, and exactly-once non-transactional effects.

This document is the normative caller-visible behavioral contract. The architecture and component specifications explain how it is implemented.

## 2. Terms

- **Command definition:** immutable typed name/version, argument/result codecs, retry policy, attempt timeout, and queue.
- **Run:** one durable aggregate containing a root command and every command staged below it.
- **Command:** one durable invocation inside a run, identified by a run-local stable key.
- **Worker:** application handler registered for one exact command name/version.
- **Attempt:** one leased, fenced invocation of a worker. A command may have several attempts.
- **Decision:** a worker's result plus its staged events and sub-commands.
- **Application event:** an immutable typed fact identified within one run by event name and key.
- **Wait:** an exact event selector declared before a command runs.
- **Projection:** mutable state used for efficient scheduling and inspection; projections commit with the semantic journal.

## 3. Definitions and registration

`DefineCommand[A,R](name, version, options...)` creates an immutable typed command definition.

- The name must be non-empty, contain no whitespace/control characters, and be at most 255 bytes.
- The version must be positive and fit PostgreSQL's signed `integer` range while remaining a Go `int` in the public API.
- `WithRetry` fixes the retry policy stored with every later declaration.
- `WithTimeout` fixes a positive per-attempt timeout, rounded upward to durable whole-millisecond precision.
- `WithQueue` selects a validated queue name; the default queue is `default`.
- Duplicate or invalid definition options make the definition invalid. The error is reported when it is registered or used.

`DefineEvent[T](name)` creates an immutable typed application-event definition. Events are not numerically versioned. A material payload-schema change uses a new event name, and publishers for old names must remain available until old waits drain.

`Handle(command, worker, options...)` creates a registration for one exact command name/version. `WithCommit` may attach one typed commit callback. `Runtime.Register` is atomic for each call: invalid registrations fail as `ErrInvalid`, duplicate exact registrations fail as `ErrConflict`, and the existing registry remains usable. Registration freezes when `Run` begins.

Compatible replicas may register different subsets and versions. A command whose exact worker is absent remains durable and unclaimed until a compatible runtime appears, it is cancelled, or an applicable wait/run deadline expires.

## 4. Starting runs

`command.Enqueue(ctx, client, runKey, args, options...)` validates and canonicalizes the start, then creates or rediscovers asynchronous durable work. It never invokes a worker inline.

The returned `Run` is the run's state snapshot as of durable acceptance, including:

- `ID`: accepted run ID;
- `Type` and `Version`: root command definition;
- `Key`: caller-supplied run key;
- `RootCommandID`: accepted root command ID;
- `Created`: true only for the call whose transaction created the run; and
- status, counters, deadline, timestamps, and metadata as of acceptance.

Inspection APIs return the same `Run` type with the current or final state; `Created` is always false on inspection reads.

### 4.1 Start options and defaults

Every run has:

- fail-fast enabled by default;
- a 30-day run deadline by default;
- a command ceiling copied from its runtime, default 1000;
- empty metadata by default;
- a root command eligible immediately unless delayed or gated; and
- permanent key scope by default.

Options are:

- `WithRunDeadline(d)`: positive duration measured from the accepted database start time;
- `WithoutRunDeadline()`: removes the run deadline;
- `WithMetadata(m)`: immutable, indexed start metadata;
- `WithFailFast(enabled)`: controls required-failure cancellation behavior;
- `WithLiveKey()`: selects live key scope and requires a non-empty key;
- `WithStartDelay(d)`: delays root eligibility by a positive duration;
- `WaitFor(event, key)`: adds one exact root event gate; and
- `Within(d)`: bounds a gated root's wait and is invalid without `WaitFor`.

Supplying a singleton option more than once is invalid, except repeated identical waits coalesce. Multiple waits are AND conditions.

Every positive public duration that becomes durable scheduling
configuration—including run deadlines, attempt timeouts, initial delays, wait
budgets, retry elapsed/backoff values, and explicit retry-after delays—is
rounded upward once to a whole millisecond before fingerprinting or
persistence. Equivalent normalized values therefore have the same durable
identity. Store and decode boundaries still reject non-exact durable values.

### 4.2 Permanent keys

A non-empty permanent key identifies at most one run for `(root command name, run key)` over the lifetime of retained Flow data.

An equivalent repeat returns the existing run with `Created=false`. Equivalent start identity includes command name/version, canonical arguments, key scope, deadline mode/duration, fail-fast, initial delay, waits, wait budget, and canonical metadata.

Changing any of those fields for an existing permanent identity returns `ErrConflict`. Runtime command ceilings and command-definition defaults such as queue/retry/timeout are not re-applied during rediscovery; the first accepted run and root declaration remain authoritative.

An empty permanent key has no idempotency identity and creates a new run on each call.

### 4.3 Live keys

`WithLiveKey` provides at-most-one live run for `(root command name, run key)`, where live means run status `running` or `failing`.

While a live holder exists, another start returns that run with `Created=false` without comparing version, arguments, metadata, or start options. This is an intentional queue-style dedupe no-op. When the holder becomes terminal, a later call creates a new run and acquires the key.

`GetCurrentRun` follows this invariant and does not return older terminal holders.

### 4.4 Caller-owned start transactions

Starting through `runtime.InTx(tx)` makes the run visible only according to the caller's PostgreSQL transaction. Flow neither commits nor rolls back that transaction. A rollback removes the entire uncommitted start; a commit makes the run and its initial journal batch visible together.

## 5. Worker invocation and command composition

A worker receives a fresh `*Work[A]` for each invocation. `Work` is the
attempt-local scope for one claimed command, not the run aggregate or immutable
command definition. It contains typed `Args`, immutable `CommandInfo`,
materialized event inputs, and private attempt-local decision state.
`CommandInfo` identifies the exact `RunID` and root `RunKey`, command, stable
command key, exact definition version, creation/budget times, and attempt
ordinal. `RunKey` is populated from the run head already loaded during claim;
it adds no point query. A `Work` value is valid only during its worker call and
must not be retained or used concurrently.

The runtime loads arguments and declared event inputs, releases all database resources, then calls application worker code. No PostgreSQL connection is held during ordinary worker execution.

Inside a worker:

- `Enqueue(work, key, command, args)` stages a sub-command and returns an ephemeral `Node`;
- `Emit(work, event, key, payload)` stages an application event in the current run;
- `GetEventValue(work, event, key)` decodes a declared event input already loaded in memory; and
- `work.Info()` returns immutable command/attempt information.

`Node` supports:

- `Optional()` to make child failure non-fatal to the run;
- `Delay(d)` to set one positive initial delay;
- `WaitFor(event, key)` to add an exact gate;
- `Within(d)` to bound a gated command's wait; and
- `Key()` to return the stable command key.

Nodes are valid only during the decision that created them.

### 5.1 Durable command identity

Command keys are unique within a run. A declaration fingerprint covers command name/version, key, canonical arguments, parent, required flag, queue, retry policy, attempt timeout, initial delay, exact waits, and wait budget.

Repeating one command key with the same core declaration during a decision coalesces. Distinct waits added by repeated declarations merge. Any disagreement in definition, arguments, optionality, delay, wait budget, queue, timeout, or retry policy poisons the complete decision.

A later attempt may repeat an already accepted declaration equivalently, but it may not amend it. Conflicting durable content rejects the complete decision. The first recorded defect wins; no partially valid subset is committed.

Children are normalized in stable key order and events in stable name/key order before settlement. The configured run command ceiling is checked against the complete batch. Zero disables the ceiling.

### 5.2 Dataflow

A worker may directly place values it computed into arguments for its children. Commands do not automatically receive parent or sibling results.

Sibling/external data may be supplied through:

- event payloads declared as exact inputs;
- stable references in arguments, resolved from application storage; or
- application tables read by the worker.

`GetResult(ctx, client, runID, key, command)` is a point inspection read over
the command projection. It returns the decoded typed value with `found=true`
only for a successful command. A missing command or one without a successful
result returns `found=false`; a missing run returns `ErrNotFound`; a mismatched
command name/version returns `ErrConflict`; and malformed retained result data
returns `ErrInvalidState`. It does not replay the run journal.

`ResultOf(trace, key, command)` decodes a successful command result already
present in a `RunTrace`. It fails permanently for a missing command, mismatched
definition, non-successful command, or undecodable result. Both APIs are
inspection helpers, not worker-time dataflow.

### 5.3 Composition granularity

A command should represent an independent retry, side-effect, isolation,
timeout, queue-ownership, or useful parallelism boundary. Small deterministic
transformations should stay inside the worker that owns them. Several small
writes to the same PostgreSQL database may share one `WithCommit` callback;
creating a durable command for each microstep adds a lifecycle without adding a
meaningful boundary.

One run is one serialized semantic aggregate. Causally related work
belongs in one run; independent bulk items or shards should use separate
runs instead of a tenant-wide or global run. The default
1,000-command ceiling is a safety limit rather than a normal target. Runs
in the tens or low hundreds are the ordinary shape; this guidance does not add
a hard limit beyond the configured ceiling.

Very large fan-outs should be chunked into bounded batch commands when one
enormous atomic child declaration is unnecessary. Large all-of inputs should
use hierarchical join commands. Parent-produced data should be passed directly
in child arguments, exact events should carry sibling/cross-branch/external
facts, and related events and children should be staged together when they form
one atomic decision. Large or sensitive documents should remain in application
storage behind stable references.

## 6. Application events and exact inputs

Application-event identity is `(run ID, event name, event key)`. The event key must be non-empty, stable, valid UTF-8 without surrounding whitespace, and at most 1024 bytes.

An equivalent repeat with the same canonical payload is an idempotent no-op. Reusing the identity with a different payload returns `ErrConflict`. Idempotency is checked before terminality: repeating an already recorded equivalent event remains successful after the run settles, while a new event cannot reopen a terminal run.

Events recorded before a command is declared still satisfy matching waits.

### 6.1 Two event paths

| API | Allowed context | Commit behavior | Failure relationship |
|---|---|---|---|
| `flow.Emit(work, ...)` | active worker decision | commits with successful fenced settlement | discarded if the attempt/decision fails |
| `event.Deliver(ctx, client, target, ...)` | external code or active worker | own transaction or caller transaction | detached from any source attempt |

`Event.Deliver` is the one immediate target-side ingress API. It uses the same
identity, journal, wait-resolution, and terminality rules as staged events but
is deliberately detached from an active worker attempt.

A regular runtime client commits delivery independently. `runtime.InTx(tx)` joins it to the caller's application writes. Once committed, delivery is not retracted if a source worker later fails, retries, loses its lease, or is cancelled. Producers must therefore choose stable keys and deterministic payloads.

Delivering to the current run is legal but detached. Same-run facts that must be atomic with worker success must use `flow.Emit`.

Delivery is targeted ingress to an already known run. It does not discover consumers, create source/target relationships, store delivery records, or provide exactly-once target handler invocation.

An external publisher that knows only a domain key may call `GetCurrentRun`
and then `Deliver` to the returned `RunID`. The run can settle between those
operations; a new event then returns `ErrTerminal`. Typed event definitions
name stable fact kinds, while applications use one deterministic composed key
for the same entity/generation at `WaitFor`, `Deliver`, and `GetEventValue`.

### 6.2 Wait semantics

All waits are exact AND gates. A command is claimable only when:

1. every declared `(event name, event key)` has a satisfying application event;
2. its initial delay has elapsed;
3. its wait budget and run deadline have not expired; and
4. it is otherwise ready and has a compatible registered worker.

`Within` starts at command creation, independently of `Delay`, and is capped by an earlier run deadline. An event committed at or before the persisted wait deadline satisfies the wait even if maintenance processes expiry later. If any wait is still absent after that deadline, the command becomes `expired`; late events cannot resurrect it.

At most 256 exact waits may be declared on one command. The store records each satisfying journal position. Claim materialization loads every selector and its canonical event body in one bounded query before releasing the connection.

Accepted event ingress updates only matching unresolved reverse-wait rows,
decrements the affected commands' `unsatisfied_waits` counters, and queues only
commands whose final wait was satisfied. Unrelated retained commands are not
part of ordinary event readiness resolution.

`GetEventValue(work, event, key)`:

- does not block;
- does not perform SQL;
- returns `(value, found, error)` from an O(1) attempt-local lookup;
- reports ordinary absence as `found=false` without poisoning the decision;
- returns the typed value when the immutable snapshot contains the selector;
  and
- poisons settlement for invalid definitions/keys or decode/type corruption.

Retries and lease takeovers receive the same immutable payloads selected by the recorded satisfying positions.

## 7. Successful settlement and `WithCommit`

After a worker returns successfully, Flow validates the attempt context, canonicalizes the result, normalizes the decision, and reacquires the run under the attempt fence.

One successful settlement transaction may include:

- the attempt conclusion and command result;
- `WithCommit` SQL;
- staged application events;
- staged child declarations and waits;
- command/run terminal events;
- queue and wait-readiness changes; and
- run counters/status progression.

Only the still-current attempt ID and lease token may settle. A lost or already concluded fence commits nothing from the stale worker.

`WithCommit` runs inside this transaction after durable changes have been prepared but before commit. It receives typed arguments/result, `CommandInfo`, and a restricted SQL transaction interface. It may write application tables in the same PostgreSQL database.

If the callback returns an error, the entire success transaction rolls back. `Permanent` and `RetryAfter` retain their normal classifications; invalid/conflict/state/payload Flow errors are permanent invalid decisions; other errors are retryable. The callback may run again on a later attempt, so it must not perform non-transactional effects as though they were exactly once.

The callback should contain only short same-database work. It must not perform
remote calls, and unnecessarily long SQL holds the run lock and delays
other semantic mutations for that run. Normalized staged events,
children, waits, and initially ready queue rows are prepared completely and
persisted with bounded set-oriented operations inside the same settlement.

## 8. Retry, timeout, and fencing

The default command retry policy permits five consumed attempts with backoff steps of 1 second, 5 seconds, 30 seconds, and 2 minutes. Later attempts reuse the final step. Backoff applies deterministic 20% proportional jitter derived from the attempt identity and persisted policy.

Public policy construction supports:

- `Attempts(n)`: positive maximum consumed attempts;
- `RetryFor(d)`: positive maximum elapsed retry budget;
- `.Attempts(n)`: combine elapsed and attempt bounds; and
- `.Backoff(delays...)`: replace the positive backoff sequence.

Retry policy, timeout, and queue are copied into the durable command declaration and do not change when another runtime deploys different defaults. Retry policy uses one canonical whole-millisecond representation and is stored as opaque bytes rather than a SQL-queryable JSON document.

Worker conclusions are classified as follows:

| Conclusion | Consumes attempt budget | Retry behavior |
|---|---:|---|
| ordinary returned error | yes | policy backoff while bounds permit |
| `RetryAfter(delay, err)` | yes | requested positive delay while bounds permit |
| `Permanent(err)` | yes | no retry |
| panic | yes | policy backoff while bounds permit |
| attempt timeout/deadline | yes | policy backoff while bounds permit |
| cooperative shutdown interruption | no | eligible for retry |
| lease loss/interruption caused by fencing | no | eligible for retry |

The run deadline caps attempt run and future retry scheduling. A retry is not scheduled at or beyond the effective elapsed/run deadline.

Claims persist an attempt ID, lease token, owner, start time, and expiry. The runtime renews active leases. If renewal is lost or the lease expires, the local worker context is cancelled and maintenance makes the command eligible for safe takeover. Even if application code ignores cancellation and returns later, the old fence prevents settlement.

Worker invocation is therefore at-least-once; durable Flow settlement is fence-once. Remote calls, files, messages, and other effects outside the settlement transaction require application idempotency.

## 9. Lifecycle and failure

Run statuses are:

```text
running -> succeeded | failed | cancelled | expired
running -> failing -> failed | cancelled | expired
```

`failing` means a required command has reached an unsuccessful terminal state but active surviving work may still be settling.

Public run, command, queue, key-scope, and terminal-status fields use typed string constants. Unknown stored values fail explicit boundary decoding as `ErrInvalidState`.

Command states are:

```text
pending | ready | running | retry_wait
    -> succeeded | failed | cancelled | expired
```

An run succeeds when all commands are terminal and no required command failed, was cancelled, or expired. Application events alone do not keep it open; a predeclared gated command does. Optional work still contributes to liveness.

Required terminal failure enters failing state. With fail-fast enabled, Flow cancels commands without active attempts while preserving already running attempts and their valid fences. A survivor that succeeds after failing began may record its result/events, but newly staged children are recorded cancelled. With fail-fast disabled, already declared work continues, but the final run still fails.

Optional command failure/cancellation/expiry remains visible in history and trace but does not by itself fail the run. Externally gated optional commands should normally have a finite `Within` budget so they do not hold the run open forever.

A failed, cancelled, or expired command emits no success application event and stages no children. Flow provides no in-run reaction to unsuccessful command outcomes. Expected business alternatives should be represented as successful typed results/events; infrastructure failures remain failures.

Run-deadline expiry terminally expires the run and its open work. Terminal runs cannot be reopened.

## 10. Cancellation

Cancellation requires a non-empty, trimmed UTF-8 reason of at most 1024 bytes.

`CancelCommand(ctx, client, commandID, reason)`:

- operates under the owning run lock;
- concludes and fences any active attempt;
- removes delivery state for the command;
- records command cancellation in the journal;
- treats cancellation of a required command as required failure; and
- allows cancellation of an optional command without necessarily failing the run.

Cancelling a required command makes the run fail, not become run status `cancelled`. Normal fail-fast rules apply to its remaining work.

`CancelRun(ctx, client, runID, reason)` atomically concludes active attempts, cancels every open command, removes their queue rows, records command terminal events, and records the run as `cancelled`.

Repeating the same cancellation on the same already-cancelled resource with the identical reason is an idempotent no-op. A different reason, or cancellation of a resource terminal for another reason/status, returns `ErrTerminal`.

Cancellation through `runtime.InTx(tx)` remains uncommitted until the caller commits and is undone by rollback.

## 11. Caller-owned transactions

`runtime.InTx(tx)` returns a named `*TransactionClient` bound to a caller-owned
`pgx.Tx`.

- Flow never commits or rolls back the transaction.
- The application creates exactly one transaction client per `pgx.Tx`, does
  not use it concurrently, and does not use it after the transaction ends.
- Flow reads can observe Flow writes already made in that transaction.
- Other database sessions cannot observe them before commit.
- `AwaitRun` rejects transaction clients because an open transaction cannot observe later external commits reliably.
- The caller owns every returned error and must decide whether to roll back.

All Flow operations in one transaction must reuse the same transaction client.
Callers perform Flow operations first, then call
`BeginApplicationWrites`, then acquire application locks or issue application
writes. The first phase call succeeds without SQL; a duplicate returns
`ErrInvalidState`. Every later Flow write or run-locking operation through the
client returns `ErrInvalidState` before SQL. The guard cannot detect
application SQL issued before it, so the order remains an explicit caller
contract.

Semantic operations touching pre-existing runs must request run locks in
ascending `RunID` order. A run first inserted by the current transaction is
transaction-owned and introduces no cross-transaction lock edge; conflict
rediscovery still locks the pre-existing holder through the ordinary ordered
path.

The caller should commit or roll back promptly after the Flow operations and
associated application writes. Every acquired run lock remains held for
the lifetime of the caller-owned transaction.

Caller-owned starts register newly inserted runs as transaction-owned; keyed
rediscovery resolves the actual pre-existing holder before locking it.
Multi-run changes are not settled atomically by Flow itself; explicit caller
transactions are the only cross-run/application-write boundary.

### 11.1 Atomic live-key replacement

`command.ReplaceCurrentRun(ctx, client, expected, key, args, reason, options...)`
is available only for a non-empty `WithLiveKey` root. In one transaction it
locks the exact expected predecessor as the CAS serialization anchor, confirms
the current holder, cancels the predecessor with the ordinary journal and
projection path, and starts one distinct successor with the ordinary start
path.

- If current `RunID == expected`, replacement always occurs—even when the
  requested declaration is byte-for-byte equivalent to the predecessor.
- If current `RunID != expected`, declaration equivalence is consulted only
  for retry/ambiguous-commit rediscovery. An equivalent current successor is
  returned with `Replaced=false`; a different declaration returns
  `ErrConflict` without mutation.
- If no current holder exists, replacement returns `ErrConflict`; callers use
  `Enqueue` when create-if-absent is intended.
- Caller rollback restores the complete predecessor state and removes the
  successor. No committed state exposes a cancel/start gap or two live holders.

## 12. Runtime and deployment

`Migrate` is explicit. `New` validates runtime options and `CheckSchema`, then starts no goroutines. The runtime is immediately usable as an API client for start, ingress, cancellation, and inspection.

Runtime defaults are:

- schema `public`;
- command ceiling 1000 per run;
- global worker concurrency `max(1, GOMAXPROCS)`;
- fixed 60-second command lease;
- one-second correctness polling;
- PostgreSQL notification hints enabled;
- 30-second shutdown grace; and
- no-op observer.

Public options are:

- `WithSchema` for the validated PostgreSQL schema;
- `WithMaxCommandsPerRun` for the ceiling copied to new runs;
- `WithWorkerConcurrency` for the process-wide handler bound;
- `WithQueueConcurrency` for a smaller process-local named-queue lane;
- `WithPollInterval` for correctness polling and maintenance cadence;
- `WithNotifications` for transactional PostgreSQL wake hints;
- `WithShutdownGrace` for cooperative handler drain time; and
- `WithObserver` for bounded operational observations.

`Run(ctx)` may be called once. It freezes registration and starts:

- one bounded command scheduler;
- lease renewal;
- wait-expiry, run-deadline, and stale-lease maintenance;
- one reconnecting PostgreSQL notification listener when enabled; and
- asynchronous observer delivery.

Polling is the correctness path. Notifications are transactional latency hints; malformed/lost hints, listener disconnects, transaction-pooling proxies, and disabled notifications do not lose work.

Flow emits a wake hint only when a committed transition creates work that is
immediately runnable. Journal-only transitions, claims, terminal settlements
without follow-up work, unmatched events, and work scheduled for the future do
not require a hint.

The scheduler may claim selected groups from independent runs
concurrently using an internal bound derived from worker capacity and the
database pool while retaining capacity for maintenance. Candidates from one
run remain in one serialized claim transaction, where eligible attempts,
event inputs, journal entries, and command/queue projection changes are handled
as bounded sets. This policy adds no public concurrency setting.

Global and named-queue concurrency limits are process-local. PostgreSQL claims and fences coordinate all replicas. The named-queue limit shares the runtime's global capacity.

`Stop(ctx)` initiates shutdown and waits for `Run` to finish or for the supplied context to end. Run-context cancellation does the same. The scheduler stops claiming first, waits through the grace period, then interrupts remaining handlers. Shutdown interruption does not consume retry budget. The caller-owned database pool is never closed.

API-only publisher processes need not register workers or call `Run`.

## 13. Storage and migrations

Flow owns exactly six `flow_` tables in one validated PostgreSQL schema:

1. `flow_runs`;
2. `flow_commands`;
3. `flow_command_queue`;
4. `flow_command_event_waits`;
5. `flow_journal`; and
6. `flow_schema_migrations`.

Applications must not use these tables as a write API. Semantic journal entries and current projections must remain transactionally consistent.

Every accepted journal write is canonicalized and its hash verified before
persistence. Claim materialization verifies the retained body hash, decodes the
bounded application-event body once, and validates the canonical payload before
giving it to a worker. Full replay independently verifies hashes and
re-canonicalizes retained bodies for stronger history diagnostics.

Database constraints require a non-null root command and enforce same-run ownership for roots, parents, delivery rows, event waits, and journal command references. Durable position references are positive, and journal causation must point to an earlier position.

`Migrate` serializes migration application with an advisory transaction lock, verifies checksums of already applied migrations, and applies each pending embedded migration in its own transaction. `MigrationFS` exposes schema-rendered SQL for an external transactional runner. `CheckSchema` verifies checksums, reader/writer compatibility, current version, and the exact six-table inventory without mutation.

`New` fails with `ErrSchema` when the configured schema is absent, incomplete, modified, unknown, or incompatible.

The current release has no journal pruning or archival API. Arguments, results, event payloads, metadata, and journal bodies remain stored for the lifetime of the retained run. Operators must budget retention accordingly and use stable external references for large or sensitive application data.

## 14. Inspection and observability

`GetRun` returns one durable run snapshot by ID.

`GetCurrentRun(type, key)` returns the current live-key holder or `found=false`; terminal runs with the same key may still exist.

`ListRuns` provides indexed keyset pagination with optional command type, key prefix, statuses, creation-time range, and metadata containment. `CreatedAfter` is inclusive, `CreatedBefore` exclusive. Page size defaults to 50 and may be 1 through 200.

`ListLiveWork` returns queued or leased commands for at most 200 exact run
keys. `ListHistoryByKeys` returns retained journal entries for the same bounded
key set. Both use opaque, versioned, query-specific keyset cursors bound to the
normalized keys; pages default to 100 rows and may be 1 through 1000. Keys must
be non-empty valid UTF-8 no larger than 1024 bytes. Ordinary calls do not
promise a cross-page snapshot; transaction-scoped clients observe the caller's
transaction and its uncommitted writes.

`AwaitRun` polls without holding a worker, lease, or database connection between reads until the run is terminal or the context ends.

`GetQueueDepth(queue)` returns a point-in-time count of ready, delayed, and running delivery rows plus how long the oldest ready item has waited. It is operational state, not semantic history.

`History` returns immutable entries ordered by journal position. `HistoryAfter` is exclusive; `HistoryLimit` defaults to 100 and may be 1 through 1000.

`Trace` folds retained journal facts and overlays current operational
command/wait/lease data. A Flow-owned call uses Repeatable Read. A
transaction-scoped call inherits its caller's isolation; a caller requiring one
coherent cross-statement snapshot must supply Repeatable Read or Serializable.
Trace exposes run state, command provenance, attempts, results/failures,
waits and satisfying positions, events, and raw ordered history. The current
implementation rejects an initial trace at 100,000 or more retained history
entries rather than returning an unbounded snapshot.

While `Run` is active, observers receive bounded operational metadata asynchronously. Delivery and shutdown drain are best-effort: a full observer queue drops observations and later reports a drop count, and observer panics do not affect run. Observers must return promptly and should honor callback-context cancellation. A blocked observer may strand its one delivery goroutine, but cannot block runtime shutdown or durable work. Observations contain no arguments, results, event payloads, SQL, connection objects, or lease tokens.

## 15. Errors and data handling

Public Flow errors support `errors.Is` with these categories:

- `ErrNotFound`: requested run/command or referenced target does not exist;
- `ErrConflict`: durable identity exists with different content;
- `ErrInvalid`: malformed definitions, options, IDs, values, or bounds;
- `ErrInvalidState`: an operation violates current/transaction/decision state;
- `ErrTerminal`: a new mutation targets terminal work;
- `ErrLeaseLost`: an attempt no longer owns its fence;
- `ErrPayloadTooLarge`: a canonical value exceeds its documented limit;
- `ErrClosed`: a stopped runtime or closed transaction/client is used; and
- `ErrSchema`: migration/schema state is missing, changed, or incompatible.

`*flow.Error` adds bounded operation/resource/identifier/reason context and unwraps to its category. Durable and public failure projections use one structured `Failure{Code, Message}` value while preserving separate latest-command, terminal-command, and run-failure fields. Store errors and observations must not expose raw SQL, driver details, payloads, secrets, or lease tokens. Persisted worker error messages are trimmed and bounded, but applications must still avoid putting secrets into returned errors.

Flow canonicalizes durable typed values as JSON and stores them in PostgreSQL; it does not provide field encryption or automatic redaction. Applications should store secrets and large objects behind stable references and apply database-level access control, encryption, backup, and retention policy appropriate to those values.

## 16. Bounds

| Value | Bound |
|---|---:|
| canonical command arguments | 256 KiB |
| canonical command results | 256 KiB |
| canonical application-event payload | 64 KiB |
| canonical run metadata | 16 KiB |
| metadata key | 128 bytes |
| metadata value | 1024 bytes |
| command/event/queue definition name | 255 bytes |
| PostgreSQL schema name | 63 bytes and a simple SQL identifier |
| run key | 1024 bytes; empty allowed only for non-idempotent permanent starts |
| command/event key | 1024 bytes and non-empty |
| cancellation reason | 1024 bytes and non-empty |
| exact waits per command | 256 |
| commands per run | runtime-configurable, default 1000; zero disables |
| run listing page | default 50, maximum 200 |
| history page | default 100, maximum 1000 |
| by-key read batch | maximum 200 non-empty UTF-8 keys, each at most 1024 bytes |
| live-work/keyed-history page | default 100, maximum 1000 |
| opaque by-key cursor | maximum 4096 encoded bytes |
| initial trace history | fewer than 100,000 entries |

Bounds apply to canonical encoded values, not merely the apparent size of Go fields. A command using 256 maximum-size event inputs may materialize about 16 MiB of encoded event data before decoding overhead; larger joins should use a command tree or stable external references.

## 17. Deliberate unsupported behavior

Flow does not provide:

- first-of-N, OR, race, or quorum gates;
- callbacks or new work triggered by failed/cancelled/expired command outcomes;
- automatic access to arbitrary command results inside workers;
- mutable run/coordinator state separate from commands and application storage;
- event subscriptions, broadcasts, global topics, or target discovery;
- arbitrary external insertion of non-root commands;
- dynamic amendment of an accepted command declaration;
- exactly-once remote/non-transactional effects; or
- automatic history retention, pruning, or archival.

These are product boundaries, not hidden future behavior. Applications requiring them should model explicit successful business outcomes, add durable commands/references, or use a coordination system designed for that run model.
