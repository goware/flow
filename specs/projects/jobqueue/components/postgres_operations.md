---
status: complete
---

# Component: PostgreSQL Backend and Operations

## 1. Purpose and Scope

This component assembles the PostgreSQL implementation and owns behavior shared by MessageQueue, JobQueue, workflows, EventBus, and EventStore.

Responsibilities:

- backend construction, configuration, schema validation, and transaction binding;
- component constructors and shared SQL/time/ID/error helpers;
- transaction retries and commit-aware observations;
- PostgreSQL notifications, direct listener connections, local wake coalescing, and poll fallback;
- worker dispatch support, lease-renewal batching, and bounded maintenance loops;
- embedded checksummed migrations and compatibility checks;
- PostgreSQL error classification, operational safety, and integration-test infrastructure.

Non-responsibilities:

- redefining component state machines or table ownership;
- running application handlers inside database transactions;
- making `LISTEN`/`NOTIFY` a correctness dependency;
- automatically tuning the operator's PostgreSQL cluster.

## 2. Public Backend API

```go
package postgres

type Backend struct {
    // immutable configuration and shared internal services
}

func New(db *pgkit.DB, opts ...Option) (*Backend, error)
func (b *Backend) InTx(pgx.Tx) *Backend

func NewMessageQueue(*Backend, ...MessageQueueOption) (*MessageQueue, error)
func NewJobQueue(*Backend, ...JobQueueOption) (*JobQueue, error)
func NewWorkflowEngine(*Backend, ...WorkflowOption) (*WorkflowEngine, error)
func NewEventBus(*Backend, ...EventBusOption) (*EventBus, error)
func NewEventStore(*Backend, ...EventStoreOption) (*EventStore, error)

func WithAutomaticQueueCreation(jobqueue.QueueConfig) MessageQueueOption

type AtomicRequest struct {
    Messages  []jobqueue.PublishRequest
    Jobs      []jobqueue.EnqueueRequest
    BusEvents []jobqueue.PublishEventRequest
    Appends   []jobqueue.EventOperation
}

type AtomicResult struct {
    Messages  []jobqueue.PublishResult
    Jobs      []jobqueue.EnqueueResult
    BusEvents []jobqueue.PublishEventResult
    Appends   []jobqueue.AppendResult
}

func (b *Backend) ExecuteAtomic(context.Context, AtomicRequest) (AtomicResult, error)
```

Each concrete component implements its root capability interfaces. Constructors are cheap immutable views over a backend; they do not start goroutines. Repeated construction is allowed.

`ExecuteAtomic` is the composition surface when one call must mix raw publication, job enqueue, direct EventBus publication, and EventStore append/store-backed publication. It validates everything first, preserves result order within each slice, applies database work in global lock order, and rolls back all categories on one conflict. On a transaction-bound backend it participates without committing; otherwise it owns one transaction. Empty requests are invalid.

`New`:

1. rejects a nil database or invalid option combination;
2. validates and quotes the configured schema;
3. derives fixed notification and advisory-lock identities;
4. checks migration metadata and schema compatibility;
5. constructs local no-op/default services;
6. performs no migration and starts no background process.

If the schema is absent or outside the library's supported compatibility range, `New` returns a typed schema error with the required action.

## 3. Backend Configuration

```go
type ListenConnector func(context.Context) (*pgx.Conn, error)

type ErrorRedactor func(context.Context, jobqueue.JobError) jobqueue.JobError

func WithSchema(string) Option
func WithObserver(jobqueue.Observer) Option
func WithErrorRedactor(ErrorRedactor) Option
func WithListenConnector(ListenConnector) Option
func WithPollInterval(time.Duration) Option
func WithTransactionRetry(TransactionRetryConfig) Option
func WithQueryLabel(string) Option
func WithDefaultJobLane(jobqueue.QueueName) Option

type TransactionRetryConfig struct {
    MaxAttempts int
    MinBackoff  time.Duration
    MaxBackoff  time.Duration
}
```

Defaults:

| Setting | Default |
|---|---:|
| schema | `jobqueue` |
| observer | no-op |
| error redactor | length-bound shared default |
| notification connector | disabled |
| fallback poll interval | 1 second with 10% local jitter |
| backend-owned transaction attempts | 3 |
| retry backoff | 5–100 milliseconds, full jitter |

The query label is a validated low-cardinality value appended to `application_name`/observations where supported; it is never interpolated into SQL comments.

Test-only clocks, UUID sources, and fault injectors are unexported options in internal test packages. Public callers cannot substitute a Go clock for durable PostgreSQL scheduling decisions.

## 4. Schema Identifiers and SQL Construction

Schema names follow PostgreSQL identifier length and character rules but are accepted as data, not raw SQL. Construction parses one identifier only; dots, quotes, null bytes, and multi-part input are rejected. The implementation quotes the validated identifier once and renders every table reference fully qualified.

Runtime SQL uses constants with one schema placeholder expanded during backend construction. All values remain positional parameters. No operation changes `search_path` or depends on connection session state.

Prepared-statement names include a stable schema hash so multiple schemas can share a process without collision. The implementation works with pgx statement-cache modes appropriate for direct PostgreSQL and PgBouncer transaction pooling.

## 5. Transaction Modes

### 5.1 Backend-owned operations

Ordinary public methods start one short `READ COMMITTED` transaction when atomicity requires it. They commit/roll back internally and may retry only SQLSTATE `40001` or `40P01` when the whole operation is library-controlled and idempotent.

Validation, JSON canonicalization, handler execution, and other unbounded work happen before the transaction. Retried operations reuse stable generated IDs and canonical request bytes.

### 5.2 Worker completion-owned operations

Worker completion owns the transaction containing fence validation, finalizer, state transition, child/mutation materialization, event append/publication, history, and dispatch changes. On a retryable database abort, the runtime may replay this database-only completion using the already-buffered handler outcome. The handler itself is not rerun.

Finalizers may be invoked again only after their prior transaction rolled back. Their contract therefore requires all effects to use the supplied transaction and prohibits irreversible external effects.

### 5.3 Caller-owned operations

`b.InTx(tx)` returns a clone whose `pgkit.DB` is bound through `db.InTx(tx)`. Bound methods:

- never begin, commit, roll back, or automatically retry a transaction;
- use the caller's isolation level and context;
- issue `pg_notify` transactionally, so PostgreSQL delivers hints only after commit;
- preserve every component lock/fence rule.

Callers composing multiple categories should perform application updates first and call `ExecuteAtomic` near the end of the transaction. Calling independent component methods in arbitrary order remains transactionally correct, but can violate the library's deadlock-minimizing lock order; a PostgreSQL deadlock is then returned to the caller without automatic replay.

Because a bare `pgx.Tx` has no commit hook, the simple `InTx` form suppresses commit-dependent observer callbacks rather than emitting false success. Callers wanting full observations use `BindTx` or `Transact`.

### 5.4 Commit-aware composition

```go
type TxBinding struct {
    // bound backend and buffered observations
}

func (b *Backend) BindTx(pgx.Tx) *TxBinding
func (x *TxBinding) Backend() *Backend
func (x *TxBinding) Finish(error)

func (b *Backend) Transact(
    context.Context,
    pgx.TxOptions,
    func(context.Context, *Backend) error,
) error
```

`Transact` begins once, invokes the callback, commits on nil, rolls back otherwise, and flushes buffered observations only after the final database result. It does not automatically replay an application callback.

For an externally owned transaction, call `Finish` once after the actual commit/rollback function returns; nil means committed and non-nil means rolled back or commit-uncertain. `Finish` is idempotent and never affects the transaction. If omitted, observations are dropped. This design makes loss preferable to reporting an uncommitted mutation.

## 6. Transaction Retry Engine

Before retry behavior, the shared ordered executor applies an `AtomicRequest` as follows:

1. validate/canonicalize every category, generate stable UUIDv7 values, reject duplicate identities, and compute fingerprints before SQL;
2. load required raw queue and job-lane configuration and perform raw/job inserts in stable namespace/ID order;
3. recognize already-idempotent bus/store requests where possible and lock routing topic/subscription rows for genuinely new fan-out;
4. create/lock event streams lexically, verify expected versions and stable event identities, and prepare stored rows;
5. acquire the global allocator only when new stream events exist;
6. insert immutable stream events, direct/store-backed bus envelopes, subscription deliveries, and component history in stable order;
7. issue one compact transactional wake hint per affected owner, map results to request order, and let the transaction owner commit.

A late conflict rolls back earlier raw/job writes. The executor invokes no application callback. Worker completion uses the same phases after its finalizer and workflow preparation.

The internal retry engine:

1. attempts only a function explicitly classified as replay-safe;
2. rolls back and drains the transaction before another attempt;
3. retries deadlock detected (`40P01`) and serialization failure (`40001`);
4. does not retry statement timeout, lock timeout, cancellation, connection failure, constraint failure, or ambiguous commit;
5. uses bounded full-jitter backoff respecting context cancellation;
6. reports attempt count and final SQLSTATE after outcome is known.

An ambiguous commit is returned with an `UncertainCommit` detail. Stable request IDs let the caller repeat the operation and discover its durable outcome. The library never assumes a lost connection means rollback.

Lock acquisition follows the architecture order:

1. workflow run;
2. jobs in UUID order;
3. dispatch/attempt/node/dependency rows in stable order;
4. EventBus topic/subscription routing rows in topic/UUID order;
5. event streams in lexical order;
6. global event allocator;
7. immutable event/history/delivery inserts;
8. `pg_notify`, then commit.

## 7. Database Time and Local Time

Durable decisions use `clock_timestamp()` inside the deciding statement or a statement CTE. One transaction-wide instant is selected explicitly when multiple inserted rows must share a time.

Go time is used only for:

- local timers and jitter;
- long-poll and shutdown deadlines;
- notification reconnect backoff;
- observability duration measurement.

Code never compares a Go timestamp with a lease to decide ownership. Returned server timestamps are authoritative.

## 8. Notification Protocol

### 8.1 Channel

The channel is `jq_` plus the first 24 lowercase hexadecimal characters of SHA-256 over the normalized schema name, for example `jq_9c6e...`. This is safely below PostgreSQL's identifier limit and isolates schemas sharing a database.

Listener SQL quotes this derived fixed identifier. Publishers use:

```sql
SELECT pg_notify($1, $2);
```

The notification is inserted within the mutating transaction and becomes visible only on commit.

### 8.2 Payload

Payload is compact versioned JSON:

```json
{"v":1,"n":"job","k":"email"}
```

For work hints, `n` is one of `raw`, `job`, `subscription`, `workflow`, or `maintenance`, and `k` is a bounded validated queue/lane/subscription hint. Cancellation uses `job_cancel` with a job UUID or `workflow_cancel` with a workflow UUID. Payloads never contain message bodies, job arguments, event data, reasons, actors, errors, or credentials and remain far below PostgreSQL's payload limit.

Work hints route through `WakeHub`. Cancellation hints route to WorkerPool's active-execution registry and cooperatively cancel matching contexts. Loss remains safe: the next lease renewal/fence revalidation observes the durable cancellation and cancels the context, while stale completion is rejected.

Unknown versions/namespaces cause a process-wide catch-up wake, not failure. Invalid payloads are counted and ignored after that safe wake.

### 8.3 Correctness boundary

Notifications only reduce latency. Every waiter has a fallback poll and every durable eligibility decision is made in SQL. Lost, duplicated, coalesced, delayed, or malformed notifications cannot lose work or cause incorrect settlement.

Transactional `pg_notify` serializes notifying commits through PostgreSQL's global notification-queue lock, held through `fsync` (see the DBOS LISTEN/NOTIFY scalability references in the architecture document). Per-owner hint coalescing bounds the exposure, and the architecture's benchmark plan measures this commit ceiling directly. Because the correctness boundary above never depends on notification delivery, a future decoupled hint flusher — buffering hints locally and emitting them in small separate transactions — is a safe, benchmark-gated evolution of this protocol.

## 9. Listener Lifecycle

The optional notifier owns one session-persistent direct `pgx.Conn`; it never borrows a transaction-pooled query connection for `LISTEN`.

On each connection:

1. connect with context and identify the application;
2. execute quoted `LISTEN` and commit it;
3. publish a local catch-up wake for every registered owner and ask active lease managers to revalidate fences;
4. wait for notifications or a health deadline;
5. decode and forward hints to `WakeHub`;
6. on error, close the connection, report it, and reconnect with capped jitter.

The catch-up wake after `LISTEN` closes the startup/reconnect race: work committed before the listener became active is found by the resulting SQL poll. While disconnected, normal fallback polls continue.

`ListenConnector` is appropriate when queries go through PgBouncer transaction mode but a direct PostgreSQL endpoint is available. With no connector, all components operate in poll-only mode without warning spam; backend inspection reports the selected mode.

## 10. WakeHub

`WakeHub` is an internal in-memory hint router keyed by namespace and owner ID. It stores no durable work.

Each key maintains a monotonically increasing local generation and subscribers receive through a capacity-one channel. Publishing increments the generation and performs a nonblocking send. A waiter records generation, performs its SQL recheck, then sleeps only if generation is unchanged. This prevents a local wake between check and wait from being lost.

Duplicate hints coalesce. Registration/unregistration is concurrency-safe and removes abandoned waiters. A process-wide wake increments every registered key. Closing the backend/runtime wakes all waiters with shutdown state.

## 11. Capacity-Aware Dispatch Support

WorkerPool owns dispatch goroutines; the backend provides shared wait/claim primitives.

For each bound lane:

- a local weighted scheduler asks only for currently free slots;
- per-lane and total semaphores enforce configured local concurrency;
- a claim batch never exceeds free capacity or the lane batch cap;
- minimally buffered handoff prevents large locally leased queues;
- completion releases capacity before publishing the next local wake;
- empty claims schedule the earlier of next-visible time and fallback poll.

Database claim filters include the process's registered kinds. Capability rows describe current registrations for diagnostics but are not the correctness source for an individual claim.

Fairness is best-effort weighted round-robin among locally ready lanes. PostgreSQL priority remains best-effort because `SKIP LOCKED`, concurrent claimers, and transaction scheduling can reorder equal/nearby candidates.

## 12. LeaseManager

One internal LeaseManager per WorkerPool tracks active job leases and their handler cancellation functions. It groups renewal by lane and target duration and issues bounded batch updates.

For each receipt:

1. schedule renewal before expiry using the configured lead;
2. extend with database time and the current fence;
3. replace the tracked expiry on success;
4. cancel only that handler context and mark lease lost on zero-row/stale result;
5. use bounded retry for a transient query error only while enough lease time plausibly remains;
6. never claim ownership after known expiry.

Base raw MessageQueue and EventBus receive APIs return leases to callers and do not silently renew them. A later reusable consumer runner may register raw/event receipts with the same internal batching machinery; until then callers explicitly call their extension APIs.

Shutdown stops accepting new registrations, attempts a final renewal only when it helps the grace window, and then waits for completion or interrupts according to WorkerPool policy.

## 13. Maintenance Runtime

```go
type MaintenanceConfig struct {
    PollInterval time.Duration
    BatchSize    int
    Concurrency  int

    RawMessages       bool
    Jobs              bool
    Workflows         bool
    EventBus          bool
    Capabilities      bool
}

type Maintenance struct { /* private */ }

func NewMaintenance(*Backend, MaintenanceConfig) (*Maintenance, error)
func (m *Maintenance) Run(context.Context) error
func (m *Maintenance) Stop(context.Context) error
```

Defaults enable all tasks represented by the installed schema, use a 30-second poll, batch 500, and concurrency 2. WorkerPool may run a lightweight job reconciler/capability cleanup itself; duplicate maintenance runners remain safe.

Tasks include:

- raw max-delivery movement, active retention, and dead cleanup;
- job start-deadline expiry, missing/stale dispatch reconciliation, and eligible standalone terminal-history cleanup;
- workflow deadline expiry;
- subscription max-delivery movement, delivery/dead retention, and bus-event cleanup;
- expired worker registration/capability deletion.

Each task uses a bounded transaction and `FOR UPDATE SKIP LOCKED`. There is no elected leader. A failed batch rolls back and is retried by this or another runner. Work is re-signalled while a full batch is found so large backlogs drain without waiting a full poll.

EventStore has no deletion maintenance in the initial release.

## 14. Migration API

```go
type MigrationOptions struct {
    Schema string
}

type SchemaStatus struct {
    Schema          string
    CurrentVersion  int
    LatestVersion   int
    Compatible      bool
    LibraryVersion  string
}

func MigrationFS(MigrationOptions) (fs.FS, error)
func Migrate(context.Context, *pgkit.DB, MigrationOptions) (SchemaStatus, error)
func CheckSchema(context.Context, *pgkit.DB, MigrationOptions) (SchemaStatus, error)
```

`MigrationFS` returns a read-only rendered filesystem whose SQL has the validated, quoted schema substituted. This supports applications which run the same migrations through their own tooling. Filenames and source order are stable.

Initial migration units are:

1. migration metadata and grants prerequisites;
2. raw queue tables;
3. jobs, attempts, dispatch, and worker capabilities;
4. workflow tables and job/workflow foreign keys;
5. EventStore streams, events, allocator, and checkpoints;
6. EventBus topics, subscriptions, events, deliveries, replay, and store-event foreign key;
7. indexes/constraints requiring all components and compatibility metadata.

Release implementation may split a unit further, but applied numeric identifiers are never reordered or reused.

## 15. Migration Algorithm

The target schema contains:

```sql
CREATE TABLE jobqueue.schema_migrations (
    version integer PRIMARY KEY,
    name text NOT NULL,
    checksum bytea NOT NULL CHECK (octet_length(checksum) = 32),
    library_version text NOT NULL,
    applied_at timestamptz NOT NULL
);

CREATE TABLE jobqueue.schema_compatibility (
    singleton boolean PRIMARY KEY DEFAULT true CHECK (singleton),
    current_version integer NOT NULL,
    min_reader_version integer NOT NULL,
    min_writer_version integer NOT NULL,
    updated_at timestamptz NOT NULL
);
```

For each pending migration, `Migrate`:

1. starts a transaction;
2. acquires `pg_advisory_xact_lock` using two fixed 32-bit keys derived from database identity and schema hash;
3. creates/loads metadata and rechecks the next required version while locked;
4. compares SHA-256 checksums of every already-applied rendered migration;
5. executes exactly one pending transactional migration;
6. records its version/name/checksum/library version and updates compatibility;
7. commits and repeats.

Releasing between units permits each documented migration to be an independent atomic upgrade. A second migrator can interleave only at a unit boundary, rechecks state under the same lock, and never applies a unit twice.

Checksum mismatch, unknown future version, or incompatible reader/writer range stops without mutation. PostgreSQL DDL transactional rollback prevents dirty partial units. A future non-transactional migration would require an explicit separate protocol and cannot be silently introduced into this engine.

## 16. Rolling Compatibility

Startup compares the installed compatibility row with the library reader/writer version. A process may run only when it can read every stored envelope/version and its writes are accepted by all supported concurrent versions.

Schema changes follow expand/migrate/contract:

- add nullable/defaulted fields and dual-readable envelope versions;
- deploy code capable of both representations;
- backfill in bounded resumable batches where needed;
- switch writers;
- remove old representation only in a later release whose compatibility floor excludes old binaries.

Unknown job-kind rolling safety comes from registered-kind claim filters, not migrations. Envelope decoders accept the current and immediately preceding supported versions unless a release note states a wider window.

## 17. Error Classification

The shared mapper uses `errors.Is`, `errors.As`, pgx error types, SQLSTATE, named constraints, and operation context.

| Source | Classification |
|---|---|
| context cancelled/deadline | preserve context error with operation detail |
| `23505` known idempotency constraint | compare stored fingerprint, then success or `ErrConflict` |
| `23503` known ownership reference | `ErrNotFound` or `ErrConflict` by operation |
| `23514` known check | `ErrInvalid` plus safe field detail |
| zero fenced update rows | diagnostic lookup, then `ErrLeaseLost`, `ErrRemoved`, or `ErrNotFound` |
| `40001`, `40P01` | internal retry when authorized, otherwise typed transient error |
| `55P03` lock-not-available | typed transient/operational error; no blind retry by default |
| connection loss during commit | uncertain-commit detail |
| schema version/checksum mismatch | `ErrSchema` with version/checksum detail |
| unmapped database failure | wrapped backend error with SQLSTATE, no SQL/data |

No mapper relies on localized PostgreSQL message text. Constraint names are stable migration API and cannot be renamed casually.

## 18. Observer Delivery

Observers receive immutable root observation values outside SQL transactions, application finalizers, and internal mutexes.

Backend-owned and worker-owned operations buffer events and flush after known commit/rollback. `Transact` and `TxBinding` provide the same behavior for composed calls. Bare `InTx` suppresses commit-dependent observations as documented.

Observer panics are recovered and reported through a final safe diagnostic hook; they never change operation results. Slow observers are the adapter's responsibility, but the library measures callback duration and supports a bounded asynchronous adapter. Payloads, arguments, event data, SQL text, lease tokens, and raw errors are excluded.

## 19. Resource and Deployment Guidance

- A worker process needs ordinary pool capacity for claims, renewals, completion, maintenance, and application finalizers; handlers do not hold connections.
- Reserve at least `2 + maintenance concurrency` pool connections above the application's own concurrent transactional demand. Actual sizing is benchmark-driven.
- One optional direct connection per process is used for `LISTEN`; it is outside a transaction-pool path.
- Set PostgreSQL `statement_timeout`/`lock_timeout` through deployment policy or per-transaction local settings, not persistent session mutation.
- Hot tables need aggressive autovacuum based on update/delete rate. Initial migration storage parameters are conservative and operator-overridable.
- Monitor dead tuples, WAL rate, transaction age, allocator wait, oldest visible work, and reconciliation repairs.
- Use separate database roles for migration and runtime. Runtime does not need DDL or allocator insert/delete privilege.
- TLS, credentials, network policy, backups, PITR, and HA belong to the application/operator environment.

## 20. Test Infrastructure

The public `jobqueuetest` package supplies backend-neutral conformance suites. PostgreSQL tests additionally create one unique schema per test run using a privileged test harness and drop only that resolved schema during cleanup.

Supported PostgreSQL majors run in CI. Tests use real PostgreSQL for row locks, SKIP LOCKED, transaction visibility, notifications, SQLSTATE, and migrations; mocks are limited to observer/listener local behavior.

Named fault points exist around:

- row claim/update and result decoding;
- handler return and completion begin;
- finalizer, workflow mutation, event allocation, and history insert;
- `pg_notify`, commit send, and commit response;
- listener connect/LISTEN/reconnect;
- each maintenance movement;
- each migration unit.

Fault injection is unavailable in production builds or requires an unexported test dependency.

## 21. Test Plan

### 21.1 Backend and transactions

- option/default/schema validation and full qualification;
- startup absent/old/new/incompatible schema behavior;
- backend-owned retry only for authorized SQLSTATEs;
- caller-bound methods never commit/retry/nest;
- `Transact`/`TxBinding` observer result matches actual commit;
- ambiguous commit is not mislabeled as rollback.

### 21.2 Notifications and polling

- notification after commit and none after rollback;
- startup/reconnect catch-up closes races;
- duplicate/malformed/unknown-version hints remain safe;
- local generation prevents check-to-sleep lost wake;
- listener outage and PgBouncer poll-only mode continue processing;
- reconnect backoff and shutdown leak no goroutines/connections.

### 21.3 Dispatch, leases, and maintenance

- claims never exceed free local or configured lane capacity;
- weighted scheduler avoids starvation in sustained multi-lane load;
- renewal batching, isolated lease loss, and shutdown races under `-race`;
- multiple maintenance processes divide work safely;
- full batches self-wake and partial batches return to interval polling;
- maintenance crash at each move leaves source or destination, never neither/both invalidly.

### 21.4 Migrations

- rendered default and custom schema installs;
- repeated and concurrent migrators;
- checksum mismatch and unknown future version;
- rollback of failing transactional unit;
- upgrade from every supported released fixture;
- rolling old/new binaries through expand/migrate/contract fixtures;
- runtime role cannot mutate migration or allocator structure.

### 21.5 Error and operational behavior

- every named constraint maps to the specified public error;
- error and observation output excludes secrets/payloads/tokens;
- observer panic/latency cannot affect durable result;
- PgBouncer-compatible statement modes;
- representative workload plans and pool/allocator contention benchmarks;
- goroutine, timer, listener, and connection leak tests.

## 22. Acceptance Conditions

This component is complete when:

1. one validated backend consistently implements all component constructors and transaction modes;
2. direct notification lowers latency while poll-only operation retains correctness;
3. wake coalescing, dispatch capacity, lease renewal, and shutdown are race-safe;
4. all cleanup/reconciliation tasks are bounded and safe with multiple runners;
5. migrations are embedded, rendered for custom schemas, checksummed, concurrent-safe, and rolling-compatible;
6. error mapping depends on codes/constraints rather than message strings;
7. commit-dependent observations are never emitted as committed before outcome is known;
8. real-PostgreSQL conformance, concurrency, fault, migration, and operational tests pass.
