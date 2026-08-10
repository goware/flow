---
status: complete
completed_at: 2026-08-05
---

# Architecture: flow

## 1. Objective

Flow is a typed Go API over a PostgreSQL-backed durable command engine. Commands are the only execution drivers. Workers record bounded deterministic decisions, the store accepts those decisions atomically, and one command scheduler per runtime delivers eligible work across replicas.

The architecture is optimized for five properties:

1. one small orchestration model that remains understandable from application code;
2. atomic progress with application tables in the same PostgreSQL database;
3. crash/retry safety through durable identity and attempt fencing;
4. efficient claims and inspection without replaying history on every operation; and
5. a semantic journal sufficient for diagnostics and deterministic replay checks.

## 2. Architectural decisions

### 2.1 Commands are the only durable execution unit

Earlier designs used plans, graphs, and coordinators in addition to commands. That created multiple schedulers and overlapping lifecycle models: command delivery, graph reconciliation, coordinator inboxes, and outcome subscriptions all needed their own durability and recovery rules.

The current design uses one command tree for ownership/provenance and exact application events for synchronization. A worker may create later commands only as part of its successful decision. This keeps every executable action on the same queue, lease, retry, cancellation, and inspection path.

A command is therefore a retry, side-effect, isolation, timeout, queue, or
parallelism boundary—not a wrapper around every deterministic line of business
logic. Small transformations stay within one worker. Independent bulk items use
separate executions, while very large fan-outs use bounded batch commands and
hierarchical joins rather than one oversized aggregate.

The accepted tradeoff is deliberate. The engine cannot react to arbitrary unsuccessful predecessor outcomes, choose the first of several events, compute quorum/race gates, or mutate open-ended workflow state. Those behaviors would require another durable state machine rather than a small extension to command gating.

### 2.2 Events synchronize; they do not execute code

Application events are immutable execution-local facts. They may satisfy commands declared in advance, but they never invoke callbacks or create commands by themselves. The waiting command is the durable representation of the future reaction.

This separates two concerns:

- the command tree answers “who owns this work and where did it come from?”; and
- exact events answer “which independently produced facts must exist before it runs?”

### 2.3 Journal and projections have different jobs

The journal records semantic history. Mutable projections provide current execution/command state, reverse wait lookup, and a hot delivery queue. Both are updated under one execution lock and commit together.

Flow does not replay the full journal on every claim. It also does not treat mutable queue/lease churn as semantic history. Attempt boundaries are journaled, while probe order, notification hints, and lease renewals remain operational projection state.

### 2.4 PostgreSQL is the coordination authority

There is no sidecar broker, distributed lock service, or in-memory leader. PostgreSQL uniqueness, row locks, transactions, `SKIP LOCKED`, durable timestamps, and constraints define accepted state. Process-local channels and `LISTEN/NOTIFY` only reduce latency.

### 2.5 Durable representation boundaries are explicit

Public versions and counters remain Go `int`, but every value and computed transition is checked against PostgreSQL's signed `integer` range before SQL. Durable scheduling configuration has exact whole-millisecond precision: fractional milliseconds are rejected at public and store boundaries, and conversions from stored milliseconds are range-checked before timestamp arithmetic.

Finite public states use typed string vocabularies with exhaustive boundary conversion. PostgreSQL stores the same vocabularies as `text` guarded by named `CHECK` constraints. Retry policies are opaque canonical bytes (`bytea`), because SQL never queries their fields. One shared structured failure value supplies execution, terminal-command, and latest-operational failure projections without collapsing those distinct lifecycle meanings.

## 3. Package and responsibility boundaries

```text
flow/
├── definitions.go          typed command/event definitions and defaults
├── execute.go              starts, external event ingress, cancellation
├── worker.go / node.go     worker decisions, GetEventValue, child builder
├── runtime.go              configuration, clients, transaction ownership
├── runtime_run.go          scheduler services, leases, notifications, shutdown
├── command_runtime.go      claim invocation, normalization, settlement routing
├── inspection.go           execution/list/await/queue queries
├── history.go / trace.go   journal access and reconstructed diagnostics
├── migrations.go           embedded migration and compatibility API
├── flowtest/               database-free decision/retry/replay test helpers
└── internal/
    ├── canonical/          bounded canonical JSON and hashes
    ├── definition/         erased codecs and stable definition validation
    ├── durable/            PostgreSQL integer and exact-duration boundaries
    ├── failure/            shared failure value plus Permanent/RetryAfter wrappers
    ├── replay/             pure semantic journal fold
    ├── retry/              persisted policy and deterministic decisions
    ├── pgschema/           validated schema/table rendering
    └── store/              SQL transitions, locks, journal, projections
```

| Layer | Owns | Must not own |
|---|---|---|
| typed API | generic safety, public validation, options, safe errors | SQL transitions or scheduler state |
| decision engine | first-defect recording, staged events/children, normalization | database connections or durable mutation |
| runtime | registration, capacity, invocation, cancellation contexts, service lifecycle | semantic truth independent of PostgreSQL |
| store | lock order, database time, transitions, constraints, journal/projections | application callbacks except fenced commit function |
| replay | pure interpretation of retained journal entries | SQL, workers, clocks, or side effects |
| `flowtest` | reuse of production codecs/decision/retry logic | imitation of PostgreSQL concurrency guarantees |

## 4. Execution aggregate and invariants

One execution is the transaction and locking aggregate for all of its commands and events.

Semantic mutations within an execution are intentionally serialized. This
makes causation, gap-free journal allocation, and fenced settlement auditable,
but it also means an execution is not a tenant-wide or global work container.
Independent items or shards scale through separate executions. The default
1,000-command ceiling remains a safety limit rather than a target; guidance to
keep ordinary executions in the tens or low hundreds is not a new hard bound.

Core invariants are:

- every execution has exactly one root command;
- every root, parent, queue, wait, and journal command reference belongs to the same execution;
- command keys are unique within the execution;
- application-event identity is unique by execution/name/key;
- semantic mutations acquire the execution row before dependent rows;
- journal positions are positive, consecutive, and commit-ordered within the execution, and every causation position is positive and points backward;
- journal entries and semantic projections commit together;
- one active attempt/fence may own a running command;
- only the owning attempt can settle;
- terminal commands/executions cannot be reopened; and
- execution counters equal the materialized command lifecycle.

Commands form a tree but readiness is not necessarily tree-shaped. A child can wait for events emitted by siblings or other branches. This does not change ownership: the command still has one parent, while each wait points to an immutable journal fact.

## 5. Six-table storage model

### 5.1 `flow_executions`

This is the aggregate head and first lock for semantic mutation. It stores root definition identity, permanent/live key scope, canonical start identity, accepted input, status, fail-fast, deadline, command/open counters, indexed metadata plus its exact canonical identity bytes, the next journal position, the non-null same-execution root command ID, and terminal failure.

Keeping the journal allocator and counters on the locked aggregate row makes per-execution ordering simple: no independent sequence can advance without the same semantic lock.

Permanent-key uniqueness retains one non-empty `(definition name, execution key)` forever. Live-key uniqueness is partial over `running`/`failing`, releasing the identity at terminal settlement.

### 5.2 `flow_commands`

This is the semantic command projection. It stores immutable declaration/provenance, canonical arguments and fingerprint, required/optional classification, opaque canonical retry bytes, exact-millisecond timeout/wait/delay settings, current state, attempt counters, result/failures, terminal journal position, and timestamps.

The projection avoids replay for common inspection and transition validation. Declaration fields are still represented in `command_created` history so replay can independently reconstruct the semantic command model.

### 5.3 `flow_command_queue`

This is the hot delivery projection for `ready`, `retry_wait`, and `running` commands. It contains only claim dimensions, next-run time, and active attempt/lease fields.

Separating it from `flow_commands` keeps frequent probes, claims, renewals, and recovery updates away from the broader semantic row and its indexes. A command has at most one queue row; terminal/pending commands have none.

### 5.4 `flow_command_event_waits`

This is the reverse readiness index keyed by command and exact event selector. An unresolved row has no satisfying position; resolution records the immutable application-event journal position.

The reverse index lets event ingress find affected commands without scanning all declarations. The satisfying position makes claim materialization and trace explain exactly which fact supplied the worker input.

### 5.5 `flow_journal`

This is immutable ordered semantic history. Entry kinds are:

- `execution_started`;
- `execution_failing`;
- `command_created`;
- `attempt_started`;
- `attempt_concluded`; and
- `event_recorded`.

Recorded event classes distinguish application events, command terminal events, and execution terminal events. Bodies are canonical bytes with hashes. Causation positions point only backward.

Application-event bodies live directly in the journal. A separate event-payload table would duplicate immutable identity/body storage, while a delivery table would introduce source semantics that targeted ingress does not promise.

### 5.6 `flow_schema_migrations`

This is the checksummed migration and reader/writer compatibility ledger. It allows the library to reject missing, locally modified, incomplete, unknown, or incompatible schemas before starting a runtime.

## 6. Semantic transaction protocol

Standalone Flow writes use `READ COMMITTED`. A semantic mutation follows this protocol:

1. begin or attach to a PostgreSQL transaction;
2. acquire `flow_executions ... FOR UPDATE` for the target execution;
3. capture `clock_timestamp()` after acquiring the lock;
4. validate aggregate, command, fence, identity, and bounds;
5. build one deterministically ordered semantic journal batch;
6. reserve exactly that many positions by updating the aggregate allocator;
7. append the immutable batch;
8. update projections, counters, readiness, and queue state;
9. execute an optional fenced application commit callback;
10. issue an optional transactional notification hint only if the transition
    created immediately runnable work; and
11. commit or roll back the whole unit.

Database time is captured after lock acquisition so transitions serialized on one execution also have a consistent decision time. Journal reservation and append occur in the same transaction, so rollback creates no visible gaps.

Execution-first locking is the global deadlock discipline. A caller-owned transaction that touches several existing executions must reuse one `InTx` client and request them in ascending execution-ID order before application-table writes. Flow tracks this order on the transaction client and rejects a reverse request before issuing its next semantic lock.

## 7. Start path

```text
typed Execute
  -> validate/canonicalize args and options
  -> compute start and root-declaration fingerprints
  -> INSERT execution (or find unique-key holder)
      -> permanent: compare complete start identity
      -> live: rediscover current holder without comparison
  -> append execution_started + command_created
  -> insert root command, waits, and queue row if ready
  -> commit
  -> Execution snapshot (Created marks a new execution)
```

The insert establishes ownership of a new execution row; the store adopts it as the semantic lock rather than selecting it again. A conflicting permanent insert loads the existing row under lock and compares canonical start identity. A live-key conflict loads only the current live holder; if it settles during the race, the start performs one bounded retry.

Initial event waits check already retained application events while the root is created. A root with unresolved waits becomes `pending`; otherwise it receives a delivery row whose time reflects its initial delay.

## 8. Claim and invocation path

```text
poll or notification wake
  -> calculate process/queue capacity
  -> probe queue for registered exact name/version work
  -> SKIP LOCKED claim under execution-first semantic transaction
  -> append attempt_started and install attempt/lease fence
  -> materialize args + all exact event input snapshots
  -> commit and release PostgreSQL resources
  -> invoke typed worker with timeout/cancellation context
```

The scheduler never claims work it cannot handle. Queue probes are bounded and may be repeated safely. `SKIP LOCKED` allows replicas to make progress independently when another execution is busy.

Selected candidates are grouped by execution. A pool-aware internal bound lets
independent execution groups claim concurrently while leaving database capacity
for lease and deadline maintenance. Candidates from one execution remain in
one transaction: the store locks the eligible set, loads all of its event inputs
in one query, appends one stable `attempt_started` batch, and updates queue and
command projections in sets. The scheduler gathers the selected claims before
probing again, preserving centralized capacity and fairness accounting.

Claim materialization validates that all waits are satisfied, joins their recorded positions to the exact application-event journal bodies, and enforces the 256-input bound. The connection is released before application argument/event codec decoding and worker invocation proceed.

The claim hot path hashes each retained event body directly, compares its
stored digest, performs one typed envelope decode, and validates the nested
canonical payload. It relies on the accepted write boundary having already
canonicalized the complete body; replay retains stronger independent
reconstruction for diagnostics.

The attempt context combines the configured attempt timeout, retry elapsed limit, execution deadline, runtime shutdown, and lease-fence cancellation. Panics are recovered at the invocation boundary.

## 9. Worker decision and success settlement

The decision engine is memory-only. It records the first misuse/conflict, staged events keyed by name/key, staged commands keyed by command key, and their stable insertion identities. Public staging calls may return an error or an ephemeral `Node`; either path records a defect so ignoring a returned error cannot accidentally commit a partial decision.

Before settlement the runtime:

1. prefers a context/fence failure over the returned result;
2. classifies a panic, worker error, or decision defect;
3. canonicalizes the typed result within its bound;
4. sorts and validates the staged change set; and
5. converts it to store-level event/command declarations with fingerprints.

Successful settlement reacquires the execution lock and verifies command ID, attempt ID, lease token, and current state. It then prepares journal/projection changes. `WithCommit` runs on this same transaction after the Flow changes are prepared and before commit.

Normalization produces one bounded deterministic change set. Existing staged
event identities and retained event positions for new waits are loaded in sets;
command, wait, and initially ready queue projections are inserted in batches.
This keeps round trips bounded by store operation rather than by child or wait
count while retaining the original atomic fault boundaries.

If `WithCommit` fails, PostgreSQL rolls back all proposed success changes. The runtime then concludes the still-owned attempt through the ordinary retry/failure path. This gives application-table writes atomicity with accepted command success without claiming exactly-once execution of the callback body.

Ambiguous settlement errors are resolved by querying attempt ownership. If the attempt is already concluded, the runtime does not settle it again; if ownership was lost, the stale worker stops; otherwise bounded settlement retries are safe under the same fence.

## 10. Event ingress and wait resolution

All three event APIs converge on one target-side storage transition:

```text
canonical target event
  -> equivalent/conflicting identity lookup
  -> lock target execution
  -> reject new event if terminal
  -> append application event at next journal position
  -> mark matching unresolved wait rows with that position
  -> move fully satisfied commands to ready/queue state
  -> commit
```

`flow.Emit(work, ...)` enters this logic from success settlement and therefore shares the worker fence transaction. `Event.Emit` enters it as external ingress but rejects an active attempt context. `Event.Deliver` uses the external path without that guard, making it deliberately independent from source settlement.

Cross-execution delivery is target-local. It adds no source ID, cross-journal causation edge, outbox row, acknowledgement, or multi-execution settlement. When producer atomicity matters, application code uses `runtime.InTx(tx)` to commit its own write and target event together.

On command creation, retained events can satisfy waits immediately. On later ingress, the reverse wait index finds only unresolved matching selectors. On expiry, the store checks for any event committed at or before the deadline before terminally expiring the command, so maintenance delay cannot overturn a timely fact.

Later ingress is delta-based. Newly satisfied reverse-wait rows are grouped by
command, `unsatisfied_waits` is decremented exactly by those rows, and only
commands reaching zero are transitioned and queued. The operation reports
whether any released command is runnable at database time so notification is
limited to useful immediate wakes.

## 11. Retry and failure transitions

Retry policies are canonicalized into every command declaration as opaque bytes with whole-millisecond elapsed/backoff fields. Decisions use PostgreSQL time, persisted budget start, consumed attempts, immutable policy, attempt identity, error classification, and execution deadline. Jitter is deterministic and rounded to a durable whole millisecond, so failover replicas calculate the same next time.

Ordinary errors, requested delays, panics, and timeouts consume budget. Shutdown interruption and lease loss do not. Permanent errors terminate immediately. Attempt and elapsed bounds plus the execution deadline cap every retry.

When the first required command becomes terminal unsuccessfully, the store records its terminal event and `execution_failing`. Reduced fail-fast cancels open commands without active attempts; running attempts remain fenced survivors. Completion waits for those survivors because accepting a valid in-flight settlement is safer than revoking an already executing effect.

If a survivor succeeds after failure began, its result and already staged events remain valid. Children newly introduced by that settlement are materialized as cancelled rather than extending a failed execution. When no open commands remain, the execution records one terminal event and projection state.

Optional unsuccessful commands use the same attempt/history machinery but do not initiate execution failure. Readiness and open-command counters determine whether the remaining execution can succeed.

## 12. Cancellation and expiry

Command and execution cancellation use the same execution-first semantic transaction protocol.

Command cancellation locks the command, concludes an active attempt if present, records terminal cancellation, deletes its queue row, resolves downstream liveness, and applies required/optional failure rules. Repeating the same reason is idempotent; a different terminal mutation is rejected.

Execution cancellation locks all open commands in stable order, concludes active attempts, records command cancellations followed by one execution-cancelled event, deletes delivery rows, and terminally updates the aggregate in one transaction.

Maintenance probes bounded indexes for expired execution deadlines, expired wait budgets, and expired attempt leases. Probes do not decide state. Each candidate is revalidated under its execution lock, so duplicate maintenance across replicas is harmless. A full page that commits progress requests a bounded prompt follow-up; every pass visits all categories, and a locked/no-progress page falls back to the ordinary poll interval instead of spinning.

## 13. Runtime concurrency and scaling

Each runtime has one scheduler and process-local capacity accounting:

- a global worker semaphore bounds all handlers;
- optional named-queue limits reserve smaller lanes within that bound; and
- exact registered name/version pairs restrict claimable work.

There is no global worker-count table. PostgreSQL row locks, queue state, and fences coordinate replicas. Adding replicas increases competing claimers; each successful claim still belongs to exactly one active fence.

Lease renewals run as one bounded set-oriented statement for locally active attempts. Exact running fences are selected `FOR UPDATE SKIP LOCKED`, so one row held by settlement cannot block unrelated renewals. Each request is classified as renewed, definitely lost, or uncertain. Definitely lost fences cancel the matching local context immediately; an uncertain locked row is neither extended nor immediately cancelled and retains its prior conservative local deadline. A separate runtime watchdog continues checking those deadlines even while renewal SQL or pool acquisition is blocked. Maintenance later recovers expired durable queue rows.

Notifications use one separately established session-capable PostgreSQL connection because pool/transaction connections cannot reliably own `LISTEN`. The listener reconnects with bounded backoff and performs a broad wake after every connection to close commit-before-LISTEN gaps. Every scheduler continues polling regardless.

The store emits at most one transactional wake for an operation that creates
immediately runnable work. Claims, journal-only transitions, unmatched events,
terminal settlement without follow-up work, and future-scheduled work do not
notify.

## 14. Runtime lifecycle and deployment

`New` validates schema/configuration and allocates no background services. This permits API-only clients and makes startup ordering explicit.

`Run` is single-use. It freezes an immutable worker registry, starts observation delivery and maintenance services, and runs the command scheduler until its context or `Stop` requests shutdown.

Shutdown occurs in phases:

1. stop accepting new claims;
2. allow active workers to finish through the configured grace period;
3. cancel remaining worker contexts as budget-neutral interruption;
4. stop lease, maintenance, and notification services;
5. close observer delivery; and
6. mark the runtime closed.

The application's `pgkit.DB` and pool remain caller-owned and are never closed by Flow.

Supported deployment shapes include one all-worker binary, independently scaled queue/command pools, rolling old/new command versions, and publisher-only processes. Unknown durable versions remain idle rather than being handled by an incompatible worker.

## 15. Inspection, history, and replay

Point/list/queue queries read indexed projections. History reads journal positions directly. Await polls the execution projection without reserving a connection between polls.

Trace uses a repeatable-read transaction when it owns the read. It loads bounded history, folds it through the pure replay reducer, loads the live execution projection and operational command/wait data in the same snapshot, and overlays those operational fields onto reconstructed semantic commands.

This division is intentional:

- replay validates semantic history without workers or SQL side effects;
- projection reads expose current lease/readiness details not modeled as facts; and
- satisfying positions connect the two for exact event inputs.

Replay is a conformance and diagnostics mechanism, not an automatic projection-rebuild or disaster-recovery API in the current release.

Integrity work is deliberately split by boundary: accepted writes canonicalize
complete journal bodies and verify their hashes; claims perform direct hash and
bounded typed event decoding without redundant full reconstruction; replay
verifies hashes and re-canonicalizes every retained body before folding it.

## 16. Migrations and compatibility

Embedded migrations are rendered for one validated schema while table names retain the `flow_` prefix. `Migrate` takes an advisory transaction lock scoped to database/schema, verifies all known checksums, and applies each pending unit transactionally. `MigrationFS` provides equivalent SQL plus ledger inserts for an external runner.

`CheckSchema` is read-only and verifies:

- the ledger exists;
- every applied version/name/checksum/compatibility tuple is known;
- the schema is at the complete current version;
- reader/writer compatibility includes this library; and
- the exact six-table inventory exists.

`New` calls this check so incompatible replicas fail before claiming or publishing work. Command definitions have their own durable versions, allowing application rollouts independently of database migration versioning.

## 17. Data safety, retention, and operational limits

Canonical command arguments/results, event payloads, metadata, retry settings, and journal bodies are stored in PostgreSQL. Retained start/declaration fingerprints and journal body hashes support identity comparison and invariant checking; redundant write-only projection hashes are not stored. These values are not encryption. Applications must avoid putting secrets in keys, metadata, errors, or observer dimensions and should prefer stable references for sensitive/large values.

Parent-produced values should travel directly in child arguments, while exact
events carry sibling, cross-branch, or external facts. Related events and
children belong in one decision when they must commit together. Large or
sensitive documents stay in application storage behind stable references.

Structured Flow errors map database/constraint failures into safe sentinel categories without including raw SQL or driver details. Observers intentionally exclude payloads, results, SQL, connections, and lease tokens; delivery is bounded and non-blocking so monitoring cannot stall correctness.

The current schema retains journal and payload data and exposes no pruning API. Operators own PostgreSQL backup, access control, encryption, capacity planning, and any future application-approved archival process. Direct deletion or mutation of Flow-owned rows is outside the supported contract because it can break replay, idempotency, waits, and projection invariants.

Flow's primary availability boundary is PostgreSQL. Notification loss is tolerated, but loss of database availability pauses starts, claims, settlement, ingress, and inspection until operations can succeed again.

## 18. Deliberate omissions

There is no coordinator/state-machine object, graph evaluator, result dependency resolver, outcome subscription, workflow reconciliation loop, event-triggered callback, global event bus, cross-execution delivery record, OR/quorum/race gate, or exactly-once remote-effect protocol.

Adding any of those as hidden special cases would undermine the one-scheduler/one-lifecycle model. New capabilities must either compose from commands plus exact events or justify a deliberate expansion of the product model and its durable state.
