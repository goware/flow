---
status: draft
---

# Component: Runtime and Operations

## 1. Purpose and scope

This component is everything that runs continuously: the worker pool, the claim loop, lease management, the notifier and wake hub, maintenance sweeps, the migration engine, error mapping, and the public test harness.

Responsibilities: process lifecycle, dispatch, leases, notification, maintenance, migrations, error classification, observation delivery, and `flowtest`.

Non-responsibilities: SQL (`schema.md`), settle-transaction algorithms (`engine.md`), public type shapes (functional spec §4).

## 2. Runtime structure

```go
type Runtime struct {
    db       *pgkit.DB
    cfg      Config
    registry *registry          // immutable after Run

    lanes    map[string]*lane   // one dispatcher per bound lane
    leases   *leaseManager      // one per process
    notifier *notifier          // optional, one session connection
    hub      *wakeHub
    maint    *maintenance

    state    lifecycleState     // created → running → stopping → stopped
}
```

Lifecycle is one-way:

```text
created → running → stopping → stopped
                  ↘ halted ───┘
```

`Register` is valid only in `created`. `Run` transitions to `running`, blocks, and may be called once. `Stop` is idempotent. There is no restart after `stopped`.

### 2.1 Registry

The registry is built from `Registration` values and frozen at `Run`:

```go
type registry struct {
    workers map[nameVersion]workerEntry   // command handlers
    plans   map[nameVersion]planEntry
    coords  map[nameVersion]coordEntry
    lanes   map[string][]nameVersion      // claim filter per lane
}
```

`lanes` is what the claim statement's `(name, version)` arrays are built from, and it is immutable — so a claim filter can never drift from what the process can actually execute. Duplicate registration of a `(name, version)` pair is rejected at registration time.

## 3. Claim loop

One dispatcher goroutine per bound lane.

```go
for {
    free := lane.slots.Available()
    if free == 0 {
        lane.waitForCapacity(ctx)
        continue
    }

    batch := min(free, lane.cfg.MaxClaimBatch)
    claimed, err := store.ClaimCommands(ctx, lane.name, registry.lanes[lane.name], batch, cfg.LeaseDuration)
    if err != nil {
        lane.backoff.Next()
        continue
    }

    if len(claimed) > 0 {
        lane.backoff.Reset()
        for _, c := range claimed {
            leases.Register(c)
            lane.dispatch(c)      // minimally buffered handoff
        }
        continue                  // immediately try for more
    }

    lane.waitAny(ctx, hub.Subscribe(lane.name), lane.nextVisibleTimer(), cfg.PollInterval)
}
```

Three rules the loop must not violate:

1. **Never claim beyond immediately free capacity.** `batch` is bounded by `free`, not by configuration alone, so a lease never starts ticking while its command sits in a local queue.
2. **Never hold a connection across handler execution.** The claim transaction commits before dispatch.
3. **Empty claim arms a timer, it does not spin.** The wait is the earliest of a wake hint, the next `eligible_at` in that lane, and the fallback poll.

`nextVisibleTimer` issues one cheap indexed query for the lane's minimum future `eligible_at`, capped by the poll interval, so a lane whose next work is an hour out does not poll every second.

### 3.1 Handler invocation

```go
func (l *lane) run(c ClaimedCommand) {
    entry := registry.workers[c.NameVersion()]
    hctx, cancel := withTimeout(ctx, effectiveTimeout(c, entry))
    defer cancel()

    result, err := entry.invoke(hctx, c.Payload, scope)      // panics recovered here
    leases.Unregister(c)

    switch classify(err, hctx) {
    case outcomeSuccess:     store.Settle(c, result, scope)
    case outcomeRetry:       store.Retry(c, retryAt(c, err), err)
    case outcomePermanent:   store.Fail(c, err)
    case outcomeInterrupted: store.Release(c)                // no budget consumed
    case outcomeLeaseLost:   /* write nothing; the fence already rejects us */
    }
}
```

Timeout precedence is per-node override, then command definition, then pool default, then none. A timeout is classified as a retryable failure that consumes budget; shutdown interruption and lease loss consume nothing.

## 4. Lease manager

One per process, tracking every active attempt.

```go
type leaseManager struct {
    active map[CommandID]*activeLease   // lease token, expiry, cancel func
}
```

Each lease is renewed at one third of its duration, bounded to 5–30 seconds, with per-lease jitter so a process holding 200 leases does not renew them in one burst. Renewal batches by lane and target duration into a single statement.

```go
func (m *leaseManager) renewBatch(ctx context.Context, batch []*activeLease) {
    kept, err := store.RenewLeases(ctx, ids(batch), tokens(batch), m.cfg.LeaseDuration)
    if err != nil {
        // transient: retry only while enough lease time plausibly remains
        return
    }
    for _, l := range batch {
        if !kept.Has(l.CommandID) {
            l.cancel(ErrLeaseLost)       // cancel only this handler's context
            m.Unregister(l.CommandID)
        }
    }
}
```

A receipt not returned by the renewal statement has lost ownership. Only that handler's context is cancelled; other handlers in the process are untouched. The manager never attempts to reacquire an expired lease, and never claims ownership after known expiry — the fence in `schema.md` §5.4 would reject the write anyway.

## 5. Notifier and wake hub

### 5.1 Channel

The channel name is `flow_` plus the first 24 lowercase hex characters of SHA-256 over the normalized schema name, so multiple schemas in one database do not cross-wake and no untrusted identifier is ever interpolated into `LISTEN`.

Payload is compact versioned JSON — a lane name or an execution ID, never payloads, arguments, errors, or credentials:

```json
{"v":1,"k":"lane","n":"monitors"}
```

Unknown versions or kinds trigger a process-wide catch-up wake rather than an error.

### 5.2 Listener lifecycle

The notifier owns one session-persistent `pgx.Conn`, obtained from `WithListenConnector` when queries go through a transaction-pooling proxy. Without a connector the runtime is poll-only, which is fully correct and reported through inspection rather than logged as a warning.

On every connection:

1. connect with context and set `application_name`;
2. execute the quoted `LISTEN` and commit it;
3. **publish a catch-up wake for every bound lane** and ask the lease manager to revalidate fences;
4. read notifications until error or shutdown;
5. on error, close, report, and reconnect with capped jittered backoff.

Step 3 closes the start-up and reconnect race: work committed while no listener was attached is found by the resulting poll rather than waiting for the next unrelated notification.

### 5.3 Wake hub

An in-memory router keyed by lane. Each key holds a monotonically increasing generation; subscribers receive through a capacity-one channel.

```go
gen := hub.Generation(lane)
if claimed := tryClaim(); len(claimed) > 0 { … }
hub.WaitSince(lane, gen)     // returns immediately if generation advanced
```

Recording the generation *before* the claim attempt and waiting on it *after* is what prevents a wake arriving between check and sleep from being lost. Duplicate hints coalesce; a process-wide wake bumps every key; shutdown wakes all waiters with terminal state.

### 5.4 The NOTIFY cost

Transactional `NOTIFY` takes a global lock held through commit, serializing all notifying commits database-wide — the DBOS finding recorded in the architecture. Every settle transaction potentially notifies, so this is a candidate system-wide ceiling.

Exposure is bounded by emitting **at most one hint per affected lane per transaction**, deduplicated in the change set before commit. If benchmarks show the lock is material, the sanctioned evolution is a decoupled hint flusher that buffers hints in memory and emits them in small separate transactions, accepting slightly higher wake latency. Fallback polling already covers hint loss, and PostgreSQL row state remains the only source of truth, so this change is safe by construction. It must be benchmarked before v1.

## 6. Maintenance

```go
type MaintenanceConfig struct {
    PollInterval time.Duration   // default 30s
    BatchSize    int             // default 500
    Concurrency  int             // default 2
}
```

Tasks, each bounded and using `SKIP LOCKED`:

| Task | Effect |
|---|---|
| due retries | `retry_wait → ready` for elapsed retry times; wakes affected lanes |
| start-deadline expiry | `pending`/`ready` past `start_deadline_at` → `expired`, then settle-path resolution |
| lease recovery | `running` past `lease_expires_at` → `ready`, attempt marked `lease_lost`, no budget consumed |
| execution deadline expiry | running executions past `deadline_at` → `expired`, outstanding work cancelled |
| coordinator dispatch | claims `active` coordinators with events above their inbox position |
| unclaimable reporting | emits backlog observations by `(name, version)` with no live registrant |

There is no leader election. Duplicate runners are safe because every task claims bounded rows with `SKIP LOCKED`. A full batch re-signals immediately so large backlogs drain without waiting a full interval.

Expiry and lease recovery re-enter the settle path per execution, taking the execution lock normally — they are not a bypass.

## 7. Graceful shutdown

`Stop(ctx)`:

1. transition to `stopping` once;
2. stop all claim loops;
3. keep renewing leases for in-flight handlers;
4. wait for handlers up to the grace period (default 30s);
5. let handlers that finished in time run their settle transactions;
6. cancel the remainder with a shutdown cause;
7. release their commands so another replica can claim immediately, recording attempts as `interrupted` with no budget consumed;
8. stop the lease manager, notifier, and maintenance;
9. return joined errors.

`Halt()` cancels immediately and relies on lease expiry where release cannot be persisted. Neither path ever acknowledges unfinished work.

## 8. Transaction composition

Three modes, matching architecture §8.1:

```go
func (r *Runtime) InTx(tx pgx.Tx) Client
func (r *Runtime) BindTx(tx pgx.Tx) *TxBinding
func (r *Runtime) Transact(ctx context.Context, opts pgx.TxOptions, fn func(context.Context, Client) error) error
```

A bound client never begins, commits, rolls back, or retries the caller's transaction, and preserves every lock and fence rule.

Because a bare `pgx.Tx` exposes no commit hook, `InTx` **suppresses commit-dependent observations** rather than reporting a mutation that might roll back. `Transact` and `BindTx.Finish(err)` provide the hooked form; `Finish` is idempotent and never affects the transaction. Dropping an observation is always preferred over emitting a false one.

Callers composing application writes with flow operations must perform application writes first and call flow last (architecture §8.2). Interleaving can deadlock against the engine's lock order, and a caller-owned transaction gets the error rather than an automatic replay.

## 9. Error mapping

The mapper uses `errors.Is`, `errors.As`, pgx error types, SQLSTATE, and constraint names — **never message text**.

| Source | Classification |
|---|---|
| context cancelled / deadline | preserve context error with operation detail |
| `23505` on a known idempotency constraint | compare stored hash → success or `ErrConflict` |
| `23503` known reference | `ErrNotFound` or `ErrConflict` by operation |
| `23514` known check | `ErrInvalid` with the safe field name |
| zero rows from a fenced update | diagnostic lookup → `ErrLeaseLost`, `ErrTerminal`, `ErrNotFound` |
| `40001`, `40P01` | internal retry where authorized, else typed transient |
| `55P03` lock not available | typed transient; no blind retry |
| connection loss at commit | uncertain-commit detail; stable keys let the caller re-derive the outcome |
| checksum or version mismatch | `ErrSchema`, startup-fatal |
| unmapped | wrapped backend error with SQLSTATE, no SQL or data |

Handler errors are job-policy input, not database errors, and never enter this table.

## 10. Migrations

```go
func MigrationFS(opts MigrationOptions) (fs.FS, error)
func Migrate(ctx context.Context, db *pgkit.DB, opts ...MigrateOption) (SchemaStatus, error)
func CheckSchema(ctx context.Context, db *pgkit.DB, opts ...MigrateOption) (SchemaStatus, error)
```

Units, applied in order and never renumbered:

1. migration metadata and compatibility tables;
2. executions;
3. commands and dependency clauses;
4. attempts;
5. events;
6. plan reads and coordinators;
7. autovacuum settings and cross-table indexes.

Per unit: begin, `pg_advisory_xact_lock` on two fixed keys derived from database identity and schema hash, re-check the next required version under the lock, verify checksums of everything already applied, execute exactly one unit, record version/name/checksum/library version, commit. Concurrent migrators interleave only at unit boundaries and never apply a unit twice.

Checksum mismatch, unknown future version, or an incompatible reader/writer range stops without mutation. `New` verifies compatibility and never migrates implicitly.

## 11. Observability

One `Observation` struct, one `Observer` interface, no-op by default. Observations are buffered inside transactions and flushed only after the commit result is known.

Required measurements:

- claims, empty claims, claim latency, batch sizes;
- handler duration and outcome by classification;
- attempt outcomes split by **operational interruption vs application failure**;
- retry depth and scheduled retry times;
- lease renewals, renewal latency, lease losses;
- **plan evaluations, evaluations skipped, nodes evaluated, evaluation duration**;
- dependency resolutions and cascading skips per settle;
- `absent_reads` per execution;
- unclaimable backlog by `(name, version)`;
- execution row lock wait time;
- notification latency versus poll-only, listener reconnects;
- maintenance rows processed per task.

Dimensions: execution type and ID, command key, name, version, lane, worker and process identity, correlation and causation IDs, outcome category. Never payloads, arguments, results, errors, lease tokens, or SQL.

Observer callbacks run outside transactions and locks. Panics are recovered and reported through a final diagnostic hook; a slow or broken observer can never change a durable result.

## 12. The `flowtest` harness

Public, so applications test their own handlers and plans without a database.

```go
package flowtest

// Workers: an ordinary function call with staged output captured.
func RunWorker[A, R any](t *testing.T, h func(context.Context, *flow.Work[A]) (R, error), payload A, opts ...WorkOption) WorkerResult[R]

type WorkerResult[R any] struct {
    Result   R
    Err      error
    Events   []StagedEvent      // from Emit
    Commands []StagedCommand    // from Send
    Writes   int                // OnCommit callbacks registered
}

// Plans: a pure function over a constructed world.
func RunPlan[A any](t *testing.T, p flow.PlanDef[A], args A, w World) PlanResult

type World struct {
    Facts []Fact                          // published events
    Nodes map[string]NodeState            // declared command states
}

type PlanResult struct {
    Declared []DeclaredNode               // key, command, payload, clauses
    Reads    []Read                       // name, kind, present
    Absent   int
    Err      error
}

// Assertions
func (r PlanResult) AssertDeclares(t *testing.T, keys ...string)
func (r PlanResult) AssertWaits(t *testing.T, key string, on ...string)
func (r PlanResult) AssertNoAbsentReads(t *testing.T)

// Determinism and encoding safety
func AssertPlanDeterministic[A any](t *testing.T, p flow.PlanDef[A], args A, w World)
func AssertCanonicalStable[T any](t *testing.T, values ...T)
```

`AssertCanonicalStable` round-trips a payload type through canonical encoding repeatedly and diffs the bytes, catching the nondeterministic `MarshalJSON` hazard from architecture §17 in the application's own tests.

Integration tests use a privileged harness that creates one uniquely named schema per run and drops only that schema on cleanup.

## 13. Fault injection

Named fault points, available only under a test build tag:

`after_claim`, `before_handler`, `after_handler`, `before_settle_lock`, `after_position_alloc`, `after_event_insert`, `after_resolution`, `after_reconcile`, `before_oncommit`, `after_oncommit`, `before_notify`, `after_commit_send`, `before_commit_response`, `listener_connect`, `each_maintenance_move`, `each_migration_unit`.

Tests restart runtimes and assert durable invariants rather than goroutine timing.

## 14. Test plan

**Runtime and dispatch** — claims never exceed free capacity; no connection held during handler execution; weighted lanes avoid starvation; empty claim arms the correct timer; 100+ concurrent handlers under `-race`.

**Leases** — renewal batching; isolated lease loss cancels one handler only; expired lease cannot settle; renewal under database outage; shutdown race with in-flight renewal.

**Notifications** — notify after commit and none after rollback; catch-up closes the start-up race; duplicate, malformed, and unknown-version hints stay safe; the generation counter prevents a lost wake between check and sleep; poll-only mode processes everything; reconnect leaks no goroutines or connections.

**Maintenance** — multiple runners divide work safely; full batches self-wake; crash mid-move leaves source or destination, never an invalid state; expiry re-enters the settle path correctly.

**Migrations** — clean install; repeated install; concurrent migrators; checksum mismatch; unknown future version; failing unit rolls back; upgrade from every released fixture; custom schema rendering; runtime role cannot alter migration structure.

**Composition** — bound client never commits or retries; `Transact` and `BindTx` observation results match actual commit; bare `InTx` suppresses rather than falsifies; ambiguous commit is not reported as rollback.

**Errors and safety** — every named constraint maps as specified; no observation or error contains payloads, tokens, or SQL; observer panic and latency cannot affect durable results.

**Benchmarks** — claim throughput by lane count and concurrency; lease renewal at 500 active leases; **transactional `NOTIFY` commit throughput versus a batched flusher versus poll-only** (§5.4); maintenance sweep cost at large backlogs; shutdown latency with long handlers.

## 15. Acceptance conditions

- claims are bounded by immediately free capacity and hold no connection during handler execution;
- the claim filter is built from the immutable registry and cannot drift;
- leases renew automatically, and lease loss cancels exactly one handler;
- catch-up on listener connect closes the start-up race, and poll-only mode is fully correct;
- at most one wake hint per lane per transaction;
- the `NOTIFY` ceiling is benchmarked before v1, with the batched flusher designed but unshipped unless measurement demands it;
- maintenance is bounded, leaderless, and safe with duplicate runners;
- migrations are per-unit transactional, advisory-locked, checksummed, and concurrent-safe;
- error classification depends only on codes and constraint names;
- commit-dependent observations are never emitted before the outcome is known;
- `flowtest` runs workers and plans with no database, and ships the determinism and canonical-stability assertions;
- all runtime, fault-injection, migration, and benchmark tests pass.
