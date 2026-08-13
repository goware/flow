# Plan 14: Reduce scheduler latency and database round trips

Status: Implemented — independent review pending

Planned at: `ea0d62b` (v0.4.1, after Plan 12) on 2026-08-12

- **Branch:** `refactor/scheduler-latency`
- **Priority:** P2; this is performance work, not a correctness repair
- **Effort:** L, delivered in measured phases
- **Risk:** MEDIUM overall; transaction merging and any scheduler-probe redesign
  are HIGH-risk subphases because they touch locks, fences, fairness, and
  ambiguous-commit handling
- **Depends on:** Plans 5, 7, 12, and 13 as completed on `master`
- **Public API impact:** none in the required work
- **Durable format impact:** none
- **Schema impact:** none in the required work; a baseline-index replacement is
  permitted only if the conditional PostgreSQL plan gate proves it worthwhile

Implementation evidence: [`plan_14_scheduler_latency.md`](../benchmark_evidence/plan_14_scheduler_latency.md).
Required Phases 4.0–4.6 are complete. Conditional maintenance and scheduler
probe redesign are deferred. The preliminary PostgreSQL 17/18 wait-expiry
predicate experiment did not capture the required five samples or actual-row
counts, so that conditional change is also deferred and the index is unchanged.

> **Executor instructions:** Read this plan completely before editing. Work in
> phase order, run each phase's focused verification, and record measurements
> before proceeding. The required work deliberately keeps Flow's polling,
> locking, fencing, journal, and recovery semantics unchanged. Notifications
> and calculated horizons remain latency hints; neither may become necessary
> for correctness. Conditional phases are decisions, not pre-approved code:
> implement them only when their evidence gate passes. If a STOP condition is
> reached, stop and report it instead of broadening the design.
>
> **Drift check (run first on the implementation branch):**
>
> ```sh
> git status --short --branch
> git log -1 --oneline
> git diff --stat ea0d62b..HEAD -- \
>   command_runtime.go command_runtime_test.go \
>   runtime.go runtime_run.go runtime_run_test.go \
>   inspection.go inspection_test.go observer.go \
>   hardening_benchmark_test.go plan14_benchmark_test.go \
>   internal/store/store.go internal/store/store_test.go \
>   internal/store/commands.go internal/store/commands_test.go \
>   internal/store/graph.go internal/store/trace.go \
>   migrations/001_initial.sql \
>   specs/projects/flow
> ```
>
> Re-verify the symbols and invariants in Sections 2 and 4 if any in-scope
> source changed after `ea0d62b`. A changed scheduler continuation model,
> semantic lock order, settlement fault seam, maintenance lifecycle, or schema
> migration policy is a STOP condition until this plan is updated.

## 0. Relationship to prior and pending plans

- **Plan 5** completed the large hot-path changes: set-oriented decisions and
  claims, bounded independent claim concurrency, delta readiness, direct event
  decoding, and index pruning. This plan must preserve all of its fairness and
  ownership regressions.
- **Plan 7** fixed lease and maintenance correctness. Its category-local drain
  decision, shutdown accounting, and multi-replica lease behavior are hard
  boundaries for maintenance work here.
- **Plan 12** is complete in v0.4.1. Per-command recovery leases now reach the
  same maintenance service this plan may make more timely; Plan 14 must not
  change their renewal, watchdog, or takeover semantics.
- **Plan 13** is complete in v0.4.0. It simplified the public API and reset the
  schema to one consolidated baseline migration. This plan must not reintroduce
  compatibility migrations or add a speculative read API.
- **Plan 15** is proposed and assumes Plan 14 lands first. Its implementation
  should build on the precomputed replica name and no-observer fast path from
  Phase 5, and must re-baseline after Phase 3's transaction changes. Plan 15
  does not require any of Plan 14's conditional phases.

All source anchors and current-state facts below were re-verified against
`master` at `ea0d62b` after Plan 12 merged.

## 1. Purpose and current evidence

Flow is already simple at the public API and materially faster than the old
Plan 5 baseline. The remaining credible performance costs are on internal
runtime and store boundaries:

1. `runCommandScheduler` always waits the full fallback poll interval when it
   makes no progress (`command_runtime.go:34-188`). The probe reads ordered
   `next_run_at` values but returns only rows already due
   (`internal/store/commands.go:55-135`), so a delayed command can wait almost
   one additional poll interval.
2. `AwaitRun` performs `GetRun` every `min(pollInterval, 250ms)` and does not
   use the runtime's existing process-local wake hub
   (`inspection.go:235-266`, `runtime_run.go:34-74`).
3. `AttachSemantic` locks the run and fetches `clock_timestamp()` in two
   statements (`internal/store/store.go:126-151`). Claim then reads the locked
   run again (`internal/store/commands.go:245-255`); settlement loads the run
   head and deadline separately (`internal/store/commands.go:1550-1563`).
4. Claim and common settlement paths contain adjacent, independent projection
   writes that can share one pgx batch round trip while retaining their present
   order and row-count checks. No production path currently uses `SendBatch`.
5. `expireRunLocked` locks all open commands once, then performs one queue and
   journal delivery read for every running command while holding the run lock
   (`internal/store/commands.go:1668-1714`). This is a concrete N+1.
6. The runtime repeatedly copies pool configuration, rebuilds its replica name,
   starts a goroutine/channel claim pool for a single run group, and sorts UUIDs
   through allocated strings (`command_runtime.go:226-254`,
   `runtime_run.go:827-829`, `internal/store/graph.go:395-400`). The default
   no-op observer still traverses the adapter path.

The historical numbers are reference points, not the implementation baseline:

- final Plan 5 evidence recorded 172.4 commands/s for one independent producer,
  463.4 for four, 417.6 for sixteen, 143.6 for a 10-command same-run fan-out,
  and 175.0 for a 100-command fan-out;
- Plan 12 recorded a 5.863 ms median / 2,729 commands/s for the 16-command
  same-run claim benchmark on PostgreSQL 18.1.

Host, Go, PostgreSQL, and container variance has been material in earlier
measurements. Phase 0 therefore records a contemporaneous `ea0d62b` baseline
on the same server used for the after measurements. Do not describe 162.2
commands/s from the original Plan 5 baseline as current Flow performance.

## 2. Controlling decisions and invariants

### 2.1 Polling remains the correctness path

The configured `pollInterval` remains the maximum scheduler and maintenance
sleep. A notification or known future deadline may shorten a wait, never extend
it. Lost notifications, disabled notifications, a newly inserted earlier row,
or an incomplete horizon must still be repaired by the ordinary poll.

Consequently, this plan does **not** promise zero idle queries between known
horizons. The horizon work improves transition latency; it does not remove the
fallback poll.

### 2.2 Scheduler and maintenance horizons are separate

The command scheduler and maintenance are independent runtime services. Each
owns its own timer state. The scheduler horizon covers registered runnable
command kinds. Maintenance may later track the earliest run deadline, wait
deadline, and lease expiry, but it must keep Plan 7's category-local drain
cadence and must not share timer state with the command scheduler.

### 2.3 Duration calculations stay in the PostgreSQL clock domain

Do not subtract a PostgreSQL timestamp from local `time.Now()`. The query that
finds a future transition must return a nonnegative duration computed from one
captured `clock_timestamp()` value in PostgreSQL. The caller may reduce that
duration by local monotonic elapsed time since the probe began. This tolerates
database/application wall-clock skew and may wake conservatively early, not
late.

### 2.4 Hints remain global and conservative

The scheduler horizon must ignore the current continuation cursor, per-turn run
exclusions, and saturated-queue exclusions. Those are temporary search aids;
using them to calculate the next wake could hide earlier work after a lock or
lane becomes available. The returned horizon covers all future eligible rows
for registered command kinds whose runs remain active.

### 2.5 Lock, fence, commit, and fault ordering do not change

Every semantic operation still acquires the run row first. Command and queue
locks remain in their existing order. Claim/settlement fence checks, journal
ordering, notification decisions, `WithCommit` placement, fault-hook boundaries,
rollback behavior, and ambiguous-commit resolution are invariants.

`SendBatch` is permitted only for statements that are already adjacent and do
not depend on one another's returned data. Every result must be drained in
statement order and every affected-row count must still be checked. `CopyFrom`,
dependent reads, journal allocation, fault hooks, commit callbacks, and
notifications must not be moved merely to enlarge a batch.

### 2.6 Measurements decide optional complexity

Required phases remove clear waits, round trips, an N+1, or measured hot-path
allocations. Maintenance fan-out, probe SQL redesign, and index replacement are
conditional. A conditional change is adopted only when five same-environment
samples show a material improvement (normally at least 10% in the target
metric, or a clear reduction in database calls/buffers) without a material
regression elsewhere.

Absolute localhost timings such as “under 100 ms” are evidence, not CI
contracts. Functional tests use generous bounds that distinguish the new path
from the fallback interval without depending on a fast host.

### 2.7 Keep the public and durable models unchanged

No required phase adds a public option, public read model, table, column,
journal field, or event shape. If a schema experiment is accepted, edit the
single consolidated baseline migration; historical data and compatibility
migrations remain out of scope for this development line.

## 3. Scope

### Required work

1. Contemporaneous baseline and round-trip census (§4.0).
2. Database-duration-aware command-scheduler wake (§4.1).
3. Process-local `AwaitRun` wake acceleration with timer fallback (§4.2).
4. Semantic lock/head and safe adjacent-write round-trip merges (§4.3).
5. Set-oriented run-expiry delivery read (§4.4).
6. Small, measured allocation and single-group fast paths (§4.5).
7. Final measurement and evidence-based decisions on optional work (§4.6).

### Conditional work

Implement only after Phase 4.6 records a passing decision gate:

- maintenance deadline horizons and bounded cross-run fan-out (§5.1);
- scheduler probe/query/index redesign (§5.2); and
- wait-expiry baseline-index predicate replacement (§5.3).

### Explicitly deferred or rejected

- A public projection-only trace API. The internal projection read already
  exists at `internal/store/trace.go:48`, but current Trails progress code at
  `rpc/info_intent_stages.go:217` needs recent history as well as commands.
  Another owner-only call at line 725 could benefit, but Trails has no plan 011
  or agreed public contract for it. Design it in a consumer-driven plan if that
  endpoint remains expensive after measurement.
- Building every schema-qualified SQL string in `store.New`. Do not add dozens
  of stored query fields unless an allocation profile shows string construction
  is a material cost after the required work.
- Jittering the two short settlement retry sleeps. They are exceptional-path
  waits, and using a cancelled worker context can incorrectly abandon required
  settlement after handler timeout or lease loss. Revisit only with a concrete
  retry-storm trace and a settlement-specific cancellation contract.
- Removing polling, changing `pollInterval`, changing lease defaults, changing
  public queue semantics, or changing durable history.

## 4. Required implementation phases

### 4.0 Phase 0 — establish the baseline and operation census

Do this before production edits.

1. Record the exact commit, Go version, CPU, PostgreSQL version, pool size, and
   `fsync`, `synchronous_commit`, and `full_page_writes` settings.
2. On one durability-on PostgreSQL 18 server, run five one-second samples of:

   ```sh
   go test -run '^$' \
     -bench '^(BenchmarkIndependentCommandLifecycle|BenchmarkSameRunFanout|BenchmarkSameRunClaimBatch)$' \
     -benchtime=1s -count=5 ./
   ```

3. Add `plan14_benchmark_test.go` for two latency shapes not covered by the
   existing suite:
   - a command scheduled far inside a deliberately long poll interval; and
   - `AwaitRun` for same-runtime completion and timer-only fallback.
   Keep fixture creation/reset outside the timed region.
4. Add a test-only pgx tracer or equivalent test fixture that counts query,
   batch, and `CopyFrom` operations in the timed claim and simple-success
   settlement regions. Count protocol operations, not SQL strings inferred from
   source. Do not add public tracing configuration or production branches for
   this census.
5. Record complete sample ranges, medians, bytes/op, allocs/op, and the measured
   operation census in
   `specs/projects/flow/benchmark_evidence/plan_14_scheduler_latency.md`.

**Verify:** the benchmark commands complete without skipped workload shapes,
the evidence names `ea0d62b`, and the census is repeatable across three runs.

**STOP if:** a reliable operation census would require public API or invasive
production instrumentation. Report the code-reviewed census separately rather
than adding permanent complexity.

### 4.1 Phase 1 — wake the command scheduler at a known future time

Change `internal/store/commands.go:ProbeCommandsExcluding` to return both the
due candidates and an optional database-computed duration until the earliest
future eligible row. Return both in the existing database round trip, including
when there are no due candidates.

The query must:

- capture PostgreSQL time once and use it for due filtering and duration
  calculation;
- consider only registered `(name, version)` kinds and active runs;
- calculate the horizon globally, without the continuation cursor or temporary
  run/queue exclusions;
- clamp the duration to zero in PostgreSQL; and
- retain the present due-row order, cursor behavior, and candidate limit.

At the start of the probe, capture a local monotonic timestamp. Convert the
returned duration into a local remaining duration by subtracting monotonic time
spent probing and processing. When a scheduler turn makes no progress, wait for:

```text
min(pollInterval, max(0, remaining database-computed duration))
```

If the probe fails or returns no horizon, wait the ordinary `pollInterval`.
Notifications and `wake.signal` continue to interrupt the wait.

Tests in `command_runtime_test.go` and `internal/store/commands_test.go` must
cover:

- a future eligible command returns a positive duration even with zero due
  candidates;
- a due row and a later future row are both represented correctly;
- the horizon ignores cursor, run exclusions, and queue exclusions;
- terminal runs and unregistered kinds do not set the horizon;
- inserting earlier work after the probe is still found by the ordinary poll or
  wake signal;
- notifications disabled/lost does not affect correctness; and
- a command scheduled around 100–200 ms ahead with a 2-second poll starts after
  its durable `next_run_at` and comfortably before the 2-second fallback. Use a
  generous upper bound suitable for CI; keep precise latency in benchmarks.

**Acceptance:** no extra database round trip; scheduled-command median latency
improves materially versus Phase 0; all Plan 5 cursor/fairness tests remain
unchanged and green.

### 4.2 Phase 2 — accelerate `AwaitRun` with the local wake hub

`AwaitRun` remains a durable row check with a timer fallback. Use the runtime's
existing `wakeHub` only as a process-local broad wake hint:

1. snapshot the wake generation before the first `GetRun`;
2. call `GetRun` and return when terminal;
3. otherwise wait for either a generation change, context cancellation, or the
   unchanged fallback timer; and
4. repeat the durable `GetRun` after every wake.

The subscribe-before-read order closes the local lost-wake window. Do not add a
new PostgreSQL channel, per-wait goroutine, per-run subscription map, or promise
that terminal transitions always send `pg_notify`. `NotifyRunnableCommands`
intentionally notifies only newly immediate runnable work
(`internal/store/store.go:273-297`). Same-runtime attempts already signal the
local hub when their worker lifecycle ends (`command_runtime.go:420-425`);
remote completion, disabled notifications, or a lost notification therefore
uses the timer fallback.

Tests in `inspection_test.go` must cover:

- already-terminal run: one `GetRun` and immediate return;
- same-runtime terminal completion: local wake beats a deliberately long
  fallback interval;
- remote runtime or notifications disabled: completion is eventually observed
  by the timer;
- a broad unrelated wake causes only a re-check, not an incorrect return;
- cancellation returns `ctx.Err()` and leaves no subscriber/goroutine leak; and
- transaction clients remain rejected exactly as today.

**Acceptance:** same-runtime `AwaitRun` latency improves materially in the Phase
0 benchmark; timer-only behavior and public error semantics remain unchanged.

### 4.3 Phase 3 — reduce semantic transaction round trips

Split this phase into two reviewable commits or PRs so initial-row reuse and
write batching can be measured independently.

#### 4.3.1 Merge the run lock, database time, and initial run projection

`AttachSemantic` currently locks by `run_id` and then issues `SELECT
clock_timestamp()`. Replace those two statements with one database round trip
that also returns the minimal initial projection needed by the hot claim and
settlement paths: status, deadline, run key, and the `RunHead` counters used
before any in-transaction projection mutation.

The timestamp must still be evaluated **after** a contended row lock is
acquired. A plain `SELECT ..., clock_timestamp() ... FOR UPDATE` may evaluate a
volatile target expression while producing the input tuple, before `LockRows`
finishes waiting. Use an explicit execution barrier, such as a `MATERIALIZED`
CTE that acquires the row lock followed by an outer SELECT that evaluates
`clock_timestamp()`, and verify the resulting PostgreSQL plan/behavior. Preserve
`SKIP LOCKED` by applying it inside the locking CTE.

Expose that data as an explicitly named **initial locked snapshot** on
`SemanticTx`; do not silently turn `LoadRunHead` into a cache. Callers that need
state after an in-transaction update must still query the current row. Keep
`AdoptSemantic` valid for the newly inserted run path without fabricating a
snapshot it does not need.

Use the initial snapshot to remove:

- claim's immediate `status, deadline_at, run_key` reread; and
- settlement's immediate `LoadRunHead` plus separate `deadline_at` reread in
  `lockCommandFence`.

Audit every `LoadRunHead` caller before reusing the snapshot. Only reads that
occur before any projection mutation in the same semantic transaction are
eligible.

#### 4.3.2 Batch only adjacent independent projection writes

Use `pgx.Batch`/`SendBatch` for these initial candidates, provided the source
audit confirms they remain adjacent and independent:

- the queue and command updates in `updateClaimBatch`;
- successful-settlement command update plus queue deletion;
- retry-settlement command and queue updates; and
- terminal-settlement command update plus queue deletion.

Create one small internal helper if needed to drain ordered command tags and
apply existing affected-row checks. Do not batch across `SemanticTx.Apply`,
`CopyFrom`, readiness resolution, child persistence, failure resolution,
`NotifyRunnableCommands`, fault hooks, `WithCommit`, or commit.

Tests must exercise success, retry, permanent failure, child/event settlement,
run deadline, caller-owned transactions, injected faults after every existing
settlement seam, rollback, ambiguous commit, and multi-command claim. Add a
blocked-lock regression that holds the run row in another transaction, releases
it later, and proves `SemanticTx.DBNow()` was captured after acquisition rather
than before the wait. A batched-statement failure must leave no partial
committed projection.

**Acceptance:** the measured claim and simple-success settlement protocol census
drops by the exact number documented in the evidence; the 16-command claim and
independent lifecycle benchmarks improve or remain within the 10% investigation
gate; journal/fence/replay behavior is byte-for-byte and position-for-position
unchanged.

**STOP if:** a proposed merge changes lock order, moves a fault hook or callback,
requires using a stale initial snapshot, obscures an affected-row check, or
makes ambiguous ownership harder to resolve. Leave that statement unmerged.

### 4.4 Phase 4 — remove the run-expiry N+1

Keep the existing run-first and command-first lock order in
`expireRunLocked`:

1. lock and load all open commands ordered by `command_id` exactly as today;
2. collect the running command IDs;
3. in one ordered query, lock their queue rows and load each active attempt ID,
   lease token, and matching `attempt_started` position; and
4. validate that exactly one complete delivery row was returned for every
   running command before constructing journal entries.

The journal lookup is read-only; the queue rows are the mutable delivery fences.
Do not issue one query per command, loosen corruption checks, or change the
later command-key ordering used to construct deterministic journal entries.

Add PostgreSQL tests for zero running commands, one running command, 100 mixed
running/waiting commands, a missing/corrupt queue projection, rollback after the
bulk read, and concurrent settlement/expiry lock behavior.

**Acceptance:** a 100-command run expiry uses O(1) delivery-read queries, returns
the same terminal journal/projection shape, and does not regress the lock-order
or race suite.

### 4.5 Phase 5 — apply only small measured allocation improvements

Implement these independently and keep before/after `allocs/op` evidence for
each hot-path item:

- cache pool `MaxConns` in `Runtime` during `New` instead of calling
  `r.db.Conn.Config()` for every claim group;
- precompute the stable `runtime-<uuid>` replica name during `New`;
- if only one run group is selected, call `claimRunGroup` directly instead of
  creating a channel, goroutine, and `WaitGroup`;
- replace UUID `.String()` ordering in `internal/store/graph.go:399` with a byte
  comparison; and
- represent the default no-observer configuration as a true fast path that does
  not enqueue into the observer adapter. Guard before constructing observation
  fields at profiled sites that allocate; a check solely inside `observe` does
  not recover allocations already made by its caller.

Plan 15 will touch observation construction and must preserve this fast path and
cached replica name. Do not prebuild all store SQL strings or add guards around
every observation site without profile evidence.

**Acceptance:** every retained change reduces measured allocations or removes a
demonstrable per-call operation; no observer lifecycle, shutdown, or delivery
test changes semantics; no item causes a material throughput regression.

### 4.6 Phase 6 — remeasure and decide the conditional work

Repeat the Phase 0 environment capture, five-sample benchmark matrix, latency
benchmarks, and protocol census on the same PostgreSQL server. Record medians,
complete ranges, bytes/op, allocs/op, and exact before/after protocol counts.

For each conditional phase in Section 5, add a short decision record to the
evidence file:

- observed remaining bottleneck;
- proposed complexity and invariants at risk;
- focused benchmark/`EXPLAIN` result; and
- `ADOPT`, `DEFER`, or `REJECT` with a one-sentence reason.

Do not implement a conditional phase in the same commit that establishes its
need. A reviewer must be able to approve the measurement before the riskier
change begins.

## 5. Conditional phases

### 5.1 Maintenance horizons and cross-run fan-out

#### 5.1.1 Maintenance horizons

If delayed wait expiry, run deadline, or Plan 12 recovery-lease latency remains
material, extend each existing maintenance probe to return its own
database-computed duration until the earliest future transition. Keep those
durations in `runMaintenance`, separate from the scheduler. When a pass is not
drainable, use the minimum of:

- Plan 7's `nextMaintenanceDelay` result;
- the three valid maintenance horizon durations, reduced by local monotonic
  elapsed time; and
- `pollInterval`.

If a category is saturated and progressed, its existing drain cadence wins. If
a probe errors, discard only that category's horizon. Do not add separate timer
goroutines.

#### 5.1.2 Maintenance fan-out

Only if serial processing remains a measured bottleneck after the expiry N+1
fix, group a page by run and process candidates for the same run sequentially.
Bound concurrency only across distinct runs.

Do not blindly reuse claim concurrency. Claims and maintenance run
simultaneously and must leave pool headroom for lease renewal and ordinary API
work. Use one maintenance worker for pools of one or two connections; for larger
pools, adopt a separately bounded limit justified in the evidence and never let
combined claim plus maintenance limits consume the entire pool.

Preserve one `MaintenanceAfterProbe` fault hit per nonempty category page,
first-error reporting, observation counts/outcomes, category-local
`drainable`, shutdown cancellation, and complete goroutine drain before
`Runtime.Run` returns.

Required regressions include small pools, row-locked candidates, multiple
candidates in one run, independent runs, concurrent claim/renewal, shutdown,
and Plan 7's multi-replica tests.

### 5.2 Scheduler probe redesign and optional claim-index replacement

Treat this as a measured design spike, not a predetermined rewrite.

The current probe already filters run and queue exclusions on re-probes. A run
cannot be known busy until its lock attempt fails, and `slots.reserve` remains
the authority for queue capacity because SQL sees only a racing snapshot.

Compare at least:

1. the existing lateral per-kind query and `free*4` candidate inflation;
2. a smaller candidate limit; and
3. one capacity-aware/global-order SQL alternative, if it can remain simple.

Benchmark few/many registered kinds, sparse/dense ready work, saturated named
queues, locked runs, and multiple replicas. A smaller limit must not merely
trade scanned rows for more query round trips.

Every candidate must pass the existing hard fairness cases:

- capacity one with the oldest run locked;
- a saturated named lane ahead of another lane;
- more than 256 distinct blocked runs and candidate 257;
- a continuously extending blocked tail while an earlier row becomes runnable;
- bounded per-turn continuation with head revisits;
- multi-replica overlap; and
- exact slot release/transfer during shutdown and ambiguous claims.

If a global-order partial index is tested, compare PostgreSQL 17 and 18
`EXPLAIN (ANALYZE, BUFFERS)` against
`flow_command_queue_claim_idx`. Prefer replacing the existing claim index over
adding a second overlapping index. Adopt no index unless representative
many-kind workloads improve materially and write amplification remains bounded.

### 5.3 Wait-expiry baseline-index predicate

The current partial index is:

```sql
CREATE INDEX flow_commands_wait_deadline_idx
    ON flow_commands (wait_deadline_at, command_id)
    INCLUDE (run_id)
    WHERE state = 'pending' AND wait_deadline_at IS NOT NULL;
```

The table constraint already implies that pending commands have positive
`unsatisfied_waits`. Adding `AND unsatisfied_waits > 0` is logically redundant
but might let PostgreSQL prove the probe predicate without a heap check.

Test the exact production query on PostgreSQL 17 and 18 with representative
resolved/unresolved populations. Record plan nodes, actual rows, heap fetches,
buffers, execution time, and index size. If the predicate materially improves
the plan, replace the definition in `migrations/001_initial.sql` and migration
catalog tests. Do not add a compatibility migration and do not INCLUDE
`unsatisfied_waits`; it is mutable and would increase write amplification.

Reject the change if the planner and buffers are effectively unchanged.

## 6. Verification matrix

### Focused gates per phase

- Phase 1: command probe, scheduler cursor/fairness, notifications-disabled,
  delayed command, and multi-replica tests.
- Phase 2: inspection/AwaitRun tests under race, including cancellation and
  fallback.
- Phase 3: claim, worker settlement, retry, fail-fast, caller-owned transaction,
  fault, ambiguous commit, replay, and journal consistency tests.
- Phase 4: run deadline/expiry, mixed command state, lock-order, fault, replay,
  and multi-replica tests.
- Phase 5: observer/no-observer, shutdown, single/multiple claim group, and
  allocation benchmarks.
- Conditional maintenance/probe/index work: all Plan 5, Plan 7, and Plan 12
  fairness, small-pool, renewal, watchdog, takeover, migration, and query-plan
  tests.

### Full gates after every production phase

Run against configured PostgreSQL with no integration skips:

```sh
gofmt -w <changed Go files>
git diff --check
make build
go vet ./...
go test -count=1 -p 1 -parallel 4 ./...
make test
```

Before final completion, run the exact ordinary and full race suites on both
PostgreSQL 17 and 18 with `fsync=on`, `synchronous_commit=on`, and
`full_page_writes=on`. Audit named test output: zero named skips and zero
failures. Re-run the complete benchmark matrix five times on one unchanged
PostgreSQL 18 server.

### Global invariants to audit after the final diff

- polling alone still discovers every command and maintenance transition;
- six tables and the consolidated baseline migration remain the storage model;
- journal bodies, positions, causation, hashes, and replay output are unchanged;
- run-first lock order and command/queue fence order are unchanged;
- no duplicate attempt fence exists under multiple runtimes;
- successful or ambiguously committed claims are transferred to worker/slot
  accounting exactly once;
- caller-owned transaction commit/rollback visibility is unchanged;
- notification calls still correspond only to immediate runnable work;
- no required phase adds a public symbol; and
- Plan 15's observer prerequisites are documented in its drift update.

## 7. STOP conditions

Stop and report rather than improvising if:

- source drift invalidates a current-state fact or test anchor in this plan;
- a horizon requires local/database wall-clock subtraction, removes the poll
  cap, or becomes necessary for correctness;
- an `AwaitRun` optimization needs a durable per-run subscription or assumes
  terminal settlement always emits `pg_notify`;
- a round-trip merge changes lock/fence order, fault hooks, callback placement,
  row-count validation, commit ownership, or ambiguous-commit recovery;
- the combined semantic-lock statement cannot prove that database time is
  captured after lock acquisition;
- the expiry bulk read cannot preserve run → command → queue lock order;
- maintenance concurrency can consume the pool headroom needed by renewal or
  claims;
- a probe/index experiment fails a fairness regression or does not show a
  material same-environment improvement;
- any target benchmark regresses by more than 10% in two repeated
  same-environment runs without an explained compensating gain;
- a required phase needs a public API, durable-format change, new table/column,
  or compatibility migration; or
- PostgreSQL 17/18, race, replay, migration, or no-skip gates fail twice after a
  reasonable focused correction.

## 8. Delivery order

Recommended PR order:

1. Phase 0 evidence plus Phase 1 scheduler horizon.
2. Phase 2 local `AwaitRun` wake.
3. Phase 3.1 initial semantic snapshot merge.
4. Phase 3.2 safe adjacent-write batching.
5. Phase 4 run-expiry N+1 removal.
6. Phase 5 measured allocation pass.
7. Phase 6 final evidence and conditional decisions.
8. Separate PRs for only the conditional phases marked `ADOPT`.

Phase 3 and Phase 5 must land before Plan 15 begins. The other conditional
phases do not block Plan 15. Do not create a release solely for an intermediate
Plan 14 phase; release timing remains a repository-level decision.

## 9. Punchlist

### Baseline and scheduler latency

- [x] Rebase implementation work on `ea0d62b` or newer and refresh every source
  anchor.
- [x] Record the contemporaneous environment, five-sample benchmark baseline,
  and claim/settle protocol census.
- [x] Add scheduled-command and `AwaitRun` latency benchmarks with setup outside
  the timer.
- [x] Return a global database-computed future-work duration in the existing
  command-probe round trip.
- [x] Cap every scheduler sleep at `pollInterval` and preserve notification
  interruption.
- [x] Prove horizon correctness across cursor/exclusion, disabled notification,
  newly earlier work, unregistered kinds, and terminal runs.

### AwaitRun

- [x] Snapshot the local wake generation before the first durable run read.
- [x] Re-check durable state after local wakes and the unchanged fallback timer.
- [x] Cover same-runtime fast wake, remote/timer fallback, unrelated broad wake,
  cancellation, no leak, and transaction-client rejection.

### Round trips and transaction safety

- [x] Merge run lock, database time, and the minimal initial run projection into
  one statement.
- [x] Prove with a contended-lock test that `DBNow` is captured after the lock is
  acquired, including `SKIP LOCKED` behavior.
- [x] Make initial snapshot semantics explicit; retain live `LoadRunHead` reads
  after projection mutation.
- [x] Remove only the verified immediate claim and settlement run rereads.
- [x] Batch adjacent independent claim projection writes, drain every result,
  and preserve every existing affected-row check.
- [x] Batch only the verified adjacent settlement projection pairs without
  crossing hooks, children/events, callbacks, notifications, or commit.
- [x] Prove rollback, fault, ambiguous-commit, caller-transaction, replay, and
  journal invariants.
- [x] Record exact before/after protocol-operation counts.

### Run expiry and allocations

- [x] Replace per-running-command expiry delivery reads with one ordered bulk
  lock/read and exact row-shape validation.
- [x] Prove zero/one/100-command, corruption, rollback, and concurrent settlement
  behavior.
- [x] Cache pool capacity and replica name at runtime construction.
- [x] Add the direct single-run claim-group path.
- [x] Replace profiled UUID string sorting with byte comparison.
- [x] Make the default no-observer path avoid adapter enqueue work and preserve
  Plan 15's extension point.
- [x] Retain only allocation changes with measured benefit and no material
  throughput regression.

### Conditional decisions

- [x] Rerun the complete five-sample matrix and append final evidence.
- [x] Record `ADOPT`, `DEFER`, or `REJECT` for maintenance horizons/fan-out,
  scheduler probe/index redesign, and wait-expiry predicate replacement.
- [x] If maintenance is adopted, preserve category-local drain behavior,
  per-run serialization, pool headroom, and shutdown drain. N/A: deferred.
- [x] If probe/index work is adopted, pass every bounded-fairness regression and
  PostgreSQL 17/18 plan gate. N/A: deferred.
- [x] If the wait index is adopted, replace only the consolidated baseline index
  and do not INCLUDE mutable counters. N/A: deferred because the preliminary
  experiment did not satisfy the five-sample and actual-row evidence gate.
- [x] Keep projection-only public reads, global SQL prebuilding, and settlement
  jitter outside Plan 14.

### Final verification

- [x] Pass format, diff, build, vet, exact ordinary, and full race gates.
- [x] Pass PostgreSQL 17 and 18 durability-on suites with zero named skips.
- [x] Complete the final lock/fence/journal/notification/public-API audit.
- [x] Update Plan 15's drift anchors and benchmark baseline before its
  implementation begins.
- [x] Mark Plan 14 complete only after required phases, evidence, and all adopted
  conditional phases pass; deferred/rejected decisions must remain recorded.
