# Plan 14: Scheduler latency and round-trip reduction

Status: Proposed
Priority: P2 · Effort: L (phased, each phase independently shippable) · Risk: LOW–MEDIUM

Branch: `refactor/scheduler-latency` (master `7abfb8c` merged forward to `d9125dc` / v0.4.0)

## 0. Relationship to prior plans (why this is not redundant)

- **Plan 12** (fast lease recovery, Proposed) owns the 60s-lease dead-time
  after a worker death. It is out of scope here; §4.4 pairs with it but
  touches the maintenance *loop shape*, which plan 12 explicitly leaves
  unchanged (its §5.4).
- **Plan 5** (hot-path efficiency, Complete) batched ingress-side inserts
  (`CopyFrom` for journal/children/waits) and made notifications meaningful.
  This plan addresses what survives plan 5 on current master: the scheduler's
  fixed sleep, claim/settle statement merges and `SendBatch` for trailing
  writes, maintenance transaction shape, probe fan-out, and allocations.
  All file:line evidence below was verified against master `7abfb8c`.
- **Plan 7** (lease/maintenance bug fixes, Complete) was correctness, not
  performance; its invariants are hard boundaries for §4.4.
- **Plan 13** (simpler core, retention, typed reads — shipped in v0.4.0)
  reworked the read API surface, retention, and the queue-*stat* N+1. No
  overlap with the phases here; key anchors re-verified on `d9125dc`:
  fixed poll (`command_runtime.go:51,187`), `AwaitRun` poll
  (`inspection.go:246`), pgxpool `Config()` copy (`command_runtime.go:233`),
  `expireRunLocked` N+1 (`internal/store/commands.go:1695`), and still no
  `SendBatch` outside tests. §4.7 should build on plan 13's typed-read
  vocabulary.

## 1. Purpose

On an idle localhost, flow's end-to-end latency is dominated by fixed timers
and per-statement round trips, not by Postgres. Plan 12 attacks the largest
single dead-time window (the 60s command lease after a worker death). This
plan covers everything else found in a hot-path audit (2026-08-12, findings
verified against `master`):

- Every delayed transition (retry backoff, `StartAfter`, re-enqueued monitor
  polls) is discovered only by the fixed poll: mean +`pollInterval/2`, worst
  +`pollInterval` per hop, because the scheduler discards knowledge of future
  `next_run_at` values.
- `AwaitRun` ignores the notification hub the runtime already maintains and
  polls a full run row (including `metadata` jsonb) every 250ms.
- A minimal command lifecycle spends ~23 sequential DB round trips
  (claim ≈ 11, settle ≈ 12; measured baseline 162 cmd/s ≈ 6.2 ms/command,
  `benchmark_evidence/plan_5_hot_path.md`); several are mergeable and none
  are pipelined.
- Maintenance recovers expired leases/waits/deadlines one full transaction
  per row, serially; run expiry has an N+1 (`FOR UPDATE` per running command)
  inside the held run lock.
- The probe scans up to `kinds × limit` index rows per wake and re-probes
  once per busy run / saturated lane per turn.

Quantified deltas below marked *(estimate)* are arithmetic on the verified
code paths, not measurements; each phase's acceptance re-measures.

## 2. Controlling decisions

### 2.1 The fixed poll remains the correctness fallback

Every change here is an *earlier wake-up* or a *cheaper statement sequence*.
No transition may come to depend on a notification or computed horizon for
correctness: the `pollInterval` sweep must still discover everything on its
own, exactly as today.

### 2.2 Sleep until the earliest known future work, capped by `pollInterval`

The probe already orders by `next_run_at`; it additionally returns
`MIN(next_run_at)` over future rows, and the scheduler sleeps
`min(pollInterval, untilNextRunAt)`. Maintenance probes adopt the same
pattern with their own horizons (`MIN(deadline_at)`, `MIN(wait_deadline_at)`,
`MIN(lease_expires_at)`). An idle runtime stops issuing queries entirely
between horizons; a delayed command wakes on time instead of on the next
tick.

### 2.3 Merge round trips; do not reorder semantics

Statement merges must keep the same lock acquisition order and the same
fence checks. Allowed: combining `SELECT … FOR UPDATE` with
`clock_timestamp()`; reusing the locked head row instead of re-reading it;
issuing independent trailing writes in one `tx.SendBatch`. Not allowed:
moving any read before its lock or any write across the fence validation.

### 2.4 Observability stays; its allocation cost goes

Observations remain emitted at the same points. The fix is precomputing the
replica name, short-circuiting when the observer is `NopObserver`, and
avoiding per-observation UUID string formatting.

## 3. Scope

In scope (flow only):

1. Next-run-aware scheduler timer (§4.1) — `command_runtime.go:51,187`,
   `internal/store/commands.go:97-118`.
2. `AwaitRun` subscribes to the wake hub, timer as fallback (§4.2) —
   `inspection.go:246-266`.
3. Round-trip merges + `SendBatch` for trailing writes (§4.3) —
   `internal/store/store.go:141,148`, `commands.go:250,551,567,
   1184-1250,1583`, `ingress.go:865`.
4. Maintenance fan-out and expiry N+1 (§4.4) — `runtime_run.go:586-634`,
   `internal/store/commands.go:1685-1700`.
5. Probe efficiency (§4.5) — cap `limit` by free slots;
   lane-capacity-aware probe; optional global-order partial index.
6. Allocation pass (§4.6) — `Config()` deep-copy per round
   (`command_runtime.go:233`), per-call SQL string building
   (`internal/pgschema`), `replicaName()` per observation
   (`runtime_run.go:669-671`), UUID-string sort (`graph.go:421`),
   single-group claim fast path (`command_runtime.go:226-254`).
7. Projection-only trace read (§4.7) — public `TraceOperational`-style API
   that skips the journal fold, for callers that need owner state, not
   history.
8. Wait-expiry index coverage (§4.8) — add `unsatisfied_waits > 0` to the
   `flow_commands_wait_deadline_idx` partial predicate (or INCLUDE it).
9. Jittered, context-aware settlement retry backoff replacing fixed
   `time.Sleep` (`command_runtime.go:565,706`).

Out of scope:

- Plan 12 (`WithRecoveryLease`) — independent; pairs well with §4.4.
- Any storage schema change beyond the §4.8 index predicate.
- Changing the default `pollInterval` or lease values.

## 4. Phases

### 4.1 Phase 1 — next-run-aware timer (biggest latency win)

Probe returns `(candidates, minFutureNextRunAt)`. `runCommandScheduler`
tracks the horizon across probe/maintenance and passes
`min(pollInterval, horizon-now)` to `wake.wait`. Notifications and completed
work still cut the sleep short exactly as today.

Acceptance: a command chain with six 1s `StartAfter` hops completes within
`6s + ~50ms` on localhost (today: `6s + up to 1.5s`) *(estimate; measure)*;
an idle runtime issues zero probe queries between horizons.

### 4.2 Phase 2 — `AwaitRun` on the wake hub

Subscribe before the first `GetRun`; re-check on every hub signal and on the
(unchanged) fallback timer. Skip fetching `metadata` until the run is
terminal.

Acceptance: `AwaitRun` on an already-terminal run returns in one query; on a
run that terminates mid-wait it returns within ~10ms of the settle NOTIFY
*(estimate; measure)*. trails-api's DB-backed test suites get this for free.

### 4.3 Phase 3 — round-trip merges

- `BeginSemantic`: one statement for lock + `clock_timestamp()`.
- Claim: drop the re-read of the locked run row (`commands.go:250`); reuse
  the head loaded under the same lock.
- Settle: same for `LoadRunHead` + `deadline_at`.
- Trailing independent writes per transaction → one `SendBatch`.

Acceptance: round-trip census (claim/settle) drops from ~11/~12 to ~7/~8;
re-run the plan-5 benchmark and record the new cmd/s in
`benchmark_evidence/`.

### 4.4 Phase 4 — maintenance fan-out + expiry N+1

- Page processing uses the `claimRunGroups` bounded-concurrency pattern,
  grouped by run.
- `expireRunLocked` replaces the per-command `FOR UPDATE` loop with one
  `= ANY($1::uuid[])` query (same pattern as `lockClaimBatch`,
  `commands.go:436-447`).

Acceptance: recovering 128 orphaned leases takes one maintenance pass and
completes in <100ms on localhost *(estimate; measure)*; expiring a
100-command run issues O(1) queries for the delivery reads.

### 4.5 Phase 5 — probe efficiency

- `limit = min(free, maxCommandProbe)` (drop the `free*4` inflation) when
  registered kinds exceed a threshold.
- Extend the probe to accept `(queue, remaining_capacity)` pairs and known
  busy run IDs so saturated lanes and locked runs are filtered in SQL, not
  discovered by failed claims and re-probes.
- Evaluate the global-order partial index
  `(next_run_at, queue, command_id) INCLUDE (name, version, run_id)
  WHERE state IN ('ready','retry_wait')` against the per-kind lateral; adopt
  whichever the benchmark favors.

### 4.6 Phase 6 — allocation pass (mechanical)

Cache `MaxConns` at `New`; build all SQL strings once in `store.New`;
precompute `replicaName`; short-circuit `observe` for `NopObserver`;
`bytes.Compare` UUID sort in `graph.go:421`; inline single-group claim.

### 4.7 Phase 7 — projection-only trace

Public read that returns run head + operational projections (current
command states, waits, attempt counters) without paging or folding the
journal. Contract documented for external orchestrators whose progress
endpoints poll many live runs (see trails-api plan 011).

### 4.8 Phase 8 — wait-expiry index predicate

`AND unsatisfied_waits > 0` added to `flow_commands_wait_deadline_idx`
(implied by `flow_commands_pending_shape_ck` already). Ship as an additive
migration; verify index-only scans in `EXPLAIN`.

## 5. Verification

- Every phase: `go test ./... -race`, plus the durable-validation suite.
- Phases 1/3/4/5: re-run the plan-5 hot-path benchmark; append results to
  `specs/projects/flow/benchmark_evidence/`.
- Phase 1 and 2 add latency-focused tests (fake clock where possible;
  wall-clock bounds where not, generous enough for CI).
- No phase may change any `flow_journal` entry shape, fence semantics, or
  recovery ordering — assert via existing replay/consistency tests.

## 6. Dependency and rollout

Phases are independent except 4.5's SQL changes build on 4.1's probe
signature. Suggested order: 1, 2, 3+6 (one PR), 4, 5, 7, 8, 9. Each phase is
a separate PR against `master`, benchmarked before merge. No consumer-facing
API changes except the additive 4.7 read (and none require trails-api
changes to keep working).
