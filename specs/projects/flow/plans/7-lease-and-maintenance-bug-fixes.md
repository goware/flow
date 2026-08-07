# Plan 7: Fix lease renewal safety and maintenance backlog recovery

Status: Complete

Planned at: `411ec71` on 2026-08-07

Completed on: 2026-08-07 in the `fixes` worktree based on `411ec71`

- **Branch:** `fixes`
- **Priority:** P1 for lease correctness; P2 for maintenance recovery latency
- **Effort:** M
- **Risk:** MEDIUM; the renewal SQL and local-deadline lifecycle require careful
  PostgreSQL and race testing
- **Depends on:** none; `411ec71` and `hardening-2` at `ccc8682` have identical
  source trees
- **Public API impact:** none; only additive, low-cardinality observation values
  may be documented

> **Executor instructions:** Read this plan completely before editing. Work in
> the phase order below and run each phase's focused gate before continuing. Do
> not use this plan as permission for adjacent performance, schema, retention,
> or API changes. If a STOP condition occurs, stop and report it rather than
> weakening Flow's attempt fence or at-least-once contract.
>
> **Drift check (run first):**
>
> ```text
> git diff --stat 411ec71..HEAD -- \
>   runtime.go runtime_run.go runtime_run_test.go command_runtime.go \
>   command_runtime_test.go claim_test.go maintenance_fault_test.go \
>   observer.go internal/store/commands.go README.md doc.go \
>   specs/projects/flow/architecture.md \
>   specs/projects/flow/components/runtime.md
> ```
>
> If these paths changed, compare the live implementation with Section 4 before
> proceeding. Reconcile harmless line movement, but treat changed lease/fence or
> maintenance semantics as a STOP condition.

## 1. Purpose

This is the deliberately small bug-fix subset of Plan 6. It fixes confirmed
failure- and contention-path defects in lease handling and bounded maintenance:

1. one locked queue row or a starved connection pool can block the complete
   lease-renewal batch;
2. while that call is blocked, the same loop that should enforce local lease
   expiry cannot run;
3. claim and renewal round-trip time is added to the local lease deadline,
   allowing cooperative work to continue longer than the runtime's conservative
   durable window;
4. a failed renewal emits both `error` and an unconditional false `ok`;
5. maintenance waits a full poll interval after every full page even when it
   successfully drained that page, multiplying recovery latency during a burst;
   and
6. maintenance probe/transition failures are silently discarded, making a
   stuck recovery loop hard to distinguish from an empty one.

These defects do **not** mean that two workers can durably settle the same
command. Flow already fences settlement with the execution lock, attempt ID,
lease token, queue state, and lease expiry. A stale handler result is rejected.
The lease fixes reduce avoidable overlap and make cooperative cancellation
prompt under contention; they do not promise exactly-once handler execution or
exactly-once remote side effects.

## 2. Why each fix is necessary

### 2.1 A blocked renewal can outlive the safe local window

`runtime_run.go:364-400` snapshots every active attempt and makes one store call
using the long-lived service context. `internal/store/commands.go:738-748`
directly updates all matching queue rows without `SKIP LOCKED`. PostgreSQL can
therefore hold the entire statement behind one row locked by settlement,
cancellation, or another supported Flow transaction. Pool acquisition can also
wait indefinitely until the service context is stopped.

The runtime calls `active.cancelExpired()` only before or after this same store
call. If the call is blocked, local expiry enforcement is blocked too. Durable
fencing still protects Flow state, but a cancellation-cooperative handler may
continue running and performing non-transactional effects after another replica
becomes eligible to take over.

### 2.2 A skipped row is not always proof of ownership loss

Adding `SKIP LOCKED` alone would create a new bug. A still-current row can be
temporarily locked by a legitimate short settlement transaction. Such a row is
**uncertain**, not definitely lost. Cancelling it immediately could abort a
valid `WithCommit` callback.

Renewal must return one typed result for every request:

- `renewed`: the exact running/unexpired fence was locked and extended;
- `lost`: the committed statement snapshot shows no row, a non-running row, a
  different attempt/token, a missing expiry, or an already expired lease; or
- `uncertain`: the snapshot still looks like the requested live fence, but the
  row could not be locked and renewed.

Only `lost` cancels the exact matching local attempt immediately. `uncertain`
keeps the previous local deadline without extending it. A query error changes
no local deadline. The independent watchdog resolves elapsed local deadlines.

### 2.3 Local deadlines currently include database delay

`command_runtime.go:395` constructs the claim-local deadline after the claim
returns:

```go
time.Now().Add(claim.LeaseExpiresAt.Sub(claim.DBNow))
```

`runtime_run.go:396` advances a renewed command after the renewal response:

```go
time.Now().Add(r.commandLease)
```

Both calculations add pool/query round-trip time to the local view. Capture a
local monotonic timestamp before each database call and anchor the deadline to
that timestamp. Early local cancellation is conservative and safe; deliberately
extending beyond the known window is not.

### 2.4 Renewal telemetry contradicts itself

At `runtime_run.go:388-400`, an error emits `renew/error`, then execution falls
through to an unconditional `renew/ok`. This is a direct observability bug: an
alert or metric consumer can see the failed pass as successful. Each pass must
emit exactly one terminal `renew` outcome.

### 2.5 Full maintenance pages pay the poll interval repeatedly

`runtime_run.go:404-456` probes bounded pages of 64 expired executions, 128
expired waits, and 128 expired leases once per ticker turn. Bounded pages are
correct, but a full page that committed useful transitions waits another full
poll interval before the next page. With the one-second default, a 1,024-lease
crash backlog pays at least eight polling intervals in addition to transaction
time.

The fix is bounded catch-up, not larger pages or parallel maintenance: promptly
run another pass only after a full page made progress, cap consecutive prompt
passes, yield, and return to the ordinary interval on a full no-progress page.

## 3. Hard boundaries

### In scope

- `internal/store/commands.go` — exact-fence lease selection, update, and typed
  per-request classification.
- `runtime_run.go` — active-attempt state, bounded renewal call, independent
  watchdog, truthful observations, and bounded maintenance continuation.
- `command_runtime.go` — conservative claim-local lease anchor.
- `claim_test.go`, `runtime_run_test.go`, `command_runtime_test.go`, and
  `maintenance_fault_test.go` — focused PostgreSQL, lifecycle, observer, and
  race regressions.
- `README.md`, `doc.go`, `specs/projects/flow/architecture.md`, and
  `specs/projects/flow/components/runtime.md` — only the behavior changed by
  this plan.

### Explicitly out of scope

- retention, archival, purge, partitioning, or deletion design/code;
- `GetQueueDepth`, queue-depth indexes, migrations, or any schema change;
- broad throughput rewrites or new claim/scheduler architecture;
- a long-running randomized soak or a new benchmark evidence project;
- parallel maintenance workers or new database connection budgeting;
- settlement retry/exhaustion changes unrelated to lease expiry;
- a public command-lease or renewal-timeout option;
- new tables, columns, indexes, constraints, or journal formats;
- weakening attempt ID/token checks, execution-first locking, or `WithCommit`
  atomicity; and
- any claim of exactly-once handler bodies or remote effects.

## 4. Current state and invariants

### 4.1 Active attempts

`runtime_run.go:69-151` stores one process-local active attempt per command. It
matches delayed results by attempt ID, which must be preserved. The current
`cancelExpired` and `cancelUnrenewed` may invoke cancellation repeatedly and a
cancelled entry remains eligible for a later renewal snapshot.

Target the smallest safe state machine:

```text
active -> renewed (same attempt only)
active -> locally cancelled (lost, elapsed, or shutdown; once only)
locally cancelled -> unregister when handler lifecycle ends
```

Add an internal `cancelled` marker or equivalent. Snapshots must exclude locally
cancelled attempts so the renewal manager cannot extend durable ownership after
the watchdog has already stopped local work. `renewed`, `cancelLost`,
`cancelExpired`, `unregister`, and `cancelAll` must all compare the exact attempt
ID where applicable.

### 4.2 Durable renewal

`internal/store/commands.go:710-764` currently accepts parallel arrays and
returns only rows updated by a direct `UPDATE`. Preserve one set-oriented round
trip, duration validation, schema qualification, and safe error mapping.

The target statement should have these operation-specific CTEs:

```sql
WITH now_value AS (
    SELECT clock_timestamp() AS now
), requested(command_id, attempt_id, token, ordinal) AS (
    SELECT command_id, attempt_id, token, ordinality
    FROM unnest($1::uuid[], $2::uuid[], $3::uuid[])
         WITH ORDINALITY AS input(command_id, attempt_id, token, ordinality)
), observed AS MATERIALIZED (
    SELECT r.command_id AS requested_command_id,
           r.attempt_id AS requested_attempt_id,
           r.token AS requested_token,
           r.ordinal,
           q.command_id, q.state, q.active_attempt_id,
           q.lease_token, q.lease_expires_at
    FROM requested r
    LEFT JOIN flow_command_queue q ON q.command_id = r.command_id
), lockable AS (
    SELECT q.command_id
    FROM flow_command_queue q
    JOIN requested r ON r.command_id = q.command_id
    CROSS JOIN now_value n
    WHERE q.active_attempt_id = r.attempt_id
      AND q.lease_token = r.token
      AND q.state = 'running'
      AND q.lease_expires_at > n.now
    FOR UPDATE OF q SKIP LOCKED
), renewed AS (
    UPDATE flow_command_queue q
    SET lease_expires_at = n.now + ($4 * interval '1 millisecond')
    FROM lockable l, now_value n
    WHERE q.command_id = l.command_id
    RETURNING q.command_id, q.lease_expires_at
)
SELECT ... one ordered classification for every requested row ...
```

Use `observed` only to distinguish definite snapshot loss from a still-current
but un-lockable fence. Validate non-zero IDs/tokens, reject duplicate command
IDs, retain input order with ordinality, validate every returned outcome, and
require exact result count and identity equality with the request.

### 4.3 Runtime lifecycle

`Runtime.Run` currently owns the scheduler, lease manager, maintenance, and
optional listener. Add exactly one runtime-wide watchdog service. It must use
the existing service context, join before `Run` returns, perform no SQL, and use
one ticker rather than one goroutine/timer per command.

The production lease remains 60 seconds. The in-package test seam enforces a
minimum 30-millisecond lease. A suitable internal renewal-call timeout is:

```text
max(10 ms, min(5 seconds, commandLease / 6))
```

It is below every accepted lease and capped for production. A suitable watchdog
interval is:

```text
max(10 ms, min(1 second, commandLease / 6))
```

### 4.4 Maintenance safety

Maintenance probes are hints. Every transition already revalidates under the
execution lock; preserve that. Keep page sizes and category order unchanged.
Do not batch unrelated executions into one semantic transaction.

Extract one pass returning at least:

```go
type maintenancePassResult struct {
    progressed bool
    saturated  bool
}
```

`saturated` means at least one category returned its full page. `progressed`
means at least one candidate committed a transition. Visit execution deadlines,
wait deadlines, and expired leases on every pass unless the service context is
cancelled.

## 5. Implementation phases

### Phase 1: Make renewal results exact and observations truthful

1. In `internal/store/commands.go`, replace the updated-row-only return type
   with an internal typed outcome and a result containing command ID, attempt
   ID, outcome, and optional renewed expiry.
2. Implement the single-statement `observed`/`lockable`/`renewed` shape from
   Section 4.2 using `FOR UPDATE OF q SKIP LOCKED`.
3. Validate inputs and require one ordered result per request. Unknown,
   duplicated, missing, or malformed result identity is `ErrInvalidState`, not
   a partial success.
4. In `runtime_run.go`, apply outcomes exactly:
   - renewed: advance only the same active attempt;
   - lost: cancel only the same active attempt once;
   - uncertain: do not cancel and do not extend;
   - store/fault error: mutate no local deadline.
5. Emit exactly one aggregate `renew` outcome per call: `ok` when all renew,
   `partial` when any result is lost/uncertain, and `error` on call failure.
   Add bounded aggregate `renew_result` observations for renewed/lost/uncertain
   counts if needed. Never expose attempt IDs, lease tokens, SQL, or raw errors.

**Focused tests:**

- one locked requested row is `uncertain`, an unrelated row is `renewed`, and
  the call completes without waiting for the lock;
- absent, terminal, mismatched, and expired fences are `lost`;
- duplicate/incomplete requests are rejected before SQL;
- a late old-attempt result cannot renew or cancel a replacement attempt;
- a forced renewal error produces one `renew/error` and zero `renew/ok` events;
- locally cancelled entries are not included in later renewal snapshots.

**Verify:**

```text
make test TEST="'Test.*(RenewCommandLeases|LeaseRenewalResult|LeaseRenewalError)'" \
  TEST_FLAGS='-count=10 -p 1 -parallel 4'
git diff --check
```

Expected: all selected tests run against PostgreSQL, pass ten times under the
race detector, and the observer regression sees no contradictory success.

### Phase 2: Bound renewal and enforce conservative local expiry independently

1. Derive a per-call timeout from Section 4.3 around both the renewal fault seam
   and store call. A timeout is an error, not proof that every attempt was lost.
2. Add one runtime-owned watchdog service that periodically calls the active-map
   elapsed-deadline cancellation helper even while renewal is blocked waiting
   for a connection or SQL.
3. Mark local cancellation once and exclude cancelled attempts from subsequent
   renewal snapshots. Preserve handler-owned unregister and runtime shutdown.
4. Capture the local timestamp immediately before `ClaimCommands`. Carry a
   private, non-persisted deadline with every returned or ambiguously retained
   claimed command:

   ```text
   claim call start + (LeaseExpiresAt - DBNow)
   ```

   Use the database timestamps only as a duration; do not assume database and
   application host clocks match.
5. Capture the local timestamp immediately before renewal. A successful result
   advances the exact active attempt no later than:

   ```text
   renewal call start + commandLease
   ```

   Never use response time plus a full lease.
6. Ensure watchdog and renewal goroutines stop and join through the existing
   runtime service lifecycle before observation delivery closes.

**Focused tests:**

- hold all connections in a two-connection test pool after a handler starts;
  renewal times out, the watchdog cancels by the prior local deadline, the
  runtime drains after pool release, and a second replica safely takes over;
- hold one queue row through a blocked short `WithCommit`; it is uncertain,
  unrelated work renews, and it is not cancelled merely because the row was
  skipped;
- hold an uncertain row beyond its old deadline; the watchdog cancels once and
  a later renewal cannot revive it;
- prove claim and renewal local deadlines are anchored to call start, including
  the ambiguous-claim handoff path;
- prove definite fence loss cancels only the exact old attempt;
- stop a runtime during an in-flight bounded renewal and prove all services and
  workers drain without a goroutine or `WaitGroup` race;
- retain existing takeover assertions: at most one accepted settlement,
  attempt journal entries remain coherent, and replay matches live state.

**Verify:**

```text
make test TEST="'Test.*(LockedSettlement|PoolStarvation|LeaseWatchdog|Takeover|Ambiguous|Shutdown)'" \
  TEST_FLAGS='-count=10 -p 1 -parallel 4'
make test TEST="'TestLeaseRenewalResultCannot.*Snapshot'" \
  TEST_FLAGS='-count=10 -p 1 -parallel 4'
git diff --check
```

Expected: all selected tests pass ten race-enabled runs; pool starvation cannot
delay local cancellation past the conservative deadline; only the current
fence can settle.

### Phase 3: Drain full maintenance pages promptly but remain bounded

1. Name the existing page sizes as internal constants; do not enlarge them.
2. Extract one pass as described in Section 4.4. Factor category helpers only
   as far as needed to aggregate progress/errors and keep the pass readable.
3. After a pass that is both full and progressed, schedule a prompt follow-up
   (for example, after 1 ms rather than a full poll interval).
4. Cap prompt passes at eight, then perform a small context-aware yield (for
   example, 25 ms) before continuing. A full page with zero progress returns to
   the ordinary poll interval immediately. These values remain internal.
5. Always visit all three categories during a pass. A failure in one category
   must be observed and must not silently skip unrelated categories.
6. Emit bounded aggregate observations for category probe/transition errors and
   for saturated `drain` versus `blocked` passes. Avoid a per-candidate event and
   avoid emitting three empty-success observations every poll.
7. Signal the scheduler wake hub when at least one transition committed, as the
   current implementation does.

**Focused tests:**

- 65 expired executions require two structural passes: the first is full and
  progressed; the second drains the final candidate;
- a full page locked by a test transaction is saturated with zero progress and
  requests no prompt spin;
- a backlog in one category does not prevent a ready candidate in each other
  category from being processed;
- two runtime replicas processing the same backlog remain idempotent;
- cancellation of the service context ends a prompt drain turn promptly;
- a forced probe/transition error is observed, later recovery succeeds, and no
  payload, SQL, token, or raw database error is exposed.

Use structural pass/progress assertions. Wall-clock limits may guard against a
hang but must not be the sole proof of behavior.

**Verify:**

```text
make test TEST="'Test.*Maintenance.*(Page|Backlog|Locked|Replica|Shutdown|Fault)'" \
  TEST_FLAGS='-count=10 -p 1 -parallel 4'
git diff --check
```

Expected: all selected tests pass under the race detector; progressed full
pages continue promptly; a locked stable page falls back without busy-spinning.

### Phase 4: Synchronize only affected documentation and run final gates

Update the four in-scope documentation surfaces only where necessary. State:

- workers remain at-least-once, while accepted settlement remains fenced;
- renewal calls are bounded and one locked row cannot block unrelated renewal;
- a skipped current fence is uncertain, not automatically lost;
- errors and uncertain results do not extend local deadlines;
- one independent runtime watchdog cancels cooperative work at conservative
  local expiry;
- cancellation cannot stop handlers that ignore their context, and external
  effects still need idempotency keys; and
- maintenance catches up full progressed pages in bounded prompt passes and
  falls back on no progress.

Do not add retention, indexing, capacity-planning, or benchmark claims.

Run final gates in this order:

```text
gofmt -w runtime_run.go runtime_run_test.go command_runtime.go \
  command_runtime_test.go claim_test.go maintenance_fault_test.go \
  internal/store/commands.go doc.go
git diff --check
make build
go vet ./...
go test -count=1 -p 1 -parallel 4 ./...
make test
```

Then repeat the combined focused set:

```text
make test \
  TEST="'Test.*(Renew|Lease|Takeover|Maintenance|Ambiguous|Shutdown)'" \
  TEST_FLAGS='-count=10 -p 1 -parallel 4'
```

Expected: formatting/diff/build/vet pass; the exact ordinary suite passes with
database-backed tests active; the full race suite and repeated focused race set
pass with no unexpected skips.

## 6. Acceptance criteria

1. Lease renewal remains one set-oriented database round trip and uses exact
   command/attempt/token/state/expiry predicates.
2. The renewable subset is selected with `FOR UPDATE OF q SKIP LOCKED` before
   update, so one locked row cannot block unrelated renewals.
3. The store returns exactly one ordered typed `renewed`, `lost`, or `uncertain`
   result for every valid request and rejects malformed/duplicate inputs.
4. A still-current but un-lockable row is uncertain: it is neither cancelled
   immediately nor locally extended.
5. A definitely absent, terminal, mismatched, malformed, or expired fence is
   lost and cancels only the exact matching local attempt.
6. A renewal error/timeout changes no local deadline and never emits `ok` for
   that call.
7. Every renewal call has an internal timeout below the accepted lease window.
8. One runtime-wide, SQL-free watchdog continues checking local expiry while
   renewal is blocked and joins cleanly at shutdown.
9. Locally cancelled attempts are cancelled once and are not renewed again.
10. Claim-local expiry is anchored to claim-call start; renewal-local expiry is
    anchored to renewal-call start.
11. A late old-attempt result cannot renew or cancel a newer attempt for the
    same command.
12. Existing execution-lock and attempt-fence behavior still permits at most
    one accepted durable settlement, including takeover and ambiguity tests.
13. A full progressed maintenance page requests bounded prompt continuation.
14. A full no-progress maintenance page returns to normal polling without a
    busy loop.
15. Deadline, wait-expiry, and lease-recovery categories remain serviced and
    their failures are observable independently.
16. No migration, schema, exported orchestration API, journal format, retry
    budget rule, or durability setting changes.
17. All focused repeated race gates and all final repository gates pass without
    unexpected database-test skips.

## 7. STOP conditions

Stop and report rather than improvising if:

- the live source has changed the attempt fence, execution-first lock, active
  map, renewal query, or maintenance ownership assumptions in this plan;
- PostgreSQL cannot provide one trustworthy per-request classification in one
  statement without weakening exact fence predicates;
- a skipped current row cannot be distinguished conservatively from definite
  committed ownership loss;
- the proposed watchdog needs one goroutine/timer per command;
- local deadline logic would require comparing host and PostgreSQL absolute
  clocks;
- a stale attempt can commit a Flow result, staged event/child, terminal state,
  or `WithCommit` write after takeover;
- maintenance continuation can spin indefinitely on a locked prefix, starve a
  category, or requires unbounded parallelism;
- a fix appears to require a migration, public lease option, new durable state,
  or another item listed as out of scope;
- unrelated user changes overlap an in-scope file and cannot be preserved; or
- a focused or full gate fails twice after a reasonable scoped correction.

## 8. Review focus

The final review should concentrate on:

- PostgreSQL statement-snapshot behavior around `observed`, `SKIP LOCKED`, and
  the update CTE;
- exact one-result-per-request and input-order validation;
- the distinction between definite `lost` and conservative `uncertain`;
- monotonic local deadline anchoring without a host/database clock assumption;
- old-attempt/new-attempt races and the locally-cancelled state;
- watchdog/renewal shutdown ownership;
- absence of contradictory or high-cardinality observations; and
- maintenance prompt-pass bounds and category fairness.

## 9. Punchlist

### Renewal correctness

- [x] Validate non-zero renewal IDs/tokens and reject duplicate command IDs.
- [x] Select exact renewable fences with `FOR UPDATE OF q SKIP LOCKED`.
- [x] Materialize observed fence state and return one ordered typed outcome per
  request.
- [x] Treat still-current skipped rows as uncertain, not lost.
- [x] Cancel only exact definitely lost attempts; leave uncertain/error
  deadlines unchanged.
- [x] Ensure locally cancelled attempts are cancelled once and excluded from
  later renewal snapshots.
- [x] Emit exactly one truthful aggregate renewal outcome per call.
- [x] Add locked-row, definite-loss, malformed-input, replacement-attempt, and
  observer regressions.

### Conservative local expiry

- [x] Add an internal bounded timeout around each renewal call.
- [x] Add one runtime-wide local-expiry watchdog and join it at shutdown.
- [x] Anchor claim-local expiry to claim-call start.
- [x] Anchor renewal-local expiry to renewal-call start.
- [x] Prove pool starvation cannot delay cooperative local cancellation.
- [x] Prove locked `WithCommit`, takeover, ambiguity, and shutdown behavior
  retain exactly-one accepted durable settlement.

### Maintenance recovery

- [x] Extract a testable maintenance pass with progressed/saturated state.
- [x] Promptly continue only full progressed pages.
- [x] Cap consecutive prompt passes and yield before another turn.
- [x] Fall back to ordinary polling on a full no-progress page.
- [x] Continue servicing all three categories when one category is backlogged or
  errors.
- [x] Emit bounded maintenance error/drain/blocked observations.
- [x] Add multi-page, locked-page, category-fairness, replica-idempotency, and
  shutdown regressions.

### Final verification

- [x] Update only the affected lease/maintenance documentation.
- [x] Confirm no schema, migration, exported API, journal, or durability change.
- [x] Pass gofmt, diff check, build, vet, exact ordinary PostgreSQL, full race,
  and repeated focused race gates with no unexpected skips.
- [x] Review the final diff against Section 3 and mark this plan complete only
  when all 17 acceptance criteria pass.
