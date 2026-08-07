# Plan 6: Harden lease ownership, recovery, observability, and retained data operations

Status: Planned

Planned at: `ccc8682` on 2026-08-07

- **Priority:** P1 for lease/observation correctness; P2 for maintenance and
  retention design; evidence-gated for the queue-depth index
- **Overall effort:** L, delivered as independently reviewed phases
- **Overall risk:** MEDIUM; renewal classification and watchdog lifecycle are
  the highest-risk implementation areas
- **Depends on:** completed Plan 5 hot-path work at `ccc8682`
- **Public API impact:** no exported type or function changes; observation
  operation/outcome values may be additively documented, retention remains
  design-only, and the optional index is internal storage

> **Executor instructions:** Read this complete plan before changing code. Work
> through the phases in order, run every phase-specific verification gate, and
> keep each phase independently reviewable. If a STOP condition occurs, stop
> and report it rather than weakening Flow's execution lock, attempt fence,
> transaction atomicity, idempotency, or replay contract.
>
> **Drift check:** Before implementation, run:
>
> ```text
> git diff --stat ccc8682..HEAD -- \
>   runtime.go runtime_run.go runtime_run_test.go observer.go \
>   command_runtime.go command_runtime_test.go claim_test.go \
>   maintenance_fault_test.go hardening_benchmark_test.go \
>   internal/store/commands.go internal/store/inspection.go \
>   internal/store/*_test.go inspection.go inspection_test.go \
>   migrations migrations.go migrations_test.go \
>   README.md doc.go specs/projects/flow
> ```
>
> If an in-scope lease, claim, settlement, maintenance, queue-depth, migration,
> or retention path has changed, compare the live implementation with the
> current-state excerpts below. A semantic mismatch is a STOP condition until
> this plan is reconciled with the new source.

## 1. Summary

Plan 5 removed the dominant avoidable work from Flow's command and event hot
paths. On the final durability-enabled PostgreSQL benchmark host, independent
small-command lifecycles reached roughly 400-460 commands per second under
concurrency, 100 commands in one execution completed at roughly 175 commands
per second, and a 16-command same-execution claim transaction reached roughly
2,500 claimed commands per second. Those results are strong enough that the
next work should not be another broad throughput rewrite.

The remaining high-value work is operational hardening around the edges of the
lease and retention model:

1. a renewal request for every locally active attempt is currently issued as
   one unbounded database call, so one blocked row or an exhausted pool can
   delay the complete renewal batch;
2. local lease expiry is extended from the time a database response is received
   rather than from a conservative point tied to the durable renewal window;
3. local expiry cancellation is checked by the same loop that can block in the
   renewal call;
4. lease and maintenance observations can hide or misreport failures;
5. maintenance processes bounded pages but waits for another poll even when a
   large, recoverable backlog remains;
6. the journal, payloads, commands, and execution projections are retained for
   the lifetime of an execution, so storage—not CPU—will dominate sustained
   high-volume use; and
7. `GetQueueDepth` has an intentionally simple aggregation but no index led by
   `queue`, which should be measured at operational scale before adding another
   write-amplifying hot-table index.

This plan preserves the current public orchestration grammar and durable model.
It does not attempt to make worker bodies exactly once. It makes lease-loss
behavior more prompt and observable, proves that stale work cannot be accepted,
improves recovery from burst failures, and produces an explicit retention
contract before any destructive data API is authorized.

## 2. Why this work is worth doing

### 2.1 Durable progression is fenced, but handler execution is at least once

Flow already provides the correct distributed invariant:

```text
one durable active fence -> one accepted settlement
```

It does not and cannot promise:

```text
one handler body is ever executing anywhere
```

A process can pause after an external effect, lose its lease, and resume after
another replica has taken over. An ambiguous claim result can also require the
runtime to conservatively execute a possibly committed attempt. The execution
row lock, queue state, attempt ID, lease token, and settlement fence ensure the
stale result cannot mutate durable Flow state. Remote or non-transactional
effects still require stable application idempotency keys.

That distinction is documented and tested. Plan 6 must not disguise it by
claiming exactly-once work. Its concurrency goal is narrower and useful: do not
create avoidable overlap by allowing local lease knowledge to lag behind the
durable lease, and detect ownership trouble quickly enough that cooperative
handlers stop before performing more work.

### 2.2 One blocked renewal must not endanger unrelated work

At `ccc8682`, `runLeaseManager` snapshots every active command and calls one
set-oriented renewal query:

```go
// runtime_run.go:374-400
current := r.active.snapshot()
renewals := make([]store.LeaseRenewal, len(current))
// ...
renewed, err := r.store.RenewCommandLeases(ctx, renewals, r.commandLease)
```

The store updates every requested queue row in one statement:

```sql
-- internal/store/commands.go:738-748
WITH now_value AS (...), requested(...) AS (...), renewed AS (
    UPDATE flow_command_queue q
    SET lease_expires_at = ...
    FROM requested r, now_value n
    WHERE q.command_id = r.command_id
      AND q.active_attempt_id = r.attempt_id
      AND q.lease_token = r.token
      AND q.state = 'running'
      AND q.lease_expires_at > n.now
    RETURNING ...
)
SELECT ... FROM renewed
```

There is no `SKIP LOCKED` selection and the runtime supplies its service context
without a per-renewal timeout. A settlement, cancellation, or caller-owned
transaction holding one queue row can therefore delay the statement. Connection
pool exhaustion can delay it before SQL begins. The usual local PostgreSQL case
will finish quickly, but a lease mechanism must be designed for the unhealthy
case, not only the median.

The fix is worth doing because its normal-path cost is small: retain one
set-oriented query, select renewable rows with `FOR UPDATE SKIP LOCKED`, classify
non-renewed rows without guessing that every lock means ownership loss, bound the
call, and run local expiry cancellation independently. This preserves the fast
path while preventing one attempt from becoming a renewal head-of-line blocker
for unrelated executions.

### 2.3 Local expiry must be conservative

Claims correctly carry PostgreSQL's accepted time and durable lease expiry, but
the runtime currently constructs its local deadline after receiving the claim:

```go
// command_runtime.go:395-398
localLeaseExpiry := time.Now().Add(max(0, claim.LeaseExpiresAt.Sub(claim.DBNow)))
```

Successful renewal currently advances it after the renewal response:

```go
// runtime_run.go:394-397
r.active.renewed(lease.CommandID, attemptedCommands[lease.CommandID],
    time.Now().Add(r.commandLease))
```

Both shapes add database/pool round-trip time to the runtime's local view. The
durable settlement fence still rejects a stale result, but the old worker may
remain cooperative for longer than the database lease. The local deadline must
never intentionally be later than the conservative durable window known to the
runtime.

### 2.4 Operational errors must be visible and truthful

The current renewal loop emits an error observation and then unconditionally
emits an `ok` observation for the same failed call:

```go
// runtime_run.go:388-400
if err != nil {
    r.observe(... Outcome: "error" ...)
} else {
    // ...
}
r.observe(... Outcome: "ok" ...)
```

Maintenance probe and transition errors are generally ignored so that the next
poll can retry. That is safe for correctness, but silent operational failure is
not acceptable for diagnosis. Similarly, a claim conclusion or success
settlement that exhausts all three internal attempts can return without one
final observation explaining that the attempt remains for lease recovery.

These are inexpensive fixes. They do not expose SQL, payloads, errors, or lease
tokens. They reuse the existing bounded `Observation` structure and stable
operation/outcome strings.

### 2.5 Burst recovery should not be poll-interval multiplied

Maintenance currently probes at most 64 expired executions, 128 expired waits,
and 128 expired command leases on each poll. It then processes each candidate
sequentially and waits for the next ticker:

```go
// runtime_run.go:404-456
if ids, err := r.store.ProbeExpiredExecutions(ctx, 64); err == nil { ... }
if waits, err := r.store.ProbeExpiredCommandWaits(ctx, 128); err == nil { ... }
if leases, err := r.store.ProbeExpiredCommandLeases(ctx, 128); err == nil { ... }
```

The bounded pages are good. The avoidable cost is waiting a complete poll
interval after successfully draining a full page. A process crash can leave
hundreds or thousands of leases expiring together. With a one-second poll, 1,000
expired leases require at least eight polling turns before transaction time is
counted.

Plan 6 adds bounded immediate follow-up passes when the preceding pass actually
made progress and returned a full page. It does not add unbounded loops or
unbounded maintenance goroutines. Independent-execution parallelism is deferred
unless measurements show sequential transition time remains the bottleneck.

### 2.6 Retention is the dominant data-scale concern

The final Plan 5 evidence retained approximately 1,996 bytes of journal tuple
data per small command, before journal indexes, command/execution projections,
TOAST overhead, WAL, backups, or replicas. At 400 commands per second, the
arithmetic alone is approximately 69 GB of journal tuples per day. This is not
a claim that a consumer desktop will sustain that load indefinitely; it is a
capacity warning that retained data becomes the constraint before Go CPU does.

Flow currently exposes no supported journal pruning or archival API. Direct
deletion is explicitly unsupported because it can break:

- permanent execution-key idempotency;
- root/parent and same-execution ownership constraints;
- event wait satisfying positions;
- history, trace, replay, and causation;
- terminal mutation idempotency; and
- projection/journal conformance.

Retention therefore needs an explicit product contract, not a cleanup query
hidden in a performance change. This plan produces that contract and supporting
size evidence. It does not authorize deleting production Flow rows.

### 2.7 Queue-depth indexing must pay for its write cost

`GetQueueDepth` performs one aggregate over `flow_command_queue WHERE queue=$1`
and classifies ready, delayed, and running rows. The current indexes begin with
handled command kind, lease expiry, execution ID, or command ID—not `queue`.
Large mixed-queue tables may therefore make a selective queue-depth read scan
more rows than necessary.

The queue table is also Flow's hottest mutable table. Adding
`(queue,state,next_run_at)` or a covering alternative would be rewritten on
claim, retry, recovery, and state changes. Plan 6 first creates a realistic
benchmark and query-plan comparison. An index is added only when its measured
read benefit justifies its size and write cost.

## 3. Controlling decisions and invariants

### 3.1 Preserve execution-first semantic locking

Every semantic mutation of an existing execution continues to acquire
`flow_executions ... FOR UPDATE` first. Claims and maintenance use no-wait
`SKIP LOCKED` acquisition where blocking would harm fairness. Caller-owned
transactions continue to request multiple executions in ascending execution-ID
order before application-table writes.

Do not replace the execution lock with advisory locks, optimistic counters,
`SERIALIZABLE` retries, or fine-grained journal allocation. Same-execution
serialization is the authority for journal position, counters, failure state,
terminality, event readiness, and `WithCommit` atomicity.

### 3.2 Preserve claim and settlement fencing

A command claim must still atomically:

1. lock the execution;
2. lock eligible command/queue rows with `SKIP LOCKED`;
3. revalidate exact name/version, state, schedule, execution status, deadline,
   retry elapsed budget, and event inputs;
4. append one `attempt_started` journal entry per accepted command;
5. install a fresh attempt ID and lease token on the queue projection; and
6. commit before invoking application code.

Settlement must still reacquire the execution and command/queue fence and
verify current state, attempt ID, lease token, and lease validity before any
result, staged event, staged child, application commit callback, or terminal
projection can commit.

No new uniqueness constraint is needed to prevent ordinary duplicate claims.
The queue row and execution lock are the live ownership authority; the existing
attempt-kind journal uniqueness is retained as durable corruption defense.

### 3.3 Preserve at-least-once semantics honestly

Plan 6 may reduce avoidable stale-worker overlap, but it must retain and
document these facts:

- a worker that ignores cancellation may continue running after lease loss;
- a takeover worker may execute concurrently with that stale worker;
- only the current durable fence may settle;
- `WithCommit` makes accepted same-database writes atomic with settlement, but
  its callback body can be re-entered after a rolled-back/ambiguous attempt; and
- remote effects require application idempotency.

Tests must assert durable outcomes and attempt history, not assume a stale
goroutine instantly stops.

### 3.4 Preserve bounded resource use

The implementation must retain:

- bounded process-local handler slots;
- bounded concurrent claim transactions derived from worker and pool capacity;
- at least the existing maintenance connection headroom;
- one optional notification connection outside the application pool;
- bounded probe pages and continuation state; and
- bounded observer delivery that cannot block correctness.

Lease hardening must not create one goroutine per renewal attempt, an unbounded
retry loop, or a second unbounded database client.

### 3.5 Preserve the current durable schema until evidence authorizes change

The six current table roles remain. Retention design may recommend a later
schema/API project, but this plan does not merge tables or delete history.

If the optional queue-depth experiment justifies a new index, add it through a
new forward migration and matching compatibility/checksum tests. Do not rewrite
the already verified `001_initial.sql` or `002_live_keys.sql` as part of this
post-Plan-5 operational work.

### 3.6 Do not turn observations into a secret-bearing error channel

New observations may include bounded operation name, outcome, count, duration,
execution/command identity already allowed by the public structure, queue,
worker, and occurrence time. They must not contain:

- raw database or driver error strings;
- SQL or relation names derived from errors;
- arguments, results, event payloads, metadata, or canonical bodies;
- connection objects or backend identifiers; or
- attempt IDs, lease tokens, or external credentials.

## 4. Goals

Plan 6 is complete when:

1. one locked queue row cannot block renewal of unrelated active attempts;
2. every renewal database call has a deliberate bound below the lease window;
3. local expiry cancellation continues while renewal SQL is slow or blocked;
4. claim and renewal local deadlines are conservative relative to the durable
   lease window;
5. renewal, maintenance, ownership loss, and exhausted settlement behavior are
   observable without unsafe detail;
6. deterministic PostgreSQL tests prove ordinary duplicate claims are rejected,
   lease takeover is fenced, locked-row renewal is isolated, ambiguous outcomes
   remain safe, and shutdown drains accounting correctly;
7. an opt-in randomized multi-replica soak checks durable invariants across
   claim, renewal, cancellation, shutdown, takeover, and ambiguous outcomes;
8. full maintenance pages are followed by bounded prompt re-probes when real
   progress is being made;
9. a retention design records eligibility, permanent-key behavior, archival and
   deletion ordering, operator controls, and compatibility consequences before
   any destructive implementation;
10. queue-depth indexing has a recorded keep/reject decision backed by repeated
    query/read/write evidence; and
11. ordinary throughput and lease/claim benchmarks show no material regression.

### 4.1 Required and conditional outcomes

| Phase | Required outcome |
|---|---|
| 0 — baseline | required before implementation; preserve reproducible evidence |
| 1 — observations | required; truthful bounded outcomes without API-shape changes |
| 2 — renewal | required; classify renewed, definitely lost, and uncertain attempts |
| 3 — concurrency soak | required; opt-in soak plus deterministic CI regressions |
| 4 — burst maintenance | required; bounded prompt draining without new concurrency |
| 5 — retention | required design artifact; production deletion remains deferred |
| 6 — queue-depth index | conditional implementation; explicitly approve or reject from evidence |
| 7 — closure | required; documentation, full gates, review, and status reconciliation |

The queue-depth phase succeeds with either one justified forward index or a
documented rejection. The retention phase succeeds with a decision-ready design
and explicit maintainer decisions or deferrals; it does not authorize deletion.

## 5. Non-goals

This plan does not:

- promise exactly-once worker execution or remote side effects;
- remove the execution row lock or gap-free journal allocator;
- add a global cluster-wide worker-count table;
- make named-queue concurrency global across replicas;
- add Redis, Kafka, a broker, or a distributed lock service;
- add automatic data deletion, journal compaction, or archival code before the
  retention decision is approved;
- encrypt or redact application payloads;
- partition the journal or command tables;
- add an index merely because `EXPLAIN` can be made to use it;
- change public command/event composition semantics; or
- tune PostgreSQL durability settings for benchmark results.

## 6. Repository conventions and commands

### 6.1 Conventions to follow

- Runtime coordination belongs in `runtime_run.go` and `command_runtime.go`.
- Durable transition SQL belongs in focused methods under `internal/store`.
- PostgreSQL time remains authoritative for accepted durable deadlines.
- Local time is used only for conservative process-local cancellation and
  latency measurement.
- Store errors continue to use `MapError` and public sentinel categories.
- Fault seams are named semantic boundaries used by deterministic tests; do not
  expose them publicly.
- Tests use isolated PostgreSQL schemas through `internal/testpg` and must fail,
  not silently skip, under the Makefile test environment.
- Performance evidence records repeated samples and query shapes; ordinary CI
  tests assert structure and semantics rather than wall-clock throughput.
- Commit messages in the current history are short imperative descriptions,
  for example `Optimize command claim throughput` and
  `Batch staged decision persistence`.

### 6.2 Commands

| Purpose | Command | Expected result |
|---|---|---|
| format | `gofmt -w <changed Go files>` | changed Go files are formatted |
| diff validation | `git diff --check` | exit 0, no output |
| build | `make build` | exit 0 |
| static analysis | `go vet ./...` | exit 0, no findings |
| ordinary suite | `go test -count=1 ./...` after exporting the Makefile PostgreSQL environment | exit 0; PostgreSQL tests do not skip |
| full race suite | `make test` | exit 0; race detector enabled |
| focused concurrency | `make test TEST='Test.*\(Lease\|Renew\|Takeover\|Claim\|Maintenance\|Ambiguous\|Shutdown\)' TEST_FLAGS='-count=10 -p 1 -parallel 4'` | all selected PostgreSQL tests run and pass |
| focused store | `go test -race -count=1 ./internal/store/...` after exporting the Makefile PostgreSQL environment | exit 0; no database-backed skip |
| benchmarks | `go test -run '^$' -bench '<Plan 6 regexp>' -benchmem -benchtime=3s -count=5 .` | all selected benchmarks run; no fixture/setup is timed |
| migration/schema | `make test TEST='TestMigration\|TestSchema' TEST_FLAGS='-count=1 -p 1 -parallel 4'` | exit 0 when a migration is added; no database-backed skip |

Use the Makefile's PostgreSQL configuration. A raw `go test` command does not
inherit variables declared only inside `make`, so database-backed focused tests
must use one of these two explicit forms:

```text
make test TEST='<regexp>' TEST_FLAGS='-count=10 -p 1 -parallel 4'
```

Because the current Makefile expands `TEST` into a shell command, escape shell
metacharacters such as `(`, `)`, and `|` in the value, as shown in the command
table. The shell removes those escapes before Go receives the regular expression.

or run `go test` only after the same `FLOW_TEST_DATABASE_URL`,
`FLOW_TEST_DATABASE_PASSWORD`, and `FLOW_TEST_ADMIN_DATABASE` values have been
exported in the shell. Benchmark commands require that exported environment
because the current Makefile has no benchmark target. Never print those values.

Every final test audit must inspect named test output and report zero unexpected
skips. A green command whose PostgreSQL tests skipped is not a passing gate.

### 6.3 Change and review discipline

Implement one phase at a time and keep phase commits independently reviewable.
Do not combine the conditional queue index with lease correctness work, and do
not add retention implementation to the retention-design phase. Preserve
unrelated worktree changes. Remain on the branch selected by the maintainer; do
not push, rewrite history, open/update a pull request, or merge unless separately
requested.

### 6.4 Rollback and migration safety

Keep required runtime/store changes schema-compatible so a deployment can roll
back to the Plan 5 binary without translating durable rows. If an implementation
phase fails before release, revert that phase's independently reviewable commit
rather than layering compensating behavior over uncertain ownership logic.

The optional queue-depth index is the only authorized schema change. Before its
migration is released, it may be removed by reverting the complete unshipped
migration commit. After release, never rewrite migration history; remove an
unhelpful index only through a separately reviewed forward migration. The
retention phase writes design documentation only, so it has no destructive data
rollback path to exercise.

## 7. Scope

### 7.1 In scope

Expected implementation paths are:

- `runtime_run.go` and `runtime_run_test.go` — lease manager, local watchdog,
  maintenance scheduling, bounded observations;
- `command_runtime.go` and `command_runtime_test.go` — conservative claim-local
  deadline, settlement exhaustion observation, multi-replica behavior;
- `internal/store/commands.go` and relevant store tests — skip-locked renewal
  selection and any result metadata required by the runtime;
- `maintenance_fault_test.go` and `claim_test.go` — recovery, fencing, and
  concurrency regression coverage;
- `observer.go` and observer tests only if operation/outcome helpers are needed;
- `internal/store/inspection.go`, `inspection_test.go`, and benchmark files for
  the queue-depth experiment;
- `migrations/`, `migrations.go`, and `migrations_test.go` only if the
  queue-depth evidence approves a forward index migration;
- `specs/projects/flow/benchmark_evidence/plan_6_operational_hardening.md` — new
  measured evidence and decisions;
- `specs/projects/flow/retention.md` — new decision-ready retention design;
- current README/package/spec/component documentation affected by clarified
  lease, recovery, observation, or retention behavior; and
- this plan and `specs/projects/flow/implementation_plan.md` for status updates
  after verification.

### 7.2 Out of scope

Do not modify:

- command, event, worker, execution, history, or trace public shapes except for
  additive documentation of existing semantics;
- the six-table responsibility split;
- journal body formats or replay reducers;
- command/event size limits or the 1,000-command default ceiling;
- retry budget classification of lease loss and shutdown interruption;
- permanent/live execution-key semantics;
- PostgreSQL durability configuration;
- `001_initial.sql` or `002_live_keys.sql` for an optional new index;
- application tables or examples unrelated to the operational guidance; or
- historical phase plans/evidence except for an explicit link marking the new
  Plan 6 evidence as the active follow-on.

## 8. Phase 0: Establish the operational baseline

### 8.1 Record environment and existing behavior

Create `specs/projects/flow/benchmark_evidence/plan_6_operational_hardening.md`.
Record:

- commit SHA and dirty/clean state;
- OS, architecture, CPU, logical CPU count, and memory;
- Go and PostgreSQL versions;
- pool maximum connections, worker concurrency, poll interval, lease duration,
  notification mode, and number of runtime replicas;
- `fsync`, `synchronous_commit`, and `full_page_writes` values;
- exact test and benchmark commands; and
- complete five-sample medians/ranges rather than a selected best sample.

Never record credentials. Mark all timings as development-machine regression
evidence, not service-level objectives.

### 8.2 Retain Plan 5 performance anchors

Before changing lease or maintenance code, rerun at least:

```text
go test -run '^$' \
  -bench 'Benchmark(IndependentCommandLifecycle|SameExecutionFanout|Plan5SameExecutionClaimBatch)' \
  -benchmem -benchtime=3s -count=5 .
```

If benchmark names have drifted, use the exact live equivalents and document
the mapping. Preserve fixture creation and reset outside the timed region.

### 8.3 Add structural baseline measurements

Add or identify benchmarks/tests for:

1. renewing 1, 16, 128, and 1,024 active lease rows in one request;
2. a renewal request containing one queue row held by another transaction plus
   unrelated unlocked attempts;
3. recovering 128, 512, and 1,024 expired leases across distinct executions;
4. `GetQueueDepth` over 1K, 100K, and opt-in 1M queue rows with one queue holding
   1%, 10%, and 100% of the rows; and
5. one opt-in sustained lifecycle sample that reports journal, command, queue,
   WAL if available, and total relation growth without making WAL availability
   a test requirement.

Setup, row creation, forced expiry, row locking, cleanup, and reset belong
outside the timer. Record query plans with `EXPLAIN (ANALYZE, BUFFERS)` only in
isolated test schemas.

### 8.4 Use explicit performance guardrails

Compare five-sample medians and ranges on the same host, PostgreSQL server, pool,
worker, poll, notification, and durability settings. A Plan 5 anchor whose median
regresses by more than 10% with non-overlapping sample ranges is an investigation
gate: rerun the planned-at baseline in a detached worktree before accepting or
rejecting the change. A greater-than-5% hot-path `B/op`, allocation, query-count,
or transaction-count increase also requires explanation even when elapsed time
is noisy. These are investigation thresholds, not permission to accept a known
correctness defect below the threshold.

For maintenance, the primary pass condition is structural: a full progressed
page no longer pays one complete poll interval per page, while a locked/no-op
page remains bounded. For queue depth, apply the evidence and write-cost decision
rule in Section 14.3; do not convert development-machine timings into an SLO.

**Phase 0 verification:**

```text
make test TEST='Test.*\(Lease\|Maintenance\|QueueDepth\)' TEST_FLAGS='-count=1 -p 1 -parallel 4'
go test -run '^$' -bench '<new Plan 6 baseline benchmark names>' -benchmem -benchtime=1x -count=1 .
git diff --check
```

Expected: all selected tests pass, every named benchmark shape runs, and the
evidence file contains the environment and baseline without performance claims
that the fixture cannot support.

## 9. Phase 1: Make operational observations truthful and actionable

### 9.1 Correct lease renewal outcomes

Refactor the renewal observation so exactly one terminal outcome is emitted for
each renewal call:

- `ok` when every requested attempt is renewed;
- `partial` when the database call succeeds but fewer rows renew than were
  requested;
- `error` when the database call or fault seam fails; and
- `cancelled` as a separate bounded count when local attempt contexts are
  cancelled; do not assign a durable loss reason until Phase 2 can distinguish
  definitely lost from uncertain.

Do not emit `ok` after `error`. Capture duration from immediately before the
store call until it returns. In this phase, use counts that let an operator
derive attempted, renewed, and not-renewed totals without guessing why a row was
omitted. After Phase 2 introduces trustworthy store classifications, extend the
same stable operation with aggregate lost and uncertain counts. Do not add
slices or arbitrary labels to `Observation`.

If one `Observation.Count` cannot express both attempted and renewed values,
emit two stable low-cardinality observations rather than extending the public
structure casually. Document the operation/outcome meanings.

### 9.2 Observe maintenance failures

Every maintenance category must emit bounded observations for:

- probe success/error and candidate count;
- transition success/no-op/error and changed count;
- whether the page was full and requested a prompt follow-up pass; and
- pass duration.

Use stable operations such as `deadline`, `wait_expiry`, and `lease_recovery`.
Do not emit one high-volume observation per no-op candidate unless an existing
command/execution identity observation already makes that useful. Prefer one
aggregate per page.

Maintenance still retries on later passes. Observation must not turn a
recoverable database error into runtime termination.

### 9.3 Observe exhausted settlement

When `executeClaim` or `concludeClaim` exhausts `settlementAttempts` while
ownership resolution cannot prove conclusion/loss, emit one final attempt
observation with:

- operation identifying success settlement or failure conclusion;
- outcome `exhausted`;
- the existing safe execution, command, command kind, queue, and worker fields;
  and
- total duration or attempt count where already supported.

Then retain the current safety behavior: return from the local handler lifecycle
and leave the durable lease/recovery mechanism to decide the next transition.
Do not invent a fourth unbounded retry loop.

### 9.4 Test observation semantics

Add tests proving:

- a renewal error emits no success observation;
- a partial result reports attempted, renewed, and not-renewed work consistently
  without prematurely calling an omitted row lost;
- a maintenance probe error and transition error are visible but later recovery
  still succeeds;
- exhausted settlement emits once and exposes no raw database error; and
- the bounded observer adapter can drop these observations without affecting
  claims, renewal, or recovery.

Model observer assertions after the existing `recordingObserver` and
notification listener tests. Avoid sleeps when a barrier or observation channel
can make the boundary deterministic.

**Phase 1 verification:**

```text
make test TEST='Test.*\(Observer\|Observation\|LeaseRenewal\|MaintenanceFault\|Settlement\)' TEST_FLAGS='-count=10 -p 1 -parallel 4'
make test
git diff --check
```

Expected: all tests pass; a forced renewal error produces zero `ok` renewal
observations; all observation values remain bounded and secret-free.

## 10. Phase 2: Isolate and bound lease renewal

### 10.1 Select and classify renewal rows with `SKIP LOCKED`

Change `Store.RenewCommandLeases` from a direct multi-row update to one focused
statement that returns an outcome for every requested attempt:

1. `requested` unnests command ID, attempt ID, and token arrays;
2. `observed` materializes the statement-snapshot fence state for each request;
3. `lockable` verifies the exact running, unexpired fence and selects it
   `FOR UPDATE OF q SKIP LOCKED`;
4. `renewed` updates only `lockable` rows and returns command ID plus durable
   expiry; and
5. the final projection classifies each request as `renewed`, definitely `lost`,
   or `uncertain`.

Target shape:

```sql
WITH now_value AS (
    SELECT clock_timestamp() AS now
), requested(command_id, attempt_id, token) AS (
    SELECT * FROM unnest($1::uuid[], $2::uuid[], $3::uuid[])
), observed AS MATERIALIZED (
    SELECT r.command_id AS requested_command_id,
           r.attempt_id AS requested_attempt_id,
           r.token AS requested_token,
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
SELECT o.requested_command_id,
       CASE
           WHEN x.command_id IS NOT NULL THEN 'renewed'
           WHEN o.command_id IS NULL
             OR o.state IS DISTINCT FROM 'running'
             OR o.active_attempt_id IS DISTINCT FROM o.requested_attempt_id
             OR o.lease_token IS DISTINCT FROM o.requested_token
             OR o.lease_expires_at IS NULL
             OR o.lease_expires_at <= n.now THEN 'lost'
           ELSE 'uncertain'
       END AS outcome,
       x.lease_expires_at
FROM observed o
CROSS JOIN now_value n
LEFT JOIN renewed x ON x.command_id = o.requested_command_id
```

Adjust the exact SQL for PostgreSQL syntax and the qualified schema helper, but
retain this operation-specific structure. Validate all input arrays before SQL,
reject duplicate command IDs, keep one database round trip, and keep row
identity/fence comparison exact. Validate that the store returns exactly one
classified result for every request and no unknown command ID. Use a small
internal enum or equivalent typed outcome rather than stringly typed runtime
branching.

Wire the classified aggregate counts into the stable Phase 1 renewal observation
without changing the public `Observation` shape. A successful result with any
lost or uncertain request remains `partial`; supplementary bounded observations
may distinguish the two counts when one `Observation.Count` cannot carry them.

The three outcomes have deliberately different safety meanings:

- `renewed`: this call extended the exact durable fence; advance only the matching
  local attempt to the conservative renewal-local deadline;
- `lost`: the committed statement snapshot definitely shows an absent queue row,
  non-running state, different attempt/token, or expired lease; cancel only the
  matching local attempt immediately; and
- `uncertain`: the committed snapshot still looks like the requested live fence,
  but the row was not lockable/renewed—normally because a settlement or another
  legitimate Flow transaction currently holds it. Do not immediately cancel it
  and do not extend it. Retain its previous local deadline and let settlement,
  later renewal, or the independent watchdog resolve it.

This distinction is required because a long but supported `WithCommit`
settlement can hold the same queue row. Treating every skipped row as lost would
cancel that callback's worker context while it is legitimately trying to commit,
potentially converting harmless renewal contention into an avoidable rollback.
The watchdog may still cancel the context if the prior conservative deadline
actually passes; `WithCommit` remains a deliberately short transaction boundary.
The classification is conservative under PostgreSQL statement snapshots: an
uncommitted ownership change still appears current and therefore uncertain; a
change committed before the snapshot can be classified definitely lost. Direct
application locking or mutation of Flow-owned queue rows remains unsupported.

### 10.2 Bound each renewal call

Wrap the store renewal call in a derived timeout that is:

- positive and exact;
- comfortably below the lease window;
- capped so a 60-second production lease does not permit a database call to
  block for most of a minute; and
- still usable by the minimum test lease.

A suitable starting rule is:

```text
max(10 ms, min(5 seconds, commandLease / 6))
```

Keep the rule internal. Do not add a public tuning option unless measurements
show one is required across supported deployments. A timeout is a renewal
error; it does not immediately prove every attempt lost. The independent local
expiry watchdog decides which local contexts have crossed their conservative
deadline. On a call error or timeout, emit the truthful error outcome, leave all
matching local deadlines unchanged, and do not run a wholesale
`cancelUnrenewed` pass.

### 10.3 Run local expiry checking independently

Add one runtime-owned watchdog loop, not one goroutine per command. It should:

- tick at a bounded interval no larger than the smaller of one second and a
  safe fraction of the command lease;
- call `active.cancelExpired()` even while the renewal call is waiting;
- stop with the runtime service context;
- join the lease-manager service before `Run` reports shutdown complete; and
- perform no SQL.

Use a short test interval derived from the test lease without spinning in
production. The watchdog is a safety cancellation path; the database remains
the ownership authority.

### 10.4 Anchor claim-local expiry conservatively

Capture a local monotonic timestamp before `ClaimCommands` begins. For every
possibly committed returned command—including commands retained after ambiguous
ownership resolution—carry an internal runtime-only local expiry no later than:

```text
claim-call-start + (durable lease expiry - accepted database time)
```

`ClaimedCommand` is internal to the implementation even though it crosses the
`flow`/`internal/store` package boundary. An additive runtime-only field is
acceptable if clearly documented and never persisted or exposed publicly. A
small runtime wrapper is also acceptable if it keeps the store type cleaner.

`executeClaim` must register this conservative value. Keep the existing
database-time values for attempt timeout and execution deadline calculation.
Do not compare host wall-clock timestamps directly with PostgreSQL absolute
timestamps as if clock synchronization were guaranteed.

### 10.5 Anchor renewal-local expiry conservatively

Capture `renewalStarted := time.Now()` immediately before the bounded store
call. A successful renewal must update local expiry using a deadline no later
than:

```text
renewalStarted + commandLease
```

Do not use response time plus the full lease. If future store SQL returns a
database-side remaining duration sampled after the update, the runtime may use
the smaller safe value. Prefer early cooperative cancellation over allowing the
local worker to outlive the durable fence.

Preserve attempt-ID matching in every active-map operation: a late result for an
older attempt must never renew or cancel a newer retry of the same command.
Replace or split the current blanket `active.cancelUnrenewed` behavior so the
runtime applies the three classified outcomes exactly. An uncertain row retains
its previous deadline; a definitely lost row is cancelled immediately; an error
changes no deadline. A result for an attempt already unregistered by settlement
is a harmless no-op.

### 10.6 Add locked-row and pool-starvation tests

Add deterministic PostgreSQL/race tests for:

1. **Locked-row isolation:** two independent attempts are active; settlement of
   one holds its command/queue row through a blocked `WithCommit`; a renewal
   pass classifies that row uncertain and renews the unrelated attempt. The
   blocked callback's worker context must not be cancelled merely because its
   row was skipped, and the unrelated command must not be taken over or gain a
   second attempt start.
2. **Pool starvation:** active work is started, all available pool connections
   are temporarily held, and the renewal call times out. The local watchdog
   cancels work at its conservative deadline. After pool release, maintenance
   and another replica recover the attempt; the stale worker cannot settle.
3. **Definite ownership loss:** transition ownership only through a supported
   Flow path, then prove the committed absent/mismatched/expired fence is
   classified lost and the exact old local attempt is cancelled immediately.
4. **Uncertain expiry:** hold a still-current row locked past one renewal pass,
   prove its prior local deadline is not extended, and prove the watchdog cancels
   it only after that deadline if settlement does not unregister it first.
5. **Snapshot replacement:** a newer attempt registered while an older renewal
   is in flight is neither renewed nor cancelled by the older result.
6. **Shutdown:** an in-flight bounded renewal and watchdog both drain before
   `Run` closes its services; no goroutine calls `WaitGroup.Add` after a wait.
7. **Ambiguous claim:** conservative claim transfer uses the anchored local
   deadline and still handles true-commit and false-commit outcomes without
   losing or accepting duplicate durable progress.

Assertions must include handler counts, command/queue state, attempt ordinals,
attempt-start/conclusion journal entries, final execution state, and replay/live
agreement. Explicitly allow stale handler overlap where the test intentionally
ignores cancellation; reject duplicate accepted settlement.

**Phase 2 verification:**

```text
make test TEST='Test.*\(LeaseRenewal\|LockedRenewal\|PoolStarvation\|Takeover\|Ambiguous\|Shutdown\)' TEST_FLAGS='-count=10 -p 1 -parallel 4'
make test TEST='TestLeaseRenewalResultCannotCancelWorkOutsideItsSnapshot' TEST_FLAGS='-count=10 -p 1 -parallel 4'
make test PACKAGES='./internal/store/...' TEST_FLAGS='-count=1 -p 1 -parallel 4'
make test
go vet ./...
git diff --check
```

Expected: every command exits zero; unrelated leases renew while one requested
row is locked; local cancellation occurs by the conservative deadline; only one
durable attempt may settle.

## 11. Phase 3: Add a multi-replica concurrency and invariant soak

### 11.1 Keep deterministic regression tests in ordinary CI

The existing focused tests already cover competing claimers, lease takeover,
shutdown, queue fairness, small pools, commit ambiguity, rollback, stale
settlement, and event-input stability. Retain them. Add any new deterministic
case discovered by the soak to ordinary CI before considering the issue fixed.

At minimum, ordinary CI must directly prove:

- two replicas probing the same ready command produce one durable active fence;
- siblings in one execution may be claimed together but one logical command is
  never ordinarily claimed twice;
- a takeover can run while a cancellation-ignoring stale worker remains alive,
  but the stale settlement receives lease loss/terminal state;
- `WithCommit` application writes from a stale attempt cannot commit;
- cancellation and execution expiry conclude only the currently owned attempt;
- renewal omission does not affect a newer attempt registered after the
  snapshot; and
- reverse execution locking in a reused `InTx` client is rejected before SQL.

### 11.2 Add an opt-in deterministic-seed soak

Add one opt-in PostgreSQL test or test binary selected by an environment flag
such as `FLOW_TEST_STRESS=1`. It should run for a bounded number of operations or
bounded duration and print the deterministic random seed on failure.

The soak should use four or more runtime instances over one database schema and
a bounded set of command definitions. Across many small executions, randomly
but reproducibly:

- start permanent, live-keyed, and unkeyed executions;
- stage bounded sibling fan-out and exact event waits;
- cancel runtime contexts and restart compatible replicas;
- delay handlers across renewal boundaries;
- inject existing pre-commit and post-commit ambiguity seams;
- force supported cancellation and deadline paths;
- temporarily constrain pool capacity or hold selected Flow rows in test-only
  transactions; and
- mix cooperative and intentionally cancellation-ignoring handlers.

Do not mutate durable Flow rows into impossible states except for test-only
expiry/time setup already used by current integration tests. The soak is a
concurrency harness, not a corruption fuzzer.

### 11.3 Verify durable invariants after every run

After all runtime instances stop and recovery is allowed to quiesce, assert:

1. journal positions are gap-free from 1 to `next_journal_position - 1` for
   every execution;
2. each attempt ID has at most one `attempt_started` and one
   `attempt_concluded` entry;
3. every concluded attempt has a corresponding start and valid causation;
4. every running queue row has a complete attempt ID/token/owner/start/expiry
   shape and matches a running command;
5. ready/retry queue rows match ready/retry command state;
6. terminal commands have no queue row and have terminal position/time;
7. execution `open_commands` equals the count of non-terminal commands;
8. execution `command_count` equals the retained command count;
9. at most one permanent/live identity holder exists according to key scope;
10. command roots, parents, waits, queue rows, and journal command references
    remain in the same execution;
11. every terminal execution has exactly one execution-terminal event;
12. replay agrees with live semantic projections; and
13. no stale attempt's `WithCommit` marker exists without the matching accepted
    command success.

Put reusable invariant queries in test helpers, not production API. Bound every
query by the soak's known schema and fixture size.

### 11.4 Record the soak without making it flaky CI

Run at least:

```text
FLOW_TEST_STRESS=1 make test TEST='^TestRuntimeMultiReplicaInvariantSoak$' TEST_FLAGS='-count=10 -p 1 -parallel 4'
```

Record operations, seed sequence, replicas, worker/pool settings, lease and
poll durations, total attempts, takeovers, cancellations, ambiguous outcomes,
and final invariant result in the Plan 6 evidence. Ordinary CI runs the
deterministic extracted regressions; the longer soak remains opt-in.

**Phase 3 verification:** focused soak and every deterministic extracted test
pass under the race detector; no invariant is weakened to accommodate a failure.

## 12. Phase 4: Drain burst maintenance backlogs without unbounded work

### 12.1 Refactor one maintenance pass into a testable operation

Extract the body of one maintenance turn into a focused helper that returns a
small internal result, for example:

```go
type maintenancePassResult struct {
    progressed bool
    saturated  bool
}
```

`saturated` means at least one probe returned its complete page size.
`progressed` means at least one candidate committed a state change. Preserve the
current category order so execution deadlines, wait deadlines, and leases are
all visited on every pass.

The helper must keep existing per-candidate revalidation under the execution
lock. Probe results never become decisions by themselves.

### 12.2 Add bounded prompt follow-up passes

When a pass is both saturated and progressed, schedule a prompt follow-up
instead of waiting the full configured poll interval. Bound this behavior with
both:

- a maximum consecutive drain-pass count per turn; and
- a small yield/backoff before starting another turn.

A suitable initial structure is no more than eight consecutive passes, followed
by a short context-aware yield. If a full page makes zero progress—because the
same executions are locked, for example—fall back to the ordinary poll interval
instead of spinning on the stable prefix.

Do not increase the existing page sizes merely to make a benchmark faster.
Bounded pages protect pool usage, memory, and fairness.

### 12.3 Keep transitions sequential initially

Do not immediately parallelize maintenance transitions. Claim transactions
already use pool-aware concurrency while reserving maintenance headroom. Adding
maintenance concurrency without a runtime-wide database budget could starve
lease renewal or application operations.

First measure the bounded prompt-drain implementation. Only if per-candidate
transaction time remains the demonstrated bottleneck may a follow-up design
group candidates by execution and add a small shared pool-aware budget. That
change requires a separate review of claim, renewal, and maintenance connection
competition.

### 12.4 Add backlog and locked-prefix tests

Add deterministic tests with more than one page of:

- expired execution deadlines;
- expired command wait deadlines;
- expired leases across independent executions; and
- multiple expired candidates in the same locked execution.

Prove:

- a progressed full page requests prompt continuation;
- all categories remain serviced while one category has a large backlog;
- a full but completely locked page does not busy-spin;
- multiple runtime replicas remain idempotent;
- lease recovery still creates exactly one conclusion and ready retry per
  attempt; and
- shutdown cancels a drain turn promptly.

Use structural pass/progress counters for ordinary tests. Do not make a narrow
wall-clock threshold the only assertion.

### 12.5 Benchmark recovery throughput

Measure 128, 512, and 1,024 expired leases before and after. Report:

- total recovery time;
- recovered attempts per second;
- queries/transactions if available;
- pool size and replica count;
- time until the first and last command become ready; and
- whether claim work remains responsive during recovery.

No result justifies skipping the execution lock or combining independent
execution journals in one transaction.

**Phase 4 verification:**

```text
make test TEST='Test.*Maintenance.*\(Backlog\|Locked\|Shutdown\|Replica\)' TEST_FLAGS='-count=10 -p 1 -parallel 4'
go test -run '^$' -bench 'Benchmark.*(Maintenance|ExpiredLeaseRecovery)' -benchmem -benchtime=3s -count=5 .
make test
go vet ./...
git diff --check
```

Expected: all tests pass; a recoverable multi-page backlog is not multiplied by
the full poll interval; a locked stable page remains bounded.

## 13. Phase 5: Produce an explicit retention and archival design

### 13.1 This phase is design-only

Create `specs/projects/flow/retention.md`. Do not add deletion SQL, a purge API,
background cleanup, partitioning, or a migration in this phase. The document
must be decision-ready enough that a later implementation plan does not need to
rediscover Flow's object graph or idempotency constraints.

This boundary is deliberate. Retention changes user-visible semantics and may
destroy diagnostic or idempotency data. It should not be smuggled into lease
hardening because both happen to affect operations.

### 13.2 Inventory retained data and ownership

For every durable table/large field, document:

- writer and reader paths;
- immutable versus mutable status;
- foreign-key ownership and delete behavior;
- whether history/replay/trace/idempotency consumes it;
- ordinary and adversarial size bounds;
- whether PostgreSQL TOAST is likely;
- whether it can contain sensitive application data; and
- what breaks if it is removed independently.

At minimum cover:

- execution input, metadata, canonical metadata, fingerprint, failure, and key;
- command args, retry policy, result, failures, and declaration fingerprint;
- queue lease fields and dead-row churn;
- wait selectors and satisfying positions;
- journal body, hash, event identity, attempt identity, and causation; and
- schema ledger records, which are never execution-retention candidates.

### 13.3 Define eligibility separately by key scope

The recommended first retention boundary is:

- terminal unkeyed executions: potentially purgeable after an approved age;
- terminal live-key executions: potentially purgeable after an approved age
  because the live uniqueness contract is already released at terminality;
- permanent-key executions: not purgeable without preserving permanent
  rediscovery and exact equivalent-start/conflict behavior; and
- running/failing executions: never retention candidates.

For permanent keys, compare at least these designs:

1. retain the complete execution indefinitely;
2. archive complete history externally while retaining enough relational data
   for exact start identity and inspection;
3. introduce a compact permanent identity/tombstone relation that preserves
   exact comparison, not only a collision-prone digest; or
4. add an explicit operator-authorized identity release with clearly different
   public semantics.

Recommend one, but record the compatibility and migration cost of every option.
Do not assume a hash alone is equivalent to the current exact byte comparison.

### 13.4 Define archive and purge atomicity

The design must specify:

- terminal-state and age predicates;
- execution-row locking and `SKIP LOCKED` behavior;
- dry-run/list behavior;
- bounded batch size and retry/idempotency key;
- child-before-parent or schema-supported deletion order;
- deletion of journal references before command rows where required;
- treatment of the deferred root-command foreign key;
- how archived history is checksummed and verified before deletion;
- how partial export, commit ambiguity, process crash, and retry are resolved;
- whether History/Trace/GetExecution return tombstone, archived, or not-found
  results afterward;
- authorization and audit logging expected of an operator;
- backup/restore and legal-hold interaction; and
- how readers and writers of different library versions coexist during rollout.

If an object store or application archive table is proposed, Flow must not make
remote archival exactly once by assertion. Use an explicit export identity,
checksum, completed marker, and retryable two-phase operational protocol.

### 13.5 Add operational capacity guidance now

Even before deletion exists, update active documentation with:

- the measured approximately 1,996 journal tuple bytes per small command;
- examples at 10, 100, and 400 sustained commands per second, clearly labeled
  as arithmetic rather than promised capacity;
- PostgreSQL queries for relation/index/TOAST size, dead tuples, last autovacuum,
  and oldest transaction age, without embedding credentials;
- advice to keep large/sensitive values behind stable application references;
- a warning that long caller-owned transactions delay locks and vacuum; and
- a recommendation to monitor queue dead tuples and journal growth.

Do not prescribe disabling `fsync`, `synchronous_commit`, full-page writes, or
autovacuum. Do not set table-specific autovacuum values in migrations without
measured evidence from a representative deployment.

### 13.6 Required human decision

The retention document must end with an explicit maintainer decision record for:

- eligible key scopes;
- minimum terminal age;
- permanent-key identity behavior;
- archive destination/format, if any;
- whether deletion is a public library API, separate operator tool, or
  application-owned procedure; and
- History/Trace/GetExecution behavior after archival.

If those decisions are not approved, Plan 6 may still complete with retention
status `Design complete; implementation deferred`. Do not infer authorization
to delete data.

**Phase 5 verification:** documentation review confirms every current foreign
key, unique identity, read path, and replay dependency is represented. Source
and schema remain unchanged by this phase.

## 14. Phase 6: Measure queue-depth indexing and make a keep/reject decision

### 14.1 Build a representative fixture

Add an opt-in benchmark with:

- 1K and 100K ordinary rows, plus 1M only under an explicit stress flag;
- ready, retry-wait, delayed, and running states in realistic proportions;
- queue selectivity of approximately 1%, 10%, and 100%;
- bounded queue/name/version lengths; and
- database/analyze/setup outside the timed operation.

Measure the exact production `QueueDepthInTx` SQL. Record sequential/index/heap
access, buffers, rows read, execution time, and relation/index sizes.

### 14.2 Compare candidate indexes only inside the isolated schema

At minimum compare:

- no new index;
- `(queue)`;
- `(queue, state, next_run_at)`; and
- any smaller partial alternative that can answer one important subset without
  duplicating the existing claim and lease indexes.

Also measure the write side: batch queue insertion, ready-to-running claim,
renewal, retry release, lease recovery, and terminal deletion. A read index that
materially slows the command lifecycle is not automatically a win.

Do not use `enable_seqscan=off` as proof of production benefit. It may be used
only to demonstrate that a candidate index is structurally capable, alongside
the ordinary cost-based plan.

### 14.3 Decision rule

Add a production index only if all are true:

1. a realistic selective queue-depth workload is materially improved;
2. PostgreSQL chooses the index without planner forcing on the intended shape;
3. index size is recorded and acceptable relative to the queue table;
4. claim, retry, renewal, recovery, and delete benchmarks show no material
   regression outside repeated-sample/environment variance; and
5. the inspection API is expected to be called frequently enough that the read
   benefit pays for continuous write maintenance.

If those conditions are not met, retain the current schema and record
`Rejected: exact queue depth is an operator read whose additional index costs
more than the measured benefit`. A documented rejection is a successful phase
outcome.

### 14.4 If approved, add one forward migration

If evidence approves an index:

- create the next numbered migration rather than rewriting migrations 1 or 2;
- update migration inventory/checksums/compatibility;
- add catalog tests for exact key/include/predicate/uniqueness shape;
- prove no duplicate or prefix-equivalent index already exists;
- capture ordinary `EXPLAIN (ANALYZE, BUFFERS)` on the final migration; and
- rerun PostgreSQL version coverage.

Do not add multiple queue-depth indexes. Do not include mutable columns merely
to force an index-only plan without proving the heap avoidance is valuable.

**Phase 6 verification:**

```text
go test -run '^$' -bench 'Benchmark.*QueueDepth' -benchmem -benchtime=3s -count=5 .
make test TEST='Test.*QueueDepth\|TestMigration\|TestSchema' TEST_FLAGS='-count=1 -p 1 -parallel 4'
make test
git diff --check
```

Expected: benchmark/evidence is complete and the plan records exactly one
outcome—approved index with passing migration gates, or explicit rejection with
no schema change.

## 15. Phase 7: Documentation and final verification

### 15.1 Synchronize active documentation

Update README, package docs, functional spec, architecture, runtime/schema
components, and project overview where relevant. They must consistently state:

- one command normally has one durable active fence;
- stale and takeover handler bodies may overlap under at-least-once delivery;
- only the current attempt ID/token may settle;
- cooperative cancellation is prompt but cannot stop code that ignores context;
- `WithCommit` is for short same-database writes and may hold the execution and
  queue lock until commit;
- caller-owned transactions must be short and follow execution-first ordering;
- renewal calls are bounded and locked rows cannot block unrelated renewals;
- a skipped locked renewal is uncertain rather than automatic ownership loss,
  while committed fence loss cancels the matching local attempt promptly;
- maintenance drains recoverable backlogs in bounded prompt passes;
- observers expose safe operational outcomes but are best-effort; and
- retained data has no automatic deletion until a separately approved retention
  implementation exists.

### 15.2 Run final performance comparisons

Repeat the Phase 0 commands on the same PostgreSQL server/settings when
possible. Report:

- independent lifecycle throughput at 1/4/16 producers;
- 10/100 same-execution fan-out;
- 16-command same-execution claim latency;
- renewal latency for 1/16/128/1,024 attempts;
- locked-row renewal behavior;
- expired-lease recovery at 128/512/1,024;
- queue-depth results and the keep/reject decision; and
- journal/storage arithmetic unchanged by lease code.

Do not hide a regression behind historical environment variance. If the host or
PostgreSQL environment changes materially, rerun the planned-at commit or the
nearest reproducible baseline in a detached worktree with an evidence artifact,
following the reproducibility approach used by Plan 5.

### 15.3 Complete verification gates

Run, in order:

```text
gofmt -w <all changed Go files>
git diff --check
make build
go vet ./...
go test -count=1 ./...
make test
```

Then run the focused lease/claim/maintenance tests at least ten times under the
race detector and the opt-in soak as specified above. If a migration was added,
run supported PostgreSQL major-version ordinary and race suites with durability
enabled and record them.

Audit named test output to prove database-backed tests did not skip. Confirm the
worktree contains only intended source, test, documentation, evidence, and
optional forward-migration changes.

### 15.4 Review and status

Require an independent code review focused on:

- renewal SQL locking and renewed/lost/uncertain classification semantics;
- local-versus-database deadline reasoning;
- watchdog shutdown and goroutine ownership;
- old-attempt/new-attempt races in the active map;
- observer safety and truthful outcomes;
- bounded maintenance continuation and locked-prefix fairness;
- durable invariant soak assertions;
- queue-depth index write amplification; and
- retention language that does not authorize direct deletion.

Mark this plan complete only after all non-conditional acceptance criteria pass
and every conditional phase records an explicit accepted/deferred/rejected
outcome.

## 16. Acceptance criteria

1. `RenewCommandLeases` uses an exact-fence, `FOR UPDATE SKIP LOCKED` selection
   before its set-oriented update and returns one typed classification for every
   requested attempt.
2. One locked requested queue row does not block renewal of unrelated rows.
3. A definitely absent, mismatched, malformed, or expired fence is classified
   lost and cancels only the exact matching local attempt promptly.
4. A row that appears current but is not lockable is classified uncertain; it is
   not cancelled immediately and its previous local deadline is not extended.
5. A renewal call error or timeout changes no local deadline and does not imply
   wholesale ownership loss.
6. Each renewal call has an internal context timeout below the lease duration.
7. A runtime-owned local expiry watchdog continues while renewal SQL is blocked.
8. Claim-local expiry is anchored no later than the claim call's conservative
   lease window.
9. Renewal-local expiry is not calculated as response time plus a full lease.
10. A late renewal result cannot renew or cancel a newer attempt for the same
    command.
11. Renewal emits exactly one truthful terminal outcome; an error is never
    followed by `ok` for the same call.
12. Maintenance probe/transition errors and settlement exhaustion are observable
    without raw error, SQL, payload, or token data.
13. Two ordinary competing replicas create at most one active durable fence per
    command.
14. A stale/cancellation-ignoring handler can overlap a takeover only in the
    documented at-least-once sense and cannot commit Flow or `WithCommit` state.
15. Locked-row, definite-loss, uncertain-expiry, pool-starvation, takeover,
    ambiguity, snapshot-replacement, and shutdown tests pass repeatedly under
    the race detector.
16. The opt-in multi-replica soak passes every journal, projection, ownership,
    counter, terminality, and replay invariant.
17. Full progressed maintenance pages receive bounded prompt follow-up passes.
18. A full locked/no-progress maintenance page falls back without busy-spinning.
19. Execution deadlines, wait deadlines, and lease recovery all remain serviced
    during a burst in another maintenance category.
20. The retention design inventories every retained field/table dependency and
    records explicit decisions or deferrals for key scope, archival, deletion,
    and post-archive inspection.
21. No production deletion/pruning code is introduced without the retention
    decision and a separately approved implementation scope.
22. Queue-depth indexing ends in one evidence-backed outcome: one forward index
    migration or an explicit rejection with no schema change.
23. No exported orchestration shape, public lease-loss/retry-budget semantics,
    journal format, six-table responsibility, or durability setting changes.
24. Plan 5 command/event throughput and memory benchmarks show no unexplained
    material regression.
25. Formatting, diff, build, vet, ordinary PostgreSQL, full race, focused repeat,
    and named zero-unexpected-skip gates pass.

## 17. STOP conditions

Stop and report rather than improvising if:

- source drift changes the execution lock, queue fence, active-attempt map,
  renewal query, maintenance scheduler, observer, or queue-depth query from the
  assumptions in this plan;
- a renewal SQL shape cannot use `SKIP LOCKED` without weakening exact token and
  state checks;
- one bounded statement cannot distinguish definitely lost fences from rows that
  still appear current but are temporarily un-lockable, or the store cannot
  return exactly one trustworthy classification per request;
- the watchdog requires one goroutine/timer per command or cannot join cleanly
  during shutdown;
- a local timestamp calculation depends on PostgreSQL and application host
  clocks being synchronized;
- pool headroom tests show the renewal timeout/watchdog competes materially with
  claims or maintenance;
- a stale attempt can commit a result, staged event/child, terminal projection,
  or `WithCommit` write after another attempt owns the queue row;
- maintenance continuation can spin on a locked stable prefix or starve another
  maintenance category;
- a proposed maintenance speedup requires unbounded concurrency or a new global
  worker-count table;
- retention implementation would change permanent-key rediscovery, history,
  trace, or replay semantics without an explicit maintainer decision;
- deletion order would require dropping/weakening same-execution, root, parent,
  journal, or wait ownership constraints;
- a queue-depth index is selected only under forced planner settings or causes a
  material claim/renew/retry regression;
- an already released migration would need rewriting;
- benchmark setup leaks into the timed operation or environment drift prevents
  an honest comparison;
- a focused/full verification command fails twice after a reasonable scoped
  correction; or
- unrelated user changes overlap an in-scope file and cannot be preserved.

## 18. Alternatives considered and rejected

### 18.1 Add another uniqueness guard to prevent concurrent handler bodies

Rejected. Ordinary claims already have one queue row, one execution lock, one
active attempt ID/token, and fenced settlement. A uniqueness constraint cannot
stop a paused process from continuing after its durable lease expires. It would
add write/index cost without changing the at-least-once boundary.

### 18.2 Hold a PostgreSQL connection for the complete worker call

Rejected. A session/row lock around application code could suppress some
takeovers but would exhaust the pool, make crash detection depend on connection
failure, and still not make remote effects exactly once. Flow correctly releases
the claim connection before invoking the worker.

### 18.3 Use advisory locks in addition to queue leases

Rejected. Advisory locks would create a second ownership authority and do not
survive or explain commit ambiguity better than the durable queue fence. The
execution row and queue token already encode the required authority.

### 18.4 Switch semantic transactions to `SERIALIZABLE`

Rejected. Execution-first row locking already serializes the intended aggregate.
`SERIALIZABLE` would add abort/retry behavior across application callbacks and
independent operations without preventing stale external effects.

### 18.5 Renew every command with one independent query/goroutine

Rejected as the default. It avoids batch head-of-line blocking but turns one
bounded set-oriented operation into connection/query amplification. A
skip-locked set-oriented renewal retains the desired normal-path shape.

### 18.6 Make lease duration publicly configurable

Rejected for this phase. The fixed 60-second production lease and one-third
renew cadence are easy to reason about. The problem is blocked renewal and local
deadline handling, not lack of another public knob. Reconsider only with
deployment evidence that one fixed duration cannot serve supported workloads.

### 18.7 Parallelize all maintenance immediately

Rejected. Claim concurrency already reserves limited pool headroom, and worker
application SQL may share the caller-owned pool. Prompt bounded sequential
draining is simpler and must be measured first.

### 18.8 Delete journal rows while keeping projections

Rejected. It breaks trace, replay, causation, event-input satisfying positions,
attempt evidence, and conformance. Retention must operate on an explicitly
defined execution/archive boundary.

### 18.9 Purge permanent-key executions like live-key executions

Rejected without a new identity contract. Permanent keys mean one execution is
rediscovered forever with exact equivalent-start comparison. Deletion would
silently change public idempotency.

### 18.10 Partition the journal before defining retention

Rejected for now. Partitioning can improve bulk lifecycle operations but does
not decide what may be deleted, how execution-local primary/foreign keys map to
partitions, or what permanent identity/history means. Define retention first.

### 18.11 Add a queue-leading index immediately

Rejected without evidence. Queue-depth reads may improve, but every extra index
on `flow_command_queue` is maintained by the hottest lifecycle transitions.
The benchmark and keep/reject gate are part of the deliverable.

## 19. Maintenance notes

After implementation:

- any new attempt-owning path must register a conservative local expiry and be
  included in the watchdog tests;
- any renewal query change must preserve exact command/attempt/token matching,
  skip-locked isolation, and the renewed/lost/uncertain distinction;
- new long-running store operations must have an explicit context bound or a
  documented reason they inherit the caller's deadline;
- observation additions must remain low-cardinality, bounded, non-blocking, and
  free of payload/SQL/token data;
- new maintenance categories must participate in the same bounded pass result
  and cannot monopolize prompt-drain turns;
- worker handlers must continue respecting context, while correctness must not
  depend on them doing so;
- operators should size a dedicated Flow-capable pool or otherwise preserve
  connection headroom when application handlers heavily use the same database;
- queue bloat, oldest transactions, autovacuum progress, journal growth, lease
  recovery lag, and renewal errors are the primary operational signals;
- no one should run direct retention deletes from the design document; and
- any future purge/archival implementation must receive its own migration/API,
  compatibility, rollback, and destructive-operation review.

Reviewers should prioritize concurrency invariants over benchmark headlines. A
faster renewal or recovery path that lets two fences settle, skips durable
history, or hides ownership uncertainty does not satisfy this plan.

## 20. Punchlist

### Baseline and evidence

- [ ] Record Plan 6 commit, host, Go/PostgreSQL versions, durability settings,
  pool, worker, poll, lease, notification, and replica configuration.
- [ ] Rerun Plan 5 independent lifecycle, same-execution fan-out, and
  same-execution claim benchmarks as regression anchors.
- [ ] Add renewal batch, locked-row renewal, maintenance backlog, queue-depth,
  and retained-growth benchmark shapes with setup outside timing.
- [ ] Create `benchmark_evidence/plan_6_operational_hardening.md` with exact
  commands, all repeated samples, and explicit limitations.

### Observability

- [ ] Remove the unconditional successful lease-renewal observation after an
  error.
- [ ] Emit one truthful `ok`, `partial`, or `error` renewal outcome with bounded
  attempted/renewed/not-renewed counts and duration.
- [ ] Observe local lease cancellation/loss without exposing attempt IDs or
  tokens.
- [ ] Add bounded probe/transition/error observations for execution deadline,
  wait expiry, and lease recovery maintenance.
- [ ] Emit a final safe observation when success settlement or failure
  conclusion exhausts its bounded internal attempts.
- [ ] Add observer regression tests for error/no-success, partial renewal,
  maintenance recovery, settlement exhaustion, drops, and panic isolation.

### Lease renewal and local ownership

- [ ] Refactor `RenewCommandLeases` through exact-fence
  `FOR UPDATE OF q SKIP LOCKED` selection and one set-oriented update.
- [ ] Return and validate exactly one typed `renewed`, `lost`, or `uncertain`
  classification for every requested attempt.
- [ ] Extend the stable renewal observations with bounded lost/uncertain counts
  only after the store can classify them truthfully.
- [ ] Apply classifications exactly: renew advances the matching local deadline,
  lost cancels the matching attempt, and uncertain/error changes no deadline.
- [ ] Add an internal renewal timeout safely below the command lease.
- [ ] Add one runtime-owned local expiry watchdog independent of renewal SQL.
- [ ] Anchor claim-local expiry to the claim call's conservative lease window.
- [ ] Anchor renewal-local expiry to renewal start or a smaller database-derived
  remaining window, never response time plus a full lease.
- [ ] Preserve old-attempt/new-attempt protection in renewal and cancellation.
- [ ] Prove one locked settlement row is uncertain and is not immediately
  cancelled while an unrelated active command renews normally.
- [ ] Prove definite fence loss cancels promptly, while an unresolved uncertain
  row retains its old deadline and is cancelled only by the watchdog at expiry.
- [ ] Prove pool starvation triggers bounded renewal failure and timely local
  cancellation, then safe takeover after capacity returns.
- [ ] Prove stale and ambiguous attempts cannot commit durable Flow or
  `WithCommit` state.
- [ ] Prove lease manager, watchdog, claim accounting, and workers drain safely
  on shutdown.

### Concurrency verification

- [ ] Retain and strengthen deterministic competing-replica, takeover, queue
  fairness, small-pool, ambiguity, cancellation, and shutdown tests.
- [ ] Add an opt-in deterministic-seed multi-replica invariant soak.
- [ ] Exercise permanent/live/unkeyed starts, fan-out, waits, cancellation,
  restart, renewal delay, pool pressure, and commit ambiguity in the soak.
- [ ] Assert gap-free journal, attempt start/conclusion uniqueness, fence shape,
  queue/command agreement, counters, terminal events, ownership, and replay.
- [ ] Promote every discovered soak failure to a deterministic ordinary test.
- [ ] Run the focused concurrency selection repeatedly under the race detector.

### Burst maintenance

- [ ] Extract one maintenance pass with explicit progressed/saturated result.
- [ ] Add bounded prompt follow-up passes only after a full page that made
  progress.
- [ ] Bound consecutive drain passes and add a context-aware yield.
- [ ] Fall back to normal polling for a full locked/no-progress page.
- [ ] Preserve service of deadlines, waits, and leases during category-specific
  backlogs.
- [ ] Add multi-page, locked-prefix, multi-replica, and shutdown tests.
- [ ] Measure 128/512/1,024 expired-lease recovery and claim responsiveness.
- [ ] Keep maintenance sequential unless evidence and a separate pool-budget
  review justify bounded concurrency.

### Retained data design

- [ ] Create `specs/projects/flow/retention.md` as a design-only artifact.
- [ ] Inventory every retained table/large field, writer, reader, constraint,
  size bound, sensitivity, and replay/idempotency dependency.
- [ ] Define terminal eligibility separately for unkeyed, live, and permanent
  execution keys.
- [ ] Compare permanent retention, archive-plus-relational identity, exact
  tombstone, and explicit identity-release options.
- [ ] Specify archive identity/checksum, dry run, locking, bounded batches,
  crash/ambiguity recovery, deletion order, and post-archive read semantics.
- [ ] Add capacity arithmetic and safe PostgreSQL relation/index/TOAST,
  dead-tuple, autovacuum, and oldest-transaction monitoring guidance.
- [ ] Record the maintainer decisions or mark retention implementation
  explicitly deferred.
- [ ] Add no production purge/deletion code under this plan without separately
  approved destructive semantics.

### Queue-depth evidence

- [ ] Benchmark exact production queue-depth SQL at 1K/100K and opt-in 1M rows
  across 1%/10%/100% queue selectivity.
- [ ] Compare no index, `(queue)`, `(queue,state,next_run_at)`, and any justified
  smaller partial candidate in isolated schemas.
- [ ] Measure candidate index size and claim/renew/retry/recovery/delete write
  effects.
- [ ] Record one explicit decision: add one justified index or reject the index.
- [ ] If approved, add only a forward migration with exact catalog, plan,
  compatibility, and PostgreSQL-version tests.
- [ ] Do not force planner settings or add mutable covering payload merely to
  claim an index-only scan.

### Documentation and final gates

- [ ] Document at-least-once handler overlap versus exactly-one accepted fenced
  progression.
- [ ] Document cooperative cancellation, short `WithCommit`, execution-first
  caller transactions, pool headroom, bounded renewal, and burst recovery.
- [ ] Synchronize README, package docs, functional spec, architecture,
  components, overview, implementation plan, and Plan 6 evidence.
- [ ] Run final repeated before/after lifecycle, claim, renewal, maintenance,
  queue-depth, and storage measurements.
- [ ] Run gofmt, diff check, build, vet, exact ordinary suite, full race suite,
  focused repeated concurrency tests, and named no-skip audit.
- [ ] Run supported PostgreSQL-version coverage if a migration is added.
- [ ] Obtain independent review of renewal SQL, local deadlines, watchdog
  lifecycle, maintenance bounds, invariant soak, retention contract, and
  optional index decision.
- [ ] Mark Plan 6 complete only when every required item passes and every
  conditional item records an explicit approved, rejected, or deferred outcome.
