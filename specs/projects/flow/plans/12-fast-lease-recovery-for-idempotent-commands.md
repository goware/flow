# Plan 12: Fast lease recovery for idempotent commands

Status: Planned — reconciled against accepted Plan 13

Reconciled at: `c450ff4b060d1a862fde0540c794fb2d9876147b` on 2026-08-12

- **Priority:** P2 — reduce dead-worker recovery latency for commands whose
  duplicate execution is safe
- **Effort:** L, phased
- **Risk:** MEDIUM-HIGH — the public option is small, but its durable value
  crosses definition, command creation, replay, batched claim, renewal timing,
  the local watchdog, and maintenance recovery
- **Depends on:** accepted Plan 13 commit `c450ff4`; Plan 7's
  lease fencing, local watchdog, and maintenance fixes remain controlling
- **Public API impact:** additive — one `CommandOption`,
  `WithRecoveryLease(time.Duration)`
- **Database impact:** development reset only — add one nullable declaration
  column to the consolidated `001_initial.sql`; do not add a second migration
  or support existing Flow data
- **Durable history impact:** current format only — include the override in
  `command_created`, declaration fingerprints, and replay; no compatibility
  decoder is required
- **Runtime impact:** keep the existing lease manager and watchdog services;
  make their timing depend on active commands rather than add another service
  or one goroutine per attempt
- **Release impact:** implementation does not tag or publish a release

> **Sequencing:** Plan 13 was independently reviewed and accepted at the commit
> above. Start this plan from that exact commit (or its descendant), repeat the
> initial drift audit, and record the implementation base here if it differs.
>
> **Executor rule:** This plan changes how long an attempt remains recoverable;
> it does not change what owns an attempt. The attempt ID, lease token,
> settlement fence, run lock, at-least-once execution contract, and
> at-most-one-durable-settlement guarantee must remain intact. If implementation
> appears to require weakening any of them, STOP and report.

## 1. Purpose

Flow currently uses one fixed 60-second production command lease. A healthy
runtime renews that lease, but a process that dies while handling a command
cannot renew it. Another replica must wait for the stored `lease_expires_at`
and the ordinary maintenance sweep before it can recover and re-claim the
command.

That conservative delay is sensible for a command whose worker may perform an
expensive or non-idempotent external side effect. It is unnecessary for a
read-only status poll, receipt lookup, or similar worker that can safely run
again. In those cases a dead process introduces roughly a full minute of idle
time even when another replica is ready and the external result already exists.

Plan 12 adds one opt-in declaration:

```go
var pollReceipt = flow.DefineCommand[PollArgs, PollResult](
	"txn.poll_receipt",
	2,
	flow.WithQueue("txn.mine"),
	flow.WithRecoveryLease(5*time.Second),
)

func pollReceiptWorker(
	ctx context.Context,
	work *flow.Work[PollArgs],
) (PollResult, error) {
	return lookupReceipt(ctx, work.Args)
}

runtime.Register(flow.Handle(pollReceipt, pollReceiptWorker))
```

The worker remains registered separately through `flow.Handle`; command
definition does not accept a handler. An undeclared command still uses Flow's
fixed 60-second production default.

The goal is deliberately narrow:

- recover a dead holder of an explicitly safe command in seconds rather than
  roughly a minute;
- keep healthy long-running handlers alive by renewing with their own declared
  window;
- retain set-oriented claim and renewal operations when one run contains mixed
  lease durations; and
- make the shorter timing safe under shutdown, slow PostgreSQL calls, renewal
  races, and multiple replicas.

## 2. Current baseline after Plan 13

The original version of this plan predated Plan 13 and made assumptions that
are no longer true. Implementation must use this baseline instead:

1. `DefineCommand[A, R](name, version, opts...)` accepts only command options.
   `flow.Handle` binds the worker later.
2. Production callers cannot configure the runtime-global command lease.
   `Runtime.commandLease` defaults to 60 seconds; only the unexported
   `withCommandLeaseForTest` seam can change it.
3. `commandDefaults` currently contains queue, retry policy, and attempt
   timeout. These values are copied into each durable command declaration.
4. `flow_commands` is part of one clean six-table `001_initial.sql`. Existing
   development databases and retained history are disposable.
5. Same-run claims are set-oriented. `ClaimCommands` locks a candidate batch,
   appends all `attempt_started` entries, and updates the command/queue
   projections in one transaction.
6. Renewal is also set-oriented, but the current manager wakes on one global
   ticker and supplies one scalar duration to one renewal statement. That shape
   cannot correctly handle mixed per-command leases without amendment.
7. The local lease watchdog currently wakes from the global lease duration.
   A short command could expire before that watchdog's next tick.
8. Maintenance already recovers rows according to their stored
   `lease_expires_at`. It needs no new recovery category or ownership rule.

## 3. Controlling semantics

### 3.1 A lease bounds duplicate execution, not durable settlement

When a lease expires, a second replica may recover and run the command while
the original handler is still alive but partitioned. Flow's settlement fence
allows only the current attempt ID and lease token to commit a durable result.
The old attempt cannot settle over a takeover.

A shorter lease therefore does **not** permit two durable settlements. It does
permit duplicate application work sooner. This option is safe only when that
duplicate work is acceptable.

Good candidates include:

- read-only receipt or status polling;
- deterministic reconciliation against an authoritative external state;
- an external request protected by its own stable idempotency key; and
- cheap work whose duplicate result is harmless and whose stale attempt can be
  discarded.

Poor candidates include:

- a payment or transaction submission without external idempotency;
- sending an email or webhook that the receiver may process twice;
- a worker with an unbounded application commit callback; and
- any side effect whose safety depends on Flow preventing concurrent handler
  execution. Flow never promises that; its guarantee is fenced settlement.

### 3.2 The override belongs to the command declaration

Recovery safety is a property of the command worker, not a property of one
enqueue call. `WithRecoveryLease` therefore belongs next to `WithQueue`,
`WithRetry`, and `WithTimeout` on `DefineCommand`.

Do not add a run option, an enqueue override, a worker option, a queue-wide
lease class, or a public runtime lease option. Those forms let the same command
semantics vary by call site or deployment configuration and make duplicate-work
bounds difficult to reason about.

Changing a recovery lease changes the durable command declaration. Callers
should bump the command version when changing it, just as they should for other
worker-relevant durable settings.

### 3.3 Unset keeps the conservative default

An absent override means "use the runtime's command lease." In production that
remains 60 seconds. The unexported runtime test seam continues to control the
fallback in focused tests.

Do not persist 60 seconds into every command merely because it is today's
default. Persist only an explicit override. Each `attempt_started` row still
records the actual resolved lease used for that attempt.

### 3.4 The declaration is durable

The runtime must not rediscover the lease only from its locally registered
command definition at claim time. Different replicas can briefly run different
builds, and a process restart must not reinterpret an existing command.

Persist the override with the command. Claim and replay use the durable value;
the registered worker supplies code and codecs, not a replacement lease policy.
This keeps a command's recovery behavior stable across replicas and restarts.

### 3.5 Duration rules

`WithRecoveryLease(d)` must:

- be accepted at most once per command definition;
- reject zero and negative values;
- round a positive fractional millisecond upward once at the public API
  boundary, matching Plan 13's other public duration options;
- reject a normalized value below 30 milliseconds, matching the engine's
  existing technical lease floor; and
- reject values that cannot be represented exactly as PostgreSQL
  milliseconds.

Thirty milliseconds is a technical testing floor, not an operational
recommendation. Documentation and examples should recommend values measured in
seconds. Very short leases create proportionally more renewal traffic and are
more sensitive to scheduler pauses and database latency.

### 3.6 Attempt timeout and recovery lease are different

`WithTimeout` limits how long the handler attempt may run. `WithRecoveryLease`
controls how quickly another replica may take over after lease renewal stops. A
healthy long-running handler may run for many recovery windows because it keeps
renewing. Neither option silently sets the other.

## 4. Durable data model

### 4.1 Consolidated schema reset

Add this nullable column to `flow_commands` in `migrations/001_initial.sql`:

```sql
recovery_lease_ms bigint
    CHECK (recovery_lease_ms IS NULL OR recovery_lease_ms >= 30)
```

Place it with `queue`, `attempt_timeout_ms`, and `retry_policy`. It is a command
declaration, not current queue ownership, so do not duplicate it into
`flow_command_queue`.

The repository is still in development and the user has explicitly declared
old Flow data disposable. Rewrite the one baseline migration and update schema
tests. Do not add `002`, data backfill, dual-read logic, an old journal decoder,
or an upgrade path. The implementation notes must state that the Flow schema is
dropped and recreated.

No seventh table or new index is needed. The claim query already joins and
locks the command and queue projections.

### 4.2 Command creation and declaration identity

Add the normalized optional duration throughout the existing declaration path:

- `commandDefaults` and `commandOptionState`;
- `equivalentCommandDefaults`, so duplicate staging under one command key only
  coalesces equivalent declarations;
- `store.CommandCreate` and its validation;
- root and staged-child preparation;
- `commandDeclarationFingerprint`;
- `preparedCommandInsert` and the existing command `CopyFrom`; and
- schema/catalog/read-path tests that enumerate command columns.

Use a zero `time.Duration` internally to mean no override until the nullable
database/journal representation is prepared. Do not add a public accessor only
to expose this internal default.

### 4.3 Journal and replay

Add optional `recovery_lease_ms` to `journalcodec.CommandCreatedBody`. An
explicit override is present and positive; the default is omitted. Replay must
retain and validate it in the replay command state.

Add the field to the current declaration fingerprint. Because the database and
history are reset, keep the clean current body/fingerprint version rather than
introducing an old-format branch.

`AttemptStartedBody.LeaseDurationMS` already exists. Populate it with the
resolved duration for that individual claimed command, including the runtime
fallback when the declaration has no override. This makes every attempt's
actual ownership window auditable.

Malformed direct-SQL values and non-positive journal durations must fail closed
as invalid durable state where the existing validation layer can observe them.
For an explicit override, replay can also require an `attempt_started` lease to
match the command declaration. For an absent override it must accept any valid
resolved duration because the unexported runtime test seam intentionally
changes the fallback. Replay does not reconstruct live ownership, but it must
still reject malformed command declarations and attempt lease values.

## 5. Claim path: retain one mixed-duration batch

Do not split a same-run batch by recovery lease and do not issue one query per
command.

### 5.1 Resolve each locked command's duration

Extend the existing locked claim row with nullable `recovery_lease_ms`. For
each claimable command:

1. validate and convert the stored override when present;
2. otherwise use the `defaultLease` argument supplied by the runtime;
3. calculate `lease_expires_at` from the semantic transaction's one database
   timestamp plus that command's duration; and
4. retain both the duration and expiry in the prepared claim row.

Keep `ClaimCommands(ctx, candidates, defaultLease, owner, hook)` as the store
entry point. Rename its parameter from a generic `lease` to `defaultLease` so
its fallback role is explicit.

### 5.2 Persist all fences set-wise

The existing claim update uses one expiry for the whole batch. Change its
prepared arrays/`unnest` input to carry one expiry per command. The transaction
must still perform:

- one run lock;
- one ordered candidate lock/read;
- one grouped event-input read;
- one journal append for all `attempt_started` entries;
- one bulk queue projection update; and
- one bulk command projection update.

Every journal entry must map back to the exact command, attempt ID, token, and
per-command lease duration. Preserve all row-count and identity checks.

### 5.3 Return the actual duration

Add `LeaseDuration time.Duration` to internal `store.ClaimedCommand`. Populate
it from the resolved durable value. Runtime local-expiry anchoring must use this
duration and the returned database time; it must not look the value up again
from worker registration.

The value is internal engine state, not a new public `Work` or inspection
field.

### 5.4 Ambiguous claim commits

Keep the existing prepared-result and ownership-resolution behavior for an
ambiguous claim commit. The prepared `ClaimedCommand` must already contain the
correct duration and conservative local expiry so a possibly committed fence
transferred to worker accounting behaves like an ordinary successful claim.

Do not change attempt identity, ownership resolution, slot accounting, or the
rule that only definitely lost/concluded attempts are dropped.

## 6. Renewal path: one statement, mixed durations

### 6.1 Carry duration per renewal

Extend internal `store.LeaseRenewal` with `Duration time.Duration` and remove
the scalar duration argument from `RenewCommandLeases`.

Validate every request's identity and exact duration before SQL. The existing
single statement should unnest aligned arrays of command IDs, attempt IDs,
tokens, and duration milliseconds, then update each lockable row with:

```sql
lease_expires_at = db_now + requested.duration_ms * interval '1 millisecond'
```

Keep ordinality, exact result count/order checks, duplicate-command rejection,
and the `renewed` / `lost` / `uncertain` classification. A mixed due set must
remain one statement. Do not group by duration and do not introduce N+1 SQL.

### 6.2 Track per-attempt timing locally

Extend `activeCommand` with the minimum timing state needed by the existing
services:

```text
lease duration
conservative local expiry
next renewal time
renewal in flight
whether the next call is a retry
cancelled
```

Registration uses the claim's returned `LeaseDuration` and conservative local
expiry. A successful renewal anchors the new local expiry to the local renewal
call start plus that command's duration; never add network time to the durable
window.

The active registry should expose one lightweight change notification shared
by the manager and watchdog. Register, unregister, renewal completion, retry
scheduling, and cancellation notify it so a newly registered short lease can
reset a timer immediately. Reuse the repository's channel-generation pattern
or an equivalently small primitive; do not create a goroutine or timer per
command.

### 6.3 Replace the global renewal ticker with an earliest-due timer

Keep one `runLeaseManager` service, but make it schedule from active attempts:

1. wait until the earliest `nextRenewAt`, an active-registry change, or runtime
   cancellation;
2. atomically snapshot all due, non-cancelled, non-renewing attempts and mark
   them renewing;
3. renew that due set in one store call, even when durations differ;
4. apply each classified result to the matching active attempt; and
5. calculate the next timer from the remaining active set.

The normal renewal target is approximately one third of each command's lease.
Exact scheduling may be slightly later due to normal Go timer behavior, but it
must leave substantial time for a retry before conservative local expiry.

Default-only workloads should retain approximately today's cadence. A short
lease must not cause every long-lease command to renew at the short cadence;
only due attempts enter the batch.

### 6.4 Bound slow calls and retry within the lease window

Keep every renewal call bounded. For an ordinary renewal, derive its timeout
from the shortest lease in the due set using the existing shape:

```text
max(10 milliseconds, min(5 seconds, shortest due lease / 6))
```

One timeout must not defer every affected command until its next ordinary
one-third cadence. On a call error or `uncertain` result, clear the in-flight
mark and schedule a bounded retry before that command's conservative local
expiry. Use a small capped delay derived from its own lease/remaining window;
do not spin and do not retry after the attempt is locally expired.

The retry call must not blindly reuse the five-second cap. Give a retry a
larger but still bounded budget, capped by both one third of the shortest due
lease and half of the shortest remaining local window. This lets a default
60-second command tolerate a slow round-trip longer than five seconds while
still retaining time for cancellation or another retry. Runtime cancellation
must cancel either budget immediately. Sustained database latency that consumes
the ownership window can still lose the lease; the feature must not turn that
case back into an unbounded wait.

This preserves the reason for Plan 7's timeout—PostgreSQL cannot hang runtime
shutdown forever—while removing both the one-shot-per-tick cliff and the hard
five-second latency cliff for a default command.

### 6.5 Close the known watchdog application race

The watchdog must not cancel an attempt while its matching bounded renewal call
is still in flight. The store may already have committed the extension while
the Go result is waiting to be returned or applied.

Mark the selected attempts renewing before the SQL call. Apply a successful
result and clear that mark under the same active-registry lock. On a definite
`lost`, cancel it. On error or `uncertain`, clear the mark and use the bounded
retry behavior above.

This closes the local race between a known in-flight renewal and watchdog
cancellation. It cannot eliminate a genuinely ambiguous database/network
failure; Flow remains at-least-once, and the durable fence remains the final
authority.

## 7. Watchdog and recovery

### 7.1 Make watchdog timing active-command-aware

The existing watchdog interval derives from the global 60-second lease and can
sleep past a short command's local expiry. Keep one `runLeaseWatchdog` service,
but replace its global ticker with a timer aimed at the earliest conservative
local expiry.

It waits on that timer, the same active-registry change notification, or
runtime cancellation. When it wakes, it cancels all expired, non-cancelled
attempts that are not currently renewing, emits the existing observation, and
recomputes the next expiry.

This adds no service and performs no SQL. Do not poll every few milliseconds
when there are no short active attempts.

### 7.2 Maintenance stays structurally unchanged

`ProbeExpiredCommandLeases` and `RecoverExpiredCommandLease` already use the
durable queue expiry. A short command naturally appears earlier. Preserve:

- bounded maintenance pages and category-local drain pacing;
- per-run locking and candidate revalidation;
- current attempt/fence cleanup;
- retry budget and fail-fast behavior; and
- runnable-only notification behavior.

Do not add a fast-recovery queue, a second maintenance loop, a heartbeat table,
or a special takeover query.

### 7.3 Shutdown remains bounded

The lease manager and watchdog remain runtime-owned services that drain before
`Run` returns. Their timers and in-flight calls must respond to runtime
cancellation. Existing shutdown cancellation must continue removing an attempt
from renewal snapshots so an aborting worker is not kept alive by fresh leases.

## 8. Documentation and developer experience

Update:

- `flow.go`, the package entrypoint, with the distinction between attempt
  timeout, recovery lease, duplicate execution, and fenced settlement;
- `definitions.go` API comments for `WithRecoveryLease`;
- `README.md` with one short safe/unsafe example and the 60-second fallback;
- functional, architecture, schema, engine/runtime, and durability specs;
- migration/schema inventory documentation; and
- Plan 12 implementation evidence after the work is complete.

Use plain wording:

> A recovery lease controls how soon another worker may retry this command if
> lease renewal stops. A shorter lease can cause concurrent duplicate handler
> execution, so use it only when repeating the worker is safe. Attempt fencing
> still permits only the current owner to durably settle.

Do not advertise it as exactly-once execution, a work timeout, a priority, or a
general performance switch.

For Trails API, inspect the current workers before opting in. Only explicitly
audited read-only/status-polling commands—or commands protected by their own
stable external idempotency key—should add the option. Do not mechanically add
it to every `txn.mine`, provider, or edge command merely because the name sounds
like polling. Any changed command declaration should also bump its version.

## 9. Implementation phases

### Phase 0: Reconcile and measure

1. Confirm Plan 13's accepted commit and clean worktree.
2. Inventory the current claim, renewal, watchdog, maintenance, replay, schema,
   and registration paths listed in this plan.
3. Record default-only claim and renewal query counts/cadence, a mixed same-run
   claim fixture, and dead-holder recovery timing using the current test seam.
4. Confirm the six-table baseline and no public command-lease runtime option.

### Phase 1: Durable declaration

1. Add option validation and command-default equivalence.
2. Add the nullable clean-baseline schema column and update command `CopyFrom`.
3. Thread the value through `CommandCreate`, root/child preparation,
   declaration fingerprint, `command_created`, validation, and replay.
4. Prove default omission, explicit persistence, duplicate-option rejection,
   duration normalization, rediscovery conflict, and malformed-history failure.
5. Review the phase diff and run focused definition/migration/replay tests.

### Phase 2: Mixed-duration batched claim

1. Load and validate the durable override in the locked claim batch.
2. Resolve per-command durations/expiries using the fixed runtime fallback.
3. Update queue/command projections set-wise and journal exact per-command
   `LeaseDurationMS` values.
4. Return `LeaseDuration` on internal claims and anchor local expiry from it.
5. Prove mixed short/default siblings claim in one transaction with exact
   journal/fence mapping, rollback, and ambiguous-commit behavior.
6. Review the phase diff and run focused PostgreSQL/race tests.

### Phase 3: Mixed-duration renewal and local scheduling

1. Move duration into each `LeaseRenewal` and preserve one statement.
2. Add active-command due/expiry state and its change notification.
3. Convert the existing manager to earliest-due scheduling with bounded
   error/uncertain retries.
4. Convert the existing watchdog to earliest-expiry scheduling and make it
   skip in-flight renewals.
5. Prove healthy short attempts renew through several windows, long attempts
   are not renewed at short cadence, slow/error renewal retries remain bounded,
   and shutdown still drains.
6. Review the phase diff and run focused PostgreSQL/race tests.

### Phase 4: Recovery, fencing, documentation, and release evidence

1. Prove short dead-holder recovery and default-command non-recovery in the
   same run and across competing replicas.
2. Prove the original holder cannot settle after takeover and exactly one
   durable settlement survives.
3. Exercise the renewal-result/watchdog boundary with a deterministic fault
   seam, including a committed renewal waiting for local application.
4. Update documentation/specs and perform a disposable Trails compile/focused
   test proof if a consumer candidate is used.
5. Run full PostgreSQL 17/18 ordinary and race gates with zero named skips,
   static analysis, schema/source scans, and bounded performance checks.
6. Record evidence and mark the plan complete only after independent review.

## 10. Required tests

### 10.1 Definition, durability, and replay

- unset, valid, duplicate, zero, negative, sub-floor, fractional-millisecond,
  and overflow option values;
- equivalent staged commands coalesce only when recovery leases match;
- declaration fingerprints change when the override changes;
- root and staged-child rows/journal bodies contain the exact nullable value;
- replay reconstructs the value and rejects malformed values;
- schema tests assert the nullable bigint/check constraint, six tables, and no
  additional index or migration; and
- fresh migration followed by ordinary runtime use succeeds on PostgreSQL 17
  and 18.

### 10.2 Claim

- default command uses the runtime fallback;
- explicit command uses its durable override;
- short and default siblings in one run claim in one batch;
- per-row queue expiry and `attempt_started.LeaseDurationMS` are exact;
- returned `LeaseDuration`, DB expiry, and conservative local expiry agree;
- mixed versions/queues/event inputs and locked siblings retain existing
  behavior;
- injected failures roll back every fence, projection, and journal row; and
- ambiguous commits preserve duration while transferring possibly owned
  attempts.

### 10.3 Renewal and watchdog

- one store call renews mixed durations with exact row ordering/results;
- a short active attempt wakes a sleeping manager immediately;
- a healthy short handler remains owned through at least three lease windows;
- default commands do not renew at the shortest command's cadence;
- one timeout/error retries before local expiry without a hot loop;
- `lost` cancels exactly the matching attempt; `uncertain` does not falsely
  extend it;
- watchdog skips a matching in-flight renewal and applies the committed result
  before reconsidering expiry;
- a bounded hung renewal eventually stops shielding an actually expired
  attempt;
- shutdown removes aborting attempts from future renewal snapshots; and
- the race detector reports no registry/timer/cancellation races.

### 10.4 Recovery and fencing

- an abandoned short command is recovered within its lease plus bounded
  maintenance polling/scheduling variance;
- a sibling on the default lease is not recovered early;
- two replicas cannot both claim the same current fence;
- the recovered attempt rejects settlement from the stale holder;
- only one `attempt_started` per actual fence and one durable terminal result
  survive adversarial recovery; and
- retry budgets, attempt ordinals, queue slots, and run counters remain exact.

### 10.5 Commands

Run database-backed tests without skip mode:

```text
gofmt -w <changed Go files>
git diff --check
go build ./...
go vet ./...
go test -count=1 ./...
make test
```

Also run the repository's named-test no-skip audit and the focused PostgreSQL
17/18 suites used by the current release process.

## 11. Performance and simplicity gates

This feature is allowed to add one nullable command column and small local
timing state. It is not allowed to turn claims or renewals into per-command
database work.

Measure five samples where timing is noisy and record environment/durability
settings. Required gates:

1. A mixed same-run claim remains one transaction and the same bounded number
   of SQL statements as a uniform-duration claim.
2. A mixed due renewal remains one SQL statement, not one per duration.
3. A short command does not increase renewal SQL cadence for unrelated
   long-lease commands.
4. Default-only lifecycle and same-run claim medians do not regress more than
   10% against a contemporaneous parent comparison without investigation.
5. No timer/goroutine is allocated per command and idle runtimes do not wake at
   short-lease frequency.
6. Schema remains exactly six tables with no new index.

Prefer the smallest code that satisfies these gates. Do not introduce a
general timer wheel, priority heap, lease class registry, scheduler framework,
or configurable retry subsystem. The number of active handlers is already
bounded by worker concurrency, so a locked linear scan to find the earliest due
time is simpler and adequate unless measurements prove otherwise.

## 12. Acceptance criteria

Plan 12 is complete only when all of the following are true:

1. `WithRecoveryLease(d)` is a validated command definition option and
   `DefineCommand` still accepts no handler.
2. Unset commands retain the fixed 60-second production fallback.
3. The explicit override is durable in the clean schema, command-created
   journal, declaration fingerprint, and replay state.
4. Existing Flow data is intentionally unsupported; only the consolidated
   baseline is changed and reset instructions are clear.
5. Mixed-duration commands claim together in one transaction with per-command
   queue expiries and exact `attempt_started` lease durations.
6. Mixed due attempts renew in one statement with their own durations.
7. Renewal and watchdog timing react to the earliest active command without a
   goroutine/timer per attempt.
8. A healthy short-lease handler remains owned across repeated renewals.
9. Renewal errors retry within the remaining local window without unbounded
   PostgreSQL calls or a hot loop.
10. The watchdog cannot cancel an attempt merely because a matching known
    in-flight renewal committed before its result was locally applied.
11. A dead short-lease holder is recovered materially sooner than a default
    holder.
12. A stale holder cannot settle after takeover, and at most one durable
    terminal result exists.
13. Attempt IDs, lease tokens, run locks, settlement fences, retry budgets,
    queue slots, and run counters retain their existing semantics.
14. Maintenance gains no category, table, or special recovery path.
15. Default-only workloads retain their renewal cadence and stay within the
    bounded performance gate.
16. Documentation plainly warns that shorter leases permit duplicate handler
    execution and are for idempotent/replay-safe workers.
17. PostgreSQL 17/18 ordinary, race, build, vet, format, migration, replay,
    no-skip, and source-audit gates pass.
18. Independent final review finds no unresolved Critical or Moderate issue.

## 13. Non-goals

This plan does not:

- promise exactly-once handler execution;
- weaken or redesign the settlement fence;
- change the default production lease;
- add a public runtime lease option;
- add per-run, per-enqueue, per-attempt, or queue-wide lease overrides;
- infer idempotency from a command name or queue;
- add stable runtime identity or startup self-reclamation;
- add heartbeats, advisory ownership, a broker, a lease table, or a seventh
  table;
- alter command attempt timeouts, run deadlines, waits, retry policy, queue
  concurrency, or retention;
- implement Plan 11 inline calls;
- modify Trails API automatically; or
- preserve old schemas or history.

## 14. Alternatives rejected

### 14.1 Globally shorten the lease

Rejected. It would make every handler eligible for earlier duplicate execution,
including external side effects that are not idempotent. Safe commands should
opt in while unknown commands retain the conservative default.

### 14.2 Runtime registration-only lease

Rejected. A restart or rolling deployment could reinterpret an already durable
command from whichever worker definition happened to claim it. The recovery
window is a durable command declaration and must travel with the row/history.

### 14.3 Per-call override

Rejected. The same command worker would have different duplicate-execution
semantics depending on its caller. That is flexibility without a needed
capability and makes audits harder.

### 14.4 Split claim or renewal batches by duration

Rejected. PostgreSQL can update aligned per-row durations through `unnest` in
one statement. Splitting reintroduces extra transactions/queries and weakens
the set-oriented design without simplifying semantics.

### 14.5 One timer or renewal goroutine per attempt

Rejected. Worker concurrency is bounded, but the existing runtime services can
schedule the earliest due/expiry directly with less lifecycle and shutdown
machinery.

### 14.6 Stable-owner immediate reclamation

Deferred. A runtime identity that survives restart can reclaim same-owner work
quickly, but it does not help replacement containers or peer takeover and is
dangerous across partitions. The opt-in lease works for every failure mode
without introducing identity coupling.

### 14.7 Heartbeats or external liveness

Deferred. They can detect process death before a lease expires, but add durable
signals, partition policy, and operational machinery disproportionate to the
current need.

## 15. Stop conditions

Stop implementation and report if:

1. Plan 13's accepted source differs materially from the baseline in Section 2
   and this plan has not been reconciled;
2. mixed durations appear to require per-command claim/renewal SQL rather than
   aligned set-oriented input;
3. any change weakens attempt/token settlement fencing or permits two durable
   settlements;
4. safe watchdog behavior appears to require an unbounded database call;
5. a short lease causes unrelated default commands to renew at short cadence;
6. the implementation adds a service/goroutine per attempt, a new table, or a
   new maintenance category;
7. schema work begins preserving old development data or introducing a
   compatibility migration; or
8. PostgreSQL/race testing exposes unresolved ownership, shutdown, timer, or
   lock behavior.

## 16. Punchlist

### Reconcile and baseline

- [ ] Record the independently accepted Plan 13 commit and confirm a clean
  implementation branch.
- [ ] Re-audit definition, durable creation, claim, renewal, watchdog,
  maintenance, replay, and schema paths against that commit.
- [ ] Record contemporaneous default/mixed claim, renewal-cadence, and recovery
  baselines.

### Durable declaration

- [ ] Add and validate `WithRecoveryLease` with one-time upward millisecond
  normalization and the 30-millisecond technical floor.
- [ ] Add the override to command defaults, staging equivalence, durable create
  validation, and declaration fingerprints.
- [ ] Add nullable `flow_commands.recovery_lease_ms` to the consolidated
  baseline and update CopyFrom/catalog tests without adding a migration/index.
- [ ] Add the optional value to `command_created`, replay, and malformed-state
  validation.

### Claim

- [ ] Resolve per-command lease durations from durable rows with runtime
  fallback.
- [ ] Preserve one mixed-duration claim transaction and set-oriented
  journal/projection writes.
- [ ] Return internal `LeaseDuration` and anchor conservative local expiry from
  the exact claimed value.
- [ ] Prove mixed, rollback, locked-sibling, and ambiguous-commit claim cases.

### Renewal and watchdog

- [ ] Move duration into each `LeaseRenewal` and retain one mixed renewal SQL
  statement.
- [ ] Track lease duration, next due time, local expiry, retry state, and
  in-flight renewal in the bounded active-command registry.
- [ ] Add the lightweight registry change notification used by both existing
  services.
- [ ] Convert the manager to earliest-due batching with bounded retries inside
  the remaining local lease window.
- [ ] Convert the watchdog to earliest-expiry timing and exclude matching
  bounded in-flight renewals.
- [ ] Preserve shutdown cancellation/drain and observation semantics.

### Recovery and fencing

- [ ] Prove healthy short handlers renew across repeated windows.
- [ ] Prove dead short holders recover sooner while default siblings do not.
- [ ] Prove competing replicas, takeover, stale settlement rejection, and one
  durable terminal result.
- [ ] Prove the committed-renewal/local-watchdog boundary with a deterministic
  race test.

### Documentation, performance, and closure

- [ ] Update `flow.go`, README, API comments, and active functional,
  architecture, schema, runtime, and durability specs.
- [ ] Audit any Trails opt-in candidate rather than applying the option by
  command/queue name.
- [ ] Prove one mixed claim transaction, one mixed renewal statement,
  default-only cadence isolation, idle efficiency, six tables, and no new
  index.
- [ ] Run PostgreSQL 17/18 ordinary/race/no-skip gates plus format, build, vet,
  migration, replay, and source scans.
- [ ] Record implementation/performance evidence and complete all 18 acceptance
  criteria.
- [ ] Obtain independent final review before marking this plan complete.
