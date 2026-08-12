# Plan 12: Fast lease recovery for idempotent commands

Status: Proposed

Planned at: `d0d873d` on 2026-08-12

- **Branch:** `feat/fast-lease-recovery`
- **Priority:** P2 — recovery latency, not correctness
- **Effort:** M
- **Risk:** MEDIUM; the change is small but touches the claim path and the
  lease field that fencing and maintenance recovery read, so it requires
  careful PostgreSQL and race testing
- **Depends on:** Plan 7 lease/maintenance fixes (already complete)
- **Public API impact:** additive only — one new command definition option and
  its documentation

> **Executor instructions:** Read this plan completely before editing. This
> plan changes *how long* a command's lease lasts, never *what a lease
> protects*. The settlement fence (attempt ID plus lease token) and the
> at-least-once contract must remain exactly as they are. If any step appears
> to require changing the fence, the claim's owner identity, or the
> at-most-one-durable-settlement guarantee, STOP and report rather than
> proceeding.

## 1. Purpose

When a worker process dies mid-attempt, the command it held stays leased until
the lease expires; only then does the maintenance sweep recover it and let
another worker (or the restarted process) re-claim it. Today the lease is a
single runtime-global value that defaults to `60s`
(`runtime.go` `commandLease`, set via `WithCommandLease`). Every interrupted
command therefore costs up to a full lease window of recovery latency,
regardless of how cheap it would be to simply re-run.

Observed in production integration testing: a process was killed while a
transaction-monitoring command was polling for an on-chain receipt. The
underlying transaction had already mined within seconds, but the owning
execution did not progress for ~60 seconds — the exact lease window — because
the dead process's lease had to expire before the work could be reclaimed. The
recovery was correct; the latency was pure dead time.

The command in that case was a read-only poll: re-running it is idempotent and
cheap. Nothing about it needs the conservative window that a non-idempotent,
side-effect-producing command needs. This plan lets a command declare that it
tolerates fast recovery, so idempotent work gets a short lease and recovers
promptly while side-effecting work keeps the conservative default.

```text
DefineCommand(..., WithRecoveryLease(5*time.Second))  // idempotent poll
DefineCommand(...)                                     // default 60s lease
```

## 2. Controlling decisions

### 2.1 The lease bounds duplicate work, not durable settlement

This is the decision the whole plan rests on, and it must be stated first
because it is what makes a shorter lease safe.

Flow already fences *durable settlement* with the attempt ID and lease token,
independently of the lease window (`flow.go`: "settlement fencing … remains"
after "the lease window expires"). A worker whose lease has expired cannot
settle its command over a takeover: its settlement is rejected by the fence.
Two workers may briefly run the same command concurrently after a short lease,
but **at most one can durably settle it**, exactly as today.

Therefore the lease length does not control correctness. It controls only how
much *duplicate worker execution* can occur during the window where the
original holder is unreachable but not yet known-dead. Shortening a lease can
never cause double settlement; it can only cause a second worker to redo work
whose result the fence will discard.

The cost of that duplicate work is what differs per command:

- an idempotent read (poll a receipt, re-check a status) costs one extra cheap
  call and is otherwise invisible;
- a non-idempotent external side effect (submit a payment, call a provider
  that is not itself idempotent) costs a duplicated real-world action, which
  the application may or may not absorb.

So the lease window should be chosen per command by the cost of redoing that
command's work, not by a single global default.

### 2.2 Declare the recovery lease at command definition, not per execution

A command's re-run cost is a property of its worker, which is fixed at
definition time. It does not vary per execution or per call site. The
declaration therefore belongs on `DefineCommand`, alongside the queue and other
static command properties, not on `Execute`.

Per-execution or per-call lease overrides are rejected (Section 11): they would
let the same worker run under different windows in different code paths, making
the duplicate-work bound unpredictable and the semantics harder to reason about.

### 2.3 The declaration asserts idempotent recovery, not "unimportant"

The option means: *this command's worker is safe to run again concurrently once
a short idle window has passed.* It is an application assertion about the
worker's re-run safety, not a priority or importance signal. Documentation must
frame it this way so callers do not attach a short lease to a command whose
duplicate execution has real external cost.

### 2.4 The global default is unchanged and remains the safe fallback

Any command that does not declare a recovery lease keeps the runtime default
(`60s`, or whatever `WithCommandLease` sets). Undeclared commands behave exactly
as they do today. The conservative default is correct for the unknown case; a
short lease is strictly opt-in.

### 2.5 Name the option `WithRecoveryLease`

The option takes an explicit duration:

```go
func WithRecoveryLease(d time.Duration) CommandOption
```

`WithRecoveryLease` is preferred because it names what the value governs — how
quickly the command is recovered after its holder goes silent. Rejected
alternatives:

- `WithLease` reads as the total lease budget and hides that it is a recovery
  window, inviting misuse as a general timeout.
- `WithIdempotent` / `WithConcurrentSafe` assert a property but hide that the
  observable effect is a shorter recovery window and force a fixed hidden value.
- `WithShortLease` describes the mechanism, not the intent, and bakes a
  qualitative word into an API that takes a quantity.

An explicit duration keeps the primitive small and lets the application choose
a window matched to its own poll cadence and partition tolerance.

### 2.6 The lease is applied at claim time from the command's declaration

The claim already writes a per-row `lease_expires_at`. The only change is which
duration produces it: the claimed command's declared recovery lease when set,
otherwise the runtime default. No new lease storage, owner identity, or
recovery loop is introduced — the existing `ProbeExpiredCommandLeases` /
`RecoverExpiredCommandLease` maintenance path recovers a short-lease command
sooner purely because its `lease_expires_at` is sooner.

## 3. User model

### 3.1 Declaring a fast-recovery command

```go
var pollReceipt = flow.DefineCommand[PollArgs, flow.None](
	"txn.mine",
	pollReceiptWorker,
	flow.WithQueue("txn.mine"),
	flow.WithRecoveryLease(5*time.Second),
)
```

`pollReceiptWorker` polls a chain receipt. Re-running it after a short idle
window simply re-polls; the settlement fence guarantees only one attempt
durably settles. If the holder dies, the command is recoverable ~5s later
instead of ~60s later.

### 3.2 Leaving a command on the default

```go
var submitPayment = flow.DefineCommand[SubmitArgs, flow.None](
	"txn.send",
	submitPaymentWorker,
	flow.WithQueue("txn.send"),
)
```

`submitPaymentWorker` performs a non-idempotent external submission. It declares
no recovery lease and keeps the conservative default window, so a partition
does not let a second worker resubmit until the full lease has elapsed.

### 3.3 Choosing the window

The recovery lease should be at least a few multiples of the worker's own
expected attempt duration plus its renewal cadence, so a healthy in-progress
attempt is not treated as dead. It is a floor on recovery latency, not a
deadline on the work: a live worker renews its lease and continues past the
window. Applications should pick the smallest window that comfortably clears a
normal healthy attempt.

## 4. Semantics

### 4.1 Claim

When a command is claimed, its `lease_expires_at` is `now()` plus the command's
declared recovery lease if set, otherwise the runtime default lease. All other
claim behavior — owner assignment, attempt creation, batch grouping — is
unchanged.

### 4.2 Renewal

A live worker renews on the existing cadence. Renewal must extend by the same
per-command lease used at claim, so a healthy short-lease worker stays owner
across long work. The renewal interval logic already derives from the lease;
it must derive from the command's lease, not the global, for a short-lease
command (Section 5.3).

### 4.3 Recovery

The maintenance sweep recovers any command whose `lease_expires_at` is in the
past. A short-lease command becomes eligible sooner; the recovery transition,
fence, and re-claim are otherwise identical. No new sweep or schedule is added.

### 4.4 Fence interaction (unchanged)

A recovered command is re-claimed under a new attempt and lease token. If the
original holder is still alive and later tries to settle, the fence rejects it
because its attempt/lease token no longer matches. This is the existing
behavior and is exactly what makes a short lease safe: earlier recovery widens
only the concurrent-execution window, never the settlement window.

### 4.5 Partition behavior

During a network partition, a short-lease command may be re-claimed and run by
a second worker while the first is still executing it. Both may perform the
command's side effects (e.g. both poll the same receipt). Only one settles.
Applications must only attach a recovery lease to workers whose side effects
tolerate this concurrent duplication — which is the definition in 2.3.

### 4.6 No change to waits, deadlines, or run lifecycle

The recovery lease governs command lease expiry only. Command event waits, run
deadlines, retry backoff, and terminal-state rules are untouched.

## 5. Minimal implementation

### 5.1 Command definition option

Add `WithRecoveryLease(d time.Duration) CommandOption` and store the value on
the command definition next to its queue. A zero or unset value means "use the
runtime default." Reject a negative duration at definition time.

### 5.2 Thread the declared lease into the claim

The claim currently applies `r.commandLease` to the batch. Change the claim so
each claimed row's `lease_expires_at` is computed from that command's declared
recovery lease, falling back to the runtime default when unset. The candidate
the claim already carries is the natural place to surface the per-command
value; the store computes expiry per row.

### 5.3 Use the command lease for renewal and local deadline

The renewal timeout and local-lease-deadline math must use the claimed
command's lease, not the global, so a short-lease command renews often enough
to stay owner while healthy. Reuse the Plan 7 local-deadline discipline (claim
round-trip time is subtracted, not added) with the per-command value.

### 5.4 No storage, owner, or scheduler changes

This plan requires:

- no schema migration (the `lease_expires_at` column already exists);
- no change to lease ownership or `replicaName`;
- no new maintenance probe, page, or loop;
- no change to the settlement fence or attempt/lease-token identity;
- no new goroutine.

If the implementation appears to need any of these, stop and reassess against
Section 2 before expanding scope.

## 6. Documentation updates

- `doc.go` and the command definition API comments: document
  `WithRecoveryLease`, its meaning (recovery window, not a work deadline), and
  the sharp edge in 2.3 / 4.5 that a shorter lease permits concurrent duplicate
  execution and must only be used for idempotent workers.
- `README.md`: a short subsection under lease/recovery explaining that the
  lease bounds duplicate work and not durable settlement, and that idempotent
  commands may opt into faster recovery.
- Architecture and runtime component specs: note the per-command recovery lease
  and reaffirm that the settlement fence is independent of the lease window.
- Cross-reference Plan 7; add a short note there only if its lease wording would
  otherwise contradict a per-command lease.

## 7. Tests

### 7.1 Definition and default tests

- `WithRecoveryLease` stores the duration on the definition.
- An undeclared command claims with the runtime default lease.
- A negative duration is rejected at definition time.

### 7.2 Claim and renewal tests (PostgreSQL)

- A claimed short-lease command has `lease_expires_at ≈ now + declared lease`.
- A claimed default command has `lease_expires_at ≈ now + runtime lease`.
- A healthy short-lease worker renews and retains ownership past its declared
  window (proves renewal uses the per-command lease).

### 7.3 Recovery-latency tests (PostgreSQL)

- Simulate a dead holder (claim, then abandon without settling) for a
  short-lease command and a default command in the same run; assert the
  short-lease command is recovered and re-claimable materially sooner, bounded
  by its declared window plus one maintenance interval.
- Assert the default command is not recovered before its full window.

### 7.4 Fence-safety tests (PostgreSQL, race)

- After a short-lease command is recovered and re-claimed, a settle attempt
  from the original (stale) holder is rejected by the fence.
- At most one durable settlement occurs even when the original holder and the
  recovering worker both run the worker to completion.
- Repeat under `-race`.

### 7.5 Regression suite

```text
go test ./...
go test -race ./...
go vet ./...
```

Run database-backed tests against the local PostgreSQL instance, not in skip
mode. Retain the Plan 7 lease/maintenance tests unchanged and confirm they
still pass with a per-command lease in play.

## 8. Acceptance criteria

This plan is complete when:

1. `WithRecoveryLease(d)` exists as an additive command definition option.
2. A command with a declared recovery lease claims, renews, and is recovered on
   that window; an undeclared command is unchanged from today.
3. The claim writes a per-command `lease_expires_at`; renewal and local
   deadline use the same per-command lease.
4. The settlement fence, attempt/lease-token identity, owner assignment, and
   at-least-once contract are byte-for-byte unchanged.
5. A recovered short-lease command still fences a stale settlement from its
   original holder; at most one durable settlement occurs.
6. No schema migration, new maintenance loop, or new goroutine is added.
7. Documentation states that the lease bounds duplicate work rather than durable
   settlement and that a recovery lease is only for idempotent workers.
8. The full PostgreSQL-backed and race suites pass, including the retained
   Plan 7 tests.

## 9. Non-goals

This plan does not:

- change the settlement fence or make recovery faster for non-idempotent work;
- add a liveness/heartbeat channel or platform death signal;
- add per-execution or per-call lease overrides;
- add startup self-reclamation by a stable runtime identity (Section 11.3);
- change the global default lease or `WithCommandLease`;
- add lease classes, priorities, or a scheduler;
- alter waits, run deadlines, retry backoff, or replay.

## 10. Alternatives rejected

### 10.1 Globally shorten the default lease

Rejected. It would speed recovery everywhere but also widen the concurrent
duplicate-execution window for non-idempotent commands (e.g. external
submissions), increasing wasted or externally-visible duplicate side effects.
The safe global default must stay conservative; fast recovery is opt-in per
command.

### 10.2 Per-execution or per-call lease

Rejected (2.2). Re-run cost is a property of the worker, fixed at definition.
Per-call windows make the duplicate-work bound depend on the call site and are
harder to reason about, without adding capability.

### 10.3 Startup self-reclamation by stable identity

Considered and deferred. If the runtime had an identity that survived restart,
a booting process could immediately reclaim leases it previously held, giving
instant recovery for same-node restarts. It does not generalize: a replaced
process (new container, new identity) cannot recognize the prior holder's
leases, and reclaiming by a shared identity across a partition would break the
duplicate-work bound. The per-command recovery lease helps every recovery path
(restart and peer takeover) without introducing identity coupling, so it is the
better primitive to ship first. Startup reclamation may be revisited separately
if same-node restart latency remains a concern.

### 10.4 Liveness heartbeat to detect death faster

Rejected for this plan. A heartbeat that lets a peer detect a dead holder
before the lease expires would speed recovery for all commands, but it adds a
new durable signal, a new failure mode (heartbeat partition), and materially
more machinery than the observed problem warrants. The per-command lease is the
minimal change that fixes the observed latency safely.

## 11. Implementation sequence

1. Add focused failing tests: the definition option and default; claim writing a
   per-command `lease_expires_at`; a short-lease command recovering sooner than
   a default command; a stale settlement from the original holder being fenced
   after recovery.
2. Add `WithRecoveryLease` and store it on the command definition.
3. Thread the per-command lease through claim, renewal, and local-deadline math,
   defaulting to the runtime lease when unset.
4. Update `doc.go`, `README.md`, and the architecture/runtime specs.
5. Run the full PostgreSQL-backed, race, and vet suites, including the retained
   Plan 7 tests.
6. Confirm the production diff contains no fence, owner-identity, schema,
   scheduler, or new-goroutine changes.

If any step begins changing what a lease protects rather than how long it lasts,
return to Section 2.1 and stop.
