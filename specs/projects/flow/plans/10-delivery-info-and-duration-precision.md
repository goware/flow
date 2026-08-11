# Plan 10: Delivery-info identity and a public duration precision policy

Status: Proposed

Planned at: `3d2b29b` (v0.2.0) on 2026-08-11

- **Branch:** create an implementation branch from `master` at or after
  `3d2b29b`
- **Priority:** P2; both findings have working caller-side workarounds today,
  but every embedder rediscovers them independently and one of them costs a
  database read per delivery
- **Effort:** S/M
- **Risk:** LOW; one additive struct field sourced from an already-joined row,
  and one exported pure function plus documentation — no durable format,
  fencing, scheduling, or settlement change
- **Depends on:** none strictly; Section 4.1 shares the claim-projection
  widening designed in Plan 9 Phase 1, so whichever plan lands first carries
  that change and the other consumes it
- **Public API impact:** additive — two new `CommandInfo` fields and one new
  exported helper; the exact-milliseconds validation contract on existing
  entry points is unchanged
- **Durable format impact:** none

> **Executor instructions:** Read this plan completely before editing. Work in
> phase order and run each phase's focused gate before continuing. This plan
> adds one identity projection and one convenience-with-a-contract; it is not
> permission to change the durable millisecond representation, loosen the
> exact-precision validation on any existing entry point, or grow `Work` into
> a general context object. When a STOP condition occurs, stop and report it
> rather than weakening a validation or fencing contract.
>
> **Drift check (run first on the implementation branch):**
>
> ```text
> git status --short --branch
> git log -1 --oneline
> git diff --stat 3d2b29b..HEAD -- \
>   types.go worker.go command_runtime.go execute.go errors.go \
>   definitions.go internal/store/commands.go internal/durable/types.go \
>   internal/failure/failure.go internal/retry/retry.go \
>   README.md doc.go specs/projects/flow
> ```
>
> If these paths changed, compare the live `CommandInfo` construction, the
> claim projection, and the duration validation sites with Sections 2 and 4
> before proceeding. If Plan 9 Phase 1 already landed, Section 4.1 reduces to
> exposing fields the claim already carries; verify rather than re-implement.

## 1. Purpose

Two small gaps push per-consumer workaround code into every embedding
application. Both belong in Flow because each is a property of Flow's own
delivery contract, not of any application's domain.

1. **A delivered command does not know its execution's identity.** Handlers
   that key their domain work on the execution key must issue a
   `GetExecution` read on every delivery to learn it. That is a database
   round trip per delivery to fetch a value the claim transaction already
   had in scope.
2. **Flow's duration precision policy is validated but not exported.**
   Public entry points reject sub-millisecond durations fail-closed, yet
   Flow's own retry internals round their computed delays to whole
   milliseconds. Callers that compute durations arithmetically must each
   rediscover and re-implement the same normalization helper, and each
   invents its own rounding direction.

## 2. Confirmed findings and why they are worth fixing

### 2.1 `CommandInfo` omits execution key and definition name

`CommandInfo` (`types.go:114-125`) carries `ExecutionID`, `CommandID`,
`CommandKey`, `Name`, `Version`, and attempt timing. It does not carry the
execution key or the root definition name. The construction site
(`command_runtime.go:430-435`) copies fields from the claimed command, and
`ClaimedCommand` (`internal/store/commands.go:138-160`) does not select
either value — although the claim query already joins the execution row for
fencing, so the values are in reach without new table access.

The consequence is a per-delivery read amplification pattern: a handler
whose settlement or fencing logic is keyed by execution key calls
`GetExecution(ctx, rt, info.ExecutionID)` inside the handler
(observed in the first production embedder's queue adapter, which performs
this read on every keyed delivery solely to obtain the key). Under a queue
draining thousands of deliveries, that is thousands of avoidable point
reads against the same database that serves claims and settlements.

The alternative available to applications today — duplicating the key into
the command's argument payload — changes the durable argument shape and
therefore needs a versioned cutover per application. A projection field is
strictly better: non-durable, additive, and correct for already-persisted
commands the moment the binary upgrades.

`Commit[A, R]` (`worker.go:25-29`) embeds the same `CommandInfo`, so commit
callbacks that write application rows keyed by execution identity gain the
same benefit with no further change.

### 2.2 The exact-milliseconds contract is asymmetric

Flow persists durations as whole milliseconds and enforces exactness at
every public boundary: `durable.ExactMilliseconds`
(`internal/durable/types.go:54-62`) returns `ErrInvalid` for any duration
with sub-millisecond remainder, applied to execution deadline, start delay,
and event windows (`execute.go:71,142,172,443-451`), sub-command initial
delay, attempt timeout, and windows (`execute.go:568-576`), command attempt
timeout at definition time (`definitions.go:143`), retry policy bounds and
backoff delays (`internal/retry/retry.go:99-107,225-236`), and the
`RetryAfter` delay (`internal/failure/failure.go:88` via
`ValidateRetryAfter`).

Fail-closed validation on the durable boundary is correct and this plan
keeps it. The asymmetry is that Flow's own computed delays do not live under
that regime: the jitter calculation (`internal/retry/retry.go:408-419`)
rounds its floating-point result to whole milliseconds and clamps to a
one-millisecond minimum. So a policy exists — round computed durations,
never below one millisecond — but it is private. Callers in the same
position (computing backoff, subtracting elapsed time from a budget,
scaling a base delay) hit `ErrInvalid` at runtime, typically first in
production where delays are computed rather than constant, and each writes
the same helper. The first production embedder's version rounds *up*,
reasoning that delivering earlier than a caller's requested delay violates
the request; Flow's internal version rounds to nearest. Two consumers of the
same library should not have to guess, and should not be able to disagree.

The fix is not to accept arbitrary durations at the durable boundary —
silent normalization inside `Execute`/`RetryAfter` would change observed
behavior of existing correct callers and blur a deliberate contract. The fix
is to export the normalization as a named, documented function with one
chosen rounding rule, and state the contract in the specification.

## 3. Hard boundaries

### In scope

- `types.go` — two additive `CommandInfo` fields.
- `internal/store/commands.go` — claim projection carries execution key and
  definition name (shared with Plan 9 Phase 1; implement once).
- `command_runtime.go` — populate the new fields at `CommandInfo`
  construction.
- `errors.go` or a small public file — one exported duration normalization
  helper and its documentation.
- Focused tests for both; claim benchmark comparison for the projection.
- `README.md`, `doc.go`, `specs/projects/flow/functional_spec.md`,
  `specs/projects/flow/components/runtime.md` — document the new fields and
  the precision contract.

### Out of scope — reject during review

- Changing the durable millisecond representation or widening it to
  microseconds/nanoseconds.
- Loosening or removing `ExactMilliseconds` validation at any existing
  entry point; implicit normalization inside `Execute`, `WithStartDelay`,
  `RetryAfter`, or retry policies.
- Growing `CommandInfo` beyond the two identity fields — no metadata, no
  execution status, no payload-adjacent data.
- Adding methods to `Work` that read from the database.
- Reordering or redesigning the claim query beyond the additive column
  selection.

## 4. Design

### 4.1 Execution identity on `CommandInfo`

Add two fields:

- `ExecutionKey string` — the execution's application-chosen key, empty
  when the execution was started without one;
- `Definition string` — the execution's root definition name.

Source both from the claim projection. The claim query already locks and
reads the owning execution row for fence checks; select the two columns
there and carry them through `ClaimedCommand`. Populate them at the single
`CommandInfo` construction site (`command_runtime.go:430-435`). `Commit`
inherits them through its embedded `CommandInfo`.

This is the same projection widening Plan 9 Phase 1 specifies for
observation dimensions. Implement it once: if Plan 9 landed first, this
phase only adds the two public fields and their population; if this plan
lands first, Plan 9 consumes the carried values. Either order, the claim
benchmark gate from Plan 9 (comparison against the Plan 5 baseline
conditions, evidence recorded in `benchmark_evidence/`) applies to
whichever plan performs the widening.

Naming must match Plan 9's `Observation` fields (`ExecutionKey`,
`Definition`) so the two surfaces stay one vocabulary.

Keys are documented non-secret identifiers (`architecture.md:389`); no new
information class crosses the boundary.

### 4.2 Exported duration normalization

Export one pure function, name to be settled in review (working name
`ExactDuration`):

- rounds a positive duration **up** to the next whole millisecond;
- clamps any duration below one millisecond (including zero) to exactly one
  millisecond;
- rejects negative durations with `ErrInvalid`, matching the durable
  boundary.

Rounding up is the correct public rule because every duration Flow accepts
is a minimum wait: a start delay, a retry delay, a timeout budget, a
deadline. Delivering or firing *earlier* than requested violates the
caller's intent; later by under a millisecond does not. Flow's private
jitter rounding (`internal/retry/retry.go:408-419`) is round-to-nearest on
an already-randomized value, where the direction is immaterial; it may stay
as is or adopt the helper — implementation's choice — but the public
contract is round-up.

The helper performs no I/O and does not change validation anywhere:
`WithStartDelay(ExactDuration(computed))` passes because the helper's
output always satisfies `ExactMilliseconds`; `WithStartDelay(computed)`
with a ragged duration still fails closed. Document the pairing on every
duration-accepting option and on `RetryAfter`.

Specification changes: `functional_spec.md` gains a short "duration
precision" statement — durable durations are whole milliseconds, exactness
is validated fail-closed at every entry point, and `ExactDuration` is the
supported normalization with round-up semantics — replacing the current
scattered per-option mentions.

### 4.3 Rejected alternatives

Recorded to prevent re-litigation:

- **Normalize implicitly at entry points.** Rejected: silently mutating a
  caller-supplied value at a validated durable boundary hides caller bugs
  (a nanosecond-scale delay from a unit mix-up becomes a 1ms delay instead
  of an error) and changes the documented fail-closed contract.
- **Store sub-millisecond durations.** Rejected: a durable format change
  with migration cost, for precision the scheduler cannot honor — polling
  and lease granularity are orders of magnitude coarser.
- **Expose execution key via a `Work` method that reads the store.**
  Rejected: hides a per-call database read behind a field-access-shaped
  API; the projection makes it free instead.
- **Let applications carry the key in their argument payloads.** Rejected
  as the recommended path: it duplicates identity Flow already owns and
  forces a versioned durable argument change on every application.

## 5. Phases

### Phase 1 — Claim projection and `CommandInfo` fields

Widen the claim projection (or consume Plan 9's widening), add and populate
the two `CommandInfo` fields, and cover: keyed execution delivers its key
and definition to the handler and to `WithCommit`; key-less execution
delivers empty key with correct definition; replayed/retried attempts carry
identical values.

Gate: focused command-runtime and store tests with `-race`; claim benchmark
comparison recorded in `benchmark_evidence/` against the Plan 5 baseline
conditions if this plan performs the widening.

### Phase 2 — `ExactDuration`

Add the exported helper with exhaustive unit tests (zero, sub-millisecond,
exact, ragged, negative, near-overflow), property test that its output
always satisfies `durable.ExactMilliseconds`, and verify no existing
validation site changed behavior.

Gate: helper tests plus the durable contract test suite unchanged and
passing.

### Phase 3 — Documentation

`functional_spec.md` duration-precision statement and `CommandInfo` field
documentation; `components/runtime.md` delivery-info update; README and
`doc.go` mentions; ensure Plan 9's vocabulary (if landed) and this plan's
field names agree.

Gate: spec build (`specs/projects/flow/Makefile`) and full
`go test -race ./...`.

## 6. STOP conditions

Stop and request review if:

1. carrying execution identity through the claim measurably regresses the
   Plan 5 claim baseline beyond noise and Plan 9 has not already accepted
   that cost;
2. any pressure appears to normalize durations implicitly at a validated
   entry point;
3. `CommandInfo` growth beyond the two identity fields is proposed to ride
   along;
4. the field values cannot be made consistent across retried attempts of
   the same command without extra reads.

## 7. Done criteria

- [ ] `Work.Info()` and `Commit.Info` expose `ExecutionKey` and
      `Definition` on every delivery, sourced from the claim with no
      additional query, empty-key semantics documented.
- [ ] A handler needing its execution key performs zero database reads to
      obtain it.
- [ ] `ExactDuration` is exported, documented with round-up-and-clamp
      semantics, rejects negatives, and its output always passes
      `ExactMilliseconds`.
- [ ] No existing entry-point validation behavior changed; durable format
      unchanged.
- [ ] Claim benchmark evidence recorded (or referenced from Plan 9's) with
      no regression beyond noise.
- [ ] Specs, README, and `doc.go` state the duration precision contract in
      one place.
- [ ] Full `go test -race ./...` passes.

## 8. Deferred follow-ups

- **Definition-level duration options audit.** `WithTimeout` and retry
  policies validate at definition time, where durations are typically
  constants; if computed definition-time durations become common, revisit
  whether the docs should steer those sites to `ExactDuration` too.
- **Execution metadata on delivery.** Deliberately not included; if a real
  consumer demonstrates a need for start metadata at delivery time, that is
  its own plan with its own projection-cost analysis.
