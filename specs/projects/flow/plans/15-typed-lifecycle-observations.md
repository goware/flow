# Plan 15: Make observations an alertable lifecycle contract

Status: Partially implemented — embedder-facing subset shipped (#30);
Plan 14 anchors synchronized 2026-08-12

Planned at: `d9125dc` (v0.4.0, post-Plan 13) on 2026-08-13

## Implementation status (#30, branch `feat/plan-15-observations`)

The subset consumed by the first production embedder (trails-api plan 012)
is implemented; the rest of this document is retained as the design record
for the deferred remainder.

Shipped:

- `Observation.RunKey` and `Observation.RootCommandName` (Section 4.1), populated
  on run-scoped ingress, claim/attempt/event paths, and the maintenance pages
  from values already held in memory or columns the probes already read. Plan 14's
  lock/time/snapshot statement in `AttachSemantic` was widened by
  `definition_name` (Section 4.2's projection change), which reaches the
  claim path through `InitialLockedSnapshot` and is carried on
  `ClaimedCommand.DefinitionName`.
- Exported constants for the terminal tuple vocabulary only
  (`ObservationOpTerminal`, `ObservationOpConclude`,
  `ObservationOpConcludeExhausted`, `ObservationOpExpire`,
  `ObservationOpRecover`, `ObservationOpLocalCancel`,
  `ObservationOpObserver`, `ObservationOpCancel`, and the terminal
  `ObservationOutcome*` values). String values equal the previously emitted
  literals.
- The Section 4.4 terminal emissions: run terminal from both settlement
  paths (surfaced via additive `SettleResult` fields — `TerminalRun`,
  `RunStatus`, `RunKey`, `Definition`, plus `StopReason` for the retry
  decision), `CancelCommand`/`CancelRun` (via additive `CancelResult`
  fields), wait-deadline expiry (`wait`/`expire`/`expired`, resolving the
  previously unused `ObservationWait` kind), the run-deadline maintenance
  page (per expired run, page-bounded), and per-command
  `lease`/`recover`/`recovered` from the recovery page.
- `conclude_exhausted`: a terminal failed conclusion caused by budget
  exhaustion (`attempt_limit`, `elapsed_limit`,
  `deadline_before_next_attempt`) is a distinct operation; a permanent
  classification remains `conclude`/`failed`.
- Section 4.5 delivery classes: one goroutine, one observer, 64 of the 1024
  slots reserved for terminal-class observations, per-class drop accounting,
  and a drain-time `runtime`/`observer`/`dropped_terminal` report.
- Minimal README, package, functional-spec, and architecture guidance now
  documents identity, terminal reserve/drop accounting, and durable-read
  reconciliation. The larger monitoring example remains deferred.

Deferred (not needed by the first embedder):

- `CommandInfo.Definition` (Section 4.2's second half).
- The full tuple registry, its exhaustiveness test, and converting
  duty-cycle emission sites to constants (Section 4.3). Duty-cycle sites
  still emit string literals; only the terminal vocabulary is pinned.
- `examples/monitor` and the remaining detailed component documentation
  (Sections 4.6, Phase 5).
- Claim benchmark evidence (the Phase 1 gate); Plan 14's claim benchmarks are
  the live baseline and cover the one-column widening.

This sits on Plan 14 (#27) as Section 0 requires. The additions ride Plan 14's
merged statements rather than adding their own: `definition_name` is one more
column on the lock/time/snapshot statement, and the `SettleResult` identity
fields come from the `commandFence` the settle paths already load. Plan 14's
§4.5 fast paths are preserved — the default observer is a nil adapter and
duty-cycle observation construction stays behind that nil check.

- **Branch:** create an implementation branch from `master` after Plans 12
  and 14 have landed; this plan assumes both are complete
- **Priority:** P1; embedding applications that must alert on lifecycle
  failures currently reconstruct those signals from log scraping and polling
  because the observer stream cannot carry them, and the first production
  embedder needs this before its cutover
- **Effort:** M
- **Risk:** LOW/MEDIUM; the emission sites and the claim projection are on the
  hot path, so the additions must be measured against the retained claim
  baseline (Plan 5 conditions, re-baselined if Plan 14's round-trip merges
  changed the claim shape), but no durable format, fence, or scheduling
  semantics change
- **Depends on:** Plan 8 (`WithObserver`, bounded shutdown drain, and the
  observer lifecycle are the base being extended); Plans 9 and 10 as released
  in v0.3.0, which renamed the public Execution family to Run and put `RunKey`
  on `ClaimedCommand` and `CommandInfo`; Plan 13 as released in v0.4.0, which
  reset the schema to a single migration, renamed the journal event class to
  `run_terminal`, and added `GetQueueStats`; Plan 12 (fast lease recovery) and
  Plan 14 (scheduler latency and round-trip reduction), both assumed complete
  — Section 0 states what each changes for this plan
- **Public API impact if the full plan is completed:** additive only — new
  `Observation` fields, one new `CommandInfo` field, exported vocabulary
  constants, and one documented delivery-class distinction; no signature
  changes and no new goroutine model
- **Durable format impact:** none; one additive widening of the claim-head
  SELECT (no schema change)

> **Executor instructions:** Read this plan completely before editing. Work in
> phase order and run each phase's focused gate before continuing. This plan
> adds dimensions, vocabulary, and terminal-fact coverage to the existing
> observer; it is not permission to build an alerting policy engine, a durable
> event outbox, a metrics library, or synchronous lifecycle hooks. Observer
> delivery stays asynchronous, best-effort, secret-free, and unable to block
> durable work or shutdown. When a STOP condition occurs, stop and report it
> rather than weakening those properties.
>
> **Drift check (run first on the implementation branch):**
>
> ```text
> git status --short --branch
> git log -1 --oneline
> git diff --stat d9125dc..HEAD -- \
>   observer.go observer_test.go runtime.go runtime_run.go \
>   command_runtime.go enqueue.go types.go worker.go \
>   internal/store/commands.go internal/store/graph.go \
>   internal/store/ingress.go examples/monitor \
>   README.md flow.go specs/projects/flow
> ```
>
> These paths WILL have changed, because Plans 12 and 14 land between this
> plan's baseline and its implementation. Before proceeding, re-verify every
> file:line anchor in Sections 2 and 4 against the implementation branch and
> compare the live emission sites and the `ClaimedCommand` projection with
> those sections. Treat a changed observer queue model, a new emission path
> not listed here, or a claim projection redesign as a STOP condition until
> this plan is updated.

## 0. Relationship to Plans 12 and 14 (assumed complete)

- **Plan 12** (fast lease recovery) changes when lease recovery runs, not
  what it is: the Section 4.4 per-command `lease`/`recover`/`recovered`
  emission attaches to whatever recovery path Plan 12 ships, bounded by its
  page size. If Plan 12 replaced the maintenance-page recovery loop, emit
  from its successor, one fact per recovered command.
- **Plan 14** (scheduler latency) touches this plan in three places. Its
  §4.3 now captures an explicit initial locked snapshot through
  `AttachSemantic`; Section 4.2 must add `definition_name` to that same
  materialized lock/time/snapshot statement, not restore the removed claim
  reread. Its settle-path projection pairs use `executeProjectionBatch`; the
  additive `SettleResult` field in Section 4.4 must ride existing result data
  without adding a statement or crossing a fault/callback boundary. Its §4.5
  represents the default observer with a nil adapter, guards profiled
  observation construction, caches pool capacity, and precomputes the replica
  name; the constant conversion in Section 4.3 must preserve those fast paths.
- **Benchmark baseline.** Use Plan 14's evidence at
  `benchmark_evidence/plan_14_scheduler_latency.md` for the Phase 1 gate. Its
  five-sample 16-command claim median is 2,080 commands/s, 598,985 B/op, and
  13,893 allocs/op.

## 1. Purpose

Plan 8 shipped `WithObserver` (`runtime.go:62-70`) as bounded operational
telemetry: an async single-goroutine adapter with a 1024-slot queue
(`observer.go:53`), drop-on-overflow accounting, panic isolation, and a
cancellable drain at shutdown (`observer.go:53-127`). That design is correct
and this plan does not reopen it.

What the observer cannot yet do is serve as the application's alerting
source. An embedding application that must page a human when a command
permanently fails, a run expires, or a lease is recovered needs four
properties the current stream lacks:

1. **Application identity.** `Observation` carries `RunID` and `CommandID`
   but neither the run key nor the definition name (`observer.go:26-40`).
   Applications correlate by key — `order:<id>`, not a UUID — so today an
   alert consumer must issue a read per observation to learn which domain
   entity it concerns. The v0.3.0 rename put `RunKey` on `ClaimedCommand`
   (`internal/store/commands.go:141`) and `CommandInfo` (`types.go:124`), so
   the attempt path now holds the key in memory and simply does not put it
   on the observation.
2. **A stable vocabulary.** Every emission site writes free-form
   `Operation`/`Outcome` strings (`command_runtime.go:75-78,284-288,456-460,
   514-528,571-575,693-697,711-715`; `runtime_run.go:242,269,287,307,319,
   330,334,339,419-420,443-444,449-450,453-454,476-477,571-572,642-643,
   665-666`; `enqueue.go:285-289,312-315,383-385,423-425,459-461`). The
   functional spec describes the stream's properties but does not enumerate
   the tuples, so consumers match magic strings with no compatibility
   promise. A renamed outcome silently blinds an alert.
3. **Terminal run facts.** `ObservationRun` is emitted only for `start`
   (`enqueue.go:312-315`), for the cancel/start pair inside
   `ReplaceCurrentRun` (`enqueue.go:285-289`), and for explicit `CancelRun`
   (`enqueue.go:459-461`). A run that reaches `failed` through command
   settlement, or `expired` through the maintenance deadline page
   (`runtime_run.go:578-596`, which observes only a per-page count),
   produces no run-level observation. Those are precisely the page-worthy
   edges.
4. **Loss visibility appropriate to the fact.** All observations share one
   queue and one drop policy (`observer.go:93-108`). Dropping a `probe`
   duration under load is fine; dropping the only `settle/expired` fact for
   a money-bearing command means the alert never fires and nothing records
   which fact was lost.

This plan closes those four gaps while preserving every Plan 8 boundary:
no payloads, results, SQL, connections, or lease tokens in observations;
best-effort delivery; and the explicit rule that durable truth remains the
read APIs and the application's own reconciliation, never the observer.

## 2. Confirmed findings and why they are worth fixing

All anchors below were verified against `d9125dc` (v0.4.0). Plans 12 and 14
land before implementation; re-verify per the drift check.

### 2.1 Observations cannot name the run they concern

`Observation` (`observer.go:26-40`) has `RunID`, `CommandID`, `CommandKey`,
`Name`, `Version`, `Queue`, and `Worker`, but neither the run key nor the
definition name.

The v0.3.0 claim path already closed half of the plumbing problem. Plan 14 now
selects `status,deadline_at,run_key` and the `RunHead` counters in the
`MATERIALIZED` locking CTE in `AttachSemantic`, then exposes them as
`InitialLockedSnapshot`. `ClaimCommands` stamps `RunKey` onto every
`ClaimedCommand` from that snapshot. So the attempt-path emission sites
already hold the run key: what remains is the definition name, which
`flow_runs.definition_name` carries but the claim-head SELECT does not read.

Run-path sites (`enqueue.go:285-289,312-315,459-461`) hold both the ID and,
on the start paths, the definition name and version, and set only some of
them. `CancelRun` and the `ReplaceCurrentRun` cancel half set neither name
nor key.

Keys are already documented as non-secret application identifiers
(`architecture.md`, Section 17), so adding them to observations introduces
no new information class.

### 2.2 The vocabulary is real but unwritten

The emitted tuples are already deliberate and bounded. The current set,
verified against every non-test `Observation{}` literal in the worktree:

- `claim` — `probe`/`ok|error`, `claim`/`ok|error`
  (`command_runtime.go:75-78,284-288`; `outcomeForError` at
  `command_runtime.go:749-754` produces exactly `ok` and `error`).
- `attempt` — `handler`/`ok|error`, `settle`/`succeeded|expired|error`,
  `conclude`/`<store status>|error`, where the store status is
  `retry_wait` or `failed` from the retry decision
  (`internal/store/commands.go:1533`), `succeeded`
  (`internal/store/commands.go:1243`), or `expired`
  (`internal/store/commands.go:994,1370`).
- `event` — `settle`/`accepted` per staged event
  (`command_runtime.go:514-518`), `deliver`/`created` on external delivery
  (`enqueue.go:383-385`).
- `run` — `start`/`created` (`enqueue.go:288-290,312-315`),
  `cancel`/`cancelled` (`enqueue.go:285-287,459-461`).
- `command` — `cancel`/`cancelled` (`enqueue.go:423-425`).
- `lease` — `renew`/`ok|partial|error`, `renew_result`/
  `renewed|lost|uncertain` (`internal/store/commands.go:700-704`),
  `local_cancel`/`lost|expired`
  (`runtime_run.go:419-420,443-444,449-450,453-454,476-477`).
- `runtime` — `run`/`started|stopped` (`runtime_run.go:242,269`);
  `notify_listener`/`connect_error|listening|reconnecting`
  (`runtime_run.go:287,307,319,339`); `notify_hint`/`received|broad_wake`
  (`runtime_run.go:330,334`); the maintenance probes
  `deadline_probe`/`wait_expiry_probe`/`lease_recovery_probe` with
  `ok|error` and the transitions `deadline`/`wait_expiry`/`lease_recovery`
  with `ok|noop|partial|error` (`runtime_run.go:638-667`);
  `maintenance_pass`/`blocked|drain` (`runtime_run.go:571-572`); and
  `observer`/`dropped`, the drain-time drop report (`observer.go:87`).

Two facts about that set matter for the registry design. `ObservationWait`
is declared (`observer.go:19`) and never emitted, so the registry must
either cover it or the constant is dead. And every ingress observation is
gated on `client.tx == nil` (`enqueue.go:283,310,382,422,458`): a
caller-owned-transaction client emits nothing, because Flow does not own the
commit. That gating is deliberate and stays.

None of this is normative. `functional_spec.md:583` promises "bounded
operational metadata" without saying which facts exist, and
`components/runtime.md:59` lists categories without tuples. Consumers
therefore couple to implementation strings that no test or document pins.

### 2.3 Terminal run transitions are unobserved

Run-terminal projection exists durably: `runTerminalEvent`
(`internal/store/ingress.go:1365-1377`) stamps a journal entry whose
`EventClass` is `run_terminal` — Plan 13's schema reset renamed the encoded
class alongside the event names (`flow.run_failed`, `flow.run_cancelled`,
`flow.run_expired`). Six paths raise it: successful settlement
(`internal/store/commands.go:1114-1147`), conclusion settlement
(`internal/store/commands.go:1411-1470`), wait-deadline failure
(`internal/store/graph.go:244-256`), command cancellation
(`internal/store/ingress.go:1155-1170`), run cancellation
(`internal/store/ingress.go:1292`), and run expiry
(`internal/store/commands.go:1740`).

None of them has an observer emission. Worse, the runtime cannot currently
detect the transition from the settle result: `SettleResult`
(`internal/store/commands.go:927-932`) has `Terminal`, but it means *command*
terminal, not run terminal — `terminalRun` is computed inside the store and
discarded. Surfacing the run-terminal edge therefore needs an additive
`SettleResult` field, not just a new emit call.

`runRunDeadlinePage` (`runtime_run.go:578-596`) expires runs one by one via
`ExpireRun` (`internal/store/commands.go:1616`) and reports only an aggregate
probe/transition count. An application alerting on "a run reached a terminal
failure" must today poll `ListRuns` (`inspection.go:174`) by status — which
is exactly the polling load the observer was added to remove.

### 2.4 One drop policy serves two loss tolerances

`emit` (`observer.go:93-108`) drops on a full queue and accounts a single
aggregate counter, reported once at drain (`observer.go:86-88`). High-volume
duty-cycle facts (probe, claim, handler, renew) dominate the queue and are
individually valueless; low-volume terminal facts are individually
page-worthy. Under sustained load the queue is full of the former when the
latter arrives. The current design makes the loss invisible even in kind:
the drop counter does not say *what class* was lost.

## 3. Hard boundaries

### In scope

- `observer.go` — new `Observation` fields, exported vocabulary constants,
  per-class drop accounting, and a bounded priority for terminal-class
  observations within the existing single-goroutine adapter.
- `command_runtime.go`, `enqueue.go`, `runtime_run.go`,
  `internal/store/commands.go` call sites — carry the new dimensions and emit
  the missing terminal run observations.
- `internal/store/commands.go` — additive widening of the claim-head SELECT
  so `ClaimedCommand` also carries the root definition name, and an additive
  run-terminal field on `SettleResult`.
- `types.go` — one new `Definition` field on `CommandInfo`.
- `observer_test.go` and a vocabulary registry test.
- `examples/monitor` — demonstrate an alert-style consumer. It currently
  polls `Trace` (`examples/monitor/main.go:173-184`) and installs no
  observer at all.
- `specs/projects/flow/functional_spec.md`, `components/runtime.md`,
  `architecture.md`, `README.md`, and the package synopsis in `flow.go` —
  normative vocabulary and delivery-class documentation.

### Out of scope — reject during review

- Alerting policy of any kind: thresholds, dedupe, severity, routing, or any
  network client. Flow emits facts; the application owns policy.
- A durable outbox, at-least-once observer delivery, or replay. Best-effort
  process-local delivery is a documented property, not a defect. An
  application whose alert must survive a crash must derive it from durable
  reads.
- Synchronous or transactional hooks. Observers never run inside Flow's
  transactions and never influence outcomes.
- Emitting observations from caller-owned-transaction clients. The
  `client.tx == nil` gate stays; Flow does not observe what it did not
  commit.
- Queue-depth or age threshold callbacks. Depth is a level, not an edge;
  it stays a polled read (`GetQueueStats`, `inspection.go:364-367`).
- Multiple observers, observer middleware chains, or filter DSLs. One
  observer; the application fans out.
- Exposing payloads, results, errors beyond the existing classified
  outcome strings, SQL, or lease tokens.
- Growing `CommandInfo` beyond the single `Definition` field specified in
  Section 4.2.
- Renaming or removing any currently emitted tuple. This plan documents and
  extends the vocabulary; pruning is a separate versioned decision.

## 4. Design

### 4.1 Observation identity dimensions

Add two fields to `Observation`:

- `RunKey string` — the application-chosen run key, empty when the run was
  started without one;
- `RootCommandName string` — the run's root command definition name.

Population rules: run-path sites (`start`, `cancel`, the `ReplaceCurrentRun`
pair) already hold or can cheaply hold both and must set them. Attempt-path
sites populate them from the claim — `RunKey` is already on `ClaimedCommand`
today, `RootCommandName` arrives with Section 4.2. Note that `Observation.Name` on
attempt-path facts is the *command* definition name, not the run's; the new
`RootCommandName` field is the run root and the two are independent. Maintenance
sites populate them from the probe row when the probe already selects the
column, and otherwise leave them empty rather than adding a per-candidate
lookup — maintenance must stay bounded. Runtime, notify-listener, and
lease-renewal batch observations have no single run and leave both empty.

Both values are already documented as non-secret; extend the
`architecture.md` Section 17 secret-hygiene paragraph to name the two new
fields explicitly.

### 4.2 Claim projection carries the root definition name

Plan 14's `AttachSemantic` locking CTE already reads `run_key`, `status`,
`deadline_at`, and the `RunHead` counters under the batch's run lock, and
`ClaimCommands` populates `ClaimedCommand.RunKey` from the explicit initial
snapshot. The remaining widening is therefore one additional column on that
same lock/time/snapshot statement: add `definition_name` to the locking CTE
and `InitialLockedSnapshot`, then add `DefinitionName` to `ClaimedCommand`.
Do not add a claim-head reread or turn `LoadRunHead` into a cache.

This is an additive projection change on a row already locked and already
read: no new table access, no new statement, no new lock, no schema change.
Because claim is the hot path — and Plan 14 has just re-tuned it — the
phase gate re-runs the retained claim benchmark against the newest baseline
in `specs/projects/flow/benchmark_evidence/` (Section 0) and records the
comparison there. A measurable regression beyond noise is a STOP condition,
with the fallback being lazy population on the observation path only for
terminal facts.

In the same phase, expose the root definition name on the public
`CommandInfo` struct (`types.go:122-134`) as a new field
`Definition string`, populated at the single construction site
(`command_runtime.go:430-435`) from `claim.DefinitionName`. Handlers reading
`Work.Info()` (`worker.go:48`) and `WithCommit` callbacks then get the run's
root definition with zero extra reads, which is why this rides along with the
projection change rather than becoming its own plan. `CommandInfo` grows by
exactly this one field; anything more is out of scope (Section 3). The
testing bridge construction (`testing_bridge.go:104`) must carry it too.

### 4.3 Exported vocabulary constants and the normative registry

Introduce exported constants for every operation and outcome enumerated in
Section 2.2 — `ObservationOpProbe`, `ObservationOpClaim`,
`ObservationOpSettle`, `ObservationOpConclude`, `ObservationOpRenewResult`,
`ObservationOutcomeOK`, `ObservationOutcomeSucceeded`,
`ObservationOutcomeExpired`, `ObservationOutcomeRetryWait`, and so on — and
convert all emission sites to use them. String values must equal the
currently emitted literals so existing consumers do not break. Outcomes that
originate in the store (`retry_wait`, `failed`, `succeeded`, `expired`,
`renewed`, `lost`, `uncertain`) must be mapped through the constants at the
emission site rather than passed through as raw store strings, so the
registry test can see them. The conversion must preserve Plan 14's emission
optimizations (`NopObserver` short-circuit, precomputed replica name).

Add a package-level registry (a var listing every legal
`(Kind, Operation, Outcome)` tuple) and a test that fails when an emission
site produces a tuple outside the registry. The registry, not the emission
sites, becomes the compatibility surface: the documented policy is that
tuples are only ever added, never renamed or removed within a major version,
and consumers must ignore unknown tuples. Decide explicitly what to do about
`ObservationWait` (`observer.go:19`), which no site emits: either register a
wait tuple that the maintenance wait-expiry path emits, or record that the
kind is currently unused. Silent omission is not acceptable.

Document the full table in `functional_spec.md` Section 14 (currently
`functional_spec.md:541-583`) and reference it from
`components/runtime.md:59`.

### 4.4 Terminal lifecycle observations

Emit the missing edges, each exactly once per durable transition, from the
code path that performed the transition:

- **Command terminal.** The conclude path already emits the store's terminal
  status (`command_runtime.go:693-697`). Split the vocabulary so retry
  scheduling (`retry_wait`) and terminal outcomes (`failed`, `cancelled`,
  `expired`) are distinct documented outcomes, and add the retry-budget
  dimension: a terminal `failed` caused by budget exhaustion must be
  distinguishable (a documented `Operation` of `conclude_exhausted` or an
  outcome refinement — pick one during implementation and register it).
- **Run terminal.** Emit `ObservationRun` with outcome `succeeded`,
  `failed`, `expired`, or `cancelled` from: the settlement paths, once the
  store's internally computed `terminalRun`
  (`internal/store/commands.go:1114,1411`) is surfaced as an additive
  `SettleResult` field (Section 2.3), riding Plan 14's merged statements
  without adding a round trip; the cancellation paths
  (`internal/store/ingress.go:1155-1170,1292`), where `CancelRun` already
  emits its `cancel`/`cancelled` fact (`enqueue.go:459-461`) but
  `CancelCommand` does not report a run it terminalized; the wait-deadline
  failure path (`internal/store/graph.go:244-256`); and
  `runRunDeadlinePage` (`runtime_run.go:578-596`), per expired run, bounded
  by the existing `maintenanceRunPage` of 64 (`runtime_run.go:28`).
- **Lease recovery.** The lease recovery path — `runLeaseRecoveryPage`
  (`runtime_run.go:618-636`) or its Plan 12 successor — emits a per-command
  `ObservationLease` `recovered` fact, bounded by its page size
  (`maintenanceLeasePage` of 128, `runtime_run.go:30`, today), alongside
  the existing aggregate transition observation
  (`runtime_run.go:646-667`).

Duplicate emission under crash/retry is acceptable and documented: the
observer stream is at-least-zero, at-most-few; consumers that require
exactly-once must correlate with the journal.

### 4.5 Delivery classes

Partition the registry into two documented classes:

- **duty-cycle** — probe, claim, handler, settle-success, event settle/
  deliver, renew, renew_result, notify listener/hint, maintenance probes and
  passes, runtime start/stop; and
- **terminal** — command terminal outcomes, run terminal outcomes, lease
  recovery, local lease cancellation, and the drop report itself.

Keep one delivery goroutine and one observer. Reserve a small fixed portion
of the existing 1024-slot buffer (`observer.go:53`; implementation chooses
the split, on the order of 64 slots) that only terminal-class observations
may occupy, so a duty-cycle flood cannot evict them. When even the reserved
capacity is exhausted, terminal observations are still dropped — delivery
remains best-effort — but drop accounting becomes per-class, and the
drain-time drop report (`observer.go:86-88`) states how many terminal facts
were lost. The documentation must say plainly: a nonzero terminal drop count
means the application's alerting missed edges and its polling reconciliation
is the backstop.

Rejected alternatives, recorded to prevent re-litigation: blocking emit
(couples durable work to observer health), a second goroutine per class
(complicates the Plan 8 shutdown drain for no delivery guarantee), and an
unbounded terminal queue (unbounded memory during an incident, which is
when terminal facts spike).

### 4.6 Consumer guidance

Extend `examples/monitor` to show the intended embedding shape: a switch on
registered tuples, counters for duty-cycle facts, and an error-log/page stub
for terminal-class facts, with a comment stating that durable truth lives in
the read APIs. The example currently demonstrates only `Trace` polling
(`examples/monitor/main.go:173-184`) and registers no observer, so this is
new code beside the existing loop, not a rewrite of it. Add a short
"alerting from observations" subsection to the README next to the existing
observer bullets (`README.md:258-260`) that states the loss model and the
polling backstop requirement.

## 5. Phases

### Phase 1 — Claim projection identity and `CommandInfo.Definition`

Add `definition_name` to the claim-head SELECT and `DefinitionName` to
`ClaimedCommand`; thread it into the attempt-path observations alongside the
already-present `RunKey`. In the same phase, add `Definition` to
`CommandInfo` and populate it at `command_runtime.go:430-435` and
`testing_bridge.go:104`.

Gate: focused store and command-runtime tests; a test proving `Work.Info()`
returns both `RunKey` and `Definition` for a claimed command; claim benchmark
comparison recorded in `specs/projects/flow/benchmark_evidence/` against the
Section 0 baseline.

### Phase 2 — Observation fields and vocabulary constants

Add `RunKey`/`RootCommandName` to `Observation`, introduce the exported
constants, convert all emission sites, add the registry and its
exhaustiveness test, and resolve the unused `ObservationWait` kind.

Gate: `go test -race` on observer, runtime, and command-runtime packages;
registry test proves every emitted tuple is registered and every registered
tuple is emitted by at least one test.

### Phase 3 — Terminal lifecycle emissions

Surface run-terminal on `SettleResult`, then add the Section 4.4 emissions
with their per-page bounds; extend fault and race tests to assert
exactly-once emission per durable transition in the non-crash case and to
tolerate duplicates across injected crash/retry.

Gate: focused settlement, cancellation, and maintenance tests including the
existing fault-injection harness.

### Phase 4 — Delivery classes

Implement the reserved terminal capacity and per-class drop accounting in
the adapter; keep the Plan 8 shutdown drain semantics byte-for-byte
(cancel, close, bounded drain, final drop report).

Gate: adapter tests covering duty-cycle flood with terminal delivery,
per-class drop counts, drain-time reporting, panic isolation, and shutdown
under a blocked observer.

### Phase 5 — Documentation and example

Normative vocabulary table and compatibility policy in the functional spec;
runtime component and architecture updates; README alerting subsection;
`examples/monitor` consumer; `flow.go` package synopsis touch-up.

Gate: spec build/lint (`specs/projects/flow/Makefile`), full serial race
suite, and a manual read of the vocabulary table against the registry var.

## 6. STOP conditions

Stop and request review if:

1. carrying the root definition name through the claim-head SELECT
   measurably regresses the Section 0 claim baseline beyond noise;
2. any design pressure appears to make observer delivery durable, blocking,
   transactional, or able to affect settlement outcomes;
3. surfacing the run-terminal edge on `SettleResult` would require a second
   read, a wider lock, an extra round trip past Plan 14's merged statements,
   or a change to what the settlement transaction commits;
4. exactly-once terminal emission in the non-crash case cannot be achieved
   without adding state to durable tables;
5. the reserved terminal capacity cannot coexist with the Plan 8 shutdown
   drain without a second delivery goroutine;
6. an existing consumer-visible tuple would need to be renamed or removed to
   fit the registry — that is a versioning decision, not a cleanup;
7. maintenance page emission would require per-candidate lookups that break
   the bounded-page contract;
8. `CommandInfo` would need to grow beyond the single `Definition` field to
   make the change coherent;
9. Plans 12 or 14 shipped an observer queue model, emission path, or claim
   projection materially different from Sections 2 and 4 — update this plan
   before implementing against it.

## 7. Done criteria

- [ ] `Observation` carries run key and definition name on every emission
      site that has them, with documented empty-field rules.
- [ ] `CommandInfo` exposes `Definition`, populated from the claim at its
      single construction site, with no other new `CommandInfo` fields.
- [ ] Every emitted `(Kind, Operation, Outcome)` tuple uses exported
      constants, appears in the registry, and is enumerated in the
      functional spec with an additive-only compatibility policy; the
      `ObservationWait` kind is either emitted or documented as unused.
- [ ] Command terminal, run terminal, retry-budget exhaustion, and
      lease-recovery facts are observable, bounded, and tested for
      exactly-once emission per durable transition in the non-crash case.
- [ ] Terminal-class observations survive a duty-cycle flood up to the
      reserved capacity; drop accounting and the drain report are per-class.
- [ ] No change to durable schema, journal encodings, fencing, settlement,
      or the Plan 8 shutdown drain contract; observations still contain no
      payloads, results, SQL, connections, or lease tokens.
- [ ] Claim benchmark evidence recorded; no regression beyond noise.
- [ ] `examples/monitor`, README, `flow.go`, and the three spec documents
      describe the same vocabulary and loss model.
- [ ] Full `go test -race ./...` passes.

## 8. Deferred follow-ups

Recorded so their absence is a decision, not an oversight:

- **Run identity on `Work.Info()`** — closed, not deferred. `RunKey` shipped
  upstream in v0.3.0 (`types.go:124`), and `Definition` ships in Phase 1 of
  this plan (Section 4.2). Nothing remains for a separate plan.
- **All-queue depth reads** — closed by Plan 13. `GetQueueStats`
  (`inspection.go:364-367`) reports every requested lane in one call with a
  shared timestamp; no observer work remains.
- **Typed history entry fields.** Journal body encoding is private, but the
  queue/classification detail inside it is legitimately useful to
  consumers; `KeyedHistoryEntry` should expose those as typed fields so
  nobody parses the body. Read-API plan, not an observer plan.
- **Vocabulary pruning.** If any current tuple proves valueless, remove it
  in a major-version vocabulary revision, never silently.
