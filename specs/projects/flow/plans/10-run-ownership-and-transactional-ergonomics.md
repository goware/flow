# Plan 10: Make run ownership and transactional use explicit

Status: Implemented (combined v0.3 candidate; final review pending)

Planned at: `788c9b5` on 2026-08-11

- **Target release:** combined v0.3.0 candidate after the untagged Plan 9
  checkpoint; Plan 11 remains separate
- **Priority:** P1 for correctness and Trails integration simplicity
- **Effort:** M-L
- **Risk:** MEDIUM-HIGH; no durable format or schema change is intended, but
  atomic replacement and caller-owned transaction ordering are concurrency-
  sensitive
- **Depends on:** Plan 9 implemented and reviewed as an untagged checkpoint
- **Schema impact:** none; the Flow table count remains six and migration 004
  remains the current schema version
- **Durable format impact:** none; replacement uses the existing run,
  cancellation, command, event, and journal records
- **Public API impact:** additive `CommandInfo.RunKey`; additive atomic
  live-run replacement; potentially breaking specialization of `Runtime.InTx`'s
  return type to a named transaction-scoped client; no `flow.Call`

> **Executor instructions:** Read this plan completely before editing. Start
> from the exact reviewed Plan 9 checkpoint, perform the Phase 0 decision proof,
> then implement in phase order. Keep each phase independently reviewable and
> run its focused ordinary and race tests before continuing. This plan removes
> application workarounds by giving Flow the smallest missing ownership APIs;
> it must not introduce a workflow/coordinator layer, generic transaction DSL,
> seventh table, journal mode, or inline command execution.
>
> **Plan boundary:** Plan 9 establishes the final `Run` vocabulary, direct-only
> root `Command.Enqueue`, staged `flow.Emit`, and detached targeted
> `Event.Deliver`. This plan preserves those choices. Plan 11's optional
> `flow.Call` proposal is separate and must not be implemented, pre-wired, or
> used by the Trails consumer proof here.
>
> **Initial drift check:**
>
> ```text
> git status --short --branch
> git log -1 --decorate --oneline
> git describe --tags --always
> git diff --stat <plan-9-checkpoint>..HEAD -- \
>   runtime.go client.go types.go command_runtime.go execute.go \
>   internal/store internal/replay testing_bridge.go flowtest \
>   compile_contract_test.go README.md flow.go specs/projects/flow
> ```
>
> Replace `<plan-9-checkpoint>` with the exact reviewed Plan 9 SHA. Reconcile
> every accepted change before implementation. Unexplained API, schema,
> journal, transaction-order, or Trails integration drift is a STOP condition.

## 1. Executive summary

Plan 9 makes Flow's nouns and verbs smaller. Plan 10 makes three ownership
facts explicit so applications do not reconstruct them with extra reads or
unsafe transaction choreography:

1. A claimed command already belongs to one run and Flow already knows that
   run's key. `CommandInfo` should carry `RunKey` so a handler does not call
   `GetRun` merely to recover its root entity key.
2. Replacing the current live-key run is one semantic operation. Flow should
   atomically cancel the expected current run and enqueue the requested
   replacement, rather than forcing an application to commit cancellation and
   start the replacement afterward.
3. A caller-owned PostgreSQL transaction has one Flow lock-order history.
   `Runtime.InTx(tx)` should return a named transaction-scoped client that is
   created once and threaded through every Flow operation in that transaction.
   The application-write phase guard should be usable through that client.

The target mental model remains:

```text
Run owns durable graph and live-key identity
  ├── CommandInfo carries RunID + RunKey at claim
  ├── Enqueue starts an independent root Run
  ├── ReplaceCurrentRun atomically supersedes one live Run
  ├── flow.Emit stages a fact with worker settlement
  └── Event.Deliver immediately targets a named Run

TransactionClient owns one caller transaction's Flow lock-order state
```

These changes simplify Trails without turning Flow into a Trails-specific
coordinator. They are generally useful whenever an application repairs one
live operation, routes a claimed command by root key, or couples Flow and
application rows in the same transaction.

## 2. Evidence and current friction

The implementation baseline must be re-audited after Plan 9, but the v0.2
source demonstrates the underlying constraints:

- `CommandInfo` carries the owning ID but not the root key. Trails
  `lib/jobqueue` therefore performs a separate `GetRun` for every claimed job
  only to recover the entity key already present on the locked run row.
- `Runtime.InTx(tx)` currently returns the broad `Client` interface and creates
  a fresh private lock-order tracker on every call. Calling `InTx` twice for the
  same `pgx.Tx` creates two independent trackers and defeats the intended
  cross-operation ordering check.
- the store already has `LockOrder.BeginApplicationPhase`, but no public
  transaction client exposes it. Production callers therefore cannot mark the
  point after which returning to Flow would invert Flow/application locks.
- a normal start inserts a fresh run and then treats its generated ID as if it
  were a pre-existing run lock. That conservative rule makes cancel-then-start
  depend on generated-ID order even though a row created by the current
  transaction cannot be held by a concurrent transaction.
- Trails retry/admin-requeue paths cancel the old `intent.run`, commit, then
  start a replacement. A crash between those steps leaves no owner until a
  reconciler repairs it. Reconciliation is valuable as a safety net, but it
  should not compensate for a non-atomic primary transition.

The source after Plan 9 may rename files and methods. The executor must map
these behaviors to the final `Run` vocabulary rather than restoring old
`Execution` names.

## 3. Controlling architecture decisions

### 3.1 Keep `Event.Deliver`

Plan 9 retains two intentionally different event operations:

| API | Settlement |
|---|---|
| `flow.Emit(work, event, key, value)` | staged; commits or rolls back with the active worker decision |
| `event.Deliver(ctx, client, runID, key, value)` | immediate and detached; targets one explicit run through a library-owned or caller-owned transaction |

Plan 10 does not rename, wrap, or relax either operation. In particular, it
does not add `DeliverToLive`: applications must compose `GetCurrentRun` with
`Deliver` so the settle-between-read-and-delivery race remains visible and a
generation-specific fact is never silently retargeted.

### 3.2 Put `RunKey` in `CommandInfo` without another query

The target public shape is:

```go
type CommandInfo struct {
    RunID     RunID
    RunKey    string
    CommandID CommandID
    CommandKey string
    Name      string
    Version   int
    // existing timing and attempt fields
}
```

Exact requirements:

1. `RunKey` is the root run key, not the command key and not a reconstructed
   suffix.
2. It is populated for root and child commands, queued commands, retry/lease
   takeover, the real runtime, the testing bridge, and `flowtest`.
3. Empty remains a valid value for an intentionally unkeyed run.
4. Claim must populate it from the run row/head already locked or loaded for
   ownership and fencing. Extend that existing row/query projection if needed;
   do not add a per-claim point query.
5. It is immutable for a run and does not enter command fingerprints, journal
   encodings, event identity, or retry behavior.

This is inspection context, not a new ownership mechanism. `RunID` remains the
exact durable identity; `RunKey` is the application correlation key.

### 3.3 Add one atomic current-run replacement operation

Replacing a live-key run is not ordinary enqueue. It has a predecessor CAS and
must preserve both cancellation and replacement atomically. The public API
should belong to the root command definition and stay explicit:

```go
type ReplaceRunResult struct {
    Run      Run
    Replaced bool // true only when this call created the replacement
}

func (cmd Command[A, R]) ReplaceCurrentRun(
    ctx context.Context,
    client Client,
    expected RunID,
    key string,
    args A,
    reason string,
    opts ...RunOption,
) (ReplaceRunResult, error)
```

Phase 0 may improve the result type or shorten the method name, but it may not
weaken the following semantics:

| State at the serialized live-key decision | Outcome |
|---|---|
| current live run ID equals `expected`, whether or not its declaration is equivalent to the request | cancel the expected run and create the requested replacement in one transaction; return the distinct new run with `Replaced=true` |
| current live run ID differs from `expected` and is declaration-equivalent to the requested replacement | return that already-committed run with `Replaced=false`; no cancellation or new journal entries |
| current live run ID differs from `expected` and is not declaration-equivalent | `ErrConflict`; no mutation |
| no current live run exists | `ErrConflict`; ordinary `Enqueue` remains the explicit create-if-absent operation |
| expected run or its graph is corrupt/impossible | fail closed with `ErrInvalidState`; no partial mutation |
| cancellation or replacement validation fails | roll back both sides |
| caller-owned transaction rolls back | neither cancellation nor replacement becomes visible |

“Declaration-equivalent” must reuse the same command definition version,
canonical input, normalized options, key scope, and start fingerprint used by
ordinary `Enqueue`. The expected-current comparison has precedence over
equivalence: when the current ID still equals `expected`, the caller is
explicitly asking to replace that generation, so even a byte-for-byte
equivalent declaration must create a distinct replacement. Equivalence is
consulted only after the current ID differs from `expected`; that ordering lets
a retry after a concurrent winner or ambiguous commit rediscover the already-
committed requested replacement without replacing it again. It does not
authorize retargeting a different declaration.

Additional rules:

- require a non-empty live key and `WithLiveKey`; reject permanent or unkeyed
  replacement;
- validate all input, reason, options, limits, and registration before the
  first durable mutation where possible;
- use the existing cancellation journal and terminal propagation exactly as
  `CancelRun`; use the existing start journal/projection exactly as `Enqueue`;
- do not add a supersession column, replacement table, new journal kind, or a
  second live-key index;
- do not infer ordering from UUIDv7. UUIDv7 improves locality but remains an
  identity detail, not a locking proof;
- emit the existing bounded cancellation/start observations after commit, with
  no arguments, results, or reason text added to metrics labels; and
- preserve ordinary notification behavior for both the cancelled graph and
  the new runnable root.

The store implementation needs one dedicated transactional operation. Do not
compose exported `CancelRun` and `Enqueue` calls if doing so repeats queries,
opens two transactions, publishes notifications early, or loses the expected-
current CAS.

### 3.4 Distinguish newly inserted rows from pre-existing run locks

The transaction lock-order guard prevents two transactions from locking
pre-existing runs in opposite ID order. A run successfully inserted by the
current transaction cannot be locked by another transaction, so it must not
force the same ordering constraint.

Phase 0 must prove and document a narrow internal model such as:

```text
BeforeExistingRun(id)   enforce ascending order among pre-existing run locks
RegisterOwnedRun(id)    record a row created by this transaction; no ordering edge
BeforeApplicationWrites reject every later Flow write/locking operation
```

The names are illustrative. Required behavior is not:

- globally remove run lock ordering;
- special-case UUIDv7 comparison;
- sort cancellation after inserting a replacement that is visible to a race;
- use an unbounded retry loop around unique-key conflicts; or
- allow a caller to return to Flow after application locks/writes begin.

Audit ordinary `Enqueue` as well as replacement. A successful fresh insert may
use the owned-row path; conflict rediscovery loads a pre-existing run and must
still use the normal ordered-lock path. Add adversarial tests with explicitly
chosen reverse-ordered UUIDs so correctness cannot accidentally depend on the
current ID generator.

### 3.5 Make the transaction-scoped client a named public concept

The target API is:

```go
type TransactionClient struct { /* unexported state */ }

func (r *Runtime) InTx(tx pgx.Tx) *TransactionClient

// BeginApplicationWrites marks the irreversible phase boundary. Every later
// Flow write/locking operation through this client returns ErrInvalidState
// before SQL.
func (c *TransactionClient) BeginApplicationWrites() error
```

`*TransactionClient` implements the sealed `Client` contract. It does not own,
commit, roll back, or expose the caller's transaction. It is valid only for the
supplied transaction, is not safe for concurrent use, and must not outlive that
transaction. The first `BeginApplicationWrites` call succeeds without issuing
SQL; a duplicate call returns `ErrInvalidState`.

Usage should read:

```go
tx, err := db.Begin(ctx)
if err != nil { /* ... */ }
defer tx.Rollback(ctx)

flowTx := runtime.InTx(tx) // exactly once at this transaction boundary

// Flow locks/operations first.
_, err = IntentRun.ReplaceCurrentRun(ctx, flowTx, oldRunID, intentID, args,
    "operator retry", flow.WithLiveKey())
if err != nil { /* ... */ }

if err := flowTx.BeginApplicationWrites(); err != nil { /* ... */ }
// Application row locks/writes now; do not call Flow again.

err = tx.Commit(ctx)
```

This method makes the existing application-phase guard usable; it cannot prove
that a caller did not write application rows before calling it. Documentation,
examples, and consumer helpers must therefore enforce the contract socially
and structurally: create the client once at the transaction boundary, thread it
into helpers as `flow.Client` or `*flow.TransactionClient`, perform Flow
operations first, call `BeginApplicationWrites`, then perform application
writes.

Do not add a runtime-global cache keyed by arbitrary `pgx.Tx`, a generic
`RunInTransaction` framework, hidden transaction context values, reflection,
goroutine ownership detection, or a wrapper around all application SQL. Those
mechanisms add more complexity than the contract they protect. Add a prominent
warning that repeated `Runtime.InTx(tx)` calls create independent clients and
are invalid usage; the Trails proof must leave no repeated wrapping within one
transaction path.

Phase 0 must check source compatibility of changing `InTx`'s return type. If a
named concrete return breaks an important supported interface/function-value
use, prefer an exported sealed `TransactionClient` interface implemented by
the existing concrete client. Do not fall back to the anonymous broad `Client`
return solely to avoid updating tests.

### 3.6 Retention remains enabled and separately designed

Do not disable or bypass the journal. The journal is the authoritative ordered
history used for replay, trace, invariant validation, and durable projection
reconstruction; command rows are current projections and are not a substitute.

Flow still needs a deliberate retention/archival design before encouraging a
high-volume inline-command workload. That design must decide permanent-key
tombstones, terminal run/history retention, bounded deletion, inspection races,
foreign-key order, vacuum behavior, and recovery evidence. It is not part of
this plan because Plan 10 adds no high-volume command mode. Plan 11 must treat
an approved retention decision—or an explicit operational acceptance of
unbounded growth—as a release gate for high-volume `flow.Call` use.

### 3.7 Expected implementation surface

Reconcile renamed files after Plan 9, then keep the change concentrated in
these responsibilities:

| Likely path | Responsibility |
|---|---|
| `types.go` | add `CommandInfo.RunKey` and public replacement result |
| `runtime.go` / `client.go` | export the transaction-scoped client, resolve it as `Client`, expose the application-write phase |
| `command_runtime.go` | carry the already loaded run key into claimed `Work.Info` |
| `execute.go` or its Plan 9 `enqueue.go` successor | typed `ReplaceCurrentRun` validation and public orchestration |
| `internal/store/commands.go` | claim/head projection supplying `RunKey` without another query |
| `internal/store/ingress.go` | dedicated atomic replacement request/result and reuse of cancellation/start paths |
| `internal/store/order.go` | pre-existing-lock versus transaction-owned-row ordering state |
| `internal/store/inspection.go` | reuse the current live-key lookup/lock contract; do not add a scan |
| `testing_bridge.go`, `internal/testengine/`, `flowtest/` | runtime/test parity for `RunKey`, replacement, and transaction behavior |
| `compile_contract_test.go` and focused store/runtime tests | public signature, type safety, removed misuse, concurrency, and SQL-shape guards |
| `README.md`, `flow.go`, active specs, examples | transaction order, replacement semantics, and combined v0.2-to-v0.3 migration guidance |

Do not create a `coordinator`, `workflow`, `replacement`, or transaction-
framework package. Small request/result and lock-order helpers belong beside
the existing start/cancel/store code they compose.

## 4. Detailed atomic replacement behavior

### 4.1 Transaction shape

For a library-owned client, replacement should use one short transaction:

```text
validate definition, args, options, reason
begin transaction
  lock the expected predecessor as the CAS serialization anchor
  read the current holder for (definition, live key)
  if current RunID != expected:
    if current declaration is equivalent: return existing
    otherwise: return ErrConflict
  validate the expected run graph using the existing cancellation path
  cancel expected run and remove its runnable deliveries
  create replacement run/root command using existing start path
  append existing cancellation and start journal entries
commit
publish existing notifications/observations
```

For `TransactionClient`, perform the same store operation inside the supplied
transaction. Defer notifications until the existing caller-owned-transaction
policy says they are safe; do not invent a pre-commit publish path.

The live-key unique constraint remains the final concurrency arbiter. Locking
the expected predecessor serializes every retry carrying that predecessor ID,
including retries after it became terminal; this is what lets the second caller
observe and rediscover the first caller's committed successor. Status changes
must order the old terminal transition before the new insert within the same
transaction, and the live-key unique constraint remains the final arbiter, so
replacement never creates two visible live holders or exposes a gap after
commit. Reading an unexpected current holder does not authorize mutating it.

### 4.2 Concurrency and retries

Required concurrent outcomes:

- two callers replacing the same expected run with different declarations:
  one commits; the other returns `ErrConflict` and does not cancel the winner;
- two callers replacing with equivalent declarations: one creates; the other
  observes a current ID different from `expected` and rediscovers the
  equivalent winner;
- a replacement whose requested declaration is identical to the still-current
  expected predecessor creates a distinct new run rather than returning the
  predecessor unchanged;
- a normal live-key `Enqueue` racing replacement: it either rediscovers an
  equivalent holder or conflicts according to existing start semantics; it
  never creates a second live holder;
- terminal settlement racing replacement: the serialized winner decides; a
  missing/terminal expected holder cannot be silently recreated by replacement;
- cancellation, expiry, or another admin action racing replacement: no partial
  terminal journal and no ownerless committed state;
- ambiguous commit: when the current ID no longer equals `expected`, a repeated
  equivalent replacement returns the committed current run, while a
  non-equivalent declaration conflicts;
- transaction rollback, context cancellation before commit, injected SQL
  errors, and process restart leave either the entire old state or the entire
  new state visible.

### 4.3 No new reconciliation requirement

Atomic replacement removes the cancel/commit/start crash window. It does not
remove reconciliation entirely: crashes, corrupt application rows, expired
runs, and historical/manual states still need bounded invariant repair.
Consumer reconciliation should stop repairing the transition that the new API
makes atomic, retain anomaly detection and expiry/stale-owner repair, and never
fight a valid current run.

## 5. Trails consumer proof

The Flow implementation is not complete until a disposable Trails branch
demonstrates that it removes application complexity. Do not commit or push
cross-repository changes without separate authorization.

Required adaptation:

1. Upgrade Trails to the exact Plan 10 Flow candidate.
2. In `lib/jobqueue`, derive the handler entity key from
   `work.Info.RunKey`; delete the per-attempt `GetRun` lookup and its error path.
3. In retry/admin-requeue recovery, replace split cancel/commit/post-commit
   start with `ReplaceCurrentRun` in one transaction. Delete comments and
   recovery branches that exist only for the split crash window.
4. At each caller-owned transaction boundary, call `Runtime.InTx(tx)` once and
   thread the resulting client through helpers. Where a helper performs
   multiple Flow operations, accept `flow.Client` or the transaction client
   instead of accepting raw `pgx.Tx` and wrapping it repeatedly.
5. Perform Flow locks/operations before application writes and mark the
   application phase where the path mixes both domains.
6. Keep bridge, edge, OIF, and provider monitor delivery as
   `Event.Deliver`; keep exact generation/row fencing unchanged.
7. Do not adopt `flow.Call` or create a new Trails abstraction around the new
   APIs.

Queue-dynamics statement: `CommandInfo.RunKey` removes one point read per
claimed job. Atomic replacement creates one new root only while cancelling its
expected predecessor in the same transaction; it adds no queue lane, waiting
row, polling loop, dequeue occupant, or retry loop. The existing new root is
bounded by its ordinary command timeout/retry policy. Existing anomalous rows
are not drained by a Flow migration because this feature branch is disposable;
Trails should retain bounded reconciler handling for independently possible
stale/expired states.

Required Trails tests include:

- a claimed root and child job receive the exact intent/entity `RunKey` with no
  `GetRun` query;
- concurrent retry/requeue requests create exactly one replacement owner;
- injected failures at every cancel/start/commit boundary expose no committed
  ownerless state;
- rollback restores the old current run and application state;
- replacing the exact expected run creates a distinct replacement even when
  its declaration is identical to the requested declaration;
- equivalent retry after ambiguous commit rediscovers the replacement;
- a stale expected ID cannot replace or cancel a newer different run;
- an enduring monitor delivers an exact generation-fenced fact into the
  current `intent.run` in the same caller-owned transaction as its domain row;
- no transaction path calls `Runtime.InTx(tx)` more than once; and
- queue accumulation still permits newer work to flow and terminal status
  propagates through transaction, receipt, and intent.

## 6. Implementation phases

### Phase 0 — Baseline and API/locking decision proof

Before production edits:

1. confirm the exact untagged Plan 9 checkpoint and clean worktree;
2. inventory `CommandInfo` construction, claim SQL/head loading, testing
   bridges, all `InTx` call sites, lock-order state, fresh-start insertion,
   cancellation, live-key lookup, notifications, and observations;
3. write compile-only examples for `RunKey`, `TransactionClient`,
   `BeginApplicationWrites`, and `ReplaceCurrentRun`;
4. freeze the replacement decision table in §3.3, including expected-ID-first
   ordering, the exact equivalence/fingerprint rule used only after an ID
   mismatch, and the public result/error classification;
5. prove with an isolated store test that a newly inserted row cannot create a
   cross-transaction run-lock cycle, while conflict rediscovery still locks a
   pre-existing row in order;
6. prove the replacement transaction can reuse existing cancellation/start
   journal builders and projections without a new schema or journal kind; and
7. run and record the full ordinary/race baseline and supported PostgreSQL
   versions.

Gate: stop before production edits if replacement cannot be atomic without a
schema change, if ambiguous-commit equivalence cannot be defined safely, or if
the lock proof requires UUID ordering.

### Phase 1 — Claim-time `RunKey`

1. add `RunKey` to `CommandInfo` and the internal claimed-work/head shape;
2. populate it from data already loaded during claim;
3. update the real runtime, testing bridge, flowtest, fixtures, and docs; and
4. add a query-count/SQL-shape regression proving no extra claim query.

Focused gate: command-runtime, claim/store, testing-bridge, flowtest, compile-
contract, and race tests.

### Phase 2 — Named transaction client and phase guard

1. export the chosen transaction-client type and return it from `Runtime.InTx`;
2. expose `BeginApplicationWrites` over the existing lock-order phase;
3. document one-client-per-transaction, non-concurrent, non-owning lifetime;
4. update all Flow examples/tests to create and thread one client; and
5. test duplicate phase entry, Flow-write-after-application rejection before SQL,
   rollback/commit visibility, nil/closed/mismatched clients, and ordinary
   `Client` compatibility.

Focused gate: runtime, store order, compile-contract, caller-owned transaction,
and race tests.

### Phase 3 — Atomic current-run replacement

1. implement the dedicated store request/result and validation;
2. distinguish pre-existing run locks from transaction-owned inserted rows;
3. reuse existing cancellation and start journal/projection builders in one
   transaction;
4. expose the typed command method and map errors consistently;
5. preserve notification/observation timing and bounded payload policy; and
6. fault-inject validation, old-run lock, cancellation apply, replacement
   insert, journal apply, projection, notify, commit, and ambiguous-commit
   boundaries.

Focused gate: the complete §4.2 concurrency matrix under repeated `-race`,
ordinary start/cancel non-regression, replay conformance, and migrations
remaining byte/checksum identical.

### Phase 4 — Documentation and Trails proof

1. update README, package docs, architecture, engine/runtime/store components,
   transaction examples, and inspection docs;
2. extend the combined v0.2-to-v0.3 migration guide with the `InTx` return type
   and new APIs;
3. perform the disposable Trails adaptation in §5 and run its Flow-focused,
   queue-level, and race tests;
4. record removed consumer queries, post-commit starts, repeated transaction
   wrappers, and redundant repair branches; and
5. confirm Trails still uses `Event.Deliver` and does not use `flow.Call`.

### Phase 5 — Full verification and release

Run, adjusted only for the repository's current supported matrix:

```text
gofmt -w <all changed Go files>
git diff --check
go mod verify
go mod tidy -diff
make build
go vet ./...
go test -count=1 -p 1 -parallel 4 ./...
make test
go run golang.org/x/vuln/cmd/govulncheck@latest ./...
```

Run ordinary and race suites against every PostgreSQL major promised by the
README with durability settings enabled. Audit named test output for skips.
Review every changed hunk against this plan, repeat the Trails proof against
the exact candidate commit, obtain human approval, then tag the approved
combined v0.3.0 release. Do not combine Plan 11.

## 7. Acceptance criteria

Plan 10 is complete only when:

1. Plan 9 is a reviewed implementation checkpoint and its
   Run/Enqueue/Emit/Deliver vocabulary remains intact.
2. `CommandInfo.RunKey` is exact for root and child work across first claim,
   retry, takeover, real runtime, testing bridge, and flowtest.
3. Populating `RunKey` adds no per-claim SQL query and no durable field.
4. `Runtime.InTx(tx)` returns one named transaction-scoped client whose
   lifetime, non-concurrency, and non-ownership contract is documented.
5. `BeginApplicationWrites` makes the existing phase guard usable and every
   later Flow write/locking operation through that client fails before SQL.
6. Documentation and examples require one transaction client per `pgx.Tx`;
   no cache, context protocol, generic transaction DSL, or application-SQL
   wrapper is added.
7. `ReplaceCurrentRun` supports only live-key roots and, whenever the current
   ID equals `expected`, atomically cancels that run plus creates a distinct
   replacement even if the two declarations are equivalent.
8. Only an unexpected current ID with an equivalent declaration is
   idempotently returned; an unexpected non-equivalent current ID conflicts,
   and absence does not silently create.
9. Cancellation and replacement reuse existing journal/projection semantics,
   observations, notifications, canonical fingerprints, and error classes.
10. No replacement column, table, journal kind, migration, alternate live-key
    constraint, or UUID-order assumption is introduced.
11. Lock ordering remains enforced among pre-existing run rows, while rows
    created by the current transaction are handled through a narrowly proven
    owned-row path.
12. Concurrent replacement, normal enqueue, terminal settlement, cancellation,
    expiry, rollback, context cancellation, and ambiguous commit satisfy the
    matrix in §4.2 under ordinary and race tests.
13. No committed state exposes two live holders, an ownerless cancel/start
    gap, partial cancellation, or a replacement after stale expected ownership.
14. Flow remains exactly six tables and migration/journal checksums and version
    compatibility remain unchanged from Plan 9.
15. Trails removes the jobqueue owner lookup, split cancel/start window,
    repeated `InTx` wrapping, and only the reconciler branches made redundant
    by atomic replacement.
16. Trails retains exact generation fencing, `Event.Deliver`, bounded anomaly/
    expiry repair, terminal propagation, and healthy queue dynamics.
17. Trails does not adopt `flow.Call`, and Plan 11 remains independently
    reviewable.
18. README, package docs, normative specs, examples, and migration guidance
    agree on the final API and transaction order.
19. All named Flow and Trails focused tests pass with no unintended skips;
    Flow ordinary/race suites pass on every supported PostgreSQL major.
20. Formatting, build, vet, module, vulnerability, diff, and final human review
    gates pass before the combined v0.3.0 release tag.

## 8. STOP conditions

Stop and report evidence rather than improvising if:

1. the Plan 9 checkpoint or worktree contains unexplained overlapping
   changes;
2. `RunKey` requires an additional query per claim or changes a durable
   fingerprint/encoding;
3. a named transaction client requires a runtime-global transaction registry,
   hidden context, reflection, or generic application transaction framework;
4. `BeginApplicationWrites` cannot reject later Flow write/locking calls before SQL or
   changes transaction ownership;
5. atomic replacement requires a seventh table, new column, new journal kind,
   changed durable bytes, or new live-key constraint;
6. replacement needs UUIDv7 ordering, commits cancellation before replacement,
   or holds application locks before Flow locks;
7. expected-ID-first replacement is weakened, equivalent ambiguous-commit
   recovery can accept a different declaration, or stale ownership can cancel
   a newer run;
8. a fresh inserted-row exception weakens ordering for any pre-existing row or
   creates a deadlock cycle;
9. notifications/observations must publish before caller commit to make the
   operation work;
10. the Trails proof needs `flow.Call`, `DeliverToLive`, a coordinator/workflow
    abstraction, restored V1 roots, or another reconciliation loop;
11. a full ordinary/race/PostgreSQL gate repeatedly fails for an in-scope
    reason; or
12. implementation pressure expands this plan into journal retention or Plan
    11 inline execution.

## 9. Explicit non-goals

- No `flow.Call`, inline command, task, step, workflow, coordinator, or
  checkpoint API.
- No changes to `flow.Emit` or `Event.Deliver` semantics.
- No `DeliverToLive` or implicit live-run routing.
- No new schema migration, table, column, journal kind, or format version.
- No replacement history relation beyond existing cancellation/start history.
- No automatic retries inside `ReplaceCurrentRun`; callers use ordinary error
  handling and desired-state rediscovery.
- No generic application transaction manager or wrapper around `pgx.Tx`.
- No journal disable switch, pruning, compaction, archival, or retention SQL.
- No Flow-specific Trails reconciliation/coordinator abstraction.
- No queue, scheduler, lease, wait, retry-policy, or worker concurrency change.

## 10. Punchlist

### Baseline and design freeze

- [x] Confirm the exact untagged Plan 9 checkpoint and clean worktree.
- [x] Record Go/PostgreSQL versions, schema/version ledger, exported API, and
  full ordinary/race baselines.
- [x] Inventory claim/head data, `CommandInfo`, `InTx`, lock-order, start,
  live-key, cancellation, journal, notification, observation, and Flow/Trails
  consumer call sites.
- [x] Freeze compile examples and the expected-ID-first replacement
  decision/error table, including identical-declaration replacement and
  unexpected-equivalent rediscovery.
- [x] Prove the pre-existing-lock versus transaction-owned-row model without
  UUID-order assumptions.
- [x] Confirm replacement reuses existing cancellation/start durable shapes
  with no migration.

### Claim ownership context

- [x] Add `CommandInfo.RunKey`.
- [x] Populate it from the already loaded claim/run head in real runtime,
  retries, and takeovers.
- [x] Update testing bridge and flowtest parity.
- [x] Prove root/child/keyed/unkeyed behavior and no extra claim query.

### Transaction client

- [x] Export the named transaction-scoped client and return it from `InTx`.
- [x] Expose `BeginApplicationWrites` over the existing order guard.
- [x] Document one-client-per-transaction, Flow-first, non-concurrent,
  non-owning lifetime.
- [x] Add compile/runtime/rollback/commit/phase-order/race coverage.
- [x] Guard against reintroducing broad anonymous transaction-client examples.

### Atomic replacement

- [x] Add the typed public request/result and validation.
- [x] Add the dedicated store transaction and expected-current CAS.
- [x] Reuse canonical start equivalence and cancellation/start journal builders.
- [x] Prove an identical declaration still replaces the exact expected run,
  while a retry rediscovers an equivalent winner whose ID differs from
  `expected`.
- [x] Add the narrowly proven owned-row lock-order path; preserve ordered locks
  for every pre-existing row and conflict rediscovery.
- [x] Preserve post-commit notifications and bounded observations.
- [x] Fault-inject every mutation/commit boundary.
- [x] Run the full repeated concurrency and ambiguous-commit matrix under race.
- [x] Prove migration/version/checksum and ordinary enqueue/cancel/replay non-
  regression.

### Documentation and consumer proof

- [x] Update README, package/normative/component docs, examples, and combined
  v0.2-to-v0.3 migration guidance.
- [x] Perform the disposable Trails adaptation without committing it unless
  separately authorized.
- [x] Remove the jobqueue `GetRun` owner lookup and prove query reduction.
- [x] Replace split cancel/start with atomic replacement and remove only the
  now-redundant repair path.
- [x] Create and thread one transaction client per Trails transaction; mark
  the application-write phase.
- [x] Retain `Event.Deliver`, exact generation fencing, and bounded anomaly/
  expiry reconciliation.
- [x] Prove queue dynamics, terminal propagation, and enduring monitor delivery.
- [x] Confirm no Trails `flow.Call` use.

### Final verification and release

- [x] Run formatting, diff, build, vet, module, vulnerability, ordinary, race,
  and supported-PostgreSQL gates with no unintended skips.
- [ ] Review every changed hunk against all acceptance criteria and STOP
  conditions.
- [ ] Repeat the Trails proof against the exact candidate commit.
- [ ] Obtain human approval and tag the combined Plans 9–10 v0.3.0 candidate;
  do not combine Plan 11.
