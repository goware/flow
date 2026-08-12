# Plan 11: Durable subroutine calls — inline progress, one command vocabulary

Status: Planned

Planned at: `788c9b5` on 2026-08-11

- **Target release:** no release is committed; if approved after the combined
  Plans 9–10 v0.3.0 release, this is a separately reviewed v0.4.0 candidate
- **Priority:** P2 optional feature; it does not block Plan 9, Plan 10, or the
  Trails integration
- **Effort:** L
- **Risk:** MEDIUM-HIGH; this plan deliberately expands the durable product
  model with one additive delivery mode and one forward migration
- **Schema impact:** additive migration 005 only — one commands column and its
  `CHECK`; no new table, no changes to existing rows
- **Upgrade schema impact:** Plan 9 migration 004 performs the breaking
  execution-to-run catalog rename; Plan 11 migration 005 then adds inline
  delivery after Plan 10, which has no schema migration
- **Durable format impact:** additive; existing journal kinds are reused, and
  tagged v0.2.0 data remains readable and replayable
- **Plan 11 API impact:** additive `flow.Call` and
  `TraceCommand.Inline`; no existing signature changes
- **Baseline API:** Plan 9's public `Run`/`Enqueue` vocabulary, direct-only
  root enqueue, retained detached `Event.Deliver`, presence-returning
  `GetEventValue`, and Plan 10's run-ownership/transaction ergonomics

> **Executor instructions:** Read this document completely before editing.
> This plan is the deliberate model expansion that Plans 8–10 explicitly
> deferred. It must be implemented as designed here or stopped — not shrunk
> into a side-table memo API and not grown into a task/step/checkpoint
> vocabulary. Every durable progress record in Flow remains a command. If an
> implementation obstacle suggests a second concept, a seventh table, or a
> weaker fence, stop and amend this plan.
>
> **Depends on:** Plans 9 and 10 completed, reviewed, and released together as
> v0.3.0.
> Plan 9 establishes `Run`/`Enqueue`, direct-only root enqueue, migration 004,
> and the explicit `flow.Emit` versus `Event.Deliver` distinction. Plan 10 adds
> atomic current-run replacement, `CommandInfo.RunKey`, and a named
> transaction-scoped client without adding inline execution. Trails is not
> expected to adopt `flow.Call`.
>
> **Initial drift check:**
>
> ```text
> git status --short --branch
> git log -1 --decorate --oneline
> git describe --tags --always
> git diff --stat <approved-plan-10-release-tag>..HEAD -- \
>   definitions.go execute.go worker.go node.go command_runtime.go runtime.go \
>   trace.go types.go testing_bridge.go migrations.go migrations migrations_test.go \
>   internal/store internal/replay internal/testengine \
>   flowtest examples README.md flow.go specs/projects/flow
> ```
>
> Start from the reviewed Plan 10 release tag and reconcile all later drift.
> Unreconciled public API, journal, replay, transaction, or schema changes are
> a STOP condition until this plan is amended.

## 1. Purpose

Flow's durable progress today lives *between* commands. A worker's staged
children and events materialize only at successful settlement, so a handler
that performs several expensive external steps — most visibly an LLM agent
loop calling a model and tools tens of times — loses all intra-attempt
progress on any crash, timeout, or lease loss. The only current remedy is one
queued command per step, which costs a scheduler round trip, a claim, and a
settlement per step and forces state to travel by reference between them.

Plan 11 adds the missing synchronous tier without adding a new durable noun: a
**durable subroutine call** represented by an inline command. A worker may call
a registered command *now*, in process, under its own attempt's fence. The
inline child's accepted success is durably recorded as an ordinary command
result. When the parent attempt retries, reaching the same call returns the
stored result from the database without invoking the worker again.

```go
flow.Enqueue(work, key, cmd, args)          // async command, independent lifecycle
flow.Call(ctx, work, key, cmd, args) (R, err) // synchronous durable subroutine
```

The model stays exactly `run -> command -> worker -> event`. There is
no `Task`, `Step`, `Checkpoint`, or replay-from-the-top runtime. An inline
child is a command: keyed within its run, typed, fingerprinted,
journaled, counted against the command ceiling, visible in history and
`Trace`, and result-projected in `flow_commands` like every other command.

This borrows the useful Absurd checkpoint property — accepted completed work
returns its recorded value forever — while expressing it in Flow's own command
vocabulary. Absurd associates checkpoints with task runs and orders competing
attempts; Flow's proposed record boundary is stronger and more specific because
it verifies the active parent attempt ID, lease token, and unexpired lease
before accepting the result. Do not describe Absurd as having no fencing.

The user-facing mental model is deliberately small:

| Form | Use |
|---|---|
| ordinary Go function | cheap or deterministic work that is safe to repeat |
| `flow.Call` | synchronous durable subroutine whose result the parent needs now |
| `flow.Enqueue` | asynchronous command needing an independent queue, lease, retry, wait, delay, or concurrency boundary |

`Call` is not a general replacement for `Enqueue`. If the called work deserves
queue isolation or an independent durable lifecycle, it must remain an
independently enqueued child.

## 2. What already exists, and the exact gap

Plan 11 must not rebuild what Flow already provides:

1. **Run-level idempotency.** A permanent-keyed enqueue is idempotent:
   repeating it returns the existing run and its stored outcome.
2. **Command results are stored.** Successful settlement writes the result
   projection and terminal journal entry; point reads never replay.
3. **Cross-settlement memoization.** A retried parent attempt may repeat an
   already accepted equivalent child declaration; it coalesces rather than
   duplicating work.

The gap is precisely intra-attempt: staged children do not exist until the
parent settles, so durable progress cannot accumulate inside one handler
invocation. Plan 11 closes only that gap.

## 3. Design

### 3.1 Public API

```go
// Call invokes cmd's registered worker inline and durably records its
// success as an ordinary command of the same run. If an equivalent
// inline call under key already succeeded — in this attempt, an earlier
// attempt, or a previous holder of the lease — the stored result is
// returned without invoking the worker.
func Call[W, A, R any](
    ctx context.Context,
    work *Work[W],
    key string,
    cmd Command[A, R],
    args A,
) (R, error)
```

Notes:

- `ctx` is required because `Call`, unlike the staging helpers, performs SQL.
- `Call` is valid only from an active queued worker. It is not a root start,
  publisher API, commit-callback API, or arbitrary memoization helper.
- `ctx` must be the handler context, or a context derived from it which retains
  the same private attempt scope. `Call` verifies that the context scope and
  `work` scope are identical and still active. A background, unrelated,
  completed-handler, or mismatched context is `ErrInvalidState` and invokes no
  user code.
- `cmd` must be registered on the invoking runtime exactly like a queued
  command's worker. Unregistered name/version is `ErrInvalid` and poisons the
  change set: an executable unit without a registered worker is a
  programming error, not a deferral case.
- `key` obeys existing command-key rules and uniqueness within the run.
  Explicit keys are deliberate: there is no auto-numbering. A loop writes
  `fmt.Sprintf("turn/%d", i)` and owns its own ordering, which removes the
  silent replay-reordering corruption class that auto-numbered step names
  create elsewhere.
- The result type is decoded from the stored projection on the memoized path
  and returned directly on the invoked path; both paths validate against
  `cmd`'s codec.
- The inline child's `CommandInfo.CommandID` is deterministic for
  `(run ID, command key)` and is therefore stable before acceptance and
  across parent retries. Use one private fixed UUID namespace and UUIDv5 over
  the canonical bytes `run UUID || 0x00 || UTF-8 command key`; freeze the
  namespace and byte recipe with golden vectors. Do not allocate a fresh child
  ID on every invocation.
- `CommandInfo.Attempt` is `1` for the accepted success-only inline lifecycle.
  Failed invocations are failures of the parent attempt and do not create
  phantom child attempts.

### 3.2 Run semantics

An inline call proceeds as follows:

1. **Validate and reserve the key in memory.** Resolve the registered worker,
   encode the typed arguments, derive the deterministic child command ID, and
   compute the full declaration fingerprint including `delivery='inline'`.
   Before any SQL or user code, inspect the parent's private decision state:
   - an already staged queued child under the key conflicts immediately;
   - an existing equivalent inline reservation returns its in-attempt result
     when available;
   - a different inline reservation conflicts immediately; and
   - an absent key is reserved as inline before invocation.

   A reservation records `reserved`, `accepted`, or `failed`. Equivalent calls
   in the same attempt return the cached accepted result or cached failure and
   never reinvoke the body. The failure still poisons the parent. This is
   attempt-local caching only; durable truth is always re-read after retry or
   takeover.

   `flow.Enqueue` must perform the inverse check so a later queued declaration
   under a successfully called/reserved key also conflicts immediately. This
   prevents an expensive subroutine from running only for the parent decision
   to become doomed by a key collision at settlement.
2. **Preflight lookup.** In one short run-locked transaction, validate
   the parent's current attempt/lease fence and run deadline, then load
   the keyed durable command:
   - existing `inline` + `succeeded` + equivalent fingerprint: decode, cache in
     the attempt-local reservation, and return the stored result;
   - conflicting definition, version, arguments, parent, defaults, or delivery
     mode: `ErrConflict` and poison the parent decision;
   - any non-succeeded inline state: `ErrInvalidState` and poison (such a state
     is forbidden by the schema and indicates corruption);
   - absent and command ceiling already exhausted: reject and poison before
     invoking user code; or
   - absent with capacity: capture database time for the child created/start
     timestamps, then commit/rollback the lookup transaction before invocation.

   No run or queue lock remains held while application code runs.
3. **Invoke.** Run the registered worker in the parent's goroutine under a
   context derived from the parent attempt context and capped by the inline
   command's `WithTimeout`, if any. Use the deterministic child command ID and
   captured database time in `CommandInfo`. Recover panics exactly as queued
   workers do. Inline invocation does not acquire another worker or queue slot.
4. **Record accepted success.** Encode the result, then begin one transaction
   which locks the run and revalidates the parent's command ID, attempt ID,
   lease token, unexpired lease, and run deadline. Re-read the key
   because another active command or takeover attempt may have raced while
   user code ran:
   - if an equivalent inline success won the race, discard the local result,
     do not run the local commit callback, decode/cache/return the accepted
     stored result, and append nothing;
   - if a conflicting command won, roll back, poison, and return
     `ErrConflict`; or
   - if still absent, continue with the recording batch.

   For a newly accepted result, run the inline registration's `WithCommit`
   callback, if present, inside this same fenced transaction with the inline
   command's typed args, result, and `CommandInfo`. A callback error or panic
   rolls back the callback write and complete inline record, poisons the
   parent, and follows the parent's ordinary failure classification.

   Append exactly four adjacent journal entries:
   `command_created`, `attempt_started`, successful `attempt_concluded`, and
   the ordinary `event_recorded` command-terminal success containing the typed
   result. Insert the `flow_commands` row directly in `succeeded` state, set
   its result and terminal position, update run counters, and create no
   queue row. The journal records durable acceptance after the handler returns;
   `AttemptStartedBody.StartedAt`, `AttemptConcludedBody.FinishedAt`, and the
   command projection retain the actual captured invocation times.

   If transaction commit reports an ambiguous outcome, perform one bounded
   fresh keyed read. An equivalent succeeded row proves acceptance and its
   stored result is returned; definite absence is failure; an unresolved read
   returns the commit error and poisons the parent. Never rerun the body or
   callback inside the same attempt merely to resolve ambiguity — the normal
   parent retry will memoize the row if it later proves committed.
5. **Every in-worker failure poisons and records no child.** Validation, lookup,
   invocation, panic, timeout, context cancellation, result encoding,
   `WithCommit`, fence, ceiling, journal, projection, or commit failure is
   returned and, whenever an active owning `Work` scope exists, poisons the
   parent decision. Ignoring the Go error inside a worker cannot let the parent
   settle successfully. A call with nil/unrelated/closed work has no parent to
   poison and simply returns `ErrInvalidState`. No failed inline child row or
   child journal lifecycle remains. The parent's retry/classification path
   governs, and a later parent attempt re-runs only calls without an accepted
   result.

   A command-ceiling race is possible because another active command may
   consume the last slot after preflight but before recording. In that case the
   completed local body cannot be accepted; the parent is poisoned, and its
   next attempt rejects at preflight without invoking the child again. Test and
   document this at-least-once edge rather than adding a durable placeholder.

Keep the dispatch mechanism private. The real runtime installs an erased
inline-call bridge on the active work scope which can resolve registered
workers and reach the store; `flowtest` installs a deterministic in-memory
bridge on the same seam. `Call` must not discover a global runtime, accept a
public client option, or smuggle database handles through application context.
The bridge is cleared when the handler scope closes, which makes use-after-
handler rejection mechanical.

The contract, stated plainly: the inline worker body is **at-least-once** (a
crash between an external effect and the recording transaction re-runs it);
the recorded result is **effectively once** per key; and recording is
**fenced** — a stale parent attempt's inline result is rejected loudly, never
last-write-wins. External idempotency keys should derive from the stable tuple
`(run ID, inline command key, command name/version)` or the deterministic
inline `CommandID`, never from an invocation-local attempt ID.

### 3.3 Fencing and concurrency

The recording transaction validates the parent's current attempt ID and lease
token against the parent's queue row, exactly as settlement does, and it
requires the lease and run deadline to remain live at the acceptance
boundary. It does not extend or otherwise special-case the parent's lease.
The ordinary lease manager continues to renew the queued parent while its
handler is active. After lease takeover, the stale parent's next `Call`
recording fails with the existing lease-lost classification; the successor
attempt sees every result accepted before takeover and re-runs only the call
that had not been accepted.

The same command key may also be reached concurrently from work in one
run. The run lock serializes recording transactions. A retry of
an ambiguously committed equivalent record returns the stored winner; a call
from a different parent command conflicts because parent provenance is part of
the fingerprint. The local body may therefore have run before discovering a
winner or conflict, which is part of the documented at-least-once contract.

Two goroutines inside one handler must not `Call` concurrently against the
same run; the change-set scope is already documented as single-threaded
per attempt, and this plan does not change that contract.

An inline command's own registered `WithCommit` callback is supported and
runs inside the inline recording transaction. Calling `Call` *from* any
`WithCommit` callback is forbidden: both ordinary settlement and inline
recording already holds the run lock, so a nested recording transaction
would wait on itself. Track an explicit private `inCommit` attempt-scope flag,
set it around every commit callback, and reject before lookup or user code
with `ErrInvalidState`. The rejection poisons the owning decision. Error and
panic handling for the inline callback must match ordinary command settlement
and roll back the whole inline acceptance transaction. These are required
tests, not documentation-only rules.

### 3.4 Durable representation and migration 005

Inline children are rows in `flow_commands` with:

- a new `delivery text NOT NULL DEFAULT 'queued'` column with a named `CHECK
  (delivery IN ('queued','inline'))`, so every existing row is valid without
  rewrite; forward migration `005_inline_commands.sql` adds only this column
  and its inline-shape checks;
- no `flow_command_queue` row at any point in their lifecycle;
- states restricted to `succeeded` (v1 records success only), enforced by a
  named `CHECK` whose inline branch requires a non-null parent, `required`, the
  succeeded/result/terminal shape, zero unsatisfied waits, null initial-delay
  and wait fields, attempt ordinal `1`, consumed attempts `0`, equal non-null
  budget/next-attempt timestamps, and null failure fields; and
- ordinary provenance: `parent_command_id` = the calling command, same
  run, same composite FK discipline.

The database cannot express “no queue or wait row exists” as a row-local
`CHECK`, so store integration tests must assert both at acceptance,
memoization, rollback, retry, and takeover boundaries.

The row's `queue`, retry policy, and attempt timeout still record the command
definition and participate in its declaration fingerprint. They do not grant
the inline call a queue slot, independent retry policy, or lease. The timeout
is honored only as the derived invocation-context deadline. `created_at`,
`budget_started_at`, and `next_attempt_at` use the database time captured
immediately before invocation. `finished_at`, `status_at`, and `updated_at`
use the recording transaction's database time after invocation. Exact values
and constraints must be specified in migration/store tests, not left to
incidental insert defaults.

Journal reuse, not new kinds: extend `CommandCreatedBody` with:

```go
Delivery string `json:"delivery,omitempty"`
```

Absence decodes as queued so all old canonical journal bodies remain valid.
The delivery mode is also part of the durable declaration fingerprint, making
a queued/inline redeclaration under one key a conflict. Preserve queued
fingerprints byte-for-byte by omitting the field for queued delivery; do not
recanonicalize existing queued declarations with an explicit `"queued"`
value.

A newly accepted inline lifecycle is exactly four adjacent entries in one
`SemanticTx.Apply` batch:

1. `command_created`, with `delivery='inline'`, initial state `ready`, the
   definition queue/retry/timeout, no waits or delay, and causation pointing to
   the active parent's `attempt_started` position;
2. `attempt_started`, with a fresh invocation attempt ID, ordinal `1`, actual
   invocation start time, consumed attempts `0`, and lease duration `0` to
   mean “inline; no independent lease”;
3. successful `attempt_concluded`, with the same attempt ID, ordinal `1`,
   classification `succeeded`, actual finish time, consumed budget `false`,
   and consumed attempts `0`; and
4. the existing command-terminal `event_recorded` with
   `CommandSucceededBody`, the typed result, and `CommitApplied` reflecting
   whether the inline registration had a commit callback.

Entry 2 points to batch index 0, entry 3 to index 1, and entry 4 to index 2;
the created entry itself points to the durable parent attempt-start position.
Journal positions represent durable acceptance order; the timestamps inside
the bodies preserve when the body actually ran. Replay must explicitly allow
zero lease duration only for an inline lifecycle, reconstruct the same
succeeded projection, and reject missing, reordered, noncontiguous, queued, or
otherwise impossible inline histories. Failed invocations create none of
these entries; their durable evidence is the parent's ordinary failed attempt.

Six-table inventory is preserved. If implementation finds a seventh table,
journal-kind redesign, or non-additive migration necessary, that is a STOP.

Migration 005 remains format-additive: queued writers omit the journal delivery
field and rely on the new column default, while readers treat an absent field
as queued. Record `min_reader_version=2` and `min_writer_version=2` only after
compatibility tests prove those exact claims; otherwise stop and amend this
plan rather than silently changing the compatibility tuple.

The deployment boundary is nevertheless coordinated, not an arbitrary mixed-
library rolling upgrade. `CheckSchema` requires the library's exact current
migration set: Plan 11 code must see schema 5, while the prior release knows
only migrations through 004. The Plan 11 release guide must prescribe:

1. drain and stop prior Flow runtimes and publisher processes;
2. back up the Flow schema and apply migration 005;
3. deploy/start only the Plan 11 release; and
4. retain the documented rollback procedure as application/database restore,
   not running the prior binary against a schema it rejects.

Plan 9's schema 4 is an already released starting point. Test clean install,
populated schema-4-to-5 upgrade, pre-migration Plan 11 rejection, post-migration
Plan 11 acceptance, and prior-release rejection of the unknown schema-5 ledger.

### 3.5 Bounds

- Inline children count against the run's existing `MaxCommands`
  ceiling and the `open_commands <= command_count` relationship (they are
  created terminal, so they never contribute to `open_commands`).
- Existing per-command argument/result canonical byte bounds apply unchanged.
- A newly invoked call uses one short preflight transaction, runs application
  code with no database transaction held, then uses one short recording
  transaction. A memoized call needs only the preflight lookup. There is no
  batching in v1.
- Call arguments and results must remain small durable values. Growing
  transcripts, documents, and model context travel by a stable application
  reference, not by repeatedly embedding the full value in every child row.
  This avoids quadratic retained data in sequential loops.
- The command ceiling is checked before invocation and atomically again at
  recording. The documented capacity race in §3.2 is accepted; no placeholder
  row or reservation lifecycle is introduced.
- Performance is a Phase 0/4 measurement, not a promise. The expected benefit
  is simpler linear code and finer crash recovery without a scheduler round
  trip; implementation must not claim a multiplier before evidence exists.

### 3.6 Queue, retry, and commit semantics

`Call` deliberately bypasses the independent lifecycle represented by
`Enqueue`:

- the target queue name is retained for definition identity and inspection,
  but no queue capacity, queue ordering, or queue-specific concurrency limit
  applies;
- the target retry policy is retained for identity but is not run; an inline
  error fails/poisons the parent attempt, whose retry policy decides what
  happens next. Repeating the same key in that poisoned attempt returns the
  cached failure rather than retrying the body;
- `WithTimeout` caps the inline invocation context;
- the target's `WithCommit` callback is honored atomically at acceptance; and
- waits, start delay, a separate lease, and a separate worker slot do not
  exist for inline delivery.

This is the central selection rule: use `Call` only when synchronous execution
inside the current worker is intended. If queue isolation, independent retry,
waits, delay, fan-out, or separate concurrency control matter, use `Enqueue`.
Plan 9's duration normalization applies before either delivery path computes
identity: the inline timeout/defaults are already canonical integer
milliseconds, and Plan 11 must not add a second rounding rule. Equivalent
positive fractional timeout inputs therefore coalesce after Plan 9's upward
normalization.

At the root there is no `Call`: Plan 9's
`cmd.Enqueue(ctx, client, key, args, ...)` starts or rediscovers the queued run.
Inside a worker, `flow.Enqueue(...)` stages a queued child and
`flow.Call(...)` invokes a synchronous durable subroutine.

The same `Command[A, R]` definition and registered handler may be used through
either verb; delivery is chosen at the call site, not by defining an `Action`,
`Step`, or second handler type. Within one run, a particular command key
must choose one delivery mode forever.

### 3.7 v1 restrictions (deliberate)

Inside an inline child's worker:

- staging (`flow.Enqueue`, `flow.Emit`) is rejected and poisons the parent's
  change set — an inline child is a leaf in v1;
- `flow.Call` nesting is rejected in v1 (depth 1); and
- `GetEventValue` returns `found=false` from an empty snapshot — inline
  children declare no waits.

Plan 9's method form `event.Deliver(ctx, client, targetRunID, key, value)` remains
legal because it is explicit immediate targeted ingress, not staged child
composition. Treat it like any other external side effect: it is detached from
inline acceptance, may survive a failed/retried call, and therefore needs a
stable event key and deterministic payload. Top-level `flow.Emit(work, ...)`
remains forbidden because it would imply an inline staged decision that v1
does not persist.

Each restriction is enforceable, tested, and removable later by amendment.
Loosening any of them now multiplies the semantics this plan must prove.

### 3.8 Naming

`Call` pairs with `Enqueue` as the second verb of one vocabulary: *enqueue* a
command for its independent durable lifecycle, or *call* it inline as a
durable subroutine. Enqueue describes asynchronous delivery intent; a wait,
gate, or delay may defer the runnable queue projection. The rejected names are
recorded so they stay rejected: no
`Step`, `Checkpoint`, `Task`, `Memo`, or `Log` appears in the public API, docs,
or schema. Every durable unit remains a command.

## 4. Worked example (documentation target)

```go
type TurnArgs struct {
    SessionID     string
    Turn          int
    TranscriptRef string
}

type TurnResult struct {
    Message       Message
    TranscriptRef string
}

var AgentTurn = flow.DefineCommand[TurnArgs, TurnResult]("agent.turn", 1,
    flow.WithTimeout(2*time.Minute))

var RunAgent = flow.DefineCommand[AgentArgs, AgentResult]("agent.run", 1,
    flow.WithQueue("agents"), flow.WithTimeout(30*time.Minute))

var RunAgentWorker = flow.Handle(
    RunAgent,
    func(ctx context.Context, work *flow.Work[AgentArgs]) (AgentResult, error) {
        transcriptRef := work.Args.TranscriptRef
        for turn := 0; turn < maxTurns; turn++ {
            step, err := flow.Call(
                ctx,
                work,
                fmt.Sprintf("turn/%d", turn),
                AgentTurn,
                TurnArgs{
                    SessionID:     work.Args.SessionID,
                    Turn:          turn,
                    TranscriptRef: transcriptRef,
                },
            )
            if err != nil {
                return AgentResult{}, err
            }
            transcriptRef = step.TranscriptRef
            if step.Message.Final {
                answer := AgentResult{
                    Answer:        step.Message.Content,
                    Turns:         turn + 1,
                    TranscriptRef: transcriptRef,
                }
                flow.Enqueue(work, "publish", PublishAnswer,
                    PublishArgs{SessionID: work.Args.SessionID, Answer: answer})
                return answer, nil
            }
        }
        return AgentResult{}, flow.Permanent(errors.New("turn budget exceeded"))
    },
)

// Register RunAgentWorker together with the AgentTurn and PublishAnswer
// handlers on the runtime. AgentTurn can be invoked through either Call or
// Enqueue; the delivery choice belongs to the caller, not a second definition.
```

The transcript lives in application/object storage and each durable command
carries only a stable reference, so retained arguments do not grow
quadratically with the number of turns. The turn worker uses the stable
run/key identity for any external idempotency it needs.

A crash after turn 7 returns turns 0–7 from their stored command results and
re-runs only turn 8. `Trace` shows every accepted turn as an ordinary command
with its arguments, result, and timing. Publishing remains `Enqueue` because
it deserves its own queue/retry boundary and is not needed synchronously by
the loop. The boundary rule from Plan 9 gains its second half: *enqueue* a
command for an independent retry, queue, fence, wait, delay, or fan-out
boundary; *call* a durable subroutine when its result is needed now; keep
everything cheaper and safely repeatable in plain Go.

## 5. Implementation phases

### Phase 0 — Base, inventory, and go/no-go proof

1. Confirm HEAD is the reviewed Plan 10 release on top of tagged v0.3.0; record
   schema version, exported API, `go list -m all`, and clean ordinary/race
   baselines.
2. Inventory the exact worker registry, decision-state, declaration
   fingerprint, fence, success-settlement, journal, replay, trace, observer,
   and flowtest seams that the implementation will reuse. Record file/symbol
   evidence in a Plan 11 evidence document before editing production code.
3. Build a disposable, test-only proof against an isolated PostgreSQL schema
   for the preflight/body/record split. Prove that no transaction or run
   lock is held during the body, the parent fence is rechecked at acceptance,
   and one four-entry batch can reproduce the proposed projection. Delete the
   prototype before Phase 1; do not let a spike become a second path.
4. Benchmark 1, 10, and 50 sequential durable subroutines against the nearest
   current queued-command composition on the same durability-on PostgreSQL
   server. Record latency, transactions, journal bytes, allocations, and the
   recovery point after an injected crash. Use the same no-op worker, fixed
   small arguments/results, setup outside the timer, recorded PostgreSQL
   version/durability settings, and at least five samples per shape. The
   decision is based primarily on linear-code clarity and finer recovery;
   performance is supporting evidence.
5. Write compile-only fixtures for the proposed signature and a complete
   semantic decision table covering invoked, memoized, ambiguous-commit,
   different-parent conflict, ceiling, timeout, callback, and stale-fence
   outcomes.
6. Locate an approved retention/archival decision that covers terminal inline
   command and journal accumulation, or record an explicit operator decision
   that the proposed usage volume accepts unbounded growth. Do not assume that
   command projections make the journal redundant.

**Gate:** stop and amend or reject Plan 11 if the proof needs a transaction
held across user code, cannot preserve the ordinary parent fence and replay
model, needs a new durable noun/table/kind, or does not provide a clear DX and
recovery advantage over queued composition. A high-volume production release
also stops without the retention decision in item 6.

### Phase 1 — Migration 005 and store recording path

1. Add `005_inline_commands.sql` (delivery column, checks), registration,
   catalog assertions, clean-install, 003-to-005, and 004-to-005 upgrade tests;
   checksums of 001–004 unchanged. Record and verify the 2/2 reader/writer
   compatibility tuple plus the schema-4-to-5 checks described in §3.4.
2. Extend the journal codec and command declaration fingerprint with the
   backward-compatible delivery marker. Add the fixed private UUID namespace
   and deterministic `(run ID, command key)` child-ID derivation with
   golden-vector tests.
3. Implement the short store preflight and fenced single-transaction recording
   path: exact four-entry journal batch, succeeded command projection,
   counters, inline callback, and no queue row or child lease.
4. Fault-inject lookup, callback, apply, projection, and commit boundaries;
   prove rollback, stale-fence rejection, ambiguous-commit memoization,
   different-parent conflict rejection, definite and ambiguous commit
   outcomes, counter/ceiling behavior, and caller-owned transaction safety
   where applicable.

**Gate:** migration/schema/store tests plus full race suite on the store
package; invoked bodies run outside transactions; each acceptance is one
recording transaction; each new lifecycle is exactly four adjacent entries;
no queue/wait row or independent lease ever exists for an inline child.

### Phase 2 — Runtime `flow.Call` and guards

1. Implement `Call` over the store path: registry resolution, typed
   encode/decode, memoized return, invocation with derived timeout context,
   panic recovery, poison-on-misuse, active context/work identity validation,
   and the private real-runtime/flowtest dispatch bridge.
2. Add attempt-local inline key reservation and result caching; make `Enqueue`
   and `Call` reject cross-delivery key reuse in either order before user code
   or settlement. Delivery remains part of the durable conflict fingerprint.
3. Add the explicit inline/in-commit guards. Support the inline worker's own
   commit callback, but reject `Call` made from any commit callback. Enforce
   leaf-only, depth-1, no-wait behavior without reintroducing a broad public
   context protocol.
4. Test memoization across retry and takeover; deterministic child identity;
   same-attempt caching; Plan 9-normalized timeout identity; divergent
   args/defaults/delivery; ambiguous-commit equivalent recovery and
   different-parent conflict races; at-least-once
   re-run of an unaccepted invocation; timeout/panic/error classification;
   callback success/error/panic; ignored-error poisoning; preflight and
   recording-time ceiling races; rejection of top-level staging from inline
   workers; detached method-`Event.Deliver` parity; and repeated race runs.

### Phase 3 — Replay, trace, inspection, flowtest

1. Teach replay the exact four-entry inline lifecycle; add mixed queued/inline
   conformance fixtures and reject missing terminal events, noncontiguous or
   reordered entries, zero-lease queued attempts, and queue-implying inline
   histories.
2. Add `TraceCommand.Inline bool` rather than a second public definition type.
   Trace/history/read APIs present inline children as ordinary commands and
   verify that journal delivery and the current row agree. Success-only timing
   must describe actual body start/finish while journal position describes
   acceptance order.
3. `flowtest` support: a database-free `Call` bridge so worker unit tests
   exercise memoized and invoked paths deterministically.
4. Add observer coverage for inline lookup/invocation/acceptance outcomes
   using bounded metadata only; never attach arguments or results to
   observations.

### Phase 4 — Example, docs, release verification

1. Add `examples/agent-loop` (or extend the agent example) exercising crash
   recovery across inline turns against real PostgreSQL. Carry a stable
   transcript/state reference rather than copying growing history into every
   command, and include a final `Enqueue` child to demonstrate the boundary.
2. Update README, flow.go, functional spec (§ terms: inline command), schema
   and engine/runtime components, and the boundary-rule documentation; state
   the at-least-once body / effectively-once result / fenced recording
   contract explicitly. Publish a Plan 11 migration guide for `Call`, the trace
   field, migration 005, and the selection rule; link to rather than duplicate
   Plan 9's v0.2-to-v0.3 guide.
3. Repeat the disposable Trails compatibility proof against the Plan 11 head.
   Prove the retained independent-monitor-to-`intent.run` targeted delivery,
   run Trails's Flow-focused tests, and run migration 005 in the application's
   test database. Trails must not adopt `Call`; this is only a compatibility
   and non-regression gate.
4. Full gates on all supported PostgreSQL majors with durability on; focused
   ten-count race set including takeover during an inline loop, ambiguous
   commit resolution, and different parent commands racing one key; rerun the
   Phase 0 matrix and record memoized/invoked call cost, transaction count,
   journal bytes, and queued comparison as evidence, not a timing promise;
   govulncheck; rerun Plan 9's removed-API/forbidden-concept/schema checks and
   confirm its 23 acceptance criteria still hold; human review of every hunk
   against this plan; merge the exact reviewed commit; verify clean local/
   remote `master`; and only then create the separately approved release tag.

## 6. Acceptance criteria

1. Phase 0 records the current baselines, inventory, prototype proof,
   semantic decision table, and fair queued-vs-inline measurements; its
   go/no-go gate passes before migration or production implementation begins.
2. The public model remains run/command/worker/event. `flow.Call` is the
   only new callable concept, documented as a durable subroutine;
   `TraceCommand.Inline` exposes delivery without adding a second definition
   type or renaming either form as a step.
3. Schema remains exactly six tables. Plan 9 migration 004 provides the
   run-named catalog; migration 005 is forward-only and additive; 001–004
   checksums are unchanged; clean install plus 3-to-5 and 4-to-5 upgrades pass;
   migration 005's 2/2 format compatibility is proven; tagged v0.2.0
   rows/journal replay unchanged; and exact-schema startup/deployment behavior
   matches §3.4.
4. Every inline child has the deterministic UUIDv5 identity derived from its
   run and key. Delivery participates in the full declaration
   fingerprint, and `Call`/`Enqueue` key reuse conflicts immediately in both
   orders and durably across attempts.
5. A newly accepted inline child is one succeeded `flow_commands` row with
   ordinary parent provenance, result and terminal position, ordinal `1`,
   consumed attempts `0`, no wait/delay fields, no queue or wait row, no
   independent lease, and correct run counters.
6. Its journal is exactly four contiguous existing-kind entries — created,
   started, concluded, terminal result — with the specified backward
   causation. Body timestamps describe invocation; positions describe durable
   acceptance. Failed invocations leave no inline child history.
7. Memoized calls return the stored typed result within one attempt, across
   parent retries, after takeover, and after an ambiguously committed
   equivalent record, without invoking the worker or local commit callback
   again.
8. Acceptance reuses the parent's attempt ID, lease token, live lease, and
   run-deadline fence. A stale parent cannot record; no special parent
   lease extension is introduced; the successor sees every accepted result.
9. Every `Call` error associated with an active `Work` poisons the parent
   decision even when application code ignores the returned Go error. An
   invalid call with no active owner returns `ErrInvalidState`. Unaccepted
   bodies are at-least-once and follow parent failure/retry classification;
   recorded results are effectively once per key, not exactly-once external
   effects.
10. The inline worker's own `WithCommit` callback runs atomically in the
    recording transaction. Its error/panic rolls back the whole record.
    Calling `Call` from any commit callback fails before SQL or user code with
    `ErrInvalidState` and cannot deadlock.
11. `WithTimeout` is honored. Queue and retry settings remain part of identity
    and inspection but do not govern invocation. Plan 9's upward duration
    normalization is reused without a second canonicalization rule. Work
    requiring queue, independent retry/lease, wait, delay, fan-out, or
    concurrency isolation remains an enqueued command.
12. Leaf-only, depth-1, no-wait, no-staging restrictions and the existing
    single-threaded work-scope contract are enforced and tested. Plan 9's
    method `Event.Deliver` remains allowed only with its documented detached,
    immediate semantics.
13. Preflight and recording-time `MaxCommands` checks, canonical byte bounds,
    and `open_commands <= command_count` are tested, including the last-slot
    race with no placeholder lifecycle.
14. Replay rejects malformed inline histories and reconstructs mixed queued/
    inline history. Trace/history/read APIs, bounded observations, and flowtest
    cover invoked, memoized, conflict, and failure paths without exposing
    payloads in observations.
15. The example and normative docs teach plain Go versus `Call` versus
    `Enqueue`, use stable references for growing data, and make the
    at-least-once body / effectively-once result / fenced acceptance contract
    explicit. The Plan 11 migration guide covers this feature, and no
    `Step`/`Checkpoint`/`Task`/`Memo` vocabulary is added.
16. Full ordinary and race gates pass on every supported PostgreSQL major
    with durability enabled and zero unintended skips; focused race tests are
    repeated; govulncheck passes; the Trails compatibility proof
    passes; all Plan 9 acceptance criteria and deletion guards still pass; the
    final benchmark/evidence rerun makes no timing-threshold test or
    unsupported throughput promise; and only then is the Plan 11 release tagged.
17. High-volume production use has an approved retention/archival decision or
    an explicit documented acceptance of unbounded terminal command and
    journal growth; the journal is never disabled as a substitute.

## 7. STOP conditions

Stop and report rather than improvising if:

1. Phase 0 cannot prove the transaction-free body boundary, ordinary parent
   fence, replay shape, or a clear DX/recovery advantage over queued
   composition;
2. acceptance requires more than one recording transaction, a new journal
   kind, a seventh table, or a non-additive migration;
3. the fence check cannot reuse the existing attempt/lease/deadline validation
   without weakening it or holding a database transaction across user code;
4. stable inline identity cannot be derived before invocation without writing
   a placeholder row or adding another durable lifecycle;
5. memoized reads require replay, scanning, or an unbounded query instead of
   the keyed command result projection;
6. the exact four-entry journal lifecycle cannot stay contiguous with
   backward causation or cannot reconstruct the succeeded projection;
7. inline callback atomicity and the no-`Call`-from-commit guard cannot be
   implemented without deadlock or broad public context plumbing;
8. cross-delivery key conflicts cannot be detected before invocation and at
   the durable acceptance race boundary;
9. inline children require queue rows, scheduler awareness, an independent
   lease, or a retry state machine of their own;
10. mixed queued/inline replay conformance cannot be proven or older journal
    bodies would change meaning;
11. any existing invariant from Plan 9 §3.1 would be weakened;
12. the Trails proof requires a V1 compatibility root or any other capability
    deliberately removed by Plans 9 or 10; or
13. the example cannot be written within the leaf/depth-1 rules without
    copying growing state into every command — evidence that the v1 shape
    needs amendment rather than a workaround; or
14. high-volume release is proposed without an approved retention decision or
    explicit operational acceptance of unbounded growth.

## 8. Explicit non-goals

- No procedural replay-from-the-top runtime and no determinism enforcement:
  explicit keys are the contract.
- No inline-child retry scheduling, queue enforcement, lease, wait, delay,
  fan-out, or nested call/staging semantics in v1. `WithTimeout` remains a
  local derived-context cap.
- No special parent lease extension from `Call`; the existing lease manager
  owns parent renewal.
- No durable failed inline command/attempt history and no placeholder
  reservation row. Failure remains on the queued parent's attempt.
- No transactional coupling between an inline child's external effect and its
  recording; exactly-once side effects remain unpromised.
- No batching, async pipelining, or concurrency inside one attempt's calls.
- No root, publisher, detached, or cross-run `Call`; it is only a worker
  subroutine API.
- No retention or compaction changes. Before high-volume production use, an
  approved retention decision must bound or explicitly accept accumulation of
  terminal inline commands and journal entries; absent that decision, release
  approval stops.
- No changes to Absurd-comparison positioning docs beyond one paragraph
  documenting the enqueue-vs-call rule.

## 9. Punchlist

### Phase 0 proof

- [ ] Confirm the reviewed Plan 10 release on top of tagged v0.3.0; record clean
  API/schema/dependency/ordinary/race baselines.
- [ ] Inventory the registry, decision, fingerprint, fence, journal, replay,
  trace, observer, and flowtest seams with file/symbol evidence.
- [ ] Complete and delete the disposable PostgreSQL proof; prove no lock or
  transaction spans user code and the four-entry batch reconstructs exactly.
- [ ] Record fair 1/10/50 queued-versus-inline proof measurements and the
  injected-crash recovery boundary; approve the go/no-go gate.
- [ ] Freeze compile fixtures and the invoked/memoized/ambiguous-commit/
  different-parent-conflict/failure semantic decision table.
- [ ] Record the approved retention/archival decision for high-volume inline
  use, or explicit operator acceptance of unbounded command/journal growth.

### Schema, codec, and store

- [ ] Add migration 005 with the delivery column and named checks; register
  version 5; prove 001–004 checksums unchanged; test clean install plus
  3-to-5 and 4-to-5 upgrades.
- [ ] Prove the migration 005 reader/writer tuple while exact-schema startup
  enforces the schema-4-to-5 deployment sequence; document backup/restore
  rather than mixed-version rollback.
- [ ] Extend catalog assertions; preserve the six-table inventory.
- [ ] Add the backward-compatible command-created delivery marker and include
  delivery in declaration fingerprints.
- [ ] Add the fixed namespace and deterministic inline command UUIDv5 helper
  with stable golden vectors.
- [ ] Implement short fenced preflight plus single-transaction acceptance with
  the exact four-entry journal batch, succeeded projection, counters, inline
  callback, and no queue/wait row or lease.
- [ ] Implement equivalent-declaration memoized lookup with conflict
  rejection and atomic raced-winner handling.
- [ ] Fault-inject every boundary; prove rollback, stale-fence rejection,
  takeover visibility, ceiling race behavior, and exact row/journal shapes.

### Runtime API

- [ ] Add `flow.Call` with registry resolution, typed codecs, derived timeout
  context, panic recovery, active context/work checks, and a private runtime/
  flowtest dispatch bridge.
- [ ] Add attempt-local inline key reservation/result caching and make
  `Enqueue`/`Call` reject cross-delivery key reuse in either order.
- [ ] Add inline/in-commit scope guards; honor the inline worker's callback
  atomically while prohibiting `Call` from every commit callback.
- [ ] Enforce leaf-only, depth-1, no-wait/no-staging behavior and unconditional
  poison semantics for every returned error.
- [ ] Prove top-level `flow.Emit` is rejected from inline work while Plan 9's
  method `Event.Deliver` remains explicit detached ingress with unchanged
  idempotency/conflict behavior.
- [ ] Prove deterministic identity, same-attempt/across-retry/takeover/
  ambiguous-commit memoization, at-least-once unaccepted bodies, callback and
  classification behavior, timeouts, bounds, and both ceiling paths under
  repeated race runs.

### Replay, inspection, flowtest

- [ ] Teach replay the exact four-entry lifecycle; add mixed-history and
  malformed-history conformance fixtures.
- [ ] Add `TraceCommand.Inline`; surface delivery and success-only timing
  accurately in trace/history/read APIs and reject journal/row disagreement.
- [ ] Add database-free flowtest coverage for memoized and invoked paths.
- [ ] Add bounded observer outcomes for inline lookup/invocation/acceptance;
  prove arguments and results never enter observations.

### Example, docs, release

- [ ] Add the agent-loop example using stable transcript references, a final
  queued `Enqueue`, and a crash-recovery integration test.
- [ ] Document the plain-Go/call/enqueue selection rule and the
  at-least-once/effectively-once/fenced contract in README, flow.go, and the
  normative specs.
- [ ] Publish a Plan 11 migration guide for `Call`, trace, schema, and
  durability changes; link to the already released Plan 9 guide.
- [ ] Repeat the disposable Trails proof against the Plan 11 head, including
  an enduring independent-monitor targeted event, Flow-focused tests, and
  migration 005; Trails must not adopt `Call`.
- [ ] Run full ordinary/race/vulnerability gates on all supported PostgreSQL
  majors; rerun and record the Phase 0 measurement matrix without promises.
- [ ] Rerun Plan 9's removed-API, forbidden-concept, six-table/no-historical-
  rewrite, and consumer guards; confirm all 23 Plan 9 criteria still pass.
- [ ] Review every hunk against this plan; obtain human approval before
  merging the release commit; verify local/remote `master` and the clean
  worktree agree, then create the separately approved release tag.
