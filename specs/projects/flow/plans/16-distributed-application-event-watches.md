# Plan 16: Await durable application events across runtime instances

Status: Draft

> **Executor instructions:** Read this plan completely before editing. Follow
> the phases in order and run every phase's verification before continuing.
> This is a read-side observation feature, not a second workflow engine:
> watches must never create commands, leases, journal acknowledgements, or
> callback workers. PostgreSQL remains the durable authority and
> `LISTEN/NOTIFY` remains a disposable latency hint. Stop and report any design
> that requires a connection, additional background goroutine, or durable row
> per waiting caller. `Next` may block the caller's existing goroutine.
>
> **Drift check (run first):**
>
> ```sh
> git status --short --branch
> git diff --stat d7277f9..HEAD -- \
>   '*.go' internal/store README.md specs/projects/flow
> ```
>
> Re-read the current event-ingress, journal, notification, and runtime
> lifecycle code named in Section 2 if any in-scope source changed after
> `d7277f9`. A change to application-event identity, the run-first lock order,
> notification payloads, journal retention, or runtime shutdown is a STOP
> condition until this plan is updated.

## 1. Status and dependency

- **Priority:** P1; required by Trails API plan 012
- **Effort:** L
- **Risk:** MEDIUM; the public API is additive, but notification routing and
  missed-wake behavior are distributed-concurrency code
- **Depends on:** Plans 13, 14, and 15 completed on `master`
- **Category:** runtime / read API / distributed coordination / performance
- **Planned at:** `d7277f9a27e0871c8e5bb74b1ed56f546b9e2a1a`
  (`master`, one documentation commit after `v0.4.3`), 2026-08-13
- **Consumer:** Trails API plan 012, whose `WaitIntentReceipt` handler needs to
  await an `intent.receipt.changed` fact for the current `intent.run`
- **Recommended release:** `v0.5.0`, because this adds a public capability;
  Trails must consume the actual tag produced after this plan passes

## 2. Why this matters

Flow already owns the hard parts of a durable event: immutable run-local
identity, atomic journal persistence, cross-replica PostgreSQL coordination,
and a dedicated notification connection. It currently exposes application
events only as inputs to durable commands. An HTTP handler that wants to wait
for the *next* application fact cannot reuse that machinery without creating a
fake command or independently rebuilding a database listener and poll loop.

That gap is causing the first production embedder to duplicate Flow in the
application: Trails PR 1063 adds application triggers, another `LISTEN`
connection, a process-local subscriber map, server restart policy, and a
second correctness poll solely to wake `WaitIntentReceipt`. The desired model
is smaller:

```text
application transaction
  -> records an immutable Flow application event
  -> commit emits a tiny run-scoped hint
  -> every Flow runtime listening to that database receives the hint
  -> local waiters for that run re-read the durable Flow journal
  -> application re-reads its own projection and returns if it changed
```

The event payload is not the application's source of truth, and notification
delivery is not a correctness promise. The durable journal closes races and a
low-frequency Flow-owned poll repairs a lost hint. No sticky sessions or
application-specific database triggers are required.

## 3. Current state and verified evidence

The implementation starts from these facts at the planned commit:

- `definitions.go:23-26` defines typed `Event[T]`; `DefineEvent` fixes its
  application namespace and codec.
- `enqueue.go:329-397` implements `Event.Deliver`. Identity is immutable by
  `(run ID, event name, event key)`, and an equivalent repeat is idempotent.
  A different payload for the same identity is `ErrConflict`.
- `flow.Emit` stages same-run events atomically with a successful worker
  decision. `Event.Deliver` can join a caller-owned transaction through
  `Runtime.InTx`.
- `flow_journal` is ordered by `(run_id, position)` and already stores event
  name, key, class, canonical body, and recorded time. `flow_runs` already
  carries `next_journal_position`; no new cursor table is needed.
- `types.go:18` declares `JournalPosition`; `history.go` exposes the durable
  history projection. Using general `History` would force every caller to
  decode/filter pages and invent its own wait protocol.
- `inspection.go:238-269` implements `AwaitRun` with a durable row check, a
  process-local broad wake, and timer fallback. It waits only for terminal run
  state; it cannot report intermediate application events.
- `runtime_run.go:389-458` owns one session-capable PostgreSQL connection per
  runtime, outside the application pool. PostgreSQL sends each committed
  notification to every listening session, which is the distribution model
  required by handlers arriving on arbitrary pods.
- The listener currently parses a run ID and then calls only the broad
  scheduler `wakeHub`; it discards the parsed identity.
- `internal/store/store.go:302-327` emits a notification only when a transition
  creates immediately runnable commands. An application event with no durable
  command gate therefore records correctly but sends no hint.
- The notification payload is deliberately small and versioned:
  `{"v":1,"kind":"run","key":"<run UUID>"}`. Unknown payloads cause a safe
  broad wake.
- Plans 13 and 14 require polling to remain the correctness path. This plan
  preserves that rule; it moves polling behind one generic Flow contract
  rather than claiming PostgreSQL notifications cannot be lost.

The functional specification currently says Flow omits event handlers and
command-outcome subscriptions. Preserve that boundary. A caller awaiting an
immutable application event is an inspection operation: it does not consume
the event, run a callback, or durably react to it.

## 4. Controlling design

### 4.1 Public API

Add a typed future-event watch to `Event[T]`:

```go
type EventRecord[T any] struct {
	RunID      RunID
	Position   JournalPosition
	EventID    EventID
	Key        string
	Payload    T
	RecordedAt time.Time
}

func (event Event[T]) Watch(
	ctx context.Context,
	client Client,
	runID RunID,
) (*EventWatch[T], error)

func (watch *EventWatch[T]) Next(ctx context.Context) (EventRecord[T], error)
func (watch *EventWatch[T]) Close()
```

The names above are the target API. Do not replace them with a Trails-specific
helper or expose internal store/journal types. `EventWatch` has unexported
state and is deliberately small.

Contract:

1. `Watch` validates the event definition and run ID locally, registers for
   run-scoped wake hints, then validates the durable run/status and captures
   the current journal head as its baseline cursor in one store read. Events
   at or before that cursor are historical and are not returned.
2. Register-before-cursor-read is required. If an event commits during watch
   construction, the cursor includes it and the application read that follows
   sees the transaction that produced it.
3. `Next` first queries durable journal state for the earliest matching
   application event with `position > cursor`, then waits for a targeted hint,
   fallback timer, context cancellation, runtime shutdown, or run terminality.
   It repeats the durable query after every wake.
4. A returned event advances the cursor to its position. Sequential `Next`
   calls drain matching events in journal order. Concurrent `Next` calls on one
   watch are invalid; document and reject them rather than introducing
   nondeterministic cursor ownership.
5. `Close` is idempotent, removes the local registration, and unblocks an
   active `Next` with `ErrClosed`. Context cancellation returns `ctx.Err()`
   but does not implicitly close the watch; a caller may use another context
   for a later `Next` and remains responsible for `Close`.
6. A missing run at watch creation is `ErrNotFound`. A run that is already
   terminal at watch creation is `ErrTerminal`. Both are ordinary races for a
   caller that resolved a live key before calling `Watch`: that caller should
   re-read application truth and, if needed, resolve the current run again.
7. If the run becomes terminal, `Next` returns any matching event already
   committed after the cursor first; otherwise it returns `ErrTerminal`.
   This lets a caller re-read application truth and, for live-key replacement,
   resolve the new current run. If the previously verified run disappears
   because an explicit retention pass pruned it after terminal settlement,
   `Next` also returns `ErrTerminal`; a watch never pins retained history.
8. A transaction client is rejected as `ErrInvalid`: an open caller
   transaction cannot observe commits made after its snapshot and must not
   block while holding locks.
9. The watch holds no PostgreSQL connection between reads, creates no durable
   row, command, wait, lease, or acknowledgement, and does not count against
   worker or queue concurrency.
10. Watchers are broadcast readers. Any number of runtimes and callers may see
    the same event; nothing is consumed globally.
11. An open runtime may use a watch before `Runtime.Run`; the durable fallback
    remains correct without background services. An active `Runtime.Run`
    supplies notification latency and runtime-shutdown signaling. Once the
    runtime stops and closes, active watches return `ErrClosed`.

The documented race-free application pattern is:

```go
watch, err := changed.Watch(ctx, runtime, runID)
if errors.Is(err, flow.ErrTerminal) || errors.Is(err, flow.ErrNotFound) {
	// The run settled, was replaced, or was pruned after lookup. Application
	// state is still the response authority.
	return readApplicationProjection(ctx)
}
if err != nil {
	return err
}
defer watch.Close()

projection, err := readApplicationProjection(ctx)
if err != nil || projectionIsReady(projection) {
	return err
}

for {
	if _, err := watch.Next(ctx); errors.Is(err, flow.ErrTerminal) {
		return readApplicationProjection(ctx)
	} else if err != nil {
		return err
	}
	projection, err = readApplicationProjection(ctx)
	if err != nil || projectionIsReady(projection) {
		return err
	}
}
```

The watch must be established before the application read. A committed event
is then either included in the watch baseline and visible to the subsequent
application read, or it is after the baseline and returned by `Next`. A
terminal error from either watch construction or `Next` is a reason to re-read
application truth, not a reason to return stale projection data.

### 4.2 Durable query contract

Add narrow store reads rather than implementing watches through public
`History` pages:

- capture the current run head and `next_journal_position - 1` in one query;
- read the earliest application event for one `(run ID, event name)` after one
  position; and
- read run terminal status in the same query, so an idle watch does not need an
  additional `GetRun` round trip.

The event query is conceptually one statement with one lateral event lookup:

```sql
SELECT r.status,
       next_event.position, next_event.event_id, next_event.event_key,
       next_event.recorded_at, next_event.body
FROM flow_runs AS r
LEFT JOIN LATERAL (
    SELECT position, event_id, event_key, recorded_at, body
    FROM flow_journal
    WHERE run_id = r.run_id
      AND position > $2
      AND event_namespace = 'application'
      AND event_class = 'application'
      AND event_name = $3
    ORDER BY position
    LIMIT 1
) AS next_event ON true
WHERE r.run_id = $1;
```

Interpret the statement in one fixed order: return a non-null matching event
first even when the run is terminal; if no event exists and the status is
terminal, return `ErrTerminal`; otherwise wait. No row means `ErrNotFound`
during construction and `ErrTerminal` after construction already verified the
run.

Decode through the event definition's existing codec and canonical
application-event envelope. A malformed retained body is `ErrInvalidState`.
Never return raw journal bodies from this typed API.

The primary key `(run_id, position)` starts the scan at the watch cursor, so
old history before watch creation is not revisited. A running run has no strict
journal-entry ceiling, however, so do not assume every run is small. Do not
change the published baseline migration or add a schema migration by default.
Before considering an index, use `EXPLAIN (ANALYZE, BUFFERS)` at 100, 1,000,
and 10,000 post-cursor entries with a sparse matching event at the end. Add a
follow-up migration only if measured work is materially unbounded for the
target workload; that is a STOP-and-report decision, not pre-approved scope
for this plan.

### 4.3 Targeted local wake hub

Add a runtime-owned event wake hub keyed by parsed `RunID`. It should use a
generation/channel pattern like the existing `wakeHub`, with these additions:

- multiple watchers for one run each wake on one signal;
- a signal is coalesced, not queued per event;
- registration/unregistration is bounded and leak-free;
- reconnect performs a catch-up signal for all current watches;
- runtime shutdown closes all watches; and
- an unrelated run hint does not wake or re-query this run's watches.

Use one shared generation/channel entry per watched run plus one close channel
per `EventWatch`; do not allocate one hub goroutine or one notification queue
per watcher. Remove the run entry when its final watch closes. Watch
construction must unregister on every validation/cursor-read failure.

Keying only by run ID is intentional. PostgreSQL hints do not carry payloads or
event names; durable queries perform the exact event-name filter. This keeps
notification payloads small and makes a single hint cover several application
events committed for the same run.

### 4.4 Notification hint vocabulary

Retain version 1 and the existing `kind:"run"` payload for immediately
runnable work. Add one understood kind, for example:

```json
{"v":1,"kind":"event","key":"<run UUID>"}
```

Semantics:

- `run`: wake the command scheduler and event watches for that run;
- `event`: wake only event watches for that run;
- unknown/malformed: retain the conservative broad scheduler wake and signal
  all event watches, because no run identity can safely be trusted.

Keep payload version 1. During a rolling upgrade, an older runtime treats the
new `event` kind as unknown and performs its existing conservative scheduler
wake; a newer runtime routes it to watchers. Hints contain no durable truth, so
no compatibility decoder or second payload version is needed.

Emit an `event` hint when a transaction records an application event but does
not make a command runnable. If that same semantic transaction makes work
runnable, a `run` hint is sufficient for both consumers.

Keep notification state local to the existing run-scoped `SemanticTx` and its
`notificationOwner`; do not add a transaction registry, transaction callback,
or map of touched runs. The simple rules are:

- suppress an `event` hint if that semantic operation already requested an
  `event` or stronger `run` hint;
- suppress a repeated `run` hint;
- if an `event` hint was already requested and the same transaction later
  makes work runnable, permit one `run` hint as the upgrade; and
- let PostgreSQL fold identical channel/payload notifications produced by
  separate semantic operations in one caller-owned transaction.

The ordinary maximum is therefore one `event` plus one `run` payload for one
run in one transaction, not an elaborate application-side exactly-one
protocol. PostgreSQL notifications are disposable hints, and distinct runs
have distinct payloads. A hint for run A must never suppress a hint for run B.
Do not send event payloads, names, keys, tenant identifiers, or secrets through
`pg_notify`.

Implement this by replacing the current single `notificationSent` bit with the
smallest equivalent per-semantic state, such as `eventHintSent` and
`runHintSent`, inherited through `notificationOwner`. Do not introduce a
general-purpose notification abstraction. Keep `NotifyRunnableCommands` for
the stronger existing `run` hint and add one narrowly named internal method
for the `event` hint, such as `NotifyEventWatchers`.

Run terminal transitions must also wake event watches. A watcher awaiting an
event that will never occur needs prompt `ErrTerminal`, especially when
`ReplaceCurrentRun` cancels one live-key holder and creates another. Emit a
run-scoped event-watch hint for terminal settlement even when no command became
runnable. Replacement must wake watchers for the predecessor; the caller then
re-resolves the live key and watches the replacement.

Call the narrow event-hint method from the store operation that already knows
an application event was accepted without immediate readiness, or that a run
projection became terminal. Do not emit from observers, public API wrappers,
or a duplicated list of after-commit call sites; those locations either run
too late or cannot share caller-owned transaction atomicity.

All notification calls remain inside the semantic transaction. PostgreSQL
must deliver them only if that transaction commits. A rolled-back application
event or terminal transition must produce neither durable data nor an
actionable wake. Do not add a general after-commit hook. A same-runtime caller
receives its own committed PostgreSQL notification when notifications are
enabled; when they are disabled, the documented fallback is the correctness
path. A local post-commit signal is allowed only at an existing code boundary
that already knows a runtime-owned commit succeeded, and is not required for
this plan.

### 4.5 Poll fallback and efficiency

Notifications are the hot path; durable polling remains correctness. Use one
private five-second event-watch fallback:

```text
eventWatchFallback = 5s
```

Do not derive it from `runtime.pollInterval`: command scheduling and read-side
HTTP observation have different load/latency tradeoffs. Five seconds bounds
recovery when notifications are disabled, a listener reconnects, or a hint is
lost, while limiting 1,000 continuously waiting callers to at most about 200
fallback reads per second before query-duration effects. Tests may use one
private duration seam rather than sleeping five seconds; do not add a public
runtime option. Every reconnect signals current watches immediately before
waiting for new notifications.

Do not create a goroutine or timer per registered watch while it is idle.
`Next` may block the caller's existing goroutine on a timer/channel select.
There is one dedicated PostgreSQL listener connection per runtime, not per
watch. Multiple waiters may each perform the one durable read needed to decode
their result after a hint; do not add a cache of typed payloads in this phase.
If measured production demand later makes shared fallback reads worthwhile,
that is a separate optimization with its own cache/lifetime contract.

## 5. Scope

### In scope

- `event_watch.go` and `event_watch_test.go` (new) for the typed public watch
  and its state.
- `runtime.go`, `runtime_run.go`, and focused tests for the run-targeted wake
  hub, listener routing, reconnect, and shutdown.
- `internal/store/store.go` plus a narrow store read file/test for cursor,
  next-event, terminal, and notification decisions.
- `enqueue.go`, `command_runtime.go`, and existing `internal/store` event and
  terminal transition paths required to emit a committed hint. Touch only
  paths that already know an event was accepted, work became runnable, or a
  run became terminal.
- `README.md`, `specs/projects/flow/functional_spec.md`,
  `specs/projects/flow/architecture.md`, and the relevant runtime/engine
  component specs.
- Compile-contract coverage for the exported generic API.

### Out of scope

- Persistent subscriptions, callbacks, event handlers, webhooks, consumers,
  acknowledgement offsets, or global topics.
- Waiting for arbitrary command outcomes or OR/quorum/race conditions.
- A durable command whose only work is waiting for another event.
- Searching runs by untyped “entity ID.” Applications use their root command
  definition and stable run key with `GetCurrentRun`.
- Direct application access to Flow SQL or the notification channel.
- New tables, columns, triggers, brokers, advisory locks, or a required schema
  migration.
- Changing application-event identity, `WaitFor` gate semantics, journal
  retention, run lock order, attempt fencing, queue scheduling, or delivery
  guarantees.
- Guaranteeing zero fallback reads. PostgreSQL notifications are hints, not a
  durable message broker.
- A public watch-poll option, shared typed-payload cache, transaction-wide
  notification registry, or general after-commit callback mechanism.

## 6. Implementation phases

### Phase 0 — characterize the current contract

Before editing, add or identify tests proving:

- an application event with no matching command wait records durably but emits
  no current notification;
- an application event that releases a wait emits the existing run hint;
- each runtime instance connected to the same database receives committed
  hints; and
- a rollback emits no visible notification.

Record the current query plan for the Section 4.2 shape at 100, 1,000, and
10,000 post-cursor journal entries with the matching event last. Also retain a
five-sample baseline for the existing no-wait external-event ingress benchmark
with notifications enabled. This is evidence only; do not add an index unless
the STOP gate is reached.

**Verify:**

```sh
go test -count=1 -p 1 -parallel 4 -run 'Notification|Deliver|Event' ./...
go test -run '^$' \
  -bench '^BenchmarkExternalEventIngress/hot_live/no_match$' \
  -benchtime=3s -count=5 ./...
```

Expected: all characterization tests pass and the no-waiter event test proves
the exact missing hint this plan addresses; five benchmark samples and the
three query plans are recorded as baseline evidence.

### Phase 1 — add typed durable event reads

Implement the store cursor/next-event query and payload decoding. Add the
public `EventRecord[T]` and watch construction/`Next`/`Close` API with timer-only
wake behavior first. Reject invalid events, invalid/missing/terminal runs,
transaction clients, concurrent `Next`, and use after close with existing Flow
sentinel categories. Map disappearance of a run that was verified during
watch construction to `ErrTerminal`; do not pin or recreate pruned history.

Tests must cover:

- events before the watch baseline are not returned;
- several events after the cursor return in position order;
- other event names and runtime events are ignored;
- event keys and typed payloads round-trip;
- malformed stored payload returns `ErrInvalidState`;
- a matching event committed between the pre-wait query and the channel wait
  is found on the next durable read;
- terminal-with-event returns the event before `ErrTerminal`;
- missing and terminal-at-start construction races;
- terminal pruning after construction returns `ErrTerminal`;
- transaction-client, close, and concurrent-`Next` behavior;
- a cancelled `Next` returns `ctx.Err()`, remains reusable, and is removed only
  when the caller invokes `Close`; and
- notifications disabled still observes an event through fallback polling.

**Verify:**

```sh
go test -race -count=1 -p 1 -parallel 4 -run 'EventWatch' ./...
```

Expected: all new API and durable-read tests pass with no race report.

### Phase 2 — route targeted hints across runtimes

Add the run-targeted event wake hub and route parsed notification payloads.
Preserve the existing broad scheduler wake for `kind:"run"`. Add reconnect
catch-up and shutdown behavior. Do not let watcher map locks cover a database
query or caller callback. These routing tests may commit `pg_notify` directly
after arranging durable event state; Phase 3, not this phase, connects normal
event/terminal writes to the new hint.

Tests must start two runtimes against one database and prove:

- a committed test `pg_notify` carrying `kind:"event"` for a watched run wakes
  Runtime B before a deliberately long fallback;
- two runtimes and multiple watchers on each all observe the same fact;
- an unrelated run hint does not wake/query the watched run;
- a malformed/future hint performs a conservative catch-up;
- forced listener disconnect/reconnect cannot strand a committed event;
- `WithNotifications(false)` uses the bounded timer path;
- caller `Close` and `Runtime.Run` shutdown remove every registration;
- a watch created before `Runtime.Run` observes through fallback, then gains
  hints after `Run` starts; and
- no application-pool connection remains checked out while `Next` waits.

**Verify:**

```sh
go test -race -count=1 -p 1 -parallel 4 -run 'EventWatch|NotificationListener' ./...
```

Expected: cross-runtime and lifecycle tests pass with no race report or leaked
connection/registration.

### Phase 3 — emit event and terminal hints transactionally

Generalize notification parsing/state with the small per-semantic rules from
Section 4.4:

- application event plus immediate readiness -> `run`;
- application event without readiness -> `event`;
- terminal run without readiness -> `event`;
- no externally relevant change -> no new hint.

Exercise `Event.Deliver`, staged `Emit`, worker settlement, direct cancellation,
deadline expiry, and `ReplaceCurrentRun`. Keep existing runnable-command hint
behavior and ambiguous-commit/fault-hook boundaries unchanged.

Tests must prove:

- event-only commits now wake remote watches without waking the new runtime's
  scheduler path;
- Runtime A commits `Event.Deliver`; a watch created through Runtime B wakes
  before a deliberately long fallback and returns the durable typed event;
- application event plus released command emits no more than one `event` and
  one `run` hint for that run and both scheduler and watcher progress;
- identical payloads from repeated semantic operations in one caller-owned
  transaction are folded by real PostgreSQL without a Flow transaction map;
- equivalent idempotent event redelivery does not generate another semantic
  event/hint;
- rollback and failed `WithCommit` generate no durable event/hint;
- every terminal status wakes the watch to `ErrTerminal`; and
- atomic live-key replacement wakes the predecessor watch and exposes the new
  holder through `GetCurrentRun`.

**Verify:**

```sh
go test -race -count=1 -p 1 -parallel 4 -run 'EventWatch|Notification|ReplaceCurrentRun|Terminal' ./...
```

Expected: all notification, replacement, and terminal regressions pass.

### Phase 4 — document and measure the contract

Update public and project documentation with:

- the watch-before-application-read recipe;
- the distinction among `WaitFor`, `Event.Deliver`, `Event.Watch`, `History`,
  `GetResult`, and `AwaitRun`;
- cross-runtime broadcast behavior and lack of sticky-session requirements;
- notification-as-hint and fallback-poll semantics;
- terminal/replacement behavior; and
- the rule that application tables remain source of truth.

Add a benchmark or bounded query-count test for 1,000 idle watches on one
runtime. It need not assert wall-clock speed. It must prove one listener
connection, no worker/queue/lease growth, no goroutine created merely by
registration, no application connection held while waiting, and fallback
reads bounded by the documented five-second interval. Use a private test seam
for the interval rather than adding a public option.

Repeat the existing no-wait external-event ingress benchmark for five samples
with notifications enabled. Record median/range and PostgreSQL protocol/query
count before and after. Investigate before completion if the new hint adds more
than 10% median latency on the same environment or introduces work that scales
with retained history. Small variance below that threshold is evidence, not a
reason to add batching, caching, or another abstraction.

**Verify:**

```sh
go test -count=1 -p 1 -parallel 4 ./...
go test -run '^$' \
  -bench '^BenchmarkExternalEventIngress/hot_live/no_match$' \
  -benchtime=3s -count=5 ./...
test -z "$(gofmt -l .)"
go vet ./...
go mod tidy -diff
go mod verify
```

Expected: ordinary suite and all quality checks pass; module files are clean.

### Phase 5 — full race gate and release

Run from a reset PostgreSQL test database:

```sh
make test-with-reset
make build
git diff --check
```

Expected: the complete serial race suite passes, all packages/examples build,
and the diff has no whitespace errors. Review the final diff specifically for
per-wait goroutines/connections, notification payload growth, application
payload logging, and any accidental schema change.

After merge, tag `v0.5.0` (or the maintainer-approved next minor version) from
the reviewed commit. Record the exact tag and commit in Trails plan 012 before
that consumer upgrades.

## 7. Test matrix

| Case | Durable result | Wake path | Expected watch result |
|---|---|---|---|
| Event existed before `Watch` | event retained | none required | excluded by baseline |
| Event commits after `Watch` on same runtime | event retained | PostgreSQL hint or fallback | next typed record |
| Event commits on another runtime/pod | event retained | PostgreSQL broadcast | next typed record |
| Hint is dropped | event retained | fallback timer | next typed record |
| Notifications disabled | event retained | fallback timer | next typed record |
| Event transaction rolls back | no event | no committed hint | continues waiting |
| Equivalent event redelivery | one event | no duplicate semantic hint | one record only |
| Different event name/run | other event retained | targeted/filtered | continues waiting |
| Run terminates | terminal projection/journal retained | terminal hint/fallback | `ErrTerminal` after matching-event drain |
| Terminal run is pruned after watch creation | run/history removed | hint/fallback | `ErrTerminal` |
| Live run is replaced | old terminal, new live holder | predecessor terminal hint | `ErrTerminal`; caller re-resolves |
| One `Next` context ends | no state change | context | `ctx.Err()`; watch remains until `Close` |
| Runtime stops | durable state unchanged | hub close | `ErrClosed` and cleanup |

## 8. Done criteria

- [ ] `Event[T].Watch`, `EventWatch[T].Next`, and `Close` implement the exact
      contract in Section 4.1.
- [ ] Future matching events come from durable journal reads, not notification
      payloads or process memory.
- [ ] One Flow runtime uses one existing dedicated listener connection for any
      number of watches.
- [ ] Cross-runtime tests prove a commit on A wakes a watch on B; no sticky
      session assumption exists.
- [ ] Application-event and terminal-only transactions emit a committed
      run-scoped hint; runnable semantics are unchanged.
- [ ] Lost/disabled notifications remain correct through the bounded fallback.
- [ ] The fallback is a private fixed five seconds and does not inherit command
      scheduler polling cadence.
- [ ] Run replacement releases the old watch so callers can re-resolve a live
      key.
- [ ] No new command, wait row, lease, table, trigger, broker, or schema
      migration is introduced.
- [ ] `make test-with-reset`, `make build`, `go vet ./...`, formatting, module
      consistency, and `git diff --check` all pass.
- [ ] README and normative specs explain the read-side-only model and the
      watch-before-read race closure.
- [ ] The reviewed release commit is tagged and available for Trails plan 012.

## 9. STOP conditions

Stop and report; do not improvise if:

- correct observation appears to require trusting `NOTIFY`, holding a database
  connection while waiting, or creating durable state per watcher;
- application events can commit without an identifiable run ID;
- the journal query needs a schema/index change to stay bounded at the tested
  10,000-entry post-cursor shape;
- event/run hinting cannot preserve current immediate-runnable behavior with
  the bounded per-semantic rules in Section 4.4 without moving a fault/commit
  boundary or adding transaction-wide state;
- terminal replacement cannot wake the predecessor without changing live-key
  or cancellation semantics;
- watcher cleanup requires one background goroutine per registered watch;
- the 10,000-entry sparse query or 1,000-watcher fallback test shows work that
  is not bounded by the stated cursor/interval contract;
- no-wait external event ingress regresses by more than 10% median in a
  same-environment five-sample comparison and the regression cannot be
  explained or corrected without expanding scope;
- a transaction client would have to wait for external commits; or
- any test exposes a journal, fencing, lock-order, queue, or scheduler behavior
  change outside this plan.

## 10. Punchlist

- [ ] Characterize event-only, runnable-event, broadcast, and rollback hints.
- [ ] Add typed cursor/next-event store reads and payload decoding.
- [ ] Add `EventRecord`, `Event.Watch`, `EventWatch.Next`, and `Close`.
- [ ] Add the run-targeted local wake hub and listener routing.
- [ ] Add the `event` notification kind with bounded per-semantic suppression;
      rely on PostgreSQL for identical-payload folding.
- [ ] Wake watches on every run terminal path and live-key replacement.
- [ ] Prove cross-runtime, reconnect, fallback, cancellation, and shutdown
      behavior under `-race`.
- [ ] Document the API distinctions and watch-before-read recipe.
- [ ] Measure sparse post-cursor reads, 1,000 idle watches, and event-ingress
      overhead without adding speculative indexes or caches.
- [ ] Run ordinary, race, build, vet, format, module, and diff gates.
- [ ] Tag the reviewed public feature release and record its commit for Trails.

## 11. Maintenance notes

- An event watch is an inspection primitive. If a future consumer asks Flow to
  durably react, retry a callback, acknowledge offsets, select one of several
  events, or keep a global subscription, that is a different architecture and
  needs its own plan.
- Never recycle an event name for a changed payload schema. Watches decode with
  the same definition contract as `WaitFor` and `Deliver`.
- Keep notification payloads identity-only. Applications may place sensitive
  data in event bodies; it must remain in the durable database path and out of
  `pg_notify`, logs, and observations.
- Polling stays as repair, but consumer code should not add another correctness
  ticker around `EventWatch`. The consumer re-reads its projection after an
  event and once when its own request deadline expires.
- Application event keys must identify one immutable fact. An empty payload
  such as `None` cannot expose accidental key reuse, so an application whose
  status can recur must include a stable causal identity or durable generation
  in the key rather than relying only on the status text.
- A future optimization may coalesce fallback reads among many watchers for
  one run, but only after production evidence justifies the additional cache
  and lifecycle state. It is deliberately absent here.
