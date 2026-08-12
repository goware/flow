# flow

`flow` is a Go library for event-driven, durable, distributed work backed by PostgreSQL.

```text
command -> worker -> result + events
                         |
                         +-> optional sub-commands
```

Commands are the only durable unit of orchestration. Workers perform typed work, emit immutable run-scoped events, and stage bounded sub-commands. Exact event gates provide sequencing and joins. PostgreSQL stores the queue, leases, projections, and a gap-free journal for each run.

## Install

```bash
go get github.com/goware/flow
```

Flow uses the application's existing PostgreSQL database. Its six tables use a `flow_` prefix and default to the `public` schema. `flow.WithSchema` selects another schema.

The current v0.x line supports Go 1.26 and PostgreSQL 17 and 18. During v0.x,
intentional Go API changes may be made with release notes. Published migration
files are immutable: upgrades add forward migrations, and applications must run
`Migrate` before starting a newer runtime. Back up durable data before upgrades.

Run migrations explicitly during deployment:

```go
if err := flow.Migrate(ctx, db); err != nil {
	return err
}
```

`flow.New` validates the installed schema and starts no goroutines. Register workers, then call `Runtime.Run` in each process that should execute work.

## A command

```go
type emailArgs struct {
	To string `json:"to"`
}

type emailResult struct {
	MessageID string `json:"message_id"`
}

var sendEmail = flow.DefineCommand[emailArgs, emailResult]("mail.send", 1)

func sendEmailWorker(ctx context.Context, work *flow.Work[emailArgs]) (emailResult, error) {
	return emailResult{MessageID: "provider-123"}, nil
}

runtime, err := flow.New(db)
if err != nil {
	return err
}
if err := runtime.Register(flow.Handle(sendEmail, sendEmailWorker)); err != nil {
	return err
}
go runtime.Run(ctx)

run, err := sendEmail.Enqueue(ctx, runtime, "email/order-42", emailArgs{
	To: "person@example.com",
})
```

`Command.Name`, `Command.Version`, and `Command.Queue` inspect the immutable
definition without accessing the database. `Queue` returns the configured
delivery lane, or Flow's normalized `"default"` lane when `WithQueue` was not
specified.

`Work[A]` is the attempt-local scope for one claimed command. It is not the
whole `Run` and it is not the immutable `Command[A, R]` definition. Each
worker invocation receives a fresh `Work` containing typed arguments,
run/command/attempt identity, materialized event inputs, and the private
decision state used by `Enqueue`, `Emit`, and `GetEventValue`. It is valid only
for that worker call and must not be retained or used concurrently.

`Enqueue` always creates or rediscovers durable asynchronous work; it never calls a worker inline. A stable non-empty run key is permanently idempotent by default. `flow.WithLiveKey()` instead deduplicates only while a run is non-terminal.

`Enqueue` returns the `Run` snapshot as of durable acceptance; `Created`
reports whether this call created it. `GetRun` and `AwaitRun`
return the same type with the run's current or final state.

Read one successful command result by its stable key without loading the full
run trace:

```go
value, found, err := flow.GetResult(ctx, runtime, run.ID, "finalize", finalizeOrder)
```

`found=false` means no successful result is currently available. Use `Trace`
when the complete command graph, attempts, events, or journal is needed.

## Composing work

A successful worker may atomically emit events and stage sub-commands:

```go
var charged = flow.DefineEvent[chargeResult]("billing.charged")

func chargeWorker(ctx context.Context, work *flow.Work[chargeArgs]) (chargeResult, error) {
	result := chargeResult{Receipt: "receipt-42"}
	if err := flow.Emit(work, charged, "charge/42", result); err != nil {
		return chargeResult{}, err
	}
	flow.Enqueue(work, "notify/42", notifyCustomer, notifyArgs{Receipt: result.Receipt})
	return result, nil
}
```

Repeated declarations with the same key and canonical content coalesce. Conflicting declarations poison the complete decision. A `WithCommit` callback can update application tables in the same fenced transaction as Flow settlement.

Choose command boundaries around independent retry, side effects, isolation,
timeouts, queue ownership, or useful parallelism. Keep small deterministic
transformations in the worker that owns them instead of turning every business
logic microstep into durable work. Several small writes to the same PostgreSQL
database can usually share one `WithCommit` callback. Keep that callback short
and database-only: it holds the run lock until settlement commits and is
not an exactly-once boundary for remote calls.

One run is one serialized semantic aggregate. Keep causally related work
together, but use separate runs for independent bulk items or shards
instead of treating one run as a tenant-wide work container. The default
1,000-command ceiling is a safety limit, not a recommended run size;
ordinary runs are usually clearer in the tens or low hundreds. For a very
large fan-out, have bounded batch commands declare later batches, and combine
large input sets through hierarchical join commands rather than one enormous
child declaration or join.

## Exact event gates and inputs

A root or sub-command may wait for exact application events:

```go
var approved = flow.DefineEvent[approval]("orders.approved")

flow.Enqueue(work, "fulfill/42", fulfill, args).
	WaitFor(approved, "approval/42").
	Within(30 * time.Minute).
	Delay(time.Second)
```

The waiting worker gets the value attached to a declared event:

```go
value, found, err := flow.GetEventValue(work, approved, "approval/42")
if err != nil {
	return result{}, err
}
if !found {
	return result{}, errors.New("required approval is absent")
}
```

Multiple waits are AND conditions. Matching is exact on event name and key within one run. Events recorded before command declaration still satisfy the gate. `Within` starts at command creation and runs independently of `Delay`. At most 256 waits may be declared for one command; larger joins should use a tree of join commands or stable external references.

Pass data computed by a parent directly in child arguments. Use exact events
for sibling, cross-branch, or external facts, and stage related events and
children in the same worker decision when they belong to one atomic change.
Large or sensitive documents should remain in application storage; pass stable
references through command arguments or event payloads.

Flow has two event paths:

| API | Use |
| --- | --- |
| `flow.Emit(work, ...)` | stage an event in the current run with the worker decision |
| `event.Deliver(ctx, client, runID, ...)` | immediately record a detached event in a known run, including from an active worker |

`Deliver` needs the exact target run ID. With a transaction client it commits or rolls back with the caller's application writes; with a regular runtime client it commits independently. A committed delivery survives source failure and retry, so producers should use stable event keys and deterministic payloads. Same-run worker events should use staged `flow.Emit`: explicitly delivering to the current run is detached and may survive a failed attempt. Delivery is targeted ingress, not publish/subscribe, and target workers remain at-least-once.

Typed event definitions name stable fact kinds. Put entity and generation identity
in one deterministic event-key helper used by `WaitFor`, `Deliver`, and
`GetEventValue`. If an external publisher knows only a domain key, it may call
`GetCurrentRun`, then `Deliver` to the returned ID. The run can settle between
those operations, so `ErrTerminal` is an expected race to handle explicitly.

Positive durable durations may be fractional: Flow rounds them upward once to
the next whole millisecond before fingerprinting or persistence. Zero and
negative values retain each option's validation rules.

Fan-out, fan-in, multi-stage joins, branches, and bounded loops are ordinary command composition. Flow intentionally has no separate coordinator/state-machine API, outcome subscriptions, OR/quorum/race gates, or automatic result dataflow.

## Examples

Each example contains its complete, self-documenting logic:

- `examples/direct`: one background command;
- `examples/fanout`: two command-owned fan-out/join phases;
- `examples/monitor`: a command gated by an externally published event;
- `examples/agent`: a bounded self-composing command loop.
- `examples/pipeline`: multiple queues, atomic worker events, an external
  transaction, an all-of join, generation-fenced keys, and dynamic work.

Run one against PostgreSQL:

```bash
FLOW_EXAMPLE_DATABASE_URL='postgres://postgres@localhost/postgres?sslmode=disable' \
  go run ./examples/direct
```

## Operations

### Caller-owned transactions

Create exactly one transaction client for each `pgx.Tx`, do all Flow writes
first, mark the application phase, and then touch application rows:

```go
tx, err := db.Conn.Begin(ctx)
if err != nil {
	return err
}
defer tx.Rollback(ctx)

flowTx := runtime.InTx(tx) // once for this transaction; do not use concurrently
if err := approved.Deliver(ctx, flowTx, runID, eventKey, value); err != nil {
	return err
}
if err := flowTx.BeginApplicationWrites(); err != nil {
	return err
}
if _, err := tx.Exec(ctx, applicationSQL); err != nil {
	return err
}
return tx.Commit(ctx)
```

`TransactionClient` does not commit or roll back the transaction and must not
outlive it. Repeating `runtime.InTx(tx)` creates an independent lock-order guard
and is invalid usage. After `BeginApplicationWrites`, every Flow write or
run-locking operation through that client fails before issuing SQL. Keep the
transaction short because locked run rows remain locked until caller commit.

For a live-key root, `Command.ReplaceCurrentRun` atomically cancels an exact
expected generation and creates its successor. If a retry finds a different,
declaration-equivalent current generation, it rediscovers that committed
successor; a different declaration conflicts. An exact expected generation is
always replaced, even when its declaration equals the requested successor.

- Claims match exact registered command name/version pairs. Unknown work remains durable until a compatible worker appears.
- Workers are at-least-once at the application boundary; settlement is fenced and durable progression commits once. External effects still need stable idempotency keys.
- Lease renewal is bounded and skip-locked: one busy settlement cannot block unrelated renewals. A locked row remains uncertain until settlement, a later renewal, or the conservative local-expiry watchdog resolves it.
- Deadline, wait-expiry, and lease-recovery maintenance drains full progressing pages promptly but remains sequential and bounded; locked/no-op pages fall back to polling.
- Required command failure enters reduced fail-fast by default. `flow.WithFailFast(false)` lets remaining work continue.
- Run deadlines, retries, queues, concurrency limits, graceful shutdown, polling, notification hints, observers, history, trace, cancellation, and caller-owned transactions are supported.
- Publishers may use a `Runtime` without calling `Run` or registering workers. Worker pools may be deployed independently.
- Observer delivery and shutdown drain are best-effort. Observers must return promptly and should honor context cancellation; observation loss never changes durable correctness.
- Flow has no pruning or archival API. Journal, payload, and terminal run data remain retained until an operator deliberately archives or removes them outside Flow's supported API.

For bounded domain-row decoration, `ListLiveWork` and `ListHistoryByKeys`
accept at most 200 exact run keys and return cursor pages of 100 rows by
default (maximum 1,000). Ordinary pages are not a cross-page snapshot; use a
Repeatable Read or Serializable caller transaction when one coherent snapshot
is required. The same rule applies to caller-owned `Trace`; Flow-owned `Trace`
uses Repeatable Read automatically.

## Tests

The Makefile uses a local `flow_test` database and sets
`FLOW_TEST_DATABASE_URL` explicitly, so PostgreSQL integration tests fail
instead of being skipped when the database is unavailable:

```bash
make db-reset
make test
```

`db-reset` recreates the database and applies Flow's embedded migrations to the
`public` schema. Individual integration tests continue to create and clean up
isolated schemas inside that database. `make test` always enables Go's race
detector.

The database connection can be customized with `PG_HOST`, `PG_PORT`, `PG_USER`,
`PG_DATABASE`, and `PGPASSWORD`, or by setting `FLOW_TEST_DATABASE_URL`
directly. `make test-with-reset` is available when a clean database and a test
run are both wanted.
