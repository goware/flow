# flow

`flow` is a Go library for event-driven, durable, distributed work execution backed by PostgreSQL.

```text
command -> worker -> result + events
                         |
                         +-> optional sub-commands
```

Commands are the only durable unit of orchestration. Workers perform typed work, emit immutable execution-scoped events, and stage bounded sub-commands. Exact event gates provide sequencing and joins. PostgreSQL stores the queue, leases, projections, and a gap-free journal for each execution.

## Install

```bash
go get github.com/goware/flow
```

Flow uses the application's existing PostgreSQL database. Its six tables use a `flow_` prefix and default to the `public` schema. `flow.WithSchema` selects another schema.

The v0.1 release line supports Go 1.26 and PostgreSQL 17 and 18. During v0.x,
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

exec, err := sendEmail.With(runtime).Execute(ctx, "email/order-42", emailArgs{
	To: "person@example.com",
})
```

`Execute` always creates or rediscovers durable asynchronous work; it never calls a worker inline. A stable non-empty execution key is permanently idempotent by default. `flow.WithLiveKey()` instead deduplicates only while an execution is non-terminal.

`Execute` returns the `Execution` snapshot as of durable acceptance; `Created`
reports whether this call created it. `GetExecution` and `AwaitExecution`
return the same type with the execution's current or final state.

## Composing work

A successful worker may atomically emit events and stage sub-commands:

```go
var charged = flow.DefineEvent[chargeResult]("billing.charged")

func chargeWorker(ctx context.Context, work *flow.Work[chargeArgs]) (chargeResult, error) {
	result := chargeResult{Receipt: "receipt-42"}
	if err := flow.Emit(work, charged, "charge/42", result); err != nil {
		return chargeResult{}, err
	}
	flow.Execute(work, "notify/42", notifyCustomer, notifyArgs{Receipt: result.Receipt})
	return result, nil
}
```

Repeated declarations with the same key and canonical content coalesce. Conflicting declarations poison the complete decision. A `WithCommit` callback can update application tables in the same fenced transaction as Flow settlement.

Choose command boundaries around independent retry, side effects, isolation,
timeouts, queue ownership, or useful parallelism. Keep small deterministic
transformations in the worker that owns them instead of turning every business
logic microstep into durable work. Several small writes to the same PostgreSQL
database can usually share one `WithCommit` callback. Keep that callback short
and database-only: it holds the execution lock until settlement commits and is
not an exactly-once boundary for remote calls.

One execution is one serialized semantic aggregate. Keep causally related work
together, but use separate executions for independent bulk items or shards
instead of treating one execution as a tenant-wide work container. The default
1,000-command ceiling is a safety limit, not a recommended execution size;
ordinary executions are usually clearer in the tens or low hundreds. For a very
large fan-out, have bounded batch commands declare later batches, and combine
large input sets through hierarchical join commands rather than one enormous
child declaration or join.

## Exact event gates and inputs

A root or sub-command may wait for exact application events:

```go
var approved = flow.DefineEvent[approval]("orders.approved")

flow.Execute(work, "fulfill/42", fulfill, args).
	WaitFor(approved, "approval/42").
	Within(30 * time.Minute).
	Delay(time.Second)
```

The waiting worker gets the value attached to a declared event:

```go
value, err := flow.GetEventValue(work, approved, "approval/42")
```

Multiple waits are AND conditions. Matching is exact on event name and key within one execution. Events recorded before command declaration still satisfy the gate. `Within` starts at command creation and runs independently of `Delay`. At most 256 waits may be declared for one command; larger joins should use a tree of join commands or stable external references.

Pass data computed by a parent directly in child arguments. Use exact events
for sibling, cross-branch, or external facts, and stage related events and
children in the same worker decision when they belong to one atomic change.
Large or sensitive documents should remain in application storage; pass stable
references through command arguments or event payloads.

Flow has three event paths:

| API | Use |
| --- | --- |
| `flow.Emit(work, ...)` | stage an event in the current execution with the worker decision |
| `event.Emit(ctx, client, id, ...)` | record an external event in a known execution |
| `event.Deliver(ctx, client, id, ...)` | deliberately record a detached event in another known execution, including from an active worker |

`Deliver` needs only the target execution ID. With `runtime.InTx(tx)`, it commits or rolls back with the caller's application writes; with a regular runtime client it commits independently. Keep caller-owned transactions short: an execution lock remains held until the caller commits or rolls back. A committed delivery survives source failure and retry, so producers should use stable keys and deterministic payloads. Same-execution worker events should use staged `flow.Emit`: explicitly delivering to the current execution is detached and may survive a failed attempt. Delivery is targeted ingress, not publish/subscribe, and target workers remain at-least-once.

Fan-out, fan-in, multi-stage joins, branches, and bounded loops are ordinary command composition. Flow intentionally has no separate coordinator/state-machine API, outcome subscriptions, OR/quorum/race gates, or automatic result dataflow.

## Examples

Each example contains its complete, self-documenting logic:

- `examples/direct`: one background command;
- `examples/fanout`: two command-owned fan-out/join phases;
- `examples/monitor`: a command gated by an externally published event;
- `examples/agent`: a bounded self-composing command loop.

Run one against PostgreSQL:

```bash
FLOW_EXAMPLE_DATABASE_URL='postgres://postgres@localhost/postgres?sslmode=disable' \
  go run ./examples/direct
```

## Operations

- Claims match exact registered command name/version pairs. Unknown work remains durable until a compatible worker appears.
- Workers are at-least-once at the application boundary; settlement is fenced and durable progression commits once. External effects still need stable idempotency keys.
- Lease renewal is bounded and skip-locked: one busy settlement cannot block unrelated renewals. A locked row remains uncertain until settlement, a later renewal, or the conservative local-expiry watchdog resolves it.
- Deadline, wait-expiry, and lease-recovery maintenance drains full progressing pages promptly but remains sequential and bounded; locked/no-op pages fall back to polling.
- Required command failure enters reduced fail-fast by default. `flow.WithFailFast(false)` lets remaining work continue.
- Execution deadlines, retries, queues, concurrency limits, graceful shutdown, polling, notification hints, observers, history, trace, cancellation, and caller-owned transactions are supported.
- Publishers may use a `Runtime` without calling `Run` or registering workers. Worker pools may be deployed independently.
- Observer delivery and shutdown drain are best-effort. Observers must return promptly and should honor context cancellation; observation loss never changes durable correctness.
- Flow has no pruning or archival API. Journal, payload, and terminal execution data remain retained until an operator deliberately archives or removes them outside Flow's supported API.

For bounded domain-row decoration, `ListLiveWork` and `ListHistoryByKeys`
accept at most 200 exact execution keys and return cursor pages of 100 rows by
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
