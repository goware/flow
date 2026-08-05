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

Flow has three event paths:

| API | Use |
| --- | --- |
| `flow.Emit(work, ...)` | stage an event in the current execution with the worker decision |
| `event.Emit(ctx, client, id, ...)` | record an external event in a known execution |
| `event.Deliver(ctx, client, id, ...)` | deliberately record a detached event in another known execution, including from an active worker |

`Deliver` needs only the target execution ID. With `runtime.InTx(tx)`, it commits or rolls back with the caller's application writes; with a regular runtime client it commits independently. A committed delivery survives source failure and retry, so producers should use stable keys and deterministic payloads. Same-execution worker events should use staged `flow.Emit`: explicitly delivering to the current execution is detached and may survive a failed attempt. Delivery is targeted ingress, not publish/subscribe, and target workers remain at-least-once.

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
- Required command failure enters reduced fail-fast by default. `flow.WithFailFast(false)` lets remaining work continue.
- Execution deadlines, retries, queues, concurrency limits, graceful shutdown, polling, notification hints, observers, history, trace, cancellation, and caller-owned transactions are supported.
- Publishers may use a `Runtime` without calling `Run` or registering workers. Worker pools may be deployed independently.

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
