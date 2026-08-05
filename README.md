# flow

`flow` is a Go library for event-driven, durable, distributed work execution backed by PostgreSQL.

```text
command -> worker -> result + events + child commands
                         |
                         +-> exact event-gated commands
```

Commands are the only durable unit of orchestration. Workers perform typed work, emit immutable execution-scoped events, and stage bounded child commands. Exact event gates provide sequencing and joins. PostgreSQL stores the queue, leases, projections, and a gap-free journal for each execution.

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

handle, err := sendEmail.With(runtime).Execute(ctx, "email/order-42", emailArgs{
	To: "person@example.com",
})
```

`Execute` always creates or rediscovers durable asynchronous work; it never calls a worker inline. A stable non-empty execution key is permanently idempotent by default. `flow.WithLiveKey()` instead deduplicates only while an execution is non-terminal.

## Composing work

A successful worker may atomically emit events and stage child commands:

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

A root or child command may wait for exact application events:

```go
var approved = flow.DefineEvent[approval]("orders.approved")

flow.Execute(work, "fulfill/42", fulfill, args).
	WaitFor(approved, "approval/42").
	Within(30 * time.Minute).
	Delay(time.Second)
```

The waiting worker reads only the events declared as its inputs:

```go
value, err := flow.ReadEvent(work, approved, "approval/42")
```

Multiple waits are AND conditions. Matching is exact on event name and key within one execution. Events recorded before command declaration still satisfy the gate. `Within` starts at command creation and runs independently of `Delay`. At most 256 waits may be declared for one command; larger joins should use a tree of join commands or stable external references.

External systems publish with `Event.Emit`. Workers use `flow.Emit` so their events commit atomically with their accepted result and child declarations.

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

Database-free tests:

```bash
go test ./...
```

PostgreSQL integration tests:

```bash
FLOW_TEST_DATABASE_URL='postgres://postgres@localhost/postgres?sslmode=disable' go test ./...
```

Integration tests create isolated schemas and clean them up.
