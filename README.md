# flow

`flow` is a Go library for event-driven, durable, distributed work execution backed by PostgreSQL.

```text
command  →  worker  →  events
                     └→ optional child commands

events/outcomes  →  coordinator  →  commands + state
```

Commands instruct work. Workers do the work. Events record immutable facts. Coordinators handle joins, branching, loops, and other stateful orchestration. PostgreSQL stores the queue, leases, current projections, and a gap-free journal for each execution.

## Install

```bash
go get github.com/goware/flow
```

Flow uses the application's existing PostgreSQL database. Its seven tables use a `flow_` prefix and default to the `public` schema. `flow.WithSchema` selects another schema.

Run migrations explicitly during deployment:

```go
if err := flow.Migrate(ctx, db); err != nil {
	return err
}
```

`flow.New` validates the installed schema and starts no goroutines. Register handlers, then call `Runtime.Run` in the process that should execute work.

## A direct command

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

## Worker decisions

A worker may atomically stage application events and bounded child commands with its successful result:

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

Repeated declarations with the same key and canonical content coalesce. A conflicting declaration poisons the decision. A `WithCommit` callback can update application tables in the same fenced transaction as Flow settlement.

## Exact event gates

A direct root command can wait for exact execution-scoped application events:

```go
var approved = flow.DefineEvent[approval]("orders.approved")

handle, err := fulfill.With(runtime).Execute(ctx, "order/42", args,
	flow.WaitFor(approved, "approval/42"),
	flow.Within(30*time.Minute),
)
```

Commands staged by workers or coordinators use the returned node:

```go
flow.Execute(work, "fulfill/42", fulfill, args).
	WaitFor(approved, "approval/42").
	Within(30 * time.Minute).
	Delay(time.Second)
```

Multiple waits are AND conditions. Matching is exact on event name and key inside one execution. Events published before command declaration still satisfy the gate. `Within` starts when the command is created, runs independently of `Delay`, and expires the command if a wait remains unresolved.

External systems publish with `Event.Emit`. Workers and coordinators use `flow.Emit` so the event commits atomically with their accepted decision.

## Coordinators

Use a coordinator when the next command depends on outcomes or events, when work fans out dynamically, or when orchestration has loops or durable state:

```go
var report = flow.DefineCoordinator[reportState]("report.build", 1,
	flow.OnStart(startReport),
	flow.OnOutcome(analyzePart, partFinished),
	flow.OnEvent(reportRequested, requestReceived),
)
```

Handlers receive one retained input at a time, mutate typed state, and stage commands/events. Call `coordination.Succeed()` or `coordination.Fail(err)` explicitly. Stable command keys make repeated delivery safe.

Use workers for bounded local continuation and coordinators for dynamic joins and branching. Pass data needed by later commands in their arguments; workers do not read results from other commands.

## Examples

Each example contains its complete logic:

- `examples/direct`: one background command;
- `examples/fanout`: coordinator-owned dynamic fan-out and fan-in;
- `examples/monitor`: a direct command gated by an externally published event;
- `examples/agent`: a durable stateful agent loop.

Run one against PostgreSQL:

```bash
FLOW_EXAMPLE_DATABASE_URL='postgres://postgres@localhost/postgres?sslmode=disable' \
  go run ./examples/direct
```

## Operations

- Command and coordinator claims match exact registered name/version pairs. Unknown work remains durable until a compatible replica appears.
- Workers are at-least-once at the application boundary; settlement is fenced and commits durable progression once. External effects still need stable idempotency keys.
- Required command failure enters reduced fail-fast by default: pending work is cancelled, while already-running attempts may settle. `flow.WithFailFast(false)` lets remaining work continue.
- Execution deadlines, retries, queues, concurrency limits, graceful shutdown, polling, notification hints, observers, history, trace, cancellation, and caller-owned transactions are supported.
- Publishers may use a `Runtime` without calling `Run` or registering handlers. Worker and coordinator pools may be deployed separately.

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
