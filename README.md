# flow

`flow` is a Go library for event-driven, durable, distributed work execution backed by PostgreSQL.

```text
command  →  worker  →  event
                     └→ optional child commands
```

Commands instruct work. Workers do the work. Events record durable facts. Optional plans react declaratively. Coordinators handle open-ended, cyclic, or adaptive work. PostgreSQL stores the queue, leases, execution graph, current projections, and a gap-free journal for each execution.

## Install

```bash
go get github.com/goware/flow
```

Flow uses the application's existing PostgreSQL database. Its nine tables all have a `flow_` prefix and default to the `public` schema; `flow.WithSchema` selects another schema without changing the table names.

Run migrations explicitly during deployment:

```go
if err := flow.Migrate(ctx, db); err != nil {
    return err
}

rt, err := flow.New(db)
if err != nil {
    return err // includes missing or incompatible schema
}
```

`db` is a `*pgkit.DB` from [`github.com/goware/pgkit/v2`](https://github.com/goware/pgkit). `flow.New` validates compatibility and starts no goroutines. `flow.MigrationFS` exposes the same embedded migrations for an external runner.

## Smallest durable worker

```go
type ReceiptArgs struct {
    OrderID string `json:"order_id"`
    Email   string `json:"email"`
}

type ReceiptSent struct {
    ProviderMessageID string `json:"provider_message_id"`
}

var SendReceipt = flow.DefineCommand[ReceiptArgs, ReceiptSent]("send_receipt", 1)

func sendReceipt(ctx context.Context, work *flow.Work[ReceiptArgs]) (ReceiptSent, error) {
    id, err := provider.Send(ctx, work.Args.Email)
    if err != nil {
        return ReceiptSent{}, err
    }
    return ReceiptSent{ProviderMessageID: id}, nil
}

rt, err := flow.New(db)
if err != nil {
    return err
}
if err := rt.Register(flow.Handle(SendReceipt, sendReceipt)); err != nil {
    return err
}

runCtx, stop := context.WithCancel(context.Background())
defer stop()
go func() {
    if err := rt.Run(runCtx); err != nil {
        log.Printf("flow runtime stopped: %v", err)
    }
}()

handle, err := SendReceipt.With(rt).Execute(ctx, "receipt/"+orderID, ReceiptArgs{
    OrderID: orderID,
    Email:   email,
})
if err != nil {
    return err
}

execution, err := flow.AwaitExecution(ctx, rt, handle.ID)
```

`Execute` always enqueues; it never calls the worker inline. Any compatible replica may claim the command. A renewable lease and settlement fence prevent two attempts from both committing progression. Handlers remain at-least-once at the external-effect boundary, so externally visible effects still need stable idempotency keys.

## Choose only the orchestration you need

- Direct mode is a durable background command and its bounded spawned child tree. No plan is required.
- A plan is a pure Go function for dependencies, joins, waits, and fact-driven branching. The runtime re-evaluates it from immutable durable inputs.
- A coordinator is a durable serialized state machine for adaptive agents, open-ended membership, loops, and mixed command outcomes.
- `flow.Publish` lets a webhook, batch monitor, or another process record a fact into an execution. An awaited command consumes no worker, connection, or lease while waiting.
- `flow.WithCommit` declares a short database-local tail that commits atomically with command success. Its inputs are restricted to durable command arguments, result, and metadata.

Use `GetExecution` or `LookupExecution` for bounded summaries, `ListExecutions` for stable cursor pagination, `History` for incremental journal pages, and `Trace` for the current graph plus events, attempts, causation, waits, coordinator state, and operational delivery detail.

## Runnable examples

The repository contains four complete examples. Application work is stubbed with deterministic prints and short sleeps; Flow itself uses real PostgreSQL, migrations, queue rows, leases, and journal settlement.

```bash
export FLOW_EXAMPLE_DATABASE_URL='postgres://postgres:password@127.0.0.1/postgres?sslmode=disable'

go run ./examples/direct
go run ./examples/fanout
go run ./examples/monitor
go run ./examples/agent
```

- `direct` is ordinary durable background work.
- `fanout` is a plan-driven dynamic fan-out and join.
- `monitor` publishes an external fact that releases waiting work.
- `agent` is an adaptive coordinator that consumes mixed tool outcomes and schedules another turn.

Each scenario is also a real-PostgreSQL end-to-end test. The tests assert the public result, `Trace`, `History`, database rows, and replay-vs-live projection equality.

## Test

Unit tests and the `flowtest` package do not need PostgreSQL:

```bash
go test ./flowtest ./internal/...
```

The full suite creates isolated schemas in a real database:

```bash
export FLOW_TEST_DATABASE_URL='postgres://postgres@127.0.0.1/postgres?sslmode=disable'
# Or set FLOW_TEST_DATABASE_PASSWORD separately.
go test ./...
go test -race ./...
```

`flowtest.RunWorker`, `RunCommit`, `RunDirect`, `RunPlan`, `Simulate`, `AssertPlanDeterministic`, and `RunCoordinator` invoke the production deterministic recorders without PostgreSQL. SQL behavior, locking, leases, fencing, and commit-function SQL remain integration tests.

## Deployment roles

One binary may serve requests and call `Run`, but the roles may also be split:

- API and publisher processes construct a `Runtime` and use it as a lightweight `Client`; they do not call `Run`.
- Command-worker pools register only the command versions they can execute.
- Plan-reconciliation pools register exact plan versions. Command workers and publishers do not need plan code.
- Coordinator pools register coordinator definitions. Command settlement does not need coordinator code; terminal events wait durably for a compatible coordinator replica.

All roles point at one PostgreSQL database. Schedulers use bounded process-local capacity, `FOR UPDATE SKIP LOCKED` claims, poll-first correctness, and transactional notification hints. Notifications are enabled by default and use one dedicated session connection per running runtime; `flow.WithNotifications(false)` selects fully correct poll-only operation for transaction-pooling proxies or connection-constrained deployments. Adding replicas adds capacity; no leader, partition map, or sticky ownership protocol is required.

## Guarantees and boundaries

- Per-execution journal positions are gap-free and commit ordered.
- Command creation and exactly one terminal event per command reconstruct the settled execution graph.
- Queue delivery, retries, leases, and attempts are operational records; they are not fabricated as application events.
- Plans are pure and additive. Facts and terminal outcomes are immutable and durably re-readable.
- Command/worker progression is atomic in PostgreSQL, but arbitrary external APIs cannot share that transaction.
- Milestone 1 retains terminal executions and complete journals indefinitely. Operational retention/archival is a near-term follow-on.

## Design documentation

- [Project overview](specs/projects/flow/project_overview.md)
- [Functional specification](specs/projects/flow/functional_spec.md)
- [Architecture](specs/projects/flow/architecture.md)
- [PostgreSQL storage and journal](specs/projects/flow/components/schema.md)
- [Definitions and execution engine](specs/projects/flow/components/engine.md)
- [Distributed runtime and operations](specs/projects/flow/components/runtime.md)
- [Milestone 1 acceptance matrix](specs/projects/flow/acceptance_evidence.md)
- [Release benchmark and example evidence](specs/projects/flow/benchmark_evidence/phase_9_release.md)
