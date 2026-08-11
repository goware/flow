# Migrating Flow v0.2 applications to the Plans 9–10 API

This guide covers the combined breaking-development window implemented by
Plans 9 and 10. Plan 9 is an intermediate implementation checkpoint, not a
separate tagged release; the reviewed release after Plan 10 is the next public
baseline. There is no compatibility shim: update the Go API and apply migration
004 before starting the new runtime.

## 1. Apply migration 004 before deploying the runtime

Run `flow.Migrate` during deployment, before any new runtime starts. Migration
004 changes only live catalog vocabulary:

| v0.2 catalog | new catalog |
|---|---|
| `flow_executions` | `flow_runs` |
| `execution_id` ownership columns | `run_id` |
| `execution_key` | `run_key` |
| Flow constraints/indexes containing `execution` | corresponding `run` names |

All six tables, rows, foreign keys, predicates, index definitions, journal
bodies, hashes, and positions are preserved. Migrations 001–003 and their
checksums remain immutable. The schema reader/writer compatibility version is
now 2/2, so a v0.2 runtime rejects schema 4 rather than querying renamed
objects.

Application SQL should not mutate Flow tables. If diagnostics intentionally
read them, rename those references in the same deployment. Versioned journal
entry kinds and encoded body tags intentionally retain their historical
`execution_*` strings; do not rewrite retained history.

## 2. Rename the public aggregate from Execution to Run

The complete public family changes coherently:

| v0.2 | new API |
|---|---|
| `Execution`, `ExecutionID`, `ExecutionStatus` | `Run`, `RunID`, `RunStatus` |
| `ExecutionFilter`, `ExecutionPage`, `ExecutionTrace` | `RunFilter`, `RunPage`, `RunTrace` |
| `GetExecution`, `AwaitExecution`, `ListExecutions` | `GetRun`, `AwaitRun`, `ListRuns` |
| `CancelExecution` | `CancelRun` |
| `LookupLiveExecution` | `GetCurrentRun` |
| `WithExecutionDeadline`, `WithoutExecutionDeadline` | `WithRunDeadline`, `WithoutRunDeadline` |
| `WithMaxCommandsPerExecution` | `WithMaxCommandsPerRun` |
| exported `ExecutionID` fields | corresponding `RunID` fields |

`GetCurrentRun(ctx, client, commandName, runKey)` returns `(Run, found,
error)` and only resolves the current `running`/`failing` live-key generation.
It does not return historical terminal generations.

## 3. Use direct Enqueue and the two event paths

Command definitions no longer retain a hidden client:

```go
// v0.2
run, err := SendEmail.With(runtime).Execute(ctx, key, args)

// new
run, err := SendEmail.Enqueue(ctx, runtime, key, args)
```

Inside a worker, rename `flow.Execute(work, ...)` to
`flow.Enqueue(work, ...)`. Root and child enqueue remain asynchronous durable
commands; no worker is invoked inline.

The event model has two forms:

```go
// Attempt-atomic fact in the current run.
err := flow.Emit(work, Fact, eventKey, payload)

// Immediate detached fact targeted to an exact run.
err := Fact.Deliver(ctx, client, runID, eventKey, payload)
```

The old method `Event.Emit` is removed. `Event.Deliver` may be called from
application code inside a worker, but its commit is detached from that worker
attempt unless an explicit caller transaction governs it. Same-run facts that
must roll back with worker failure still use top-level `flow.Emit`.

When a publisher knows only a domain key, compose `GetCurrentRun` and
`Deliver` explicitly. The run can settle between those calls; handle
`ErrTerminal`. Define event names as stable fact kinds and carry entity plus
generation identity in one deterministic key helper shared by `WaitFor`,
`Deliver`, and `GetEventValue`.

## 4. Handle event-input presence explicitly

`GetEventValue` is now the single presence-aware snapshot read:

```go
value, found, err := flow.GetEventValue(work, Approved, key)
if err != nil {
	return Result{}, err
}
if !found {
	// Optional first pass, or reject absence when this input is required.
}
```

It performs no SQL and never waits. `found=false` is ordinary absence and does
not poison the decision. Invalid definitions/keys and corrupt typed snapshots
remain errors.

## 5. Remove application-side millisecond ceiling helpers

Positive public durable durations are rounded upward once to a whole
millisecond before fingerprinting and persistence. This applies to run
deadlines, attempt timeouts, root/child delays, wait budgets, retry elapsed and
backoff values, and `RetryAfter`. Zero/negative validation remains specific to
the option. Internal store and decode boundaries still require exact
milliseconds.

## 6. Create one transaction client per pgx.Tx

`Runtime.InTx` now returns a named `*flow.TransactionClient`. Create it once at
the transaction boundary, thread it through helpers, perform Flow operations
first, then mark the application phase:

```go
tx, err := db.Conn.Begin(ctx)
if err != nil {
	return err
}
defer tx.Rollback(ctx)

flowTx := runtime.InTx(tx) // exactly once for this pgx.Tx

if err := Approved.Deliver(ctx, flowTx, runID, eventKey, value); err != nil {
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

The client is not concurrent, does not own the transaction, and must not
outlive it. Calling `InTx(tx)` repeatedly for the same transaction creates
independent ordering guards and is invalid usage. After
`BeginApplicationWrites`, all Flow writes and run-locking operations through
that client fail before SQL. Reads that take no semantic lock remain reads.

Pre-existing runs must be locked in ascending `RunID` order. A run inserted by
the current transaction is transaction-owned and creates no cross-transaction
ordering edge; conflict rediscovery still locks the pre-existing holder through
the ordinary ordered path.

## 7. Replace split cancel/start with ReplaceCurrentRun

For a live-key root, do not commit cancellation and enqueue a successor in two
transactions:

```go
replacement, err := IntentRun.ReplaceCurrentRun(
	ctx,
	flowTx,
	expectedRunID,
	intentID,
	args,
	"operator retry",
	flow.WithLiveKey(),
)
```

The expected-ID comparison is authoritative:

- current ID equals `expected`: cancel it and create a distinct successor in
  one transaction, even when the declaration is identical;
- current ID differs and the declaration is equivalent: return the committed
  current successor with `Replaced=false` (retry/ambiguous-commit recovery);
- current ID differs and the declaration is not equivalent: `ErrConflict`;
- no current holder: `ErrConflict`; use `Enqueue` for create-if-absent.

`ReplaceRunResult.Replaced` is true only for the caller that created the
successor. Caller rollback restores the predecessor and removes the successor.
The operation reuses ordinary cancellation/start journal and projection
shapes; it adds no table, column, or journal kind.

## 8. Consumer checklist

Before deploying:

1. update all embedded fields, filters, traces, observers, tests, and
   diagnostics to Run vocabulary;
2. replace every bound/direct `Execute` and worker `Execute` call;
3. retain `Event.Deliver` only for genuinely detached targeted ingress;
4. update every `GetEventValue` caller to inspect `found`;
5. delete local duration-ceiling helpers;
6. create and thread one transaction client per caller transaction, and mark
   the application-write phase;
7. replace cancel/commit/enqueue recovery windows with
   `ReplaceCurrentRun` where an exact expected live generation is known;
8. use `CommandInfo.RunKey` instead of querying the run merely to recover its
   root entity key;
9. apply migration 004, then confirm `CheckSchema` reports schema 4 and reader/
   writer 2/2; and
10. run ordinary and race suites against every supported PostgreSQL major with
    durability settings enabled.

See `examples/pipeline` for direct enqueue, multiple queues, deterministic
generation-fenced event keys, presence-aware snapshots, a caller-owned
delivery transaction, and a dynamic successor without a workflow DSL.
