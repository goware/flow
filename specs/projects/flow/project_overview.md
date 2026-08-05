---
status: complete
completed_at: 2026-08-04
---

# flow

## Summary

`flow` is a typed Go library for durable, distributed work execution on PostgreSQL. Its complete orchestration model is:

```text
commands + workers + execution-scoped events
```

- A command is a durable typed instruction.
- A worker performs one command, returns a typed result, and may atomically emit events or stage bounded sub-commands.
- A command may wait for exact application events and read only those declared inputs.
- The command tree owns lifecycle and provenance; events synchronize work across branches.

There is one execution kind and one scheduler. Flow has no declarative workflow graph, coordinator/state-machine API, command outcome subscription, or automatic result dataflow.

## Composition

Every execution starts with one root command. Workers may stage sequences, dynamic fan-outs, gated joins, repeated fan-out/join phases, and bounded command loops. Stable execution-local command keys make retries idempotent. Multiple event waits are exact AND gates; `Within` gives externally gated work a deliberate lifetime.

Workers consume sibling or external data with `ReadEvent`, which is limited to gates declared on the current command. Parent-computed data is passed directly in sub-command arguments. Larger data belongs behind stable application references.

Flow deliberately does not implement first-of-N races, quorum gates, reactions to unsuccessful command outcomes, or open-ended mutable workflow state.

## Durability

Every semantic mutation locks the execution row, allocates gap-free journal positions, appends immutable journal entries, and updates projections in one PostgreSQL transaction. Command invocation is at-least-once; lease fencing guarantees that only a valid settlement commits durable progress.

The six owned tables are executions, commands, command queue, command event waits, journal, and schema migrations. Application events are immutable journal facts identified by `(execution ID, event name, event key)`. External code uses `Event.Emit`; application code that deliberately targets another known execution, including from an active worker, uses detached `Event.Deliver`. A transaction-bound client makes that ingress atomic with caller-owned application writes.

## Runtime and operations

`New` validates configuration and starts nothing. `Run` owns a bounded command scheduler, lease renewal, wait/deadline/recovery maintenance, optional notification hints, and graceful shutdown. Polling is always the correctness path.

Worker pools register exact command name/version handlers. API-only publishers may use a runtime without registering workers or calling `Run`.

`GetExecution`, `LookupLiveExecution`, `ListExecutions`, `AwaitExecution`, `History`, and `Trace` expose durable state without application callbacks. Trace includes command provenance, exact waits and satisfying positions, attempts, events, and ordered history. `ResultOf` decodes a successful command result from a trace snapshot.

`flowtest` exercises production worker decision recording without PostgreSQL. PostgreSQL integration tests cover migrations, claims, fencing, retries, exact event inputs, fail-fast, replay conformance, multi-replica behavior, and all examples.
