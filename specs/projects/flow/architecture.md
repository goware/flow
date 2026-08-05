---
status: complete
completed_at: 2026-08-04
---

# Architecture: flow

## Objective

Flow is a typed API over a PostgreSQL-backed durable command engine. Commands are the only execution drivers. Workers record deterministic decisions; the store accepts them atomically; one scheduler delivers eligible work across replicas.

## Package layout

```text
flow/
├── definitions.go          command and event definitions
├── execute.go              execution starts, external events, cancellation
├── worker.go / node.go     worker decisions, ReadEvent, child builder
├── runtime*.go             registration, command scheduling, shutdown
├── inspection.go           execution queries
├── history.go / trace.go   journal and reconstructed diagnostics
├── flowtest/               database-free worker harness
└── internal/
    ├── canonical/          bounded canonical JSON
    ├── definition/         erased codecs and identities
    ├── replay/             pure journal reducer
    ├── retry/              retry policy and classification
    └── store/              migrations and PostgreSQL transitions
```

The typed layer validates command/event definitions and erases worker types behind codecs. The decision engine records events and child commands without SQL. The store owns lock ordering, journal batches, projections, readiness, and fences. The runtime claims registered command versions and invokes workers without holding a connection.

## Transaction and command model

Every semantic write belongs to one execution and locks `flow_executions` first. The transaction validates state, allocates consecutive journal positions, appends canonical entries, updates projections/readiness, and optionally sends a notification hint.

```text
created
  ├─ unresolved waits -> pending
  └─ gates resolved  -> ready/retry_wait -> running

running -> succeeded | failed | retry_wait
pending/ready/retry_wait -> cancelled | expired
```

Initial delay and exact event waits are independent prerequisites. Claims install attempt/lease fences. A reclaimed stale invocation may finish locally but cannot settle.

Worker success may atomically include application commit SQL, a result, events, child commands and waits, parent terminal facts, readiness changes, and execution progression. Duplicate declarations compare canonical fingerprints; same-decision repeats may add distinct waits but may not change singleton declaration fields.

## Event-input architecture

Application events live in the journal. `flow_command_event_waits` is the reverse readiness index and records each satisfying journal position. Command claim loads all declared wait rows plus the referenced application-event bodies in one bounded query. The connection is released before invocation; `ReadEvent` decodes from an immutable in-memory selector map.

This mechanism supports sibling and cross-branch joins without a second state machine or scheduler. Commands form ownership/provenance trees; events provide synchronization.

## Failure, replay, and scaling

Required failure can enter execution `failing`, cancelling work without live attempts. Running attempts retain their fences; accepted events/results remain durable, while newly staged children are recorded cancelled.

The journal is semantic history. Replay folds it without callbacks. Trace adds current operational command data and wait satisfaction positions. Queue and lease churn remain projection-only except for attempt boundaries.

Worker and named-queue concurrency are process-local bounds. PostgreSQL is the cross-process authority. `LISTEN/NOTIFY` reduces latency, while polling guarantees discovery after lost notifications or reconnects. Unknown command versions remain durable for compatible replicas.

## Deliberate omissions

There is no coordinator/state-machine object, graph evaluator, outcome subscription, workflow reconciliation loop, arbitrary result lookup inside workers, OR/quorum/race gate, or event-triggered callback. Events only release commands declared in advance.
