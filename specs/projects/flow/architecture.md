---
status: complete
---

# Architecture: flow

## Objective

Flow presents a small typed API over a PostgreSQL-backed durable execution engine. Commands and coordinators are the only execution drivers. Workers and coordinators record deterministic decisions; the store accepts them atomically; schedulers deliver eligible work across replicas.

## Package layout

```text
flow/
├── definitions.go          command, event, coordinator definitions
├── execute.go              execution starts, external events, cancellation
├── worker.go / node.go      worker decisions and staged command builder
├── coordinator.go           durable state handlers and terminal decisions
├── runtime*.go              registration, scheduling, leases, shutdown
├── inspection.go            execution queries
├── history.go / trace.go    journal and reconstructed diagnostics
├── flowtest/                database-free decision harness
└── internal/
    ├── canonical/           bounded canonical JSON
    ├── definition/          erased codecs and identities
    ├── replay/              pure journal reducer
    ├── retry/               retry policy and classification
    └── store/               migrations and PostgreSQL transitions
```

## Layer boundaries

### Typed API

Generics exist at definition, handler, event, and outcome boundaries. Registration erases these types only after validation and retains codecs for durable decoding.

### Decision engine

Worker and coordinator handlers receive private scope state. `Execute` and `Emit` only mutate that state. The recorder validates sizes, keys, owner scope, duplicate identities, modifier consistency, and coordinator terminality before any store call.

### Store

Store operations accept canonical values and normalized change sets. Each semantic operation owns its transaction shape, lock ordering, journal batch, projection changes, readiness resolution, and wake hints.

### Runtime

Schedulers discover only work for exact registered definitions, claim with bounded concurrency, execute handlers without holding database connections, and settle through fenced store calls. Maintenance handles wait expiry, execution deadlines, and expired leases.

## Durable transaction model

Every semantic write belongs to one execution and locks `flow_executions` first. Under that lock the store:

1. reads the relevant projection state;
2. validates the requested transition;
3. allocates consecutive journal positions;
4. writes canonical immutable entries;
5. mutates projections and readiness;
6. emits a transactional notification hint.

This produces a gap-free, commit-ordered journal per execution. Queue claims are deliberately no-wait and perform their semantic attempt-start transaction only after obtaining an eligible candidate.

Caller-owned transactions follow the same lock order. Flow never commits or rolls back the caller transaction.

## Command lifecycle

```text
created
  ├─ unresolved waits → pending
  └─ gates resolved  → ready/retry_wait → running

running → succeeded | failed | retry_wait
pending/ready/retry_wait → cancelled | expired
```

Initial delay controls `next_attempt_at`. Event waits control `unsatisfied_waits`. Both conditions must be clear before a command is claimable.

Claims install an attempt ID, lease token, owner, and expiry. Renewals and settlement require the same fenced identity. A reclaimed stale invocation can finish locally but cannot commit durable progress.

## Decision acceptance

Worker success acceptance can include:

- application `WithCommit` SQL;
- result and hash;
- ordered staged events;
- ordered child commands and event waits;
- parent attempt conclusion and terminal event;
- exact-wait resolution and command readiness;
- failure/completion progression.

Coordinator acceptance can include:

- new typed state and state hash;
- ordered staged events and commands;
- accepted inbox delivery position;
- retry counters or terminal selection;
- coordinator transition and terminal events;
- execution progression.

Duplicate command declarations are compared by canonical fingerprint. Same-decision repeats merge waits additively and reject singleton conflicts.

## Event delivery architecture

Application events are journal entries and the journal is their retention store. Exact command waits use `flow_command_event_waits` as a reverse index. On command creation, retained journal events are checked; on event ingress, unresolved wait rows are updated.

Coordinators scan retained journal positions monotonically. Selectors match application event name or command name/version terminal outcome. The durable inbox/delivery fields guarantee accepted-at-most-once state transition even though invocation can repeat.

## Failure architecture

Required failure with fail-fast enabled enters an execution-level `failing` phase. The transition cancels work without active attempts. It intentionally leaves running attempts fenced and settleable.

If a surviving attempt succeeds and stages children after failing began, those children are recorded for audit but immediately cancelled. The accepted result, commit hook, and events are preserved. This avoids revoking in-flight work while preventing new execution work from escaping fail-fast.

## Replay and inspection

The journal is the semantic history. `internal/replay` folds it without application callbacks. `Trace` performs a repeatable-read journal fold and overlays operational fields such as current leases and next-attempt times. Public result lookup is derived from that trace snapshot.

Projection conformance tests cover every semantic journal path. Queue/lease churn is operational and not represented as new semantic journal kinds beyond attempt boundaries.

## Concurrency and scaling

Command handlers, coordinator handlers, and queue lanes have independent process-local bounds. PostgreSQL is the cross-process authority; there is no required leader and no unbounded in-memory queue.

`LISTEN/NOTIFY` reduces latency but carries no correctness state. Polling discovers all eligible work after dropped notifications or listener reconnects. Unknown versions remain unclaimed for compatible future replicas.

## Security and data handling

Definition names, keys, canonical JSON size, metadata, queue names, schema identifiers, and cursor values are validated at boundaries. SQL identifiers come only from validated schema configuration. Durable bodies are hashed; secrets belong in application secret stores rather than command arguments or metadata.

## Deliberate omissions

There is no graph evaluator, dependency table, workflow reconciliation scheduler, automatic worker result lookup, or fact-query DSL. Dynamic coordination is ordinary typed coordinator code, making durable behavior visible to the application rather than implicit in a second orchestration engine.
