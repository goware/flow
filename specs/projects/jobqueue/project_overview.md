---
status: complete
---

# jobqueue

`jobqueue` is a reusable Go library for building durable asynchronous jobs and event-driven workflows on PostgreSQL. It is intended for Go applications that already rely on PostgreSQL and need reliable background execution, retries, and multi-step state transitions without operating a separate message broker or workflow platform.

PostgreSQL is the source of truth. A low-level, SQS-inspired `MessageQueue` provides durable, at-least-once delivery using expiring leases and fencing tokens. A higher-level `JobQueue` manages job lifecycle, attempts, retries, scheduling, cancellation, outcomes, expiration, and worker execution. A later workflow milestone will compose jobs into deterministic directed acyclic graphs (DAGs), including chains, fan-out, fan-in and joins, conditional branches, dependencies, and dynamically created work.

Applications can use the system as an asynchronous state machine: work is enqueued, workers process it, and successful work can create subsequent jobs or publish events that advance the application to its next state. Callers observe job or workflow outcomes asynchronously; the library will not introduce a separate synchronous execution path.

When the queue and application state share PostgreSQL, successful work can atomically persist application state, finalize the job, create subsequent work, and record emitted events. Execution remains at-least-once: handlers must tolerate retries, and effects against external systems require explicit idempotency and reconciliation.

Jobs, events, and deliveries are distinct concepts. Jobs represent executable work; events represent immutable facts; deliveries represent leased processing attempts. Retrying a job, redriving an event delivery, and replaying an event stream are separate operations. The project will provide durable event fan-out and append-only event storage and replay as separate, composable `EventBus` and `EventStore` primitives after the core queue, job system, and workflow graph are established. All of these primitives will use PostgreSQL as their initial backend while retaining distinct contracts and semantics.

## Initial milestones

1. Build the low-level `MessageQueue`, worker runtime, and higher-level `JobQueue`.
2. Add deterministic workflow DAGs, dependencies, dynamic spawning, fan-out, and joins as the next milestone.
3. Build PostgreSQL-backed `EventBus` and `EventStore` primitives after the workflow core is proven.

## Technical requirements

- Go library published as `github.com/goware/jobqueue`
- PostgreSQL as the backing store, using `github.com/goware/pgkit/v2` over `github.com/jackc/pgx/v5`
- Backend-neutral contracts in the root package, with PostgreSQL as the initially supported backend
- Durable, at-least-once, lease-based delivery inspired by Amazon SQS
- Explicit lease fencing so stale workers cannot settle work or commit outcomes
- A real terminal `expired` job state driven by an explicit job deadline
- Reusable across applications; the queue and application state can share one database and transaction
- Fully asynchronous execution, with job and workflow outcomes observed asynchronously

## Initial non-goals

- Globally exactly-once effects against external systems
- Strict global FIFO processing
- Kafka-style infinite stream retention
- Transparent replay of arbitrary nondeterministic Go code
- Replacing a general-purpose durable execution platform such as Temporal
