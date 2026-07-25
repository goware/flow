---
status: complete
---

# Functional Spec: jobqueue

## 1. Purpose

`jobqueue` is a reusable Go library for durable asynchronous work, event-driven applications, and deterministic workflows backed by PostgreSQL.

It serves applications that already use PostgreSQL and want reliable background execution without operating a separate queue or workflow service. It exposes a small SQS-inspired message queue, a higher-level job system, a worker runtime, workflow DAGs, durable event fan-out, and append-only event streams as distinct but composable capabilities.

The system is asynchronous. Enqueueing work returns durable identity and metadata, not the handler's eventual result. Callers inspect or observe jobs, workflows, and events separately.

## 2. Delivery Milestones

### 2.1 Milestone 1: Queue and jobs

The first stable milestone includes:

- a low-level `MessageQueue` abstraction;
- a PostgreSQL implementation;
- queue administration and inspection;
- durable publish, receive, lease, acknowledge, release, and lease-extension operations;
- batch operations needed by the worker runtime;
- dead-letter handling and redrive;
- a capacity-aware worker runtime with automatic lease renewal;
- a higher-level `JobQueue` with job and attempt history;
- atomic batch job enqueue;
- scheduling, retries, cancellation, expiration, results, and administrative retry;
- typed Go helpers over JSON job arguments;
- atomic job completion and deterministic child-job creation;
- an optional PostgreSQL finalizer for atomic application-state changes.

### 2.2 Milestone 2: Workflow DAGs

The next milestone adds:

- deterministic directed acyclic workflow graphs;
- static graph construction;
- dynamic node creation;
- dependencies, fan-out, fan-in, and joins;
- simple conditional transitions;
- workflow cancellation and terminal-state propagation;
- workflow history, graph inspection, and timeline queries;
- recorded workflow and handler definition versions.

### 2.3 Milestone 3: Events

After the workflow core is proven, the project adds PostgreSQL-backed:

- `EventBus`, for durable topic/subscription fan-out;
- `EventStore`, for ordered append-only streams and replay;
- atomic job completion plus event append and publication;
- projection-building helpers.

These are project deliverables, not external dependencies. They remain separate APIs because queue delivery, fan-out, and event sourcing have different semantics.

### 2.4 Later capabilities

The following may be added after the first three milestones are stable:

- concurrency keys and per-key limits;
- recurring jobs;
- rate limits and tenant quotas;
- event-store snapshots;
- archival and export;
- OpenTelemetry adapters;
- administrative HTTP APIs or a UI;
- alternative backends.

They are not requirements for the initial milestones.

## 3. Terminology

- **Message:** Opaque bytes published to a named queue.
- **Delivery:** A message returned to a consumer with a temporary lease.
- **Receipt:** The current message and lease identity required to settle or extend a delivery.
- **Lease:** A period during which a received message is normally hidden from competing consumers.
- **Acknowledge:** Confirm successful processing and remove the active low-level message.
- **Release (API verb: `Nack`):** Make a leased delivery eligible again, immediately or after a delay.
- **Job:** Durable executable work with arguments, lifecycle state, attempts, and an optional result.
- **Dispatch record:** Internal, reconstructable wake-up state connecting a non-terminal job to an eligible worker lane. It is not a raw application message.
- **Attempt:** One leased execution of a job.
- **Handler:** Application code registered for a job kind.
- **Event:** An immutable fact that has already happened.
- **Topic:** An event publication namespace.
- **Subscription:** A durable, independently retried consumer of topic events.
- **Stream:** An ordered sequence of immutable events for one domain identity.
- **Workflow run:** One durable execution of a workflow graph.
- **Node:** A job participating in a workflow run.
- **Spawn key:** A caller-supplied stable key identifying child work created by a job.
- **Fencing token:** A unique lease or attempt identity that prevents stale workers from settling work or committing outcomes.
- **Redrive:** Move failed/dead work back to an eligible queue for another processing cycle.
- **Replay:** Read an event stream again, normally to rebuild a projection. Replay is not synonymous with retrying a job or redriving a delivery.

## 4. System-Wide Guarantees

### 4.1 PostgreSQL is authoritative

All durable state is recoverable by querying PostgreSQL. Notifications may reduce latency but are never required for correctness.

After a process crash, missed notification, or listener reconnect, polling PostgreSQL must be sufficient to resume eligible work.

### 4.2 At-least-once execution

The default guarantee is at-least-once delivery and execution. A message or job can be received or executed more than once, including after an apparently successful handler run.

The library must not describe ordinary delivery as globally exactly-once. Documentation and examples must teach users to make handlers idempotent.

### 4.3 Lease fencing

Every receive creates a new receipt containing a fresh lease identity. Acknowledgement, release, lease extension, job finalization, and failure recording require the current receipt or equivalent attempt fence.

A stale owner must be unable to:

- acknowledge or release current work;
- extend an expired or replaced lease;
- commit a job result;
- create child jobs or workflow transitions;
- append events as part of stale job completion.

Stale settlement returns a recognizable lease-lost error rather than silently succeeding.

### 4.4 Atomic same-database outcomes

When application data and `jobqueue` use the same PostgreSQL database, a short completion transaction can atomically:

- verify current ownership;
- update application state through an optional finalizer;
- persist the job result;
- create deterministic child jobs;
- resolve workflow dependencies;
- append or publish events when those primitives are available;
- mark the attempt and job successful;
- acknowledge the low-level delivery.

If any operation fails, none of these completion effects commit.

### 4.5 External side effects

The library cannot atomically commit with external HTTP APIs, email providers, blockchains, object stores, or other databases. Handlers performing such effects must use downstream idempotency keys, durable operation records, reconciliation, or equivalent safeguards.

### 4.6 Asynchronous results

No enqueue or publish method waits for handler execution. A caller receives durable identity and can query or observe the eventual state and result asynchronously.

## 5. Public Capability Model

### 5.1 Root package

The root `github.com/goware/jobqueue` package defines backend-neutral contracts, identifiers, request/response types, errors, worker-facing abstractions, and typed helpers.

Capabilities are split into focused interfaces rather than one mandatory all-purpose interface. Applications can depend only on publication, reception, settlement, job enqueueing, job reading, job control, event publication, or event reading as needed.

### 5.2 PostgreSQL package

`github.com/goware/jobqueue/postgres` provides the initially supported implementation, migrations, PostgreSQL-specific configuration, transaction binding, listener integration, maintenance, and the PostgreSQL finalizer capability.

Backend-neutral packages do not expose `pgx` or `pgkit` transaction types.

### 5.3 API stability

Before v1.0, public APIs and migrations may evolve with documented upgrade instructions. After v1.0, compatibility follows semantic versioning, and supported rolling upgrades must preserve readability of durable message envelopes across adjacent versions.

### 5.4 Delivery ownership and namespaces

Raw messages, job dispatch records, and event-subscription deliveries occupy separate logical namespaces. This is an internal ownership boundary, not a user-facing system of different queue types.

- `MessageQueue` addresses only application-created raw message queues.
- A `JobQueue` queue name is a job worker lane owned by the job system.
- An event subscription owns its delivery lane through `EventBus`.
- The same human-readable name can exist in different namespaces without collision.
- Raw receive, purge, delete, or redrive operations cannot address managed job or subscription deliveries.
- Job and subscription administration is performed through their corresponding higher-level APIs.
- Internal job dispatch records are governed by job lifecycle policy, not by raw-message retention or maximum-delivery policy.

The PostgreSQL implementation may store these delivery forms in one physical table or several tables. That is an architecture decision; the functional requirement is that one public capability cannot accidentally steal or corrupt another capability's managed deliveries.

## 6. MessageQueue

### 6.1 Queue administration

Applications can explicitly create, inspect, update, purge, and delete raw message queues. Managed job lanes and subscription deliveries use their corresponding higher-level administration APIs.

Required behavior:

- publishing to a missing queue returns `ErrNotFound` with resource `queue`;
- explicit creation is the production default;
- optional automatic creation may be enabled for development and tests;
- creating an existing queue is idempotent when every explicitly supplied persisted setting equals the normalized stored value;
- any explicitly supplied persisted setting that differs from the stored value returns `ErrConflict`;
- purging is a distinct destructive operation;
- ordinary deletion rejects a non-empty or in-use queue unless a separately explicit force operation is added later;
- queue configuration changes do not retroactively rewrite messages already published with snapshotted limits;
- queue statistics are advisory snapshots rather than transactionally exact counters.

Each queue supports configuration for:

- default visibility timeout;
- maximum deliveries;
- message retention;
- optional dead-letter destination;
- maximum payload and header sizes;
- optional development-only automatic creation.

Queue names are non-empty, at most 128 bytes, and restricted to ASCII letters, digits, `.`, `_`, and `-`. A name must begin with a letter or digit.

### 6.2 Publish

A publisher provides:

- queue name;
- opaque message body;
- optional headers;
- optional caller-supplied message ID;
- optional initial delay or absolute availability time;
- optional priority;
- optional deduplication key;
- optional per-message maximum-delivery override.

Behavior:

- omitted identity is generated by the backend;
- caller-supplied identity enables idempotent application workflows but duplicate IDs return a recognizable conflict unless the existing message is an exact idempotent match;
- delay and absolute availability time are mutually exclusive;
- a scheduled message is durable immediately but cannot be received before it becomes available;
- delays are not limited to Amazon SQS's 15-minute timer limit;
- priority affects claim preference but does not guarantee strict completion order;
- the low-level body remains opaque bytes;
- the initial default body limit is 1 MiB;
- headers have an initial encoded-size limit of 64 KiB;
- messages above configured limits are rejected before persistence;
- successful publication returns the durable message metadata.

Deduplication keys apply within one queue. Repeating a live key returns the existing message or an explicit duplicate result; it must not create another active message. Deduplication retention beyond the active message lifetime is not promised initially.

### 6.3 Receive and lease

A receiver specifies:

- queue name;
- maximum batch size;
- optional visibility timeout override;
- optional wait duration.

Behavior:

- receive returns zero or more eligible deliveries, never more than requested;
- the backend may return fewer messages than requested;
- a delivery includes the message, receipt, receive count, lease start, and lease expiration;
- receiving increments the delivery count and assigns a fresh lease identity;
- an unacknowledged message normally becomes eligible again after lease expiry;
- multiple processes can receive concurrently without intentionally issuing the same current lease to two consumers;
- standard queues provide best-effort claim order, not FIFO completion;
- short polling is available with zero wait;
- bounded long polling waits for eligible work or the caller's context;
- caller cancellation stops waiting without consuming a message that was not returned;
- expired-retention messages are never returned as valid deliveries.

The PostgreSQL backend documents and enforces a configurable safety maximum for one receive call. Worker runtimes use smaller batches whenever immediate execution capacity is lower.

### 6.4 Acknowledge

Acknowledgement requires the current receipt.

On success:

- the active message is deleted;
- the same receipt cannot settle the message again;
- no low-level success history is retained unless a higher-level job or event record exists.

A missing, stale, expired, or replaced receipt returns `ErrLeaseLost`. An optional explicitly idempotent administrative helper may treat an already absent message as success, but normal worker settlement remains fenced.

### 6.5 Release and retry delay

A consumer can release a current delivery immediately or after a requested delay.

On success:

- the current lease is cleared;
- the message becomes eligible at the requested time;
- the delivery count is not decremented;
- an immediate release wakes eligible consumers;
- a stale receipt returns `ErrLeaseLost`.

Setting a delivery's remaining visibility to zero is equivalent to immediate release.

### 6.6 Lease extension

A consumer can extend a still-valid current lease. Extension returns the new database-authoritative expiration.

An expired or replaced lease cannot be resurrected. Missing receipts in a batch extension are reported individually so the worker runtime can cancel only the affected handlers.

### 6.7 Retention and expiration

Low-level retention is queue-configured and defaults to four days. The supported initial range is one minute through fourteen days, matching familiar SQS defaults; the architecture may permit a wider range later without changing semantics.

The retention window begins when a message first becomes eligible for delivery, not when it is published. A message scheduled far into the future therefore remains durable through its scheduled time and then receives its full configured retention window. This intentionally differs from Amazon SQS and avoids requiring a separate scheduling subsystem.

Messages remaining beyond retention are deleted without becoming job-level `expired` records. Higher-level jobs separately record terminal expiration.

Retention expiration is cleanup behavior, not a retry strategy.

### 6.8 Dead-letter handling

When an eligible message has reached its configured maximum-delivery count without acknowledgement, it is atomically removed from active delivery and placed in dead-letter storage or its configured dead-letter queue.

This low-level policy applies to raw messages and to higher-level delivery types only where their owning API explicitly adopts it. It never independently dead-letters an internal job dispatch record or makes a job terminal.

Dead-letter records retain enough information for diagnosis:

- original message ID and queue;
- body and headers;
- publication and receive timestamps;
- delivery count;
- terminal reason and bounded error metadata when available.

Redrive creates a new active message identity and enqueue time while recording its relationship to the dead-letter record. Redrive can target the source queue or another explicitly selected queue. It does not mutate payloads in the initial implementation.

### 6.9 Batch capabilities

The PostgreSQL backend supports batch publish, acknowledge, release, and extension. Batch operations preserve per-item errors where partial success is possible and must never hide which receipts lost ownership.

The worker runtime detects and uses batch capability without making it mandatory for future simple backends.

### 6.10 Queue inspection

Applications can query approximate:

- available, leased, delayed, and dead counts;
- oldest available message age;
- next scheduled availability;
- configured delivery and retention policies.

These inspection calls do not lease work.

## 7. Worker Runtime

### 7.1 Handler registration

Applications register one handler per job kind. Registering the same kind twice is an error unless an explicit replacement API is used before the pool starts.

Starting a pool with no handlers or no queue bindings is rejected.

Worker pools advertise or pass the set of job kinds registered in that process when claiming work. The runtime must prefer claiming only jobs it can handle. This allows old and new worker versions with different handler sets to share a lane safely during rolling deployment.

If a process nevertheless receives an unknown kind:

- the handler is not invoked;
- no application attempt or retry-budget unit is consumed;
- the dispatch is released with bounded backoff and jitter;
- the job remains non-terminal;
- a structured `ErrUnavailable` observation with resource `handler` is emitted;
- newer workers remain able to claim it.

Permanently failing unknown kinds is available only through explicit opt-in policy, optionally after a configured grace period. It is never the default.

### 7.2 Capacity-aware dispatch

The runtime leases no more jobs than can begin executing immediately. It does not build a large in-memory backlog whose leases can expire before execution starts.

Configuration supports:

- total process concurrency;
- per-queue concurrency;
- relative queue weights;
- maximum claim batch;
- visibility timeout;
- handler timeout;
- shutdown grace period.

Weights and priorities are best-effort scheduling controls and must not be documented as strict fairness guarantees.

If no running process currently advertises a queued kind, inspection and observability must expose the resulting unhandled backlog rather than silently failing it.

### 7.3 Notifications and polling

The PostgreSQL runtime may use a dedicated `LISTEN` connection per process as a wake-up optimization. It always retains polling fallback.

Users can disable notifications for PgBouncer transaction-pooling environments. Poll-only mode is fully correct but may have higher idle latency and database load.

### 7.4 Automatic lease renewal

The runtime automatically renews active leases before expiration, preferably in batches. Handlers do not implement their own heartbeat loops for ordinary use.

When no handler timeout is configured, renewal can continue for as long as the handler remains active. The runtime always exposes running duration and last successful renewal, and it can emit a one-time long-running-attempt observation after a configurable threshold.

When renewal shows that ownership was lost:

- the handler context is cancelled;
- no success or failure outcome is committed by the stale attempt;
- the attempt history records lease loss where possible;
- another valid delivery can continue processing.

### 7.5 Panic and context handling

The runtime recovers handler panics, records a bounded and redactable stack, and applies the normal retry policy unless attempts are exhausted or configuration marks panics permanent.

Handler contexts can be cancelled because of:

- process shutdown;
- explicit job or workflow cancellation;
- lease loss;
- configured execution timeout.

Context cancellation is cooperative and cannot forcibly undo external side effects.

### 7.6 Graceful shutdown

Graceful shutdown:

1. stops claiming new work;
2. continues renewing leases for active handlers;
3. waits up to the configured grace period;
4. lets completed handlers finalize;
5. cancels remaining handlers;
6. releases unfinished deliveries where ownership remains;
7. stops maintenance and listener resources.

Immediate halt cancels local handlers without acknowledging unfinished work.

Shutdown interruption is recorded as an interrupted attempt but does not count as an application failure for retry-policy purposes.

## 8. JobQueue

### 8.1 Job identity and arguments

A job contains:

- stable job ID;
- kind;
- queue;
- JSON arguments;
- lifecycle state;
- priority;
- scheduling and expiration timestamps;
- execution and retry-budget counts plus maximum application attempts;
- optional uniqueness key;
- optional result and structured error;
- metadata and correlation identifiers;
- optional parent and future workflow identity.

IDs are opaque root-package types. The PostgreSQL backend stores UUID-compatible identities and generates IDs when omitted. Callers can supply stable IDs for idempotent creation.

Job arguments and results are JSON. Typed generic helpers marshal and validate Go values while the underlying storage and interfaces remain usable without generics.

### 8.2 Enqueue

Enqueue durably creates the job and its managed dispatch record in one transaction.

The request can specify:

- caller-provided job ID;
- kind and queue;
- JSON arguments;
- priority;
- initial delay or availability time;
- optional `ExpiresAt` start deadline;
- optional per-job execution-timeout override, distinct from the start deadline;
- maximum attempts;
- uniqueness key;
- metadata and correlation fields.

Behavior:

- an empty kind is invalid;
- an unregistered kind may be enqueued because producers are not required to share a worker's local handler registry;
- an omitted queue uses an explicitly configured default job lane;
- delay and absolute availability are mutually exclusive;
- `ExpiresAt` must be later than the effective availability time;
- duplicate stable IDs are idempotent only when immutable creation fields match;
- conflicting reuse returns `ErrConflict`;
- the managed dispatch record contains a versioned job reference rather than a duplicate copy of all job state.

#### Batch enqueue

Milestone 1 provides bounded `EnqueueBatch` behavior for creating many independent jobs in one call and PostgreSQL transaction.

- Results correspond to request order.
- Matching idempotent duplicates return their existing jobs.
- Every request is validated before commit.
- A conflicting identity, conflicting uniqueness key, or invalid request rolls back the whole batch.
- Partial or best-effort batch creation is deferred until a concrete need justifies its more complicated error contract.
- The maximum batch size is configurable and documented by the backend.

### 8.3 Uniqueness

An optional uniqueness key prevents more than one non-terminal job with the same queue and key.

If an equivalent active job exists, enqueue returns that job with an indication that it was not newly created. Terminal jobs do not block later work with the same uniqueness key unless a future policy explicitly requests permanent uniqueness.

Job identity and uniqueness solve related but different problems:

- a caller-supplied stable job ID makes retries of the same enqueue request idempotent throughout retained job history;
- a uniqueness key prevents semantically equivalent work from being active more than once, even when callers generated different job IDs;
- a parent spawn key prevents a retried parent from creating the same logical child twice.

Concurrent enqueue calls with the same identity or uniqueness key must resolve through database constraints rather than a check-then-insert race. A matching duplicate returns the existing job; reuse with conflicting immutable input returns `ErrConflict`.

After terminal job history is removed by retention, the library no longer promises deduplication for that old identity. Applications requiring permanent business-level deduplication must retain their own durable business key or idempotency ledger.

### 8.4 Job states

The public lifecycle includes:

- `available`;
- `running`;
- `retrying`;
- `succeeded`;
- `failed`;
- `cancelled`;
- `expired`;
- `discarded`;
- `blocked` for workflow nodes introduced in milestone 2.

Ordinary state transitions are monotonic toward a terminal state. Only an explicit administrative retry can move an eligible terminal job back to `retrying`.

There is no separate persisted `scheduled` state in the initial model:

- `available` with `AvailableAt` later than database time is scheduled but not yet eligible;
- `available` with `AvailableAt` at or before database time is eligible now;
- `retrying` retains its distinct state while waiting for its persisted retry time;
- inspection filters and metrics expose scheduled jobs as a derived classification.

This avoids a correctness-dependent scheduler transition whose only purpose would be changing `scheduled` to `available`.

### 8.5 Attempt start

Claiming a job for a handler registered in the current process atomically claims its managed dispatch record, creates an attempt, and transitions an eligible job to `running` under a fence.

Each attempt records:

- attempt ID and number;
- job, message, and lease identity;
- worker and process identity;
- start, heartbeat, and completion timestamps;
- terminal attempt state;
- bounded structured error information;
- handler/build version when configured.

A stale dispatch record for an already terminal or otherwise ineligible job is reconciled without rerunning the job.

An application attempt begins only when the runtime is ready to invoke a registered handler. Dispatches deferred because of an unknown handler, shutdown before invocation, or other infrastructure conditions do not create application attempts.

Attempt history may record an execution interrupted after invocation by shutdown or lease loss, but such an interruption is explicitly marked as not consuming the application retry budget.

#### Managed dispatch invariant

The job row is authoritative. Internal dispatch records exist only to wake workers and must not independently decide job terminality.

- Raw-message maximum-delivery and retention policies do not apply to job dispatch records.
- A non-terminal job cannot be stranded because an internal dispatch record expired, was deleted, or exhausted raw delivery attempts.
- Reconciliation recreates missing dispatch state for non-terminal jobs that should be or become eligible.
- Reconciliation removes dispatch state for terminal or otherwise ineligible jobs.
- Recreated dispatch is idempotently keyed to the logical job so reconciliation cannot create duplicate logical work.
- Only job expiration, cancellation, discard, successful completion, permanent application failure, or exhausted application retry policy makes the job terminal.

### 8.6 Expiration and execution timeout

`ExpiresAt` is the latest time at which a job may begin execution.

- A job claimed at or after `ExpiresAt` is marked `expired`, its delivery is acknowledged, and its handler is not invoked.
- A job that starts before `ExpiresAt` may finish normally after that timestamp.
- Expiration does not kill an already running handler.
- A separate handler execution timeout cancels work that runs too long.
- An expired job remains inspectable and can be administratively retried only with a new valid expiration policy.

This separates queue waiting deadlines from execution-duration limits.

### 8.7 Handler outcomes

A handler does not acknowledge its own delivery. It returns success or a classified error while recording intended result and child operations in its run context.

On success, the runtime attempts one fenced completion transaction. Returning `nil` means the handler is ready to commit its buffered outcome; it does not mean acknowledgement has already occurred.

A result is optional JSON and initially limited to 1 MiB. Larger results must be stored externally with a reference in the job result.

### 8.8 Deterministic child jobs

Milestone 1 handlers can create child jobs as part of successful completion.

Each child requires a stable spawn key. The identity scope is the parent job plus spawn key. Retrying a parent with the same spawn operation returns or reuses the previously materialized child rather than creating duplicates.

Initial child creation supports chains and independent fan-out. Dependency edges, joins, and arbitrary workflow graph queries arrive in milestone 2.

Child operations are buffered and become durable only if parent completion commits.

### 8.9 Error classification and retry

Handler errors support these classifications:

- ordinary retryable failure;
- retry after an explicit delay;
- permanent failure;
- discard without dead-letter escalation;
- panic;
- shutdown interruption;
- lease loss.

Default behavior:

- ordinary errors retry with bounded exponential backoff and jitter;
- explicit retry delay overrides the calculated delay;
- permanent errors transition directly to `failed`;
- discard transitions to `discarded`;
- panic follows the normal retry policy;
- exhausted attempts transition to `failed` and preserve terminal failure diagnostics;
- shutdown interruption releases the job without counting an application failure;
- lease loss permits no stale finalization.

The initial default is five retry-budget-consuming application attempts with configurable per-job or per-handler override. Ordinary handler failures and panics consume that budget. Unknown-handler deferral, shutdown interruption, and lease loss do not. All invoked executions can still appear in attempt history.

The chosen retry timestamp is persisted so a replay does not regenerate different jitter.

### 8.10 Failure transaction

Failure handling atomically:

- verifies attempt ownership;
- records the attempt error and completion;
- transitions the job to `retrying`, `failed`, or `discarded`;
- reschedules managed dispatch or makes the job terminal according to job policy;
- records workflow history when applicable.

If failure recording cannot commit, lease expiry permits another delivery. This can repeat handler execution and is part of the at-least-once contract.

### 8.11 Cancellation

Cancellation is durable and cooperative.

- `CancelJob` accepts a job ID and optional bounded reason and actor metadata.
- An available job, including one with a future `AvailableAt`, or a retrying or blocked job atomically becomes terminal `cancelled`, and its pending dispatch can no longer start.
- A running job atomically becomes terminal `cancelled`, its current completion fence is invalidated, and its handler context receives cancellation.
- If success commits before cancellation, success wins.
- If cancellation commits first, stale success finalization is rejected.
- Cancellation of a terminal job is idempotent only if it is already cancelled; otherwise it returns a terminal-state conflict.
- Cancelling work cannot guarantee reversal of external side effects already performed.

Cancellation therefore forces the durable job to a terminal state, but it cannot forcibly stop an arbitrary Go goroutine. A non-cooperative handler may continue executing locally; fencing prevents it from committing a job result, child jobs, workflow transitions, or library-managed events afterward.

### 8.12 Administrative retry

An authorized caller can retry `failed`, `cancelled`, `expired`, or `discarded` jobs subject to policy.

Administrative retry:

- preserves the logical job ID, original inputs, and history;
- creates new managed dispatch state and eventually a new attempt and lease;
- can override availability, expiration, queue, and maximum remaining attempts;
- records actor, reason, timestamp, and overrides;
- rejects a job already active unless a separate force operation is introduced later.

### 8.13 Results and inspection

Applications can retrieve a job by ID and list jobs using bounded filters for:

- state;
- queue;
- kind;
- creation or completion range;
- workflow, parent, correlation, or uniqueness identity.

Inspection returns job state, result, last error, and attempt summaries without exposing internal database rows as a compatibility contract.

Job and attempt history use configurable retention. The initial default is 30 days after terminal completion. Applications requiring longer audit retention can increase it or export history.

### 8.14 PostgreSQL finalizer

Milestone 1 includes an optional PostgreSQL-specific finalizer invoked inside the short fenced completion transaction after handler execution.

The finalizer can write application state through a transaction-bound `pgkit` handle. It must not perform slow network calls or execute the whole handler.

If the finalizer returns an error:

- the completion transaction rolls back;
- the job is not acknowledged or marked successful;
- the failure is processed under the configured retry policy;
- buffered child jobs and events do not become visible.

## 9. Workflow DAGs

### 9.1 Workflow definition and run

A workflow definition identifies a workflow type and version. Starting it creates a workflow run with stable identity, input, metadata, and a graph of deterministic node keys.

An optional workflow key provides idempotent start within one workflow type. Repeating a matching start returns the existing run; conflicting input or definition version returns `ErrConflict`.

### 9.2 Graph validation

Static graphs are validated before persistence:

- node keys are unique within the run;
- all referenced dependencies exist;
- self-dependencies are rejected;
- cycles are rejected;
- every node has a registered or otherwise permitted job kind;
- graph and payload size limits are enforced.

The complete initial graph is inserted atomically. Only nodes with no unresolved dependencies become available.

### 9.3 Dependencies and readiness

Each node tracks unresolved dependencies. Supported initial dependency conditions are:

- predecessor succeeded;
- predecessor failed;
- predecessor reached any terminal state.

A node becomes available exactly once when all required conditions resolve. Dependency resolution and creation of its queue delivery commit with the predecessor transition.

General user-authored expressions or SQL conditions are not supported.

### 9.4 Dynamic graph mutation

Running jobs can create deterministic new nodes and edges through buffered outcome operations.

Initial dynamic mutation permits edges from existing work toward newly created nodes, preventing cycles by construction. Arbitrary rewiring of existing graph regions is out of scope.

Stable node or spawn keys are mandatory. Retried graph mutations must resolve to the same nodes.

### 9.5 Fan-out and joins

The API supports creating multiple children and an explicit join node depending on those children. Join behavior is graph syntax over ordinary nodes and dependencies, not a hidden execution mechanism.

Large fan-out is subject to configured transaction and graph-size limits. Chunked materialization may be added for very large graphs without weakening deterministic identity.

### 9.6 Workflow states

Workflow runs expose:

- pending;
- running;
- succeeded;
- failed;
- cancelled;
- expired when a workflow-level deadline is configured and reached before completion.

Workflow completion is derived from its required nodes and terminal policy. One node failure does not implicitly define the whole workflow outcome unless the definition says that node is required.

### 9.7 Workflow cancellation

Cancelling a workflow:

- marks the run cancelled when cancellation wins the state race;
- cancels or prevents non-terminal pending nodes;
- signals running handlers cooperatively;
- prevents cancelled nodes from committing stale success;
- records cancellation reason and actor;
- does not undo external side effects.

### 9.8 History and inspection

Every meaningful workflow transition appends an immutable operational event. Heartbeats are excluded by default to control volume.

Applications can query:

- workflow current state and output;
- nodes and dependency edges;
- node attempts and results;
- chronological transition history;
- correlation and causation metadata.

The operational event log supports audit and debugging but is distinct from the general domain `EventStore`.

Workflow runs, nodes, dependencies, operational history, and their referenced node jobs are retained indefinitely in the initial milestones. A future explicit workflow-retention policy must remove the graph and job history as one referentially safe unit.

### 9.9 Versioning and replay boundary

Each workflow run records its definition version, and attempts can record handler/build versions.

The system does not promise transparent replay of arbitrary historical Go handler code. Workflow history can explain and project durable transitions; rerunning business code is an explicit retry or new workflow operation.

## 10. EventBus

### 10.1 Event publication

An event contains:

- stable event ID;
- topic and type;
- source and optional subject;
- JSON data and metadata;
- occurrence time;
- correlation and causation identity.

Publication stores the immutable event once and creates an independent durable delivery reference for each matching active subscription.

Repeating a stable event ID with matching content is idempotent. Conflicting content returns `ErrConflict`.

Event identity is the deduplication boundary. Concurrent publication of the same event ID must produce one immutable stored event and one delivery per matching subscription, not duplicate fan-out. The event ID remains reserved for as long as the event is retained.

### 10.2 Topics and subscriptions

Applications can create, inspect, enable, disable, and delete topics and subscriptions.

Each subscription has its own:

- durable backlog;
- retry and dead-letter policy;
- acknowledgement state;
- concurrency configuration;
- simple deterministic filters.

Initial filters support exact or prefix matching for event type, source, and subject. Arbitrary SQL or executable filter expressions are not supported.

Creating a subscription affects future publications by default. Historical delivery requires an explicit replay/redrive request so an accidental subscription cannot enqueue an unbounded history.

### 10.3 Event delivery

Event delivery uses the same lease and settlement semantics as `MessageQueue` while exposing event metadata and subscription identity.

Delivery is at least once per subscription. One subscription's acknowledgement, retry, pause, or failure does not affect another subscription.

Event payload is stored once; subscription queue messages contain versioned references.

### 10.4 Event redrive

Dead subscription deliveries can be inspected and redriven independently. Redrive creates a new delivery identity while preserving the immutable event identity and redrive history.

Redriving event delivery does not append a duplicate domain event to its source stream.

### 10.5 Event retention

EventBus events use configurable retention with an initial default of 30 days. The event payload cannot be removed while any active or retained dead-letter subscription delivery still references it.

An event becomes eligible for cleanup only when:

- its configured minimum retention window has elapsed; and
- every subscription delivery is acknowledged, discarded, expired, or removed under its own retained dead-letter policy.

Applications requiring an authoritative indefinite history append events to `EventStore`. EventBus retention is a delivery and operational-history policy, not event sourcing.

## 11. EventStore

### 11.1 Streams and append

The event store provides append-only streams. Appending requires:

- stream ID;
- expected stream version;
- one or more immutable events with stable IDs and types;
- JSON data and metadata;
- correlation and causation identity.

An append succeeds only if the current stream version matches the expected version. Otherwise it returns `ErrConflict` and appends nothing.

Successful appends assign contiguous versions within each stream. Events also receive unique global positions for ordered traversal, but positions may contain gaps and do not claim to represent wall-clock order.

Checkpointed global reads must not permanently skip an event whose transaction becomes visible later. The reader therefore exposes a safe checkpoint or equivalent high-water mark that advances only when earlier position reservations can no longer appear.

### 11.2 Reading and replay

Applications can read:

- one stream from a requested version with a bounded limit;
- global events from a requested position with a bounded limit;
- event metadata needed to build projections.

Replay means reading stored immutable events again. It does not automatically execute job handlers, republish to all subscriptions, or mutate source events.

Projection helpers checkpoint the last applied global position and must tolerate replay after an uncertain commit.

### 11.3 Atomic composition

Within one PostgreSQL transaction, applications or job completion can:

- append domain events with optimistic concurrency;
- update projections or application state;
- enqueue jobs;
- publish appended events to matching subscriptions;
- finalize current job/workflow state.

Any conflict or failure rolls back the entire composed operation.

### 11.4 Snapshots and retention

Event streams are append-only, retained indefinitely by default, and unaffected by queue or EventBus retention. Snapshotting, archival, and stream deletion policies are deferred until demonstrated workloads require them.

Administrative erasure required by an application remains the application's responsibility until an explicit policy is designed; the library must not silently claim immutable storage satisfies every regulatory regime.

## 12. Error Contract

Public sentinel or typed errors allow callers to recognize at least:

- queue, job, workflow, topic, subscription, or stream not found;
- lost or stale lease;
- already removed message;
- duplicate/idempotent creation;
- optimistic concurrency conflict;
- invalid state transition;
- terminal-state conflict;
- invalid request or payload limit;
- handler not registered in the current worker process;
- unavailable/closed runtime;
- migration or compatibility failure.

Errors preserve a machine-checkable category through wrapping. Human messages include useful identity but must not include arbitrary job payloads, secrets, database connection strings, or unredacted handler errors.

Batch APIs report item-level failures without losing the batch-level database or transport error.

## 13. Configuration and Defaults

Configuration is provided through typed constructors and options, queue configuration records, worker pool configuration, and per-operation overrides where appropriate. Environment-variable parsing is an application concern, not a core library requirement.

Initial defaults:

- raw-message and EventBus-delivery visibility timeout: 30 seconds;
- job-lane visibility timeout: 60 seconds;
- low-level message retention: 4 days;
- maximum raw-message deliveries: 5;
- maximum job application attempts: 5;
- maximum message or JSON result body: 1 MiB;
- maximum encoded headers: 64 KiB;
- idle correctness poll: 1 second, configurable;
- notifications: enabled when a dedicated session connection is configured;
- handler timeout: none unless configured;
- long-running observation threshold: configurable without imposing a second hard timeout;
- standalone job/attempt terminal history: 30 days; workflow-node jobs follow the indefinite initial workflow-retention policy;
- EventBus event retention: 30 days minimum and never shorter than outstanding delivery references;
- strict FIFO: disabled and unsupported initially;
- automatic queue creation: disabled except when explicitly enabled.

Durations must be positive where required and bounded to values representable safely by both Go and PostgreSQL. Invalid combinations fail at configuration or request validation time.

## 14. Observability

The core library exposes optional no-op-by-default observation hooks for:

- publish and receive;
- acknowledgement and release;
- job start and finish;
- retries, cancellation, expiration, and dead-letter transitions;
- lease extension and loss;
- listener reconnect and polling fallback;
- unknown-handler deferral and unhandled job backlog;
- long-running attempts and last successful lease renewal;
- workflow and event transitions.

Observations include relevant queue, message, lease, job, attempt, workflow, node, correlation, worker, and process identity.

The library does not force a logging, metrics, or tracing vendor. Adapter packages can integrate OpenTelemetry or application-specific systems later.

Inspection queries expose queue depth, oldest eligible age, retry schedule, running and terminal jobs, dead work, workflow graph/timeline, and per-kind outcome data needed by operators.

Job inspection additionally exposes derived scheduled counts, unhandled counts by kind and lane, current running duration, and last successful lease-renewal time. A configured long-running threshold emits an observation without changing job state or stopping lease renewal.

## 15. Security and Resource Safety

Initial deployments assume trusted application code and no hostile tenants directly querying queue tables.

The library still enforces:

- payload, header, name, batch, and graph-size limits;
- fully qualified database objects under a configurable schema;
- error redaction hooks;
- no arbitrary SQL event filters;
- no secrets in default logs or metrics;
- context cancellation for blocking operations;
- bounded error and stack persistence;
- short queue and completion transactions;
- no database connection held solely for the duration of handler execution.

Row-level security, tenant quotas, and hostile multi-tenant isolation require a later explicit design.

## 16. Compatibility and Migrations

The initial compatibility targets are:

- module path `github.com/goware/jobqueue`;
- the Go version declared by the repository's `go.mod` for development, with the supported minimum frozen before v1.0;
- PostgreSQL 15 or newer unless deployment requirements establish an older supported version;
- `github.com/goware/pgkit/v2` over `github.com/jackc/pgx/v5`;
- ordinary pooled PostgreSQL queries;
- notification mode with a session-persistent listener or fully correct poll-only mode.

Migrations are embedded and also exposed as raw files for application-owned migration tooling. Schema migration is explicit; constructing a backend does not silently modify production databases.

Migrations record version, checksum, and application time. An incompatible schema version causes a clear startup or operation error rather than undefined behavior.

## 17. Performance and Operational Expectations

Correctness takes priority over a promised throughput number before benchmarks exist.

The initial design must nevertheless support:

- multiple application processes claiming concurrently;
- at least 100 local worker goroutines without one queue connection per worker;
- batch claim, acknowledgement, and lease renewal;
- jobs lasting from milliseconds through hours via renewable leases;
- notification loss and database reconnect;
- queue depths from small development workloads through millions of rows, subject to published benchmark results;
- bounded local buffering based on immediately available execution capacity.

The project publishes benchmark methodology and results rather than claiming AWS-scale throughput. Partitioning and specialized physical layouts require benchmark evidence.

## 18. Testing and Conformance

Every `MessageQueue` backend must pass a shared conformance suite covering:

- publish, receive, acknowledge, and release;
- delayed availability;
- lease expiry and extension;
- stale-receipt rejection;
- concurrent claim behavior;
- batch limits and item errors;
- deduplication;
- retention and dead-letter transitions;
- redrive;
- caller cancellation.

The PostgreSQL implementation uses real PostgreSQL integration tests for concurrent claims, transaction rollback, listener reconnect, polling recovery, atomic application writes, child creation, lease fencing, retry scheduling, and graceful shutdown.

Fault-injection tests cover crashes or uncertainty before and after completion commit. Workflow tests verify deterministic child and node identity. Event-store tests verify optimistic concurrency and gapless committed stream versions.

Rolling-deployment tests run old and new worker versions with different registered kind sets and verify that an old worker cannot permanently fail new-kind work. Reconciliation tests remove internal dispatch records and verify that non-terminal jobs become executable again without duplicating logical jobs.

## 19. Initial Non-Goals

The initial milestones do not promise:

- globally exactly-once external effects;
- strict global FIFO or FIFO message groups;
- a Kafka-compatible log or infinite stream retention;
- arbitrary workflow expression languages;
- transparent deterministic replay of arbitrary Go handlers;
- long-lived handler transactions;
- a database connection per worker;
- multi-region active/active queueing;
- hostile multi-tenant isolation;
- recurring cron replacement;
- a bundled operational web UI;
- transparent compatibility with Temporal, SQS, Kafka, or another provider's API.

## 20. Milestone Acceptance Criteria

### 20.1 Queue and jobs

Milestone 1 is complete when:

- queue conformance and PostgreSQL integration tests pass;
- stale receipts cannot settle or finalize work;
- crash recovery demonstrates at-least-once behavior without message loss;
- workers do not hold queue connections during handler execution;
- notification loss recovers through polling;
- job retries, cancellation, expiration, results, and history behave as specified;
- workers with different registered kind sets can share a lane during rolling deployment without permanently failing unknown work;
- missing managed dispatch state is reconciled without stranding or duplicating a non-terminal job;
- raw message retention and delivery limits cannot independently terminate a managed job;
- scheduled-job inspection derives eligibility consistently from `AvailableAt` and database time;
- atomic batch enqueue either commits every valid/idempotent request or none;
- concurrent duplicate job requests resolve to one logical job under stable IDs or uniqueness keys;
- cancellation that wins the state race prevents a running attempt from committing stale outcomes;
- parent success and deterministic child creation commit atomically;
- application state can be finalized atomically through the PostgreSQL finalizer;
- graceful shutdown leaves unfinished work recoverable;
- public examples demonstrate idempotent handlers and asynchronous result observation.

### 20.2 Workflow DAGs

Milestone 2 is complete when:

- static cycles and invalid dependencies are rejected;
- eligible nodes become available exactly once;
- retried dynamic mutation does not duplicate nodes;
- fan-out and joins survive process crashes;
- cancellation prevents stale transitions;
- current graph and immutable transition history are inspectable;
- workflow and handler versions are recorded.

### 20.3 Events

Milestone 3 is complete when:

- one event publication creates independent durable subscription deliveries;
- EventBus cleanup cannot remove an event still referenced by an active or retained dead-letter delivery;
- concurrent publication of one stable event ID does not duplicate the event or its subscription fan-out;
- subscription retries and acknowledgements do not affect one another;
- stream append enforces expected versions;
- committed versions are contiguous within each stream;
- global checkpoint traversal cannot permanently skip a late-visible committed event;
- replay reads immutable events without implicitly rerunning work;
- job completion, event append, event fan-out, and application state can compose atomically in PostgreSQL;
- event and delivery redrive preserve audit relationships without duplicating source events.
