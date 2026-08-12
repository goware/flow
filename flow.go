// Package flow provides event-driven, durable, distributed work on
// PostgreSQL.
//
// # Core model
//
// Flow has a small set of foundational concepts:
//
//   - [Command] is an immutable, typed definition of work. Its argument and
//     result types are part of the Go API, while its stable name and version
//     are part of durable identity.
//   - A worker, registered with [Handle], implements one command definition.
//     Each invocation receives a fresh [Work]: the attempt-local scope for one
//     claimed command, containing typed arguments, durable identity, event
//     inputs, and the decision being built. Work is neither the whole Run nor
//     the immutable Command definition.
//   - [Event] is an immutable, typed definition of a durable fact. An event
//     name describes the fact kind; its key carries domain and generation
//     identity.
//   - [Run] is one durable command graph and its consistency boundary.
//     It owns the root command, staged descendants, exact event inputs,
//     attempts, and ordered journal.
//   - [Runtime] is both a [Client] for durable operations and, when passed to
//     [Runtime.Run], a processor for locally registered workers.
//
// The usual shape is:
//
//	Command definition --Enqueue--> Run
//	Runtime.Run -----------claim----> attempt-local Work
//	Work ----------------Enqueue----> staged child command
//	Work -----------------Emit------> staged application event
//	Event + WaitFor ----------------> runnable command
//
// Root run starts are durable and asynchronous: Enqueue always enqueues
// rather than calling a worker inline. Inside a worker, [Enqueue] and [Emit]
// build one typed decision in memory. That decision, the worker result, and an
// optional short same-database [WithCommit] callback settle atomically after
// the attempt fence is rechecked.
//
// Exact event gates provide durable sequencing and all-of joins without
// consuming a worker or database connection while waiting. Matching is scoped
// to one run and uses the tuple (event name, event key). Values for the
// current command's declared gates are materialized before invocation and read
// from memory with [GetEventValue].
//
// # Definitions, clients, and processing
//
// Definitions are immutable values and registration is runtime-local; Flow
// keeps no process-global registry. A [Client] is a sealed durable-operation
// capability implemented by [Runtime] and its transaction-scoped client. New
// validates the explicitly migrated schema and starts no goroutines, so a
// Runtime that is not passed to Run remains a lightweight client for API and
// publisher processes.
//
// Runtime.Run processes compatible commands with bounded concurrency,
// renewable leases, settlement fencing, and anonymous takeover across
// replicas. Renewal calls are internally time-bounded and skip rows held by
// another Flow transaction so one settlement cannot delay unrelated attempts.
// A process-local watchdog conservatively cancels attempts whose last known
// lease window expires; PostgreSQL attempt ID and lease-token fencing remains
// the durable ownership authority.
//
// Handler invocation is at-least-once. Durable PostgreSQL progression is
// fenced so that only the current attempt can settle. Application handlers
// should therefore use stable idempotency keys for remote effects rather than
// interpreting fenced settlement as exactly-once handler invocation.
//
// # Run identity and history
//
// A stable non-empty run key is permanently idempotent by default.
// [WithLiveKey] instead gives at most one non-terminal run for a command
// definition and key; after that run becomes terminal, a new generation
// may start with the same key. [GetCurrentRun] resolves the current
// non-terminal generation when an external caller knows the domain key but not
// its exact [RunID]. Terminal generations remain durable history.
//
// Flow retains journal, payload, and terminal data indefinitely and exposes no
// pruning API. Inspection, history, and trace APIs read durable state without
// invoking application code.
//
// # Choosing command boundaries
//
// A command should mark an independent retry, side-effect, isolation, queue,
// timeout, external-wait, or parallelism boundary—not every deterministic
// business-logic step. Keep causally related commands in one run, but
// use separate runs for independent bulk items because one run is
// a serialized semantic aggregate.
//
// Large fan-outs should be chunked into bounded command batches and large
// all-of inputs reduced through hierarchical joins. Parent-produced data
// belongs directly in child arguments; large or sensitive values should stay
// in application storage behind stable references.
//
// # Transactions, events, and operations
//
// [WithCommit] is intended for short same-database writes and must not contain
// remote calls. Caller-owned transactions should also be short because a run
// lock remains held until the caller commits or rolls back. Create exactly one
// [TransactionClient] with [Runtime.InTx] for each caller transaction, perform
// Flow writes first, call [TransactionClient.BeginApplicationWrites], and then
// perform application row locks/writes. The client is non-concurrent, does not
// own the transaction, and must not outlive it.
//
// External callers record run-scoped events with [Event.Deliver], which
// provides deliberately detached ingress to a known run,
// including from an active worker; passing [Runtime.InTx] joins it to
// caller-owned application writes. Same-run worker events should
// normally use staged [Emit] so they commit atomically with the worker result.
// External code that knows a domain key rather than an exact run ID may compose
// [GetCurrentRun] with [Event.Deliver], handling the ordinary race in which the
// selected run settles before delivery. Event definitions should name stable
// fact kinds; deterministic keys should carry entity and generation identity.
//
// [Command.ReplaceCurrentRun] atomically cancels an exact expected live-key
// generation and creates a distinct successor. Retries can rediscover a
// declaration-equivalent successor only after the current run ID differs from
// the expected predecessor.
//
// Positive fractional public durations are rounded upward to a whole
// millisecond before durable fingerprints or rows are produced. Stored and
// decoded durations remain strictly exact milliseconds.
//
// Observer delivery is bounded and best-effort. Observers must return promptly
// and should honor context cancellation; a blocked or failed observer never
// changes durable run correctness or prevents runtime shutdown.
//
// Each [Observation] is a typed lifecycle fact: a (Kind, Operation, Outcome)
// tuple named by exported constants, the run ID, and the run key and root
// definition name wherever the emitting path holds them. Tuples are only added
// within a major version, so consumers must ignore unknown ones. Terminal
// facts — run and command terminal transitions, wait expiry, and lease
// recovery — hold reserved queue capacity so a duty-cycle flood cannot evict
// them, and the shutdown drain reports dropped terminal facts separately.
// Alerting built on observations still needs a polling reconciliation over the
// read APIs, which remain the only durable truth.
//
// The current v0.x line supports Go 1.26 and PostgreSQL 17 and 18. Published
// migrations are immutable and upgrades are forward-only. During v0.x,
// intentional Go API changes may be described in release notes.
package flow
