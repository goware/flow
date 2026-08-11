// Package flow provides event-driven, durable, distributed work execution on
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
//     Each invocation receives a [Work] value containing the typed arguments
//     and durable command identity.
//   - [Event] is an immutable, typed definition of a durable fact. An event
//     name describes the fact kind; its key carries domain and generation
//     identity.
//   - [Execution] is one durable command graph and its consistency boundary.
//     It owns the root command, staged descendants, exact event inputs,
//     attempts, and ordered journal.
//   - [Runtime] is both a [Client] for durable operations and, when passed to
//     [Runtime.Run], a processor for locally registered workers.
//
// The usual shape is:
//
//	Command definition --Execute--> Execution
//	Runtime.Run -----------claim----> Work
//	Work ----------------Execute----> staged child command
//	Work -----------------Emit------> staged application event
//	Event + WaitFor ----------------> runnable command
//
// Root execution starts are durable and asynchronous: Execute always enqueues
// rather than calling a worker inline. Inside a worker, [Execute] and [Emit]
// build one typed decision in memory. That decision, the worker result, and an
// optional short same-database [WithCommit] callback settle atomically after
// the attempt fence is rechecked.
//
// Exact event gates provide durable sequencing and all-of joins without
// consuming a worker or database connection while waiting. Matching is scoped
// to one execution and uses the tuple (event name, event key). Values for the
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
// interpreting fenced settlement as exactly-once execution.
//
// # Execution identity and history
//
// A stable non-empty execution key is permanently idempotent by default.
// [WithLiveKey] instead gives at most one non-terminal execution for a command
// definition and key; after that execution becomes terminal, a new generation
// may start with the same key. [LookupLiveExecution] resolves the current
// non-terminal generation when an external caller knows the domain key but not
// its exact [ExecutionID]. Terminal generations remain durable history.
//
// Flow retains journal, payload, and terminal data indefinitely and exposes no
// pruning API. Inspection, history, and trace APIs read durable state without
// invoking application code.
//
// # Choosing command boundaries
//
// A command should mark an independent retry, side-effect, isolation, queue,
// timeout, external-wait, or parallelism boundary—not every deterministic
// business-logic step. Keep causally related commands in one execution, but
// use separate executions for independent bulk items because one execution is
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
// remote calls. Caller-owned transactions should also be short because an
// execution lock remains held until the caller commits or rolls back.
//
// External callers record execution-scoped events with [Event.Emit].
// [Event.Deliver] provides deliberately detached ingress to a known execution,
// including from an active worker; passing [Runtime.InTx] joins it to
// caller-owned application writes. Same-execution worker events should
// normally use staged [Emit] so they commit atomically with the worker result.
//
// Observer delivery is bounded and best-effort. Observers must return promptly
// and should honor context cancellation; a blocked or failed observer never
// changes durable execution correctness or prevents runtime shutdown.
//
// The v0.1 release line supports Go 1.26 and PostgreSQL 17 and 18. Published
// migrations are immutable and upgrades are forward-only. During v0.x,
// intentional Go API changes may be described in release notes.
package flow
