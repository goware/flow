// Package flow provides event-driven, durable, distributed work execution on
// PostgreSQL.
//
// The primary model is command → worker → events: commands instruct work,
// workers do the work, and events record durable facts. Workers may atomically
// stage typed application events and bounded sub-commands. Exact event gates
// provide bounded sequencing and all-of joins by holding a command until its
// execution-scoped facts exist. Command creation, terminal events, and causation are retained in one
// ordered per-execution journal.
//
// Execute always enqueues. Runtime.Run processes compatible work with bounded
// concurrency, renewable leases, settlement fencing, and anonymous takeover
// across replicas. New validates the explicitly migrated schema and starts no
// goroutines. A Runtime not passed to Run remains a lightweight Client for API
// and publisher processes.
//
// Definitions are immutable values and registration is runtime-local; flow
// keeps no process-global registry. Application handlers should use stable
// idempotency keys for external effects because invocation remains at-least-once
// even though durable PostgreSQL progression commits exactly once.
//
// A command should mark an independent retry, side-effect, isolation, queue, or
// parallelism boundary, rather than each deterministic business-logic step.
// Keep causally related commands in one execution, but use separate executions
// for independent bulk items because one execution is a serialized semantic
// aggregate. Large fan-outs should be chunked into bounded command batches and
// large all-of inputs reduced through hierarchical joins. Parent-produced data
// belongs directly in child arguments; large or sensitive values should stay in
// application storage behind stable references.
//
// WithCommit is intended for short same-database writes and must not contain
// remote calls. Caller-owned transactions should also be kept short because an
// execution lock remains held until the caller commits or rolls back.
//
// External callers record execution-scoped events with Event.Emit. Event.Deliver
// provides deliberately detached ingress to a known execution, including from
// an active worker; passing Runtime.InTx joins it to caller-owned application
// writes. Same-execution worker events should normally use staged Emit instead.
package flow
