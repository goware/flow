// Package flow provides event-driven, durable, distributed work execution on
// PostgreSQL.
//
// The primary model is command → worker → events: commands instruct work,
// workers do the work, and events record durable facts. Workers may atomically
// stage typed application events and bounded child commands. Durable
// coordinators handle joins, branching, adaptive agents, loops, and open-ended
// work. Exact event gates can hold a command until execution-scoped facts
// exist. Command creation, terminal events, and causation are retained in one
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
package flow
