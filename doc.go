// Package flow provides event-driven, durable, distributed work execution on
// PostgreSQL.
//
// Commands instruct work, workers do the work, and events record durable
// facts. Workers may atomically stage bounded child commands. Optional pure
// plans describe dependencies, joins, waits, and fact-driven branching;
// durable coordinators handle adaptive agents, loops, and open-ended work.
// The execution graph is a projection of command creation, terminal events,
// dependencies, child membership, and causation retained in one ordered
// per-execution journal.
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
