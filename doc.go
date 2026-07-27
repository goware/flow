// Package flow provides event-driven, durable, distributed work execution on
// PostgreSQL.
//
// Commands instruct work, workers do the work, events record durable facts,
// optional plans react declaratively, and workers may spawn child commands.
// Definitions are immutable values and registration is runtime-local; package
// flow keeps no process-global registry.
package flow
