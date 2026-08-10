// Package uuid generates Flow's durable identifiers and re-exports the
// identifier type so identifier-producing packages depend on one policy.
package uuid

import guuid "github.com/google/uuid"

// UUID is Flow's durable identifier type. The alias keeps values fully
// interchangeable with the underlying library type used by pgx and tests.
type UUID = guuid.UUID

// Nil is the zero identifier.
var Nil = guuid.Nil

// Parse decodes an identifier at a public or storage boundary.
func Parse(value string) (UUID, error) {
	return guuid.Parse(value)
}

// New returns a UUIDv7 identifier. Time-ordered identifiers keep hot
// primary-key indexes append-mostly and make identifier byte order correlate
// with creation order; generation is strictly monotonic within one process.
// The tradeoff is deliberate: identifiers encode their creation time, so they
// are not secrets, matching Flow's existing guidance that keys, metadata, and
// identifiers must not carry sensitive values.
func New() UUID {
	return guuid.Must(guuid.NewV7())
}
