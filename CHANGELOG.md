# Changelog

## Unreleased

- Added typed durable command and execution primitives backed by a six-table PostgreSQL schema and gap-free per-execution journal.
- Added permanent and live execution-key scopes for durable idempotency and live-work deduplication.
- Added bounded, cursor-paginated live-work and keyed-history inspection APIs.
- Added fenced at-least-once workers, renewable leases, conservative recovery, retries, deadlines, cancellation, and graceful shutdown.
- Added set-oriented worker decisions and claims, exact event gates, delta readiness, batched fan-out, and caller-owned transactions.
- Established Go 1.26 and PostgreSQL 17/18 as the initial supported release baseline.
