---
status: complete
historical: true
superseded_by: ../plans/2-remove-plan.md
---

# Phase 2: PostgreSQL Schema and Store Foundation

> Historical delivery record. The active seven-table schema is defined by `../plans/2-remove-plan.md` and `../components/schema.md`.

## Overview

Create Flow's complete physical PostgreSQL contract and the narrow store machinery used by every later semantic transition. The migration is embedded, schema-qualified, checksum-verified, and safe for Flow to coexist with application tables. Semantic transactions lock the execution first, capture PostgreSQL time after that lock, allocate gap-free journal positions, append deterministic batches, and expose bounded immutable history. Tests use an actual PostgreSQL database and isolated schemas; no SQL behavior is faked.

## Steps

1. Add schema options, embedded monotonic migration units, advisory-lock migration execution, migration checksums/compatibility metadata, `Migrate`, `CheckSchema`, and `MigrationFS`.
2. Implement all nine fixed `flow_` tables, named constraints, indexes, and foreign keys. Keep physical storage parameters at PostgreSQL defaults until the required workload benchmarks provide evidence for the migration that tunes them.
3. Add internal row and journal-body codecs plus SQLSTATE/constraint error mapping that never leaks SQL or payloads.
4. Implement the store and ordered semantic-transaction executor: `READ COMMITTED`, blocking or skip-locked execution-first acquisition, PostgreSQL decision time captured after locking, exact journal reservation, deterministic batch append, commit/rollback closure, and ascending multi-execution ordering state for later transaction-scoped clients.
5. Implement the bounded indexed `History` store query and migration/schema inspection helpers.
6. Add a reusable real-PostgreSQL test harness with isolated schemas, direct database invariant assertions, concurrency tests, and migration repeatability/checksum tests.
7. Run unit, PostgreSQL integration, race, vet, coverage, and spec-aware review before marking the phase complete.

## Tests

- `TestMigrateAndCheckSchema`: all nine qualified tables, expected constraints/indexes, compatibility row, idempotent rerun, and rendered migration filesystem.
- `TestMigrationChecksumMismatch`: an altered applied checksum is rejected as `ErrSchema` without changing the database.
- `TestSchemaConstraints`: invalid statuses, hashes, terminal shapes, cross-table references, and duplicate identities are rejected by named constraints.
- `TestBeginSemanticExecutionFirst`: blocking serialization and skip-locked acquisition both lock the execution before capturing database time.
- `TestJournalAllocationGapFree`: rollback restores the allocator; committed batches have consecutive execution-local positions and deterministic order.
- `TestHistory`: bounded position-based scans return immutable journal rows in order and reject invalid limits.
- `TestSQLErrorMapping`: known SQLSTATE/constraint categories map to safe public sentinels without SQL or values.
