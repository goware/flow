---
status: complete
historical: true
superseded_by: ../plans/3-remove-coordinator.md
---

# Phase 3: Transactional Ingress and Initial Replay

> Historical delivery record for the original ingress model. The active command-only ingress and replay contract is defined by `../plans/3-remove-coordinator.md` and the synchronized component specifications.

## Overview

Deliver Flow's complete durable ingress surface without starting background processors. A configured runtime validates schema compatibility and implements the sealed client capability. Direct, plan, and coordinator starts create their materialized rows and complete initial journal batches atomically; `Issue`, `Publish`, and cancellation reuse the execution-first semantic transaction path. The same operations work inside caller-owned PostgreSQL transactions without Flow committing or rolling them back. Public `History` exposes the immutable journal, and a pure reducer begins reconstructing execution and command projections from `ExecutionStarted`, `CommandCreated`, ingress events, and terminal cancellation records.

## Steps

1. Add runtime configuration and lifecycle foundations: `New`, shared `WithSchema`, `WithMaxCommandsPerExecution`, schema compatibility checks, observer plumbing, and sealed runtime/transaction clients.
2. Add execution options and canonical request preparation for deadlines, metadata, fail-fast, typed inputs, stable fingerprints, and safe validation.
3. Extend the semantic store executor with atomic execution, command, queue, coordinator, event, dirty-plan, counter, and terminal mutations while retaining deterministic journal allocation and execution-first locking.
4. Implement idempotent direct, plan, and coordinator `Execute` paths, including direct root materialization, plan dirty marking, and coordinator start activation.
5. Implement `Issue`, `Publish`, command cancellation, and execution cancellation with idempotency-before-terminal checks, stored command ceilings, queue creation, safe bounded reasons, and deterministic history.
6. Implement caller-owned `InTx` behavior, ascending existing-execution ordering, closed-transaction handling, Flow-before-application documentation, and rollback/commit tests.
7. Expose bounded public `History` records and add the initial pure replay reducer plus replay-versus-projection conformance assertions.
8. Thread Phase 3 observer events and named fault points through each ingress commit boundary.
9. Verify public and transaction-scoped behavior against real PostgreSQL, including concurrency, rollback, idempotency conflicts, queue rows, journal causation/order, cancellation, and replay equality; run race, vet, coverage, and spec-aware review.

## Tests

- Concurrent equivalent starts converge on one execution and one initial history batch; material mismatch returns `ErrConflict` even after terminality.
- Direct start creates `root`, its ready queue row, accepted policy, counters, `ExecutionStarted`, and `CommandCreated` atomically.
- Plan start is dirty with no inline declarations; coordinator start stores typed state and one ready start activation.
- `Issue` enforces mode, execution-wide keys, canonical identity, and the persisted command ceiling without partial writes.
- `Publish` coalesces an equivalent event, conflicts across payload or version, checks idempotency before terminality, and dirties plan mode.
- Caller-owned commit and rollback include or exclude all Flow rows with application rows; reverse execution order and post-close use fail safely.
- Cancellation records exactly one terminal event per affected subject and cannot be overwritten by later ingress.
- Public `History` paginates immutable records, and replay of each committed prefix agrees with the settled execution/command projections implemented in this phase.
