---
status: complete
historical: true
superseded_by: ../plans/3-remove-coordinator.md
---

# Phase 8: Inspection, testing, and operational surfaces

> Historical delivery record. Removed workflow fields no longer exist in the active inspection contract.

## Goal

Complete the bounded public read model and the developer/operations support needed to use Flow without querying internal tables: execution lookup/list/await, rich trace projection, replay conformance, database-free deterministic harnesses, safe observations, compatibility checks, and deployment documentation.

## Work

1. Add store-backed `GetExecution`, definition/key lookup, stable cursor-filtered listing, and polling `AwaitExecution` with structured errors and transaction-scoped reads.
2. Enrich `Trace` with events, coordinator state/inbox/attempts, execution result/failure, and current durable waiting/scheduling detail while keeping `History` bounded and immutable.
3. Extend replay for every Phase 7 transition and add live-materialization conformance checks over direct, plan, and coordinator executions.
4. Grow `flowtest` around the production deterministic engine for worker decisions, plan snapshots/simulation, coordinator deliveries, commit functions, and command ceilings without PostgreSQL.
5. Complete safe observer coverage and panic isolation, schema compatibility checks, package documentation, runnable-example instructions, and supported deployment-role guidance.

## Exit checks

- Public get, lookup, list pagination/filtering, trace, history paging, and await behavior have unit and real-PostgreSQL tests.
- Lookup never chooses ambiguously across driver modes; list cursors are stable and bounded.
- Trace exposes commands, events, attempts, coordinator progression, causation, and unresolved work without fabricating transient application events.
- Replay/live settled projections agree for all four e2e scenarios.
- Database-free tests exercise ordinary workers, staged outputs, plan determinism/transition simulation, and coordinator mixed outcomes.
- Observer panics never affect execution and observations contain no payload/state/token data.
- README/package docs explain migration, direct/plan/coordinator roles, examples, PostgreSQL test configuration, and deployment splits.
- Full PostgreSQL suite, race detector, vet, and examples pass.

## Completion evidence

- `inspection.go` and the store inspection queries implement bounded get, strict cross-mode lookup, literal-prefix/status/time/metadata filters, opaque stable cursor pagination, and lease-free polling await. Transaction-scoped reads see their own writes; awaiting through an open caller transaction is rejected.
- `Trace` now runs its fixed query set under a repeatable-read snapshot, folds the complete bounded journal, overlays safe live delivery materializations, and exposes events, causation, plan diagnostics, command waits/retries/leases, attempts, coordinator state/inbox/delivery, and execution result/failure.
- All four real-PostgreSQL examples compare retained-journal replay against live execution, command, and coordinator projections. This found and fixed terminal coordinator inbox advancement and persisted result-reference gaps.
- `flowtest` invokes the production worker decision recorder, commit function, plan recorder, and coordinator recorder through a private internal bridge. It supports dependencies, staged outputs, recursive direct trees, plan prefix simulation/determinism, mixed coordinator outcomes, and command ceilings without PostgreSQL.
- Observations use a bounded, non-blocking adapter started by `Run`; queued calls contain only the safe `Observation` shape, panics are isolated, overflow is counted, and `New` still starts no goroutine.
- README and package documentation cover migration, the smallest path, driver selection, all four runnable examples, PostgreSQL test configuration, inspection, guarantees, and split deployment roles.
- `go vet ./...`, `git diff --check`, the full real-PostgreSQL suite, and `go test -race ./...` pass.
