---
status: complete
historical: true
superseded_by: ../plans/2-remove-plan.md
---

# Phase 9: Milestone 1 hardening and release

> Historical delivery record. Current release evidence is in `../acceptance_evidence.md` and `../benchmark_evidence/remove_plan.md`.

## Goal

Turn the complete vertical implementation into release evidence: close the remaining notification/polling and compatibility gaps, exercise every functional-specification §22 invariant at the most appropriate unit/store/runtime/e2e layer, run fault and concurrency matrices, record realistic benchmark/storage evidence, execute every example binary against PostgreSQL, and freeze a reviewed Milestone 1 API.

## Work

1. Implement bounded transactional PostgreSQL notification hints with one reconnecting listener per enabled runtime, catch-up generation after `LISTEN`, a poll-only option, safe payload/version handling, and no correctness dependency on the listener.
2. Audit §22 against existing tests and add focused missing coverage for lifecycle/registry, capacity and connection ownership, rolling definition/event versions, transaction ordering and closed clients, notification commit/rollback/reconnect, maintenance duplication, shutdown interruption, deadlines, error/observation redaction, and database constraints/invariants.
3. Expand fault tests around command, plan, coordinator, renewal, maintenance, and ambiguous commits; always assert durable state after restart rather than local scheduling details.
4. Add/re-run workload and query-plan benchmarks for claim/settle throughput, wide execution contention, lease renewal, coordinator sparse delivery, poll/notify latency and cost, trace/history, journal/WAL growth, and shutdown. Keep large 1M/10M fixtures opt-in and record reproducible commands plus measured development-machine baselines.
5. Run all four example binaries with a real PostgreSQL URL in addition to their shared e2e tests; inspect the resulting `flow_` tables and journal through the automated scenarios.
6. Review exported API/docs against the settled functional spec, remove accidental surface/unsafe diagnostics, update completion evidence and release guidance, and run formatting, vet, race, full PostgreSQL, and repeated stress gates.

## Exit checks

- Every functional-specification §22 acceptance statement is covered by an existing or newly named test, benchmark, example, documented deliberate boundary, or deferred non-M1 item; a matrix records the mapping.
- Notify-enabled and explicitly poll-only replicas both progress all driver modes; commit emits a hint, rollback does not, listener reconnect catch-up closes the gap, and notifications never carry durable work.
- Crash/fault/lease/ambiguous-commit tests leave journal, counters, queue, coordinator inbox, and materialized state internally consistent after a different runtime resumes.
- Direct invariant queries and replay/live comparison pass after randomized and concurrent transitions.
- Required ordinary-scale query plans use the intended indexes; large-scale evidence remains reproducible and recorded; workload results do not reveal an unbounded connection/goroutine/local-queue pattern.
- Each `go run ./examples/{direct,fanout,monitor,agent}` completes against real PostgreSQL.
- `go test ./...`, repeated/stress selections, `go test -race ./...`, `go vet ./...`, and `git diff --check` pass with PostgreSQL integration enabled.
- Overview, functional spec, architecture, components, phase plans, implementation plan, README, and package docs are complete and consistent with the shipped API.
