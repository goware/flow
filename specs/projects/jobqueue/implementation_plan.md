---
status: complete
---

# Implementation Plan: jobqueue

## Phases

- [ ] Phase 1: Establish root contracts, validation, errors, observations, PostgreSQL backend foundations, migration engine, and real-PostgreSQL test harness.
- [ ] Phase 2: Implement the raw MessageQueue, administration, leases, settlement, dead-lettering, retention, notifications, and conformance suite.
- [ ] Phase 3: Implement job lanes, job/attempt/dispatch persistence, enqueue identity, state transitions, claim fencing, cancellation, retry, and reconciliation.
- [ ] Phase 4: Implement WorkerPool execution, registered-kind routing, lease renewal, finalizers, shutdown, capability reporting, and milestone-1 integration/fault tests.
- [ ] Phase 5: Implement workflow graph construction, static DAG persistence, dependency resolution, run outcomes, cancellation, history, and inspection.
- [ ] Phase 6: Implement deterministic dynamic workflow mutation, fan-out, joins, workflow crash recovery, and workflow contention/scale tests.
- [ ] Phase 7: Implement EventStore streams, optimistic append, global allocation, safe reads, checkpoints, projections, and allocator failure/concurrency tests.
- [ ] Phase 8: Implement EventBus topics, subscriptions, immutable publication, filters, atomic fan-out, leasing, settlement, and retention.
- [ ] Phase 9: Implement EventBus dead-letter/redrive/replay, store-backed publication, ordered `ExecuteAtomic` composition, and cross-component transaction tests.
- [ ] Phase 10: Complete migration compatibility fixtures, multi-component fault injection, adversarial contention benchmarks, operational guidance, examples, and final conformance hardening.

## Execution Notes

- Phase 3 establishes the internal ordered executor for the raw-message and job subset; Phase 9 extends that existing executor with EventBus and EventStore categories when it exposes the complete public `ExecuteAtomic` surface.
- Phase plans use functional-spec §20 as milestone exit criteria: Phase 4 closes §20.1, Phase 6 closes §20.2, and Phase 9 closes §20.3. Later hardening must preserve those completed criteria.
