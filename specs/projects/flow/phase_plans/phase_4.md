---
status: complete
historical: true
superseded_by: ../plans/2-remove-plan.md
---

# Phase 4: Distributed Command Runtime

> Historical delivery record. The current runtime contract is in `../components/runtime.md`.

## Overview

Deliver the first complete vertical slice: a direct command is durably created, claimed by any compatible replica, invoked without holding a database connection, renewed or taken over through a fenced lease, and settled to retry or exactly one terminal outcome. Runtime registration and lifecycle become operational, declared commit functions join successful settlement, polling alone remains sufficient for progress, and graceful shutdown never acknowledges unfinished work. Minimal `Trace`, attempt-aware replay, observations, fault points, and the direct background-command example ship with the runtime rather than arriving later.

## Steps

1. Implement the runtime-local registry and lifecycle state machine: validated worker registration, immutable freeze at `Run`, single-run enforcement, API-only use, idempotent `Stop`, configurable worker capacity/queues, lease, poll interval, shutdown grace, and stable replica identity.
2. Add store operations for bounded candidate probes and execution-first, no-wait `SKIP LOCKED` claims that revalidate exact registered definitions, execution state/deadline, schedule, and elapsed retry bounds before atomically recording `AttemptStarted` and a fresh fence.
3. Implement worker preparation and invocation: decode canonical arguments with the registered codec, construct accepted `CommandInfo`, derive the earliest handler deadline, recover panics, classify outcomes, and hold no PostgreSQL connection or row lock while application code runs.
4. Implement fenced conclusion and settlement for the Phase 4 surface: canonical typed result, `AttemptConcluded`, command success/failure event, command and queue projection updates, plan dirtying, direct-execution completion/failure, commit-function atomicity, persisted retry decisions, and ambiguous-commit recovery by stable identity.
5. Implement active-lease tracking, batched renewal, lease-loss cancellation, expired-lease recovery, and graceful interruption/release that consumes no retry budget and remains recoverable when PostgreSQL is unavailable.
6. Implement capacity-bounded poll-first scheduling and an in-process wake hub. Polling is the correctness path; optional PostgreSQL notification listening may be added only if it remains a latency hint and does not delay the phase's direct vertical slice.
7. Extend journal codecs and the replay reducer for attempt starts/conclusions, command terminal outcomes, and direct execution terminality; add minimal `Trace` exposing current status, command state, accepted timing, attempts, and waiting/running diagnostics needed by the example and tests.
8. Thread Phase 4 observer events and the documented claim, handler, settle, renewal, and shutdown fault points through the paths as they are introduced.
9. Add the runnable direct background-command example backed by real PostgreSQL and share its scenario with an end-to-end test. Exercise multiple replicas, retry, timeout, declared commit, crash/takeover, and database projection/history assertions.
10. Record claim-path `EXPLAIN` evidence at representative scales and benchmark free-capacity bounds, same-execution bursts, and an adversarial unhandled-kind distribution before closing the phase.

## Tests

- Registration rejects duplicates and invalid workers; `Run` freezes registration, may run once, and `Stop` is concurrent and idempotent.
- Claims never exceed immediately free capacity, wait on neither execution nor queue locks, filter exact registered name/version pairs, and do not let an unhandled lane head starve handled work.
- A committed `AttemptStarted` always precedes invocation; handler execution holds no database connection; stable IDs and accepted database timing reach `Work.Info`.
- Success records result, attempt conclusion, one command-success event, and one execution-success event atomically; retryable, permanent, panic, timeout, and exhausted outcomes have the specified history and counters.
- A declared commit function receives only durable args/result/info; its application write and successful settlement commit together, while its error rolls both back and enters ordinary retry handling.
- Lease renewal preserves ownership; stale fences cannot settle; takeover after expiry completes work; interruption and lease loss do not consume retry budget or reset `BudgetStartedAt`.
- Poll-only operation processes all work, multiple replicas distribute claims safely, and graceful shutdown either settles returned work or leaves it durably reclaimable.
- Minimal `Trace`, `History`, replay, and direct database queries agree on every final projection and journal position in the direct example and fault variants.

## Completion evidence

- The direct command example is runnable and is shared with a real-PostgreSQL end-to-end test.
- Real-PostgreSQL tests cover exact-version rolling deployment, bounded global and queue-lane concurrency, fair lane selection, batched same-execution claims, renewal, takeover, active cancellation, shutdown interruption, settlement outage, commit-function atomicity, and ambiguous commit recovery.
- The 10K, 1M, and 10M adversarial claim plans and the reusable claim benchmarks are recorded in `benchmark_evidence/phase_4_claim.md`.
- Polling is the complete correctness path. Transactional PostgreSQL notification hints are deliberately deferred to the Phase 9 hardening pass, where they remain disableable without changing correctness.
- `go test ./...`, `go test -race ./...`, `go vet ./...`, and the direct example end-to-end test pass; public-package statement coverage is 83.1% at phase completion.
