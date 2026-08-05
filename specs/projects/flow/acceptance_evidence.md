---
status: complete
recorded_at: 2026-08-04
---

# Flow acceptance evidence

This matrix records evidence for the command/worker/event architecture. PostgreSQL tests create isolated schemas on a real server; example tests invoke the same scenario functions as their binaries.

## Public model and composition

| Contract | Evidence |
|---|---|
| Commands are the only execution starts and workers the only registrations | `definitions_test.go`, `execute_test.go`, `TestRemovedPublicAPINamesStayRemoved` |
| `Execute` and inspection return one `Execution` snapshot type; `Created` marks acceptance | execution and inspection tests |
| `Execute`/`Emit` are worker-only and `Node` is non-generic | `decision_test.go`, `flowtest/engine_test.go` |
| Typed successful lookup is trace-only | `TestResultOfEnforcesSnapshot`, inspection/replay tests |
| Dynamic two-stage fan-out/fan-in needs no second state machine | `examples/fanout/main.go`, `examples/fanout/main_test.go` |
| Bounded self-composition implements the agent loop | `examples/agent/main.go`, `examples/agent/main_test.go` |
| External gating supplies typed payloads | `examples/monitor/main.go`, event-gate tests |
| Active workers can deliver detached events to a separate gated execution | `TestDeliverFromActiveWorker` |
| Delivery preserves target-local identity/lifecycle rules and external-emit gate behavior | `TestDeliverIdentityLifecycleAndGateParity` |
| Independent producer executions satisfy a command-owned exact AND join | `TestDeliverMultiProducerFanIn` |

## Exact event inputs

| Contract | Evidence |
|---|---|
| Event-before/after declaration resolves exact name/key waits | `TestDirectRootWaitsForExactApplicationEvent` and store tests |
| Same-decision staged event supplies a sub-command gate | `TestWorkerEventSatisfiesNewChildGateInSameDecision` |
| Multiple waits are AND conditions and duplicate declarations normalize | decision and `flowtest` gate tests |
| `GetEventValue` is declared-only, typed, repeatable, and decision-poisoning on misuse | `flowtest` declared-input tests and runtime event-gate tests |
| Retry/lease takeover receives the same immutable gated payload and satisfying position | `TestRuntimeCapacityLeaseRenewalAndTakeover` |
| Claim input materialization is bounded and detached from the connection | `BenchmarkEventSnapshotMaterialization256`, runtime claim tests |
| 256 waits are accepted and 257 rejected | `command_limit_test.go` |
| Trace links waits to satisfying journal positions | inspection/trace tests |
| Wait expiry and delay remain independent | `TestWaitCanExpireWhileInitialDelayIsPending` |
| Late events cannot resurrect expired waits; optional open gates terminalize at the execution deadline | event-gate expiry and optional-liveness tests |
| Required producer failure cancels a gated join without invoking it | `TestRequiredChildFailureCancelsGatedJoin` |
| New events cannot reopen a terminal execution | `TestApplicationEventCannotReopenTerminalExecution` |
| Cross-decision command-key reuse is a hard conflict | `TestCrossDecisionCommandKeyReuseIsAConflict` |

## Durability and operations

| Contract | Evidence |
|---|---|
| Fenced settlement, retries, takeover, cancellation, and reduced fail-fast | command runtime/store integration tests |
| Journal replay matches live command-only projections | `internal/replay` and inspection conformance tests |
| Caller transactions preserve lock/commit ownership | transaction integration tests |
| Application writes and delivered events commit or roll back together | `TestDeliverInCallerTransaction` |
| Notifications are hints and polling recovers | distributed notification/reconnect tests |
| Baseline migrations own exactly six tables | migration inventory and schema-constraint tests |
| Removed public/runtime/storage symbols do not remain | compile contract plus repository scans |

## Verification commands

The completion pass runs `gofmt`, `go vet ./...`, database-free and PostgreSQL-backed `go test -count=1 ./...`, `go test -race -count=1 ./...`, example package tests, migration inventory checks, `git diff --check`, and removed-symbol scans. Benchmark workloads remain in `hardening_benchmark_test.go` for repeatable local measurement.
