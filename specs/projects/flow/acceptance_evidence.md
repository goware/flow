---
status: complete
recorded_at: 2026-08-04
---

# Flow acceptance evidence

This matrix records evidence for the command/event/coordinator architecture. PostgreSQL tests create isolated schemas on a real server; the example package tests invoke the same scenario functions as the binaries.

## Public surface

| Contract | Evidence |
|---|---|
| Direct commands and coordinators are the only execution starts | `definitions_test.go`, `execute_test.go`, `compile_contract_test.go`, and the repository removed-symbol scan |
| Workers/coordinators share `flow.Execute` and non-generic `Node` | `decision_test.go`, `definitions_test.go`, `flowtest/engine_test.go` |
| Worker inputs contain only arguments and command info | `definitions_test.go`, `decision_test.go`; `Work` no longer implements `ResultSource` |
| Trace retains typed result/outcome lookup | `TestResultOfAndOutcomeOfEnforceSnapshot`, inspection/replay tests |
| Staged and external event APIs are distinct | staged-event tests and `TestExecutionStartsAndEventEmit` |

## Exact event gates

| Contract | Evidence |
|---|---|
| Direct root waits for exact name/key and ignores mismatches | `TestDirectRootWaitsForExactApplicationEvent` |
| Event and child gate can commit in one worker decision | `TestWorkerEventSatisfiesNewChildGateInSameDecision` |
| Coordinator-staged multiple waits use AND semantics | `TestCoordinatorStagesMultipleEventGatesWithANDSemantics` |
| Wait expiry runs independently of an initial delay | `TestWaitCanExpireWhileInitialDelayIsPending` |
| Duplicate normalization and invalid combinations poison decisions | `TestDecisionBufferNormalizesEventGates`, `TestDecisionBufferRejectsInvalidEventGates`, `flowtest` assertions |
| Event publication is idempotent and transaction-aware | `TestExecutionStartsAndEventEmit`, `TestCallerOwnedTransactionCommitAndRollback`, staged-event conflict tests |

## Coordinators and examples

| Contract | Evidence |
|---|---|
| Retained start/event/outcome delivery, retries, and completion | `TestCoordinatorHistoricalDeliveryRetryOutcomesAndCompletion` |
| Lease takeover and scan progression are durable | coordinator lease and scan-cursor tests |
| Terminal mutation and permanent failure are rejected/fail durably | coordinator terminal tests |
| Dynamic fan-out/fan-in is explicit coordinator state | `examples/fanout/main.go` and its PostgreSQL package tests |
| Event-gated external monitoring needs no coordinator | `examples/monitor/main.go` and its PostgreSQL package tests |
| Direct and durable-agent patterns remain runnable | `examples/direct`, `examples/agent` package tests |

## Failure, fencing, and recovery

| Contract | Evidence |
|---|---|
| Required fail-fast cancels queued siblings | `TestRuntimeFailFastCancelsQueuedSiblings` |
| Running attempts settle after failure; their events survive and new children cancel | `TestRunningAttemptSettlementAfterRequiredFailureHandlesNewChildren` |
| Retry, permanent errors, timeout, and commit hooks are durable | `TestRuntimeRetriesPermanentTimeoutAndCommit` |
| Lease loss/takeover cannot duplicate progression | capacity/takeover, command fault, and settlement outage tests |
| Cancellation concludes only the owned attempt | `TestRuntimeCommandCancellationConcludesOnlyOwnedAttempt` |
| Deadlines and recovery survive faults | runtime deadline and maintenance fault tests |
| Command ceiling rejects complete batches atomically | `TestCommandCeilingRejectsWorkerAndCoordinatorBatchesAtomically` |

## Storage and replay

| Contract | Evidence |
|---|---|
| Migration creates exactly seven expected tables | `TestMigrateAndCheckSchema`, `TestMigrationFSAppliesCompatibleSchema` |
| Only direct/coordinator modes and retained command/status shapes are allowed | `TestSchemaConstraints` |
| Journal allocation is gap-free and history bounded | `TestJournalAllocationGapFreeAndHistory`, public history tests |
| Replay matches retained direct/coordinator semantics | `internal/replay` tests, inspection tests, runtime tests |
| Caller transactions obey execution-first locking | `TestTransactionExecutionOrdering`, `TestLockOrder` |
| Claim probes skip locked rows and unhandled work | `TestClaimSkipsLockedRowsAndUnhandledBacklog` |

## Runtime and operations

| Contract | Evidence |
|---|---|
| Handlers release database connections and capacity is bounded | connection-release, queue-concurrency, and capacity tests |
| Unknown versions remain durable for compatible replicas | `TestRuntimeRollingVersionLeavesUnknownWorkUnclaimed` |
| Notifications are hints and reconnect catches up | notification commit/rollback and reconnect tests |
| Cooperative shutdown is retryable and budget-neutral | `TestRuntimeCooperativeShutdownIsRetryableAndBudgetNeutral` |
| Observers are bounded and panic-isolated | `TestObserverAdapterIsBoundedAndPanicIsolated` |
| Public inspection and pagination are stable | execution inspection and transaction-scoped inspection tests |

## Performance and storage evidence

Retained workload coverage is in `hardening_benchmark_test.go`: notification ingress, sparse coordinator scanning over 10K unrelated events, history/trace over 100 commands, and measured journal growth for a 100-command coordinator decision. The journal-growth test expects 104 semantic rows.

The obsolete workflow-reconciliation benchmark and query-plan fixture were deleted. Claim and coordinator benchmarks remain representative of the two schedulers that ship.

## Verification commands

```bash
go test ./...
FLOW_TEST_DATABASE_URL='postgres://postgres@localhost/postgres?sslmode=disable' go test ./...
go vet ./...
git diff --check
```

Repository scans additionally verify no removed API, runtime mode, dependency storage, journal kind, status, or active documentation claim remains outside intentionally historical specs and the controlling removal plan.
