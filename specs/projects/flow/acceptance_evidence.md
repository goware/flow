---
status: complete
recorded_at: 2026-07-29
---

# Flow acceptance evidence

This matrix maps every acceptance statement in functional specification §17 to executable evidence for the smaller Flow API. PostgreSQL tests use a real PostgreSQL 17 server and an isolated schema. The four example tests call the same scenario functions as their runnable programs; Flow itself is never mocked.

## Public surface (§17.1)

| Criterion | Evidence |
|---|---|
| All four `Execute` forms mean durable asynchronous execution | `TestExecutionStartsAndEventEmit`, `TestRuntimeExecutesDirectCommand`, `TestPlanDynamicFanOutJoinEndToEnd`, `TestCoordinatorHistoricalDeliveryRetryOutcomesAndCompletion`; worker invocation occurs only in `Runtime.Run`. |
| One in-execution command verb; no compatibility aliases | `TestGenericAPIMisuseDoesNotCompile`, `TestDecisionBufferCoalescesAndPoisonsConflicts`, and the recorded `go doc github.com/goware/flow` surface audit. |
| Staged and external application-event APIs are distinct | `TestStagedEventsAreRecordedDeterministically`, `TestStagedEventDefectsPoisonDecisions`, `TestExternalEventIngressIsRejectedInsideAttempt`, `TestExecutionStartsAndEventEmit`; the removed-symbol audit finds no `Publish`. |
| Events are unversioned and `EventRef` is sealed | `TestDefinitionIdentityAndBinding`, `TestDefinitionValidation`, compile-contract tests, migration/schema constraint tests, and the public documentation audit. |
| `Node[R]` preserves typing and its modifier/read surface | `TestPlanRecorderValidatesTopologyWithoutDatabase`, `TestPlanRecorderReadAvailabilityAndDeterministicFingerprint`, `TestDecisionBufferRejectsInvalidOptions`, negative compiler fixtures. |
| One `Outcome[R]` vocabulary | `TestResultOfAndOutcomeOfEnforceSnapshot`, `TestPlanFailureBranchAndWorkerOutcome`, `TestRunCoordinatorHandlesMixedOutcomes`, plus the removed-symbol audit. |
| Removed advanced/overlapping APIs are absent | Public `go doc` and repository scans verify no `AfterAny`, plan retry override, `Command.Done`, external issue, exact type/key lookup, configurable jitter, or public lease option. |
| Retry surface is `WithRetry`, `RetryFor`, and `Attempts` | `TestRetryPolicyPublicBuilders`, `internal/retry` builder/validation/decision/canonical-round-trip tests. |
| Coordinator terminality is method-based | `TestCoordinatorTerminalDecision`, `TestCoordinatorHistoricalDeliveryRetryOutcomesAndCompletion`, agent E2E, and scope-poisoning tests. |
| `WithCommit` is the sole application-write hook | `TestRunWorkerCommitAndDirectUseProductionDecisionRecorder`, `TestRuntimeRetriesPermanentTimeoutAndCommit`, `TestCommandFaultBoundariesRecoverWithoutDuplicateProgress`, and the public surface audit. |

## Execution behavior (§17.2)

| Criterion | Evidence |
|---|---|
| Direct work needs no orchestration and supports another replica | `TestRuntimeExecutesDirectCommand`, `TestDirectExampleEndToEnd`, `TestRuntimeCapacityLeaseRenewalAndTakeover`. |
| Worker child creation and membership closure are atomic | `TestRuntimeStagesDelayedChildrenAtomically`, `TestCommandFaultBoundariesRecoverWithoutDuplicateProgress`, fan-out E2E trace/history assertions. |
| Worker/coordinator staged events share their fenced decision transaction | `TestWorkerStagedEventsSettleAtomicallyWithChildrenAndCommit`, `TestCoordinatorStagedEventCommitsWithTerminalTransition`, `TestWorkerStagedEventSatisfiesExactPlanWait`, flowtest staged-event tests, agent E2E trace/history assertions. |
| Plans are pure, monotonic, exact-keyed, and idempotently reconciled | `TestPlanRecorderValidatesTopologyWithoutDatabase`, `TestPlanLazyFactsAndDeterminismFailure`, `TestPlanSimulationAndDeterminism`, `TestPlanReconcilerRollbackLeavesDirtyForTakeover`. |
| Missing dependency keys fail as plan defects | `TestPlanRecorderValidatesTopologyWithoutDatabase`, `TestPlanDefectFailsDurablyWithoutRunningWorker`. |
| Every terminal command yields an available typed outcome | `TestPlanFailureBranchAndWorkerOutcome`, `TestResultOfAndOutcomeOfEnforceSnapshot`, `TestPlanImmediateSkipReconcilesFailureBranchInOneRevision`. |
| Exact keys and both emit/wait orders work | `TestPlanAwaitPublishBeforeDeclareAndWithinExpiry`, `TestExternalMonitorExampleEndToEnd`; keyed facts are also covered by plan recorder and flowtest simulation tests. |
| `Within` starts after command dependencies and late facts do not resurrect expiry | `TestPlanAwaitPublishBeforeDeclareAndWithinExpiry`, `TestWithinLateFactRemainsHistoryWithoutResurrectingWait`. |
| `OnOutcome` observes every terminal state exactly once | `TestCoordinatorHistoricalDeliveryRetryOutcomesAndCompletion`, `TestRunCoordinatorHandlesMixedOutcomes`, durable agent E2E. |
| Coordinator completion is atomic and invalid combinations poison the decision | `TestCoordinatorTerminalDecision`, `TestDecisionBufferCoalescesAndPoisonsConflicts`, coordinator fault/retry and agent E2E assertions. |
| Accepted retry timing remains stable | `TestConcurrentStartDefaultsAndCommandCeiling`, `TestRetryPolicyPublicBuilders`, retry canonical round-trip/decision tests, coordinator replay hash assertions in `TestExecutionStartsAndEventEmit`. |

## PostgreSQL and distribution (§17.3)

| Criterion | Evidence |
|---|---|
| All tables are `flow_`-prefixed in the application database | `TestMigrateAndCheckSchema`, `TestSchemaConstraints`; schema audit counts exactly nine tables. |
| Claims are capacity-bounded, skip locked work, and release connections before handlers | `TestClaimSkipsLockedRowsAndUnhandledBacklog`, `TestClaimProbeQueryPlan`, `TestRuntimeReleasesDatabaseConnectionBeforeWorker`, queue concurrency tests. |
| Production command lease is fixed at 60 seconds | Public `go doc` has no lease option; expiry/renewal/takeover tests use only the unexported in-package seam. |
| Notifications are hints and poll-only remains correct | `TestNotificationHintsCommitButDoNotRollback`, `TestDistributedNotificationAndReconnectCatchUp`, examples and schedulers exercised with notification and polling configurations. |
| Stale leases cannot settle Flow or application state | `TestRuntimeCapacityLeaseRenewalAndTakeover`, `TestSettlementOutageRecoversByLeaseExpiry`, `TestCommandFaultBoundariesRecoverWithoutDuplicateProgress`. |
| Caller transactions obey Flow-first and ascending execution order | `TestCallerOwnedTransactionCommitAndRollback`, `TestTransactionExecutionOrdering`, `internal/store.TestLockOrder`. |
| External facts work in caller transactions without plan registration | `TestCallerOwnedTransactionCommitAndRollback`, `TestExternalMonitorExampleEndToEnd`, plan-dirty takeover tests. |
| Split and rolling deployments work | `TestRuntimeRollingVersionLeavesUnknownWorkUnclaimed`, command/coordinator lease takeover tests, publisher/runtime separation in monitor E2E, distributed notification tests. |

## History and operations (§17.4)

| Criterion | Evidence |
|---|---|
| Every command has creation and exactly one terminal journal entry | `TestJournalAllocationGapFreeAndHistory`, `TestSchemaConstraints`, command runtime/fault suites, all example E2E histories. |
| Replay reconstructs topology, attempts, facts, and terminal state | `internal/replay.TestFoldInitialProjectionAndValidation`, replay/live conformance in execution tests and all example E2E tests. |
| `Trace` exposes graph and operational causes | `TestExecutionInspectionAndStablePagination`, command retry tests, plan/fan-out/monitor/agent E2E trace assertions. |
| Applications use durable `ExecutionID`; listing is operational | `TestExecutionStartsAndEventEmit`, `TestExecutionInspectionAndStablePagination`, `TestTransactionScopedInspectionAndAwait`; public surface has no exact historical type/key lookup. |
| Coordinator retry policy remains durable but not configurable | projection/replay hash assertions in `TestExecutionStartsAndEventEmit`, schema constraints, and public `go doc` audit. |
| Four real-PostgreSQL examples pass | `TestDirectExampleEndToEnd`, `TestFanOutExampleEndToEnd`, `TestExternalMonitorExampleEndToEnd`, `TestDurableAdaptiveAgentExampleEndToEnd`. |

## Release gates

The final smaller-API verification run completed with PostgreSQL integration enabled:

```text
go test ./...
go test ./... -count=3
go test -race ./...
go vet ./...
git diff --check
```

Targeted query-plan and workload evidence additionally runs `TestClaimProbeQueryPlan`, `TestDirtyPlanAndEventSnapshotQueryPlans`, `BenchmarkClaimProbeUnhandledHead10K`, `BenchmarkPlanReconciliation`, and `BenchmarkCoordinatorSparseOutcomeScan10K`. Repository scans verify the removed symbols and durable columns are absent, and the migration inventory contains exactly nine `flow_` tables.
