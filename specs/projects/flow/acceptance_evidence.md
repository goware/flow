---
status: complete
recorded_at: 2026-07-26
---

# Milestone 1 acceptance evidence

This matrix maps every acceptance statement in functional specification §22 to executable evidence. `Test*` names run under `go test ./...`; PostgreSQL tests use an isolated schema in a real server. The four example tests call the same scenario functions as their runnable binaries. Architecture contracts are cited only where the property is intentionally enforced by the Go type system or by an omitted capability rather than a runtime branch.

| Criterion | Evidence |
|---|---|
| AC-01 | `TestExecutionStartsIssueAndPublish`, `TestExecutionInspectionAndStablePagination`, `TestIngressCancellationAndTerminalIdempotency` |
| AC-02 | `TestDefinitionIdentityAndBinding`, plus every direct `runtime` client call in `execute_test.go` |
| AC-03 | `TestDefinitionIdentityAndBinding` concurrently executes independently bound copies and checks the original remains unbound |
| AC-04 | `TestDefinitionIdentityAndBinding` verifies replacement binding without shared mutation |
| AC-05 | `TestDefinitionIdentityAndBinding`, `TestRuntimeAndIngressValidation` |
| AC-06 | `TestDefinitionIdentityAndBinding`, `TestCallerOwnedTransactionCommitAndRollback` |
| AC-07 | `TestExecutionStartsIssueAndPublish`, `TestConcurrentStartDefaultsAndCommandCeiling`, replay assertions in all example e2e tests |
| AC-08 | `TestConcurrentStartDefaultsAndCommandCeiling` checks the persisted ceiling and unchanged creation-time command defaults |
| AC-09 | `TestRuntimeExecutesDirectCommand`, `TestDirectExampleEndToEnd` |
| AC-10 | `TestExecutionStartsIssueAndPublish`, `TestPlanDynamicFanOutJoinEndToEnd` |
| AC-11 | `TestPlanDefectFailsDurablyWithoutRunningWorker`; `TestExecutionStartsIssueAndPublish` proves no inline evaluation |
| AC-12 | `TestExecutionStartsIssueAndPublish`, `TestCoordinatorHistoricalDeliveryRetryOutcomesAndCompletion` |
| AC-13 | `TestConcurrentStartDefaultsAndCommandCeiling`, `TestCommandCeilingRejectsWorkerPlanAndCoordinatorBatchesAtomically` |
| AC-14 | `TestCommandCeilingRejectsWorkerPlanAndCoordinatorBatchesAtomically`, `TestDecisionBufferCoalescesAndPoisonsConflicts`, plan repeated-evaluation tests |
| AC-15 | `TestCommandCeilingRejectsWorkerPlanAndCoordinatorBatchesAtomically`, `TestConcurrentStartDefaultsAndCommandCeiling` |
| AC-16 | `TestRuntimeStagesEventsAndDelayedChildrenAtomically`, `TestDirectExampleEndToEnd` |
| AC-17 | `TestRuntimeFailFastCancelsQueuedSiblings` and the fail-fast/settle-all cases in `TestRuntimeStagesEventsAndDelayedChildrenAtomically` |
| AC-18 | `TestDirectExampleEndToEnd`, `TestFanOutExampleEndToEnd`, `TestExternalMonitorExampleEndToEnd`, `TestDurableAdaptiveAgentExampleEndToEnd`; release evidence also records all four `go run` executions |
| AC-19 | `TestGenericAPIMisuseDoesNotCompile` runs negative compiler fixtures for command arguments, worker results, and event payloads |
| AC-20 | `TestRuntimeRetriesPermanentTimeoutAndCommit`, `TestFanOutExampleEndToEnd`, replay/live conformance assertions |
| AC-21 | oversized arguments and published events in `TestRuntimeAndIngressValidation`; oversized results in `TestRuntimeRetriesPermanentTimeoutAndCommit` |
| AC-22 | `TestExecutionStartsIssueAndPublish`, `TestRuntimeStagesEventsAndDelayedChildrenAtomically`, and command-created counts plus replay in fan-out/agent e2e tests |
| AC-23 | retuned `Issue` in `TestExecutionStartsIssueAndPublish`; changed queue/retry/timeout defaults in `TestConcurrentStartDefaultsAndCommandCeiling` |
| AC-24 | `TestRegistrationValidation`, `TestRuntimeRollingVersionLeavesUnknownWorkUnclaimed`, exact-version plan/coordinator registry tests |
| AC-25 | `TestPlanRecorderValidatesTopologyWithoutDatabase`, `TestPlanDefectFailsDurablyWithoutRunningWorker`, `TestDecisionBufferCoalescesAndPoisonsConflicts` |
| AC-26 | ordered attempt histories in `TestRuntimeRetriesPermanentTimeoutAndCommit` and `TestRuntimeCooperativeShutdownIsRetryableAndBudgetNeutral` |
| AC-27 | `TestRuntimeCooperativeShutdownIsRetryableAndBudgetNeutral`, `TestRuntimeCapacityLeaseRenewalAndTakeover`, retry reducer tests |
| AC-28 | `TestRuntimeExecutesDirectCommand`, `TestRuntimeCapacityLeaseRenewalAndTakeover`, `TestRuntimeCooperativeShutdownIsRetryableAndBudgetNeutral` |
| AC-29 | `TestRetryPolicyPublicBuilders`, `internal/retry` decision suite, `TestRuntimeRetriesPermanentTimeoutAndCommit` |
| AC-30 | timeout and deadline cases in `TestRuntimeRetriesPermanentTimeoutAndCommit`, `TestRuntimeDeadlineAndRegistrationLifecycle`, retry decision tests |
| AC-31 | `TestRuntimeStagesEventsAndDelayedChildrenAtomically`, `TestCommandFaultBoundariesRecoverWithoutDuplicateProgress` |
| AC-32 | `TestDecisionBufferRejectsInvalidOptions`, `TestRuntimeStagesEventsAndDelayedChildrenAtomically`, agent delayed-turn e2e |
| AC-33 | delayed-child assertions in `TestRuntimeStagesEventsAndDelayedChildrenAtomically`; delayed turn in agent e2e |
| AC-34 | `TestDecisionBufferCoalescesAndPoisonsConflicts`, settlement rollback matrix in `TestCommandFaultBoundariesRecoverWithoutDuplicateProgress` |
| AC-35 | commit success/failure in `TestRuntimeRetriesPermanentTimeoutAndCommit`, every settle boundary in `TestCommandFaultBoundariesRecoverWithoutDuplicateProgress`, database-free invocation in `TestRunWorkerCommitAndDirectUseProductionDecisionRecorder` |
| AC-36 | terminal-history assertions in `TestRuntimeRetriesPermanentTimeoutAndCommit` and `TestCommandFaultBoundariesRecoverWithoutDuplicateProgress` |
| AC-37 | Coordinator API has no commit option; compile-time surface is exercised by `TestErasedCoordinatorRegistration` and coordinator harness/e2e tests |
| AC-38 | overlap rejection in `TestRegistrationValidation`; mixed typed outcomes in `TestCoordinatorHistoricalDeliveryRetryOutcomesAndCompletion` and agent e2e |
| AC-39 | `TestCoordinatorHistoricalDeliveryRetryOutcomesAndCompletion`, `TestDurableAdaptiveAgentExampleEndToEnd` |
| AC-40 | `TestDurableAdaptiveAgentExampleEndToEnd` |
| AC-41 | `TestCoordinatorLeaseTakeoverAcrossReplicas`, `TestRuntimeCapacityLeaseRenewalAndTakeover`, coordinator rollback/retry assertions in agent and coordinator e2e tests |
| AC-42 | `TestCoordinatorHistoricalDeliveryRetryOutcomesAndCompletion`, `TestCoordinatorLeaseTakeoverAcrossReplicas`, agent replay/live equality |
| AC-43 | decision rollback cases in `TestRuntimeStagesEventsAndDelayedChildrenAtomically`, `TestCommandFaultBoundariesRecoverWithoutDuplicateProgress`, `TestCommandCeilingRejectsWorkerPlanAndCoordinatorBatchesAtomically` |
| AC-44 | `TestDecisionBufferCoalescesAndPoisonsConflicts`, `TestRuntimeStagesEventsAndDelayedChildrenAtomically` |
| AC-45 | `TestRuntimeStagesEventsAndDelayedChildrenAtomically`, `TestPlanDynamicFanOutJoinEndToEnd`, agent trace assertions |
| AC-46 | `TestPlanDynamicFanOutJoinEndToEnd`, `TestPlanRecorderReadAvailabilityAndDeterministicFingerprint` |
| AC-47 | `TestResultOfAndOutcomeOfEnforceSnapshot`, fan-out worker and e2e tests |
| AC-48 | `TestResultOfAndOutcomeOfEnforceSnapshot`, `TestPlanFailureBranchAndWorkerOutcome` |
| AC-49 | `TestResultOfAndOutcomeOfEnforceSnapshot`, `TestPlanDynamicFanOutJoinEndToEnd` |
| AC-50 | publisher/runtime separation in external-monitor scenario; plan dirty assertions in `TestExecutionStartsIssueAndPublish` and plan reconciliation tests |
| AC-51 | `TestPlanReconcilerRollbackLeavesDirtyForTakeover`, `TestDirtyPlanAndEventSnapshotQueryPlans` |
| AC-52 | dirty-trigger coalescing in `TestPlanLazyFactsAndDeterminismFailure` and database-free plan-queue harness coverage |
| AC-53 | compact reconciliation bodies asserted by `TestPlanImmediateSkipReconcilesFailureBranchInOneRevision`; `TestJournalGrowthMeasurement100Commands` |
| AC-54 | `TestPlanDynamicFanOutJoinEndToEnd`, `TestPlanImmediateSkipReconcilesFailureBranchInOneRevision`, replay equality |
| AC-55 | `TestPlanLazyFactsAndDeterminismFailure` |
| AC-56 | plan batch atomicity in `TestPlanDynamicFanOutJoinEndToEnd`, `TestCommandCeilingRejectsWorkerPlanAndCoordinatorBatchesAtomically`, plan fault rollback test |
| AC-57 | `TestPlanImmediateSkipReconcilesFailureBranchInOneRevision`, `TestPlanInitialScheduleBeyondDeadlineExpiresInFixedPoint` |
| AC-58 | `TestPlanAwaitPublishBeforeDeclareAndWithinExpiry`, `TestExternalMonitorExampleEndToEnd` |
| AC-59 | `TestExternalMonitorExampleEndToEnd`, `TestCallerOwnedTransactionCommitAndRollback` |
| AC-60 | external-monitor scenario and `TestPlanReconcilerRollbackLeavesDirtyForTakeover`; publishers use ingress without plan registration |
| AC-61 | invalid declaration in `TestPlanRecorderValidatesTopologyWithoutDatabase`; persisted anchor/expiry in `TestPlanAwaitPublishBeforeDeclareAndWithinExpiry` |
| AC-62 | `TestPlanAwaitPublishBeforeDeclareAndWithinExpiry` |
| AC-63 | early winner in `TestPlanAwaitPublishBeforeDeclareAndWithinExpiry`; late retained loser in `TestWithinLateFactRemainsHistoryWithoutResurrectingWait` |
| AC-64 | `TestPlanFailureBranchAndWorkerOutcome`, `TestPlanImmediateSkipReconcilesFailureBranchInOneRevision` |
| AC-65 | `TestPlanDynamicFanOutJoinEndToEnd`, `TestPlanMissingFactWaitsUntilExecutionDeadline`, replay/live equality |
| AC-66 | `TestPlanFailureBranchAndWorkerOutcome` explicitly consults the permanently unavailable success result while its failure branch settles |
| AC-67 | `TestPlanMissingFactWaitsUntilExecutionDeadline` |
| AC-68 | valid and invalid reads in `TestPlanRecorderReadAvailabilityAndDeterministicFingerprint` and `TestPlanDefectFailsDurablyWithoutRunningWorker` |
| AC-69 | `TestPlanRecorderReadAvailabilityAndDeterministicFingerprint`, `TestPlanFailureBranchAndWorkerOutcome` |
| AC-70 | forward/missing dependency cases in `TestPlanRecorderValidatesTopologyWithoutDatabase` and durable defect test |
| AC-71 | `TestPlanDefectFailsDurablyWithoutRunningWorker`, `TestPlanLazyFactsAndDeterminismFailure`, plan fault takeover test |
| AC-72 | The sealed `Plan` surface exposes facts/results/outcomes but no timing API; compile-time contract plus plan determinism tests enforce the boundary |
| AC-73 | `TestPlanSimulationAndDeterminism`, `TestPlanLazyFactsAndDeterminismFailure`, `TestPlanRecorderReadAvailabilityAndDeterministicFingerprint` |
| AC-74 | `TestPlanSimulationAndDeterminism`, `TestPlanRecorderReadAvailabilityAndDeterministicFingerprint` |
| AC-75 | `BenchmarkPlanReconciliation`, `TestCommandCeilingRejectsWorkerPlanAndCoordinatorBatchesAtomically`, dirty-plan query-plan evidence |
| AC-76 | `TestCommandFaultBoundariesRecoverWithoutDuplicateProgress`, `TestRuntimeStagesEventsAndDelayedChildrenAtomically`, `TestPlanReconcilerRollbackLeavesDirtyForTakeover` |
| AC-77 | `TestCallerOwnedTransactionCommitAndRollback`; Flow-first ordering is the normative lock-order contract and is mirrored by settle-before-commit-function tests |
| AC-78 | `TestTransactionExecutionOrdering`, `internal/store.TestLockOrder` |
| AC-79 | `TestCallerOwnedTransactionCommitAndRollback`, `TestPlanDefectFailsDurablyWithoutRunningWorker` |
| AC-80 | `TestRuntimeCapacityLeaseRenewalAndTakeover`, `TestSettlementOutageRecoversByLeaseExpiry` |
| AC-81 | publish-before-declare in `TestPlanAwaitPublishBeforeDeclareAndWithinExpiry`; historical event/outcome delivery in `TestCoordinatorHistoricalDeliveryRetryOutcomesAndCompletion` |
| AC-82 | `TestCoordinatorHistoricalDeliveryRetryOutcomesAndCompletion`, agent transition/history assertions |
| AC-83 | execution-local journal allocation in `TestJournalAllocationGapFreeAndHistory`; no global-position public type or API exists |
| AC-84 | `TestExecutionInspectionAndStablePagination`, all example `assertReplayMatchesLive` calls, replay reducer unit tests |
| AC-85 | `TestIngressCancellationAndTerminalIdempotency` |
| AC-86 | keyed idempotency/conflict/cross-version cases in `TestExecutionStartsIssueAndPublish`; staged event conflicts in decision tests |
| AC-87 | terminal-event uniqueness constraints and counts in `TestSchemaConstraints`, command runtime and example tests |
| AC-88 | cross-version conflict in `TestExecutionStartsIssueAndPublish`, exact worker-version selection in `TestRuntimeRollingVersionLeavesUnknownWorkUnclaimed` |
| AC-89 | ingress, claim, settle, plan, coordinator, maintenance, lease, and ambiguous-commit fault suites |
| AC-90 | `TestRuntimeRollingVersionLeavesUnknownWorkUnclaimed`, `TestClaimSkipsLockedRowsAndUnhandledBacklog` |
| AC-91 | `TestExecutionInspectionAndStablePagination`, `TestRuntimeRetriesPermanentTimeoutAndCommit`, all e2e trace assertions |
| AC-92 | `flowtest` worker/plan/coordinator suites run database-free; store, runtime, examples, and migration suites use real PostgreSQL |

## Release gates

The release run uses the repository's configured PostgreSQL integration environment and requires:

```text
go test ./...
go test ./... -count=3
go test -race ./...
go vet ./...
git diff --check
```

The separate Phase 9 benchmark evidence records ordinary-scale plans, workload timings, notification cost, coordinator sparse-history behavior, inspection cost, and journal growth. Phase 4 and Phase 6 evidence retain the opt-in 1M/10M query-plan commands.
