---
status: complete
recorded_at: 2026-08-04
---

# Command/event/coordinator benchmark evidence

The architecture reduction deletes the background workflow probe, snapshot loader, reconciliation transaction, and command-dependency resolution path. The retained regression workloads are:

- `BenchmarkExecutionIngressNotification`: poll-only versus transactional wake-hint ingress;
- `BenchmarkCoordinatorSparseOutcomeScan10K`: one coordinator scans a 10K unrelated-event prefix;
- `BenchmarkInspection100Commands`: bounded history and trace over a 100-command coordinator declaration;
- `TestJournalGrowthMeasurement100Commands`: 100 staged commands produce 104 semantic journal rows.

Run the benchmarks against the configured PostgreSQL test server:

```bash
FLOW_TEST_DATABASE_URL='postgres://postgres@localhost/postgres?sslmode=disable' \
  go test -run '^$' \
  -bench 'Benchmark(ExecutionIngressNotification|CoordinatorSparseOutcomeScan10K|Inspection100Commands)$' \
  -benchtime=1x -count=1 .
```

These are regression workloads, not service-level objectives. The important structural result is that idle runtimes have only command, coordinator, and maintenance polling; command settlement and event ingress perform no workflow-dirty update or dependency scan.

One-iteration development-machine results on Linux/amd64, Intel Core Ultra 7 255H:

| Workload | Result |
|---|---:|
| ingress, polling only | 5.86 ms/op |
| ingress, notification hint | 5.81 ms/op |
| sparse coordinator scan, 10K events | 5.29 ms/op |
| history, 100 commands | 0.86 ms/op |
| trace, 100 commands | 5.81 ms/op |

The one-iteration values are smoke baselines only; repeated benchmark runs are required before drawing performance conclusions.

The old workflow-reconciliation evidence was removed because its code and schema no longer ship. Historical release documents are explicitly marked and do not define the current product.
