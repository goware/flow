---
status: complete
recorded_at: 2026-08-04
---

# Command/event benchmark evidence

The command-only reduction removes the state-machine probe, inbox scan, serialized state row, delivery lease, and second scheduler. Retained regression workloads in `hardening_benchmark_test.go` are:

- `BenchmarkExecutionIngressNotification`: event ingress with polling versus notification hints;
- `BenchmarkGetEventValueLookup256`: O(1) in-memory lookups across a maximum-size declared input set;
- `BenchmarkEventSnapshotMaterialization256`: claim loading for 256 maximum-size event payloads;
- `BenchmarkInspection100Commands`: bounded history and trace over 100 commands;
- `TestJournalGrowthMeasurement100Commands`: semantic journal growth for 100 commands.

Run them against PostgreSQL:

```bash
FLOW_TEST_DATABASE_URL='postgres://postgres@localhost/postgres?sslmode=disable' \
  go test -run '^$' \
  -bench 'Benchmark(ExecutionIngressNotification|GetEventValueLookup256|EventSnapshotMaterialization256|Inspection100Commands)$' \
  -benchtime=1x -count=1 .
```

These are regression workloads, not service-level objectives. The structural assertions are bounded wait count/payload size, one batched event-input query per claim, no connection held during worker execution, and only command plus maintenance polling while idle. Fresh machine-specific timing should be recorded only after running the benchmark suite on the target environment.

One-iteration completion-pass results on Linux/amd64, Intel Core Ultra 7 255H:

| Workload | Result |
|---|---:|
| ingress, polling only | 5.86 ms/op |
| ingress, notification hint | 6.80 ms/op |
| `GetEventValue` lookup with 256 inputs | 3.00 µs/op, 2.4 KiB/op |
| claim materialization, 256 × 64 KiB inputs | 386.82 ms/op, 390.3 MiB allocated/op |
| history, 100 commands | 1.11 ms/op |
| trace, 100 commands | 6.96 ms/op |

The maximum-payload case is deliberately adversarial: it carries 16 MiB of canonical event data before JSON/driver decoding overhead. The measured allocation volume makes the documented guidance material—large joins should use a join tree or stable application references, and deployments permitting maximum-size joins must budget worker concurrency accordingly. Repeated samples and peak-heap profiling are required before treating these smoke values as capacity guidance.
