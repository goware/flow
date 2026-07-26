# flow

`flow` is a Go library for event-driven, durable, distributed work execution backed by PostgreSQL.

```text
command  →  worker  →  event
                     └→ optional child commands
```

## Specs

- [Project Overview](specs/projects/flow/project_overview.md) — what `flow` is, the developer model, scope and non-goals.
- [Functional Spec](specs/projects/flow/functional_spec.md) — commands, workers, events, plans, and the distributed execution semantics.

### Architecture & Components

- [Architecture](specs/projects/flow/architecture.md) — durable identity, journal ordering, transactions, locks, delivery, activation, failure and recovery.

- [PostgreSQL storage and journal](specs/projects/flow/components/schema.md) — tables, constraints, indexes, statement contracts, migrations, and database tests.
- [Definitions and execution engine](specs/projects/flow/components/engine.md) — typed definitions, staged decisions, plan reconciliation, dependencies, coordinator decisions, retry policy, and completion.
- [Distributed runtime and operations](specs/projects/flow/components/runtime.md) — registration, claiming, handler invocation, leases, polling and notifications, shutdown, inspection, and `flowtest`.
