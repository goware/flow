---
status: draft
---

# flow-draft-1

> **Superseded.** This is the first design draft for `flow`, built on a graph-first model of flows, steps, and signals. The active design lives in `specs/projects/flow` and uses a command / worker / event model with declarative plans instead. This document is retained for its reasoning; the `flow.` package identifiers in its examples describe this draft's API, not the current one.

`flow-draft-1` proposes a Go library for durable, event-driven execution graphs backed by PostgreSQL.

Many backend operations are long-lived state machines. A request arrives, and completing it means running many dependent steps: some wait on external systems, some fan out in parallel, some only run if an earlier step took a particular branch. The intended transitions are known — the difficulty is that execution is distributed, it may take minutes or hours, failures can happen anywhere, and reconstructing "what is this thing doing right now" is hard.

Today that logic gets hand-rolled: a status column per table, a poll loop per status, a bespoke claim query, a stale-recovery sweep, and a set of timeout rules that must be documented so nobody breaks them. Every one of those is a place to get it subtly wrong, and the failure modes are silent — work that waits forever because a release signal was lost, or a timeout that never fires because the clock it reads is reset by the loop that polls it.

`flow` exists to make that infrastructure a library instead of a per-project reinvention, with three ideas:

- **Flow** — one durable execution of a state machine. It has an identity, a graph of steps, an outcome, and an immutable history. Its steps may execute concurrently, but durable flow commits are serialized so the graph always advances consistently.
- **Step** — one unit of work in a flow. Durable, individually retriable, individually traceable, and fenced so a stalled worker can never commit a stale result.
- **Signal** — an immutable external fact delivered to a flow, which can release steps that are waiting on it. A deposit confirmed, an attestation published, a bridge delivery observed. Facts are never overwritten by later ones.

A step handler is an ordinary Go function. It receives typed arguments and returns what happened: done, waiting on a signal, retry, or permanently failed. Handlers spawn further work by describing it — a step declares the steps that come after it, and that declaration commits atomically with the step's own success. Nothing is created twice on retry, and nothing is left dangling on crash.

## Goals

- **Small and intuitive.** Three concepts, and a handler signature an engineer can hold in their head. The common path should need no knowledge of leases, dispatch records, or claim queries.
- **Type safe.** Typed step arguments, typed results, and typed signals. Serialization is the library's problem, not the caller's.
- **Easy to test.** A handler is an ordinary, re-invocable Go function that can be exercised without running the durable runtime.
- **Easy to trace live.** At any moment, one query returns the whole graph: every step, its state, its attempts, its errors, and the ordered history of how it got there.
- **Correct under failure.** At-least-once execution with lease fencing, atomic completion, deterministic step identity, and a hard rule that every wait has a deadline.
- **PostgreSQL only.** No broker, no orchestration service, no separate control plane. When application state lives in the same database, a step's application writes and its own completion commit in one transaction.

## Technical requirements

- Go library, published as `github.com/goware/flow`
- PostgreSQL as the only backing store, via `github.com/goware/pgkit/v2` over `github.com/jackc/pgx/v5`
- Fully asynchronous; results are observed, not awaited by blocking a worker
- Handler code carries no determinism or replay constraints — handlers are re-invoked, never replayed
- Usable as a library inside an existing service, sharing that service's database and transactions

## Future direction

The stored graph and immutable history are intentionally suitable for a first-party operational UI: live topology, attempts, waits, errors, and causation should all be inspectable without reconstructing state from logs. The runtime also exposes vendor-neutral observations in the initial release so OpenTelemetry, metrics, and structured-logging adapters can be added as a near-term follow-on without changing execution semantics.

## Non-goals

- Not a Temporal or DBOS replacement — no transparent replay of arbitrary Go code, and no code-version pinning
- No exactly-once guarantees for external side effects; handlers use idempotency keys and reconciliation
- Not a general message broker or pub/sub system
- Not an event-sourcing store
- No multi-region or multi-database coordination
