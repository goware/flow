---
status: draft
---

# flow

`flow` is a Go library for modeling an execution graph of work on PostgreSQL.

Real backend workloads are state machines. A request arrives, and completing it means running many dependent steps: some wait on external systems, some fan out in parallel, some only run if an earlier step took a particular branch. The steps are deterministic — the difficulty is that there are many of them, they take minutes or hours, failures can happen anywhere, and reconstructing "what is this thing doing right now" is hard.

Today that logic gets hand-rolled: a status column per table, a poll loop per status, a bespoke claim query, a stale-recovery sweep, and a set of timeout rules that must be documented so nobody breaks them. Every one of those is a place to get it subtly wrong, and the failure modes are silent — work that waits forever because a release signal was lost, or a timeout that never fires because the clock it reads is reset by the loop that polls it.

`flow` exists to make that infrastructure a library instead of a per-project reinvention, with three ideas:

- **Flow** — one durable execution of a state machine. It has an identity, optional durable state, a graph of steps, and an immutable history. Its steps may execute concurrently, but durable flow commits are serialized, so no handler races another over the same flow's state.
- **Step** — one unit of work in a flow. Durable, individually retriable, individually traceable, and fenced so a stalled worker can never commit a stale result.
- **Signal** — an immutable external fact delivered to a flow, which can release steps that are waiting on it. A deposit confirmed, an attestation published, a bridge delivery observed. Facts are never overwritten by later ones.

A step handler is an ordinary Go function. It receives typed arguments and returns what happened: done, waiting on a signal, retry, or permanently failed. Handlers spawn further work by describing it — a step declares the steps that come after it, and that declaration commits atomically with the step's own success. Nothing is created twice on retry, and nothing is left dangling on crash.

## Goals

- **Small and intuitive.** Three concepts, and a handler signature an engineer can hold in their head. The common path should need no knowledge of leases, dispatch records, or claim queries.
- **Type safe.** Typed step arguments, typed flow state, typed results. Serialization is the library's problem, not the caller's.
- **Easy to test.** A handler is a pure function of its arguments, its flow's state, and the signals it has received. Testing one should not require a database.
- **Easy to trace live.** At any moment, one query returns the whole graph: every step, its state, its attempts, its errors, and the ordered history of how it got there.
- **Correct under failure.** At-least-once execution with lease fencing, atomic completion, deterministic step identity, and a hard rule that every wait has a deadline.
- **PostgreSQL only.** No broker, no orchestration service, no separate control plane. When application state lives in the same database, a step's application writes and its own completion commit in one transaction.

## Technical requirements

- Go library, published as `github.com/goware/flow`
- PostgreSQL as the only backing store, via `github.com/goware/pgkit/v2` over `github.com/jackc/pgx/v5`
- Fully asynchronous; results are observed, not awaited by blocking a worker
- Handler code carries no determinism or replay constraints — handlers are re-invoked, never replayed
- Usable as a library inside an existing service, sharing that service's database and transactions

## Non-goals

- Not a Temporal or DBOS replacement — no transparent replay of arbitrary Go code, and no code-version pinning
- No exactly-once guarantees for external side effects; handlers use idempotency keys and reconciliation
- Not a general message broker or pub/sub system
- Not an event-sourcing store
- No multi-region or multi-database coordination
