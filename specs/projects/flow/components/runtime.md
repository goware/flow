---
status: complete
---

# Component: distributed runtime and operations

## Purpose

The runtime connects typed decisions to durable PostgreSQL work. It owns registration, scheduling, capacity, claims, handler invocation, lease renewal, settlement, maintenance, notification hints, observation, and shutdown.

## Lifecycle

`Migrate` is explicit. `New` validates configuration and installed migrations, allocates no dedicated database session, and starts no goroutines. A new runtime is immediately usable as a client or caller-transaction adapter.

`Register` accepts worker registrations and coordinator definitions before `Run`. Duplicate exact name/version keys are rejected. Registry mutation freezes when `Run` begins; a runtime runs at most once.

`Run(ctx)` starts:

- a bounded command scheduler;
- a bounded coordinator scheduler;
- command and coordinator lease renewal;
- wait-expiry, deadline, and recovery maintenance;
- an optional reconnecting PostgreSQL notification listener;
- graceful shutdown coordination.

## Defaults and options

| Setting | Default |
|---|---:|
| command lease | 60 seconds |
| worker concurrency | `GOMAXPROCS`, minimum 1 |
| coordinator concurrency | 1 |
| poll interval | 1 second |
| shutdown grace | 30 seconds |
| notifications | enabled |
| maximum commands/execution | 1000 |

Public options configure worker, coordinator, and named-queue concurrency; poll interval; notification hints; shutdown grace; observer; schema; and command ceiling. The lease duration remains an internal invariant.

## Command scheduling

The scheduler probes `flow_command_queue` for registered exact command versions whose `next_run_at` has elapsed. Claims use `SKIP LOCKED`, respect global and queue-lane capacity, and install an attempt/lease fence.

Invocation loads typed arguments and command information, then releases database resources before application code runs. The runtime establishes attempt context, optional timeout, panic recovery, error classification, and the private decision scope.

Success settlement invokes any `WithCommit` callback inside the fenced transaction and atomically accepts result, events, children, readiness, and execution progression. Failure settlement records classification, chooses retry or terminal failure, and applies reduced fail-fast/completion.

## Coordinator scheduling

Idle active coordinators scan retained journal positions to find the next matching start, application event, or command terminal outcome. A matching input becomes a durable ready delivery. Claims install a separate coordinator lease fence and use independent concurrency.

The handler runs without a connection. Settlement verifies its delivery position/fence, writes state and staged decisions, and advances the durable inbox. Errors retry the same delivery; terminal selection closes the coordinator.

Command workers do not need coordinator code. Their terminal events remain retained until a coordinator-capable replica advances the inbox.

## Event ingress and readiness

External `Event.Emit` canonicalizes content and performs idempotency checks. A new event appends history, resolves matching exact waits, updates command readiness, and makes coordinator scanning eligible in one transaction.

Worker/coordinator staged events use the same resolution path within settlement. Handler scopes intentionally expose no client, preventing unfenced nested external ingress from escaping their decision transaction.

The maintenance scheduler expires unresolved wait budgets. Initial delays remain separate: satisfying waits does not bypass `next_attempt_at`, and an elapsed wait budget can expire before the delay ends.

## Lease renewal and recovery

Active handler registries track attempt ID, lease token, and cancellation function. Renewers extend leases in bounded batches. Fence loss cancels local context.

Maintenance recovers expired command/coordinator leases. The next replica can retry because application invocation is at-least-once and accepted durable progress is fenced. Recovery does not depend on process identity surviving restart.

## Notifications and polling

Transactions publish versioned, bounded wake hints after commit. The listener reconnects and triggers a catch-up generation before trusting new hints. Invalid or unknown payloads are ignored safely.

Every scheduler polls regardless of notification mode. Therefore transaction-pooling proxies, dropped messages, reconnects, and `WithNotifications(false)` affect latency only, never correctness.

## Shutdown

Cancellation stops new claims, listener activity, and maintenance. The runtime waits up to shutdown grace for handlers to settle. It then cancels remaining handler contexts and returns after scheduler goroutines exit. It does not close the caller-owned database pool.

## Observability and faults

The optional observer receives bounded operational facts for start, event ingress, command/coordinator claim and settlement, retry, wait/deadline maintenance, and shutdown. The no-op observer is allocation-conscious and default.

Internal fault hooks bracket journal, projection, queue, fence, commit, coordinator, notification, and ambiguous-commit boundaries. Tests restart runtimes and assert durable state rather than relying on local scheduler timing.

## Deployment shapes

- Combined service: registers all definitions and runs all schedulers.
- Worker pool: registers selected command versions.
- Coordinator pool: registers coordinator definitions and any local workers.
- Publisher/API process: creates a runtime client but does not call `Run`.

Several replicas may share all roles. PostgreSQL remains the sole coordination authority.

## Operational invariants

- Handler work is bounded by configured semaphores and is never placed in an unbounded local queue.
- Application handlers hold no PostgreSQL connection while running.
- Every accepted settlement is fenced and journaled.
- Unknown definition versions remain durable and unclaimed.
- Polling alone always progresses eligible work.
- Running attempts remain settleable during reduced fail-fast; newly staged work cannot escape the failing execution.
