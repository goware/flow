---
status: complete
---

# Component: definitions and execution engine

## 1. Purpose

The engine owns Flow's typed contracts and deterministic state-transition rules. It knows nothing about goroutine scheduling or PostgreSQL query plans. Given durable snapshot data and one application decision, it validates, canonicalizes, and produces the exact change set and ordered journal body values the store commits.

## 2. Internal layers

```text
typed public definitions
        ↓ erase through codecs
canonical descriptors and policy data
        ↓
plan / worker / coordinator decision recorders
        ↓
validated normalized change sets
        ↓
journal batch + projection mutations
```

Pure packages cover canonical JSON, descriptor validation, retry policy, failure classification, replay, and deterministic test decisions.

## 3. Definition contracts

### 3.1 Command

An erased command descriptor contains name, positive version, argument/result codecs, and no derived public success event. The typed `Command[A,R]` adds compile-time shape, immutable client binding, and creation defaults.

Definition defaults are retry policy, attempt timeout, and queue. `WithRetry` is the sole retry option. Duplicate options or invalid values accumulate into the immutable definition error and fail registration/execution before durable writes.

### 3.2 Event

An erased event contains name, application namespace, and payload codec. Its durable schema identity has no integer version. `EventRef` is sealed and implemented only by typed events.

### 3.3 Plan and coordinator

Plans retain name/version, start-argument codec, and typed pure invocation. Coordinators retain name/version, state codec, and erased handlers. Registration freezes exact name/version pairs and rejects duplicates.

### 3.4 Outcomes

`Outcome[R]` carries terminal `CommandStatus`, `R` on success, and structured failure on unsuccessful states. It is the sole shared terminal value for plan nodes, worker dependency reads, coordinator deliveries, and tests.

## 4. Canonical identity

Definition values are encoded once through their registered codec, canonicalized, bounded, and fingerprinted. Durable equality never calls application code.

Command declaration fingerprints cover key, definition, arguments, origin, parent, required flag, normalized dependencies, exact waits, initial schedule, and accepted effective settings. The accepted retry policy includes the fixed 20% jitter so existing work is independent of later library defaults.

Root execution fingerprints cover driver, definition, version, key semantics, canonical input/initial state, deadline, fail-fast, ceiling, and semantically relevant options. Metadata is canonicalized separately.

## 5. Retry engine

Canonical retry policy contains:

- optional positive maximum consumed attempts;
- optional positive maximum elapsed duration;
- ordered non-empty positive backoff;
- fixed proportional jitter `0.20`.

At least one bound is required. The final backoff repeats. Retry evaluation receives persisted `BudgetStartedAt`, consumed attempts, database now, attempt identity, error class, and optional `RetryAfter`. It calls no clock or application callback.

Permanent errors never retry. `RetryAfter` replaces the policy delay but not its bounds. The selected absolute next-attempt time is persisted. Interruption and lease loss do not increment consumed attempts or choose a new schedule.

Coordinator delivery uses the same canonical policy shape, snapshotted from the runtime default at coordinator creation.

## 6. Unified `Execute` recorder

The public generic function switches only across sealed library scopes.

### 6.1 Plan path

The recorder creates or finds one `planDeclaration`, validates typed definition and arguments, and returns `Node[R]`. Repeated keys must be equivalent after all builder modifiers.

### 6.2 Worker/coordinator path

The recorder creates or finds one staged command in the decision buffer and returns `Node[R]`. A staged command initially uses command defaults, is required, and has no delay. The owner records call order for deterministic journal ordering while key maps provide idempotent lookup.

### 6.3 Node behavior

Every node stores owner kind, command key, descriptor, and record reference.

- `Key` always returns the stable key.
- `Optional` and `Delay` work for every owner.
- `After`, `AfterSettled`, `AfterFailed`, `WaitFor`, `Within`, `Outcome`, and `Children` require a plan owner.

Invalid scope use stores the first deterministic defect on the owner. Later calls may be no-ops, but validation always fails the complete decision. No partial outputs survive.

`Node[R]` contains no durable capability and must never escape its callback. Tests verify that behavior contract and owner poisoning.

## 7. Worker decisions

`Work[A]` exposes immutable `Args`, `Info`, and dependency-scoped result access. Its private buffer contains staged events, staged commands, and the first defect. `flow.Emit` validates a typed event, stable non-empty key, canonical payload, and the 64 KiB bound without performing SQL.

Repeated `(event name,key)` identities coalesce only when canonical content matches; disagreement poisons the decision. Events are ordered by name then key for deterministic settlement. Plans are not an emitting scope and attempted use poisons reconciliation.

Repeated child keys coalesce only when command definition/version, canonical arguments, required flag, and delay agree. `Delay` must be finite and at least one millisecond. The accepting transaction later converts the duration into an immutable absolute schedule using PostgreSQL time.

Handler conclusions:

- `(result,nil)`: encode/bound result, validate decision, attempt fenced settlement;
- retryable error: discard staged events and commands and apply policy;
- permanent error/panic/timeout with exhausted policy: discard outputs and terminalize failed;
- lost fence: write nothing from the stale handler.

Successful settlement closes child membership even when it is empty.

## 8. Commit function

`WithCommit` binds a statically registered typed function to a worker. It is not part of handler output and cannot capture per-attempt hidden values. Its inputs are decoded from the durable command arguments, accepted successful result, and command metadata.

The function runs after Flow locks and semantic writes are staged, before notification and transaction commit. Error rolls back the entire settlement; the same already-computed handler decision may be attempted again only under the transaction retry rules. The function must perform short local database work and no external I/O.

## 9. Plan snapshot and reads

The snapshot indexes commands by key, terminal results/failures by command ID, children by parent, and application events by `(name,key)` plus journal order.

`Fact` reads one exact key. `Facts` reads all values for one definition. `Node.Outcome` validates that its definition matches the snapshot command and returns unavailable only while non-terminal. `Node.Children` returns unavailable until successful membership closure.

Reads are recorded in-memory for diagnostics, simulation, and double-evaluation comparison. They are not stored in a relational plan-read table. Dirty reconciliation is the scheduling authority.

## 10. Plan validation and reconciliation

At evaluation end:

1. surface panic or recorded defect;
2. finish decoding any lazily requested event/result bodies;
3. verify duplicate declarations agree;
4. verify every dependency exists in snapshot or declaration set;
5. normalize dependency members and reject cycles;
6. reject unsupported threshold/quorum semantics;
7. require non-empty exact event keys;
8. require `Within` only with `WaitFor`;
9. reject ownership conflicts and command-ceiling overflow;
10. compute declaration and evaluation fingerprints.

Reconciliation accepts only new command keys. Existing plan-owned keys must remain equivalent. Existing worker/coordinator keys cannot be redeclared by a plan. Commands immediately unsatisfiable may terminalize as skipped in the same transaction; the dirty scheduler later re-evaluates from their terminal facts. `plan_quiescent` means the accepted evaluation declared no new commands.

`PlanReconciled` records revision, snapshot position, quiescence, and accepted keys/fingerprints without duplicating arguments already recorded in each `CommandCreated`.

## 11. Dependencies and waits

Dependency group evaluation returns `pending`, `satisfied`, or `impossible`:

- all-succeeded becomes impossible on any unsuccessful member;
- all-settled becomes satisfied when every member terminalizes;
- all-failed becomes impossible on any successful member.

When all groups satisfy, the engine evaluates exact waits. Existing matching facts mark waits satisfied. Missing waits start their once-only `Within` deadline when specified. With no remaining wait, the engine sets the command ready at the maximum of database now and explicit initial delay.

Impossible required commands become skipped and participate in failure processing. Optional skips do not determine execution outcome.

## 12. Command state machine

The engine validates every transition and derives counters/queue effects:

```text
pending → ready → running → succeeded
                    └→ retry_wait → ready
pending|ready|running|retry_wait → failed|cancelled|expired|skipped
```

Success appends the typed terminal event and closes membership. Retry records attempt conclusion and immutable next schedule. Cancellation and expiry own terminality if they win the execution lock before settlement. A late fact never changes a terminal wait.

Required failure enters execution failing and resolves `AfterFailed`/`AfterSettled` before fail-fast cancellation. This ordering preserves explicit failure-handling branches.

## 13. Completion

The engine consumes incrementally maintained execution counters and flags.

- direct: required closed tree settled successfully and no open command;
- plan: no open command/wait, not dirty, and quiescent;
- coordinator: explicit `Succeed` plus no required open command;
- failure: required unsuccessful outcome plus mode/fail-fast settlement rules;
- cancellation/deadline: immediate execution terminality plus outstanding command terminalization.

Consulted-but-unavailable plan reads block success only. Once required failure makes success impossible, those reads do not delay terminal failure.

## 14. Coordinator decisions

Erased handler selectors are start, application event name, or command terminal outcome name/version. `OnOutcome` decodes the command terminal journal row into `Outcome[R]`.

The decision buffer contains cloned state, staged events, staged commands, optional terminal decision, and first defect. `Succeed` and `Fail` return no error and record terminal intent. Mixed/repeated incompatible calls or later mutation poison the decision.

Nil handler return validates command ceiling and success preconditions, canonicalizes state, and creates one atomic change set. Handler error discards all changes and invokes persisted delivery retry policy. Permanent/exhausted failure records coordinator failure and fails the execution.

## 15. Event ingress

`Event.Emit` is external ingress, distinct from decision-scoped `flow.Emit`. Validation covers typed definition, non-empty exact key, canonical payload, 64 KiB limit, and execution ID. The store owns terminal/idempotency ordering and rejects ingress through an attempt context.

No public command-injection operation exists. Existing executions gain commands only through their plan, workers, or coordinator authority.

## 16. Journal builder and replay

The engine emits ordered semantic entries. Within a worker success batch the stable category order is:

1. attempt conclusion;
2. staged application events in `(name,key)` order;
3. child `CommandCreated` entries in key order;
4. parent terminal event;
5. dependency/wait-derived terminal events in deterministic key order;
6. execution transition if reached.

Other paths define equally fixed category order. The store assigns positions but never invents meaning.

Replay folds journal kinds into execution, command, attempt, graph, fact, plan, and coordinator state. Projection conformance tests extend whenever a journal kind/body changes.

## 17. `flowtest`

The deterministic harness supports:

- invoking workers with args/info/dependency outcomes and inspecting staged events and child commands;
- testing registered commit functions with a transaction double;
- evaluating plans over synthetic commands/facts and inspecting typed nodes/reads;
- double-evaluation and always-evaluate purity assertions;
- delivering `Outcome[R]` and events to coordinators and inspecting state, staged events, commands, and terminal decisions;
- recursively simulating closed direct trees;
- asserting scope poisoning and declaration conflicts.

It exposes the reduced public vocabulary and no legacy aliases.

## 18. Test plan

Unit/property coverage includes definition validation, canonical equality, fixed jitter policy, every dependency truth table, exact event keys, node owner matrix, duplicate staging, missing keys, cycles, plan purity, terminal read availability, failure completion, coordinator terminal poisoning, and replay.

PostgreSQL integration contracts cover idempotent command creation, atomic membership, wait races, fenced commit functions, command ceilings, plan dirty reconciliation, coordinator retries, and journal/projection equality.

## 19. Acceptance conditions

- Public definitions expose the exact functional-spec API and no removed aliases.
- `Execute` retains one meaning while producing correct scope-specific records.
- Worker/coordinator scopes stage bounded application events but expose no Flow client/transaction; plan scopes cannot emit.
- `Outcome[R]` losslessly represents every command terminal state.
- Event definitions and waits contain no version.
- No quorum, plan-node policy override, or external command-injection branch remains.
- Every defect rejects the whole enclosing decision.
- Retry and coordinator policies are canonical persisted data.
- Replay and `flowtest` cover every retained transition.
