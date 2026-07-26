---
status: draft
---

# Component: definitions and execution engine

## 1. Purpose and scope

The engine owns Flow's deterministic meaning. It turns typed definitions, a durable snapshot, a database timestamp, and one staged handler/ingress transition into a complete atomic change set. It implements command identity, declarative retry, dependency and wait resolution, plans, coordinators, fail-fast, completion, and journal semantics.

It does not run goroutines, claim work, renew leases, issue SQL, or read a clock. The runtime invokes it; the store persists its output.

## 2. Internal layers

```text
public generic descriptors
        |
erased immutable definitions + codecs
        |
handler scopes / plan builder / coordinator scope
        |
durable Snapshot + Trigger + DBNow
        |
Engine.Apply(...) -> ChangeSet or DecisionDefect
        |
store validates expected states and commits
```

The engine has no database interface. Snapshot-loading can take multiple store round trips, but the completed `Snapshot` passed into a semantic operation is immutable. Plan payload loading uses the resumable mechanism in §8.2 so engine code remains I/O-free.

## 3. Typed definitions and erased descriptors

### 3.1 Definitions

The public generic types contain private immutable descriptors:

```go
type commandDef struct {
    name       string
    version    int
    args       codec
    result     codec
    defaults   commandDefaults
    done       eventDef
    client     clientCapability // excluded from durable identity
}

type eventDef struct {
    namespace string             // application or command_success
    name      string
    version   int
    payload   codec
}

type planDef struct {
    name    string
    version int
    args    codec
    invoke  func(*Plan, any)
    client  clientCapability
}

type coordinatorDef struct {
    name         string
    version      int
    state        codec
    start        erasedCoordHandler
    events       map[eventSelector]erasedCoordHandler
    outcomes     map[commandSelector]erasedCoordHandler
    client       clientCapability
}
```

A `codec` exposes canonical encode/decode plus a diagnostic Go type token. Type tokens are never durable identity; name/version and canonical schema meaning are. Function bodies are likewise not fingerprinted: applications must bump a plan version when declarations/reads can change for one snapshot, and a coordinator version when state, subscriptions, or decisions materially change.

`Command.Done()` returns the descriptor's derived success event. Its internal namespace prevents external `Publish` from impersonating command completion while preserving one public `Event[T]` model. `OnOutcome` is a typed view over all terminal events for the command pair, not another event descriptor or row.

### 3.2 Binding

`.With(client)` copies the definition value, replacing only its private capability. Definition comparisons, registration, `Done`, `Name`, and `Version` ignore the binding. A transaction-bound capability contains a lifetime token checked on every call; use after the transaction finishes returns `ErrClosed` before SQL.

### 3.3 Registration

`Handle` produces a registration with one worker function and zero or one static commit function. `DefineCoordinator` validates its handler table immediately. Runtime registration additionally rejects collisions across separately constructed values:

- more than one worker per command pair;
- incompatible payload/result codec tokens for one pair;
- multiple commit functions;
- duplicate `OnStart`, `On`, or `OnOutcome` selectors;
- `On(cmd.Done())` and `OnOutcome(cmd)` overlap;
- invalid retry policy, queue, timeout, name, or version.

The frozen registry entry contains every function needed to decode, invoke, settle, and inspect that durable version.

## 4. Canonical values and fingerprints

Every accepted application value goes through:

1. ordinary typed JSON marshaling;
2. parse and RFC 8785 canonicalization;
3. configured size/depth validation;
4. SHA-256 digest;
5. optional typed decode round trip in debug/test mode.

Fingerprints are canonical internal records, not concatenated strings. The record includes a schema version and length-delimited fields before hashing.

### 4.1 Start identity

Start identity includes driver mode, definition name/version, execution key, canonical input, explicit execution deadline choice, fail-fast, and canonical metadata. It excludes the runtime's current command ceiling default and current direct-root operational defaults, both of which are accepted once and returned unchanged on an idempotent repeat.

### 4.2 Command declaration identity

Declaration identity includes:

- execution-local key, command name/version, and canonical arguments;
- origin and direct parent when worker-spawned;
- required/optional classification;
- normalized dependency groups and event waits;
- explicit `Delay`, `Within`, or `StartAfter` declaration;
- presence and canonical value of an explicit plan-node retry override.

It excludes command-definition queue, timeout, and retry defaults. The accepted operational record includes their resolved values and is stored/journaled separately.

### 4.3 Equality

Equality first compares digest, then canonical bytes on a stable-key collision. Hash equality alone is never enough to declare an idempotent match. Diagnostics name the conflicting field category but never include application bytes.

## 5. Immutable retry policy

The public `RetryPolicy` is sealed immutable data. Its internal canonical form is:

```go
type retryPolicy struct {
    MaxAttempts *int           // budget-consuming invocations; nil means no count bound
    MaxElapsed  *time.Duration // from immutable BudgetStartedAt; nil means no time bound
    Backoff     []time.Duration
    Jitter      float64        // inclusive range 0..1
}
```

At least one bound is required. Backoff entries are positive and the final entry repeats. `WithMaxAttempts(n)` creates an attempt-only policy with default delays; `RetryFor(d)` creates elapsed-only policy until `.Attempts(n)` adds the other bound. Definition validation rejects conflicting option forms.

### 5.1 Decision input and output

```go
type RetryInput struct {
    DBNow             time.Time
    BudgetStartedAt   time.Time
    ConsumedAttempts  int
    AttemptID         AttemptID
    Classification    ErrorClass
    ExplicitDelay     *time.Duration
    ExecutionDeadline *time.Time
}

type RetryDecision struct {
    Retry         bool
    NextAttemptAt time.Time
    StopReason    string
}
```

The engine increments the consumed count for plain errors, panics, timeouts, explicit retry, and permanent failures. Shutdown interruption and lease loss do not increment it. `Attempt` in `CommandInfo` remains the invocation ordinal and may therefore be larger.

Permanent classification stops before policy. A count bound stops when the just-consumed attempt reaches it. An elapsed bound stops when `DBNow` reaches the budget deadline or when the selected next attempt would be at or beyond it. The execution deadline applies the same cap.

Plain retry delays use the backoff element selected by consumed count. Jitter is deterministic for one attempt: a SHA-256-derived unit fraction from `(AttemptID, policy hash)` scales the allowed jitter range. A settlement retry therefore chooses the same value without an application callback or clock. `RetryAfter` supplies its exact positive delay instead of policy backoff/jitter, while both bounds still apply. The chosen absolute time is persisted.

## 6. Worker scope and staged decision

### 6.1 Scope

Before invocation, the runtime decodes arguments and creates:

```go
type workScope struct {
    args       any
    info       CommandInfo
    deps       map[string]dependencyValue
    decision   stagedDecision
    firstError error
}

type stagedDecision struct {
    events    []stagedEvent
    commands map[string]stagedCommand
    order     []string
    terminal *coordinatorTerminalDecision
}
```

`Work[A].Args` and `Info()` are immutable. Dependency values are batch-loaded and decoded before invocation. `ResultOf` and `OutcomeOf` verify that the requested key is an explicit durable dependency and that the supplied command definition matches its name/version. A nonterminal dependency or unsuccessful `ResultOf` returns its specified structured error; `OutcomeOf` returns every terminal state.

### 6.2 Emit and Spawn

`Emit` validates the event descriptor, non-empty stable key where required, canonical payload, and size before appending in call order. Repeated same event identity/content coalesces; disagreement poisons the decision with `ErrConflict`.

`Spawn` validates and canonicalizes before staging. Its options are sealed:

- no option -> required and immediate;
- `Optional()` at most once;
- `StartAfter(d)` at most once, finite, and strictly positive.

Repeated command keys inside one decision coalesce only when declaration identity and accepted effective settings agree. `StartAfter` equivalence compares the declared duration; its absolute PostgreSQL schedule is computed later by the accepting transaction. A first staging error remains on the scope even if application code ignores the returned error, preventing partial or silently corrupt success.

Worker spawns record `parent_command_id`; coordinator spawns do not. Neither operation performs SQL or invokes nested work.

### 6.3 Handler conclusion

The runtime recovers panic and returns one `HandlerConclusion`:

```go
type HandlerConclusion struct {
    Result       canonicalValue
    Err          error
    Class        ErrorClass
    Decision     stagedDecision
    ContextCause error
}
```

If the handler returns an error, panics, or the scope is poisoned, every staged output is discarded. Scope errors caused by deterministic output conflicts/limits are permanent decision failures. Context cancellation is disambiguated as attempt timeout, execution cancellation/deadline, shutdown interruption, or lease loss by the runtime's cause.

### 6.4 Declared commit function

The commit function is part of the worker registration, never the staged decision. It receives decoded copies of durable arguments/result/info and a wrapper that exposes only `Exec`, `Query`, and `QueryRow`. It cannot commit, roll back, start a nested transaction, obtain a Flow client, or register another function.

`flowtest` invokes it directly with a transaction double. The engine marks the terminal success event's internal metadata `commit_applied=true` only in the same change set in which that function is scheduled to run. A function error rolls the entire success change set back and is classified as the attempt's conclusion.

## 7. Snapshot and change set

### 7.1 Snapshot

```go
type Snapshot struct {
    Execution    ExecutionState
    Commands     map[string]CommandState
    Children     map[string][]string
    Groups       map[CommandID][]DependencyGroup
    Waits        map[CommandID][]EventWait
    EventIndex   map[EventSelector][]EventHeader
    PlanReads    []PlanRead
    Coordinator  *CoordinatorState
    LoadedValues map[valueLocator]canonicalValue
}
```

Commands are keyed by execution-local key. `Children` values are sorted. Event headers contain position and value locator, not necessarily payload bytes. The runtime/store applies in-transaction trigger changes to the snapshot before asking the engine to resolve or evaluate.

### 7.2 Change set

```go
type ChangeSet struct {
    Expected       []ExpectedState
    Commands       []CommandMutation
    Dispatch       []DispatchMutation
    Groups         []GroupMutation
    Waits          []WaitMutation
    Attempts       []AttemptMutation
    PlanReads      []PlanRead
    Coordinator    *CoordinatorMutation
    Execution      ExecutionMutation
    Journal        []JournalRecord
    WakeQueues     []string
    CommitCall     *CommitInvocation
}
```

Every mutation names its expected prior state/revision. The store treats a mismatch or wrong affected-row count as lost ownership/invariant failure and rolls back. The engine sorts commands, groups, cascaded terminal records, and wake queues before returning.

## 8. Plan evaluation

### 8.1 Plan recorder

`*Plan` is an in-memory recorder:

```go
type Plan struct {
    snapshot     *Snapshot
    declarations map[string]*planNode
    order        []string
    reads        map[planReadKey]ReadAvailability
    valueMisses  map[valueLocator]struct{}
    defect       error
}
```

`Do` records a proposal and returns a builder. Builder calls append normalized dependency groups or waits and set single-valued options. Conflicting repeats poison the plan. `Fact`, `Facts`, `Children`, `Result`, and `Outcome` validate and record availability.

Semantics are stable and monotonic:

- `Fact` returns the earliest matching event by position;
- `Facts` returns all matching events in position order;
- `Children` returns sorted direct-child keys after successful membership closure, including an empty set;
- `Result` returns only a successful typed result;
- `Outcome` returns every terminal state.

The internal availability matrix is exactly the functional specification's available/temporary/permanent model. Only temporary reads count against successful completion.

### 8.2 Resumable value loading

The narrow initial snapshot contains all command states and event headers but may omit large bodies. When a read finds a known available value whose bytes are not loaded, it records a `valueMiss` and returns the public zero/false for that provisional pass. The engine discards that pass's declarations, returns `NeedValues`, and the store loads all requested locators in one query. The pure plan is invoked again.

This repeats until one pass has no misses. Each pass discovers at least one new locator, so it terminates for a bounded plan. Value-loading passes count toward the same per-transaction invocation guard as fixed-point continuations. This permits lazy loading without giving the engine SQL or a callback and makes it explicit that a pure plan may be invoked more than once for one durable transition.

Debug determinism compares only complete passes. A plan with side effects already violates its contract; repeated internal evaluation is another reason that such code is unsupported.

### 8.3 Validation and normalization

After a complete invocation:

1. recover panic as a plan defect;
2. reject invalid descriptor/name/version/payload/options;
3. resolve every read key against a prior declaration or durable command;
4. validate every dependency after the whole function returns, allowing forward references;
5. normalize group member order and reject duplicate/invalid thresholds;
6. require `Within` to accompany `Await`, and accept each single-valued option once;
7. detect cycles among new plan-owned nodes;
8. coalesce equivalent repeated `Do` keys and reject disagreement;
9. compare existing plan-owned declaration identity, excluding changed definition defaults;
10. reject `Do` ownership of worker/coordinator/external keys.

Previously accepted commands absent from this pass remain untouched. The plan only grows.

### 8.4 Reconciliation

For each genuinely new declaration, the engine evaluates groups/waits against the post-trigger snapshot:

- all prerequisites satisfied -> create with dispatch state `ready` and initial schedule;
- unresolved prerequisite -> create `pending` with no dispatch;
- permanently impossible dependency -> create directly `skipped` plus its terminal event;
- missing awaited fact after command groups satisfy -> pending, initialize `Within` when configured;
- early awaited facts -> mark waits satisfied before readiness.

Each declaration delta goes through the execution command ceiling once. Existing equivalent declarations append no history and do not change counters. Applying ordinary `ready` or `pending` declarations ends the current cycle with `plan_quiescent = false`; their open state already prevents completion, and a later routed terminal transition provides the next evaluation.

The engine evaluates the pure plan again in the same transaction only if applying the delta also created a plan-observable terminal transition, such as an immediately skipped or expired command. It repeats while each new delta produces another such terminal transition, then stops on either a no-new-command pass (`plan_quiescent = true`) or an ordinary open declaration (`plan_quiescent = false`). This is the fixed point needed to prevent an immediate skip/expiry cascade from waiting for a wake that can no longer arrive, without doubling every normal plan evaluation.

Every continuation accepts at least one genuinely new terminal command. A positive stored command ceiling therefore bounds the fixed-point cycle. Independently, a fixed technical guard of 10,000 total plan invocations per semantic transaction—including lazy value-loading and fixed-point passes—protects the execution lock. Exceeding it is a plan defect. The guard does not limit the number of commands one pass may declare and is exposed through plan diagnostics.

The latest accepted read set includes public reads plus the routing-only observations required by the architecture. An event selector present only in a stored `Await` is topology, not a plan read. `plan_quiescent` records whether the latest completed cycle reached a no-new-command pass.

### 8.5 Purity and routing verification

`flowtest.AssertPlanDeterministic` and optional runtime debug mode evaluate the same fully loaded snapshot twice and compare:

- canonical declarations and accepted settings;
- normalized dependency/wait topology;
- declaration order only where it affects diagnostics, never identity;
- consulted read selectors and availability;
- provisional value-miss sequence.

Routing assertion mode evaluates a transition the persisted read set says can be skipped. Any change in complete output is an invariant error; verification output never commits.

## 9. Dependencies, waits, and readiness

### 9.1 Group evaluation

```go
func evaluateGroup(g Group, members []CommandState) GroupState {
    switch g.Kind {
    case AllSucceeded:
        if allSucceeded(members) { return Satisfied }
        if anyUnsuccessfulTerminal(members) { return Unsatisfiable }
    case AllSettled:
        if allTerminal(members) { return Satisfied }
    case AllFailed:
        if allUnsuccessfulTerminal(members) { return Satisfied }
        if anySucceeded(members) { return Unsatisfiable }
    case AtLeast:
        successes := countSucceeded(members)
        possible := successes + countNonTerminal(members)
        if successes >= g.Threshold { return Satisfied }
        if possible < g.Threshold { return Unsatisfiable }
    }
    return Unresolved
}
```

Terminal changes seed only groups found through the reverse index. The engine updates its in-memory snapshot as it builds mutations, so cascading skip and readiness happen in the same transaction. A group transition is once-only.

### 9.2 Wait evaluation

An event selector is satisfied by its earliest retained matching event that is timely for the wait. With no `Within`, any retained match before the execution remains terminal is timely. With `Within`, the event's PostgreSQL `RecordedAt` must be no later than the persisted wait deadline. When a timely new event arrives, unresolved matching wait rows receive that position and decrement the dependent's wait count once. A late event remains in the snapshot/history but does not mutate the wait.

When command groups first become satisfied:

1. recheck all awaited selectors against retained event headers;
2. if all exist, apply initial `Delay` and create dispatch;
3. otherwise, set `wait_started_at = DBNow` and, for `Within`, set the capped deadline once.

An expired wait terminates the command as `expired`, appends its terminal event, removes any dispatch, and seeds dependency/plan processing. Expiry uses the persisted deadline even when a matching event was recorded later, so sweep timing cannot change the winner. `Await` without `Within` remains bounded by execution deadline unless the execution explicitly has none.

### 9.3 Readiness and initial schedule

`makeReady` is the sole operation that establishes first eligibility:

```go
eligible := dbNow
if command.ScheduleKind == PlanDelay {
    eligible = dbNow.Add(command.InitialDelay)
}
if execution.Deadline != nil && !eligible.Before(*execution.Deadline) {
    return expireWithoutClaim(command)
}
command.BudgetStartedAt = eligible
command.NextAttemptAt = eligible
insertDispatch(eligible)
```

Worker/coordinator `StartAfter` commands have no dependency phase; creation computes `eligible = accepting DBNow + duration` and uses the same deadline check. The accepted absolute time never changes after commit.

## 10. Command settlement state machine

### 10.1 Success

Given a fenced running attempt and successful canonical result:

1. validate staged decision and complete child batch;
2. append attempt conclusion success;
3. set command result/success, remove dispatch, close children, decrement open count;
4. append emitted application events;
5. create/journal children and increment counts;
6. append the command success event after emitted events;
7. resolve affected dependencies/waits and cascaded skips;
8. evaluate a relevant plan and reconcile its delta;
9. apply required-failure state if a cascaded transition introduced one;
10. evaluate execution completion;
11. attach the optional commit invocation and wake queues.

The journal batch builder places records in architecture §7.3 order and fills causation after positions are reserved.

### 10.2 Retryable conclusion

Staged outputs are discarded. The policy either:

- appends attempt conclusion with `retry_scheduled`, increments consumed attempts, stores the selected absolute next time, clears the lease, and moves command/dispatch to `retry_wait`; or
- appends attempt conclusion plus terminal `CommandFailed`, removes dispatch, decrements open count, and runs terminal processing.

The engine never writes an intermediate application event for a retry.

### 10.3 Interruption and lost lease

A graceful shutdown release or recovered expired lease appends an attempt conclusion classified `interrupted` or `lease_lost`, clears ownership, and restores the command to its pre-attempt `ready`/`retry_wait` schedule. It does not increment consumed attempts, move `BudgetStartedAt`, or choose fresh jitter. A handler that merely discovers it lost its fence writes nothing; the winning recovery/cancellation transition owns the conclusion record.

### 10.4 Cancellation and expiry

Cancellation/expiry verifies nonterminal state, cancels the active local context through the runtime when known, removes dispatch, writes the terminal event, and resolves dependents. A late handler cannot settle because its fence is gone. An already identical cancellation is idempotent; a different terminal state returns `ErrTerminal`.

## 11. Failure handling and completion

### 11.1 Failure processing order

For a newly unsuccessful required command:

1. terminal state and event already exist in the in-memory transition;
2. resolve existing dependencies and cascades;
3. evaluate the plan if relevant, allowing newly declared failure branches to be created against the terminal outcome;
4. mark execution `failing`;
5. if fail-fast, compute the surviving failure scope and cancel everything else;
6. evaluate terminal failure only after survivors finish.

The surviving set begins with commands made runnable/retained by failure conditions and every command already running. It includes their dependency descendants. Commands carry an internal `failure_scope` projection once selected; later children of a surviving running command inherit it, and new plan declarations survive only when their prerequisites are in the scope or they are part of the newly selected failure branch. Multiple failures union scopes.

The first transition to `failing` appends `ExecutionBecameFailing` with the triggering terminal position and survivor-set decision before fail-fast cancellation events. Later required failures may extend the survivor set but do not append a second lifecycle entry; their command events and resulting command creations/cancellations remain sufficient history.

Execution-level cancellation bypasses this algorithm and cancels everything without selecting branches.

### 11.2 Counters

Every command insertion increments `command_count`; every nonterminal insertion also increments `open_commands`. Exactly one terminal transition decrements open count. Retrying and claiming do neither. Immediate skipped/expired insertion increments command count but not open count.

The engine maintains deltas, and the store's guarded updates and replay/property tests verify them. It never derives command ceiling or completion by an unbounded count query.

### 11.3 Completion

```go
func completion(s Snapshot, cs ChangeSet) *ExecutionTerminal {
    open := s.Execution.OpenCommands + cs.OpenDelta
    if open != 0 { return nil }

    switch s.Execution.DriverMode {
    case Direct:
        return successUnlessFailing(s, cs)
    case Plan:
        if isFailing(s, cs) { return failed(s, cs) }
        if cs.TemporaryPlanReads != 0 || !cs.PlanQuiescent { return nil }
        return succeeded()
    case Coordinator:
        return validateExplicitCoordinatorDecision(s, cs)
    }
    panic("validated driver mode")
}
```

For plan failure, temporary reads are ignored; explicit open failure-handling work remains counted. `SucceedExecution` is invalid while work remains after the same coordinator decision. `FailExecution` cancels outstanding work and completes failed. Every terminal result appends one execution event and closes the coordinator.

## 12. Plan defects

A `DecisionDefect` includes stable code, safe message, and source. Plan panic, nondeterminism, invalid read/reference, cycle, conflicting declaration, changed explicit node override, and plan command-ceiling overflow are terminal plan defects.

The transaction preserves the durable transition that triggered evaluation:

- at plan start, retain `ExecutionStarted` and fail the new execution;
- after ingress, retain the accepted event/command;
- after worker success, retain result, emitted events, children, and declared commit-function write.

It discards the invalid plan delta, appends `PlanFailed`, cancels outstanding commands with terminal events, appends `ExecutionFailed`, and proposes that complete change set. If its optional commit invocation also succeeds and the transaction commits, none of that accepted work becomes another worker attempt. If the commit invocation fails, the entire proposed change set—including the plan defect—rolls back and the commit-function error follows the ordinary command retry path; no worker success had yet been accepted.

The public call that triggered an initial/ingress defect returns a typed error containing the affected `ExecutionID`. A library-owned transaction commits the terminal record before returning, so an idempotent repeat finds it rather than evaluating again. Under `InTx`, the change set remains subject to the caller's commit or rollback like every other engine output.

## 13. Coordinator engine

### 13.1 Matching

The frozen coordinator definition produces exact selectors:

- `On(event)` matches application/derived-success namespace plus event name/version;
- `OnOutcome(command)` matches any command-terminal event whose command name/version match;
- `On(command.Done())` is the success-only selector and cannot overlap `OnOutcome`.

The store chooses the first matching retained position; the engine validates it again before invocation. `Received.Key` is event idempotency key for `On` and command key for `OnOutcome`. The latter decodes the terminal event and stored result/failure into `CommandOutcome[R]` without another row.

### 13.2 Scope and decision

The runtime decodes current state into a private copy and constructs `Coordination[S]`. The handler may mutate `State`, `Emit`, `Spawn`, or stage exactly one compatible `SucceedExecution`/`FailExecution`. It has no result reads, application transaction hook, Flow client, or nested execution authority.

On nil return, the engine canonicalizes state and validates the full output/ceiling before accepting any part. It appends attempt conclusion and `CoordinatorTransition`, advances start/inbox, increments state revision, applies outputs, and resets the per-delivery retry fields atomically. Start with no handler is the same transition with unchanged state and no output.

### 13.3 Error and retry

Handler error discards state and output and applies coordinator retry policy to the same stable delivery key. The inbox never advances. A deterministic output/ceiling defect or exhausted/permanent error appends `CoordinatorFailed`, fails the execution, and cancels outstanding work.

Only one delivery can be selected/running. Later matching events remain in the journal. Unmatched events require no state transition and are jumped over by the next-match query because subscriptions cannot change for the execution's coordinator version.

### 13.4 Managed command failures

A coordinator that needs to interpret failure spawns the command optional and registers `OnOutcome`. Exactly one terminal event then produces exactly one coordinator delivery whether the command succeeded, failed, expired, cancelled, or skipped. Required child failure may enter ordinary execution fail-fast before the coordinator can apply a fallback; the registration/API does not hide that policy interaction.

## 14. Ingress operations

### 14.1 Execute

Direct start creates `root` and its dispatch in the execution-start transaction. Plan start runs initial evaluation and creates the complete initial declaration batch. Coordinator start persists state and a ready start delivery. None invokes user code inline.

### 14.2 Issue

`Issue` is rejected in direct mode and against a genuinely terminal execution. Under the execution lock it performs declaration idempotency, ceiling validation, creation/journaling, and completion-counter maintenance. Command creation is not a plan-observable input and does not by itself invoke the plan. `Issue` has no dependencies in M1 and is required.

### 14.3 Publish

`Publish` validates canonical bytes and checks event-key idempotency before terminal rejection. A new event appends, satisfies stored waits, and becomes available to a coordinator's later indexed scan. In plan mode it invokes the plan in the same transaction only when the durable `Fact`/`Facts` routing set says the event changes a plan input; an event used solely by `Await` is resolved without plan code. Direct mode retains the fact without progression.

### 14.4 Cancel

Command and execution cancellation reuse the terminal processing state machine. Cancellation reasons are redacted and bounded. Execution cancellation closes the coordinator and selects no failure branch.

## 15. Journal builder and replay reducer

The engine produces semantic records without positions. `journal.Build(changeSet, firstPosition, dbNow)` assigns positions, event/entry IDs, and within-batch causation in deterministic order. Store encoding is versioned and cannot omit fields required by `History` reconstruction.

The paired pure reducer consumes journal entries and rebuilds settled projections:

- execution identity/status/counters derivable from accepted commands and terminal records;
- command identity/topology/result/terminal state;
- attempts and chosen retry schedules;
- closed child membership;
- coordinator state revision/inbox from transitions.

Current lease expiry/renewal and exact live dispatch ownership are outside replay. A conformance suite applies each committed change set to both reducer and database materializations and compares the settled subset.

## 16. Database-free `flowtest` engine

The engine powers public testing rather than a second mock implementation:

```go
flowtest.RunWorker(...)
flowtest.RunCommit(...)
flowtest.RunDirect(...)
flowtest.RunPlan(...)
flowtest.Simulate(...)
flowtest.AssertPlanDeterministic(...)
flowtest.RunCoordinator(...)
flowtest.AssertCanonicalStable(...)
```

Test worlds provide synthetic canonical facts, command states/results/outcomes, dependency edges, closed children, PostgreSQL timestamps, and command ceilings. Simulations return staged events/commands, normalized declarations, read availability, state revisions, terminal decisions, and retry times. The agent fixture delivers mixed tool outcomes, external input, and a delayed next turn without a process loop.

## 17. Test plan

### 17.1 Unit and property tests

- every retry-policy builder/invalid combination, bound precedence, deterministic jitter, interruption, and deadline cap;
- canonical equality and definition-default exclusion from reconciliation;
- Spawn/Emit duplicate equivalence, poisoning, Optional, and StartAfter validation;
- all dependency matrices and monotonic group transitions;
- early/late waits, `Within`, Delay, deadline, and immutable budget anchor;
- plan read availability, lazy value passes, forward references, ownership, cycles, growth, fragments, deterministic/routing assertions, and immediate-terminal fixed points without redundant normal passes;
- fail-fast branch declaration after failure, survivor inheritance, multiple failures, and skip cascades;
- direct/plan/coordinator completion and plan temporary-read failure exception;
- coordinator start, event matching, `OnOutcome`, retry, state discard, mixed fan-in, and explicit terminal decisions;
- journal deterministic order/causation and replay equivalence.

Property generators assert that accepted commands never disappear, terminal states never reopen, closed child sets never change, counters never become negative, and equivalent replay is idempotent.

### 17.2 PostgreSQL integration contracts

Engine/store integration tests cover all-or-nothing worker/plan/coordinator deltas; trigger visibility inside plan snapshots; commit-function rollback and plan-defect preservation; ceiling source-specific behavior; failure closure under concurrent running work; coordinator inbox fencing; and replay/live projection equality after crash injection.

### 17.3 Benchmarks

- plan complete-pass and lazy-loading cost at 10/100/1,000 commands;
- 1,000-member dependencies, wide failure closure, and cascading skip;
- 1,000 staged children canonicalization/validation;
- coordinator state/event processing at 1, 10, and 100 events per second per execution;
- replay and graph projection of a 1,000-command / multi-attempt history.

## 18. Acceptance conditions

- the engine imports neither `pgx` nor a database/clock package and performs no I/O;
- public generic type checks terminate at erased descriptors without losing durable name/version validation;
- retry policy is immutable canonical data, and all decisions use supplied PostgreSQL time;
- every handler output is staged and all-or-nothing, including delayed children and command ceiling;
- plan evaluation is pure, resumable for values, additive, deterministic-verifiable, and reconciles changed defaults correctly;
- failure dependency/plan resolution precedes fail-fast cancellation;
- coordinator success/failure outcomes are consumed once from the existing terminal event and state/inbox/output commit together;
- journal order and causation are deterministic and replay reconstructs settled projections;
- all unit, property, integration, agent, and benchmark suites pass.
