---
status: draft
---

# Component: Execution Engine

## 1. Purpose and scope

The engine is the code that runs inside the settle transaction. It owns plan evaluation, dependency resolution, fail-fast closure computation, completion, and coordinator inbox delivery.

Responsibilities: the execution snapshot, the `*Plan` binding, reconciliation, clause satisfaction, the failure closure, completion evaluation, and coordinator delivery.

Non-responsibilities: SQL (`schema.md`), process lifecycle, claiming, leases, and notification (`runtime.md`), and public type shapes (functional spec §4).

The engine is a pure function of its snapshot plus the handler's staged output. It performs no I/O of its own: the caller loads a snapshot, the engine computes a **change set**, and the caller writes it. This is what makes the whole component unit-testable without a database.

## 2. The snapshot

Every settle transaction begins by locking the execution row and loading one snapshot with the three queries in `schema.md` §5.3.

```go
type Snapshot struct {
    Execution  ExecutionRow
    Commands   map[string]CommandRow      // keyed by command_key
    Clauses    map[string][]ClauseRow     // keyed by command_key
    EventIndex map[NameVersion]EventStat  // counts and first position
    Reads      []PlanRead                 // the previous evaluation's consulted set

    facts func(NameVersion) []FactRow     // lazy, memoized payload loader
}
```

The snapshot is immutable during a transaction. Because the execution row lock is held, nothing can change underneath it, and two evaluations for one execution can never interleave — the property that lets a plan be a plain pure function with no concurrency contract.

`facts` is lazy: the event *index* is always loaded (names, versions, counts), but payloads are fetched only for names a plan actually reads, and memoized within the transaction.

## 3. The change set

The engine returns a description of what should be written, never writing anything itself.

```go
type ChangeSet struct {
    NewCommands   []NewCommand    // ordered by command_key
    NewClauses    []NewClause
    ClauseUpdates []ClauseUpdate  // satisfied / unsatisfiable transitions
    Promote       []string        // command keys: pending → ready
    Skip          []string        // command keys: pending → skipped
    Cancel        []string        // command keys: fail-fast cancellation
    Reads         []PlanRead      // replaces plan_reads wholesale
    AbsentReads   int
    OpenDelta     int             // adjustment to executions.open_commands
    Outcome       *Outcome        // set only when the execution becomes terminal
    WakeLanes     []string        // lanes with newly runnable work
}
```

An empty change set is valid and common — most events change nothing.

## 4. Plan evaluation

### 4.1 Deciding whether to evaluate

Per FS §10.3, evaluation is required only when the plan's inputs can have changed:

```go
func mustEvaluate(s *Snapshot, trigger Trigger) bool {
    switch trigger.Kind {
    case TriggerStart:
        return true
    case TriggerCommandState:
        return true                      // a declared node changed state
    case TriggerEvent:
        if len(s.Reads) == 0 {
            return true                  // never evaluated, or consulted nothing
        }
        return s.readsName(trigger.EventName)
    }
    return false
}
```

An event whose name appears in no prior read cannot change the plan's output, because the plan is pure and its inputs are exactly the reads plus root arguments plus declared node states. Skipping is therefore sound, not a heuristic. An implementation may always return `true` without changing behavior — only cost.

### 4.2 The Plan binding

`*Plan` is a recorder over the snapshot. It never touches the database.

```go
type Plan struct {
    snap     *Snapshot
    declared map[string]*Node   // this evaluation's declarations
    order    []string           // declaration order, for stable diagnostics
    reads    map[readKey]bool   // key → present
    err      error              // first defect encountered
}
```

`Do` records a declaration and returns a `*Node` whose builder methods accumulate clauses. `Fact`, `Facts`, and `Result` read the snapshot **and record the read**:

```go
func Fact[T any](p *Plan, e Event[T]) (T, bool) {
    k := readKey{kind: "fact", name: e.Name(), version: e.Version()}
    rows := p.snap.facts(k.nv())
    p.reads[k] = len(rows) > 0
    if len(rows) == 0 {
        var zero T
        return zero, false
    }
    var v T
    if err := canonical.Unmarshal(rows[0].Payload, &v); err != nil {
        p.fail(fmt.Errorf("decoding %s: %w", k.name, err))
        return v, false
    }
    return v, true
}

func Result[A, R any](p *Plan, key string, c Command[A, R]) (R, bool) {
    var zero R
    if _, declared := p.declared[key]; !declared {
        p.fail(fmt.Errorf("Result(%q): key not declared in this evaluation", key))
        return zero, false      // → ErrInvalid, a plan defect
    }
    p.reads[readKey{kind: "result", name: key}] = false
    row, ok := p.snap.Commands[key]
    if !ok || row.State != "succeeded" {
        return zero, false      // recorded as an absent read
    }
    p.reads[readKey{kind: "result", name: key}] = true
    ...
}
```

Two rules fall directly out of this shape. `Result` on an undeclared key is a defect, because a plan cannot depend on work it never asked for. And **every read is recorded with whether it found anything**, which is what feeds `absent_reads` and the completion rule in §7.

### 4.3 Purity enforcement

The plan function is documented as pure. The engine enforces what it can cheaply:

- the `*Plan` exposes no I/O, no clock, and no randomness — the only reads available are snapshot reads;
- a panic aborts the transaction and is reported as a plan defect, never as a command failure;
- `flowtest` provides a determinism assertion that runs a plan twice over one snapshot and diffs the declared set and read set.

Non-determinism that survives all three — a plan reading a package-level variable, say — produces a `payload_hash` mismatch on the next evaluation and surfaces as `ErrConflict` against the declaring command. That is a loud failure, not a silent divergence.

### 4.4 Reconciliation

```go
func reconcile(s *Snapshot, p *Plan, cs *ChangeSet) error {
    for _, key := range p.order {
        node := p.declared[key]
        existing, present := s.Commands[key]

        switch {
        case !present:
            clauses := buildClauses(node)
            unsatisfied := countUnsatisfied(s, clauses)
            state := "ready"
            if unsatisfied > 0 {
                state = "pending"
            }
            cs.NewCommands = append(cs.NewCommands, newCommand(node, state, unsatisfied))
            cs.NewClauses = append(cs.NewClauses, clauses...)
            cs.OpenDelta++
            if state == "ready" {
                cs.WakeLanes = append(cs.WakeLanes, node.lane)
            }

        case existing.NameVersion() != node.NameVersion() ||
             !bytes.Equal(existing.PayloadHash, node.PayloadHash):
            return conflict(key, existing, node)   // plan defect

        default:
            // already materialized and identical: no-op
        }
    }
    // Keys previously declared but absent from this evaluation are retained
    // untouched. A plan only grows (FS §10.2).
    return nil
}
```

`NewCommands` is emitted in `command_key` order so concurrent executions inserting overlapping keys cannot deadlock on the unique index.

The `default` branch is the common case at steady state: an execution with 40 nodes re-evaluated on its 40th event does 40 map lookups and writes nothing.

## 5. Dependency resolution

### 5.1 Clause satisfaction

A clause is evaluated against member states held in the snapshot:

```go
func evaluate(s *Snapshot, c ClauseRow) (satisfied, unsatisfiable bool) {
    switch c.Kind {
    case AllSucceeded:
        return allAre(s, c.Members, succeeded), anyIs(s, c.Members, unsuccessfulTerminal)
    case AllSettled:
        return allAre(s, c.Members, terminal), false          // never unsatisfiable
    case AllUnsuccessful:
        return allAre(s, c.Members, unsuccessfulTerminal), anyIs(s, c.Members, succeeded)
    case AtLeast:
        n := countIs(s, c.Members, succeeded)
        remaining := countIs(s, c.Members, nonTerminal)
        return n >= c.Threshold, n+remaining < c.Threshold
    case AwaitEvent:
        return allEventsPresent(s, c.MemberEvents), false     // append-only: monotonic
    }
}
```

Two properties matter. `AllSettled` and `AwaitEvent` can never become unsatisfiable — everything eventually settles, and events are append-only — so they can only delay, never skip. And `AtLeast` detects impossibility as soon as successes plus still-possible members fall below the threshold, rather than waiting for every member to settle.

### 5.2 The resolution pass

Resolution is an in-memory work queue over the snapshot, not recursive SQL:

```go
func resolve(s *Snapshot, seeds []string, cs *ChangeSet) {
    queue := append([]string(nil), seeds...)   // keys that just became terminal
    seen := map[string]bool{}

    for len(queue) > 0 {
        k := queue[0]
        queue = queue[1:]
        if seen[k] { continue }
        seen[k] = true

        for depKey, clauses := range s.Clauses {
            dep := s.Commands[depKey]
            if dep.State != "pending" { continue }

            changed := false
            for i, c := range clauses {
                if c.Satisfied || c.Unsatisfiable || !c.names(k) { continue }
                sat, unsat := evaluate(s, c)
                if !sat && !unsat { continue }
                cs.ClauseUpdates = append(cs.ClauseUpdates, ClauseUpdate{depKey, i, sat, unsat})
                s.markClause(depKey, i, sat, unsat)   // snapshot mutated for this pass only
                changed = true
            }
            if !changed { continue }

            switch {
            case s.anyUnsatisfiable(depKey):
                cs.Skip = append(cs.Skip, depKey)
                s.setState(depKey, "skipped")
                cs.OpenDelta--
                queue = append(queue, depKey)          // skipped is terminal: propagate
            case s.allSatisfied(depKey):
                cs.Promote = append(cs.Promote, depKey)
                s.setState(depKey, "ready")
                cs.WakeLanes = append(cs.WakeLanes, dep.Lane)
            }
        }
    }
}
```

Termination is guaranteed: the growth rule (FS §10.2) says a new edge's destination is always a node created in the current declaration, so the graph is acyclic by construction, and `seen` bounds the pass to one visit per key regardless.

The snapshot is mutated locally so that cascading resolution within a single pass sees its own effects — a skip that makes a downstream clause unsatisfiable resolves in the same transaction rather than waiting for another event.

Every emitted `ClauseUpdate` is written with a `NOT satisfied AND NOT unsatisfiable` guard (`schema.md` §5.6), so a concurrent or replayed pass cannot double-apply.

### 5.3 Await satisfaction

When events are appended, any `await_event` clause naming one of those `(name, version)` pairs is re-evaluated in the same pass, seeded by name rather than by command key. Since satisfaction is monotonic, this needs no bookkeeping beyond the clause row.

## 6. Fail-fast and the failure closure

FS §6.3 fixes the ordering, and the engine implements it exactly:

```go
func applyFailure(s *Snapshot, failedKey string, cs *ChangeSet) {
    // 1. the terminal state is already recorded by the caller

    // 2. resolve edges FIRST — this is what gives AfterFailed branches their chance
    resolve(s, []string{failedKey}, cs)

    if s.Execution.Optional(failedKey) || !s.Execution.FailFast {
        return
    }

    // 3. mark failing and cancel everything outside the failure-handling closure
    cs.Failing = true
    keep := failureClosure(s, cs.Promote)
    for key, cmd := range s.Commands {
        if terminal(cmd.State) || keep[key] { continue }
        if cmd.State == "running" { continue }        // already running: let it finish
        cs.Cancel = append(cs.Cancel, key)
        cs.OpenDelta--
    }
}

// The closure is the nodes just made runnable, plus everything transitively
// reachable from them through dependency edges.
func failureClosure(s *Snapshot, promoted []string) map[string]bool {
    keep := map[string]bool{}
    queue := append([]string(nil), promoted...)
    for len(queue) > 0 {
        k := queue[0]; queue = queue[1:]
        if keep[k] { continue }
        keep[k] = true
        for depKey, clauses := range s.Clauses {
            for _, c := range clauses {
                if c.names(k) { queue = append(queue, depKey) }
            }
        }
    }
    return keep
}
```

Step 2 strictly preceding step 3 is the whole point: resolving edges first is what turns a `refund` node from cancelled-before-it-ran into runnable. Running commands are also preserved, because cancelling work already in flight would strand external effects mid-way.

With `fail_fast = false` the execution still records `failing`, so inspection is truthful, but nothing is cancelled and the outcome is computed once every command settles.

## 7. Completion

```go
func evaluateCompletion(s *Snapshot, cs *ChangeSet) {
    if s.Execution.CoordinatorDriven() {
        return                       // only SucceedExecution / FailExecution decide
    }
    open := s.Execution.OpenCommands + cs.OpenDelta
    if open != 0 || cs.AbsentReads != 0 || len(cs.NewCommands) != 0 {
        return
    }
    if s.Execution.Failing || anyRequiredUnsuccessful(s) {
        cs.Outcome = &Outcome{Status: "failed", Failure: summarize(s)}
        return
    }
    cs.Outcome = &Outcome{Status: "succeeded"}
}
```

Three counters, all on the already-locked execution row. `AbsentReads` is the condition that prevents a plan branching on a never-arriving fact from reporting false success (FS §10.1).

The engine's decision is advisory: `complete_execution` re-checks `open_commands = 0 AND absent_reads = 0` in its `WHERE` clause (`schema.md` §5.8), so an engine bug cannot produce a false success. Zero rows means the execution was no longer eligible, and the transaction proceeds without completing.

## 8. Coordinator delivery

A hand-written coordinator replaces steps 4–7 with one handler invocation, but the transaction shape is identical.

```go
func deliver(s *Snapshot, c CoordinatorRow, ev EventRow, h Handler, cs *ChangeSet) error {
    if !h.Subscribes(ev.Name, ev.Version) {
        cs.AdvanceInbox = ev.Position          // not interesting: skip forward
        return nil
    }
    state := decode(c.CoordState)
    scope := newScope(s, c, ev, state)

    if err := h.Invoke(ctx, scope, ev); err != nil {
        return err                             // nothing commits; retry per policy
    }

    cs.CoordState = encode(scope.State)
    cs.NewCommands = append(cs.NewCommands, scope.stagedCommands...)
    cs.NewEvents = append(cs.NewEvents, scope.stagedEvents...)
    cs.AdvanceInbox = ev.Position
    return nil
}
```

Delivery selects the lowest-positioned event above `inbox_position` and advances strictly one at a time, so ordering is preserved. A failed delivery does not advance the position — the intended head-of-line blocking of FS §9.4 — and the coordinator retries under the standard policy until it succeeds or exhausts its budget, at which point it fails and fails the execution.

Because a new instance starts at position 0, historical facts are delivered in order with no replay mechanism. Uninteresting events advance the cursor without invoking a handler, so a coordinator subscribing to one name does not walk every event individually at delivery time.

Commands staged by a coordinator carry no clauses; a coordinator's `Send` creates directly runnable work.

## 9. Interaction with the settle transaction

The runtime drives the engine in this order (architecture §10):

| Step | Owner |
|---|---|
| lock execution, load snapshot | runtime |
| verify attempt fence | runtime |
| allocate positions, append events | runtime |
| mark command terminal, adjust counters | runtime |
| **resolve dependencies** | engine §5 |
| **evaluate plan and reconcile**, or **deliver to coordinator** | engine §4, §8 |
| **apply fail-fast** | engine §6 |
| run `OnCommit` callbacks | runtime |
| **evaluate completion** | engine §7 |
| write change set, notify, commit | runtime |

Resolution runs before evaluation so a plan sees the post-resolution node states in its snapshot — a plan that branches on `Result` of a command completing in this very transaction observes it immediately rather than one event later.

## 10. Test plan

### 10.1 Pure engine tests

No database. A `Snapshot` is constructed in memory, the engine runs, and the `ChangeSet` is asserted.

- **Clause satisfaction** — a table-driven matrix over all five kinds × member state combinations, asserting `(satisfied, unsatisfiable)`; property test that `AllSettled` and `AwaitEvent` are never unsatisfiable.
- **Resolution** — chains, diamonds, joins, fan-out, cascading skips; property tests that `unsatisfied_clauses` never goes negative, a command is promoted at most once, and a pass visits each key at most once.
- **Reconciliation** — first evaluation creates all; second creates none; changed payload conflicts; a node no longer declared is retained; insert order is by key.
- **Read recording** — present and absent reads recorded correctly; `Result` on an undeclared key is a defect; `absent_reads` matches the count of absent reads.
- **Fail-fast** — `AfterFailed` dependents survive; unrelated pending work is cancelled; running work is preserved; `fail_fast = false` cancels nothing; optional failures change nothing.
- **Completion** — succeeds only at all three conditions; a pending absent read blocks success; a newly declared node blocks success.
- **Determinism** — a plan evaluated twice over one snapshot yields identical declarations and reads.

### 10.2 Integration tests

Against real PostgreSQL, through the runtime:

- plan evaluated once per settle produces no duplicate commands across 1,000 events;
- a fact published before its consuming node is declared still satisfies it;
- a plan branching on a never-arriving fact keeps its execution running to its deadline;
- a refund branch runs to completion under fail-fast, and the execution stays non-terminal until it resolves;
- cascading skips propagate through three levels in one transaction;
- crash injection at each step of §9 leaves the execution recoverable and consistent;
- a coordinator's inbox never skips or reorders under concurrent publication;
- `complete_execution` rejects a false success even when the engine proposes one.

### 10.3 Benchmarks

- evaluation cost at 10, 100, and 1,000 nodes, confirming FS §10.5's model;
- resolution cost for a 1,000-member join;
- failure-closure computation on a wide graph;
- evaluation-skip rate on a workload dominated by unconsulted event names.

## 11. Acceptance conditions

- the engine performs no I/O and is fully exercised without a database;
- resolution needs no reverse-edge index and no recursive SQL;
- dependency resolution strictly precedes fail-fast cancellation;
- plan evaluation strictly follows dependency resolution;
- every clause update is written with a resolve-once guard;
- an undeclared `Result` read is a defect, and every read is recorded with presence;
- completion requires zero open commands, zero absent reads, and no new declarations, and is re-verified in SQL;
- coordinator delivery advances one position at a time and blocks on failure;
- all engine, integration, property, and benchmark tests pass.
