---
status: complete
---

# Component: Workflow DAG Engine

> **Superseded.** This is an earlier design for the same problem, structured as five layers — `MessageQueue`, `JobQueue`, Workflow DAG, `EventBus`, and `EventStore`. The active design lives in `specs/projects/flow` and uses a command / worker / event model with declarative plans instead. These documents are retained for their reasoning and their PostgreSQL mechanics, much of which carried forward; the APIs they describe are not current.

## 1. Purpose and Scope

This component coordinates durable directed acyclic graphs whose executable nodes are ordinary `JobQueue` jobs.

Responsibilities:

- workflow definitions, identities, current state, and immutable operational history;
- static graph construction and validation;
- atomic creation of runs, jobs, nodes, dependencies, and initially-ready dispatches;
- conditional dependency resolution, fan-out, joins, and deterministic dynamic mutation;
- workflow cancellation, deadlines, terminal-state derivation, and inspection;
- correlation of workflow nodes with job attempts and results.

Non-responsibilities:

- executing handlers or renewing job leases;
- replaying historical Go handler code;
- general-purpose expression evaluation;
- domain event sourcing, which belongs to `EventStore`;
- arbitrary rewiring of an already-persisted graph.

## 2. Semantic Decisions

### 2.1 A node is a job

Every workflow node owns exactly one job. Node current execution state is read from the job row rather than duplicated. `workflow_nodes` stores graph-specific state: activation, requirement policy, dependency counts, and deterministic node identity.

### 2.2 Resolved is distinct from satisfied

When a predecessor becomes terminal, each outgoing edge is resolved exactly once. Its condition is then evaluated:

- `succeeded` is satisfied only by predecessor state `succeeded`;
- `failed` is satisfied by `failed`, `expired`, or `discarded`;
- `terminal` is satisfied by every terminal job state, including `cancelled`.

After all incoming edges resolve:

- if every edge is satisfied, the node is activated and made available;
- if any edge is unsatisfied, the node is not activated and its blocked job becomes `cancelled` with reason `dependency_condition_unsatisfied`.

The second outcome is a branch skip, not a workflow cancellation. It is propagated as a terminal predecessor so downstream `terminal` branches can still run. This distinction prevents a failed condition from stranding the graph indefinitely.

### 2.3 Required nodes

`Required` controls run outcome only after a node is activated. An unactivated node skipped because its branch condition was false does not fail the run.

An activated required node in `failed`, `expired`, or `discarded` makes the run fail. An activated required node cancelled by explicit job cancellation also makes the run fail unless the whole workflow is already `cancelled` or `expired`.

### 2.4 Fail-fast

Runs default to fail-fast. The first activated required-node failure marks the run `failed` and cancels its remaining non-terminal jobs. With fail-fast disabled, independent branches finish and the run becomes `failed` only when all nodes are terminal.

## 3. Root Public Model

```go
package jobqueue

type WorkflowRunID string
type NodeKey string

type WorkflowState string

const (
    WorkflowStatePending   WorkflowState = "pending"
    WorkflowStateRunning   WorkflowState = "running"
    WorkflowStateSucceeded WorkflowState = "succeeded"
    WorkflowStateFailed    WorkflowState = "failed"
    WorkflowStateCancelled WorkflowState = "cancelled"
    WorkflowStateExpired   WorkflowState = "expired"
)

type DependencyCondition string

const (
    DependencySucceeded DependencyCondition = "succeeded"
    DependencyFailed    DependencyCondition = "failed"
    DependencyTerminal  DependencyCondition = "terminal"
)

type WorkflowRun struct {
    ID                WorkflowRunID
    Type              string
    Key               string
    DefinitionVersion string
    State             WorkflowState
    Input             json.RawMessage
    Output            json.RawMessage
    Metadata          map[string]string
    FailFast          bool
    ExpiresAt         time.Time
    Failure           *JobError
    CancelReason      string
    CancelActor       string
    NodeCount         int
    TerminalNodeCount int
    Version           int64
    CreatedAt         time.Time
    StartedAt         time.Time
    FinishedAt        time.Time
}

type WorkflowNode struct {
    WorkflowID             WorkflowRunID
    Key                    NodeKey
    JobID                  JobID
    Required               bool
    Activated              bool
    DependencyCount        int
    ResolvedDependencyCount int
    SatisfiedDependencyCount int
    CreatedAt              time.Time
    ActivatedAt            time.Time
}

type WorkflowDependency struct {
    WorkflowID WorkflowRunID
    NodeKey    NodeKey
    DependsOn  NodeKey
    Condition  DependencyCondition
    Resolved   bool
    Satisfied  bool
    ResolvedAt time.Time
}

type WorkflowEvent struct {
    WorkflowID   WorkflowRunID
    Sequence     int64
    Type         string
    NodeKey      NodeKey
    Data         json.RawMessage
    Actor        string
    CorrelationID string
    CausationID  string
    OccurredAt   time.Time
}
```

Empty timestamps and optional strings mean “not set.” Stored JSON values are copied before being returned to callers.

## 4. Graph Builder

### 4.1 Public graph types

```go
type WorkflowGraph struct {
    // unexported immutable representation after Build
}

type WorkflowGraphBuilder struct {
    // unexported mutable maps and size accounting
}

type WorkflowNodeSpec struct {
    Key       NodeKey
    Kind      string
    Queue     QueueName
    Args      json.RawMessage
    Optional  bool
    Priority  int16
    AvailableAt time.Time
    ExpiresAt time.Time
    MaxAttempts int
    ExecutionTimeout time.Duration
    Metadata  map[string]string
}

type WorkflowDependencySpec struct {
    NodeKey   NodeKey
    DependsOn NodeKey
    Condition DependencyCondition
}

func NewWorkflowGraph() *WorkflowGraphBuilder
func (b *WorkflowGraphBuilder) AddNode(WorkflowNodeSpec) error
func (b *WorkflowGraphBuilder) DependsOn(WorkflowDependencySpec) error
func (b *WorkflowGraphBuilder) FanOut([]WorkflowNodeSpec) error
func (b *WorkflowGraphBuilder) Join(WorkflowNodeSpec, []NodeKey, DependencyCondition) error
func (b *WorkflowGraphBuilder) Build() (WorkflowGraph, error)
```

`Optional=false` is the useful zero value and creates a required node. `Optional=true` excludes an activated failure of that node from determining the run result.

`FanOut` is equivalent to repeated `AddNode`. `Join` adds one ordinary node plus one edge from every predecessor. They do not create hidden execution behavior.

### 4.2 Validation

`Build` performs all of the following before any database work:

1. validates workflow/node/kind/queue names and JSON size limits;
2. rejects duplicate node keys and duplicate edges;
3. rejects missing predecessor or dependent nodes;
4. rejects self-edges and unsupported conditions;
5. runs Kahn's algorithm and rejects cycles;
6. verifies configured node, edge, and total-payload limits;
7. emits nodes and edges in stable lexical order.

`Build` rejects a node whose explicit availability is at or after its own explicit start deadline. `StartWorkflow` and dynamic mutation additionally cap every node deadline to the earlier of its own `ExpiresAt` and the workflow deadline, then repeat that validation. This makes the claim query enforce the workflow deadline even before asynchronous workflow-expiry maintenance runs.

Registration of handler kinds is process-local and may differ during rolling deployment. `StartWorkflow` therefore validates kinds against an optional allow-list supplied by the backend configuration, not against the transient registration set of the calling process. If no allow-list is configured, any syntactically valid kind is permitted and unhandled-backlog inspection exposes missing workers.

## 5. Workflow APIs

```go
type StartWorkflowRequest struct {
    ID                WorkflowRunID
    Type              string
    Key               string
    DefinitionVersion string
    Input             json.RawMessage
    Metadata          map[string]string
    Graph             WorkflowGraph
    FailFast          *bool
    ExpiresAt         time.Time
}

type StartWorkflowResult struct {
    Run     WorkflowRun
    Created bool
}

type WorkflowListOptions struct {
    Type   string
    State  WorkflowState
    Limit  int
    Cursor string
}

type WorkflowListPage struct {
    Runs       []WorkflowRun
    NextCursor string
}

type WorkflowGraphView struct {
    Run          WorkflowRun
    Nodes        []WorkflowNode
    Jobs         []Job
    Dependencies []WorkflowDependency
}

type WorkflowHistoryOptions struct {
    AfterSequence int64
    Limit         int
}

type WorkflowController interface {
    StartWorkflow(context.Context, StartWorkflowRequest) (StartWorkflowResult, error)
    CancelWorkflow(context.Context, CancelWorkflowRequest) (WorkflowRun, error)
}

type CancelWorkflowRequest struct {
    WorkflowID WorkflowRunID
    Reason     string
    Actor      string
}

type WorkflowReader interface {
    GetWorkflow(context.Context, WorkflowRunID) (WorkflowRun, error)
    ListWorkflows(context.Context, WorkflowListOptions) (WorkflowListPage, error)
    GetWorkflowGraph(context.Context, WorkflowRunID) (WorkflowGraphView, error)
    ReadWorkflowHistory(context.Context, WorkflowRunID, WorkflowHistoryOptions) ([]WorkflowEvent, error)
}
```

`FailFast == nil` means true. An explicitly false pointer disables fail-fast. `StartWorkflow` requires a non-empty type and definition version. The backend generates a UUIDv7 when `ID` is empty.

## 6. Dynamic Mutation API

Dynamic graph changes are buffered in a handler outcome and materialized only inside fenced job completion.

```go
type WorkflowMutation struct {
    Nodes        []WorkflowNodeSpec
    Dependencies []WorkflowDependencySpec
}

type WorkflowOperation = WorkflowMutation

type WorkflowMutationBuilder struct {
    // unexported current-workflow identity and deterministic maps
}

func (r *RunContext) WorkflowMutation() (*WorkflowMutationBuilder, error)
func (b *WorkflowMutationBuilder) AddNode(WorkflowNodeSpec) error
func (b *WorkflowMutationBuilder) DependsOn(WorkflowDependencySpec) error
func (b *WorkflowMutationBuilder) FanOut([]WorkflowNodeSpec) error
func (b *WorkflowMutationBuilder) Join(WorkflowNodeSpec, []NodeKey, DependencyCondition) error
func (b *WorkflowMutationBuilder) Build() (WorkflowMutation, error)
```

Rules:

- the current job must belong to a workflow;
- every added node key is stable and unique within the run;
- every new edge must point to a node added by this mutation;
- an edge predecessor may be an existing node or another node in this mutation;
- existing nodes and edges cannot be deleted or changed;
- cycles among new nodes are rejected by the builder;
- node and edge counts are bounded both per mutation and per run;
- a retried mutation with byte-equivalent node/edge definitions is idempotent;
- the same key with different job or edge content returns `ErrConflict` and rolls back completion.

`WorkflowOperation` is the immutable form stored in `Outcome`; it aliases `WorkflowMutation` so the handler-facing builder and finalizer-facing outcome use one representation. Outcome construction copies its slices and payloads.

## 7. PostgreSQL Data Model

The workflow migration is applied after the job migration. It adds the workflow foreign keys and indexes which cannot exist when the job tables are first created.

### 7.1 Runs

```sql
CREATE TABLE jobqueue.workflow_runs (
    id uuid PRIMARY KEY,
    workflow_type text NOT NULL,
    workflow_key text,
    definition_version text NOT NULL,
    state text NOT NULL CHECK (state IN
        ('pending','running','succeeded','failed','cancelled','expired')),
    input jsonb NOT NULL,
    output jsonb,
    metadata jsonb NOT NULL DEFAULT '{}'::jsonb,
    fail_fast boolean NOT NULL DEFAULT true,
    expires_at timestamptz,
    failure jsonb,
    cancel_reason text,
    cancel_actor text,
    request_fingerprint bytea NOT NULL CHECK (octet_length(request_fingerprint) = 32),
    node_count integer NOT NULL CHECK (node_count >= 0),
    terminal_node_count integer NOT NULL DEFAULT 0
        CHECK (terminal_node_count >= 0 AND terminal_node_count <= node_count),
    activated_required_failure_count integer NOT NULL DEFAULT 0
        CHECK (activated_required_failure_count >= 0),
    version bigint NOT NULL DEFAULT 1 CHECK (version > 0),
    next_event_sequence bigint NOT NULL DEFAULT 1 CHECK (next_event_sequence > 0),
    created_at timestamptz NOT NULL,
    started_at timestamptz,
    finished_at timestamptz,
    UNIQUE (workflow_type, workflow_key)
);

CREATE INDEX workflow_runs_state_created_idx
    ON jobqueue.workflow_runs (state, created_at, id);
CREATE INDEX workflow_runs_deadline_idx
    ON jobqueue.workflow_runs (expires_at, id)
    WHERE state IN ('pending','running') AND expires_at IS NOT NULL;
```

The uniqueness constraint treats `NULL` keys as non-idempotent starts. Empty keys are normalized to `NULL`.

### 7.2 Nodes

```sql
CREATE TABLE jobqueue.workflow_nodes (
    workflow_id uuid NOT NULL REFERENCES jobqueue.workflow_runs(id) ON DELETE RESTRICT,
    node_key text NOT NULL,
    job_id uuid NOT NULL UNIQUE REFERENCES jobqueue.jobs(id) ON DELETE RESTRICT,
    required boolean NOT NULL,
    activated boolean NOT NULL DEFAULT false,
    dependency_count integer NOT NULL CHECK (dependency_count >= 0),
    resolved_dependency_count integer NOT NULL DEFAULT 0
        CHECK (resolved_dependency_count >= 0 AND resolved_dependency_count <= dependency_count),
    satisfied_dependency_count integer NOT NULL DEFAULT 0
        CHECK (satisfied_dependency_count >= 0 AND satisfied_dependency_count <= resolved_dependency_count),
    created_at timestamptz NOT NULL,
    activated_at timestamptz,
    PRIMARY KEY (workflow_id, node_key),
    UNIQUE (workflow_id, job_id)
);

ALTER TABLE jobqueue.jobs
    ADD CONSTRAINT jobs_workflow_run_fk
    FOREIGN KEY (workflow_run_id) REFERENCES jobqueue.workflow_runs(id) ON DELETE RESTRICT;

ALTER TABLE jobqueue.jobs
    ADD CONSTRAINT jobs_workflow_identity_unique
    UNIQUE (workflow_run_id, node_key, id);

CREATE UNIQUE INDEX jobs_workflow_node_unique_idx
    ON jobqueue.jobs (workflow_run_id, node_key)
    WHERE workflow_run_id IS NOT NULL;

ALTER TABLE jobqueue.workflow_nodes
    ADD CONSTRAINT workflow_nodes_job_identity_fk
    FOREIGN KEY (workflow_id, node_key, job_id)
    REFERENCES jobqueue.jobs(workflow_run_id, node_key, id)
    ON DELETE RESTRICT;
```

The job-row constraint requires `workflow_run_id` and `node_key` to be both null or both non-null. The composite foreign key proves that a node cannot accidentally reference a job belonging to another workflow/key. A migration validation asserts existing shape before adding these constraints.

### 7.3 Dependencies

```sql
CREATE TABLE jobqueue.workflow_dependencies (
    workflow_id uuid NOT NULL,
    node_key text NOT NULL,
    depends_on_node_key text NOT NULL,
    condition text NOT NULL CHECK (condition IN ('succeeded','failed','terminal')),
    resolved boolean NOT NULL DEFAULT false,
    satisfied boolean NOT NULL DEFAULT false,
    resolved_at timestamptz,
    PRIMARY KEY (workflow_id, node_key, depends_on_node_key),
    FOREIGN KEY (workflow_id, node_key)
        REFERENCES jobqueue.workflow_nodes(workflow_id, node_key) ON DELETE RESTRICT,
    FOREIGN KEY (workflow_id, depends_on_node_key)
        REFERENCES jobqueue.workflow_nodes(workflow_id, node_key) ON DELETE RESTRICT,
    CHECK (node_key <> depends_on_node_key),
    CHECK (NOT satisfied OR resolved),
    CHECK ((resolved_at IS NULL) = (NOT resolved))
);

CREATE INDEX workflow_dependencies_predecessor_idx
    ON jobqueue.workflow_dependencies
       (workflow_id, depends_on_node_key, node_key);
```

### 7.4 Operational history

```sql
CREATE TABLE jobqueue.workflow_events (
    workflow_id uuid NOT NULL REFERENCES jobqueue.workflow_runs(id) ON DELETE RESTRICT,
    sequence bigint NOT NULL CHECK (sequence > 0),
    event_type text NOT NULL,
    node_key text,
    data jsonb NOT NULL DEFAULT '{}'::jsonb,
    actor text,
    correlation_id text,
    causation_id text,
    occurred_at timestamptz NOT NULL,
    PRIMARY KEY (workflow_id, sequence)
);
```

History rows are never updated. Per-run sequence numbers are reserved by incrementing `workflow_runs.next_event_sequence` while that run row is locked.

## 8. Static Start Algorithm

Before opening a transaction, canonicalize the graph and compute a request fingerprint over workflow type/key/version/input/metadata/fail-fast/deadline and canonical node/edge definitions.

In one `READ COMMITTED` transaction:

1. obtain database time;
2. if an explicit run ID or workflow key already exists, lock it and compare the stored fingerprint; return the existing run on equality or `ErrConflict` otherwise;
3. insert the run in `pending` with its node count and fingerprint;
4. generate UUIDv7 job IDs in Go for every node in this newly-created run; an uncertain retry which finds the committed run returns its stored graph rather than recreating IDs;
5. insert job rows in lexical node-key order; zero-dependency jobs start `available`, all others start `blocked`; each job receives the node's effective start deadline and execution-timeout override;
6. insert node and dependency rows in lexical order;
7. insert managed dispatches only for zero-dependency jobs, using their absolute availability time;
8. mark every zero-dependency node activated;
9. set the run `running` and `started_at` when at least one node exists; an empty graph completes `succeeded` immediately;
10. append `workflow.started`, `node.created`, and initial `node.activated` events using a contiguous per-run sequence range;
11. notify after durable mutations and commit.

The fingerprint is a fixed SHA-256 byte column in the implementation migration even if inspection APIs do not expose it.

## 9. Completion and Dependency Resolution

Workflow-aware job settlement is part of the job completion/failure transaction and uses the global lock order.

### 9.1 Lock set

The transaction:

1. locks the workflow run;
2. discovers the transitively affected branch using a recursive CTE over outgoing dependencies;
3. locks affected jobs in stable UUID order and affected dependency rows in stable node-key order;
4. revalidates the current job's fence;
5. applies its ordinary job terminal transition;
6. resolves graph state in memory from the locked snapshot and writes guarded updates.

The graph is acyclic and bounded, so the in-memory work queue terminates. New dynamic nodes are included before resolution when both occur in the same outcome.

The workflow-run row deliberately serializes terminal transitions within one run so counters, per-run history sequence, dependency propagation, and derived run outcome have one clear order. Consequently, very wide workflows with extremely short child handlers may bottleneck on this row even when worker capacity is abundant; this is an accepted initial correctness/throughput trade-off and a measured optimization target rather than an implicit scalability claim.

### 9.2 Edge resolution

For each newly terminal node, update unresolved outgoing edges with `resolved = true`, the condition result, and database time. Guard every update with `resolved = false`; zero affected rows means another idempotent path already resolved it.

For each affected dependent, atomically add the number resolved and satisfied. Once `resolved_dependency_count = dependency_count`:

- all satisfied and the effective start deadline remains in the future: update the node to activated and its job from `blocked` to `available`, create its dispatch at `max(database now, jobs.available_at)`, and record activation;
- all satisfied but the effective start deadline has passed: mark the node activated and its job `expired`, count the required failure when applicable, and propagate its terminal state without creating dispatch;
- any unsatisfied: update its job from `blocked` to `cancelled`, record a structured branch-skip reason, increment terminal counts, and enqueue that node for outgoing-edge propagation.

The `blocked -> available` and `blocked -> cancelled` predicates make activation or skip exactly once.

### 9.3 Run outcome

After propagation:

- increment terminal and activated-required-failure counts by the transitions performed in this transaction;
- if fail-fast and a new required failure exists, set the run `failed`, fence/cancel remaining jobs, and propagate their terminal state;
- otherwise, if every node is terminal, choose `failed` when activated-required-failure count is nonzero, else `succeeded`;
- a run already `cancelled` or `expired` never changes state;
- set `finished_at` only on the first transition to terminal;
- append all workflow events before commit.

Workflow output is an optional explicit outcome field. If no output is set, it remains null; the engine does not infer output by merging node results.

Explicit `CancelJob` for a workflow node runs this same workflow-aware terminal algorithm. Cancelling a blocked node is an explicit operator decision, so it marks the node activated before cancellation; a required node therefore fails the run. The internal `dependency_condition_unsatisfied` branch skip is the only cancellation path which intentionally leaves `activated=false`.

## 10. Dynamic Mutation Algorithm

Before handler execution ends, the builder validates local shape and limits. During fenced completion:

1. lock the run first and reject mutation if it is terminal;
2. canonicalize operations and validate the resulting run-size limits;
3. load any existing nodes/edges with the supplied keys;
4. compare exact canonical specifications for idempotency, returning `ErrConflict` for mismatches;
5. insert absent job rows as `blocked`, followed by nodes and edges in stable order;
6. calculate dependency counts from the complete inserted edge set;
7. for dependencies whose predecessors are already terminal, resolve conditions immediately;
8. activate, skip, or retain new nodes using the same algorithm as ordinary completion;
9. increase `workflow_runs.node_count`, append mutation events, then continue completion.

New node specifications and edge definitions are persisted in canonical form so retries can compare meaning, not incidental JSON encoding. The operation limit defaults are 100 nodes and 1,000 edges per completion; the run defaults are 10,000 nodes and 100,000 edges. All are configurable downward or upward within hard safety caps.

## 11. Cancellation and Deadlines

### 11.1 Explicit cancellation

`CancelWorkflow` locks the run. If already terminal, an equal repeat returns the run; an incompatible terminal state returns `ErrTerminal`.

When cancellation wins:

1. set run state, reason, actor, version, and `finished_at`;
2. lock all non-terminal node jobs in UUID order;
3. transition `blocked`, `available`, and `retrying` jobs to `cancelled` and delete dispatches;
4. transition running jobs to `cancelled`, clear their lease token, delete dispatches, and mark attempts cancelled;
5. append workflow and node cancellation events;
6. commit, then signal matching in-process handler contexts.

Clearing fences prevents a late handler from committing success. Cancellation is cooperative for handler goroutines and cannot undo external effects.

### 11.2 Deadline maintenance

The maintenance loop claims due non-terminal workflow runs with `FOR UPDATE SKIP LOCKED`. It applies the cancellation algorithm with state `expired`, actor `system`, and reason `workflow_deadline_reached`. A concurrently completed run wins if the maintenance predicate no longer matches after locking.

## 12. Inspection and Pagination

Run lists order by `(created_at, id)` and encode both values plus query filters in an opaque versioned cursor. Graph reads use one repeatable-read transaction when a caller requests a self-consistent snapshot; the default read may observe a newer job state than run metadata but never returns dangling references.

History reads order by per-run sequence and return at most the configured maximum. Attempt inspection is delegated to `JobReader` using the node's job ID.

## 13. Error Mapping

| Condition | Public error |
|---|---|
| missing run | `ErrNotFound` with resource `workflow` |
| duplicate start, identical fingerprint | success with `Created=false` |
| duplicate key/ID, different fingerprint | `ErrConflict` |
| invalid graph/cycle/condition/limit | `ErrInvalid` |
| mutation after terminal run | `ErrTerminal` |
| mutation key with different content | `ErrConflict` |
| lost current job lease during mutation | `ErrLeaseLost` |
| unsupported state transition | `ErrInvalidState` |

Database constraint names are mapped explicitly; raw driver strings are never part of the public contract.

## 14. Observability

Emit commit-aware observations for:

- run start, state change, cancellation, expiry, and duration;
- node creation, activation, branch skip, and terminal state;
- edges resolved and conditions unsatisfied;
- mutation sizes, conflicts, and validation failures;
- ready-node dispatch latency;
- workflow history append count;
- graph size and dependency-resolution transaction latency.

Dimensions include workflow type/version, run ID, node key, job kind, and outcome. Payloads and results are excluded.

## 15. Test Plan

### 15.1 Builder and property tests

- unique keys, missing references, self-edges, duplicate edges, and conditions;
- cycle detection, including dynamic cycles among new nodes;
- canonical ordering and fingerprint stability across map/JSON ordering;
- fan-out/join equivalence to primitive operations;
- fuzzed DAGs preserve topological ordering and size bounds.

### 15.2 Start and idempotency

- atomic static creation and only zero-dependency dispatches;
- repeat by explicit ID or `(type,key)` returns the same run;
- conflicting version/input/graph returns `ErrConflict`;
- concurrent identical starts produce one run and one job per node;
- empty graph succeeds immediately.

### 15.3 Dependency semantics

- success, failure, and terminal conditions;
- false branches skip instead of remaining blocked;
- skipped branches propagate terminal dependencies;
- a join activates exactly once under concurrent predecessor completion;
- nonnegative counters and no duplicate dispatch under retries;
- optional skipped and failed nodes produce the specified run result.

### 15.4 Mutation and crash recovery

- deterministic retry of the same mutation;
- conflicting reused node/edge key rolls back completion;
- existing-to-new and new-to-new edges work; existing rewiring is rejected;
- predecessors already terminal resolve new edges immediately;
- crash at every completion fault point converges without duplicate nodes.

### 15.5 Cancellation and deadline

- cancel versus completion race has one durable winner;
- running job fences are invalidated and handler contexts signalled;
- cancellation removes every pending dispatch;
- deadline versus final completion race;
- repeated same cancellation is idempotent.

### 15.6 History and scale

- per-run sequences are contiguous under concurrent node completion;
- current-state changes and history commit atomically;
- bounded fan-out/join benchmark at configured maximums;
- benchmark `N` parallel trivial children completing concurrently in one run, including 10 ms handlers and at least 1,000 ready nodes, and report workflow-row lock wait/commit throughput;
- pagination remains stable during concurrent new runs/events;
- `go test -race` covers context signalling and runtime bookkeeping.

## 16. Acceptance Conditions

This component is complete when:

1. static DAGs are validated and created atomically;
2. each node is an ordinary fenced job and initially blocked nodes have no dispatch;
3. conditional branches always resolve to activation or deterministic skip;
4. concurrent joins activate once without negative counters or duplicate dispatch;
5. deterministic dynamic fan-out and joins survive handler retry and uncertain commit;
6. required/optional and fail-fast policies produce documented run outcomes;
7. cancellation and deadlines fence every unfinished node;
8. current graph inspection and immutable ordered history agree after commit;
9. all PostgreSQL concurrency, crash, migration, and property tests pass.
