---
status: complete
---

# Component: JobQueue and Worker Runtime

## 1. Purpose and Scope

This component turns durable job records into fenced handler executions.

Responsibilities:

- job lanes and enqueue APIs;
- job identity, active uniqueness, and deterministic child spawning;
- managed dispatch rows and reconciliation;
- registered-kind claim filtering;
- job attempts and retry-budget accounting;
- handler registry, execution, lease renewal, and shutdown;
- buffered outcomes and per-kind PostgreSQL finalizers;
- completion, failure, cancellation, expiration, and administrative retry;
- job and attempt inspection;
- worker capability registration and unhandled-backlog queries.

Non-responsibilities:

- raw message retention and dead-lettering;
- workflow dependency algorithms beyond fields/hooks needed for milestone 2;
- domain event stream ordering beyond buffered extension points;
- external side-effect idempotency.

## 2. Root Public Model

### 2.1 States and IDs

```go
package jobqueue

type JobID string
type AttemptID string

type JobState string

const (
    JobStateAvailable JobState = "available"
    JobStateRunning   JobState = "running"
    JobStateRetrying  JobState = "retrying"
    JobStateSucceeded JobState = "succeeded"
    JobStateFailed    JobState = "failed"
    JobStateCancelled JobState = "cancelled"
    JobStateExpired   JobState = "expired"
    JobStateDiscarded JobState = "discarded"
    JobStateBlocked   JobState = "blocked"
)

type AttemptState string

const (
    AttemptStateRunning     AttemptState = "running"
    AttemptStateSucceeded   AttemptState = "succeeded"
    AttemptStateFailed      AttemptState = "failed"
    AttemptStateInterrupted AttemptState = "interrupted"
    AttemptStateLeaseLost   AttemptState = "lease_lost"
    AttemptStateCancelled   AttemptState = "cancelled"
)
```

### 2.2 Job and attempt

```go
type Job struct {
    ID    JobID
    Kind  string
    Queue QueueName
    Args  json.RawMessage

    State    JobState
    Priority int16

    AvailableAt time.Time
    ExpiresAt   time.Time
    ExecutionTimeout time.Duration

    ExecutionCount       int
    ConsumedAttemptCount int
    MaxAttempts          int

    CurrentAttemptID AttemptID
    CurrentLeaseID   LeaseID

    UniqueKey  string
    ParentJobID JobID
    SpawnKey   string

    WorkflowRunID WorkflowRunID
    NodeKey       string

    Result    json.RawMessage
    LastError *JobError
    Metadata  map[string]string

    CorrelationID string
    CausationID   string

    CreatedAt   time.Time
    StartedAt   time.Time
    CompletedAt time.Time
    CancelledAt time.Time
    Version     int64
}

type JobAttempt struct {
    ID              AttemptID
    JobID           JobID
    ExecutionNumber int

    LeaseID LeaseID
    WorkerID  string
    ProcessID string

    State               AttemptState
    ConsumesRetryBudget bool

    HandlerVersion string

    StartedAt    time.Time
    HeartbeatAt  time.Time
    CompletedAt  time.Time
    Error        *JobError
}

type PageRequest struct {
    Limit  int
    Cursor string
}

type AttemptPage struct {
    Attempts   []JobAttempt
    NextCursor string
}

type JobError struct {
    Message   string
    Type      string
    Stage     string
    Retryable bool
    Stack     string
    Metadata  map[string]string
}
```

Empty optional IDs/times use their zero value. Returned maps and byte slices are defensive copies.

`ExecutionCount` counts handler invocations. `ConsumedAttemptCount` counts executions that consumed retry budget. The latter is compared with `MaxAttempts`.

## 3. Enqueue and Control APIs

```go
type EnqueueRequest struct {
    ID    JobID
    Kind  string
    Queue QueueName
    Args  json.RawMessage

    Priority int16

    AvailableAt time.Time
    Delay       time.Duration
    ExpiresAt   time.Time
    ExecutionTimeout time.Duration

    MaxAttempts int
    UniqueKey   string

    ParentJobID JobID
    SpawnKey    string

    Metadata      map[string]string
    CorrelationID string
    CausationID   string
}

type EnqueueResult struct {
    Job     Job
    Created bool
}

type JobEnqueuer interface {
    Enqueue(context.Context, EnqueueRequest) (EnqueueResult, error)
    EnqueueBatch(context.Context, []EnqueueRequest) ([]EnqueueResult, error)
}

type JobFilter struct {
    States []JobState
    Queue  QueueName
    Kind   string

    ScheduledOnly bool
    UnhandledOnly bool

    ParentJobID   JobID
    WorkflowRunID WorkflowRunID
    CorrelationID string
    UniqueKey     string

    CreatedAfter  time.Time
    CreatedBefore time.Time

    Limit  int
    Cursor string
}

type JobPage struct {
    Jobs       []Job
    NextCursor string
}

type JobReader interface {
    GetJob(context.Context, JobID) (Job, error)
    ListJobs(context.Context, JobFilter) (JobPage, error)
    ListAttempts(context.Context, JobID, PageRequest) (AttemptPage, error)
}

type CancelJobRequest struct {
    JobID  JobID
    Reason string
    Actor  string
}

type RetryJobRequest struct {
    JobID JobID

    Reason string
    Actor  string

    Queue       QueueName
    AvailableAt time.Time
    Delay       time.Duration
    ExpiresAt   time.Time
    MaxAttempts int
}

type JobController interface {
    CancelJob(context.Context, CancelJobRequest) (Job, error)
    RetryJob(context.Context, RetryJobRequest) (Job, error)
}

type JobQueue interface {
    JobEnqueuer
    JobReader
    JobController
}

type JobLaneConfig struct {
    Name                     QueueName
    DefaultMaxAttempts       int
    DefaultVisibilityTimeout time.Duration
    MaxArgsBytes             int
    MaxResultBytes           int
    HistoryRetention         time.Duration
}

type JobLaneStats struct {
    Available int64
    Scheduled int64
    Running   int64
    Retrying  int64
    Blocked   int64
    Unhandled int64
}

type JobLaneAdmin interface {
    CreateJobLane(context.Context, JobLaneConfig) (JobLaneConfig, error)
    GetJobLane(context.Context, QueueName) (JobLaneConfig, error)
    UpdateJobLane(context.Context, JobLaneConfig) (JobLaneConfig, error)
    DeleteJobLane(context.Context, QueueName) error
    JobLaneStats(context.Context, QueueName) (JobLaneStats, error)
}
```

Job-lane creation follows the same exact-normalized idempotency rule as raw queue creation. Deletion requires no retained jobs, dispatches, or live worker registrations. Updating defaults affects future enqueue operations; maximum payload limits apply to future writes. Existing jobs retain their snapshotted maximum-attempt and history-retention values.

An empty enqueue queue resolves through `postgres.WithDefaultJobLane`; without that explicit backend option it is invalid. There is no implicit production lane creation.

List cursors encode the stable tuple `(created_at, id)` and are versioned, opaque, authenticated only against accidental corruption, and bounded to the filter shape. Cursor decoding never produces SQL fragments.

## 4. Typed Helpers

```go
func Enqueue[T any](
    ctx context.Context,
    q JobEnqueuer,
    kind string,
    args T,
    opts ...EnqueueOption,
) (EnqueueResult, error)

func TypedHandler[T any](
    fn func(context.Context, *JobContext[T]) error,
) Handler

type JobContext[T any] struct {
    Job  Job
    Args T
    run  *RunContext
}

func (j *JobContext[T]) SetResult(v any) error
func (j *JobContext[T]) Spawn(key string, req SpawnRequest) error
```

Typed decoding failure is a permanent application error with stage `decode_args`. Encoding failure in enqueue returns `ErrInvalid` before persistence.

## 5. Handler and Outcome Contracts

```go
type Handler interface {
    Handle(context.Context, *RunContext) error
}

type HandlerFunc func(context.Context, *RunContext) error

func (f HandlerFunc) Handle(ctx context.Context, run *RunContext) error {
    return f(ctx, run)
}

type SpawnRequest struct {
    Kind  string
    Queue QueueName
    Args  json.RawMessage

    Priority int16
    Delay    time.Duration
    AvailableAt time.Time
    ExpiresAt   time.Time
    ExecutionTimeout time.Duration
    MaxAttempts int

    UniqueKey string
    Metadata  map[string]string
}

type SpawnOperation struct {
    Key     string
    Request SpawnRequest
}

type Outcome struct {
    // unexported immutable fields
}

type RunContext struct {
    job     Job
    attempt JobAttempt
    outcome Outcome
    sealed  bool
}

func (r *RunContext) Job() Job
func (r *RunContext) Attempt() JobAttempt
func (r *RunContext) SetResult(any) error
func (r *RunContext) Spawn(string, SpawnRequest) error

func (o Outcome) Result() json.RawMessage
func (o Outcome) Spawns() []SpawnOperation
func (o Outcome) WorkflowOperations() []WorkflowOperation
func (o Outcome) EventOperations() []EventOperation
```

Rules:

- `RunContext` is not concurrency-safe; a handler coordinates its own goroutines before mutating outcome.
- `SetResult` may be called once; a second call returns `ErrInvalidState`.
- spawn keys are non-empty, at most 256 bytes, and unique within one outcome;
- `Spawn` validates and copies input immediately but performs no database write;
- the context is sealed when `Handle` returns; later mutation fails;
- outcome JSON has the same size limits as enqueue/result contracts;
- an effective execution timeout is chosen in order: job override, handler-kind option, pool default; zero across all three means no hard timeout;
- application code cannot construct or mutate `Outcome` fields directly; exported read-only representations are returned by copy to finalizers.

## 6. Error Classification and Retry

```go
func Permanent(error) error
func RetryAfter(time.Duration, error) error
func Discard(error) error

type RetryDecision struct {
    Retry bool
    Delay time.Duration
}

type RetryPolicy interface {
    Next(Job, JobAttempt, error) RetryDecision
}
```

Classification precedence from outermost recognized wrapper:

1. `Discard`;
2. `Permanent`;
3. `RetryAfter`;
4. context cause owned by runtime;
5. panic;
6. ordinary retryable error.

Default delays by consumed attempt number:

```text
1 → 1 second
2 → 5 seconds
3 → 30 seconds
4 → 2 minutes
5+ → 10 minutes
```

Bounded proportional jitter chooses `[0.8×delay, 1.2×delay]`; the selected database retry timestamp is persisted.

Permanent, discard, ordinary failure, panic, and finalizer failure consume retry budget because the registered application execution ran. Shutdown cancellation and lease loss do not. Unknown-handler deferral creates no attempt.

## 7. PostgreSQL Public API

```go
package postgres

type JobQueue struct {
    backend *Backend
}

func NewJobQueue(*Backend, ...JobQueueOption) (*JobQueue, error)
func (q *JobQueue) InTx(pgx.Tx) *JobQueue

type Finalizer func(
    context.Context,
    *pgkit.DB,
    jobqueue.Job,
    jobqueue.JobAttempt,
    jobqueue.Outcome,
) error

type WorkerPool struct { /* private fields */ }

func NewWorkerPool(
    *JobQueue,
    jobqueue.WorkerPoolConfig,
    ...WorkerOption,
) (*WorkerPool, error)

func (p *WorkerPool) Handle(
    kind string,
    handler jobqueue.Handler,
    opts ...HandlerOption,
) error

func (p *WorkerPool) Run(context.Context) error
func (p *WorkerPool) Stop(context.Context) error
func (p *WorkerPool) Halt()

func WithFinalizer(Finalizer) HandlerOption
func WithRetryPolicy(jobqueue.RetryPolicy) HandlerOption
func WithHandlerTimeout(time.Duration) HandlerOption
func WithHandlerVersion(string) HandlerOption
```

`Handle` is valid only before `Run`. `Run` may be called once. `Stop` is idempotent and waits for graceful shutdown. `Halt` is idempotent, nonblocking, and cancels immediately.

## 8. Worker Configuration

```go
type WorkerPoolConfig struct {
    ProcessID string

    Concurrency int
    Queues      []QueueBinding

    VisibilityTimeout time.Duration
    LeaseRenewalLead  time.Duration
    MaxClaimBatch     int

    HandlerTimeout      time.Duration
    LongRunningThreshold time.Duration
    ShutdownGracePeriod  time.Duration

    CapabilityTTL       time.Duration
    CapabilityHeartbeat time.Duration
}

type QueueBinding struct {
    Name        QueueName
    Concurrency int
    Weight      int
}
```

Defaults:

- process ID: generated UUIDv7 plus hostname/pid metadata;
- concurrency: `runtime.GOMAXPROCS(0)` with minimum 1;
- visibility: 60 seconds for jobs;
- renewal lead: one third of visibility, bounded to 5–30 seconds;
- maximum claim batch: 32, capped by free capacity;
- handler timeout: none;
- long-running observation: disabled unless configured;
- shutdown grace: 30 seconds;
- capability TTL: 30 seconds;
- capability heartbeat: 10 seconds.

Validation requires unique queue bindings, positive weights/concurrency, per-lane concurrency not exceeding total, renewal lead shorter than visibility, and capability heartbeat below half the TTL.

## 9. PostgreSQL Data Model

### 9.1 Job lanes

```sql
CREATE TABLE jobqueue.job_lanes (
    name text PRIMARY KEY,
    default_max_attempts integer NOT NULL DEFAULT 5,
    default_visibility_ms bigint NOT NULL DEFAULT 60000,
    max_args_bytes integer NOT NULL DEFAULT 1048576,
    max_result_bytes integer NOT NULL DEFAULT 1048576,
    history_retention_ms bigint NOT NULL DEFAULT 2592000000,
    created_at timestamptz NOT NULL DEFAULT clock_timestamp(),
    updated_at timestamptz NOT NULL DEFAULT clock_timestamp(),
    CHECK (default_max_attempts > 0),
    CHECK (default_visibility_ms > 0),
    CHECK (max_args_bytes > 0),
    CHECK (max_result_bytes > 0),
    CHECK (history_retention_ms > 0)
);
```

### 9.2 Jobs

Use `text` plus named `CHECK` constraints instead of PostgreSQL enum types to simplify rolling addition of states.

```sql
CREATE TABLE jobqueue.jobs (
    id uuid PRIMARY KEY,
    kind text NOT NULL,
    lane_name text NOT NULL REFERENCES jobqueue.job_lanes(name),
    args jsonb NOT NULL,
    metadata jsonb NOT NULL DEFAULT '{}'::jsonb,

    state text NOT NULL,
    priority smallint NOT NULL DEFAULT 0,
    available_at timestamptz NOT NULL,
    expires_at timestamptz NULL,
    execution_timeout_ms bigint NULL,

    execution_count integer NOT NULL DEFAULT 0,
    consumed_attempt_count integer NOT NULL DEFAULT 0,
    max_attempts integer NOT NULL,
    history_retention_ms bigint NOT NULL,

    current_attempt_id uuid NULL,
    current_lease_id uuid NULL,

    unique_key text NULL,
    parent_job_id uuid NULL REFERENCES jobqueue.jobs(id),
    spawn_key text NULL,

    workflow_run_id uuid NULL,
    node_key text NULL,

    result jsonb NULL,
    last_error jsonb NULL,

    correlation_id text NULL,
    causation_id text NULL,
    request_fingerprint bytea NOT NULL
        CHECK (octet_length(request_fingerprint) = 32),

    created_at timestamptz NOT NULL DEFAULT clock_timestamp(),
    started_at timestamptz NULL,
    completed_at timestamptz NULL,
    cancelled_at timestamptz NULL,
    history_delete_at timestamptz NULL,

    version bigint NOT NULL DEFAULT 0,

    CONSTRAINT jobs_state_valid CHECK (state IN (
        'available', 'running', 'retrying', 'succeeded',
        'failed', 'cancelled', 'expired', 'discarded', 'blocked'
    )),
    CONSTRAINT jobs_kind_nonempty CHECK (length(kind) > 0),
    CONSTRAINT jobs_execution_nonnegative CHECK (execution_count >= 0),
    CONSTRAINT jobs_consumed_nonnegative CHECK (consumed_attempt_count >= 0),
    CONSTRAINT jobs_consumed_not_above_execution
        CHECK (consumed_attempt_count <= execution_count),
    CONSTRAINT jobs_max_attempts_positive CHECK (max_attempts > 0),
    CONSTRAINT jobs_history_retention_positive CHECK (history_retention_ms > 0),
    CONSTRAINT jobs_expiry_after_availability
        CHECK (expires_at IS NULL OR expires_at > available_at),
    CONSTRAINT jobs_execution_timeout_positive
        CHECK (execution_timeout_ms IS NULL OR execution_timeout_ms > 0),
    CONSTRAINT jobs_current_fence_shape CHECK (
        (current_attempt_id IS NULL AND current_lease_id IS NULL)
        OR
        (current_attempt_id IS NOT NULL AND current_lease_id IS NOT NULL)
    ),
    CONSTRAINT jobs_spawn_shape CHECK (
        (parent_job_id IS NULL AND spawn_key IS NULL)
        OR
        (parent_job_id IS NOT NULL AND spawn_key IS NOT NULL)
    ),
    CONSTRAINT jobs_workflow_shape CHECK (
        (workflow_run_id IS NULL AND node_key IS NULL)
        OR
        (workflow_run_id IS NOT NULL AND node_key IS NOT NULL)
    )
);

CREATE UNIQUE INDEX jobs_active_unique_idx
ON jobqueue.jobs (lane_name, unique_key)
WHERE unique_key IS NOT NULL
  AND state IN ('available', 'running', 'retrying', 'blocked');

CREATE UNIQUE INDEX jobs_spawn_idx
ON jobqueue.jobs (parent_job_id, spawn_key)
WHERE parent_job_id IS NOT NULL;

CREATE INDEX jobs_list_idx
ON jobqueue.jobs (created_at DESC, id DESC);

CREATE INDEX jobs_state_list_idx
ON jobqueue.jobs (state, created_at DESC, id DESC);

CREATE INDEX jobs_expiry_idx
ON jobqueue.jobs (expires_at, id)
WHERE expires_at IS NOT NULL
  AND state IN ('available', 'retrying');

CREATE INDEX jobs_history_cleanup_idx
ON jobqueue.jobs (history_delete_at, id)
WHERE history_delete_at IS NOT NULL;
```

The request fingerprint excludes generated job ID and database-assigned timestamps. It preserves the caller's schedule form (`database_now`, delay duration, or absolute time) and whether defaults were requested. Thus an uncertain retry of the same request remains equal after time advances or lane defaults change. `max_attempts` and `history_retention_ms` retain the actual snapshotted values. Workflow-created and spawned jobs store the corresponding canonical node/spawn specification fingerprint.

Workflow foreign keys are added by the workflow migration after `workflow_runs` exists. Until then, `workflow_run_id` and `node_key` must be null for ordinary enqueue.

### 9.3 Managed dispatch

```sql
CREATE TABLE jobqueue.job_dispatches (
    job_id uuid PRIMARY KEY REFERENCES jobqueue.jobs(id) ON DELETE CASCADE,
    lane_name text NOT NULL REFERENCES jobqueue.job_lanes(name),
    priority smallint NOT NULL,
    visible_at timestamptz NOT NULL,

    lease_id uuid NULL,
    leased_at timestamptz NULL,
    lease_expires_at timestamptz NULL,

    dispatch_count integer NOT NULL DEFAULT 0,
    first_dispatched_at timestamptz NULL,
    last_dispatched_at timestamptz NULL,

    CONSTRAINT job_dispatch_count_nonnegative CHECK (dispatch_count >= 0),
    CONSTRAINT job_dispatch_lease_shape CHECK (
        (lease_id IS NULL AND leased_at IS NULL AND lease_expires_at IS NULL)
        OR
        (lease_id IS NOT NULL AND leased_at IS NOT NULL AND lease_expires_at IS NOT NULL)
    )
);

CREATE INDEX job_dispatch_claim_idx
ON jobqueue.job_dispatches (
    lane_name,
    priority DESC,
    visible_at,
    job_id
);
```

There is no retention timestamp or maximum dispatch count.

### 9.4 Attempts

```sql
CREATE TABLE jobqueue.job_attempts (
    id uuid PRIMARY KEY,
    job_id uuid NOT NULL REFERENCES jobqueue.jobs(id) ON DELETE CASCADE,
    execution_number integer NOT NULL,

    lease_id uuid NOT NULL,
    worker_id text NOT NULL,
    process_id text NOT NULL,

    state text NOT NULL,
    consumes_retry_budget boolean NOT NULL DEFAULT false,
    handler_version text NULL,

    started_at timestamptz NOT NULL DEFAULT clock_timestamp(),
    heartbeat_at timestamptz NOT NULL DEFAULT clock_timestamp(),
    completed_at timestamptz NULL,
    error jsonb NULL,

    CONSTRAINT job_attempt_state_valid CHECK (state IN (
        'running', 'succeeded', 'failed', 'interrupted',
        'lease_lost', 'cancelled'
    )),
    CONSTRAINT job_attempt_execution_positive CHECK (execution_number > 0),
    UNIQUE (job_id, execution_number),
    UNIQUE (lease_id)
);

CREATE INDEX job_attempts_job_list_idx
ON jobqueue.job_attempts (job_id, execution_number DESC);

CREATE INDEX job_attempts_running_heartbeat_idx
ON jobqueue.job_attempts (heartbeat_at, id)
WHERE state = 'running';

ALTER TABLE jobqueue.jobs
    ADD CONSTRAINT jobs_current_attempt_fk
    FOREIGN KEY (current_attempt_id)
    REFERENCES jobqueue.job_attempts(id)
    ON DELETE RESTRICT;
```

### 9.5 Administrative events

```sql
CREATE TABLE jobqueue.job_admin_events (
    id uuid PRIMARY KEY,
    job_id uuid NOT NULL REFERENCES jobqueue.jobs(id) ON DELETE CASCADE,
    job_version bigint NOT NULL,
    event_type text NOT NULL,
    actor text NULL,
    reason text NULL,
    data jsonb NOT NULL DEFAULT '{}'::jsonb,
    occurred_at timestamptz NOT NULL DEFAULT clock_timestamp(),
    UNIQUE (job_id, job_version, event_type)
);

CREATE INDEX job_admin_events_list_idx
ON jobqueue.job_admin_events (job_id, occurred_at, id);
```

### 9.6 Worker capability tables

```sql
CREATE TABLE jobqueue.worker_registrations (
    id uuid PRIMARY KEY,
    process_id text NOT NULL,
    lane_name text NOT NULL REFERENCES jobqueue.job_lanes(name),
    handler_version text NULL,
    metadata jsonb NOT NULL DEFAULT '{}'::jsonb,
    started_at timestamptz NOT NULL DEFAULT clock_timestamp(),
    heartbeat_at timestamptz NOT NULL DEFAULT clock_timestamp(),
    expires_at timestamptz NOT NULL,
    UNIQUE (process_id, lane_name)
);

CREATE TABLE jobqueue.worker_capabilities (
    registration_id uuid NOT NULL
        REFERENCES jobqueue.worker_registrations(id) ON DELETE CASCADE,
    kind text NOT NULL,
    PRIMARY KEY (registration_id, kind)
);

CREATE INDEX worker_capabilities_kind_idx
ON jobqueue.worker_capabilities (kind, registration_id);

CREATE INDEX worker_registrations_expiry_idx
ON jobqueue.worker_registrations (expires_at, id);
```

Capability rows are observational and never consulted as a correctness prerequisite inside claim transactions.

## 10. State Transition Matrix

| From | Event | To | Fence/condition |
|---|---|---|---|
| — | enqueue | available | Initial job, no dependencies |
| — | workflow create | blocked | Has unresolved dependencies |
| available | registered-kind claim | running | `available_at <= db_now < expires_at`, if any |
| retrying | registered-kind claim | running | retry time due and attempts remain |
| running | handler success commit | succeeded | Current attempt + lease, cancellation has not won |
| running | retryable failure | retrying | Current fence and budget remains |
| running | exhausted/permanent failure | failed | Current fence |
| running | discard | discarded | Current fence |
| running | cancellation wins | cancelled | Job row lock/version |
| available/retrying/blocked | cancellation | cancelled | Job row lock/version |
| available/retrying | start deadline reached | expired | Not running |
| blocked | dependencies resolve | available | Remaining count reaches zero |
| terminal | administrative retry | retrying | Explicit request; not already active |

No ordinary transition leaves a terminal state.

## 11. Enqueue Algorithm

### 11.1 Single

1. Validate root request and canonical JSON sizes.
2. Resolve lane defaults and database availability time.
3. Generate job UUIDv7 if omitted and compute the canonical request fingerprint.
4. Begin transaction unless caller-bound.
5. Load and lock conflicting identity/active unique rows only as needed.
6. Attempt job insert.
7. On ID conflict, compare the canonical request fingerprint; exact match returns existing.
8. On active unique conflict, load the active job. A matching canonical request fingerprint returns existing; conflicting semantics return `ErrConflict`.
9. Insert dispatch `(job_id, lane, priority, available_at)`.
10. Emit one lane wake hint.
11. Commit and return a defensive job copy.

The active uniqueness comparison excludes generated ID and server timestamps but includes kind, canonical args, priority, availability, expiration, execution timeout, max attempts, parent/spawn, and metadata that affects execution.

### 11.2 Batch

Prevalidate all requests, generate IDs, and detect duplicate IDs/unique keys within the batch. Sort database insertion order by lane then stable ID. One conflict that is not an exact idempotent replay aborts the transaction.

Dispatch insertion and notifications occur once per resulting newly created job/lane. Results restore request order and mark idempotent existing rows `Created=false`.

## 12. Claim Algorithm

### 12.1 Pre-claim expiration

Before claiming, a bounded statement marks due non-running jobs expired and deletes their dispatch rows for the requested lane. This prevents expired rows from repeatedly occupying the head of the claim index.

### 12.2 Candidate lock

The claim statement receives registered kinds as `text[]`, available capacity, UUIDv7 arrays for leases and attempts, worker/process IDs, and visibility.

Candidate predicate:

```sql
d.lane_name = $lane
AND d.visible_at <= statement_time
AND j.kind = ANY($registered_kinds)
AND j.available_at <= statement_time
AND (j.expires_at IS NULL OR j.expires_at > statement_time)
AND (
    j.state IN ('available', 'retrying')
    OR (
        j.state = 'running'
        AND d.lease_expires_at <= statement_time
        AND j.current_lease_id = d.lease_id
    )
)
```

Order by dispatch priority, visibility, and job ID; lock both job and dispatch rows with `FOR UPDATE OF j, d SKIP LOCKED` and a bounded limit.

### 12.3 Transaction steps

For candidates in stable order:

1. Mark a previous running attempt `lease_lost` when its lease is provably expired and still matches the job fence; set `consumes_retry_budget=false`.
2. Assign distinct lease and attempt UUIDs to the dispatch/job.
3. Increment `dispatch_count` and `execution_count`.
4. Insert a running attempt with the new execution number.
5. Set job state `running`, current fence, first `started_at`, and increment version.
6. Return job, attempt, lease expiry, and dispatch metadata.

All steps commit together. The transaction ends before local handler dispatch.

The worker rechecks its immutable handler map before invocation. It derives the effective execution timeout using job, handler, then pool precedence and wraps the handler context when nonzero. Runtime timeout is classified as a retryable application failure with stage `execution_timeout` and consumes retry budget. A theoretically unknown result is immediately released as infrastructure deferral and its just-created attempt is converted to `interrupted` with no budget consumption. This path is an invariant alarm.

Registered-kind filtering preserves rolling-deploy safety but can repeatedly probe and skip an adversarial head-of-lane backlog whose kinds no local process handles. The initial normalized design keeps kind authoritative on `jobs`; if benchmarks show material index or heap-scan amplification, the first optimization to evaluate is copying immutable `kind` into `job_dispatches` and adding a composite lane/kind/priority/visibility index.

## 13. Lease Renewal

Job lease batch extension joins requested `(job_id, attempt_id, lease_id)` tuples to all three current records:

- job is running with matching current fence;
- dispatch has matching lease and a still-future expiry;
- attempt is running with matching IDs.

It updates dispatch expiry/visibility and attempt heartbeat to one statement timestamp. Missing tuples indicate lease loss and cancel local contexts.

Extensions never alter job state, execution count, or retry budget.

## 14. Completion Algorithm

The runtime validates JSON/spawn limits before beginning the transaction, then:

1. Lock workflow run first when present.
2. For a workflow job, discover the bounded affected dependency closure and lock every affected job in UUID order; otherwise lock the current job. Then lock attempts, dispatches, nodes, and dependencies in their stable order.
3. Require job `running`, exact current attempt and lease, attempt `running`, dispatch exact lease, and lease expiry after database time.
4. Recheck cancellation/workflow state.
5. Invoke the registered per-kind finalizer with transaction-bound pgkit DB.
6. Materialize spawns sorted by spawn key.
7. Let later workflow/event components materialize their buffered operations under their lock order; EventBus routing preparation precedes stream locks and global allocation.
8. Set attempt `succeeded`, `completed_at`, and no error.
9. Set job `succeeded`, result, completion/history-delete timestamps, clear fence, and increment version.
10. Delete dispatch.
11. Append operational/admin history as required.
12. Notify child lanes and commit.

Spawn insert uses unique `(parent_job_id, spawn_key)`. On conflict it compares immutable child fields; mismatch returns `ErrConflict` and rolls back parent completion.

Database-deadlock/serialization retry reruns steps 1–12 and the finalizer, never the handler. Finalizers must therefore tolerate transaction rollback and reinvocation.

## 15. Failure and Interruption Algorithms

### 15.1 Application failure

Lock/fence identically to completion. Persist bounded/redacted error and mark attempt failed with `consumes_retry_budget=true`. Increment job consumed count.

If permanent or budget exhausted:

- job → `failed`;
- set completion/history-delete timestamps;
- clear fence;
- delete dispatch.

If retryable:

- choose/persist retry time with database now plus policy delay;
- set `jobs.available_at=retry_at`;
- job → `retrying`;
- clear current fence;
- dispatch clears lease and sets `visible_at=retry_at`;
- notify only when immediately due.

### 15.2 Discard

Identical fencing, attempt bookkeeping, and completion timestamps; job → `discarded`; dispatch deleted.

### 15.3 Shutdown interruption

If the handler was invoked, mark attempt `interrupted`, `consumes_retry_budget=false`, and persist stage `shutdown`. Job → `retrying`, fence clears, and both `jobs.available_at` and dispatch visibility become immediate or use the same small shutdown-jitter timestamp.

If shutdown occurs before invocation, no attempt is created where possible; otherwise the defensive conversion above applies.

### 15.4 Lease loss

A worker that discovers lease loss does not write job outcome. Best-effort attempt annotation is permitted only with a predicate proving the attempt is no longer current; the next claim transaction performs authoritative repair.

### 15.5 Finalizer failure

Rollback the completion transaction, then process the finalizer error as an application failure with stage `finalizer`. The handler may run again. Documentation requires idempotent external handler effects.

## 16. Cancellation, Expiration, and Retry

### 16.1 Cancel

Lock workflow run first if present, then job/current attempt/dispatch.

- If already cancelled, return current job idempotently.
- If another terminal state, return `ErrTerminal`.
- Set job cancelled timestamps/history retention, clear fence, increment version.
- Mark current running attempt cancelled without retry-budget consumption.
- Delete dispatch.
- Insert admin event with actor/reason.
- Send a compact cancellation notification after commit.

For a workflow node, the workflow component also records the node terminal transition, resolves outgoing dependencies, and derives run outcome in this same transaction. Explicit cancellation of a blocked node marks it activated for required-node outcome policy; dependency-condition branch skips use a separate internal path.

Each process maps active job IDs to cancellation functions. Local and remote notification cancels matching handler contexts. Notification loss does not affect durable fencing.

### 16.2 Expire

Bounded expiry maintenance finds available/retrying jobs with due `ExpiresAt`; for a workflow job it begins a per-job transaction by locking the workflow run before the job. It sets `expired`, deletes dispatch, records history, and runs workflow dependency/outcome resolution. Running jobs are excluded because they started before their deadline. Blocked workflow jobs are evaluated when their dependencies resolve, avoiding a false required failure for a branch which would instead have been skipped.

### 16.3 Administrative retry

Lock terminal job. Validate new scheduling/expiry. Preserve execution and consumed counts for audit, and replace `max_attempts` with an explicit new total ceiling greater than current consumed count.

Initial API interprets `MaxAttempts` as the new total ceiling. Omitted value sets `consumed_attempt_count + lane.default_max_attempts`.

`RetryJob` rejects jobs belonging to a workflow with `ErrInvalidState`. Rewinding one terminal DAG node after descendants resolved would violate immutable edge history; a future workflow-level retry policy may create a new run or explicit replacement node instead.

Set state retrying, clear result/error/completion timestamps as appropriate while history remains in attempts/admin events, create dispatch, increment version, record actor/reason, and notify when due.

## 17. Managed Dispatch Reconciliation

Run in bounded `SKIP LOCKED` batches.

Repair classes:

1. **Missing:** insert dispatch for available/retrying jobs with no row, using `available_at` or persisted retry time.
2. **Terminal orphan:** delete dispatch for terminal jobs.
3. **Blocked orphan:** delete dispatch for blocked workflow nodes.
4. **Expired start:** mark due jobs expired and delete dispatch.
5. **Fence mismatch:** repair only if current lease is absent/expired and no matching running attempt can still own work; otherwise record anomaly without mutation.
6. **Duplicate logical work:** impossible under dispatch PK; duplicates found through manual corruption stop reconciliation and emit a fatal invariant observation.

Reconciliation uses job state as authority and never decrements retry budget.

### 17.1 Terminal history cleanup

Terminal standalone jobs become eligible at `history_delete_at`, initially 30 days after terminal transition using the job's snapshotted retention. A bounded maintenance transaction deletes only eligible jobs which have no workflow node and no retained child job. Attempts and administrative events cascade only with that aggregate deletion. Child jobs are removed before parents over successive passes.

Workflow node jobs remain referenced by their graph and therefore outlive ordinary job history cleanup until a future explicit workflow-retention policy removes the whole graph safely. Retention is a minimum, not a promise that every row disappears immediately at its deadline.

## 18. Worker Runtime State

`WorkerPool` owns:

- immutable handler registrations;
- root context and cancellation cause;
- total and per-lane semaphores;
- weighted lane scheduler;
- process registration IDs;
- dispatch loops;
- active execution map keyed by job ID and lease ID;
- shared LeaseManager registration;
- wait groups and lifecycle state.

Lifecycle states:

```text
created → running → stopping → stopped
                  ↘ halted ────┘
```

No restart after stopped/halted.

Weighted round-robin selects among lanes that have free configured capacity. An empty claim backs off that lane without blocking others.

## 19. Capability Registration

At startup, one transaction upserts a registration per bound lane and replaces its capability rows with the pool's registered kind set.

Heartbeat updates `heartbeat_at` and `expires_at` using database time. Reconnect recreates registrations. Shutdown deletes them best-effort.

Unhandled query finds nonterminal job kinds with no matching capability belonging to an unexpired registration. It is advisory because a process may be between registration and claim startup.

## 20. Graceful Shutdown

`Stop(ctx)`:

1. transition to stopping once;
2. stop claim loops;
3. retain capability/lease heartbeats during grace;
4. wait for handlers and allow successful finalization;
5. at grace expiry or caller cancellation, cancel remaining handlers with shutdown cause;
6. persist interruption/release for receipts still owned;
7. stop LeaseManager registrations;
8. delete capability registrations best-effort;
9. return joined shutdown errors.

`Halt()` immediately cancels, stops claims, and relies on lease expiry when release cannot be persisted.

## 21. Inspection and Pagination

Scheduled derivation:

```text
state = available AND available_at > database time
```

Unhandled derivation uses active capability registration expiry. Running duration and heartbeat are calculated from database timestamps.

List filters are parameterized. Cursor ordering always includes UUID as a unique tiebreaker. Maximum page size is 500, default 100.

## 22. Error Mapping

| Condition | Error/category |
|---|---|
| Missing lane | `ErrNotFound` with resource `job_lane` |
| Missing job | `ErrNotFound` with resource `job` |
| Invalid state transition | `ErrInvalidState` |
| Different stable ID/unique/spawn content | `ErrConflict` |
| Current fence missing/stale | `ErrLeaseLost` |
| Unknown local handler fallback | `ErrUnavailable` with resource `handler`, deferred |
| Cancellation against another terminal state | `ErrTerminal` |
| Result/args/error too large | `ErrPayloadTooLarge` |
| Invalid cursor/filter/config | `ErrInvalid` |

Constraint-name mapping distinguishes active uniqueness, spawn identity, and ordinary primary-key conflicts.

## 23. Observability

Emit post-transaction observations for:

- enqueue created/idempotent/conflict and batch size;
- claim/empty claim/registered kind count;
- handler start/finish/panic/duration;
- attempt state and retry-budget consumption;
- retry schedule, finalizer duration/error, completion duration;
- cancellation/expiration/admin retry;
- lease renewal/loss and heartbeat age;
- capability registration/expiry;
- unhandled backlog;
- reconciliation repair class/anomaly;
- long-running threshold crossing.

## 24. Dependencies

Depends on:

- root queue-independent IDs, errors, observers, JSON validation;
- PostgreSQL backend transaction, notification, LeaseManager, and migration helpers;
- raw semantics only as conceptual precedent, not table/API dependency.

Extended by:

- workflow component through nullable workflow identity and completion hooks;
- event components through buffered outcome hooks.

## 25. Test Plan

### 25.1 State and validation

- `TestJobStateTransitionMatrix`
- `TestScheduledIsDerivedNotPersisted`
- `TestRetryBudgetClassification`
- `TestRunContextSealsAfterHandler`
- `TestRunContextRejectsDuplicateSpawnKey`
- `TestTypedHandlerDecodePermanentError`

### 25.2 Enqueue and identity

- `TestEnqueueCreatesManagedDispatch`
- `TestEnqueueStableIDIdempotent`
- `TestEnqueueStableIDConflict`
- `TestEnqueueActiveUniqueConcurrent`
- `TestEnqueueSpawnIdentityConcurrent`
- `TestEnqueueBatchAtomicAndOrdered`
- `TestEnqueueBatchConflictRollsBack`

### 25.3 Claims and rolling deployment

- `TestClaimFiltersRegisteredKinds`
- `TestOldWorkerCannotClaimNewKind`
- `TestUnknownFallbackConsumesNoBudget`
- `TestClaimCreatesDistinctAttemptsAndLeases`
- `TestExpiredLeaseStartsNewNonConsumingHistoryRepair`
- `TestClaimCapacityBound`
- benchmark a lane whose claim-index head is dominated by large unhandled-kind backlogs at several handled/unhandled ratios, recording rows probed, buffers, claim latency, and throughput before considering dispatch-kind denormalization;

### 25.4 Completion/failure/finalizer

- `TestCompletionFence`
- `TestCompletionAtomicResultSpawnAndAck`
- `TestCompletionSpawnConflictRollsBack`
- `TestPerKindFinalizerUsesSameTransaction`
- `TestFinalizerRollbackLeavesJobRunnable`
- `TestDatabaseRetryMayReinvokeFinalizerNotHandler`
- `TestRetryableFailurePersistsChosenTime`
- `TestPanicConsumesRetryBudget`
- `TestShutdownDoesNotConsumeRetryBudget`
- `TestLeaseLossCannotFinalize`

### 25.5 Cancellation/expiry/admin

- `TestCancellationWinsCompletionRace`
- `TestCompletionWinsCancellationRace`
- `TestCancelScheduledJob`
- `TestCancelRunningInvalidatesFence`
- `TestExpireNeverCancelsAlreadyRunningJob`
- `TestAdministrativeRetryPreservesHistory`

### 25.6 Reconciliation/capability

- `TestReconcilerRecreatesMissingDispatch`
- `TestReconcilerDeletesTerminalDispatch`
- `TestReconcilerDoesNotRepairLiveFence`
- `TestCapabilityExpiryMarksBacklogUnhandled`
- `TestCapabilityRegistryNotRequiredForClaim`

### 25.7 Runtime/fault/race

- `TestWorkerPoolGracefulStop`
- `TestWorkerPoolHaltReliesOnExpiry`
- `TestNoPrefetchBeyondCapacity`
- `TestLongRunningObservation`
- crash before/after every completion step;
- 100+ concurrent worker goroutines under `go test -race`;
- commit uncertainty after successful completion;
- database outage during heartbeat and finalization.

## 26. Acceptance Conditions

- Jobs are never governed by raw message retention or maximum deliveries.
- Registered-kind filtering makes heterogeneous rolling deployment safe.
- Every handler invocation has auditable execution history.
- Only specified application outcomes consume retry budget.
- All terminal transitions delete managed dispatch and invalidate fences.
- Missing dispatch is reconstructable without duplicating logical jobs.
- Handler work never runs inside a database transaction.
- Finalizer, job result, children, future workflow/events, and dispatch deletion commit atomically.
- Cancellation and lease loss prevent stale outcome commit.
- Batch enqueue is atomic and preserves result order.
- All state, concurrency, fault-injection, and race tests pass.
