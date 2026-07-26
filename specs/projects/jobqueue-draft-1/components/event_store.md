---
status: complete
---

# Component: EventStore

> **Superseded.** This is an earlier design for the same problem, structured as five layers — `MessageQueue`, `JobQueue`, Workflow DAG, `EventBus`, and `EventStore`. The active design lives in `specs/projects/flow` and uses a command / worker / event model with declarative plans instead. These documents are retained for their reasoning and their PostgreSQL mechanics, much of which carried forward; the APIs they describe are not current.

## 1. Purpose and Scope

`EventStore` provides immutable append-only streams with optimistic concurrency and one safe global traversal order.

Responsibilities:

- stable stream and event identities;
- expected-version append, including atomic multi-stream append;
- contiguous per-stream versions and unique global positions;
- idempotent recovery from uncertain append commits;
- bounded stream and global reads;
- safe global checkpoints which cannot skip a transaction that becomes visible later;
- PostgreSQL projection checkpoint helpers;
- atomic composition with application state, jobs, workflows, and EventBus publication.

Non-responsibilities:

- automatically rerunning handlers when events are replayed;
- EventBus subscription delivery;
- snapshots, archival, stream deletion, or regulatory erasure policy in the initial release;
- claiming that global position is wall-clock or causal order.

Streams are retained indefinitely by default.

## 2. Root Public Model

### 2.1 Stream versions and positions

```go
package jobqueue

type StreamID string
type StreamVersion int64
type GlobalPosition int64

const NoStreamVersion StreamVersion = 0

type ProposedEvent struct {
    ID            EventID
    Type          string
    Data          json.RawMessage
    Metadata      map[string]string
    OccurredAt    time.Time
    CorrelationID string
    CausationID   string
}

type StoredEvent struct {
    ID             EventID
    StreamID       StreamID
    StreamVersion  StreamVersion
    GlobalPosition GlobalPosition
    Type           string
    Data           json.RawMessage
    Metadata       map[string]string
    OccurredAt     time.Time
    CorrelationID  string
    CausationID    string
    AppendedAt     time.Time
}
```

Event stream versions and global positions begin at 1. Version/position 0 means no event or no checkpoint. Event IDs are mandatory. `OccurredAt` defaults to database append time when omitted.

### 2.2 Append

```go
type AppendRequest struct {
    StreamID       StreamID
    ExpectedVersion StreamVersion
    Events         []ProposedEvent
}

type AppendResult struct {
    StreamID       StreamID
    PreviousVersion StreamVersion
    CurrentVersion StreamVersion
    Events         []StoredEvent
    Created        bool
}

type EventAppender interface {
    Append(context.Context, AppendRequest) (AppendResult, error)
    AppendBatch(context.Context, []AppendRequest) ([]AppendResult, error)
}
```

`ExpectedVersion` is always explicit and nonnegative. There is deliberately no “any version” mode: callers either know their aggregate version or read it before deciding what to append.

`AppendBatch` is one transaction across distinct streams, preserves caller result order, and rejects duplicate stream IDs or event IDs within the request before SQL. A conflict on any stream rolls back every append.

### 2.3 Reads

```go
type ReadStreamRequest struct {
    StreamID    StreamID
    AfterVersion StreamVersion
    Limit       int
}

type StreamPage struct {
    Events         []StoredEvent
    CurrentVersion StreamVersion
    NextVersion    StreamVersion
    CaughtUp       bool
}

type ReadGlobalRequest struct {
    AfterPosition GlobalPosition
    Limit         int
}

type GlobalPage struct {
    Events          []StoredEvent
    SafeCheckpoint  GlobalPosition
    NextCheckpoint  GlobalPosition
    CaughtUp        bool
}

type EventReader interface {
    ReadStream(context.Context, ReadStreamRequest) (StreamPage, error)
    ReadGlobal(context.Context, ReadGlobalRequest) (GlobalPage, error)
}

type EventStore interface {
    EventAppender
    EventReader
}
```

`NextCheckpoint` is safe to persist after every event in the returned page has been applied. When the page contains every event through the safe high-water mark, it advances to `SafeCheckpoint`, including across harmless position gaps. Otherwise it is the last returned position.

## 3. Validation and Limits

| Setting | Default |
|---|---:|
| events per append | maximum 1,000 |
| streams per batch | maximum 100 |
| events per batch | maximum 5,000 |
| event data | maximum 1 MiB |
| event metadata | maximum 64 KiB |
| read page | default 100, maximum 1,000 |
| stream ID | maximum 1 KiB |
| type/correlation/causation field | maximum 1 KiB |

Append requires a non-empty stream ID, at least one event, valid JSON objects where objects are required, unique non-empty event IDs, and a valid event type. Limits are checked on input bytes before JSON canonicalization and again on canonical output.

The canonical event fingerprint covers stream ID, event ID, type, canonical data/metadata, explicit normalized occurrence time or the omitted-time marker, correlation ID, and causation ID. Assigned stream version, global position, actual defaulted occurrence time, and append time are excluded.

## 4. PostgreSQL Data Model

### 4.1 Streams

```sql
CREATE TABLE jobqueue.event_streams (
    stream_id text PRIMARY KEY,
    current_version bigint NOT NULL CHECK (current_version >= 0),
    created_at timestamptz NOT NULL,
    updated_at timestamptz NOT NULL
);
```

Empty stream rows are an internal transient of append and are removed by rollback on failure. There is no public create-empty-stream operation.

### 4.2 Events

```sql
CREATE TABLE jobqueue.event_store_events (
    id uuid PRIMARY KEY,
    stream_id text NOT NULL
        REFERENCES jobqueue.event_streams(stream_id) ON DELETE RESTRICT,
    stream_version bigint NOT NULL CHECK (stream_version > 0),
    global_position bigint NOT NULL UNIQUE CHECK (global_position > 0),
    event_type text NOT NULL,
    data jsonb NOT NULL,
    metadata jsonb NOT NULL DEFAULT '{}'::jsonb,
    occurred_at timestamptz NOT NULL,
    occurred_at_defaulted boolean NOT NULL,
    correlation_id text,
    causation_id text,
    content_fingerprint bytea NOT NULL CHECK (octet_length(content_fingerprint) = 32),
    appended_at timestamptz NOT NULL,
    UNIQUE (stream_id, stream_version)
);

CREATE INDEX event_store_stream_read_idx
    ON jobqueue.event_store_events (stream_id, stream_version);
CREATE INDEX event_store_global_read_idx
    ON jobqueue.event_store_events (global_position);
CREATE INDEX event_store_correlation_idx
    ON jobqueue.event_store_events (correlation_id, global_position)
    WHERE correlation_id IS NOT NULL;
```

The `bus_events.store_event_id` foreign key is added after both event components exist:

```sql
ALTER TABLE jobqueue.bus_events
    ADD CONSTRAINT bus_events_store_event_fk
    FOREIGN KEY (store_event_id)
    REFERENCES jobqueue.event_store_events(id)
    ON DELETE RESTRICT;
```

### 4.3 Global allocator

```sql
CREATE TABLE jobqueue.event_global_allocator (
    singleton boolean PRIMARY KEY DEFAULT true CHECK (singleton),
    next_position bigint NOT NULL CHECK (next_position > 0)
);

INSERT INTO jobqueue.event_global_allocator (singleton, next_position)
VALUES (true, 1);
```

Exactly one row exists. Application roles receive `SELECT` and `UPDATE`, not `INSERT` or `DELETE`, on this table after migration.

### 4.4 Projection checkpoints

```sql
CREATE TABLE jobqueue.event_projection_checkpoints (
    consumer_name text PRIMARY KEY,
    global_position bigint NOT NULL DEFAULT 0 CHECK (global_position >= 0),
    version bigint NOT NULL DEFAULT 1 CHECK (version > 0),
    metadata jsonb NOT NULL DEFAULT '{}'::jsonb,
    updated_at timestamptz NOT NULL
);
```

The checkpoint row is convenience state, not part of event immutability. Projection-owned tables remain application-owned.

## 5. Append Semantics

### 5.1 Stable uncertain-commit retry

An append has two successful forms:

- `Created=true`: this call inserted the events;
- `Created=false`: all proposed event IDs already exist as the exact block that this request would have created.

For an idempotent repeat, existing rows must:

1. all exist;
2. belong to the requested stream;
3. occupy exactly versions `ExpectedVersion+1 ... ExpectedVersion+n` in request order;
4. have equal content fingerprints.

If only some IDs exist, any fingerprint differs, or the block is in a different order/stream, return `ErrConflict`. Events appended after the exact block do not invalidate idempotent recovery of the earlier successful operation. Stable IDs do not silently deduplicate semantically different appends.

### 5.2 Single-stream append

Single append delegates to the batch algorithm with one request.

### 5.3 Atomic batch algorithm

Before the transaction, validate, canonicalize, fingerprint, and sort a separate lock-order view of requests by stream ID.

In one `READ COMMITTED` transaction:

1. obtain one database timestamp used for omitted occurrence times and append times; an omitted occurrence time fingerprints a stable `database_now` marker and stores `occurred_at_defaulted=true`;
2. for composed EventBus publication, validate and lock enabled topic/subscription routing rows in their global order and prepare the fan-out set;
3. insert absent `event_streams` rows with version 0 using `ON CONFLICT DO NOTHING` in lexical stream-ID order;
4. lock every requested stream row using one lexical `ORDER BY stream_id FOR UPDATE` query;
5. load any rows matching proposed event IDs;
6. for each request, first recognize the complete idempotent-repeat form above;
7. otherwise require stored `current_version = ExpectedVersion` and require none of its proposed IDs to exist;
8. reserve per-stream versions in request event order and update each changed stream to its new version;
9. calculate the total count of newly inserted events and acquire one contiguous global range using the singleton update;
10. assign global positions in lexical stream-ID order and then request event order;
11. insert event rows, followed by the already-prepared EventBus envelopes/deliveries;
12. buffer observations and notifications and return; the owner commits.

Already-idempotent requests in a mixed batch do not consume new positions. Results are remapped to caller order.

In worker completion, application finalizers and other user callbacks finish before step 2. The allocator is acquired after routing/stream validation. No user callback is invoked after routing preparation begins or after allocation. Remaining work is bounded library SQL and result decoding so lock hold time is small.

### 5.4 Allocation SQL

```sql
UPDATE jobqueue.event_global_allocator
SET next_position = next_position + $1
WHERE singleton = true
RETURNING next_position - $1 AS first_position,
          next_position - 1  AS last_position;
```

No update is issued when every request is an idempotent repeat.

## 6. Allocator Correctness Argument

The initial allocator deliberately serializes transactions which append new events globally.

1. PostgreSQL row updates are transactional. If an append rolls back, both allocator increment and event inserts roll back.
2. Only one transaction can hold the allocator row lock. A later allocator waits for the current holder to commit or roll back.
3. After waiting, the later transaction updates the committed next value; it cannot commit a higher allocated range before the earlier range's transaction resolves.
4. The committed allocator value minus one is therefore a high-water mark below which no unresolved reservation can later appear.
5. A global read observes allocator state and event rows in the same statement snapshot and restricts results to that high-water mark.
6. Thus advancing a projection through the reported checkpoint cannot permanently skip an event from a transaction that becomes visible later.

Positions may contain gaps in the public contract, even though this allocator normally rolls back unused ranges. The contract permits future allocation strategies, administrative erasure policies, and migrations without making projections rely on density.

Global position represents allocator/commit serialization order. Events within one transaction have a deterministic order, but positions do not prove wall-clock occurrence, causality, or business priority.

## 7. Read Algorithms

### 7.1 Stream read

In one statement, read the stream's current version and up to `limit+1` events where `stream_version > AfterVersion`, ordered ascending. Missing stream returns `ErrNotFound`; an existing stream cannot be empty in normal committed state.

The extra row determines `CaughtUp`. `NextVersion` is the last returned event version when more rows remain, otherwise current stream version. Results never exceed the requested limit.

### 7.2 Global read

The core query uses one PostgreSQL statement snapshot:

```sql
WITH safe AS (
    SELECT next_position - 1 AS checkpoint
    FROM jobqueue.event_global_allocator
    WHERE singleton = true
), page AS (
    SELECT e.*
    FROM jobqueue.event_store_events e, safe
    WHERE e.global_position > $1
      AND e.global_position <= safe.checkpoint
    ORDER BY e.global_position
    LIMIT $2
)
SELECT page.*, safe.checkpoint
FROM safe
LEFT JOIN page ON true
ORDER BY page.global_position NULLS LAST;
```

The implementation binds `$2` to `limit+1` rather than interpolating SQL. The left join returns the safe checkpoint even for an empty page.

If the extra event exists, return the first `limit`, `CaughtUp=false`, and `NextCheckpoint` equal to the last returned position. Otherwise return all rows, `CaughtUp=true`, and `NextCheckpoint=SafeCheckpoint`.

When `AfterPosition > SafeCheckpoint`, reject the request as `ErrInvalid`; callers may not manufacture future checkpoints.

### 7.3 Snapshot visibility detail

A plain MVCC read of the allocator does not treat an uncommitted row update as committed; it sees the prior committed row version. Events from the updating transaction are also invisible in that statement snapshot. After that transaction commits, both its allocator value and rows become visible together to a later read. This is the required late-visibility property.

## 8. Projection APIs

The root exposes checkpoint data without PostgreSQL callbacks:

```go
type ProjectionCheckpoint struct {
    ConsumerName string
    Position     GlobalPosition
    Version      int64
    Metadata     map[string]string
    UpdatedAt    time.Time
}

type ProjectionCheckpointStore interface {
    GetProjectionCheckpoint(context.Context, string) (ProjectionCheckpoint, error)
    PutProjectionCheckpoint(context.Context, ProjectionCheckpoint, int64) (ProjectionCheckpoint, error)
}
```

`PutProjectionCheckpoint` takes the expected checkpoint-row version and updates only on equality. Position must never move backward.

The PostgreSQL package adds the same-database helper:

```go
type ProjectionHandler func(context.Context, pgx.Tx, []jobqueue.StoredEvent) error

func (s *EventStore) ProjectOnce(
    ctx context.Context,
    consumerName string,
    limit int,
    fn ProjectionHandler,
) (jobqueue.ProjectionCheckpoint, error)
```

`ProjectOnce`:

1. begins a transaction and inserts/locks the consumer checkpoint row;
2. reads one safe global page after the stored position using the transaction-bound store;
3. invokes `fn` inside the transaction with only that page's events;
4. updates the application's projection tables through the supplied transaction;
5. advances the checkpoint to `NextCheckpoint` and commits;
6. returns without advancing on handler error, panic, context cancellation, or commit failure.

Competing calls for the same consumer serialize on its checkpoint row. Different consumers proceed independently. Projection code must be deterministic under replay after an uncertain commit and must not perform irreversible external side effects inside the transaction.

## 9. Atomic Composition APIs

The PostgreSQL implementation supports `InTx(pgx.Tx)` binding. The bound value never commits or rolls back the caller's transaction.

Applications mixing append with job/raw/direct-bus work use `postgres.Backend.ExecuteAtomic`, after their application-table updates, so EventBus routing rows can be prepared before stream/allocation locks and all immutable inserts can be delayed to the final phase. Standalone `Append` and `AppendBatch` internally use the same ordered executor.

Job outcomes use immutable event operations:

```go
type EventOperation struct {
    Append      AppendRequest
    Publications []StoredEventPublication
}

type StoredEventPublication struct {
    ID           EventID
    StoreEventID EventID
    Topic        TopicName
    Source       string
    Subject      string
}

func (r *RunContext) AppendEvents(AppendRequest, ...StoredEventPublication) error
```

Each publication has its own stable EventBus `ID` and references `StoreEventID` in the same operation or an already-stored event. For a newly appended event, EventBus uses the stored event's immutable type, data, metadata, occurrence, correlation, and causation values plus the supplied topic/source/subject routing envelope. Distinct bus IDs allow one stored event to be routed to multiple topics without copying its payload.

During job completion, event operations are validated before opening the settlement transaction, prepared after the job/workflow lock set, and allocated late. A stream conflict, duplicate event conflict, EventBus fan-out failure, finalizer error, or stale job fence rolls back application state, job outcome, stream events, bus deliveries, and allocator movement together.

Worker finalizers do not append through a transaction-bound `EventAppender`; handlers buffer `EventOperation` values before settlement so the runtime can acquire stream and allocator locks at the correct late phase. Outside WorkerPool, caller-owned application transactions may use a bound `EventAppender`, should perform application updates first and append near commit, and must not invoke further user callbacks after append allocation.

## 10. EventBus Integration

Store-backed publication is not automatic. The caller explicitly requests a bus topic/routing envelope. This prevents replay or append from unexpectedly broadcasting domain history.

The EventBus row references `event_store_events.id`; it does not duplicate data or metadata. EventStore retention is independent and indefinite, so bus cleanup can safely delete its envelope when delivery retention permits. Redriving a bus delivery never appends another stream event.

## 11. Error Mapping

| Condition | Public error |
|---|---|
| missing stream read | `ErrNotFound` with resource `stream` |
| expected version mismatch | `ErrConflict` with expected/current versions |
| complete equal uncertain-commit repeat | success with `Created=false` |
| partial/equal-ID-different-content repeat | `ErrConflict` |
| duplicate stream/event ID in batch | `ErrInvalid` |
| invalid future read checkpoint | `ErrInvalid` |
| projection checkpoint version race | `ErrConflict` |
| payload/batch/identifier limit | `ErrInvalid` |
| append after allocator phase | `ErrInvalidState` |

Constraint and PostgreSQL errors are classified through stable names/codes. Public errors include safe identifiers and numeric versions, never raw event data.

## 12. Observability

Emit commit-aware observations for:

- append count, stream count, event count, bytes, and outcome;
- expected-version and event-ID conflicts;
- idempotent append repeats;
- stream/global read size and latency;
- global allocator wait and lock-hold duration;
- safe checkpoint and consumer projection lag;
- projection batch, retry, handler failure, and checkpoint advance;
- composed store-to-bus publication count.

Allowed dimensions include stream category supplied by a caller hook, event type, outcome, and consumer name. Raw stream IDs, event data, and metadata values are not metric labels by default.

## 13. Maintenance and Retention

No routine deletes event rows, stream rows, or allocator state. `VACUUM` and index health are PostgreSQL operational concerns handled by the operations component.

Snapshots, archival, tombstones, stream deletion, and regulatory erasure need a separate functional and migration design. They must define how global gaps, bus references, projection rebuilds, and idempotent event-ID reservation behave. The initial implementation does not expose unsafe deletion helpers.

## 14. Future Concurrent Allocation Ideas

The singleton allocator is intentionally the first implementation. It should be replaced only after workload benchmarks show allocator wait is material relative to total append latency.

Candidates retained for investigation:

- durable concurrent reservation rows with explicit committed/abandoned resolution;
- sequence-allocated ranges plus an independently durable reservation registry and conservative watermark;
- commit order derived from logical decoding/WAL LSN for operators willing to manage replication slots;
- grouped allocation which amortizes one singleton acquisition across multiple ready append transactions;
- partitioned event storage while retaining one independent safe-watermark service.

Any replacement must prove:

1. every allocated range is durably committed or provably abandoned;
2. the safe watermark never passes the earliest unresolved range;
3. process/database crashes resolve abandoned ranges without elapsed-time guesses alone;
4. committed reservation state and event rows cannot disagree;
5. cleanup cannot make a temporarily unresolved committed event permanently invisible;
6. a projection restart yields the same ordered safe sequence;
7. migration preserves existing positions and checkpoints;
8. operational recovery for stuck reservations is explicit and testable.

Independent per-shard sequences do not meet the current single-total-order contract by themselves.

## 15. Test Plan

### 15.1 Validation and model

- IDs, stream IDs, JSON, limits, duplicate requests, and explicit expected version;
- canonical fingerprint stability across JSON/map ordering;
- result payload copying and cursor/checkpoint validation;
- fuzz malformed envelopes without panics or unbounded allocation.

### 15.2 Append and concurrency

- first append, subsequent append, and contiguous stream versions;
- stale expected-version conflict inserts nothing;
- atomic multi-stream success/rollback and lexical locking;
- concurrent writers to one and many overlapping stream sets;
- equal uncertain-commit retry returns the original stored block;
- partial/different stable-ID reuse conflicts;
- allocator positions are unique and transaction blocks are contiguous.

### 15.3 Global safety

- pause transaction after allocation but before commit while readers poll;
- prove readers report only the previous safe checkpoint until commit;
- prove the event and advanced checkpoint appear together afterward;
- rollback after allocation reuses/restores allocator state safely;
- higher-position append cannot commit ahead of a blocked lower allocation;
- gaps do not prevent caught-up checkpoint advancement.

### 15.4 Reads and projections

- bounded stream/global pagination with exact boundary semantics;
- empty global pages still advance through safe gaps;
- two projectors for one consumer serialize; distinct consumers do not;
- projection table updates and checkpoint commit or roll back together;
- uncertain commit replay is harmless for deterministic upserts;
- projection handler panic/error leaves checkpoint unchanged.

### 15.5 Composition and operations

- application update + events + job + EventBus delivery commit atomically;
- any finalizer/stream/fan-out/fence error rolls back all effects;
- store-backed EventBus read and retention behavior;
- allocator wait/hold observations occur only after transaction result known;
- `EXPLAIN (ANALYZE, BUFFERS)` and contention benchmarks at realistic sizes.

## 16. Acceptance Conditions

This component is complete when:

1. expected-version append is atomic per stream and across batch streams;
2. stable event IDs make exact uncertain-commit retries idempotent without hiding conflicts;
3. stream versions and global positions satisfy uniqueness and ordering invariants;
4. a persisted reported checkpoint can never skip a later-visible earlier-position event;
5. projection state and checkpoints can commit in one PostgreSQL transaction;
6. jobs, workflows, application state, EventStore, and explicit EventBus publication compose atomically;
7. no initial API silently deletes or republishes immutable stream history;
8. all concurrency, crash, pagination, projection, and composition tests pass.
