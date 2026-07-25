---
status: complete
---

# Component: MessageQueue

## 1. Purpose and Scope

`MessageQueue` is the low-level backend-neutral competing-consumer primitive:

```text
Publish → Receive/lease → Ack, Nack, or lease expiry
```

It owns raw application messages only. It does not own jobs, job attempts, workflow nodes, bus events, or subscription deliveries.

Responsibilities:

- raw queue administration;
- opaque message publication and scheduling;
- at-least-once leasing;
- fenced acknowledgement, release, and extension;
- active-message deduplication;
- raw retention, dead-letter storage, and redrive;
- batch operations and inspection;
- backend conformance behavior.

Non-responsibilities:

- handler registration or execution;
- application retry budgets;
- durable results or attempt history;
- workflow dependencies;
- topic fan-out or stream ordering;
- strict FIFO guarantees.

## 2. Public Types

```go
package jobqueue

import (
    "context"
    "time"
)

type QueueName string
type MessageID string
type LeaseID string
type DeadMessageID string

type Headers map[string]string

type Message struct {
    ID          MessageID
    Queue       QueueName
    Body        []byte
    Headers     Headers
    Priority    int16
    PublishedAt time.Time
    AvailableAt time.Time
    ExpiresAt   time.Time
}

type Receipt struct {
    MessageID MessageID
    LeaseID   LeaseID
}

type Delivery struct {
    Message        Message
    Receipt        Receipt
    ReceiveCount   int
    FirstReceivedAt time.Time
    LeasedAt       time.Time
    LeaseExpiresAt time.Time
}

type PublishRequest struct {
    Queue QueueName
    ID    MessageID

    Body    []byte
    Headers Headers

    AvailableAt time.Time
    Delay       time.Duration
    Priority    int16

    DeduplicationKey string
    MaxDeliveries    int
}

type PublishResult struct {
    Message Message
    Created bool
}

type ReceiveRequest struct {
    Queue QueueName

    MaxMessages       int
    VisibilityTimeout time.Duration
    WaitTime          time.Duration
}

type NackOptions struct {
    Delay  time.Duration
    Reason string
}

type Lease struct {
    Receipt   Receipt
    ExpiresAt time.Time
}
```

Zero-time fields are omitted values. Returned times are UTC values sourced from PostgreSQL.

`Delivery.FirstReceivedAt` is zero only if the backend cannot provide it; the PostgreSQL backend always provides it.

## 3. Capability Interfaces

```go
type QueuePublisher interface {
    Publish(context.Context, PublishRequest) (PublishResult, error)
}

type QueueReceiver interface {
    Receive(context.Context, ReceiveRequest) ([]Delivery, error)
}

type QueueSettler interface {
    Ack(context.Context, Receipt) error
    Nack(context.Context, Receipt, NackOptions) error
    Extend(context.Context, Receipt, time.Duration) (Lease, error)
}

type MessageQueue interface {
    QueuePublisher
    QueueReceiver
    QueueSettler
}

type BatchQueuePublisher interface {
    PublishBatch(context.Context, []PublishRequest) ([]PublishResult, error)
}

type BatchQueueSettler interface {
    AckBatch(context.Context, []Receipt) BatchSettleResult
    NackBatch(context.Context, []NackRequest) BatchSettleResult
    ExtendBatch(context.Context, []ExtendRequest) BatchExtendResult
}

type NackRequest struct {
    Receipt Receipt
    Options NackOptions
}

type ExtendRequest struct {
    Receipt   Receipt
    Visibility time.Duration
}

type ItemError struct {
    Index int
    Err   error
}

type BatchSettleResult struct {
    Succeeded []int
    Failed    []ItemError
    Err       error
}

type LeaseResult struct {
    Index int
    Lease Lease
}

type BatchExtendResult struct {
    Succeeded []LeaseResult
    Failed    []ItemError
    Err       error
}
```

Batch result rules:

- indices refer to original input order;
- `Err` is a batch-level database/context failure;
- item failures are meaningful only when `Err == nil`;
- duplicate receipts in one request fail validation before SQL;
- empty batches return `ErrInvalid`.

PostgreSQL `PublishBatch` is atomic. Matching idempotent requests return existing messages; any invalid or conflicting request rolls back the entire batch.

Settlement batches are intentionally item-granular: current receipts succeed and stale receipts return per-item `ErrLeaseLost`.

## 4. Queue Administration API

```go
type QueueConfig struct {
    Name QueueName

    DefaultVisibilityTimeout time.Duration
    MaxDeliveries            int
    Retention                time.Duration
    DeadRetention            time.Duration
    DeadLetterQueue          QueueName

    MaxBodyBytes    int
    MaxHeadersBytes int
}

type QueueStats struct {
    Available       int64
    Leased          int64
    Delayed         int64
    Dead            int64
    OldestAvailable time.Duration
    NextAvailableAt time.Time
}

type QueueAdmin interface {
    CreateQueue(context.Context, QueueConfig) (QueueConfig, error)
    GetQueue(context.Context, QueueName) (QueueConfig, error)
    UpdateQueue(context.Context, QueueConfig) (QueueConfig, error)
    DeleteQueue(context.Context, QueueName) error
    PurgeQueue(context.Context, QueueName) (int64, error)
    QueueStats(context.Context, QueueName) (QueueStats, error)
}

type DeadMessage struct {
    ID              DeadMessageID
    OriginalMessage Message
    SourceQueue     QueueName
    DeadLetterQueue QueueName
    ReceiveCount    int
    FirstReceivedAt time.Time
    LastReceivedAt  time.Time
    DeadAt          time.Time
    Reason          string
    LastError       []byte
}

type DeadMessageFilter struct {
    SourceQueue QueueName
    Before      time.Time
    Limit       int
    Cursor      string
}

type RedriveRequest struct {
    DeadMessageID DeadMessageID
    Destination   QueueName
    AvailableAt   time.Time
    Delay         time.Duration
}

type DeadLetterAdmin interface {
    ListDead(context.Context, DeadMessageFilter) ([]DeadMessage, string, error)
    Redrive(context.Context, RedriveRequest) (PublishResult, error)
    DeleteDead(context.Context, DeadMessageID) error
    PurgeDead(context.Context, QueueName) (int64, error)
}
```

Dead-letter storage is an administrative queue view, not an active raw queue. Messages become active again only through explicit redrive. This prevents failed backlog from being consumed accidentally while retaining an SQS-like named dead-letter destination.

## 5. Validation and Defaults

Queue name:

- 1–128 bytes;
- ASCII letters, digits, `.`, `_`, and `-`;
- begins with a letter or digit.

Defaults:

- visibility: 30 seconds;
- maximum deliveries: 5;
- active retention after first availability: 4 days;
- dead retention: 14 days;
- body limit: 1 MiB;
- encoded header limit: 64 KiB.

Production behavior requires explicit queue creation. `postgres.WithAutomaticQueueCreation(template)` is an opt-in MessageQueue constructor option for development/tests; the template name must be empty and is replaced with the requested queue name. Concurrent first publishers run the ordinary exact-idempotent create algorithm.

Durations:

- visibility must be between 1 second and 12 hours;
- retention and dead retention must be between 1 minute and 14 days initially;
- delay must be nonnegative and representable as a PostgreSQL interval;
- `Delay` and `AvailableAt` are mutually exclusive;
- retention starts at effective availability, so scheduling may extend beyond 14 days.

Publication:

- body must contain at least one byte;
- header keys are non-empty UTF-8 strings without control characters;
- header values are valid UTF-8;
- encoded headers are measured using canonical JSON produced by the backend;
- caller IDs must parse as UUID;
- deduplication keys are at most 256 bytes and non-empty when supplied;
- per-message `MaxDeliveries` must be positive and cannot exceed 10,000;
- omitted maximum uses the current queue default, snapshotted into the message.

Receive:

- `MaxMessages` must be positive;
- PostgreSQL default safety maximum is 100 and is configurable downward per backend;
- omitted visibility uses queue default;
- wait must be nonnegative and at most 20 seconds in one call.

Nack reasons are optional, UTF-8, redactable, and capped at 2 KiB after encoding.

## 6. PostgreSQL Data Model

The following uses the default schema. Migration rendering safely substitutes a configured schema.

### 6.1 Queue configuration

```sql
CREATE TABLE jobqueue.raw_queues (
    name text PRIMARY KEY,

    default_visibility_ms bigint NOT NULL,
    max_deliveries integer NOT NULL,
    retention_ms bigint NOT NULL,
    dead_retention_ms bigint NOT NULL,
    dead_letter_queue_name text NULL,

    max_body_bytes integer NOT NULL,
    max_headers_bytes integer NOT NULL,

    created_at timestamptz NOT NULL DEFAULT clock_timestamp(),
    updated_at timestamptz NOT NULL DEFAULT clock_timestamp(),

    CONSTRAINT raw_queues_visibility_positive
        CHECK (default_visibility_ms > 0),
    CONSTRAINT raw_queues_max_deliveries_positive
        CHECK (max_deliveries > 0),
    CONSTRAINT raw_queues_retention_positive
        CHECK (retention_ms > 0),
    CONSTRAINT raw_queues_dead_retention_positive
        CHECK (dead_retention_ms > 0),
    CONSTRAINT raw_queues_body_limit_positive
        CHECK (max_body_bytes > 0),
    CONSTRAINT raw_queues_headers_limit_nonnegative
        CHECK (max_headers_bytes >= 0),
    CONSTRAINT raw_queues_dlq_not_self
        CHECK (dead_letter_queue_name IS NULL OR dead_letter_queue_name <> name)
);
```

`dead_letter_queue_name` is a logical administrative destination name. It need not identify an active `raw_queues` row, avoiding creation-order cycles. Applications normally create a config row for it when they want separate policies and stats.

### 6.2 Active raw messages

```sql
CREATE TABLE jobqueue.raw_messages (
    id uuid PRIMARY KEY,
    queue_name text NOT NULL
        REFERENCES jobqueue.raw_queues(name),

    body bytea NOT NULL,
    headers jsonb NOT NULL DEFAULT '{}'::jsonb,
    priority smallint NOT NULL DEFAULT 0,

    published_at timestamptz NOT NULL DEFAULT clock_timestamp(),
    available_at timestamptz NOT NULL,
    visible_at timestamptz NOT NULL,
    expires_at timestamptz NOT NULL,

    lease_id uuid NULL,
    leased_at timestamptz NULL,
    lease_expires_at timestamptz NULL,

    first_received_at timestamptz NULL,
    last_received_at timestamptz NULL,
    receive_count integer NOT NULL DEFAULT 0,
    max_deliveries integer NOT NULL,

    deduplication_key text NULL,
    request_fingerprint bytea NOT NULL CHECK (octet_length(request_fingerprint) = 32),
    last_nack_reason text NULL,

    CONSTRAINT raw_messages_body_nonempty CHECK (octet_length(body) > 0),
    CONSTRAINT raw_messages_receive_count_nonnegative CHECK (receive_count >= 0),
    CONSTRAINT raw_messages_max_deliveries_positive CHECK (max_deliveries > 0),
    CONSTRAINT raw_messages_expiry_after_availability CHECK (expires_at > available_at),
    CONSTRAINT raw_messages_lease_shape CHECK (
        (lease_id IS NULL AND leased_at IS NULL AND lease_expires_at IS NULL)
        OR
        (lease_id IS NOT NULL AND leased_at IS NOT NULL AND lease_expires_at IS NOT NULL)
    )
);

CREATE INDEX raw_messages_claim_idx
ON jobqueue.raw_messages (
    queue_name,
    priority DESC,
    visible_at,
    id
);

CREATE UNIQUE INDEX raw_messages_dedup_idx
ON jobqueue.raw_messages (queue_name, deduplication_key)
WHERE deduplication_key IS NOT NULL;

CREATE INDEX raw_messages_maintenance_idx
ON jobqueue.raw_messages (expires_at, visible_at, id);
```

The publish transaction computes:

```text
available_at = explicit AvailableAt, database now + Delay, or database now
expires_at   = available_at + snapshotted retention
visible_at   = available_at
```

`request_fingerprint` hashes canonical body, headers, priority, queue, deduplication key, maximum-delivery request, and the original schedule form. Omitted availability is encoded as `database_now` and delay is encoded as its duration rather than the resulting timestamp. Message identity and server-assigned timestamps are excluded. This makes a same-request retry compare equal after database time advances and lets a semantic deduplication key match requests carrying different message IDs.

### 6.3 Dead messages

```sql
CREATE TABLE jobqueue.raw_dead_messages (
    id uuid PRIMARY KEY,
    original_message_id uuid NOT NULL,
    source_queue_name text NOT NULL,
    dead_letter_queue_name text NULL,

    body bytea NOT NULL,
    headers jsonb NOT NULL,
    priority smallint NOT NULL,
    deduplication_key text NULL,

    published_at timestamptz NOT NULL,
    available_at timestamptz NOT NULL,
    expires_at timestamptz NOT NULL,
    first_received_at timestamptz NULL,
    last_received_at timestamptz NULL,
    receive_count integer NOT NULL,
    max_deliveries integer NOT NULL,

    dead_at timestamptz NOT NULL DEFAULT clock_timestamp(),
    delete_at timestamptz NOT NULL,
    reason text NULL,
    last_error jsonb NULL,

    redriven_message_id uuid NULL,
    redriven_at timestamptz NULL,

    CONSTRAINT raw_dead_receive_count_nonnegative CHECK (receive_count >= 0),
    CONSTRAINT raw_dead_delete_after_dead CHECK (delete_at > dead_at)
);

CREATE INDEX raw_dead_list_idx
ON jobqueue.raw_dead_messages (source_queue_name, dead_at DESC, id DESC);

CREATE INDEX raw_dead_destination_idx
ON jobqueue.raw_dead_messages (dead_letter_queue_name, dead_at DESC, id DESC);

CREATE INDEX raw_dead_cleanup_idx
ON jobqueue.raw_dead_messages (delete_at, id)
WHERE redriven_at IS NULL;
```

Dead rows deliberately do not retain a foreign key to a queue config; deleting or renaming configuration must not destroy failure evidence.

## 7. Queue Administration Algorithms

### 7.1 Create

1. Validate and resolve omitted defaults in Go.
2. Insert the complete normalized configuration.
3. On primary-key conflict, load the existing row.
4. Compare every field explicitly supplied by the caller after normalization.
5. Return the existing row with no mutation if they match; otherwise return `ErrConflict` with safe field names.

The request representation in the PostgreSQL package tracks supplied fields through constructor options or pointer-backed internal input. A zero-value `QueueConfig` returned from storage is never used to infer which fields the caller supplied.

### 7.2 Update

`UpdateQueue` replaces the normalized mutable configuration under a row lock. `Name` is immutable.

Changes affect:

- subsequent receive calls for default visibility;
- newly published messages for snapshotted max deliveries and retention;
- subsequent dead transitions for dead retention/destination;
- subsequent publications for size limits.

Existing messages are not rewritten.

### 7.3 Purge and delete

`PurgeQueue` deletes all active rows for the named raw queue in bounded database batches inside one public operation. It invalidates current receipts and reports the total deleted count. It does not remove dead rows.

`DeleteQueue` locks the config and returns `ErrConflict` with reason `not_empty` when active rows or non-redriven dead rows exist. It also returns `ErrConflict` with reason `in_use` when another queue currently names it as a dead-letter destination. Applications must purge active and dead storage and update references explicitly.

## 8. Publish Algorithm

### 8.1 Single publish

1. Validate request fields not dependent on queue configuration.
2. Generate UUIDv7 when ID is absent.
3. Begin a `READ COMMITTED` transaction unless caller-bound.
4. Load the queue configuration; if absent and development auto-creation is enabled, create the named queue from the validated template in this transaction, otherwise return `ErrNotFound`.
5. Canonically encode headers and enforce queue size limits.
6. Calculate availability and expiration with `clock_timestamp()` and compute the canonical request fingerprint.
7. Insert the active row and fingerprint.
8. If ID or active deduplication conflict occurs, load the conflicting row and compare its fingerprint.
9. Return the existing row with `Created=false` only for an exact idempotent match; otherwise return `ErrConflict`.
10. Call `pg_notify` with the raw namespace/queue hint when the row is immediately or eventually relevant to local timers.
11. Commit and emit observations.

Immutable idempotency comparison uses original request semantics rather than a newly calculated database-now timestamp. Queue-default selections are encoded as default markers, while explicit values remain part of the fingerprint.

### 8.2 Batch publish

Batch validation and ID generation occur before the transaction. Database writes are sorted by `(queue_name, message_id)` to reduce deadlocks, while results are restored to request order.

All rows commit atomically. Repeated queue configs are loaded once. At most one notification hint per affected queue is sent inside the transaction.

## 9. Claim SQL and Mapping

The PostgreSQL component uses raw SQL through `pgkit.RawQuery` because data-modifying CTEs and row-lock clauses are clearer and safer than composing them dynamically.

Claim statement:

```sql
WITH statement_time AS MATERIALIZED (
    SELECT clock_timestamp() AS now
),
locked AS (
    SELECT m.id, m.priority, m.visible_at
    FROM jobqueue.raw_messages AS m, statement_time AS t
    WHERE m.queue_name = $1
      AND m.visible_at <= t.now
      AND m.expires_at > t.now
      AND m.receive_count < m.max_deliveries
    ORDER BY m.priority DESC, m.visible_at ASC, m.id ASC
    FOR UPDATE OF m SKIP LOCKED
    LIMIT $2
),
candidates AS (
    SELECT
        id,
        row_number() OVER (
            ORDER BY priority DESC, visible_at ASC, id ASC
        )::integer AS ord
    FROM locked
),
leased AS (
    UPDATE jobqueue.raw_messages AS m
    SET lease_id = ($3::uuid[])[c.ord],
        leased_at = t.now,
        lease_expires_at = t.now + $4::interval,
        visible_at = t.now + $4::interval,
        receive_count = m.receive_count + 1,
        first_received_at = COALESCE(m.first_received_at, t.now),
        last_received_at = t.now
    FROM candidates AS c, statement_time AS t
    WHERE m.id = c.id
    RETURNING m.*, c.ord
)
SELECT * FROM leased
ORDER BY ord;
```

The implementation generates one UUIDv7 per maximum requested row in Go and supplies the UUID array as parameter `$3`. PostgreSQL arrays are one-indexed, matching `row_number()`. Unused UUIDs are harmless when fewer rows are claimable.

Claim timestamps may differ by microseconds because `clock_timestamp()` is intentionally wall-clock time. Returned row values are authoritative.

### 9.1 Waiting receive

`Receive` performs one immediate claim. If empty and `WaitTime > 0`, it waits for the earliest of:

- raw queue notification;
- next scheduled `visible_at` capped by remaining wait;
- fallback poll timer;
- context cancellation.

It then retries until a delivery is returned or wait expires. An empty successful result is not an error.

The waiting implementation never holds a database transaction or connection between claim attempts.

## 10. Settlement SQL

### 10.1 Ack

```sql
DELETE FROM jobqueue.raw_messages
WHERE id = $1
  AND lease_id = $2
  AND lease_expires_at > clock_timestamp()
RETURNING id;
```

No row returns `ErrLeaseLost`. Ack requires a still-valid lease; an expired worker cannot delete a delivery that may be eligible for another consumer.

### 10.2 Nack

```sql
UPDATE jobqueue.raw_messages
SET lease_id = NULL,
    leased_at = NULL,
    lease_expires_at = NULL,
    visible_at = clock_timestamp() + $3::interval,
    last_nack_reason = $4
WHERE id = $1
  AND lease_id = $2
  AND lease_expires_at > clock_timestamp()
RETURNING id, visible_at;
```

Immediate nack emits a wake hint after commit.

### 10.3 Extend

```sql
UPDATE jobqueue.raw_messages
SET lease_expires_at = clock_timestamp() + $3::interval,
    visible_at = clock_timestamp() + $3::interval
WHERE id = $1
  AND lease_id = $2
  AND lease_expires_at > clock_timestamp()
RETURNING lease_expires_at;
```

Extension is from database now, not previous expiry. This bounds unexpectedly long leases after delayed heartbeat execution.

### 10.4 Batch settlement

Batch statements use `unnest` with explicit ordinal indices. Returned ordinals are successes; missing ordinals map to `ErrLeaseLost`.

Nack and extend group inputs by delay/visibility only when doing so reduces statements. A batch-level error invalidates item-level interpretation.

## 11. Dead-Letter and Retention Maintenance

### 11.1 Exhausted delivery movement

A bounded transaction selects rows where:

```text
visible_at <= database now
receive_count >= max_deliveries
lease absent or expired
```

It locks them with `SKIP LOCKED`, deletes them, and inserts complete snapshots into `raw_dead_messages`. The dead ID is a new UUIDv7; original message ID remains recorded.

The reason is `max_deliveries_exhausted`. `delete_at` uses the source queue's current dead retention at transition time.

### 11.2 Active expiration

Expired active messages are deleted in bounded batches by `expires_at`. Expiration does not create dead rows and does not emit job-level state.

### 11.3 Dead cleanup

Dead rows past `delete_at` are deleted in bounded batches. Redriven rows can be retained until their original `delete_at` for audit, then removed.

### 11.4 Redrive

Redrive locks one dead row, validates destination queue/configuration, creates a new UUIDv7 message and fresh availability/expiry/delivery count, and records the new ID on the dead row in one transaction.

Repeated redrive of the same dead row returns the already-created message when it still exists; otherwise it returns `ErrConflict` rather than generating multiple active copies.

Payload mutation is not supported. A caller wanting transformed data publishes a new message explicitly and then deletes the dead record.

## 12. Error Mapping

| Condition | Error |
|---|---|
| Missing queue | `ErrNotFound` with resource `queue` |
| Existing differing queue config | `ErrConflict` |
| Invalid name/body/header/duration/batch | `ErrInvalid` |
| Payload over limit | `ErrPayloadTooLarge` |
| Conflicting ID or dedup key | `ErrConflict` |
| Stale/expired/missing receipt | `ErrLeaseLost` |
| Non-empty/in-use queue deletion | `ErrConflict` with reason `not_empty` / `in_use` |
| Missing dead row | `ErrRemoved` |
| Closed backend/runtime | `ErrClosed` |

PostgreSQL constraint names map to these categories. Database errors remain wrapped for `errors.As(*pgconn.PgError)`.

## 13. Observability

Operations emit post-transaction observations for:

- queue creation/update/purge/delete;
- publish created/idempotent/conflicting;
- claim count, empty claim, wait wake reason, and latency;
- ack/nack/extend success and lease loss;
- batch sizes and item failures;
- dead transition, active expiry, dead cleanup, and redrive;
- queue depths and oldest available age.

Payload bodies and headers are excluded. Queue, message, lease, delivery count, durations, and error category are included.

## 14. Dependencies

Depends on:

- root identifiers, request types, errors, observer contracts;
- PostgreSQL backend transaction/error/notification helpers;
- maintenance scheduler for cleanup and dead movement.

Depended on by:

- applications using raw queues;
- conformance tests;
- no managed higher-level delivery implementation. Jobs and EventBus reuse semantics, not tables or public raw operations.

## 15. Test Plan

### 15.1 Root validation tests

- `TestQueueNameValidation`
- `TestPublishRequestDelayAndAvailableAtExclusive`
- `TestPublishRequestBodyAndHeaderLimits`
- `TestReceiveRequestLimits`
- `TestBatchRejectsDuplicateReceipts`

### 15.2 Conformance tests in `jobqueuetest`

- `TestMessageQueuePublishReceiveAck`
- `TestMessageQueueAckDeletesActiveMessage`
- `TestMessageQueueNackImmediate`
- `TestMessageQueueNackDelayed`
- `TestMessageQueueLeaseExpiryRedelivery`
- `TestMessageQueueExtendPreventsRedelivery`
- `TestMessageQueueExpiredLeaseCannotExtend`
- `TestMessageQueueStaleReceiptCannotAck`
- `TestMessageQueueReceiveLimit`
- `TestMessageQueueShortPollEmpty`
- `TestMessageQueueWaitCancellation`
- `TestMessageQueueScheduledAvailability`
- `TestMessageQueueRetentionStartsAtAvailability`
- `TestMessageQueueCallerIDIdempotency`
- `TestMessageQueueCallerIDConflict`
- `TestMessageQueueDeduplicationKeyConcurrent`
- `TestMessageQueuePriorityBestEffortClaimOrder`
- `TestMessageQueueBatchPublishAtomic`
- `TestMessageQueueBatchSettlementItemErrors`

### 15.3 PostgreSQL concurrency tests

- `TestRawClaimSkipLockedNoCurrentLeaseDuplicate`
- `TestRawClaimFreshLeaseOnRedelivery`
- `TestRawClaimHundredConcurrentConsumers`
- `TestRawAckLeaseExpiryRace`
- `TestRawNackClaimRace`
- `TestRawExtendClaimRace`
- `TestRawBatchDistinctLeaseIDs`
- `TestRawPublishRollbackSendsNoVisibleWork`
- `TestRawNotificationLossFallsBackToPoll`

### 15.4 Administration and maintenance tests

- `TestCreateQueueIdempotentExplicitFields`
- `TestCreateQueueDifferentExplicitFieldConflicts`
- `TestQueueUpdateDoesNotRewriteExistingSnapshots`
- `TestDeleteQueueRejectsActiveAndDeadRows`
- `TestPurgeInvalidatesReceipts`
- `TestExhaustedMessageMovesToDeadStorage`
- `TestExpiredMessageDeletesWithoutDeadRecord`
- `TestDeadRetentionCleanup`
- `TestRedriveCreatesNewMessageIdentity`
- `TestConcurrentRedriveIsIdempotent`

### 15.5 Fault and performance tests

- rollback after candidate lock and before lease update;
- disconnect after commit before claim response;
- dead transition crash at every statement boundary;
- `EXPLAIN (ANALYZE, BUFFERS)` for claim at 1K, 100K, 1M, and 10M rows;
- WAL and autovacuum measurement under publish/claim/extend/delete churn.

## 16. Acceptance Conditions

This component design is implementable when:

- all public types and behavior above are represented in code;
- exact migration SQL preserves the named constraints and lifecycle separation;
- conformance and PostgreSQL race tests pass;
- every settlement mutation is fenced by a current, unexpired lease;
- batch claims always assign distinct lease IDs;
- scheduling cannot lose messages to retention before first availability;
- raw maintenance cannot address job or subscription tables;
- dead redrive is auditable and idempotent;
- hot query plans and maintenance batch sizes are documented with benchmark evidence.
