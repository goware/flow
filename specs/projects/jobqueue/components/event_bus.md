---
status: complete
---

# Component: EventBus

## 1. Purpose and Scope

`EventBus` stores one immutable event envelope and fans out independent durable delivery references to matching subscriptions.

Responsibilities:

- topic and subscription administration;
- stable event-ID idempotency and immutable publication;
- deterministic subscription filters and atomic fan-out;
- subscription-specific leases, retry, concurrency, dead-lettering, and redrive;
- explicit historical replay;
- event and delivery retention without orphaned payloads;
- composition with caller transactions, job outcomes, workflows, and `EventStore`.

Non-responsibilities:

- authoritative indefinite event history;
- global event ordering or stream optimistic concurrency;
- arbitrary executable filter expressions;
- exactly-once subscriber side effects.

EventBus delivery tables are not addressable through raw `MessageQueue` APIs.

## 2. Root Public Model

### 2.1 Topics, filters, and subscriptions

```go
package jobqueue

type TopicName string
type SubscriptionID string
type DeliveryID string
type ReplayID string

type StringMatch struct {
    Exact  string
    Prefix string
}

type SubscriptionFilter struct {
    Type    StringMatch
    Source  StringMatch
    Subject StringMatch
}

type TopicConfig struct {
    EventRetention time.Duration
}

type DeliveryRetryPolicy struct {
    InitialDelay time.Duration
    Multiplier   float64
    MaxDelay     time.Duration
    Jitter       float64
}

type SubscriptionConfig struct {
    Filter             SubscriptionFilter
    VisibilityTimeout  time.Duration
    MaxDeliveries      int
    RetryPolicy        DeliveryRetryPolicy
    MaxInFlight        int
    DeliveryRetention  time.Duration
    DeadRetention      time.Duration
}

type Topic struct {
    Name      TopicName
    Config    TopicConfig
    Enabled   bool
    CreatedAt time.Time
    UpdatedAt time.Time
}

type Subscription struct {
    ID        SubscriptionID
    Topic     TopicName
    Name      string
    Enabled   bool
    Config    SubscriptionConfig
    CreatedAt time.Time
    UpdatedAt time.Time
}
```

For each `StringMatch`, `Exact` and `Prefix` are mutually exclusive. An empty pair matches all values. Non-empty fields are ANDed. Matching is bytewise, case-sensitive, and locale-independent.

### 2.2 Event envelope

```go
type Event struct {
    ID            EventID
    Topic         TopicName
    Type          string
    Source        string
    Subject       string
    Data          json.RawMessage
    Metadata      map[string]string
    OccurredAt    time.Time
    CorrelationID string
    CausationID   string
    PublishedAt   time.Time
    StoreEventID  EventID
}

type PublishEventRequest struct {
    Event Event
}

type PublishEventResult struct {
    Event         Event
    Created       bool
    DeliveryCount int
}
```

`ID` is mandatory for direct publication. A typed helper may generate a UUIDv7 before making the request, but an implicit server-generated ID would not make an uncertain retry idempotent. `OccurredAt` defaults to database publication time when omitted.

`StoreEventID` is set only when the bus envelope references an immutable `EventStore` event for its data and metadata. Direct `PublishEvent` requires it to be empty; callers use the EventStore composition API for store-backed publication.

### 2.3 Delivery

```go
type EventDelivery struct {
    ID             DeliveryID
    SubscriptionID SubscriptionID
    Event          Event
    Receipt        EventReceipt
    DeliveryCount  int
    FirstAvailableAt time.Time
    VisibleAt      time.Time
    LeaseExpiresAt time.Time
    RedriveOf      DeliveryID
    ReplayID       ReplayID
}

type EventReceipt struct {
    SubscriptionID SubscriptionID
    DeliveryID     DeliveryID
    LeaseID        LeaseID
}

type ReceiveEventsRequest struct {
    SubscriptionID SubscriptionID
    MaxMessages    int
    WaitTime       time.Duration
    VisibilityTimeout time.Duration
}

type NackEventOptions struct {
    Delay time.Duration
    Error *JobError
}
```

Event receipts are distinct from raw-message receipts. Passing either receipt to the other component is a compile-time type mismatch in typed code and a namespace validation error in generic adapters.

## 3. Capability Interfaces

```go
type EventPublisher interface {
    PublishEvent(context.Context, PublishEventRequest) (PublishEventResult, error)
    PublishEvents(context.Context, []PublishEventRequest) ([]PublishEventResult, error)
}

type EventReceiver interface {
    ReceiveEvents(context.Context, ReceiveEventsRequest) ([]EventDelivery, error)
}

type EventSettler interface {
    AckEvent(context.Context, EventReceipt) error
    NackEvent(context.Context, EventReceipt, NackEventOptions) error
    ExtendEventLease(context.Context, EventReceipt, time.Duration) (time.Time, error)
}

type EventBus interface {
    EventPublisher
    EventReceiver
    EventSettler
}
```

Batch publication is atomic and preserves input order. A conflict in one item rolls back the batch. Batch settlement is exposed as an optional PostgreSQL capability and returns one result per input; it is not part of the minimal portable interface.

## 4. Administration APIs

```go
type TopicAdmin interface {
    CreateTopic(context.Context, TopicName, TopicConfig) (Topic, error)
    GetTopic(context.Context, TopicName) (Topic, error)
    ListTopics(context.Context, PageRequest) (TopicPage, error)
    UpdateTopic(context.Context, TopicName, TopicConfig) (Topic, error)
    SetTopicEnabled(context.Context, TopicName, bool) (Topic, error)
    DeleteTopic(context.Context, TopicName) error
}

type SubscriptionAdmin interface {
    CreateSubscription(context.Context, CreateSubscriptionRequest) (Subscription, error)
    GetSubscription(context.Context, SubscriptionID) (Subscription, error)
    ListSubscriptions(context.Context, SubscriptionListOptions) (SubscriptionPage, error)
    UpdateSubscription(context.Context, SubscriptionID, SubscriptionConfig) (Subscription, error)
    SetSubscriptionEnabled(context.Context, SubscriptionID, bool) (Subscription, error)
    DeleteSubscription(context.Context, SubscriptionID, DeleteSubscriptionOptions) error
}

type CreateSubscriptionRequest struct {
    ID     SubscriptionID
    Topic  TopicName
    Name   string
    Config SubscriptionConfig
    Enabled *bool
}

type DeleteSubscriptionOptions struct {
    DiscardBacklogAndHistory bool
}

type TopicPage struct {
    Topics     []Topic
    NextCursor string
}

type SubscriptionListOptions struct {
    Topic   TopicName
    Enabled *bool
    Limit   int
    Cursor  string
}

type SubscriptionPage struct {
    Subscriptions []Subscription
    NextCursor    string
}

type DeadEventDelivery struct {
    ID             DeliveryID
    SubscriptionID SubscriptionID
    Event          Event
    DeliveryCount  int
    Reason         string
    LastError      *JobError
    DeadAt         time.Time
    RedrivenAt     time.Time
    RedriveDeliveryID DeliveryID
}

type DeadEventListOptions struct {
    SubscriptionID SubscriptionID
    Reason         string
    Before         time.Time
    Limit          int
    Cursor         string
}

type DeadEventListPage struct {
    Deliveries []DeadEventDelivery
    NextCursor string
}

type SubscriptionDeadLetterAdmin interface {
    ListDeadEventDeliveries(context.Context, DeadEventListOptions) (DeadEventListPage, error)
    RedriveEventDelivery(context.Context, SubscriptionID, DeliveryID) (RedriveEventResult, error)
    DeleteDeadEventDelivery(context.Context, SubscriptionID, DeliveryID) error
}

type RedriveEventResult struct {
    DeliveryID DeliveryID
    Created    bool
    Active     bool
}
```

Creation is idempotent only when all explicitly supplied stored fields equal existing configuration after defaulting. A different explicit field returns `ErrConflict`.

Disabling a topic rejects new direct or store-backed publications but retains its events and deliveries. Disabling a subscription pauses claims and excludes it from future publication fan-out. Existing backlog remains. Re-enabling either resource does not retroactively fill a disabled interval; callers use explicit replay for a subscription.

Topic deletion succeeds only when no subscription or retained bus event references it. Subscription deletion succeeds only when its active/dead backlog and replay history are empty, unless `DiscardBacklogAndHistory` explicitly authorizes deletion of all those records in dependency order in the same transaction.

## 5. Defaults and Limits

| Setting | Default |
|---|---:|
| topic event minimum retention | 30 days |
| visibility timeout | 30 seconds |
| max deliveries | 5 |
| retry policy | exponential, 1 second base, 5 minutes cap, full jitter |
| max in flight per subscription | 100 |
| active delivery retention | 30 days from first availability |
| dead delivery retention | 14 days |
| receive batch | 1, maximum 100 |
| long-poll wait | 0, maximum 20 seconds |
| publication batch | maximum 1,000 |
| event data | maximum 1 MiB |
| event metadata | maximum 64 KiB |
| filter field | maximum 1 KiB |

Durations must be positive where required and within configured hard caps. Topic, type, source, subject, and subscription names use root validators. Metadata keys and values are bounded individually.

## 6. PostgreSQL Data Model

### 6.1 Topics

```sql
CREATE TABLE jobqueue.bus_topics (
    name text PRIMARY KEY,
    enabled boolean NOT NULL DEFAULT true,
    event_retention interval NOT NULL CHECK (event_retention > interval '0'),
    created_at timestamptz NOT NULL,
    updated_at timestamptz NOT NULL
);
```

### 6.2 Subscriptions

```sql
CREATE TABLE jobqueue.bus_subscriptions (
    id uuid PRIMARY KEY,
    topic_name text NOT NULL REFERENCES jobqueue.bus_topics(name) ON DELETE RESTRICT,
    name text NOT NULL,
    enabled boolean NOT NULL DEFAULT true,
    filter_type_exact text,
    filter_type_prefix text,
    filter_source_exact text,
    filter_source_prefix text,
    filter_subject_exact text,
    filter_subject_prefix text,
    visibility_timeout interval NOT NULL CHECK (visibility_timeout > interval '0'),
    max_deliveries integer NOT NULL CHECK (max_deliveries > 0),
    retry_policy jsonb NOT NULL,
    max_in_flight integer NOT NULL CHECK (max_in_flight > 0),
    delivery_retention interval NOT NULL CHECK (delivery_retention > interval '0'),
    dead_retention interval NOT NULL CHECK (dead_retention > interval '0'),
    created_at timestamptz NOT NULL,
    updated_at timestamptz NOT NULL,
    UNIQUE (topic_name, name),
    CHECK (filter_type_exact IS NULL OR filter_type_prefix IS NULL),
    CHECK (filter_source_exact IS NULL OR filter_source_prefix IS NULL),
    CHECK (filter_subject_exact IS NULL OR filter_subject_prefix IS NULL)
);

CREATE INDEX bus_subscriptions_publish_idx
    ON jobqueue.bus_subscriptions (topic_name, id)
    WHERE enabled;
```

Empty filter strings are normalized to SQL null. Delivery retry policy JSON uses a versioned canonical representation owned by EventBus.

### 6.3 Immutable events

```sql
CREATE TABLE jobqueue.bus_events (
    id uuid PRIMARY KEY,
    topic_name text NOT NULL REFERENCES jobqueue.bus_topics(name) ON DELETE RESTRICT,
    event_type text NOT NULL,
    source text NOT NULL,
    subject text,
    data jsonb,
    metadata jsonb,
    occurred_at timestamptz NOT NULL,
    occurred_at_defaulted boolean NOT NULL,
    correlation_id text,
    causation_id text,
    store_event_id uuid,
    content_fingerprint bytea NOT NULL CHECK (octet_length(content_fingerprint) = 32),
    published_at timestamptz NOT NULL,
    retain_until timestamptz NOT NULL,
    CHECK (
        (store_event_id IS NULL AND data IS NOT NULL AND metadata IS NOT NULL) OR
        (store_event_id IS NOT NULL AND data IS NULL AND metadata IS NULL)
    )
);

CREATE INDEX bus_events_cleanup_idx
    ON jobqueue.bus_events (retain_until, id);
CREATE INDEX bus_events_topic_time_idx
    ON jobqueue.bus_events (topic_name, published_at, id);
```

`EventStore` migrations create `event_store_events` before enabling stored-event publication and add a restrictive foreign key from `bus_events.store_event_id` to its immutable event ID. Direct bus publication always owns `data` and `metadata`; composed store publication references the store row and does not copy them.

### 6.4 Active deliveries

```sql
CREATE TABLE jobqueue.subscription_deliveries (
    id uuid PRIMARY KEY,
    subscription_id uuid NOT NULL
        REFERENCES jobqueue.bus_subscriptions(id) ON DELETE RESTRICT,
    event_id uuid NOT NULL REFERENCES jobqueue.bus_events(id) ON DELETE RESTRICT,
    delivery_count integer NOT NULL DEFAULT 0 CHECK (delivery_count >= 0),
    first_available_at timestamptz NOT NULL,
    visible_at timestamptz NOT NULL,
    lease_token uuid,
    lease_expires_at timestamptz,
    redrive_of uuid,
    replay_id uuid,
    created_at timestamptz NOT NULL,
    UNIQUE (subscription_id, event_id),
    CHECK ((lease_token IS NULL) = (lease_expires_at IS NULL))
);

CREATE INDEX subscription_deliveries_claim_idx
    ON jobqueue.subscription_deliveries
       (subscription_id, visible_at, created_at, id);
CREATE INDEX subscription_deliveries_live_lease_idx
    ON jobqueue.subscription_deliveries
       (subscription_id, lease_expires_at)
    WHERE lease_token IS NOT NULL;
CREATE INDEX subscription_deliveries_retention_idx
    ON jobqueue.subscription_deliveries
       (first_available_at, id);
```

The active uniqueness constraint is the fan-out idempotency fence. Once a delivery is acknowledged or moved to dead storage, an explicit replay/redrive may create a new active identity.

### 6.5 Dead deliveries

```sql
CREATE TABLE jobqueue.subscription_dead_deliveries (
    id uuid PRIMARY KEY,
    subscription_id uuid NOT NULL
        REFERENCES jobqueue.bus_subscriptions(id) ON DELETE RESTRICT,
    event_id uuid NOT NULL REFERENCES jobqueue.bus_events(id) ON DELETE RESTRICT,
    delivery_count integer NOT NULL CHECK (delivery_count > 0),
    first_available_at timestamptz NOT NULL,
    reason text NOT NULL,
    last_error jsonb,
    dead_at timestamptz NOT NULL,
    retain_until timestamptz NOT NULL,
    redrive_of uuid,
    replay_id uuid,
    redriven_at timestamptz,
    redrive_delivery_id uuid,
    CHECK ((redriven_at IS NULL) = (redrive_delivery_id IS NULL))
);

CREATE INDEX subscription_dead_lookup_idx
    ON jobqueue.subscription_dead_deliveries
       (subscription_id, dead_at, id);
CREATE INDEX subscription_dead_cleanup_idx
    ON jobqueue.subscription_dead_deliveries (retain_until, id);
```

### 6.6 Replay requests

```sql
CREATE TABLE jobqueue.subscription_replays (
    id uuid PRIMARY KEY,
    subscription_id uuid NOT NULL
        REFERENCES jobqueue.bus_subscriptions(id) ON DELETE RESTRICT,
    from_published_at timestamptz,
    through_published_at timestamptz,
    request_fingerprint bytea NOT NULL CHECK (octet_length(request_fingerprint) = 32),
    last_published_at timestamptz,
    last_event_id uuid,
    completed boolean NOT NULL DEFAULT false,
    created_at timestamptz NOT NULL,
    updated_at timestamptz NOT NULL
);

ALTER TABLE jobqueue.subscription_deliveries
    ADD CONSTRAINT subscription_deliveries_redrive_fk
    FOREIGN KEY (redrive_of)
    REFERENCES jobqueue.subscription_dead_deliveries(id)
    ON DELETE RESTRICT;

ALTER TABLE jobqueue.subscription_deliveries
    ADD CONSTRAINT subscription_deliveries_replay_fk
    FOREIGN KEY (replay_id)
    REFERENCES jobqueue.subscription_replays(id)
    ON DELETE RESTRICT;

ALTER TABLE jobqueue.subscription_dead_deliveries
    ADD CONSTRAINT subscription_dead_replay_fk
    FOREIGN KEY (replay_id)
    REFERENCES jobqueue.subscription_replays(id)
    ON DELETE RESTRICT;
```

Replay is chunked and resumable. Its stable request ID and checkpoint make commit uncertainty idempotent.

## 7. Publication and Fan-out

### 7.1 Canonical fingerprint

The fingerprint covers topic, type, source, normalized subject, canonical JSON data/metadata or store-event identity, occurrence-time request, correlation, and causation. An omitted occurrence time hashes a stable `database_now` marker while the row stores the actual publication time and `occurred_at_defaulted=true`. Publication time and retention deadline are excluded because the database assigns them only on first creation.

### 7.2 Direct publication transaction

For one event:

1. validate and canonicalize outside the transaction;
2. select the topic and its retention configuration `FOR SHARE` and require it to be enabled, serializing publication with topic disable/update/delete;
3. insert `bus_events` with `ON CONFLICT (id) DO NOTHING`;
4. if not inserted, lock/read the existing row, compare fingerprint, and return `Created=false` or `ErrConflict`; never fan out an already-existing event;
5. select enabled subscriptions for the topic in UUID order whose filters match and lock those rows `FOR SHARE`, serializing their participation with disable/filter update/delete;
6. insert one new active delivery per match with UUIDv7 identity, zero count, and immediate visibility;
7. append compact post-commit wake hints for affected subscriptions;
8. commit and report inserted delivery count.

The event row and all initial delivery references commit atomically. A concurrent same-ID publisher either creates everything or observes the completed immutable event; it cannot create a second fan-out.

A subscription created or enabled after the publication statement snapshot is treated as later and receives no implicit history. A disable/update which finds its row share-locked waits; if publication commits first, that event belongs to the old enabled/filter configuration, otherwise publication observes the new configuration. This gives every administration/publication race a valid transaction order.

`PublishEvents` validates and deduplicates request IDs before opening one transaction. It inserts/fans out in stable event-ID order and returns results in caller order.

### 7.3 Filter SQL

A subscription matches when every non-null condition passes:

```sql
(s.filter_type_exact IS NULL OR s.filter_type_exact = $event_type)
AND (s.filter_type_prefix IS NULL OR left($event_type, length(s.filter_type_prefix)) = s.filter_type_prefix)
AND (s.filter_source_exact IS NULL OR s.filter_source_exact = $source)
AND (s.filter_source_prefix IS NULL OR left($source, length(s.filter_source_prefix)) = s.filter_source_prefix)
AND (s.filter_subject_exact IS NULL OR s.filter_subject_exact = $subject)
AND (s.filter_subject_prefix IS NULL OR left($subject, length(s.filter_subject_prefix)) = s.filter_subject_prefix)
```

Null subjects match only a filter with no subject condition. Prefix matching does not use SQL `LIKE`, so `%`, `_`, and collation rules have no special meaning.

## 8. Claim and Concurrency Algorithm

Claims are per subscription. In one short transaction:

1. lock the subscription row and require `enabled = true`;
2. use database time for all eligibility checks;
3. count current live leases where `lease_expires_at > now`;
4. compute capacity as `max_in_flight - live_count`; return empty if non-positive;
5. move retention-expired or max-delivery exhausted eligible rows to dead storage in a bounded pre-pass;
6. select up to `min(requested, capacity)` eligible deliveries with `FOR UPDATE SKIP LOCKED`, ordered by `(visible_at, created_at, id)`;
7. set a fresh lease token and expiry and increment `delivery_count`;
8. join immutable event data, commit, and return receipts.

Expired leases do not consume concurrency capacity. Reclaiming an expired delivery replaces its token; stale settlement then returns `ErrLeaseLost`.

Claims intentionally serialize on one subscription row to enforce a database-wide `MaxInFlight`, while publication holds matching subscription rows `FOR SHARE` to obtain a transactionally consistent fan-out. A single very hot subscription can therefore make claims contend with publication; this is an accepted correctness trade-off whose practical limit must be measured.

Long polling follows the shared listener/poll loop: attempt claim, subscribe through `WakeHub`, recheck to close the race, and then wake on notification, fallback poll, context, or deadline. Notifications are advisory.

## 9. Settlement

Every settlement statement joins the subscription configuration and requires matching subscription ID, delivery ID, lease token, and `lease_expires_at > clock_timestamp()`.

### 9.1 Acknowledge

`AckEvent` deletes the active delivery. Zero rows maps to not-found/already-removed versus stale lease using a bounded diagnostic query.

### 9.2 Negative acknowledgement

`NackEvent` chooses an explicit validated delay when supplied; otherwise it computes delay from the stored retry policy and current delivery count.

- below max deliveries: clear lease fields and set `visible_at = now + delay`;
- at max deliveries: atomically insert the full row into dead storage with reason `max_deliveries`, then delete active storage.

The stored error passes through the shared redaction and size-bound hook.

### 9.3 Extend

`ExtendEventLease` sets expiry to `now + duration`, never client time plus duration. It returns the database timestamp and does not alter delivery count.

### 9.4 Lease-expiry exhaustion

A maintenance pass moves an expired delivery whose count has reached max deliveries to dead storage before it can be claimed again. Crashing instead of nacking therefore still honors max delivery policy.

## 10. Dead Letter and Redrive

Moving to dead storage is one transaction and preserves event identity, subscription identity, counts, first availability, prior redrive/replay identity, bounded last error, and reason.

`RedriveEventDelivery`:

1. locks the dead row;
2. if already redriven, returns the recorded new identity with `Created=false` and reports whether it is still active;
3. inserts a fresh active delivery ID with count zero, immediate visibility, and `redrive_of = dead.id`;
4. marks the dead row with redrive time and new ID;
5. emits a wake hint and commits.

It never inserts another `bus_events` row or another EventStore event. A conflict with an already-active `(subscription,event)` returns `ErrConflict`; callers must settle that active delivery first. The result is identity-oriented because an idempotent repeat may occur after the redriven delivery was itself settled.

## 11. Historical Replay

```go
type ReplaySubscriptionRequest struct {
    ID                 ReplayID
    SubscriptionID     SubscriptionID
    FromPublishedAt    time.Time
    ThroughPublishedAt time.Time
    BatchSize          int
}

type ReplaySubscriptionResult struct {
    ID        ReplayID
    Inserted  int
    Completed bool
}

type SubscriptionReplay interface {
    ReplaySubscription(context.Context, ReplaySubscriptionRequest) (ReplaySubscriptionResult, error)
}
```

Replay snapshots the inclusive time bounds into a request row and processes one bounded page per call, ordered by `(published_at,id)`. It reuses the subscription's current filter. Each inserted delivery records the replay ID. An active `(subscription,event)` is skipped; an acknowledged or dead historic delivery may be delivered again because replay is explicit.

Repeating a replay ID with different bounds/subscription returns `ErrConflict`. Repeating after uncertain commit resumes from its stored cursor. A convenience loop may drive pages until complete but remains context-cancellable.

## 12. Retention

### 12.1 Delivery retention

Active retention begins at `first_available_at`, not event publication. A due unleased or expired-leased delivery moves to dead storage with reason `delivery_retention_expired`; live unexpired leases are not stolen by cleanup. Dead rows remain until their own `retain_until` even after redrive.

### 12.2 Event retention

An event cleanup query selects `retain_until <= now` rows with `FOR UPDATE SKIP LOCKED` and deletes only when neither active nor dead delivery references the event. Restrictive foreign keys are a second line of defense.

The minimum retention deadline is fixed from the topic configuration at first publication. Updating a topic changes future events only; explicit administration is required to extend existing retained events.

Store-backed bus envelopes obey both policies: removing the bus envelope never removes its referenced EventStore event.

## 13. Transaction Composition

The PostgreSQL implementation exposes `InTx(pgx.Tx)`-bound capabilities. PostgreSQL notifications remain transactional. Commit-dependent observer records use `TxBinding`/`Transact`; the bare `InTx` form suppresses them because it cannot observe the caller's later commit result.

Job `Outcome` event operations use the same direct publication algorithm after finalizer and job/workflow locks but before commit. EventStore append plus bus publication inserts the store event first, then its bus envelope and subscription deliveries, all in one transaction. A fan-out or conflict error rolls back the entire composed operation.

Applications mixing direct bus events with jobs, raw messages, or stream appends use `postgres.Backend.ExecuteAtomic`; standalone `PublishEvent(s)` delegates to that same ordered executor. This lets routing rows be locked before stream allocation while delaying immutable bus inserts until the final transaction phase.

## 14. Inspection

Bounded APIs expose:

- topic and subscription lists;
- active backlog counts by available, delayed, and leased state;
- oldest first-availability and next-visible times;
- live lease count versus configured concurrency;
- dead deliveries by reason and time;
- retained event lookup by event ID;
- replay progress.

Lists use stable `(timestamp,id)` or `(name,id)` cursors with filter fingerprints. Event data is returned only from explicit event/delivery reads and is not an observability dimension.

## 15. Error Mapping

| Condition | Public error |
|---|---|
| missing topic/subscription/event/delivery | `ErrNotFound` with resource detail |
| same event ID and equal content | success with `Created=false` |
| same event ID and different content | `ErrConflict` |
| disabled topic publication or subscription receive | `ErrUnavailable` |
| invalid/expired lease | `ErrLeaseLost` |
| already acknowledged/removed delivery | `ErrRemoved` |
| delete with retained references | `ErrConflict` |
| invalid filter/config/payload/limit | `ErrInvalid` |
| replay ID with different request | `ErrConflict` |

## 16. Observability

Emit commit-aware observations for publication, matched subscription count, fan-out latency, receive/empty receive, acknowledgement, nack, redrive, replay, delivery expiry, dead movement, cleanup, lease loss, and listener wake/fallback poll.

Dimensions include topic, subscription, event type, source, delivery outcome, and redrive/replay marker. Event data, metadata values, subject, and error bodies are excluded by default to avoid cardinality and sensitive-data leakage.

## 17. Test Plan

### 17.1 Administration and filters

- exact idempotent create and conflicting configuration;
- enable/disable semantics and explicit replay after disabled interval;
- exact/prefix/all matching, null subject, and special characters;
- safe delete versus explicitly discarded backlog;
- concurrent update/publication has a transactionally consistent winner.

### 17.2 Publication

- one immutable event and one delivery per matching enabled subscription;
- concurrent same-ID equal publication creates one fan-out;
- same ID with different content conflicts;
- atomic batch rollback and caller-order results;
- zero matching subscriptions still retains the event;
- direct and store-backed envelopes read identically.

### 17.3 Delivery

- at-least-once claim, fresh fences, expiry reclaim, and stale settlement;
- database-enforced max in flight across multiple processes;
- max-delivery dead movement after nack and process crash;
- retry delay and long-poll race closure;
- one subscription's pause/retry/ack never affects another.

### 17.4 Redrive, replay, and retention

- redrive preserves event identity and is idempotent after uncertain commit;
- replay resumes by stable request cursor and deliberately redelivers acknowledged history;
- event cleanup cannot delete while active or dead references remain;
- active/dead retention uses the documented clocks;
- deleting a bus envelope never deletes a store event.

### 17.5 Concurrency and faults

- hundreds of claimers across subscriptions;
- benchmark concurrent high-rate publication and claims against one hot subscription, reporting subscription-row lock wait, publish latency, claim latency, and throughput at increasing publisher/consumer counts;
- publication versus create/disable/delete races;
- crash fault points around fan-out, settlement, dead movement, and commit response;
- no orphan reference under repeated cleanup;
- representative fan-out and filter-query `EXPLAIN (ANALYZE, BUFFERS)` plans.

## 18. Acceptance Conditions

This component is complete when:

1. event-ID idempotency and subscription fan-out commit atomically;
2. filters are deterministic, bounded, and match the documented semantics;
3. subscriptions have isolated backlogs, leases, retry/dead policies, and enforced concurrency;
4. stale receipts cannot settle reclaimed delivery;
5. redrive creates delivery identity only and explicit replay is resumable;
6. event payload cannot be cleaned while any retained delivery references it;
7. direct and EventStore-backed publication compose with caller/job transactions;
8. all administration, conformance, concurrency, retention, and fault tests pass.
