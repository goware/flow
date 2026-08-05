package flow

import (
	"context"
	"encoding/base64"
	"encoding/json"
	"time"
	"unicode/utf8"

	"github.com/google/uuid"
	"github.com/goware/flow/internal/canonical"
	"github.com/goware/flow/internal/definition"
	"github.com/goware/flow/internal/store"
)

const (
	defaultExecutionPageSize = 50
	maxExecutionPageSize     = 200
)

// ExecutionFilter is the bounded, indexed filter supported by
// ListExecutions. CreatedBefore is exclusive and CreatedAfter is inclusive.
type ExecutionFilter struct {
	Type          string
	KeyPrefix     string
	Statuses      []string
	CreatedAfter  *time.Time
	CreatedBefore *time.Time
	Metadata      map[string]string
	PageSize      int
	Cursor        string
}

type ExecutionPage struct {
	Executions []Execution
	NextCursor string
}

type executionCursor struct {
	CreatedAt time.Time `json:"created_at"`
	ID        string    `json:"id"`
}

func GetExecution(ctx context.Context, c Client, id ExecutionID) (Execution, error) {
	executionID, err := parseExecutionID(id)
	if err != nil {
		return Execution{}, err
	}
	client, err := resolveClient(c)
	if err != nil {
		return Execution{}, err
	}
	row, err := client.runtime.store.GetExecutionInTx(ctx, client.tx, executionID)
	if err != nil {
		return Execution{}, err
	}
	return executionFromStore(row), nil
}

// LookupLiveExecution finds the one non-terminal execution currently holding
// a live-scoped key for the definition, if any. Live keys admit many settled
// executions per key over time but at most one live holder; this is the
// lookup that matches that invariant. found=false means no live holder —
// settled executions with the key may still exist.
func LookupLiveExecution(ctx context.Context, c Client, typ, key string) (Execution, bool, error) {
	if err := definition.ValidateName(typ); err != nil {
		return Execution{}, false, newError(ErrInvalid, "lookup", "execution type", typ, "invalid definition name")
	}
	if key == "" || len(key) > maxExecutionKeyBytes || !utf8.ValidString(key) {
		return Execution{}, false, newError(ErrInvalid, "lookup", "execution key", "", "key is empty, malformed, or too long")
	}
	client, err := resolveClient(c)
	if err != nil {
		return Execution{}, false, err
	}
	row, found, err := client.runtime.store.LookupLiveExecutionInTx(ctx, client.tx, typ, key)
	if err != nil || !found {
		return Execution{}, false, err
	}
	return executionFromStore(row), true, nil
}

func ListExecutions(ctx context.Context, c Client, filter ExecutionFilter) (ExecutionPage, error) {
	client, err := resolveClient(c)
	if err != nil {
		return ExecutionPage{}, err
	}
	if filter.Type != "" {
		if err := definition.ValidateName(filter.Type); err != nil {
			return ExecutionPage{}, newError(ErrInvalid, "list", "execution type", filter.Type, "invalid definition name")
		}
	}
	if len(filter.KeyPrefix) > maxExecutionKeyBytes || !utf8.ValidString(filter.KeyPrefix) {
		return ExecutionPage{}, newError(ErrInvalid, "list", "key prefix", "", "key prefix is malformed or too long")
	}
	pageSize := filter.PageSize
	if pageSize == 0 {
		pageSize = defaultExecutionPageSize
	}
	if pageSize < 1 || pageSize > maxExecutionPageSize {
		return ExecutionPage{}, newError(ErrInvalid, "list", "page size", "", "page size must be between 1 and 200")
	}
	statuses, err := validateExecutionStatuses(filter.Statuses)
	if err != nil {
		return ExecutionPage{}, err
	}
	if filter.CreatedAfter != nil && filter.CreatedBefore != nil && !filter.CreatedAfter.Before(*filter.CreatedBefore) {
		return ExecutionPage{}, newError(ErrInvalid, "list", "time range", "", "created-after must precede created-before")
	}
	if err := validateMetadata(filter.Metadata); err != nil {
		return ExecutionPage{}, err
	}
	var metadataBytes []byte
	if len(filter.Metadata) != 0 {
		metadata, err := canonical.Marshal(filter.Metadata, maxExecutionMetadataBytes)
		if err != nil {
			return ExecutionPage{}, mapCanonicalError("list", "metadata", err)
		}
		metadataBytes = metadata.BytesCopy()
	}
	var cursorTime *time.Time
	var cursorID *uuid.UUID
	if filter.Cursor != "" {
		decoded, err := decodeExecutionCursor(filter.Cursor)
		if err != nil {
			return ExecutionPage{}, err
		}
		cursorTime, cursorID = &decoded.CreatedAt, &decoded.ID
	}
	rows, err := client.runtime.store.ListExecutionsInTx(ctx, client.tx, store.ExecutionListFilter{
		DefinitionName: filter.Type, KeyPrefix: filter.KeyPrefix, Statuses: statuses,
		CreatedAfter: cloneTimePointer(filter.CreatedAfter), CreatedBefore: cloneTimePointer(filter.CreatedBefore),
		Metadata: metadataBytes, CursorCreated: cursorTime, CursorID: cursorID, Limit: pageSize + 1,
	})
	if err != nil {
		return ExecutionPage{}, err
	}
	page := ExecutionPage{Executions: make([]Execution, min(len(rows), pageSize))}
	for i := range page.Executions {
		page.Executions[i] = executionFromStore(rows[i])
	}
	if len(rows) > pageSize {
		last := rows[pageSize-1]
		page.NextCursor, err = encodeExecutionCursor(last.CreatedAt, last.ID)
		if err != nil {
			return ExecutionPage{}, err
		}
	}
	return page, nil
}

// AwaitExecution polls the durable execution row until it reaches a terminal
// state or ctx ends. It consumes no command worker, connection while waiting,
// or durable lease.
func AwaitExecution(ctx context.Context, c Client, id ExecutionID) (Execution, error) {
	client, err := resolveClient(c)
	if err != nil {
		return Execution{}, err
	}
	if client.tx != nil {
		return Execution{}, newError(ErrInvalid, "await", "transaction client", string(id), "await cannot observe commits while a caller transaction remains open")
	}
	interval := min(client.runtime.pollInterval, 250*time.Millisecond)
	if interval <= 0 {
		interval = 250 * time.Millisecond
	}
	timer := time.NewTimer(0)
	defer timer.Stop()
	for {
		select {
		case <-ctx.Done():
			return Execution{}, ctx.Err()
		case <-timer.C:
		}
		execution, err := GetExecution(ctx, c, id)
		if err != nil {
			return Execution{}, err
		}
		if isTerminalExecutionStatus(execution.Status) {
			return execution, nil
		}
		timer.Reset(interval)
	}
}

func executionFromStore(row store.ExecutionRow) Execution {
	return Execution{
		ID: ExecutionID(row.ID.String()), Type: row.DefinitionName, Version: row.DefinitionVersion,
		Key: row.Key, Status: row.Status, FailFast: row.FailFast, MaxCommands: row.MaxCommands,
		CommandCount: row.CommandCount, OpenCommands: row.OpenCommands,
		DeadlineAt:  cloneTimePointer(row.DeadlineAt),
		FailureCode: row.FailureCode, FailureMessage: row.FailureMessage,
		CreatedAt: row.CreatedAt, UpdatedAt: row.UpdatedAt, StatusAt: row.StatusAt,
		FinishedAt: cloneTimePointer(row.FinishedAt), Metadata: json.RawMessage(append([]byte(nil), row.Metadata...)),
	}
}

func validateExecutionStatuses(values []string) ([]string, error) {
	allowed := map[string]struct{}{
		"running": {}, "failing": {}, "succeeded": {}, "failed": {}, "cancelled": {}, "expired": {},
	}
	seen := make(map[string]struct{}, len(values))
	result := make([]string, 0, len(values))
	for _, value := range values {
		if _, ok := allowed[value]; !ok {
			return nil, newError(ErrInvalid, "list", "status", value, "unknown execution status")
		}
		if _, ok := seen[value]; ok {
			continue
		}
		seen[value] = struct{}{}
		result = append(result, value)
	}
	return result, nil
}

func isTerminalExecutionStatus(status string) bool {
	switch status {
	case "succeeded", "failed", "cancelled", "expired":
		return true
	default:
		return false
	}
}

func encodeExecutionCursor(createdAt time.Time, id uuid.UUID) (string, error) {
	encoded, err := json.Marshal(executionCursor{CreatedAt: createdAt, ID: id.String()})
	if err != nil {
		return "", newError(ErrInvalidState, "list", "cursor", "", "cannot encode cursor")
	}
	return base64.RawURLEncoding.EncodeToString(encoded), nil
}

func decodeExecutionCursor(value string) (struct {
	CreatedAt time.Time
	ID        uuid.UUID
}, error) {
	var result struct {
		CreatedAt time.Time
		ID        uuid.UUID
	}
	if len(value) > 512 {
		return result, newError(ErrInvalid, "list", "cursor", "", "cursor is malformed")
	}
	data, err := base64.RawURLEncoding.DecodeString(value)
	if err != nil {
		return result, newError(ErrInvalid, "list", "cursor", "", "cursor is malformed")
	}
	var cursor executionCursor
	if err := json.Unmarshal(data, &cursor); err != nil || cursor.CreatedAt.IsZero() {
		return result, newError(ErrInvalid, "list", "cursor", "", "cursor is malformed")
	}
	id, err := uuid.Parse(cursor.ID)
	if err != nil {
		return result, newError(ErrInvalid, "list", "cursor", "", "cursor is malformed")
	}
	result.CreatedAt, result.ID = cursor.CreatedAt, id
	return result, nil
}

// QueueDepth is a point-in-time operational snapshot of one queue lane.
// Ready commands are deliverable now, Delayed commands wait out a retry
// backoff or start delay, and Running commands hold an attempt lease.
// OldestReadyFor is how long the oldest deliverable command has been ready;
// a growing value with stable Ready means no compatible worker is claiming
// the lane.
type QueueDepth struct {
	Queue          string
	Ready          int64
	Delayed        int64
	Running        int64
	OldestReadyFor time.Duration
}

// GetQueueDepth reports the lane's current deliverable, scheduled, and leased
// command counts. It reads operational delivery state, not application
// events: the counts change as attempts are claimed and settled.
func GetQueueDepth(ctx context.Context, c Client, queue string) (QueueDepth, error) {
	client, err := resolveClient(c)
	if err != nil {
		return QueueDepth{}, err
	}
	row, err := client.runtime.store.QueueDepthInTx(ctx, client.tx, queue)
	if err != nil {
		return QueueDepth{}, err
	}
	return QueueDepth{
		Queue: queue, Ready: row.Ready, Delayed: row.Delayed,
		Running: row.Running, OldestReadyFor: row.OldestReadyFor,
	}, nil
}
