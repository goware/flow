package flow

import (
	"context"
	"encoding/base64"
	"encoding/json"
	"time"
	"unicode/utf8"

	"github.com/google/uuid"
	"github.com/goware/flow/internal/definition"
	"github.com/goware/flow/internal/failure"
	"github.com/goware/flow/internal/store"
)

const (
	defaultRunPageSize = 50
	maxRunPageSize     = 200
)

// RunFilter is the bounded, indexed filter supported by
// ListRuns. CreatedBefore is exclusive and CreatedAfter is inclusive.
type RunFilter struct {
	Type          string
	KeyPrefix     string
	Statuses      []RunStatus
	CreatedAfter  *time.Time
	CreatedBefore *time.Time
	PageSize      int
	Cursor        string
}

type RunPage struct {
	Runs       []Run
	NextCursor string
}

type runCursor struct {
	CreatedAt time.Time `json:"created_at"`
	ID        string    `json:"id"`
}

func GetRun(ctx context.Context, c Client, id RunID) (Run, error) {
	runID, err := parseRunID(id)
	if err != nil {
		return Run{}, err
	}
	client, err := resolveClient(c)
	if err != nil {
		return Run{}, err
	}
	row, err := client.runtime.store.GetRunInTx(ctx, client.tx, runID)
	if err != nil {
		return Run{}, err
	}
	return runFromStore(row)
}

// GetResult reads the typed result projection for one command key without
// loading or replaying the run trace. found=false means that the run exists
// but the command has no successful result yet, including when the command is
// absent, pending, running, or terminal without success. A stored command with
// a different name or version returns ErrConflict.
func GetResult[A, R any](
	ctx context.Context,
	c Client,
	id RunID,
	key string,
	cmd Command[A, R],
) (R, bool, error) {
	return getResult(ctx, c, id, key, cmd)
}

// GetResult reads this command definition's typed result projection. It is
// equivalent to the top-level GetResult form, with the definition supplied by
// the receiver.
func (cmd Command[A, R]) GetResult(
	ctx context.Context,
	c Client,
	id RunID,
	key string,
) (R, bool, error) {
	return getResult(ctx, c, id, key, cmd)
}

func getResult[A, R any](
	ctx context.Context,
	c Client,
	id RunID,
	key string,
	cmd Command[A, R],
) (R, bool, error) {
	var zero R
	if cmd.def == nil || cmd.err != nil {
		return zero, false, newError(ErrInvalid, "get", "command result", key, "invalid command definition")
	}
	runID, err := parseRunID(id)
	if err != nil {
		return zero, false, err
	}
	if err := validateStableKey(key, maxCommandKeyBytes, "command"); err != nil {
		return zero, false, err
	}
	client, err := resolveClient(c)
	if err != nil {
		return zero, false, err
	}
	row, exists, err := client.runtime.store.GetCommandResultInTx(ctx, client.tx, runID, key)
	if err != nil || !exists {
		return zero, false, err
	}
	if row.Name != cmd.Name() || row.Version != cmd.Version() {
		return zero, false, newError(ErrConflict, "get", "command result", key, "stored command definition differs")
	}
	status, err := commandStatusFromString(row.State)
	if err != nil {
		return zero, false, newError(ErrInvalidState, "get", "command result", key, "stored command status is unknown")
	}
	if status != CommandStatusSucceeded {
		return zero, false, nil
	}
	if len(row.Result) == 0 {
		return zero, false, newError(ErrInvalidState, "get", "command result", key, "stored successful result is missing")
	}
	decoded, err := cmd.def.Result.Decode(row.Result)
	if err != nil {
		return zero, false, newError(ErrInvalidState, "get", "command result", key, "stored result cannot be decoded")
	}
	result, ok := decoded.(R)
	if !ok {
		return zero, false, newError(ErrInvalidState, "get", "command result", key, "stored result has an incompatible type")
	}
	return result, true, nil
}

// GetCurrentRun finds the one non-terminal run currently holding
// a live-scoped key for the definition, if any. Live keys admit many settled
// runs per key over time but at most one live holder; this is the
// lookup that matches that invariant. found=false means no live holder —
// settled runs with the key may still exist.
func GetCurrentRun(ctx context.Context, c Client, rootCommandName, key string) (Run, bool, error) {
	return getCurrentRun(ctx, c, rootCommandName, key)
}

// GetCurrentRun finds the live-key run currently held by this root command
// family. The lookup is name-scoped rather than version-scoped so an older
// command version that is still running remains discoverable.
func (cmd Command[A, R]) GetCurrentRun(ctx context.Context, c Client, key string) (Run, bool, error) {
	if cmd.def == nil || cmd.err != nil {
		return Run{}, false, newError(ErrInvalid, "lookup", "root command", cmd.Name(), "invalid command definition")
	}
	return getCurrentRun(ctx, c, cmd.Name(), key)
}

func getCurrentRun(ctx context.Context, c Client, rootCommandName, key string) (Run, bool, error) {
	if err := definition.ValidateName(rootCommandName); err != nil {
		return Run{}, false, newError(ErrInvalid, "lookup", "root command name", rootCommandName, "invalid definition name")
	}
	if key == "" || len(key) > maxRunKeyBytes || !utf8.ValidString(key) {
		return Run{}, false, newError(ErrInvalid, "lookup", "run key", "", "key is empty, malformed, or too long")
	}
	client, err := resolveClient(c)
	if err != nil {
		return Run{}, false, err
	}
	row, found, err := client.runtime.store.GetCurrentRun(ctx, client.tx, rootCommandName, key)
	if err != nil || !found {
		return Run{}, false, err
	}
	run, err := runFromStore(row)
	return run, err == nil, err
}

func ListRuns(ctx context.Context, c Client, filter RunFilter) (RunPage, error) {
	client, err := resolveClient(c)
	if err != nil {
		return RunPage{}, err
	}
	if filter.Type != "" {
		if err := definition.ValidateName(filter.Type); err != nil {
			return RunPage{}, newError(ErrInvalid, "list", "run type", filter.Type, "invalid definition name")
		}
	}
	if len(filter.KeyPrefix) > maxRunKeyBytes || !utf8.ValidString(filter.KeyPrefix) {
		return RunPage{}, newError(ErrInvalid, "list", "key prefix", "", "key prefix is malformed or too long")
	}
	pageSize := filter.PageSize
	if pageSize == 0 {
		pageSize = defaultRunPageSize
	}
	if pageSize < 1 || pageSize > maxRunPageSize {
		return RunPage{}, newError(ErrInvalid, "list", "page size", "", "page size must be between 1 and 200")
	}
	statuses, err := validateRunStatuses(filter.Statuses)
	if err != nil {
		return RunPage{}, err
	}
	if filter.CreatedAfter != nil && filter.CreatedBefore != nil && !filter.CreatedAfter.Before(*filter.CreatedBefore) {
		return RunPage{}, newError(ErrInvalid, "list", "time range", "", "created-after must precede created-before")
	}
	var cursorTime *time.Time
	var cursorID *uuid.UUID
	if filter.Cursor != "" {
		decoded, err := decodeRunCursor(filter.Cursor)
		if err != nil {
			return RunPage{}, err
		}
		cursorTime, cursorID = &decoded.CreatedAt, &decoded.ID
	}
	rows, err := client.runtime.store.ListRunsInTx(ctx, client.tx, store.RunListFilter{
		DefinitionName: filter.Type, KeyPrefix: filter.KeyPrefix, Statuses: statuses,
		CreatedAfter: cloneTimePointer(filter.CreatedAfter), CreatedBefore: cloneTimePointer(filter.CreatedBefore),
		CursorCreated: cursorTime, CursorID: cursorID, Limit: pageSize + 1,
	})
	if err != nil {
		return RunPage{}, err
	}
	page := RunPage{Runs: make([]Run, min(len(rows), pageSize))}
	for i := range page.Runs {
		page.Runs[i], err = runFromStore(rows[i])
		if err != nil {
			return RunPage{}, err
		}
	}
	if len(rows) > pageSize {
		last := rows[pageSize-1]
		page.NextCursor, err = encodeRunCursor(last.CreatedAt, last.ID)
		if err != nil {
			return RunPage{}, err
		}
	}
	return page, nil
}

// AwaitRun polls the durable run row until it reaches a terminal
// state or ctx ends. It consumes no command worker, connection while waiting,
// or durable lease.
func AwaitRun(ctx context.Context, c Client, id RunID) (Run, error) {
	client, err := resolveClient(c)
	if err != nil {
		return Run{}, err
	}
	if client.tx != nil {
		return Run{}, newError(ErrInvalid, "await", "transaction client", string(id), "await cannot observe commits while a caller transaction remains open")
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
			return Run{}, ctx.Err()
		case <-timer.C:
		}
		run, err := GetRun(ctx, c, id)
		if err != nil {
			return Run{}, err
		}
		if isTerminalRunStatus(run.Status) {
			return run, nil
		}
		timer.Reset(interval)
	}
}

func runFromStore(row store.RunRow) (Run, error) {
	status, err := runStatusFromString(row.Status)
	if err != nil {
		return Run{}, newError(ErrInvalidState, "decode", "run status", row.Status, "stored status is unknown")
	}
	if row.RootCommandID == nil {
		return Run{}, newError(ErrInvalidState, "decode", "root command", row.ID.String(), "stored root command is missing")
	}
	run := Run{
		ID: RunID(row.ID.String()), Type: row.DefinitionName, Version: row.DefinitionVersion,
		Key: row.Key, Status: status, MaxCommands: row.MaxCommands,
		RootCommandID: CommandID(row.RootCommandID.String()),
		CommandCount:  row.CommandCount, OpenCommands: row.OpenCommands,
		DeadlineAt: cloneTimePointer(row.DeadlineAt), Failure: failure.Clone(row.Failure),
		CreatedAt: row.CreatedAt, UpdatedAt: row.UpdatedAt, StatusAt: row.StatusAt,
		FinishedAt: cloneTimePointer(row.FinishedAt),
	}
	return run, nil
}

func validateRunStatuses(values []RunStatus) ([]string, error) {
	seen := make(map[string]struct{}, len(values))
	result := make([]string, 0, len(values))
	for _, value := range values {
		if _, err := runStatusFromString(string(value)); err != nil {
			return nil, newError(ErrInvalid, "list", "status", string(value), "unknown run status")
		}
		encoded := string(value)
		if _, ok := seen[encoded]; ok {
			continue
		}
		seen[encoded] = struct{}{}
		result = append(result, encoded)
	}
	return result, nil
}

func isTerminalRunStatus(status RunStatus) bool {
	switch status {
	case RunStatusSucceeded, RunStatusFailed, RunStatusCancelled, RunStatusExpired:
		return true
	default:
		return false
	}
}

func encodeRunCursor(createdAt time.Time, id uuid.UUID) (string, error) {
	encoded, err := json.Marshal(runCursor{CreatedAt: createdAt, ID: id.String()})
	if err != nil {
		return "", newError(ErrInvalidState, "list", "cursor", "", "cannot encode cursor")
	}
	return base64.RawURLEncoding.EncodeToString(encoded), nil
}

func decodeRunCursor(value string) (struct {
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
	var cursor runCursor
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
