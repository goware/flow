package store

import (
	"bytes"
	"context"
	"crypto/sha256"
	"encoding/hex"
	"encoding/json"
	"errors"
	"fmt"
	"math"
	"slices"
	"time"

	"github.com/google/uuid"
	"github.com/goware/flow/internal/canonical"
	"github.com/goware/flow/internal/flowerr"
	"github.com/goware/flow/internal/pgschema"
	"github.com/goware/flow/internal/store/journalcodec"
	"github.com/goware/pgkit/v2"
	"github.com/jackc/pgx/v5"
)

const MaxHistoryLimit = 1000

var ErrLockUnavailable = errors.New("flow store: execution lock unavailable")

type LockMode uint8

const (
	LockBlocking LockMode = iota
	LockSkipLocked
)

type Store struct {
	db                  *pgkit.DB
	schema              string
	notifications       bool
	notificationChannel string
}

// New constructs the PostgreSQL store. Notifications controls transactional
// wake hints; correctness never depends on it.
func New(db *pgkit.DB, schema string, notifications bool) (*Store, error) {
	if db == nil || db.Conn == nil {
		return nil, fmt.Errorf("%w: database is nil", flowerr.ErrInvalid)
	}
	if err := pgschema.Validate(schema); err != nil {
		return nil, fmt.Errorf("%w: schema: %s", flowerr.ErrInvalid, err)
	}
	database := db.Conn.Config().ConnConfig.Database
	return &Store{
		db: db, schema: schema, notifications: notifications,
		notificationChannel: NotificationChannel(schema, database),
	}, nil
}

// NotificationChannel returns the database-local channel used for bounded
// wake hints. PostgreSQL already isolates channels by database; including the
// database and schema identities in the digest also makes the name stable and
// collision-resistant for diagnostics and tests.
func NotificationChannel(schema, database string) string {
	digest := sha256.Sum256([]byte(schema + "\x00" + database))
	return "flow_" + hex.EncodeToString(digest[:12])
}

// NotificationChannel returns this store's stable LISTEN/NOTIFY channel.
func (s *Store) NotificationChannel() string {
	if s == nil {
		return ""
	}
	return s.notificationChannel
}

// ParseNotificationHint validates the deliberately tiny, versioned payload.
// A hint is never durable work and is safe to discard; polling remains the
// correctness mechanism for malformed or future versions.
func ParseNotificationHint(payload string) (uuid.UUID, bool) {
	var hint struct {
		V    int    `json:"v"`
		Kind string `json:"kind"`
		Key  string `json:"key"`
	}
	if err := json.Unmarshal([]byte(payload), &hint); err != nil || hint.V != 1 || hint.Kind != "execution" {
		return uuid.Nil, false
	}
	id, err := uuid.Parse(hint.Key)
	return id, err == nil
}

type SemanticTx struct {
	store       *Store
	tx          pgx.Tx
	executionID uuid.UUID
	dbNow       time.Time
	closed      bool
	applied     bool
	failed      bool
}

func (s *Store) BeginSemantic(ctx context.Context, id uuid.UUID, mode LockMode) (*SemanticTx, error) {
	if id == uuid.Nil {
		return nil, fmt.Errorf("%w: execution ID is nil", flowerr.ErrInvalid)
	}
	if mode != LockBlocking && mode != LockSkipLocked {
		return nil, fmt.Errorf("%w: unknown lock mode", flowerr.ErrInvalid)
	}
	tx, err := s.db.Conn.BeginTx(ctx, pgx.TxOptions{IsoLevel: pgx.ReadCommitted})
	if err != nil {
		return nil, MapError("begin semantic transaction", err)
	}
	semantic, err := s.AttachSemantic(ctx, tx, id, mode)
	if err != nil {
		_ = tx.Rollback(context.WithoutCancel(ctx))
		return nil, err
	}
	return semantic, nil
}

// AttachSemantic acquires an execution-first semantic lock inside a
// caller-owned transaction. The returned value never takes ownership of the
// transaction; callers that use this entry point remain responsible for its
// final commit or rollback.
func (s *Store) AttachSemantic(ctx context.Context, tx pgx.Tx, id uuid.UUID, mode LockMode) (*SemanticTx, error) {
	if tx == nil {
		return nil, fmt.Errorf("%w: transaction is nil", flowerr.ErrInvalid)
	}
	if id == uuid.Nil {
		return nil, fmt.Errorf("%w: execution ID is nil", flowerr.ErrInvalid)
	}
	if mode != LockBlocking && mode != LockSkipLocked {
		return nil, fmt.Errorf("%w: unknown lock mode", flowerr.ErrInvalid)
	}
	lockSQL := `SELECT execution_id FROM ` + pgschema.Table(s.schema, "flow_executions") + ` WHERE execution_id=$1 FOR UPDATE`
	if mode == LockSkipLocked {
		lockSQL += ` SKIP LOCKED`
	}
	var locked uuid.UUID
	if err := tx.QueryRow(ctx, lockSQL, id).Scan(&locked); err != nil {
		if mode == LockSkipLocked && errors.Is(err, pgx.ErrNoRows) {
			return nil, ErrLockUnavailable
		}
		return nil, MapError("lock execution", err)
	}
	var dbNow time.Time
	if err := tx.QueryRow(ctx, `SELECT clock_timestamp()`).Scan(&dbNow); err != nil {
		return nil, MapError("capture database time", err)
	}
	return &SemanticTx{store: s, tx: tx, executionID: locked, dbNow: dbNow}, nil
}

// AdoptSemantic wraps a newly inserted execution row that the supplied
// transaction already owns. It is used only by the start path after the insert
// has established row ownership and database time has been captured.
func (s *Store) AdoptSemantic(tx pgx.Tx, id uuid.UUID, dbNow time.Time) (*SemanticTx, error) {
	if tx == nil || id == uuid.Nil || dbNow.IsZero() {
		return nil, fmt.Errorf("%w: incomplete adopted semantic transaction", flowerr.ErrInvalid)
	}
	return &SemanticTx{store: s, tx: tx, executionID: id, dbNow: dbNow}, nil
}

func (tx *SemanticTx) PGX() pgx.Tx {
	if tx == nil {
		return nil
	}
	return tx.tx
}

func (tx *SemanticTx) DBNow() time.Time {
	if tx == nil {
		return time.Time{}
	}
	return tx.dbNow
}

func (tx *SemanticTx) ExecutionID() uuid.UUID {
	if tx == nil {
		return uuid.Nil
	}
	return tx.executionID
}

type EntryKind string

const (
	ExecutionStarted EntryKind = "execution_started"
	ExecutionFailing EntryKind = "execution_failing"
	CommandCreated   EntryKind = "command_created"
	AttemptStarted   EntryKind = "attempt_started"
	AttemptConcluded EntryKind = "attempt_concluded"
	EventRecorded    EntryKind = "event_recorded"
)

type JournalEntry struct {
	EntryID             uuid.UUID
	Kind                EntryKind
	CausationPosition   *int64
	CausationBatchIndex *int
	CommandID           *uuid.UUID
	AttemptID           *uuid.UUID
	EventID             *uuid.UUID
	EventNamespace      *string
	EventName           *string
	EventKey            *string
	EventClass          *string
	TerminalStatus      *string
	Body                canonical.Value
}

func NewJournalEntry(kind EntryKind, body any) (JournalEntry, error) {
	encoded, err := journalcodec.Encode(body)
	if err != nil {
		return JournalEntry{}, fmt.Errorf("journal body: %w", err)
	}
	return JournalEntry{EntryID: uuid.New(), Kind: kind, Body: encoded}, nil
}

type PersistedChangeSet struct {
	Journal []JournalEntry
}

type ApplyResult struct {
	Journal []JournalRow
}

// Apply appends exactly one deterministically ordered semantic batch through a
// SemanticTx value. Internal batch operations may derive another value over
// the same owned PostgreSQL transaction and DBNow after the first application.
func (tx *SemanticTx) Apply(ctx context.Context, changes PersistedChangeSet) (ApplyResult, error) {
	if err := tx.ensureOpen("apply"); err != nil {
		return ApplyResult{}, err
	}
	if tx.applied {
		return ApplyResult{}, fmt.Errorf("%w: semantic change set already applied", flowerr.ErrInvalidState)
	}
	if len(changes.Journal) == 0 {
		return ApplyResult{}, fmt.Errorf("%w: semantic change set is empty", flowerr.ErrInvalid)
	}
	if err := validateJournalBatch(changes.Journal); err != nil {
		return ApplyResult{}, err
	}
	first, err := tx.nextJournalPosition(ctx)
	if err != nil {
		return ApplyResult{}, err
	}
	rows := make([]JournalRow, len(changes.Journal))
	copyRows := make([][]any, len(changes.Journal))
	for i, entry := range changes.Journal {
		position := first + int64(i)
		causation, err := resolveCausation(entry, first, i)
		if err != nil {
			return ApplyResult{}, err
		}
		row := rowFromEntry(tx.executionID, position, tx.dbNow, causation, entry)
		rows[i] = row
		copyRows[i] = row.copyValues()
	}
	if err := tx.reserveJournal(ctx, first, len(changes.Journal)); err != nil {
		tx.failed = true
		return ApplyResult{}, err
	}
	count, err := tx.tx.CopyFrom(ctx,
		pgx.Identifier{tx.store.schema, "flow_journal"}, journalColumns, pgx.CopyFromRows(copyRows))
	if err != nil {
		tx.failed = true
		return ApplyResult{}, MapError("append journal", err)
	}
	if count != int64(len(copyRows)) {
		tx.failed = true
		return ApplyResult{}, fmt.Errorf("%w: journal batch inserted %d of %d rows", flowerr.ErrInvalidState, count, len(copyRows))
	}
	if tx.store.notifications {
		payload := `{"v":1,"kind":"execution","key":"` + tx.executionID.String() + `"}`
		if _, err := tx.tx.Exec(ctx, `SELECT pg_notify($1, $2)`, tx.store.notificationChannel, payload); err != nil {
			tx.failed = true
			return ApplyResult{}, MapError("emit notification hint", err)
		}
	}
	tx.applied = true
	return ApplyResult{Journal: cloneJournalRows(rows)}, nil
}

func (tx *SemanticTx) continueBatch() *SemanticTx {
	return &SemanticTx{
		store: tx.store, tx: tx.tx, executionID: tx.executionID, dbNow: tx.dbNow,
	}
}

func (tx *SemanticTx) nextJournalPosition(ctx context.Context) (int64, error) {
	var next int64
	if err := tx.tx.QueryRow(ctx, `SELECT next_journal_position FROM `+
		pgschema.Table(tx.store.schema, "flow_executions")+` WHERE execution_id=$1`, tx.executionID).Scan(&next); err != nil {
		return 0, MapError("read journal position", err)
	}
	return next, nil
}

func (tx *SemanticTx) reserveJournal(ctx context.Context, expected int64, count int) error {
	if count <= 0 {
		return fmt.Errorf("%w: journal reservation must be positive", flowerr.ErrInvalid)
	}
	var first int64
	err := tx.tx.QueryRow(ctx, `UPDATE `+pgschema.Table(tx.store.schema, "flow_executions")+`
		SET next_journal_position=next_journal_position+$2, updated_at=$3
		WHERE execution_id=$1 AND next_journal_position=$4
		RETURNING next_journal_position-$2`, tx.executionID, count, tx.dbNow, expected).Scan(&first)
	if err != nil {
		return MapError("reserve journal", err)
	}
	if first != expected {
		return fmt.Errorf("%w: journal allocator changed while execution was locked", flowerr.ErrInvalidState)
	}
	return nil
}

func (tx *SemanticTx) Commit(ctx context.Context) error {
	if err := tx.ensureOpen("commit"); err != nil {
		return err
	}
	if tx.failed {
		_ = tx.Rollback(context.WithoutCancel(ctx))
		return fmt.Errorf("%w: semantic transaction has a failed durable mutation", flowerr.ErrInvalidState)
	}
	tx.closed = true
	if err := tx.tx.Commit(ctx); err != nil {
		return MapError("commit semantic transaction", err)
	}
	return nil
}

func (tx *SemanticTx) Rollback(ctx context.Context) error {
	if tx == nil || tx.closed {
		return nil
	}
	tx.closed = true
	if err := tx.tx.Rollback(ctx); err != nil && !errors.Is(err, pgx.ErrTxClosed) {
		return MapError("rollback semantic transaction", err)
	}
	return nil
}

func (tx *SemanticTx) ensureOpen(operation string) error {
	if tx == nil || tx.closed {
		return fmt.Errorf("%w: cannot %s closed semantic transaction", flowerr.ErrClosed, operation)
	}
	return nil
}

type JournalRow struct {
	ExecutionID       uuid.UUID
	Position          int64
	EntryID           uuid.UUID
	Kind              EntryKind
	RecordedAt        time.Time
	CausationPosition *int64
	CommandID         *uuid.UUID
	AttemptID         *uuid.UUID
	EventID           *uuid.UUID
	EventNamespace    *string
	EventName         *string
	EventKey          *string
	EventClass        *string
	TerminalStatus    *string
	Body              []byte
	BodyHash          [sha256.Size]byte
}

var journalColumns = []string{
	"execution_id", "position", "entry_id", "entry_kind", "recorded_at", "causation_position",
	"command_id", "attempt_id",
	"event_id", "event_namespace", "event_name", "event_key", "event_class", "terminal_status",
	"body", "body_hash",
}

func rowFromEntry(executionID uuid.UUID, position int64, recordedAt time.Time, causation *int64, entry JournalEntry) JournalRow {
	return JournalRow{
		ExecutionID: executionID, Position: position, EntryID: entry.EntryID, Kind: entry.Kind,
		RecordedAt: recordedAt, CausationPosition: clonePointer(causation),
		CommandID: clonePointer(entry.CommandID), AttemptID: clonePointer(entry.AttemptID),
		EventID: clonePointer(entry.EventID), EventNamespace: clonePointer(entry.EventNamespace),
		EventName: clonePointer(entry.EventName),
		EventKey:  clonePointer(entry.EventKey), EventClass: clonePointer(entry.EventClass),
		TerminalStatus: clonePointer(entry.TerminalStatus), Body: entry.Body.BytesCopy(), BodyHash: entry.Body.Digest,
	}
}

func (row JournalRow) copyValues() []any {
	return []any{
		row.ExecutionID, row.Position, row.EntryID, string(row.Kind), row.RecordedAt, row.CausationPosition,
		row.CommandID, row.AttemptID,
		row.EventID, row.EventNamespace, row.EventName, row.EventKey, row.EventClass, row.TerminalStatus,
		row.Body, row.BodyHash[:],
	}
}

func (s *Store) History(ctx context.Context, id uuid.UUID, after uint64, limit int) ([]JournalRow, error) {
	return s.HistoryInTx(ctx, nil, id, after, limit)
}

// HistoryInTx reads history through tx when supplied so transaction-scoped
// inspection can observe its own uncommitted Flow writes.
func (s *Store) HistoryInTx(ctx context.Context, tx pgx.Tx, id uuid.UUID, after uint64, limit int) ([]JournalRow, error) {
	if id == uuid.Nil {
		return nil, fmt.Errorf("%w: execution ID is nil", flowerr.ErrInvalid)
	}
	if after > math.MaxInt64 || limit <= 0 || limit > MaxHistoryLimit {
		return nil, fmt.Errorf("%w: history bounds are invalid", flowerr.ErrInvalid)
	}
	query := `SELECT ` + joinIdentifiers(journalColumns) + `
		FROM ` + pgschema.Table(s.schema, "flow_journal") + `
		WHERE execution_id=$1 AND position>$2
		ORDER BY position LIMIT $3`
	var rows pgx.Rows
	var err error
	if tx != nil {
		rows, err = tx.Query(ctx, query, id, int64(after), limit)
	} else {
		rows, err = s.db.Conn.Query(ctx, query, id, int64(after), limit)
	}
	if err != nil {
		return nil, MapError("read history", err)
	}
	defer rows.Close()
	history := make([]JournalRow, 0, min(limit, 64))
	for rows.Next() {
		row, err := scanJournalRow(rows)
		if err != nil {
			return nil, err
		}
		history = append(history, row)
	}
	if err := rows.Err(); err != nil {
		return nil, MapError("read history rows", err)
	}
	return history, nil
}

func scanJournalRow(row pgx.Row) (JournalRow, error) {
	var result JournalRow
	var kind string
	var bodyHash []byte
	if err := row.Scan(
		&result.ExecutionID, &result.Position, &result.EntryID, &kind, &result.RecordedAt, &result.CausationPosition,
		&result.CommandID, &result.AttemptID,
		&result.EventID, &result.EventNamespace, &result.EventName, &result.EventKey,
		&result.EventClass, &result.TerminalStatus, &result.Body, &bodyHash,
	); err != nil {
		return JournalRow{}, MapError("scan journal row", err)
	}
	if len(bodyHash) != sha256.Size {
		return JournalRow{}, fmt.Errorf("%w: journal body hash has invalid length", flowerr.ErrInvalidState)
	}
	result.Kind = EntryKind(kind)
	copy(result.BodyHash[:], bodyHash)
	result.Body = slices.Clone(result.Body)
	return result, nil
}

func validateJournalBatch(entries []JournalEntry) error {
	seen := make(map[uuid.UUID]struct{}, len(entries))
	for index, entry := range entries {
		if entry.EntryID == uuid.Nil {
			return fmt.Errorf("%w: journal entry %d has nil ID", flowerr.ErrInvalid, index)
		}
		if _, ok := seen[entry.EntryID]; ok {
			return fmt.Errorf("%w: duplicate journal entry ID", flowerr.ErrConflict)
		}
		seen[entry.EntryID] = struct{}{}
		if !validEntryKind(entry.Kind) {
			return fmt.Errorf("%w: invalid journal entry kind", flowerr.ErrInvalid)
		}
		canonicalBody, err := canonical.Canonicalize(entry.Body.Bytes, 0)
		if err != nil || canonicalBody.Digest != entry.Body.Digest || !bytes.Equal(canonicalBody.Bytes, entry.Body.Bytes) {
			return fmt.Errorf("%w: journal body is not canonical or its hash differs", flowerr.ErrInvalid)
		}
		if entry.CausationPosition != nil && entry.CausationBatchIndex != nil {
			return fmt.Errorf("%w: journal entry has two causation forms", flowerr.ErrInvalid)
		}
	}
	return nil
}

func validEntryKind(kind EntryKind) bool {
	switch kind {
	case ExecutionStarted, ExecutionFailing, CommandCreated, AttemptStarted,
		AttemptConcluded, EventRecorded:
		return true
	default:
		return false
	}
}

func resolveCausation(entry JournalEntry, first int64, index int) (*int64, error) {
	if entry.CausationBatchIndex != nil {
		if *entry.CausationBatchIndex < 0 || *entry.CausationBatchIndex >= index {
			return nil, fmt.Errorf("%w: batch causation must name an earlier entry", flowerr.ErrInvalid)
		}
		position := first + int64(*entry.CausationBatchIndex)
		return &position, nil
	}
	if entry.CausationPosition != nil {
		position := first + int64(index)
		if *entry.CausationPosition < 1 || *entry.CausationPosition >= position {
			return nil, fmt.Errorf("%w: causation position must precede its entry", flowerr.ErrInvalid)
		}
	}
	return clonePointer(entry.CausationPosition), nil
}

func cloneJournalRows(rows []JournalRow) []JournalRow {
	clone := make([]JournalRow, len(rows))
	for i, row := range rows {
		clone[i] = row
		clone[i].Body = slices.Clone(row.Body)
	}
	return clone
}

func clonePointer[T any](value *T) *T {
	if value == nil {
		return nil
	}
	copy := *value
	return &copy
}

func joinIdentifiers(columns []string) string {
	var buffer bytes.Buffer
	for i, column := range columns {
		if i > 0 {
			buffer.WriteByte(',')
		}
		buffer.WriteString(pgschema.Quote(column))
	}
	return buffer.String()
}
