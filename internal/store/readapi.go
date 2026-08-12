package store

import (
	"context"
	"crypto/sha256"
	"fmt"
	"time"
	"unicode/utf8"

	"github.com/google/uuid"
	"github.com/goware/flow/internal/flowerr"
	"github.com/goware/flow/internal/pgschema"
	"github.com/jackc/pgx/v5"
)

const (
	// MaxReadKeys bounds every by-keys batch read before duplicate removal.
	MaxReadKeys = 200
	// MaxReadPageLimit is the largest store read, including the public layer's
	// one-row lookahead used to decide whether a next page exists.
	MaxReadPageLimit = 1001
	maxReadKeyBytes  = 1024
)

// LiveWorkCursor is the last immutable ordering tuple returned by a live-work
// list. It is internal store state; the public API binds it to its key filter.
type LiveWorkCursor struct {
	RunKey         string
	DefinitionName string
	RunCreatedAt   time.Time
	RunID          uuid.UUID
	CommandID      uuid.UUID
}

// LiveWorkListFilter selects a bounded keyset page of live work.
type LiveWorkListFilter struct {
	Keys   []string
	Limit  int
	Cursor *LiveWorkCursor
}

// LiveWorkRow is one queued (or leased) command of a non-terminal run,
// carrying the run's identity for key-addressed batch reads.
type LiveWorkRow struct {
	RunID            uuid.UUID
	DefinitionName   string
	RunKey           string
	KeyScope         string
	RunStatus        string
	RunCreatedAt     time.Time
	CommandID        uuid.UUID
	CommandKey       string
	CommandName      string
	Queue            string
	QueueState       string
	NextRunAt        time.Time
	LeaseOwner       *string
	LeaseExpiresAt   *time.Time
	AttemptOrdinal   int
	CommandCreatedAt time.Time
}

// KeyedHistoryCursor is the last immutable ordering tuple returned by a
// keyed-history list.
type KeyedHistoryCursor struct {
	RunKey         string
	DefinitionName string
	RunCreatedAt   time.Time
	RunID          uuid.UUID
	Position       int64
}

// KeyedHistoryListFilter selects a bounded keyset page of retained history.
type KeyedHistoryListFilter struct {
	Keys   []string
	Limit  int
	Cursor *KeyedHistoryCursor
}

// KeyedJournalRow is a journal entry carrying its run's identity, for
// key-addressed reads that span runs.
type KeyedJournalRow struct {
	DefinitionName string
	RunKey         string
	KeyScope       string
	RunCreatedAt   time.Time
	Entry          JournalRow
}

func validateReadKeys(keys []string) error {
	if len(keys) > MaxReadKeys {
		return fmt.Errorf("%w: at most %d keys per batch read", flowerr.ErrInvalid, MaxReadKeys)
	}
	for _, key := range keys {
		if key == "" || len(key) > maxReadKeyBytes || !utf8.ValidString(key) {
			return fmt.Errorf("%w: batch read keys must be non-empty valid UTF-8 and at most %d bytes", flowerr.ErrInvalid, maxReadKeyBytes)
		}
	}
	return nil
}

func validateReadLimit(limit int) error {
	if limit < 1 || limit > MaxReadPageLimit {
		return fmt.Errorf("%w: batch read limit must be between 1 and %d", flowerr.ErrInvalid, MaxReadPageLimit)
	}
	return nil
}

func validateReadCursor(runKey, definitionName string, createdAt time.Time, runID uuid.UUID) error {
	if runKey == "" || len(runKey) > maxReadKeyBytes || !utf8.ValidString(runKey) ||
		definitionName == "" || !utf8.ValidString(definitionName) || createdAt.IsZero() || runID == uuid.Nil {
		return fmt.Errorf("%w: invalid batch read cursor", flowerr.ErrInvalid)
	}
	return nil
}

// ListLiveWorkInTx returns one bounded keyset page of queued commands for
// non-terminal runs, reading through tx when supplied.
func (s *Store) ListLiveWorkInTx(ctx context.Context, tx pgx.Tx, filter LiveWorkListFilter) ([]LiveWorkRow, error) {
	if err := validateReadKeys(filter.Keys); err != nil {
		return nil, err
	}
	if err := validateReadLimit(filter.Limit); err != nil {
		return nil, err
	}
	if filter.Cursor != nil {
		if err := validateReadCursor(filter.Cursor.RunKey, filter.Cursor.DefinitionName, filter.Cursor.RunCreatedAt, filter.Cursor.RunID); err != nil {
			return nil, err
		}
		if filter.Cursor.CommandID == uuid.Nil {
			return nil, fmt.Errorf("%w: invalid live-work cursor command ID", flowerr.ErrInvalid)
		}
	}
	if len(filter.Keys) == 0 {
		return []LiveWorkRow{}, nil
	}

	query, args := s.listLiveWorkQuery(filter)

	rows, err := queryReadRows(ctx, s, tx, query, args...)
	if err != nil {
		return nil, MapError("list live work", err)
	}
	defer rows.Close()
	work := make([]LiveWorkRow, 0, filter.Limit)
	for rows.Next() {
		var row LiveWorkRow
		if err := rows.Scan(
			&row.RunID, &row.DefinitionName, &row.RunKey, &row.KeyScope, &row.RunStatus,
			&row.RunCreatedAt, &row.CommandID, &row.CommandKey, &row.CommandName, &row.Queue,
			&row.QueueState, &row.NextRunAt, &row.LeaseOwner, &row.LeaseExpiresAt,
			&row.AttemptOrdinal, &row.CommandCreatedAt,
		); err != nil {
			return nil, MapError("scan live work row", err)
		}
		work = append(work, row)
	}
	if err := rows.Err(); err != nil {
		return nil, MapError("list live work", err)
	}
	return work, nil
}

func (s *Store) listLiveWorkQuery(filter LiveWorkListFilter) (string, []any) {
	query := `SELECT fe.run_id, fe.definition_name, fe.run_key, fe.key_scope, fe.status, fe.created_at,
			c.command_id, c.command_key, cq.name, cq.queue, cq.state, cq.next_run_at,
			cq.lease_owner, cq.lease_expires_at, c.attempt_ordinal, c.created_at
		FROM ` + pgschema.Table(s.schema, "flow_runs") + ` fe
		JOIN ` + pgschema.Table(s.schema, "flow_command_queue") + ` cq ON cq.run_id = fe.run_id
		JOIN ` + pgschema.Table(s.schema, "flow_commands") + ` c
			ON c.run_id = cq.run_id AND c.command_id = cq.command_id
		WHERE fe.run_key COLLATE "C" = ANY($1::text[])
			AND fe.status IN ('running', 'failing')`
	args := []any{filter.Keys}
	if filter.Cursor != nil {
		query += ` AND (fe.run_key COLLATE "C", fe.definition_name, fe.created_at, fe.run_id, c.command_id)
			> ($2::text COLLATE "C", $3::text, $4::timestamptz, $5::uuid, $6::uuid)`
		args = append(args, filter.Cursor.RunKey, filter.Cursor.DefinitionName,
			filter.Cursor.RunCreatedAt, filter.Cursor.RunID, filter.Cursor.CommandID)
	}
	args = append(args, filter.Limit)
	query += fmt.Sprintf(` ORDER BY fe.run_key COLLATE "C", fe.definition_name, fe.created_at, fe.run_id, c.command_id
		LIMIT $%d`, len(args))
	return query, args
}

// ListJournalByKeysInTx returns one bounded keyset page of retained journal
// entries for runs that ever held one of the keys, reading through tx
// when supplied.
func (s *Store) ListJournalByKeysInTx(ctx context.Context, tx pgx.Tx, filter KeyedHistoryListFilter) ([]KeyedJournalRow, error) {
	if err := validateReadKeys(filter.Keys); err != nil {
		return nil, err
	}
	if err := validateReadLimit(filter.Limit); err != nil {
		return nil, err
	}
	if filter.Cursor != nil {
		if err := validateReadCursor(filter.Cursor.RunKey, filter.Cursor.DefinitionName, filter.Cursor.RunCreatedAt, filter.Cursor.RunID); err != nil {
			return nil, err
		}
		if filter.Cursor.Position < 1 {
			return nil, fmt.Errorf("%w: invalid keyed-history cursor position", flowerr.ErrInvalid)
		}
	}
	if len(filter.Keys) == 0 {
		return []KeyedJournalRow{}, nil
	}

	query, args := s.listJournalByKeysQuery(filter)

	rows, err := queryReadRows(ctx, s, tx, query, args...)
	if err != nil {
		return nil, MapError("list journal by keys", err)
	}
	defer rows.Close()
	entries := make([]KeyedJournalRow, 0, filter.Limit)
	for rows.Next() {
		var row KeyedJournalRow
		var kind string
		var bodyHash []byte
		if err := rows.Scan(
			&row.DefinitionName, &row.RunKey, &row.KeyScope, &row.RunCreatedAt,
			&row.Entry.RunID, &row.Entry.Position, &row.Entry.EntryID, &kind, &row.Entry.RecordedAt,
			&row.Entry.CausationPosition, &row.Entry.CommandID, &row.Entry.AttemptID,
			&row.Entry.EventID, &row.Entry.EventNamespace, &row.Entry.EventName, &row.Entry.EventKey,
			&row.Entry.EventClass, &row.Entry.TerminalStatus, &row.Entry.Body, &bodyHash,
		); err != nil {
			return nil, MapError("scan keyed journal row", err)
		}
		if len(bodyHash) != sha256.Size {
			return nil, fmt.Errorf("%w: journal body hash has invalid length", flowerr.ErrInvalidState)
		}
		row.Entry.Kind = EntryKind(kind)
		copy(row.Entry.BodyHash[:], bodyHash)
		entries = append(entries, row)
	}
	if err := rows.Err(); err != nil {
		return nil, MapError("list journal by keys", err)
	}
	return entries, nil
}

func (s *Store) listJournalByKeysQuery(filter KeyedHistoryListFilter) (string, []any) {
	query := `SELECT fe.definition_name, fe.run_key, fe.key_scope, fe.created_at,
			j.run_id, j.position, j.entry_id, j.entry_kind, j.recorded_at, j.causation_position,
			j.command_id, j.attempt_id,
			j.event_id, j.event_namespace, j.event_name, j.event_key, j.event_class, j.terminal_status,
			j.body, j.body_hash
		FROM ` + pgschema.Table(s.schema, "flow_runs") + ` fe
		JOIN ` + pgschema.Table(s.schema, "flow_journal") + ` j ON j.run_id = fe.run_id
		WHERE fe.run_key COLLATE "C" = ANY($1::text[])`
	args := []any{filter.Keys}
	if filter.Cursor != nil {
		query += ` AND (fe.run_key COLLATE "C", fe.definition_name, fe.created_at, fe.run_id, j.position)
			> ($2::text COLLATE "C", $3::text, $4::timestamptz, $5::uuid, $6::bigint)`
		args = append(args, filter.Cursor.RunKey, filter.Cursor.DefinitionName,
			filter.Cursor.RunCreatedAt, filter.Cursor.RunID, filter.Cursor.Position)
	}
	args = append(args, filter.Limit)
	query += fmt.Sprintf(` ORDER BY fe.run_key COLLATE "C", fe.definition_name, fe.created_at, fe.run_id, j.position
		LIMIT $%d`, len(args))
	return query, args
}

func queryReadRows(ctx context.Context, s *Store, tx pgx.Tx, query string, args ...any) (pgx.Rows, error) {
	if tx != nil {
		return tx.Query(ctx, query, args...)
	}
	return s.db.Conn.Query(ctx, query, args...)
}
