package store

import (
	"context"
	"crypto/sha256"
	"fmt"
	"time"

	"github.com/google/uuid"
	"github.com/goware/flow/internal/flowerr"
	"github.com/goware/flow/internal/pgschema"
	"github.com/jackc/pgx/v5"
)

// MaxReadKeys bounds every by-keys batch read.
const MaxReadKeys = 200

// LiveWorkRow is one queued (or leased) command of a non-terminal execution,
// carrying the execution's identity for key-addressed batch reads.
type LiveWorkRow struct {
	ExecutionID      uuid.UUID
	DefinitionName   string
	ExecutionKey     string
	KeyScope         string
	ExecutionStatus  string
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

// KeyedJournalRow is a journal entry carrying its execution's identity, for
// key-addressed batch reads that span executions.
type KeyedJournalRow struct {
	DefinitionName string
	ExecutionKey   string
	KeyScope       string
	Entry          JournalRow
}

func validateReadKeys(keys []string) error {
	if len(keys) > MaxReadKeys {
		return fmt.Errorf("%w: at most %d keys per batch read", flowerr.ErrInvalid, MaxReadKeys)
	}
	for _, key := range keys {
		if key == "" {
			return fmt.Errorf("%w: batch read keys must be non-empty", flowerr.ErrInvalid)
		}
	}
	return nil
}

// LiveWorkByKeysInTx returns the queued commands of every non-terminal
// execution whose execution key is in keys, reading through tx when supplied.
func (s *Store) LiveWorkByKeysInTx(ctx context.Context, tx pgx.Tx, keys []string) ([]LiveWorkRow, error) {
	if err := validateReadKeys(keys); err != nil {
		return nil, err
	}
	if len(keys) == 0 {
		return nil, nil
	}
	query := `SELECT fe.execution_id, fe.definition_name, fe.execution_key, fe.key_scope, fe.status,
			c.command_id, c.command_key, cq.name, cq.queue, cq.state, cq.next_run_at,
			cq.lease_owner, cq.lease_expires_at, c.attempt_ordinal, c.created_at
		FROM ` + pgschema.Table(s.schema, "flow_executions") + ` fe
		JOIN ` + pgschema.Table(s.schema, "flow_command_queue") + ` cq ON cq.execution_id = fe.execution_id
		JOIN ` + pgschema.Table(s.schema, "flow_commands") + ` c ON c.command_id = cq.command_id
		WHERE fe.execution_key = ANY($1) AND fe.status IN ('running', 'failing')
		ORDER BY fe.execution_key, cq.next_run_at, c.command_id`
	var rows pgx.Rows
	var err error
	if tx != nil {
		rows, err = tx.Query(ctx, query, keys)
	} else {
		rows, err = s.db.Conn.Query(ctx, query, keys)
	}
	if err != nil {
		return nil, MapError("read live work", err)
	}
	defer rows.Close()
	work := make([]LiveWorkRow, 0, len(keys))
	for rows.Next() {
		var row LiveWorkRow
		if err := rows.Scan(
			&row.ExecutionID, &row.DefinitionName, &row.ExecutionKey, &row.KeyScope, &row.ExecutionStatus,
			&row.CommandID, &row.CommandKey, &row.CommandName, &row.Queue, &row.QueueState, &row.NextRunAt,
			&row.LeaseOwner, &row.LeaseExpiresAt, &row.AttemptOrdinal, &row.CommandCreatedAt,
		); err != nil {
			return nil, MapError("scan live work row", err)
		}
		work = append(work, row)
	}
	if err := rows.Err(); err != nil {
		return nil, MapError("read live work", err)
	}
	return work, nil
}

// JournalByKeysInTx returns the journal of every execution that ever held one
// of the keys, in write order (recorded_at, then execution, then position),
// reading through tx when supplied.
func (s *Store) JournalByKeysInTx(ctx context.Context, tx pgx.Tx, keys []string) ([]KeyedJournalRow, error) {
	if err := validateReadKeys(keys); err != nil {
		return nil, err
	}
	if len(keys) == 0 {
		return nil, nil
	}
	query := `SELECT fe.definition_name, fe.execution_key, fe.key_scope,
			j.execution_id, j.position, j.entry_id, j.entry_kind, j.recorded_at, j.causation_position,
			j.command_id, j.attempt_id,
			j.event_id, j.event_namespace, j.event_name, j.event_key, j.event_class, j.terminal_status,
			j.body, j.body_hash
		FROM ` + pgschema.Table(s.schema, "flow_journal") + ` j
		JOIN ` + pgschema.Table(s.schema, "flow_executions") + ` fe ON fe.execution_id = j.execution_id
		WHERE fe.execution_key = ANY($1)
		ORDER BY j.recorded_at, j.execution_id, j.position`
	var rows pgx.Rows
	var err error
	if tx != nil {
		rows, err = tx.Query(ctx, query, keys)
	} else {
		rows, err = s.db.Conn.Query(ctx, query, keys)
	}
	if err != nil {
		return nil, MapError("read journal by keys", err)
	}
	defer rows.Close()
	entries := make([]KeyedJournalRow, 0, 64)
	for rows.Next() {
		var row KeyedJournalRow
		var kind string
		var bodyHash []byte
		if err := rows.Scan(
			&row.DefinitionName, &row.ExecutionKey, &row.KeyScope,
			&row.Entry.ExecutionID, &row.Entry.Position, &row.Entry.EntryID, &kind, &row.Entry.RecordedAt,
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
		return nil, MapError("read journal by keys", err)
	}
	return entries, nil
}
