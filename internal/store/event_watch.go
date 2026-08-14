package store

import (
	"bytes"
	"context"
	"crypto/sha256"
	"errors"
	"fmt"

	"github.com/goware/flow/internal/flowerr"
	"github.com/goware/flow/internal/pgschema"
	"github.com/goware/flow/internal/uuid"
	"github.com/jackc/pgx/v5"
)

type EventWatchCursor struct {
	Position int64
	Status   string
}

type EventWatchRead struct {
	RunFound bool
	Status   string
	Position int64
	Key      string
	Body     []byte
	Found    bool
}

// OpenEventWatch captures one run's current journal head. Events through this
// position are historical to the newly constructed watch.
func (s *Store) OpenEventWatch(ctx context.Context, runID uuid.UUID) (EventWatchCursor, error) {
	if runID == uuid.Nil {
		return EventWatchCursor{}, fmt.Errorf("%w: run ID is nil", flowerr.ErrInvalid)
	}
	var result EventWatchCursor
	err := s.db.Conn.QueryRow(ctx, `SELECT status,next_journal_position-1
		FROM `+pgschema.Table(s.schema, "flow_runs")+` WHERE run_id=$1`, runID).
		Scan(&result.Status, &result.Position)
	if err != nil {
		return EventWatchCursor{}, MapError("open event watch", err)
	}
	if result.Position < 0 {
		return EventWatchCursor{}, fmt.Errorf("%w: event watch cursor is negative", flowerr.ErrInvalidState)
	}
	return result, nil
}

// ReadEventWatch returns the first matching application event after cursor and
// the run status in one durable read. A missing run is represented explicitly
// because an already-open watch treats retention pruning as terminal.
func (s *Store) ReadEventWatch(
	ctx context.Context,
	runID uuid.UUID,
	after int64,
	eventName string,
) (EventWatchRead, error) {
	if runID == uuid.Nil || after < 0 || eventName == "" {
		return EventWatchRead{}, fmt.Errorf("%w: event watch read is invalid", flowerr.ErrInvalid)
	}
	var result EventWatchRead
	var position *int64
	var key *string
	var body, bodyHash []byte
	err := s.db.Conn.QueryRow(ctx, s.readEventWatchSQL(), runID, after, eventName).
		Scan(&result.Status, &position, &key, &body, &bodyHash)
	if errors.Is(err, pgx.ErrNoRows) {
		return EventWatchRead{}, nil
	}
	if err != nil {
		return EventWatchRead{}, MapError("read event watch", err)
	}
	result.RunFound = true
	if position == nil {
		if key != nil || body != nil || bodyHash != nil {
			return EventWatchRead{}, fmt.Errorf("%w: event watch row is incomplete", flowerr.ErrInvalidState)
		}
		return result, nil
	}
	if *position <= after || key == nil || *key == "" || len(bodyHash) != sha256.Size {
		return EventWatchRead{}, fmt.Errorf("%w: event watch row is invalid", flowerr.ErrInvalidState)
	}
	digest := sha256.Sum256(body)
	if !bytes.Equal(digest[:], bodyHash) {
		return EventWatchRead{}, fmt.Errorf("%w: event watch body hash differs", flowerr.ErrInvalidState)
	}
	result.Position, result.Key, result.Body, result.Found = *position, *key, append([]byte(nil), body...), true
	return result, nil
}

func (s *Store) readEventWatchSQL() string {
	return `SELECT r.status,
			next_event.position,next_event.event_key,next_event.body,next_event.body_hash
		FROM ` + pgschema.Table(s.schema, "flow_runs") + ` AS r
		LEFT JOIN LATERAL (
			SELECT position,event_key,body,body_hash
			FROM ` + pgschema.Table(s.schema, "flow_journal") + `
			WHERE run_id=r.run_id AND position>$2
			  AND entry_kind='event_recorded'
			  AND event_namespace='application' AND event_class='application'
			  AND event_name=$3
			ORDER BY position LIMIT 1
		) AS next_event ON true
		WHERE r.run_id=$1`
}
