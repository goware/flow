package store

import (
	"context"
	"fmt"
	"time"

	"github.com/goware/flow/internal/flowerr"
	"github.com/goware/flow/internal/pgschema"
	"github.com/goware/flow/internal/uuid"
	"github.com/jackc/pgx/v5"
)

const MaxPruneLimit = 1000

// PruneCounts reports actual rows removed from the three explicitly deleted
// aggregate tables. Queue and event-wait rows cascade from command deletion.
type PruneCounts struct {
	Runs           int64
	Commands       int64
	JournalEntries int64
}

// PruneTerminalRuns deletes one bounded batch of terminal, non-permanent run
// aggregates in a store-owned transaction.
func (s *Store) PruneTerminalRuns(ctx context.Context, finishedBefore time.Time, limit int) (PruneCounts, error) {
	if finishedBefore.IsZero() {
		return PruneCounts{}, fmt.Errorf("%w: prune cutoff is required", flowerr.ErrInvalid)
	}
	if limit < 1 || limit > MaxPruneLimit {
		return PruneCounts{}, fmt.Errorf("%w: prune limit must be between 1 and %d", flowerr.ErrInvalid, MaxPruneLimit)
	}
	tx, err := s.db.Conn.BeginTx(ctx, pgx.TxOptions{IsoLevel: pgx.ReadCommitted})
	if err != nil {
		return PruneCounts{}, MapError("begin terminal run prune", err)
	}
	defer tx.Rollback(context.WithoutCancel(ctx))

	rows, err := tx.Query(ctx, s.pruneCandidatesQuery(), finishedBefore, limit)
	if err != nil {
		return PruneCounts{}, MapError("select terminal runs for pruning", err)
	}
	runIDs, err := pgx.CollectRows(rows, pgx.RowTo[uuid.UUID])
	if err != nil {
		return PruneCounts{}, MapError("read terminal runs for pruning", err)
	}
	if len(runIDs) == 0 {
		if err := tx.Commit(ctx); err != nil {
			return PruneCounts{}, MapError("commit empty terminal run prune", err)
		}
		return PruneCounts{}, nil
	}

	journalTag, err := tx.Exec(ctx, `DELETE FROM `+pgschema.Table(s.schema, "flow_journal")+`
		WHERE run_id=ANY($1::uuid[])`, runIDs)
	if err != nil {
		return PruneCounts{}, MapError("delete pruned run journal", err)
	}
	commandTag, err := tx.Exec(ctx, `DELETE FROM `+pgschema.Table(s.schema, "flow_commands")+`
		WHERE run_id=ANY($1::uuid[])`, runIDs)
	if err != nil {
		return PruneCounts{}, MapError("delete pruned run commands", err)
	}
	runTag, err := tx.Exec(ctx, `DELETE FROM `+pgschema.Table(s.schema, "flow_runs")+`
		WHERE run_id=ANY($1::uuid[])`, runIDs)
	if err != nil {
		return PruneCounts{}, MapError("delete pruned runs", err)
	}
	if runTag.RowsAffected() != int64(len(runIDs)) {
		return PruneCounts{}, fmt.Errorf("%w: pruned %d of %d selected runs",
			flowerr.ErrInvalidState, runTag.RowsAffected(), len(runIDs))
	}
	result := PruneCounts{
		Runs: runTag.RowsAffected(), Commands: commandTag.RowsAffected(),
		JournalEntries: journalTag.RowsAffected(),
	}
	if err := tx.Commit(ctx); err != nil {
		return PruneCounts{}, MapError("commit terminal run prune", err)
	}
	return result, nil
}

func (s *Store) pruneCandidatesQuery() string {
	return `SELECT run_id FROM ` + pgschema.Table(s.schema, "flow_runs") + `
		WHERE finished_at IS NOT NULL
			AND (run_key = '' OR key_scope = 'live')
			AND finished_at < $1
		ORDER BY finished_at,run_id
		FOR UPDATE SKIP LOCKED
		LIMIT $2`
}
