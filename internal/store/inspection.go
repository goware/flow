package store

import (
	"context"
	"fmt"
	"strings"
	"time"

	"github.com/google/uuid"
	"github.com/goware/flow/internal/failure"
	"github.com/goware/flow/internal/flowerr"
	"github.com/goware/flow/internal/pgschema"
	"github.com/jackc/pgx/v5"
)

// MaxRunListLimit includes the one-row look-ahead used to construct a
// public page cursor. Public pages remain capped at 200 runs.
const MaxRunListLimit = 201

type RunRow struct {
	ID                uuid.UUID
	DefinitionName    string
	DefinitionVersion int
	Key               string
	RootCommandID     *uuid.UUID
	Status            string
	MaxCommands       int
	CommandCount      int
	OpenCommands      int
	DeadlineAt        *time.Time
	Failure           *failure.Value
	CreatedAt         time.Time
	UpdatedAt         time.Time
	StatusAt          time.Time
	FinishedAt        *time.Time
}

type RunListFilter struct {
	DefinitionName string
	KeyPrefix      string
	Statuses       []string
	CreatedAfter   *time.Time
	CreatedBefore  *time.Time
	CursorCreated  *time.Time
	CursorID       *uuid.UUID
	Limit          int
}

// CommandResultRow is the result projection metadata for one command.
// Result is populated only when State is succeeded.
type CommandResultRow struct {
	Name    string
	Version int
	State   string
	Result  []byte
}

// GetCommandResultInTx reads one command projection without replaying the
// run journal. found=false means the run exists but the command key does not.
func (s *Store) GetCommandResultInTx(
	ctx context.Context,
	tx pgx.Tx,
	runID uuid.UUID,
	key string,
) (CommandResultRow, bool, error) {
	if runID == uuid.Nil {
		return CommandResultRow{}, false, fmt.Errorf("%w: run ID is nil", flowerr.ErrInvalid)
	}
	if key == "" {
		return CommandResultRow{}, false, fmt.Errorf("%w: command key is required", flowerr.ErrInvalid)
	}
	query := `SELECT r.run_id,c.name,c.version,c.state,c.result
		FROM ` + pgschema.Table(s.schema, "flow_runs") + ` r
		LEFT JOIN ` + pgschema.Table(s.schema, "flow_commands") + ` c
			ON c.run_id=r.run_id AND c.command_key=$2
		WHERE r.run_id=$1`
	var scan pgx.Row
	if tx != nil {
		scan = tx.QueryRow(ctx, query, runID, key)
	} else {
		scan = s.db.Conn.QueryRow(ctx, query, runID, key)
	}
	var observedRunID uuid.UUID
	var name, state *string
	var version *int
	var result []byte
	if err := scan.Scan(&observedRunID, &name, &version, &state, &result); err != nil {
		return CommandResultRow{}, false, MapError("get command result", err)
	}
	if observedRunID != runID {
		return CommandResultRow{}, false, fmt.Errorf("%w: command result run identity differs", flowerr.ErrInvalidState)
	}
	if name == nil && version == nil && state == nil && result == nil {
		return CommandResultRow{}, false, nil
	}
	if name == nil || version == nil || state == nil {
		return CommandResultRow{}, false, fmt.Errorf("%w: command result projection is incomplete", flowerr.ErrInvalidState)
	}
	return CommandResultRow{
		Name: *name, Version: *version, State: *state, Result: append([]byte(nil), result...),
	}, true, nil
}

func (s *Store) GetRunInTx(ctx context.Context, tx pgx.Tx, id uuid.UUID) (RunRow, error) {
	if id == uuid.Nil {
		return RunRow{}, fmt.Errorf("%w: run ID is nil", flowerr.ErrInvalid)
	}
	query := s.runSelect() + ` WHERE run_id=$1`
	if tx != nil {
		return scanRun(tx.QueryRow(ctx, query, id))
	}
	return scanRun(s.db.Conn.QueryRow(ctx, query, id))
}

// GetCurrentRun finds the non-terminal run holding a
// live-scoped key for one definition. The live-key partial unique index
// guarantees at most one match.
func (s *Store) GetCurrentRun(ctx context.Context, tx pgx.Tx, definitionName, key string) (RunRow, bool, error) {
	if definitionName == "" || key == "" {
		return RunRow{}, false, fmt.Errorf("%w: definition name and key are required", flowerr.ErrInvalid)
	}
	query := s.runSelect() + ` WHERE definition_name=$1 AND run_key=$2
		AND key_scope='live' AND status IN ('running','failing') LIMIT 1`
	rows, err := s.queryRuns(ctx, tx, query, definitionName, key)
	if err != nil {
		return RunRow{}, false, err
	}
	if len(rows) == 0 {
		return RunRow{}, false, nil
	}
	return rows[0], true, nil
}

func (s *Store) ListRunsInTx(ctx context.Context, tx pgx.Tx, filter RunListFilter) ([]RunRow, error) {
	if filter.Limit <= 0 || filter.Limit > MaxRunListLimit {
		return nil, fmt.Errorf("%w: run list limit is invalid", flowerr.ErrInvalid)
	}
	if (filter.CursorCreated == nil) != (filter.CursorID == nil) {
		return nil, fmt.Errorf("%w: run list cursor is incomplete", flowerr.ErrInvalid)
	}
	query, args := s.listRunsQuery(filter)
	return s.queryRuns(ctx, tx, query, args...)
}

func (s *Store) listRunsQuery(filter RunListFilter) (string, []any) {
	clauses := make([]string, 0, 7)
	args := make([]any, 0, 8)
	add := func(sql string, value any) {
		args = append(args, value)
		clauses = append(clauses, fmt.Sprintf(sql, len(args)))
	}
	if filter.DefinitionName != "" {
		add(`definition_name=$%d`, filter.DefinitionName)
	}
	if filter.KeyPrefix != "" {
		escaped := strings.NewReplacer("!", "!!", "%", "!%", "_", "!_").Replace(filter.KeyPrefix)
		add(`run_key LIKE $%d || '%%' ESCAPE '!'`, escaped)
	}
	if len(filter.Statuses) != 0 {
		add(`status=ANY($%d::text[])`, filter.Statuses)
	}
	if filter.CreatedAfter != nil {
		add(`created_at >= $%d`, *filter.CreatedAfter)
	}
	if filter.CreatedBefore != nil {
		add(`created_at < $%d`, *filter.CreatedBefore)
	}
	if filter.CursorCreated != nil {
		args = append(args, *filter.CursorCreated, *filter.CursorID)
		clauses = append(clauses, fmt.Sprintf(`(created_at,run_id) < ($%d,$%d)`, len(args)-1, len(args)))
	}
	args = append(args, filter.Limit)
	query := s.runSelect()
	if len(clauses) != 0 {
		query += ` WHERE ` + strings.Join(clauses, ` AND `)
	}
	query += fmt.Sprintf(` ORDER BY created_at DESC,run_id DESC LIMIT $%d`, len(args))
	return query, args
}

const runSelectColumns = `SELECT run_id,definition_name,definition_version,run_key,status,
	max_commands,command_count,open_commands,deadline_at,failure,created_at,updated_at,status_at,
	finished_at,root_command_id FROM `

func (s *Store) runSelect() string {
	return runSelectColumns + pgschema.Table(s.schema, "flow_runs")
}

func (s *Store) queryRuns(ctx context.Context, tx pgx.Tx, query string, args ...any) ([]RunRow, error) {
	var rows pgx.Rows
	var err error
	if tx != nil {
		rows, err = tx.Query(ctx, query, args...)
	} else {
		rows, err = s.db.Conn.Query(ctx, query, args...)
	}
	if err != nil {
		return nil, MapError("list runs", err)
	}
	defer rows.Close()
	result := make([]RunRow, 0, min(len(args)+8, MaxRunListLimit))
	for rows.Next() {
		value, err := scanRun(rows)
		if err != nil {
			return nil, err
		}
		result = append(result, value)
	}
	if err := rows.Err(); err != nil {
		return nil, MapError("read run rows", err)
	}
	return result, nil
}

func scanRun(row pgx.Row) (RunRow, error) {
	var value RunRow
	var failureBytes []byte
	if err := row.Scan(
		&value.ID, &value.DefinitionName, &value.DefinitionVersion, &value.Key, &value.Status,
		&value.MaxCommands, &value.CommandCount, &value.OpenCommands, &value.DeadlineAt,
		&failureBytes, &value.CreatedAt, &value.UpdatedAt, &value.StatusAt, &value.FinishedAt,
		&value.RootCommandID,
	); err != nil {
		return RunRow{}, MapError("scan run", err)
	}
	if len(failureBytes) != 0 && string(failureBytes) != "null" {
		decoded, err := failure.Decode(failureBytes)
		if err != nil {
			return RunRow{}, fmt.Errorf("%w: invalid run failure projection", flowerr.ErrInvalidState)
		}
		value.Failure = decoded
	}
	return value, nil
}

// QueueStatsRow is a point-in-time projection of one queue lane's operational
// state, derived from flow_command_queue.
type QueueStatsRow struct {
	Queue          string
	Ready          int64
	Delayed        int64
	Running        int64
	OldestReadyFor time.Duration
}

// QueueStatsInTx counts each requested lane's deliverable, scheduled, and
// leased commands against one statement-stable timestamp.
func (s *Store) QueueStatsInTx(ctx context.Context, tx pgx.Tx, queues []string) ([]QueueStatsRow, error) {
	for _, queue := range queues {
		if queue == "" {
			return nil, fmt.Errorf("%w: queue name is required", flowerr.ErrInvalid)
		}
	}
	query := s.queueStatsQuery()
	var rows pgx.Rows
	var err error
	if tx != nil {
		rows, err = tx.Query(ctx, query, queues)
	} else {
		rows, err = s.db.Conn.Query(ctx, query, queues)
	}
	if err != nil {
		return nil, MapError("query queue statistics", err)
	}
	defer rows.Close()
	result := make([]QueueStatsRow, 0, len(queues))
	for rows.Next() {
		var row QueueStatsRow
		var oldestSeconds float64
		if err := rows.Scan(&row.Queue, &row.Ready, &row.Delayed, &row.Running, &oldestSeconds); err != nil {
			return nil, MapError("scan queue statistics", err)
		}
		row.OldestReadyFor = time.Duration(oldestSeconds * float64(time.Second))
		result = append(result, row)
	}
	if err := rows.Err(); err != nil {
		return nil, MapError("read queue statistics", err)
	}
	return result, nil
}

func (s *Store) queueStatsQuery() string {
	return `WITH observed AS MATERIALIZED (SELECT clock_timestamp() AS now),
	requested(queue) AS (SELECT * FROM unnest($1::text[])),
	stats AS (
		SELECT q.queue,
			COUNT(*) FILTER (WHERE q.state IN ('ready','retry_wait') AND q.next_run_at <= observed.now) AS ready,
			COUNT(*) FILTER (WHERE q.state IN ('ready','retry_wait') AND q.next_run_at > observed.now) AS delayed,
			COUNT(*) FILTER (WHERE q.state = 'running') AS running,
			COALESCE(EXTRACT(EPOCH FROM observed.now - MIN(q.next_run_at)
				FILTER (WHERE q.state IN ('ready','retry_wait') AND q.next_run_at <= observed.now)), 0) AS oldest_ready_seconds
		FROM ` + pgschema.Table(s.schema, "flow_command_queue") + ` q CROSS JOIN observed
		WHERE q.queue=ANY($1::text[])
		GROUP BY q.queue,observed.now
	)
	SELECT requested.queue,COALESCE(stats.ready,0),COALESCE(stats.delayed,0),
		COALESCE(stats.running,0),COALESCE(stats.oldest_ready_seconds,0)
	FROM requested LEFT JOIN stats USING (queue)
	ORDER BY requested.queue`
}
