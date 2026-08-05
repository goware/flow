package store

import (
	"context"
	"encoding/json"
	"fmt"
	"time"

	"github.com/google/uuid"
	"github.com/goware/flow/internal/flowerr"
	"github.com/goware/flow/internal/pgschema"
	"github.com/jackc/pgx/v5"
)

type TraceCommandRow struct {
	ID               uuid.UUID
	State            string
	UnsatisfiedWaits int
	BudgetStartedAt  *time.Time
	NextAttemptAt    *time.Time
	WaitStartedAt    *time.Time
	WaitDeadlineAt   *time.Time
	AttemptOrdinal   int
	ConsumedAttempts int
	LastErrorCode    string
	LastErrorMessage string
	CreatedAt        time.Time
	UpdatedAt        time.Time
	StatusAt         time.Time
	FinishedAt       *time.Time
	DeliveryState    string
	LeaseOwner       string
	LeaseStartedAt   *time.Time
	LeaseExpiresAt   *time.Time
}

type TraceEventWaitRow struct {
	CommandID         uuid.UUID
	Name              string
	Key               string
	SatisfiedPosition *int64
}

type TraceOperationalRows struct {
	Commands []TraceCommandRow
	Waits    []TraceEventWaitRow
}

func (s *Store) TraceOperationalInTx(ctx context.Context, tx pgx.Tx, id uuid.UUID) (TraceOperationalRows, error) {
	if id == uuid.Nil {
		return TraceOperationalRows{}, fmt.Errorf("%w: execution ID is nil", flowerr.ErrInvalid)
	}
	query := `SELECT c.command_id,c.state,c.unsatisfied_waits,c.budget_started_at,c.next_attempt_at,
		c.wait_started_at,c.wait_deadline_at,c.attempt_ordinal,c.consumed_attempts,c.last_error,
		c.created_at,c.updated_at,c.status_at,c.finished_at,q.state,q.lease_owner,q.lease_started_at,q.lease_expires_at
		FROM ` + pgschema.Table(s.schema, "flow_commands") + ` c
		LEFT JOIN ` + pgschema.Table(s.schema, "flow_command_queue") + ` q USING(command_id)
		WHERE c.execution_id=$1 ORDER BY c.command_key`
	var rows pgx.Rows
	var err error
	if tx != nil {
		rows, err = tx.Query(ctx, query, id)
	} else {
		rows, err = s.db.Conn.Query(ctx, query, id)
	}
	if err != nil {
		return TraceOperationalRows{}, MapError("read trace commands", err)
	}
	result := TraceOperationalRows{}
	for rows.Next() {
		var value TraceCommandRow
		var lastError []byte
		var deliveryState, leaseOwner *string
		if err := rows.Scan(&value.ID, &value.State, &value.UnsatisfiedWaits,
			&value.BudgetStartedAt, &value.NextAttemptAt, &value.WaitStartedAt, &value.WaitDeadlineAt,
			&value.AttemptOrdinal, &value.ConsumedAttempts, &lastError, &value.CreatedAt, &value.UpdatedAt,
			&value.StatusAt, &value.FinishedAt, &deliveryState, &leaseOwner, &value.LeaseStartedAt, &value.LeaseExpiresAt); err != nil {
			rows.Close()
			return TraceOperationalRows{}, MapError("scan trace command", err)
		}
		if deliveryState != nil {
			value.DeliveryState = *deliveryState
		}
		if leaseOwner != nil {
			value.LeaseOwner = *leaseOwner
		}
		if err := decodeTraceFailure(lastError, &value.LastErrorCode, &value.LastErrorMessage); err != nil {
			rows.Close()
			return TraceOperationalRows{}, err
		}
		result.Commands = append(result.Commands, value)
	}
	if err := rows.Err(); err != nil {
		rows.Close()
		return TraceOperationalRows{}, MapError("read trace command rows", err)
	}
	rows.Close()

	query = `SELECT command_id,event_name,event_key,satisfied_position
		FROM ` + pgschema.Table(s.schema, "flow_command_event_waits") + `
		WHERE execution_id=$1 ORDER BY command_id,event_name,event_key`
	if tx != nil {
		rows, err = tx.Query(ctx, query, id)
	} else {
		rows, err = s.db.Conn.Query(ctx, query, id)
	}
	if err != nil {
		return TraceOperationalRows{}, MapError("read trace event waits", err)
	}
	defer rows.Close()
	for rows.Next() {
		var wait TraceEventWaitRow
		if err := rows.Scan(&wait.CommandID, &wait.Name, &wait.Key, &wait.SatisfiedPosition); err != nil {
			return TraceOperationalRows{}, MapError("scan trace event wait", err)
		}
		result.Waits = append(result.Waits, wait)
	}
	if err := rows.Err(); err != nil {
		return TraceOperationalRows{}, MapError("read trace event wait rows", err)
	}
	return result, nil
}

func decodeTraceFailure(value []byte, code, message *string) error {
	if len(value) == 0 || string(value) == "null" {
		return nil
	}
	var failure struct {
		Code    string `json:"code"`
		Message string `json:"message"`
	}
	if err := json.Unmarshal(value, &failure); err != nil {
		return fmt.Errorf("%w: invalid command error projection", flowerr.ErrInvalidState)
	}
	*code, *message = failure.Code, failure.Message
	return nil
}
