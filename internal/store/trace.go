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
	ID                uuid.UUID
	State             string
	UnsatisfiedGroups int
	UnsatisfiedWaits  int
	BudgetStartedAt   *time.Time
	NextAttemptAt     *time.Time
	WaitStartedAt     *time.Time
	WaitDeadlineAt    *time.Time
	AttemptOrdinal    int
	ConsumedAttempts  int
	LastErrorCode     string
	LastErrorMessage  string
	CreatedAt         time.Time
	UpdatedAt         time.Time
	StatusAt          time.Time
	FinishedAt        *time.Time
	DeliveryState     string
	LeaseOwner        string
	LeaseStartedAt    *time.Time
	LeaseExpiresAt    *time.Time
}

type TraceCoordinatorRow struct {
	ID               uuid.UUID
	Status           string
	State            []byte
	StateRevision    int64
	StatePosition    int64
	StartPending     bool
	InboxPosition    int64
	DeliveryKey      string
	DeliveryPosition *int64
	DeliveryState    string
	AttemptOrdinal   int
	ConsumedAttempts int
	LeaseOwner       string
	LeaseStartedAt   *time.Time
	LeaseExpiresAt   *time.Time
	CreatedAt        time.Time
	UpdatedAt        time.Time
	FinishedAt       *time.Time
}

type TraceOperationalRows struct {
	Commands    []TraceCommandRow
	Coordinator *TraceCoordinatorRow
}

func (s *Store) TraceOperationalInTx(ctx context.Context, tx pgx.Tx, id uuid.UUID) (TraceOperationalRows, error) {
	if id == uuid.Nil {
		return TraceOperationalRows{}, fmt.Errorf("%w: execution ID is nil", flowerr.ErrInvalid)
	}
	query := `SELECT c.command_id,c.state,c.unsatisfied_groups,c.unsatisfied_waits,c.budget_started_at,c.next_attempt_at,
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
		if err := rows.Scan(&value.ID, &value.State, &value.UnsatisfiedGroups, &value.UnsatisfiedWaits,
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

	query = `SELECT coordinator_id,status,state,state_revision,state_position,start_pending,inbox_position,
		delivery_key,delivery_position,delivery_state,attempt_ordinal,consumed_attempts,lease_owner,
		lease_started_at,lease_expires_at,created_at,updated_at,finished_at
		FROM ` + pgschema.Table(s.schema, "flow_coordinators") + ` WHERE execution_id=$1`
	var row pgx.Row
	if tx != nil {
		row = tx.QueryRow(ctx, query, id)
	} else {
		row = s.db.Conn.QueryRow(ctx, query, id)
	}
	var coordinator TraceCoordinatorRow
	var deliveryKey, leaseOwner *string
	err = row.Scan(&coordinator.ID, &coordinator.Status, &coordinator.State, &coordinator.StateRevision,
		&coordinator.StatePosition, &coordinator.StartPending, &coordinator.InboxPosition, &deliveryKey,
		&coordinator.DeliveryPosition, &coordinator.DeliveryState, &coordinator.AttemptOrdinal,
		&coordinator.ConsumedAttempts, &leaseOwner, &coordinator.LeaseStartedAt, &coordinator.LeaseExpiresAt,
		&coordinator.CreatedAt, &coordinator.UpdatedAt, &coordinator.FinishedAt)
	if err == nil {
		if deliveryKey != nil {
			coordinator.DeliveryKey = *deliveryKey
		}
		if leaseOwner != nil {
			coordinator.LeaseOwner = *leaseOwner
		}
		coordinator.State = append([]byte(nil), coordinator.State...)
		result.Coordinator = &coordinator
	} else if err != pgx.ErrNoRows {
		return TraceOperationalRows{}, MapError("read trace coordinator", err)
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
