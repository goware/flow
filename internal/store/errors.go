package store

import (
	"context"
	"errors"
	"strings"

	"github.com/goware/flow/internal/flowerr"
	"github.com/jackc/pgx/v5"
	"github.com/jackc/pgx/v5/pgconn"
)

var ErrTransient = errors.New("flow store: transient database error")

type DBError struct {
	Category   error
	Operation  string
	SQLState   string
	Constraint string
}

func (e *DBError) Error() string {
	if e == nil {
		return "<nil>"
	}
	message := "flow store"
	if e.Operation != "" {
		message += " " + e.Operation
	}
	if e.SQLState != "" {
		message += " (SQLSTATE " + e.SQLState + ")"
	}
	if e.Constraint != "" {
		message += " constraint " + e.Constraint
	}
	return message
}

func (e *DBError) Unwrap() error {
	if e == nil {
		return nil
	}
	return e.Category
}

func MapError(operation string, err error) error {
	if err == nil {
		return nil
	}
	if errors.Is(err, context.Canceled) || errors.Is(err, context.DeadlineExceeded) {
		return err
	}
	if errors.Is(err, pgx.ErrNoRows) {
		return &DBError{Category: flowerr.ErrNotFound, Operation: operation}
	}
	if errors.Is(err, pgx.ErrTxClosed) {
		return &DBError{Category: flowerr.ErrClosed, Operation: operation}
	}
	var pgError *pgconn.PgError
	if !errors.As(err, &pgError) {
		return &DBError{Category: ErrTransient, Operation: operation}
	}
	category := categoryForPostgres(pgError)
	return &DBError{
		Category: category, Operation: operation,
		SQLState: pgError.Code, Constraint: safeIdentifier(pgError.ConstraintName),
	}
}

func categoryForPostgres(pgError *pgconn.PgError) error {
	switch pgError.ConstraintName {
	case "flow_runs_idempotency_uq", "flow_commands_run_key_uq",
		"flow_journal_application_event_key_uq":
		return flowerr.ErrConflict
	}
	switch pgError.Code {
	case "23503":
		return flowerr.ErrNotFound
	case "23502", "23514", "22P02", "22001", "22003":
		return flowerr.ErrInvalid
	case "40001", "40P01", "55P03":
		return ErrTransient
	}
	if strings.HasPrefix(pgError.Code, "08") || pgError.Code == "57P01" {
		return ErrTransient
	}
	if pgError.Code == "23505" {
		// Other unique violations protect internal exactly-once invariants.
		return flowerr.ErrInvalidState
	}
	return ErrTransient
}

func safeIdentifier(value string) string {
	for _, r := range value {
		if !(r == '_' || r >= 'a' && r <= 'z' || r >= 'A' && r <= 'Z' || r >= '0' && r <= '9') {
			return ""
		}
	}
	if len(value) > 128 {
		return ""
	}
	return value
}
