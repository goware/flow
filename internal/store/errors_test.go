package store_test

import (
	"context"
	"errors"
	"strings"
	"testing"

	"github.com/goware/flow"
	"github.com/goware/flow/internal/store"
	"github.com/jackc/pgx/v5"
	"github.com/jackc/pgx/v5/pgconn"
)

func TestSQLErrorMapping(t *testing.T) {
	t.Parallel()

	tests := []struct {
		name string
		err  error
		want error
	}{
		{name: "no rows", err: pgx.ErrNoRows, want: flow.ErrNotFound},
		{name: "closed", err: pgx.ErrTxClosed, want: flow.ErrClosed},
		{name: "idempotency", err: &pgconn.PgError{Code: "23505", ConstraintName: "flow_executions_idempotency_uq", Detail: "secret"}, want: flow.ErrConflict},
		{name: "foreign key", err: &pgconn.PgError{Code: "23503", ConstraintName: "some_fk", Detail: "secret"}, want: flow.ErrNotFound},
		{name: "check", err: &pgconn.PgError{Code: "23514", ConstraintName: "flow_commands_state_ck", Detail: "secret"}, want: flow.ErrInvalid},
		{name: "internal unique", err: &pgconn.PgError{Code: "23505", ConstraintName: "flow_journal_entry_id_uq", Detail: "secret"}, want: flow.ErrInvalidState},
		{name: "serialization", err: &pgconn.PgError{Code: "40001", Detail: "secret"}, want: store.ErrTransient},
		{name: "connection", err: &pgconn.PgError{Code: "08006", Detail: "secret"}, want: store.ErrTransient},
		{name: "unknown", err: errors.New("driver includes secret"), want: store.ErrTransient},
	}
	for _, tt := range tests {
		t.Run(tt.name, func(t *testing.T) {
			got := store.MapError("test", tt.err)
			if !errors.Is(got, tt.want) {
				t.Fatalf("MapError() = %v, want %v", got, tt.want)
			}
			if strings.Contains(got.Error(), "secret") || strings.Contains(got.Error(), "driver includes") {
				t.Fatalf("MapError leaked database detail: %q", got)
			}
		})
	}
	if store.MapError("test", nil) != nil {
		t.Fatal("MapError(nil) is not nil")
	}
	if !errors.Is(store.MapError("test", context.Canceled), context.Canceled) {
		t.Fatal("MapError did not preserve cancellation")
	}
	var nilError *store.DBError
	if nilError.Error() != "<nil>" || nilError.Unwrap() != nil {
		t.Fatal("nil DBError is unsafe")
	}
}
