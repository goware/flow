package store

import (
	"context"
	"errors"
	"math"
	"strconv"
	"testing"
	"time"

	"github.com/google/uuid"
	"github.com/goware/flow/internal/canonical"
	"github.com/goware/flow/internal/flowerr"
	retrypolicy "github.com/goware/flow/internal/retry"
	"github.com/jackc/pgx/v5"
	"github.com/jackc/pgx/v5/pgconn"
)

// noSQLTx panics on every database operation. Invalid persistence requests
// must be rejected before StartInTx attempts to use the transaction.
type noSQLTx struct{}

func (noSQLTx) Begin(context.Context) (pgx.Tx, error) { panic("unexpected SQL") }
func (noSQLTx) Commit(context.Context) error          { panic("unexpected SQL") }
func (noSQLTx) Rollback(context.Context) error        { panic("unexpected SQL") }
func (noSQLTx) CopyFrom(context.Context, pgx.Identifier, []string, pgx.CopyFromSource) (int64, error) {
	panic("unexpected SQL")
}
func (noSQLTx) SendBatch(context.Context, *pgx.Batch) pgx.BatchResults { panic("unexpected SQL") }
func (noSQLTx) LargeObjects() pgx.LargeObjects                         { panic("unexpected SQL") }
func (noSQLTx) Prepare(context.Context, string, string) (*pgconn.StatementDescription, error) {
	panic("unexpected SQL")
}
func (noSQLTx) Exec(context.Context, string, ...any) (pgconn.CommandTag, error) {
	panic("unexpected SQL")
}
func (noSQLTx) Query(context.Context, string, ...any) (pgx.Rows, error) { panic("unexpected SQL") }
func (noSQLTx) QueryRow(context.Context, string, ...any) pgx.Row        { panic("unexpected SQL") }
func (noSQLTx) Conn() *pgx.Conn                                         { panic("unexpected SQL") }

func TestStartInTxRejectsDurableBoundsBeforeSQL(t *testing.T) {
	t.Parallel()

	value, err := canonical.Marshal(struct{}{}, 0)
	if err != nil {
		t.Fatal(err)
	}
	policy, err := retrypolicy.CanonicalPublic(retrypolicy.DefaultPublic())
	if err != nil {
		t.Fatal(err)
	}
	base := StartRequest{
		ID: uuid.New(), DefinitionName: "store.boundary", DefinitionVersion: 1,
		Key: "key", KeyScope: KeyScopePermanent, Input: value, Metadata: value,
		Deadline: DeadlineSpec{Mode: "none"}, MaxCommands: 1,
		Root: &CommandCreate{
			ID: uuid.New(), Key: "root", Name: "store.boundary", Version: 1,
			Args: value, Required: true, Queue: "default", RetryPolicy: policy,
		},
	}
	repository := &Store{}
	assertRejected := func(name string, mutate func(*StartRequest)) {
		t.Helper()
		t.Run(name, func(t *testing.T) {
			request := base
			root := *base.Root
			request.Root = &root
			mutate(&request)
			if _, err := repository.StartInTx(context.Background(), noSQLTx{}, request, nil); !errors.Is(err, flowerr.ErrInvalid) {
				t.Fatalf("StartInTx() error = %v, want ErrInvalid", err)
			}
		})
	}

	fractional := time.Millisecond + time.Nanosecond
	assertRejected("fractional deadline", func(request *StartRequest) {
		request.Deadline = DeadlineSpec{Mode: "duration", Duration: fractional}
	})
	assertRejected("fractional attempt timeout", func(request *StartRequest) {
		request.Root.AttemptTimeout = fractional
	})
	assertRejected("fractional initial delay", func(request *StartRequest) {
		request.Root.InitialDelay = fractional
	})
	assertRejected("fractional wait timeout", func(request *StartRequest) {
		request.Root.Within = fractional
	})

	if strconv.IntSize > 32 {
		outsideInteger := int64(math.MaxInt32) + 1
		assertRejected("oversized definition version", func(request *StartRequest) {
			request.DefinitionVersion = int(outsideInteger)
		})
		assertRejected("oversized maximum commands", func(request *StartRequest) {
			request.MaxCommands = int(outsideInteger)
		})
		assertRejected("oversized command version", func(request *StartRequest) {
			request.Root.Version = int(outsideInteger)
		})
	}
}
