package store_test

import (
	"errors"
	"testing"

	"github.com/google/uuid"
	"github.com/goware/flow"
	"github.com/goware/flow/internal/store"
)

func TestLockOrder(t *testing.T) {
	t.Parallel()

	low := uuid.MustParse("00000000-0000-0000-0000-000000000001")
	high := uuid.MustParse("00000000-0000-0000-0000-000000000002")
	var order store.LockOrder
	if err := order.BeforeExecution(low); err != nil {
		t.Fatalf("BeforeExecution(low) error = %v", err)
	}
	if err := order.BeforeExecution(low); err != nil {
		t.Fatalf("BeforeExecution(equal) error = %v", err)
	}
	if err := order.BeforeExecution(high); err != nil {
		t.Fatalf("BeforeExecution(high) error = %v", err)
	}
	if err := order.BeforeExecution(low); !errors.Is(err, flow.ErrInvalidState) {
		t.Fatalf("reverse BeforeExecution() error = %v", err)
	}
	if err := order.BeginApplicationPhase(); err != nil {
		t.Fatalf("BeginApplicationPhase() error = %v", err)
	}
	if err := order.BeforeExecution(high); !errors.Is(err, flow.ErrInvalidState) {
		t.Fatalf("Flow after application phase error = %v", err)
	}
	if err := order.BeforeExecution(uuid.Nil); !errors.Is(err, flow.ErrInvalid) && !errors.Is(err, flow.ErrInvalidState) {
		t.Fatalf("nil BeforeExecution() error = %v", err)
	}
	if err := order.BeginApplicationPhase(); err == nil {
		t.Fatal("duplicate BeginApplicationPhase() succeeded")
	}
}
