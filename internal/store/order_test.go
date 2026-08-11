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
	if err := order.BeforeRun(low); err != nil {
		t.Fatalf("BeforeRun(low) error = %v", err)
	}
	if err := order.BeforeRun(low); err != nil {
		t.Fatalf("BeforeRun(equal) error = %v", err)
	}
	if err := order.BeforeRun(high); err != nil {
		t.Fatalf("BeforeRun(high) error = %v", err)
	}
	if err := order.BeforeRun(low); !errors.Is(err, flow.ErrInvalidState) {
		t.Fatalf("reverse BeforeRun() error = %v", err)
	}
	if err := order.BeginApplicationPhase(); err != nil {
		t.Fatalf("BeginApplicationPhase() error = %v", err)
	}
	if err := order.BeforeRun(high); !errors.Is(err, flow.ErrInvalidState) {
		t.Fatalf("Flow after application phase error = %v", err)
	}
	if err := order.BeforeRun(uuid.Nil); !errors.Is(err, flow.ErrInvalid) && !errors.Is(err, flow.ErrInvalidState) {
		t.Fatalf("nil BeforeRun() error = %v", err)
	}
	if err := order.BeginApplicationPhase(); err == nil {
		t.Fatal("duplicate BeginApplicationPhase() succeeded")
	}
}

func TestLockOrderOwnedRunCreatesNoPreExistingOrderEdge(t *testing.T) {
	t.Parallel()
	low := uuid.MustParse("00000000-0000-0000-0000-000000000001")
	high := uuid.MustParse("ffffffff-ffff-ffff-ffff-ffffffffffff")
	var order store.LockOrder
	if err := order.BeforeRun(high); err != nil {
		t.Fatal(err)
	}
	if err := order.RegisterOwnedRun(low); err != nil {
		t.Fatalf("RegisterOwnedRun(reverse ID) error = %v", err)
	}
	if err := order.BeforeRun(low); err != nil {
		t.Fatalf("BeforeRun(owned reverse ID) error = %v", err)
	}
	if err := order.BeginApplicationPhase(); err != nil {
		t.Fatal(err)
	}
	if err := order.RegisterOwnedRun(uuid.New()); !errors.Is(err, flow.ErrInvalidState) {
		t.Fatalf("RegisterOwnedRun(application phase) error = %v, want ErrInvalidState", err)
	}
}
