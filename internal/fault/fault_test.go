package fault

import (
	"context"
	"errors"
	"testing"
)

func TestHooks(t *testing.T) {
	t.Parallel()

	if err := (None{}).Hit(context.Background(), HandlerStart); err != nil {
		t.Fatalf("None.Hit() error = %v", err)
	}
	if err := (Func(nil)).Hit(context.Background(), HandlerStart); err != nil {
		t.Fatalf("nil Func.Hit() error = %v", err)
	}
	want := errors.New("stop")
	var got Point
	hook := Func(func(_ context.Context, point Point) error {
		got = point
		return want
	})
	if err := hook.Hit(context.Background(), SettleBeforeCommit); !errors.Is(err, want) || got != SettleBeforeCommit {
		t.Fatalf("Func.Hit() = %v at %s", err, got)
	}
	if err := Injected(PlanBeforeCommit); !errors.Is(err, ErrInjected) {
		t.Fatalf("Injected() = %v", err)
	}
}
