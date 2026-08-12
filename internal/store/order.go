package store

import (
	"fmt"

	"github.com/google/uuid"
	"github.com/goware/flow/internal/flowerr"
)

// LockOrder is transaction-local state used by caller-owned transactions.
// It rejects reverse run locking and any return to Flow after the
// application-write phase begins, before issuing SQL.
type LockOrder struct {
	last             uuid.UUID
	hasLast          bool
	owned            map[uuid.UUID]struct{}
	applicationPhase bool
}

// BeforeFlowOperation rejects a Flow operation after application-owned writes
// have begun without registering a run lock that the operation has not
// resolved yet.
func (o *LockOrder) BeforeFlowOperation() error {
	if o.applicationPhase {
		return fmt.Errorf("%w: Flow operation follows application-write phase", flowerr.ErrInvalidState)
	}
	return nil
}

func (o *LockOrder) BeforeRun(id uuid.UUID) error {
	if id == uuid.Nil {
		return fmt.Errorf("%w: run ID is nil", flowerr.ErrInvalid)
	}
	if err := o.BeforeFlowOperation(); err != nil {
		return err
	}
	if _, ok := o.owned[id]; ok {
		return nil
	}
	if o.hasLast && bytesCompare(id, o.last) < 0 {
		return fmt.Errorf("%w: run locks must be requested in ascending order", flowerr.ErrInvalidState)
	}
	o.last = id
	o.hasLast = true
	return nil
}

// RegisterOwnedRun records a row created by this transaction. No concurrent
// transaction can hold that row, so it creates no ordering edge among locks
// for pre-existing runs.
func (o *LockOrder) RegisterOwnedRun(id uuid.UUID) error {
	if id == uuid.Nil {
		return fmt.Errorf("%w: owned run ID is nil", flowerr.ErrInvalid)
	}
	if err := o.BeforeFlowOperation(); err != nil {
		return err
	}
	if o.owned == nil {
		o.owned = make(map[uuid.UUID]struct{})
	}
	o.owned[id] = struct{}{}
	return nil
}

func (o *LockOrder) BeginApplicationPhase() error {
	if o.applicationPhase {
		return fmt.Errorf("%w: application-write phase already began", flowerr.ErrInvalidState)
	}
	o.applicationPhase = true
	return nil
}

func bytesCompare(a, b uuid.UUID) int {
	for i := range len(a) {
		if a[i] < b[i] {
			return -1
		}
		if a[i] > b[i] {
			return 1
		}
	}
	return 0
}
