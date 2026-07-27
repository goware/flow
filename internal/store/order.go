package store

import (
	"errors"
	"fmt"

	"github.com/google/uuid"
	"github.com/goware/flow/internal/flowerr"
)

// LockOrder is transaction-local state used by caller-owned transactions.
// It rejects reverse execution locking and any return to Flow after the
// application-write phase begins, before issuing SQL.
type LockOrder struct {
	last             uuid.UUID
	hasLast          bool
	applicationPhase bool
}

func (o *LockOrder) BeforeExecution(id uuid.UUID) error {
	if id == uuid.Nil {
		return fmt.Errorf("%w: execution ID is nil", flowerr.ErrInvalid)
	}
	if o.applicationPhase {
		return fmt.Errorf("%w: Flow operation follows application-write phase", flowerr.ErrInvalidState)
	}
	if o.hasLast && bytesCompare(id, o.last) < 0 {
		return fmt.Errorf("%w: execution locks must be requested in ascending order", flowerr.ErrInvalidState)
	}
	o.last = id
	o.hasLast = true
	return nil
}

func (o *LockOrder) BeginApplicationPhase() error {
	if o.applicationPhase {
		return errors.New("application-write phase already began")
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
