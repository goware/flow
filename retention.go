package flow

import (
	"context"
	"time"

	"github.com/goware/flow/internal/store"
)

// PruneResult reports the actual run, command, and journal rows deleted by one
// bounded retention pass. Queue and event-wait rows owned by deleted commands
// are removed by their foreign-key cascades and are not counted separately.
type PruneResult struct {
	Runs           int64
	Commands       int64
	JournalEntries int64
}

// PruneTerminalRuns deletes at most limit terminal run aggregates finished
// before the exclusive cutoff. Only unkeyed and live-keyed runs are eligible;
// permanent non-empty keys are retained to preserve idempotency ownership.
//
// Pruning always uses a Flow-owned transaction and affects only Flow's schema.
// Application rows written by WithCommit are outside its ownership and remain
// the application's responsibility.
func PruneTerminalRuns(ctx context.Context, runtime *Runtime, finishedBefore time.Time, limit int) (PruneResult, error) {
	client, err := resolveClient(runtime)
	if err != nil {
		return PruneResult{}, err
	}
	if finishedBefore.IsZero() {
		return PruneResult{}, newError(ErrInvalid, "prune", "cutoff", "", "finished-before time is required")
	}
	if limit < 1 || limit > store.MaxPruneLimit {
		return PruneResult{}, newError(ErrInvalid, "prune", "limit", "", "limit must be between 1 and 1000")
	}
	counts, err := client.runtime.store.PruneTerminalRuns(ctx, finishedBefore, limit)
	if err != nil {
		return PruneResult{}, err
	}
	return PruneResult{
		Runs: counts.Runs, Commands: counts.Commands, JournalEntries: counts.JournalEntries,
	}, nil
}
