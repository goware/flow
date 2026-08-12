package store_test

import (
	"context"
	"errors"
	"testing"
	"time"

	"github.com/goware/flow"
	"github.com/goware/flow/internal/pgschema"
	"github.com/goware/flow/internal/store"
)

func TestStorePruneTerminalRunsValidatesAndDeletesAggregate(t *testing.T) {
	t.Parallel()
	db, schema, repository := setupStore(t)
	ctx := context.Background()
	for _, test := range []struct {
		cutoff time.Time
		limit  int
	}{
		{time.Time{}, 1}, {time.Now(), 0}, {time.Now(), store.MaxPruneLimit + 1},
	} {
		if _, err := repository.PruneTerminalRuns(ctx, test.cutoff, test.limit); !errors.Is(err, flow.ErrInvalid) {
			t.Fatalf("PruneTerminalRuns(%v,%d) error = %v", test.cutoff, test.limit, err)
		}
	}
	id := seedRun(t, db, schema, "")
	finished := time.Now().UTC().Add(-time.Hour)
	if _, err := db.Conn.Exec(ctx, `UPDATE `+pgschema.Table(schema, "flow_commands")+`
		SET state='succeeded',result='{}'::text::bytea,terminal_position=1,finished_at=$2,
			budget_started_at=NULL,next_attempt_at=NULL WHERE run_id=$1`, id, finished); err != nil {
		t.Fatal(err)
	}
	if _, err := db.Conn.Exec(ctx, `UPDATE `+pgschema.Table(schema, "flow_runs")+`
		SET status='succeeded',open_commands=0,finished_at=$2 WHERE run_id=$1`, id, finished); err != nil {
		t.Fatal(err)
	}
	result, err := repository.PruneTerminalRuns(ctx, time.Now().UTC(), 1)
	if err != nil {
		t.Fatal(err)
	}
	if result.Runs != 1 || result.Commands != 1 || result.JournalEntries != 0 {
		t.Fatalf("store prune result = %#v", result)
	}
}
