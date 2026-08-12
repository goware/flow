package flow

import (
	"context"
	"testing"
)

func mustGetRun(t testing.TB, client Client, id RunID) Run {
	t.Helper()
	run, err := GetRun(context.Background(), client, id)
	if err != nil {
		t.Fatalf("GetRun(%s) error = %v", id, err)
	}
	return run
}
