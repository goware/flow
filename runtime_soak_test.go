package flow

import (
	"context"
	"fmt"
	"math/rand"
	"os"
	"strconv"
	"sync/atomic"
	"testing"
	"time"

	"github.com/google/uuid"
	"github.com/goware/flow/internal/fault"
	"github.com/goware/flow/internal/pgschema"
	"github.com/goware/flow/internal/testpg"
)

type runtimeSoakArgs struct {
	DelayMS int `json:"delay_ms"`
}

func TestRuntimeMultiReplicaInvariantSoak(t *testing.T) {
	if os.Getenv("FLOW_TEST_STRESS") != "1" {
		t.Skip("set FLOW_TEST_STRESS=1 to run the bounded multi-replica soak")
	}
	seed := int64(6)
	if configured := os.Getenv("FLOW_TEST_STRESS_SEED"); configured != "" {
		parsed, err := strconv.ParseInt(configured, 10, 64)
		if err != nil {
			t.Fatalf("invalid FLOW_TEST_STRESS_SEED %q: %v", configured, err)
		}
		seed = parsed
	}
	t.Logf("multi-replica soak seed=%d", seed)
	random := rand.New(rand.NewSource(seed))
	database := testpg.OpenWithMaxConns(t, 12)
	ctx := context.Background()
	if err := Migrate(ctx, database.DB, WithSchema(database.Schema)); err != nil {
		t.Fatal(err)
	}
	command := DefineCommand[runtimeSoakArgs, runtimeSoakArgs]("runtime.multi_replica_soak", 1)
	newReplica := func() *Runtime {
		runtime, err := New(database.DB, WithSchema(database.Schema), WithNotifications(false),
			WithWorkerConcurrency(4), WithPollInterval(5*time.Millisecond),
			withCommandLeaseForTest(120*time.Millisecond), WithShutdownGrace(10*time.Millisecond))
		if err != nil {
			t.Fatal(err)
		}
		if err := runtime.Register(Handle(command, func(ctx context.Context, work *Work[runtimeSoakArgs]) (runtimeSoakArgs, error) {
			timer := time.NewTimer(time.Duration(work.Args.DelayMS) * time.Millisecond)
			defer timer.Stop()
			select {
			case <-ctx.Done():
				return runtimeSoakArgs{}, ctx.Err()
			case <-timer.C:
				return work.Args, nil
			}
		})); err != nil {
			t.Fatal(err)
		}
		return runtime
	}

	const replicas = 4
	runtimes := make([]*Runtime, replicas)
	cancels := make([]context.CancelFunc, replicas)
	results := make([]<-chan error, replicas)
	for index := range replicas {
		runtimes[index] = newReplica()
		cancels[index], results[index] = startRuntime(t, runtimes[index])
	}
	var ambiguousClaims atomic.Int32
	runtimes[2].faults = fault.Func(func(_ context.Context, point fault.Point) error {
		if point == fault.ClaimCommitAmbiguous && ambiguousClaims.Add(1)%11 == 1 {
			return fault.Injected(point)
		}
		return nil
	})
	stopReplica := func(index int) {
		if cancels[index] == nil {
			return
		}
		stopRuntime(t, cancels[index], results[index])
		cancels[index], results[index] = nil, nil
	}
	defer func() {
		for index := range replicas {
			stopReplica(index)
		}
	}()

	const operations = 120
	executions := make(map[ExecutionID]struct{}, operations)
	expectedStatus := make(map[ExecutionID]ExecutionStatus, operations)
	for index := range operations {
		if index == operations/2 {
			stopReplica(0)
			runtimes[0] = newReplica()
			cancels[0], results[0] = startRuntime(t, runtimes[0])
		}
		client := runtimes[random.Intn(replicas)]
		delay := 1 + random.Intn(20)
		if index%17 == 0 {
			delay = 180 // crosses one lease boundary and requires renewal
		}
		if index == 37 {
			delay = 500 // leave time for an explicit cancellation to win
		}
		args := runtimeSoakArgs{DelayMS: delay}
		key := fmt.Sprintf("soak/permanent/%03d", index)
		var options []ExecutionOption
		switch index % 3 {
		case 0:
			key = "" // unkeyed execution
		case 1:
			key = fmt.Sprintf("soak/live/%03d", index)
			options = append(options, WithLiveKey())
		}
		execution, err := command.With(client).Execute(ctx, key, args, options...)
		if err != nil {
			t.Fatalf("execute %d: %v", index, err)
		}
		executions[execution.ID] = struct{}{}
		expectedStatus[execution.ID] = ExecutionStatusSucceeded
		if index == 37 {
			if err := CancelExecution(ctx, runtimes[(index+1)%replicas], execution.ID, "bounded soak cancellation"); err != nil {
				t.Fatalf("cancel %d: %v", index, err)
			}
			expectedStatus[execution.ID] = ExecutionStatusCancelled
		}
		if index%20 == 1 && key != "" {
			duplicateClient := runtimes[(index+1)%replicas]
			duplicate, err := command.With(duplicateClient).Execute(ctx, key, args, options...)
			if err != nil || duplicate.ID != execution.ID {
				t.Fatalf("duplicate start %d = %s, %v; want %s", index, duplicate.ID, err, execution.ID)
			}
		}
	}

	reader := runtimes[1]
	for executionID := range executions {
		waitCtx, cancel := context.WithTimeout(ctx, 10*time.Second)
		execution, err := AwaitExecution(waitCtx, reader, executionID)
		cancel()
		if err != nil || execution.Status != expectedStatus[executionID] || execution.CommandCount != 1 {
			t.Fatalf("await %s = %#v, %v; want status %s (seed=%d)",
				executionID, execution, err, expectedStatus[executionID], seed)
		}
	}
	if ambiguousClaims.Load() == 0 {
		t.Fatal("soak did not exercise ambiguous post-commit claim handling")
	}
	for index := range replicas {
		stopReplica(index)
	}
	assertRuntimeSoakInvariants(t, runtimes[1], len(executions), seed)
}

func assertRuntimeSoakInvariants(t *testing.T, runtime *Runtime, executionCount int, seed int64) {
	t.Helper()
	ctx := context.Background()
	journal := pgschema.Table(runtime.schema, "flow_journal")
	executions := pgschema.Table(runtime.schema, "flow_executions")
	commands := pgschema.Table(runtime.schema, "flow_commands")
	queue := pgschema.Table(runtime.schema, "flow_command_queue")
	checks := []struct {
		name  string
		query string
	}{
		{"journal gaps", `SELECT count(*) FROM (` +
			`SELECT e.execution_id FROM ` + executions + ` e LEFT JOIN ` + journal + ` j USING (execution_id) ` +
			`GROUP BY e.execution_id,e.next_journal_position ` +
			`HAVING count(j.position)<>e.next_journal_position-1 OR min(j.position)<>1 OR max(j.position)<>e.next_journal_position-1) bad`},
		{"duplicate attempt entries", `SELECT count(*) FROM (` +
			`SELECT execution_id,attempt_id,entry_kind FROM ` + journal +
			` WHERE attempt_id IS NOT NULL AND entry_kind IN ('attempt_started','attempt_concluded') ` +
			`GROUP BY execution_id,attempt_id,entry_kind HAVING count(*)>1) bad`},
		{"execution counters", `SELECT count(*) FROM ` + executions + ` e WHERE ` +
			`e.command_count<>(SELECT count(*) FROM ` + commands + ` c WHERE c.execution_id=e.execution_id) OR ` +
			`e.open_commands<>(SELECT count(*) FROM ` + commands + ` c WHERE c.execution_id=e.execution_id ` +
			`AND c.state NOT IN ('succeeded','failed','cancelled','expired'))`},
		{"queue projection", `SELECT count(*) FROM ` + queue + ` q JOIN ` + commands + ` c USING (command_id) ` +
			`WHERE q.execution_id<>c.execution_id OR (q.state='running')<>(c.state='running') ` +
			`OR (q.state IN ('ready','retry_wait'))<>(c.state IN ('ready','retry_wait'))`},
		{"terminal queue rows", `SELECT count(*) FROM ` + queue + ` q JOIN ` + executions + ` e USING (execution_id) ` +
			`WHERE e.status IN ('succeeded','failed','cancelled','expired')`},
	}
	for _, check := range checks {
		var failures int
		if err := runtime.db.Conn.QueryRow(ctx, check.query).Scan(&failures); err != nil {
			t.Fatalf("%s query: %v (seed=%d)", check.name, err, seed)
		}
		if failures != 0 {
			t.Fatalf("%s failures=%d (seed=%d)", check.name, failures, seed)
		}
	}
	var retainedExecutions int
	if err := runtime.db.Conn.QueryRow(ctx, `SELECT count(*) FROM `+executions).Scan(&retainedExecutions); err != nil {
		t.Fatal(err)
	}
	if retainedExecutions != executionCount {
		t.Fatalf("retained executions=%d, want %d (seed=%d)", retainedExecutions, executionCount, seed)
	}
	rows, err := runtime.db.Conn.Query(ctx, `SELECT execution_id FROM `+executions+` ORDER BY execution_id`)
	if err != nil {
		t.Fatal(err)
	}
	defer rows.Close()
	var ids []ExecutionID
	for rows.Next() {
		var id uuid.UUID
		if err := rows.Scan(&id); err != nil {
			t.Fatal(err)
		}
		ids = append(ids, ExecutionID(id.String()))
	}
	if err := rows.Err(); err != nil {
		t.Fatal(err)
	}
	for _, id := range ids {
		assertReplayMatches(t, runtime, id)
	}
}
