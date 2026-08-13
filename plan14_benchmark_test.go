package flow

import (
	"bytes"
	"context"
	"fmt"
	"sort"
	"sync"
	"testing"
	"time"

	"github.com/google/uuid"
	"github.com/goware/flow/internal/pgschema"
	"github.com/goware/flow/internal/store"
	"github.com/goware/flow/internal/testpg"
)

var (
	plan14BenchmarkInt        int
	plan14BenchmarkString     string
	plan14BenchmarkClaims     []commandGroupClaim
	plan14BenchmarkCommandIDs []uuid.UUID
)

func BenchmarkPlan14ScheduledCommandLatency(b *testing.B) {
	b.StopTimer()
	database := testpg.Open(b)
	ctx := context.Background()
	if err := Migrate(ctx, database.DB, WithSchema(database.Schema)); err != nil {
		b.Fatal(err)
	}
	command := DefineCommand[None, None]("plan14.benchmark.scheduled", 1)
	runtime, err := New(database.DB, WithSchema(database.Schema), WithNotifications(false),
		WithPollInterval(2*time.Second), WithWorkerConcurrency(1))
	if err != nil {
		b.Fatal(err)
	}
	if err := runtime.Register(Handle(command, func(context.Context, *Work[None]) (None, error) {
		return None{}, nil
	})); err != nil {
		b.Fatal(err)
	}
	stop := startBenchmarkRuntime(b, runtime)
	defer stop()

	b.ReportAllocs()
	var total time.Duration
	for index := range b.N {
		run, err := command.Enqueue(ctx, runtime, fmt.Sprintf("scheduled/%d", index), None{},
			WithStartDelay(150*time.Millisecond), WithoutRunDeadline())
		if err != nil {
			b.Fatal(err)
		}
		started := time.Now()
		b.StartTimer()
		settled, err := AwaitRun(ctx, runtime, run.RunID)
		b.StopTimer()
		if err != nil || settled.Status != RunStatusSucceeded {
			b.Fatalf("AwaitRun() status=%s, err=%v", settled.Status, err)
		}
		total += time.Since(started)
	}
	if b.N > 0 {
		b.ReportMetric(float64(total)/float64(time.Millisecond)/float64(b.N), "latency_ms/op")
	}
}

func BenchmarkPlan14AwaitRunLatency(b *testing.B) {
	for _, test := range []struct {
		name        string
		sameRuntime bool
	}{
		{name: "same_runtime", sameRuntime: true},
		{name: "timer_only", sameRuntime: false},
	} {
		b.Run(test.name, func(b *testing.B) {
			b.StopTimer()
			database := testpg.Open(b)
			ctx := context.Background()
			if err := Migrate(ctx, database.DB, WithSchema(database.Schema)); err != nil {
				b.Fatal(err)
			}
			command := DefineCommand[None, None]("plan14.benchmark.await."+test.name, 1)
			reader, err := New(database.DB, WithSchema(database.Schema), WithNotifications(false),
				WithPollInterval(250*time.Millisecond), WithWorkerConcurrency(1))
			if err != nil {
				b.Fatal(err)
			}
			worker := reader
			if !test.sameRuntime {
				worker, err = New(database.DB, WithSchema(database.Schema), WithNotifications(false),
					WithPollInterval(5*time.Millisecond), WithWorkerConcurrency(1))
				if err != nil {
					b.Fatal(err)
				}
			}
			if err := worker.Register(Handle(command, func(context.Context, *Work[None]) (None, error) {
				time.Sleep(10 * time.Millisecond)
				return None{}, nil
			})); err != nil {
				b.Fatal(err)
			}
			stop := startBenchmarkRuntime(b, worker)
			defer stop()

			b.ReportAllocs()
			var total time.Duration
			for index := range b.N {
				run, err := command.Enqueue(ctx, reader, fmt.Sprintf("await/%s/%d", test.name, index), None{}, WithoutRunDeadline())
				if err != nil {
					b.Fatal(err)
				}
				started := time.Now()
				b.StartTimer()
				settled, err := AwaitRun(ctx, reader, run.RunID)
				b.StopTimer()
				if err != nil || settled.Status != RunStatusSucceeded {
					b.Fatalf("AwaitRun() status=%s, err=%v", settled.Status, err)
				}
				total += time.Since(started)
			}
			if b.N > 0 {
				b.ReportMetric(float64(total)/float64(time.Millisecond)/float64(b.N), "latency_ms/op")
			}
		})
	}
}

func BenchmarkPlan14CommandProbeFutureBacklog(b *testing.B) {
	b.StopTimer()
	database := testpg.Open(b)
	ctx := context.Background()
	if err := Migrate(ctx, database.DB, WithSchema(database.Schema)); err != nil {
		b.Fatal(err)
	}
	runtime, err := New(database.DB, WithSchema(database.Schema), WithNotifications(false))
	if err != nil {
		b.Fatal(err)
	}
	command := DefineCommand[None, None]("plan14.benchmark.probe_backlog", 1)
	run, err := command.Enqueue(ctx, runtime, "due", None{}, WithoutRunDeadline())
	if err != nil {
		b.Fatal(err)
	}
	snapshot, err := GetRun(ctx, runtime, run.RunID)
	if err != nil {
		b.Fatal(err)
	}
	seedPlan14FutureBacklog(b, database, run.RunID, snapshot.RootCommandID, command.Name(), 100_000)
	if _, err := database.DB.Conn.Exec(ctx, `ANALYZE `+
		pgschema.Table(database.Schema, "flow_runs")+`, `+
		pgschema.Table(database.Schema, "flow_command_queue")); err != nil {
		b.Fatal(err)
	}
	kinds := []store.CommandKind{{Name: command.Name(), Version: command.Version()}}

	b.ReportAllocs()
	b.ReportMetric(100_000, "future_rows")
	b.ResetTimer()
	b.StartTimer()
	for range b.N {
		probe, err := runtime.store.ProbeCommandsExcluding(ctx, kinds, 1, nil, nil, nil)
		if err != nil {
			b.Fatal(err)
		}
		if len(probe.Candidates) != 1 || probe.FutureDelay == nil {
			b.Fatalf("large-backlog probe = %#v", probe)
		}
	}
}

func seedPlan14FutureBacklog(
	t testing.TB,
	database testpg.Database,
	runID RunID,
	rootCommandID CommandID,
	commandName string,
	count int,
) {
	t.Helper()
	commands := pgschema.Table(database.Schema, "flow_commands")
	queue := pgschema.Table(database.Schema, "flow_command_queue")
	if _, err := database.DB.Conn.Exec(context.Background(), `WITH inserted AS (
		INSERT INTO `+commands+` (
			command_id,run_id,command_key,name,version,parent_command_id,args,declaration_fingerprint,
			state,queue,retry_policy,created_position,created_at,updated_at,status_at
		)
		SELECT md5($1::text||':plan14-future:'||g::text)::uuid,$1::uuid,
		       'plan14/future/'||g::text,$3,1,$2::uuid,convert_to('{}','UTF8'),
		       decode(repeat('00',32),'hex'),'ready','default',convert_to('{}','UTF8'),1,
		       clock_timestamp(),clock_timestamp(),clock_timestamp()
		FROM generate_series(1,$4::integer) AS g
		RETURNING command_id,run_id,queue,name,version
	)
	INSERT INTO `+queue+` (command_id,run_id,queue,name,version,state,next_run_at)
	SELECT command_id,run_id,queue,name,version,'ready',clock_timestamp()+interval '1 hour'
	FROM inserted`, runID, rootCommandID, commandName, count); err != nil {
		t.Fatalf("seed %d future commands: %v", count, err)
	}
}

func BenchmarkPlan14RetainedAllocationChanges(b *testing.B) {
	b.StopTimer()
	database := testpg.Open(b)
	ctx := context.Background()
	if err := Migrate(ctx, database.DB, WithSchema(database.Schema)); err != nil {
		b.Fatal(err)
	}
	runtime, err := New(database.DB, WithSchema(database.Schema), WithNotifications(false), WithWorkerConcurrency(4))
	if err != nil {
		b.Fatal(err)
	}

	b.Run("pool_capacity/config_each_call", func(b *testing.B) {
		b.ReportAllocs()
		for range b.N {
			plan14BenchmarkInt = claimConcurrencyLimit(runtime.workerConcurrency, int(runtime.db.Conn.Config().MaxConns))
		}
	})
	b.Run("pool_capacity/cached", func(b *testing.B) {
		b.ReportAllocs()
		for range b.N {
			plan14BenchmarkInt = claimConcurrencyLimit(runtime.workerConcurrency, runtime.poolCapacity)
		}
	})

	b.Run("replica_name/construct_each_call", func(b *testing.B) {
		b.ReportAllocs()
		for range b.N {
			plan14BenchmarkString = "runtime-" + runtime.instanceID.String()
		}
	})
	b.Run("replica_name/cached", func(b *testing.B) {
		b.ReportAllocs()
		for range b.N {
			plan14BenchmarkString = runtime.replicaName()
		}
	})

	cancelled, cancel := context.WithCancel(ctx)
	cancel()
	groups := [][]store.CommandCandidate{{{}}}
	b.Run("single_group/worker_dispatch", func(b *testing.B) {
		b.ReportAllocs()
		for range b.N {
			plan14BenchmarkClaims = plan14ClaimOneGroupWithWorker(runtime, cancelled, groups)
		}
	})
	b.Run("single_group/direct", func(b *testing.B) {
		b.ReportAllocs()
		for range b.N {
			plan14BenchmarkClaims = runtime.claimRunGroups(cancelled, groups)
		}
	})

	commandIDs := make([]uuid.UUID, 16)
	for index := range commandIDs {
		commandIDs[index] = uuid.New()
	}
	b.Run("uuid_sort/string", func(b *testing.B) {
		b.ReportAllocs()
		for range b.N {
			commandIDs[0], commandIDs[len(commandIDs)-1] = commandIDs[len(commandIDs)-1], commandIDs[0]
			sort.Slice(commandIDs, func(i, j int) bool { return commandIDs[i].String() < commandIDs[j].String() })
		}
		plan14BenchmarkCommandIDs = commandIDs
	})
	b.Run("uuid_sort/bytes", func(b *testing.B) {
		b.ReportAllocs()
		for range b.N {
			commandIDs[0], commandIDs[len(commandIDs)-1] = commandIDs[len(commandIDs)-1], commandIDs[0]
			sort.Slice(commandIDs, func(i, j int) bool {
				return bytes.Compare(commandIDs[i][:], commandIDs[j][:]) < 0
			})
		}
		plan14BenchmarkCommandIDs = commandIDs
	})

	withAdapter := &Runtime{
		instanceID:   runtime.instanceID,
		replica:      runtime.replica,
		observer:     noOpObserver{},
		observations: newObserverAdapter(noOpObserver{}),
	}
	defer withAdapter.observations.close()
	observationRunID := uuid.New()
	b.Run("no_observer/adapter_enqueue", func(b *testing.B) {
		b.ReportAllocs()
		for range b.N {
			if withAdapter.observations != nil {
				withAdapter.observe(ctx, Observation{
					Kind: ObservationClaim, Operation: "claim", Outcome: "ok",
					RunID: RunID(observationRunID.String()), Worker: withAdapter.replicaName(),
				})
			}
		}
	})
	b.Run("no_observer/nil_guard", func(b *testing.B) {
		b.ReportAllocs()
		for range b.N {
			if runtime.observations != nil {
				runtime.observe(ctx, Observation{
					Kind: ObservationClaim, Operation: "claim", Outcome: "ok",
					RunID: RunID(observationRunID.String()), Worker: runtime.replicaName(),
				})
			}
		}
	})
}

func plan14ClaimOneGroupWithWorker(
	runtime *Runtime,
	ctx context.Context,
	groups [][]store.CommandCandidate,
) []commandGroupClaim {
	results := make([]commandGroupClaim, len(groups))
	jobs := make(chan int)
	var workers sync.WaitGroup
	workers.Add(1)
	go func() {
		defer workers.Done()
		for index := range jobs {
			results[index].candidates = groups[index]
			if ctx.Err() != nil {
				results[index].err = ctx.Err()
				continue
			}
			results[index].result, results[index].err = runtime.claimRunGroup(ctx, groups[index])
		}
	}()
	for index := range groups {
		jobs <- index
	}
	close(jobs)
	workers.Wait()
	return results
}
