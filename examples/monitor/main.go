package main

import (
	"context"
	"fmt"
	"io"
	"os"
	"sync"
	"time"

	"github.com/goware/flow"
	"github.com/goware/pgkit/v2"
	"github.com/jackc/pgx/v5/pgxpool"
)

type bridgeDelivery struct {
	TransactionHash string `json:"transaction_hash"`
}

type confirmBridgeResult struct {
	Confirmed bool `json:"confirmed"`
}

const bridgeDeliveredName = "example.bridge_delivered"

var (
	bridgeDelivered = flow.DefineEvent[bridgeDelivery](bridgeDeliveredName)
	confirmBridge   = flow.DefineCommand[flow.None, confirmBridgeResult]("example.confirm_bridge", 1)
	bridgePlan      = flow.DefinePlan[flow.None]("example.bridge", 1, func(plan *flow.Plan, _ flow.None) {
		flow.Execute(plan, "confirm", confirmBridge, flow.None{}).
			WaitFor(bridgeDelivered, "delivery/example").
			Within(2 * time.Second)
	})
)

func main() {
	ctx := context.Background()
	databaseURL := os.Getenv("FLOW_EXAMPLE_DATABASE_URL")
	if databaseURL == "" {
		fmt.Fprintln(os.Stderr, "FLOW_EXAMPLE_DATABASE_URL is required")
		os.Exit(2)
	}
	config, err := pgxpool.ParseConfig(databaseURL)
	if err != nil {
		panic(err)
	}
	db, err := pgkit.ConnectWithPGX("flow-monitor-example", config)
	if err != nil {
		panic(err)
	}
	defer db.Conn.Close()
	if err := flow.Migrate(ctx, db); err != nil {
		panic(err)
	}

	handle, trace, err := runMonitor(ctx, db, "public", os.Stdout)
	if err != nil {
		panic(err)
	}
	fmt.Printf("execution %s completed with %d journal entries\n", handle.ID, len(trace.History))
}

// runMonitor demonstrates a long external wait without a polling worker. The
// command consumes no worker, lease, or connection while a separately deployed
// publisher waits for the external fact and emits the event that releases it.
func runMonitor(ctx context.Context, db *pgkit.DB, schema string, output io.Writer) (flow.ExecutionHandle, flow.ExecutionTrace, error) {
	output = synchronized(output)
	runtime, err := flow.New(db,
		flow.WithSchema(schema),
		flow.WithWorkerConcurrency(2),
		flow.WithPollInterval(20*time.Millisecond),
	)
	if err != nil {
		return flow.ExecutionHandle{}, flow.ExecutionTrace{}, err
	}

	// The monitor has only the lightweight client surface. It deliberately
	// registers neither the plan nor its worker, proving publishers and plan
	// processors can be deployed independently.
	publisher, err := flow.New(db, flow.WithSchema(schema))
	if err != nil {
		return flow.ExecutionHandle{}, flow.ExecutionTrace{}, err
	}
	if err := runtime.Register(
		bridgePlan,
		flow.Handle(confirmBridge, func(_ context.Context, _ *flow.Work[flow.None]) (confirmBridgeResult, error) {
			fmt.Fprintln(output, "bridge delivery confirmed")
			return confirmBridgeResult{Confirmed: true}, nil
		}),
	); err != nil {
		return flow.ExecutionHandle{}, flow.ExecutionTrace{}, err
	}

	cancelRun, runResult := startRuntime(runtime)
	defer stopRuntime(cancelRun, runResult)

	handle, err := bridgePlan.With(runtime).Execute(ctx, "bridge/example", flow.None{})
	if err != nil {
		return flow.ExecutionHandle{}, flow.ExecutionTrace{}, err
	}
	monitorResult := make(chan error, 1)
	go func() {
		select {
		case <-ctx.Done():
			monitorResult <- ctx.Err()
			return
		case <-time.After(50 * time.Millisecond):
		}
		fmt.Fprintln(output, "external monitor observed bridge delivery")
		monitorResult <- bridgeDelivered.Emit(ctx, publisher, handle.ID, "delivery/example", bridgeDelivery{
			TransactionHash: "0xexample",
		})
	}()

	trace, err := waitForTerminal(ctx, runtime, handle.ID, 8*time.Second)
	if err != nil {
		return flow.ExecutionHandle{}, flow.ExecutionTrace{}, err
	}
	if err := <-monitorResult; err != nil {
		return flow.ExecutionHandle{}, flow.ExecutionTrace{}, err
	}
	return handle, trace, nil
}

type synchronizedWriter struct {
	mu     sync.Mutex
	writer io.Writer
}

func (writer *synchronizedWriter) Write(value []byte) (int, error) {
	writer.mu.Lock()
	defer writer.mu.Unlock()
	return writer.writer.Write(value)
}

func synchronized(writer io.Writer) io.Writer {
	if writer == nil {
		writer = io.Discard
	}
	return &synchronizedWriter{writer: writer}
}

func startRuntime(runtime *flow.Runtime) (context.CancelFunc, <-chan error) {
	runContext, cancel := context.WithCancel(context.Background())
	result := make(chan error, 1)
	go func() { result <- runtime.Run(runContext) }()
	return cancel, result
}

func stopRuntime(cancel context.CancelFunc, result <-chan error) {
	cancel()
	select {
	case <-result:
	case <-time.After(2 * time.Second):
	}
}

func waitForTerminal(ctx context.Context, runtime *flow.Runtime, id flow.ExecutionID, timeout time.Duration) (flow.ExecutionTrace, error) {
	timer := time.NewTimer(timeout)
	defer timer.Stop()
	ticker := time.NewTicker(10 * time.Millisecond)
	defer ticker.Stop()
	for {
		trace, err := flow.Trace(ctx, runtime, id)
		if err != nil {
			return flow.ExecutionTrace{}, err
		}
		switch trace.Execution.Status {
		case "succeeded":
			return trace, nil
		case "failed", "cancelled", "expired":
			return flow.ExecutionTrace{}, fmt.Errorf("example execution ended %s", trace.Execution.Status)
		}
		select {
		case <-ctx.Done():
			return flow.ExecutionTrace{}, ctx.Err()
		case <-timer.C:
			return flow.ExecutionTrace{}, fmt.Errorf("example execution timed out")
		case <-ticker.C:
		}
	}
}
