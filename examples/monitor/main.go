package main

import (
	"context"
	"fmt"
	"io"
	"os"
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
)

type monitorExample struct {
	output io.Writer
}

type externalMonitor struct {
	publisher *flow.Runtime
	output    io.Writer
}

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

	runtime, err := newFlowRuntime(db, "public", os.Stdout)
	if err != nil {
		panic(err)
	}
	monitor, err := newExternalMonitor(db, "public", os.Stdout)
	if err != nil {
		panic(err)
	}
	stopFlowRuntime := runFlowRuntime(runtime)
	defer stopFlowRuntime()

	handle, trace, err := runExampleCommand(ctx, runtime, monitor)
	if err != nil {
		panic(err)
	}
	fmt.Printf("execution %s completed with %d journal entries\n", handle.ID, len(trace.History))
}

func newFlowRuntime(db *pgkit.DB, schema string, output io.Writer) (*flow.Runtime, error) {
	if output == nil {
		output = io.Discard
	}
	example := &monitorExample{output: output}
	runtime, err := flow.New(db,
		flow.WithSchema(schema),
		flow.WithWorkerConcurrency(2),
		flow.WithPollInterval(20*time.Millisecond),
	)
	if err != nil {
		return nil, err
	}
	if err := runtime.Register(
		flow.Handle(confirmBridge, example.confirmBridge),
	); err != nil {
		return nil, err
	}
	return runtime, nil
}

// newExternalMonitor creates only the lightweight publisher surface. It
// registers no worker, demonstrating that publishers and command processors
// can be deployed independently.
func newExternalMonitor(db *pgkit.DB, schema string, output io.Writer) (*externalMonitor, error) {
	if output == nil {
		output = io.Discard
	}
	publisher, err := flow.New(db, flow.WithSchema(schema))
	if err != nil {
		return nil, err
	}
	return &externalMonitor{publisher: publisher, output: output}, nil
}

func runFlowRuntime(runtime *flow.Runtime) func() {
	runContext, cancel := context.WithCancel(context.Background())
	result := make(chan error, 1)
	go func() { result <- runtime.Run(runContext) }()
	return func() {
		cancel()
		select {
		case <-result:
		case <-time.After(2 * time.Second):
		}
	}
}

// runExampleCommand creates a direct command gated on an independently
// published event and returns its terminal trace.
func runExampleCommand(ctx context.Context, runtime *flow.Runtime, monitor *externalMonitor) (flow.ExecutionHandle, flow.ExecutionTrace, error) {
	handle, err := confirmBridge.With(runtime).Execute(ctx, "bridge/example", flow.None{},
		flow.WaitFor(bridgeDelivered, "delivery/example"),
		flow.Within(2*time.Second),
	)
	if err != nil {
		return flow.ExecutionHandle{}, flow.ExecutionTrace{}, err
	}
	monitorResult := make(chan error, 1)
	go func() { monitorResult <- monitor.observeBridgeDelivery(ctx, handle.ID) }()

	trace, err := waitForTerminal(ctx, runtime, handle.ID, 8*time.Second)
	if err != nil {
		return flow.ExecutionHandle{}, flow.ExecutionTrace{}, err
	}
	if err := <-monitorResult; err != nil {
		return flow.ExecutionHandle{}, flow.ExecutionTrace{}, err
	}
	return handle, trace, nil
}

// confirmBridge is the worker handler.
func (example *monitorExample) confirmBridge(_ context.Context, _ *flow.Work[flow.None]) (confirmBridgeResult, error) {
	fmt.Fprintln(example.output, "bridge delivery confirmed")
	return confirmBridgeResult{Confirmed: true}, nil
}

func (monitor *externalMonitor) observeBridgeDelivery(ctx context.Context, executionID flow.ExecutionID) error {
	select {
	case <-ctx.Done():
		return ctx.Err()
	case <-time.After(50 * time.Millisecond):
	}
	fmt.Fprintln(monitor.output, "external monitor observed bridge delivery")
	return bridgeDelivered.Emit(ctx, monitor.publisher, executionID, "delivery/example", bridgeDelivery{
		TransactionHash: "0xexample",
	})
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
