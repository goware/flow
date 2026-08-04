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

type receiptArgs struct {
	OrderID string `json:"order_id"`
	Email   string `json:"email"`
}

type receiptSent struct {
	ProviderMessageID string `json:"provider_message_id"`
}

var sendReceipt = flow.DefineCommand[receiptArgs, receiptSent]("example.send_receipt", 1)

type directExample struct {
	output io.Writer
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
	db, err := pgkit.ConnectWithPGX("flow-direct-example", config)
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
	stopFlowRuntime := runFlowRuntime(runtime)
	defer stopFlowRuntime()

	handle, trace, err := runExampleCommand(ctx, runtime)
	if err != nil {
		panic(err)
	}
	fmt.Printf("execution %s completed with %d journal entries\n", handle.ID, len(trace.History))
}

func newFlowRuntime(db *pgkit.DB, schema string, output io.Writer) (*flow.Runtime, error) {
	if output == nil {
		output = io.Discard
	}
	example := &directExample{output: output}
	runtime, err := flow.New(db,
		flow.WithSchema(schema),
		flow.WithWorkerConcurrency(2),
		flow.WithPollInterval(20*time.Millisecond),
	)
	if err != nil {
		return nil, err
	}
	if err := runtime.Register(flow.Handle(sendReceipt, example.sendReceipt)); err != nil {
		return nil, err
	}
	return runtime, nil
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

// runExampleCommand submits one command and waits for its terminal trace.
func runExampleCommand(ctx context.Context, runtime *flow.Runtime) (flow.ExecutionHandle, flow.ExecutionTrace, error) {
	handle, err := sendReceipt.With(runtime).Execute(ctx, "receipt/example-order", receiptArgs{
		OrderID: "example-order",
		Email:   "person@example.com",
	})
	if err != nil {
		return flow.ExecutionHandle{}, flow.ExecutionTrace{}, err
	}
	trace, err := waitForTerminal(ctx, runtime, handle.ID, 5*time.Second)
	return handle, trace, err
}

// sendReceipt is the worker handler
func (example *directExample) sendReceipt(ctx context.Context, work *flow.Work[receiptArgs]) (receiptSent, error) {
	fmt.Fprintf(example.output, "sending receipt for %s to %s (command %s)\n",
		work.Args.OrderID, work.Args.Email, work.Info().CommandID)
	select {
	case <-ctx.Done():
		return receiptSent{}, ctx.Err()
	case <-time.After(25 * time.Millisecond):
	}
	result := receiptSent{ProviderMessageID: "stub-" + work.Args.OrderID}
	fmt.Fprintf(example.output, "receipt sent: %s\n", result.ProviderMessageID)
	return result, nil
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
