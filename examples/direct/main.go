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

type receiptArgs struct {
	OrderID string `json:"order_id"`
	Email   string `json:"email"`
}

type receiptSent struct {
	ProviderMessageID string `json:"provider_message_id"`
}

var sendReceipt = flow.DefineCommand[receiptArgs, receiptSent]("example.send_receipt", 1)

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

	handle, trace, err := runDirect(ctx, db, "public", os.Stdout)
	if err != nil {
		panic(err)
	}
	fmt.Printf("execution %s completed with %d journal entries\n", handle.ID, len(trace.History))
}

// runDirect executes one command using a real PostgreSQL-backed Flow runtime.
// The injected writer lets the end-to-end test observe the same application
// output that a user sees when running this example.
func runDirect(ctx context.Context, db *pgkit.DB, schema string, output io.Writer) (flow.ExecutionHandle, flow.ExecutionTrace, error) {
	output = synchronized(output)
	runtime, err := flow.New(db,
		flow.WithSchema(schema),
		flow.WithWorkerConcurrency(2),
		flow.WithPollInterval(20*time.Millisecond),
	)
	if err != nil {
		return flow.ExecutionHandle{}, flow.ExecutionTrace{}, err
	}

	worker := func(ctx context.Context, work *flow.Work[receiptArgs]) (receiptSent, error) {
		fmt.Fprintf(output, "sending receipt for %s to %s (command %s)\n",
			work.Args.OrderID, work.Args.Email, work.Info().CommandID)
		select {
		case <-ctx.Done():
			return receiptSent{}, ctx.Err()
		case <-time.After(25 * time.Millisecond):
		}
		result := receiptSent{ProviderMessageID: "stub-" + work.Args.OrderID}
		fmt.Fprintf(output, "receipt sent: %s\n", result.ProviderMessageID)
		return result, nil
	}
	if err := runtime.Register(flow.Handle(sendReceipt, worker)); err != nil {
		return flow.ExecutionHandle{}, flow.ExecutionTrace{}, err
	}

	cancelRun, runResult := startRuntime(runtime)
	defer stopRuntime(cancelRun, runResult)

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
