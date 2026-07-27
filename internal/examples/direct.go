package examples

import (
	"context"
	"fmt"
	"io"
	"time"

	"github.com/goware/flow"
	"github.com/goware/pgkit/v2"
)

type ReceiptArgs struct {
	OrderID string `json:"order_id"`
	Email   string `json:"email"`
}

type ReceiptSent struct {
	ProviderMessageID string `json:"provider_message_id"`
}

var SendReceipt = flow.DefineCommand[ReceiptArgs, ReceiptSent]("example.send_receipt", 1)

type DirectResult struct {
	Handle flow.ExecutionHandle
	Trace  flow.ExecutionTrace
}

// RunDirect runs the executable direct-command example. Stub application work
// prints and sleeps, while every Flow operation uses the real PostgreSQL store.
func RunDirect(ctx context.Context, db *pgkit.DB, schema string, output io.Writer) (DirectResult, error) {
	if output == nil {
		output = io.Discard
	}
	runtime, err := flow.New(db, flow.WithSchema(schema), flow.WithWorkerConcurrency(2),
		flow.WithPollInterval(20*time.Millisecond))
	if err != nil {
		return DirectResult{}, err
	}
	worker := func(ctx context.Context, work *flow.Work[ReceiptArgs]) (ReceiptSent, error) {
		fmt.Fprintf(output, "sending receipt for %s to %s (command %s)\n",
			work.Args.OrderID, work.Args.Email, work.Info().CommandID)
		select {
		case <-ctx.Done():
			return ReceiptSent{}, ctx.Err()
		case <-time.After(25 * time.Millisecond):
		}
		result := ReceiptSent{ProviderMessageID: "stub-" + work.Args.OrderID}
		fmt.Fprintf(output, "receipt sent: %s\n", result.ProviderMessageID)
		return result, nil
	}
	if err := runtime.Register(flow.Handle(SendReceipt, worker)); err != nil {
		return DirectResult{}, err
	}
	runCtx, cancelRun := context.WithCancel(context.Background())
	runResult := make(chan error, 1)
	go func() { runResult <- runtime.Run(runCtx) }()
	defer func() {
		cancelRun()
		select {
		case <-runResult:
		case <-time.After(2 * time.Second):
		}
	}()

	handle, err := SendReceipt.With(runtime).Execute(ctx, "receipt/example-order", ReceiptArgs{
		OrderID: "example-order", Email: "person@example.com",
	})
	if err != nil {
		return DirectResult{}, err
	}
	deadline := time.NewTimer(5 * time.Second)
	defer deadline.Stop()
	ticker := time.NewTicker(10 * time.Millisecond)
	defer ticker.Stop()
	for {
		trace, traceErr := flow.Trace(ctx, runtime, handle.ID)
		if traceErr != nil {
			return DirectResult{}, traceErr
		}
		if trace.Execution.Status == "succeeded" {
			return DirectResult{Handle: handle, Trace: trace}, nil
		}
		if trace.Execution.Status == "failed" || trace.Execution.Status == "cancelled" || trace.Execution.Status == "expired" {
			return DirectResult{}, fmt.Errorf("direct example ended %s", trace.Execution.Status)
		}
		select {
		case <-ctx.Done():
			return DirectResult{}, ctx.Err()
		case <-deadline.C:
			return DirectResult{}, fmt.Errorf("direct example timed out")
		case <-ticker.C:
		}
	}
}
