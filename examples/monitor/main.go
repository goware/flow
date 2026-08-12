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

	runtime, alerts, err := newFlowRuntime(db, "public", os.Stdout)
	if err != nil {
		panic(err)
	}
	monitor, err := newExternalMonitor(db, "public", os.Stdout)
	if err != nil {
		panic(err)
	}
	stopFlowRuntime := runFlowRuntime(runtime)
	defer stopFlowRuntime()

	exec, trace, err := runExampleCommand(ctx, runtime, monitor)
	if err != nil {
		panic(err)
	}
	dutyCycle, pages := alerts.summary()
	fmt.Printf("run %s completed with %d journal entries\n", exec.ID, len(trace.History))
	fmt.Printf("observer summary: %d duty-cycle facts, %d pages\n", dutyCycle, pages)
}

func newFlowRuntime(db *pgkit.DB, schema string, output io.Writer) (*flow.Runtime, *alertConsumer, error) {
	if output == nil {
		output = io.Discard
	}
	example := &monitorExample{output: output}
	alerts := &alertConsumer{output: output}
	runtime, err := flow.New(db,
		flow.WithSchema(schema),
		flow.WithWorkerConcurrency(2),
		flow.WithPollInterval(20*time.Millisecond),
		flow.WithObserver(alerts),
	)
	if err != nil {
		return nil, nil, err
	}
	if err := runtime.Register(
		flow.Handle(confirmBridge, example.confirmBridge),
	); err != nil {
		return nil, nil, err
	}
	return runtime, alerts, nil
}

// alertConsumer is the intended embedding shape for observations: count the
// duty-cycle facts, page on the terminal ones, and ignore every tuple this
// switch does not know so a later Flow release can add facts without breaking
// it. Delivery is best-effort and process-local, so the Trace polling in
// waitForTerminal, not this stream, remains the durable truth.
type alertConsumer struct {
	output    io.Writer
	mu        sync.Mutex
	dutyCycle int
	pages     int
}

func (consumer *alertConsumer) Observe(_ context.Context, observation flow.Observation) {
	consumer.mu.Lock()
	defer consumer.mu.Unlock()
	switch {
	case observation.Kind == flow.ObservationRun && observation.Operation == flow.ObservationOpTerminal &&
		observation.Outcome != flow.ObservationOutcomeSucceeded:
		consumer.page(observation, "run ended "+observation.Outcome)
	case observation.Kind == flow.ObservationAttempt &&
		observation.Operation == flow.ObservationOpConcludeExhausted:
		consumer.page(observation, "command exhausted its retry budget")
	case observation.Kind == flow.ObservationAttempt &&
		observation.Operation == flow.ObservationOpConclude &&
		observation.Outcome == flow.ObservationOutcomeFailed:
		consumer.page(observation, "command failed permanently")
	case observation.Kind == flow.ObservationRuntime &&
		observation.Operation == flow.ObservationOpObserver &&
		observation.Outcome == flow.ObservationOutcomeDroppedTerminal:
		// Lifecycle edges were lost. Reconcile from the read APIs.
		consumer.page(observation, "terminal observations were dropped")
	default:
		consumer.dutyCycle++
	}
}

func (consumer *alertConsumer) page(observation flow.Observation, reason string) {
	consumer.pages++
	fmt.Fprintf(consumer.output, "PAGE %s/%s/%s run=%s key=%q definition=%q: %s\n",
		observation.Kind, observation.Operation, observation.Outcome,
		observation.RunID, observation.RunKey, observation.Definition, reason)
}

func (consumer *alertConsumer) summary() (dutyCycle, pages int) {
	consumer.mu.Lock()
	defer consumer.mu.Unlock()
	return consumer.dutyCycle, consumer.pages
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
func runExampleCommand(ctx context.Context, runtime *flow.Runtime, monitor *externalMonitor) (flow.Run, flow.RunTrace, error) {
	exec, err := confirmBridge.Enqueue(ctx, runtime, "bridge/example", flow.None{},
		flow.WaitFor(bridgeDelivered, "delivery/example"),
		flow.Within(2*time.Second),
	)
	if err != nil {
		return flow.Run{}, flow.RunTrace{}, err
	}
	monitorResult := make(chan error, 1)
	go func() { monitorResult <- monitor.observeBridgeDelivery(ctx, exec.ID) }()

	trace, err := waitForTerminal(ctx, runtime, exec.ID, 8*time.Second)
	if err != nil {
		return flow.Run{}, flow.RunTrace{}, err
	}
	if err := <-monitorResult; err != nil {
		return flow.Run{}, flow.RunTrace{}, err
	}
	return exec, trace, nil
}

// confirmBridge is the worker handler.
func (example *monitorExample) confirmBridge(_ context.Context, work *flow.Work[flow.None]) (confirmBridgeResult, error) {
	delivery, found, err := flow.GetEventValue(work, bridgeDelivered, "delivery/example")
	if err != nil || !found {
		if err == nil {
			err = fmt.Errorf("required bridge delivery is absent")
		}
		return confirmBridgeResult{}, err
	}
	fmt.Fprintf(example.output, "bridge delivery %s confirmed\n", delivery.TransactionHash)
	return confirmBridgeResult{Confirmed: true}, nil
}

func (monitor *externalMonitor) observeBridgeDelivery(ctx context.Context, runID flow.RunID) error {
	select {
	case <-ctx.Done():
		return ctx.Err()
	case <-time.After(50 * time.Millisecond):
	}
	fmt.Fprintln(monitor.output, "external monitor observed bridge delivery")
	return bridgeDelivered.Deliver(ctx, monitor.publisher, runID, "delivery/example", bridgeDelivery{
		TransactionHash: "0xexample",
	})
}

func waitForTerminal(ctx context.Context, runtime *flow.Runtime, id flow.RunID, timeout time.Duration) (flow.RunTrace, error) {
	timer := time.NewTimer(timeout)
	defer timer.Stop()
	ticker := time.NewTicker(10 * time.Millisecond)
	defer ticker.Stop()
	for {
		trace, err := flow.Trace(ctx, runtime, id)
		if err != nil {
			return flow.RunTrace{}, err
		}
		switch trace.Run.Status {
		case "succeeded":
			return trace, nil
		case "failed", "cancelled", "expired":
			return flow.RunTrace{}, fmt.Errorf("example run ended %s", trace.Run.Status)
		}
		select {
		case <-ctx.Done():
			return flow.RunTrace{}, ctx.Err()
		case <-timer.C:
			return flow.RunTrace{}, fmt.Errorf("example run timed out")
		case <-ticker.C:
		}
	}
}
