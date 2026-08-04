package main

import (
	"context"
	"errors"
	"fmt"
	"io"
	"os"
	"sync"
	"time"

	"github.com/goware/flow"
	"github.com/goware/pgkit/v2"
	"github.com/jackc/pgx/v5/pgxpool"
)

type agentState struct {
	Turn         int                           `json:"turn"`
	PendingTools map[string]bool               `json:"pending_tools"`
	Outcomes     map[string]flow.CommandStatus `json:"outcomes"`
	UserMessage  string                        `json:"user_message"`
}

type thinkArgs struct {
	Turn        int    `json:"turn"`
	UserMessage string `json:"user_message"`
}

type thinkResult struct {
	FinalResultRef string   `json:"final_result_ref"`
	Tools          []string `json:"tools"`
}

type toolArgs struct {
	Name string `json:"name"`
}

type toolResult struct {
	OutputRef string `json:"output_ref"`
}

type agentMessage struct {
	Text string `json:"text"`
}

type agentFinished struct {
	ResultRef string `json:"result_ref"`
}

var (
	agentThink       = flow.DefineCommand[thinkArgs, thinkResult]("example.agent_think", 1)
	agentTool        = flow.DefineCommand[toolArgs, toolResult]("example.agent_tool", 1, flow.WithRetry(flow.Attempts(1)))
	agentUserMessage = flow.DefineEvent[agentMessage]("example.agent_user_message")
	agentCompleted   = flow.DefineEvent[agentFinished]("example.agent_completed")
	researchAgent    = flow.DefineCoordinator[agentState]("example.research_agent", 1,
		flow.OnStart(startResearchAgent),
		flow.OnOutcome(agentThink, handleAgentThought),
		flow.OnOutcome(agentTool, handleAgentTool),
		flow.On(agentUserMessage, handleAgentMessage),
	)
)

type agentExample struct {
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
	db, err := pgkit.ConnectWithPGX("flow-agent-example", config)
	if err != nil {
		panic(err)
	}
	defer db.Conn.Close()
	if err = flow.Migrate(ctx, db); err != nil {
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
	fmt.Printf("agent execution %s completed with %d commands and %d journal entries\n",
		handle.ID, len(trace.Commands), len(trace.History))
}

func newFlowRuntime(db *pgkit.DB, schema string, output io.Writer) (*flow.Runtime, error) {
	example := &agentExample{output: synchronized(output)}
	runtime, err := flow.New(db,
		flow.WithSchema(schema),
		flow.WithWorkerConcurrency(4),
		flow.WithCoordinatorConcurrency(2),
		flow.WithPollInterval(10*time.Millisecond),
	)
	if err != nil {
		return nil, err
	}
	if err := runtime.Register(
		researchAgent,
		flow.Handle(agentThink, example.agentThink),
		flow.Handle(agentTool, example.agentTool),
	); err != nil {
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

// runExampleCommand executes a durable two-turn agent and waits for its
// terminal trace.
func runExampleCommand(ctx context.Context, runtime *flow.Runtime) (flow.ExecutionHandle, flow.ExecutionTrace, error) {
	handle, err := researchAgent.With(runtime).Execute(ctx, "agent/example", agentState{})
	if err != nil {
		return flow.ExecutionHandle{}, flow.ExecutionTrace{}, err
	}
	if err = agentUserMessage.Emit(ctx, runtime, handle.ID, "message/1", agentMessage{
		Text: "focus on durability",
	}); err != nil {
		return flow.ExecutionHandle{}, flow.ExecutionTrace{}, err
	}
	trace, err := waitForTerminal(ctx, runtime, handle.ID, 8*time.Second)
	return handle, trace, err
}

func startResearchAgent(_ context.Context, coordination *flow.Coordination[agentState]) error {
	coordination.State.Turn = 1
	coordination.State.PendingTools = map[string]bool{}
	coordination.State.Outcomes = map[string]flow.CommandStatus{}
	flow.Execute(coordination, "turn/1/think", agentThink, thinkArgs{Turn: 1}).Optional()
	return nil
}

func handleAgentMessage(_ context.Context, coordination *flow.Coordination[agentState], received flow.Received[agentMessage]) error {
	coordination.State.UserMessage = received.Payload.Text
	return nil
}

func handleAgentThought(_ context.Context, coordination *flow.Coordination[agentState], received flow.Received[flow.Outcome[thinkResult]]) error {
	if received.Payload.Status != flow.StatusSucceeded {
		coordination.Fail(errors.New("model command failed"))
		return nil
	}
	if received.Payload.Result.FinalResultRef != "" {
		if err := flow.Emit(coordination, agentCompleted, "final", agentFinished{
			ResultRef: received.Payload.Result.FinalResultRef,
		}); err != nil {
			return err
		}
		coordination.Succeed()
		return nil
	}
	if len(received.Payload.Result.Tools) == 0 {
		coordination.Fail(errors.New("model returned no action"))
		return nil
	}
	for _, name := range received.Payload.Result.Tools {
		key := "turn/1/tool/" + name
		flow.Execute(coordination, key, agentTool, toolArgs{Name: name}).Optional()
		coordination.State.PendingTools[key] = true
	}
	return nil
}

func handleAgentTool(_ context.Context, coordination *flow.Coordination[agentState], received flow.Received[flow.Outcome[toolResult]]) error {
	if !coordination.State.PendingTools[received.Key] {
		return flow.Permanent(fmt.Errorf("unexpected tool outcome %q", received.Key))
	}
	delete(coordination.State.PendingTools, received.Key)
	coordination.State.Outcomes[received.Key] = received.Payload.Status
	if len(coordination.State.PendingTools) > 0 {
		return nil
	}
	coordination.State.Turn = 2
	flow.Execute(coordination, "turn/2/think", agentThink, thinkArgs{
		Turn:        2,
		UserMessage: coordination.State.UserMessage,
	}).Optional().Delay(20 * time.Millisecond)
	return nil
}

// agentThink is the model worker handler.
func (example *agentExample) agentThink(ctx context.Context, work *flow.Work[thinkArgs]) (thinkResult, error) {
	fmt.Fprintf(example.output, "agent thinking on turn %d\n", work.Args.Turn)
	select {
	case <-ctx.Done():
		return thinkResult{}, ctx.Err()
	case <-time.After(15 * time.Millisecond):
	}
	if work.Args.Turn == 1 {
		return thinkResult{Tools: []string{"search", "broken"}}, nil
	}
	return thinkResult{FinalResultRef: "result/final-report"}, nil
}

// agentTool is the tool worker handler.
func (example *agentExample) agentTool(ctx context.Context, work *flow.Work[toolArgs]) (toolResult, error) {
	fmt.Fprintf(example.output, "running tool %s\n", work.Args.Name)
	select {
	case <-ctx.Done():
		return toolResult{}, ctx.Err()
	case <-time.After(20 * time.Millisecond):
	}
	if work.Args.Name == "broken" {
		return toolResult{}, flow.Permanent(errors.New("controlled tool failure"))
	}
	return toolResult{OutputRef: "result/" + work.Args.Name}, nil
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
