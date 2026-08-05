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

type thinkArgs struct {
	Turn           int      `json:"turn"`
	UserMessageKey string   `json:"user_message_key,omitempty"`
	ToolKeys       []string `json:"tool_keys,omitempty"`
}

type thinkResult struct {
	FinalResultRef string   `json:"final_result_ref,omitempty"`
	Tools          []string `json:"tools,omitempty"`
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
	agentTool        = flow.DefineCommand[toolArgs, toolResult]("example.agent_tool", 1)
	agentUserMessage = flow.DefineEvent[agentMessage]("example.agent_user_message")
	toolCompleted    = flow.DefineEvent[toolResult]("example.agent_tool_completed")
	agentCompleted   = flow.DefineEvent[agentFinished]("example.agent_completed")
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
		flow.WithPollInterval(10*time.Millisecond),
	)
	if err != nil {
		return nil, err
	}
	if err := runtime.Register(
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

// runExampleCommand starts a bounded command chain. The root is declared
// before an external user message exists, so the exact gate keeps it live.
func runExampleCommand(ctx context.Context, runtime *flow.Runtime) (flow.ExecutionHandle, flow.ExecutionTrace, error) {
	handle, err := agentThink.With(runtime).Execute(ctx, "agent/example", thinkArgs{
		Turn: 1, UserMessageKey: "message/1",
	}, flow.WaitFor(agentUserMessage, "message/1"), flow.Within(2*time.Second))
	if err != nil {
		return flow.ExecutionHandle{}, flow.ExecutionTrace{}, err
	}
	if err = agentUserMessage.Emit(ctx, runtime, handle.ID, "message/1", agentMessage{Text: "focus on durability"}); err != nil {
		return flow.ExecutionHandle{}, flow.ExecutionTrace{}, err
	}
	trace, err := waitForTerminal(ctx, runtime, handle.ID, 8*time.Second)
	return handle, trace, err
}

// agentThink owns the entire agent transition: it reads only declared event
// inputs, stages tool children and the next gated turn, or emits completion.
func (example *agentExample) agentThink(ctx context.Context, work *flow.Work[thinkArgs]) (thinkResult, error) {
	fmt.Fprintf(example.output, "agent thinking on turn %d\n", work.Args.Turn)
	select {
	case <-ctx.Done():
		return thinkResult{}, ctx.Err()
	case <-time.After(15 * time.Millisecond):
	}
	if work.Args.Turn == 1 {
		message, err := flow.ReadEvent(work, agentUserMessage, work.Args.UserMessageKey)
		if err != nil {
			return thinkResult{}, err
		}
		fmt.Fprintf(example.output, "user asked: %s\n", message.Text)
		tools := []string{"search", "archive"}
		toolKeys := make([]string, len(tools))
		for index, name := range tools {
			toolKeys[index] = "turn/1/tool/" + name
		}
		next := flow.Execute(work, "turn/2/think", agentThink, thinkArgs{Turn: 2, ToolKeys: toolKeys})
		for index, name := range tools {
			key := toolKeys[index]
			flow.Execute(work, key, agentTool, toolArgs{Name: name})
			next.WaitFor(toolCompleted, key)
		}
		return thinkResult{Tools: tools}, nil
	}
	for _, key := range work.Args.ToolKeys {
		if _, err := flow.ReadEvent(work, toolCompleted, key); err != nil {
			return thinkResult{}, err
		}
	}
	result := thinkResult{FinalResultRef: "result/final-report"}
	if err := flow.Emit(work, agentCompleted, "final", agentFinished{ResultRef: result.FinalResultRef}); err != nil {
		return thinkResult{}, err
	}
	return result, nil
}

func (example *agentExample) agentTool(ctx context.Context, work *flow.Work[toolArgs]) (toolResult, error) {
	fmt.Fprintf(example.output, "running tool %s\n", work.Args.Name)
	select {
	case <-ctx.Done():
		return toolResult{}, ctx.Err()
	case <-time.After(20 * time.Millisecond):
	}
	result := toolResult{OutputRef: "result/" + work.Args.Name}
	return result, flow.Emit(work, toolCompleted, work.Info().CommandKey, result)
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
