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

type reportArgs struct {
	Parts int `json:"parts"`
}

type prepareReportArgs struct {
	Parts int `json:"parts"`
}

type prepareReportResult struct {
	Count int `json:"count"`
}

type analyzePartArgs struct {
	Part int `json:"part"`
}

type analyzePartResult struct {
	Score int `json:"score"`
}

type generateReportArgs struct {
	AnalysisKeys []string `json:"analysis_keys"`
}

type generateReportResult struct {
	Total int `json:"total"`
}

var (
	analyzePart    = flow.DefineCommand[analyzePartArgs, analyzePartResult]("example.analyze_part", 1)
	prepareReport  = flow.DefineCommand[prepareReportArgs, prepareReportResult]("example.prepare_report", 1)
	generateReport = flow.DefineCommand[generateReportArgs, generateReportResult]("example.generate_report", 1)
	reportPlan     = flow.DefinePlan[reportArgs]("example.report", 1, func(plan *flow.Plan, args reportArgs) {
		prepare := flow.Execute(plan, "prepare", prepareReport, prepareReportArgs{Parts: args.Parts})
		analysisKeys, childrenClosed := prepare.Children()
		if !childrenClosed {
			return
		}
		flow.Execute(plan, "generate", generateReport, generateReportArgs{
			AnalysisKeys: analysisKeys,
		}).After(analysisKeys...)
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
	db, err := pgkit.ConnectWithPGX("flow-fanout-example", config)
	if err != nil {
		panic(err)
	}
	defer db.Conn.Close()
	if err := flow.Migrate(ctx, db); err != nil {
		panic(err)
	}

	handle, trace, err := runFanOut(ctx, db, "public", os.Stdout)
	if err != nil {
		panic(err)
	}
	fmt.Printf("execution %s completed with %d commands and %d journal entries\n",
		handle.ID, len(trace.Commands), len(trace.History))
}

// runFanOut executes a dynamic plan. The prepare worker creates independent
// child analyses; once that authoritative child set is closed, the plan joins
// the children and the final worker reads their durable results.
func runFanOut(ctx context.Context, db *pgkit.DB, schema string, output io.Writer) (flow.ExecutionHandle, flow.ExecutionTrace, error) {
	output = synchronized(output)
	runtime, err := flow.New(db,
		flow.WithSchema(schema),
		flow.WithWorkerConcurrency(8),
		flow.WithPollInterval(20*time.Millisecond),
		flow.WithPlanVerification(true),
	)
	if err != nil {
		return flow.ExecutionHandle{}, flow.ExecutionTrace{}, err
	}
	if err := runtime.Register(
		reportPlan,
		flow.Handle(prepareReport, func(_ context.Context, work *flow.Work[prepareReportArgs]) (prepareReportResult, error) {
			fmt.Fprintf(output, "planning %d report analyses\n", work.Args.Parts)
			for part := range work.Args.Parts {
				key := fmt.Sprintf("analysis/%d", part)
				flow.Execute(work, key, analyzePart, analyzePartArgs{Part: part})
			}
			return prepareReportResult{Count: work.Args.Parts}, nil
		}),
		flow.Handle(analyzePart, func(ctx context.Context, work *flow.Work[analyzePartArgs]) (analyzePartResult, error) {
			fmt.Fprintf(output, "analyzing part %d\n", work.Args.Part)
			select {
			case <-ctx.Done():
				return analyzePartResult{}, ctx.Err()
			case <-time.After(20 * time.Millisecond):
			}
			return analyzePartResult{Score: work.Args.Part + 1}, nil
		}),
		flow.Handle(generateReport, func(_ context.Context, work *flow.Work[generateReportArgs]) (generateReportResult, error) {
			total := 0
			for _, key := range work.Args.AnalysisKeys {
				analysis, err := flow.ResultOf(work, key, analyzePart)
				if err != nil {
					return generateReportResult{}, err
				}
				total += analysis.Score
			}
			fmt.Fprintf(output, "generated report with score %d\n", total)
			return generateReportResult{Total: total}, nil
		}),
	); err != nil {
		return flow.ExecutionHandle{}, flow.ExecutionTrace{}, err
	}

	cancelRun, runResult := startRuntime(runtime)
	defer stopRuntime(cancelRun, runResult)

	handle, err := reportPlan.With(runtime).Execute(ctx, "report/example", reportArgs{Parts: 3})
	if err != nil {
		return flow.ExecutionHandle{}, flow.ExecutionTrace{}, err
	}
	trace, err := waitForTerminal(ctx, runtime, handle.ID, 8*time.Second)
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
