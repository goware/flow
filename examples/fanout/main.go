package main

import (
	"context"
	"fmt"
	"io"
	"math/rand/v2"
	"os"
	"sync"
	"time"

	"github.com/goware/flow"
	"github.com/goware/pgkit/v2"
	"github.com/jackc/pgx/v5/pgxpool"
)

type prepareReportArgs struct {
	Parts []int `json:"parts"`
}

type prepareReportResult struct {
	Parts int `json:"parts"`
}

type analyzePartArgs struct {
	Part int `json:"part"`
}

type analyzedPart struct {
	Part  int `json:"part"`
	Score int `json:"score"`
}

type joinAnalysisArgs struct {
	Parts []int `json:"parts"`
}

type joinAnalysisResult struct {
	Parts int `json:"parts"`
}

type enrichPartArgs struct {
	Part  int `json:"part"`
	Score int `json:"score"`
}

type enrichedPart struct {
	Part  int `json:"part"`
	Score int `json:"score"`
}

type joinEnrichmentArgs struct {
	Parts []int `json:"parts"`
}

type joinEnrichmentResult struct {
	Total int `json:"total"`
}

type generateReportArgs struct {
	Total int `json:"total"`
}

type generateReportResult struct {
	Total int `json:"total"`
}

var (
	prepareReport  = flow.DefineCommand[prepareReportArgs, prepareReportResult]("example.prepare_report", 1)
	analyzePart    = flow.DefineCommand[analyzePartArgs, flow.None]("example.analyze_part", 1)
	joinAnalysis   = flow.DefineCommand[joinAnalysisArgs, joinAnalysisResult]("example.join_analysis", 1)
	enrichPart     = flow.DefineCommand[enrichPartArgs, flow.None]("example.enrich_part", 1)
	joinEnrichment = flow.DefineCommand[joinEnrichmentArgs, joinEnrichmentResult]("example.join_enrichment", 1)
	generateReport = flow.DefineCommand[generateReportArgs, generateReportResult]("example.generate_report", 1)
	partAnalyzed   = flow.DefineEvent[analyzedPart]("example.part_analyzed")
	partEnriched   = flow.DefineEvent[enrichedPart]("example.part_enriched")
)

type fanoutExample struct {
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
	db, err := pgkit.ConnectWithPGX("flow-fanout-example", config)
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
	fmt.Printf("execution %s completed with %d commands and %d journal entries\n",
		handle.ID, len(trace.Commands), len(trace.History))
}

func newFlowRuntime(db *pgkit.DB, schema string, output io.Writer) (*flow.Runtime, error) {
	example := &fanoutExample{output: synchronized(output)}
	runtime, err := flow.New(db,
		flow.WithSchema(schema),
		flow.WithWorkerConcurrency(8),
		flow.WithPollInterval(20*time.Millisecond),
	)
	if err != nil {
		return nil, err
	}
	if err := runtime.Register(
		flow.Handle(prepareReport, example.prepareReport),
		flow.Handle(analyzePart, example.analyzePart),
		flow.Handle(joinAnalysis, example.joinAnalysis),
		flow.Handle(enrichPart, example.enrichPart),
		flow.Handle(joinEnrichment, example.joinEnrichment),
		flow.Handle(generateReport, example.generateReport),
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

// runExampleCommand executes two command-owned fan-out/join phases.
func runExampleCommand(ctx context.Context, runtime *flow.Runtime) (flow.ExecutionHandle, flow.ExecutionTrace, error) {
	handle, err := prepareReport.With(runtime).Execute(ctx, "report/example", prepareReportArgs{Parts: []int{0, 1, 2}})
	if err != nil {
		return flow.ExecutionHandle{}, flow.ExecutionTrace{}, err
	}
	trace, err := waitForTerminal(ctx, runtime, handle.ID, 8*time.Second)
	return handle, trace, err
}

// prepareReport discovers the stable part list and atomically stages every
// analysis plus their exact all-of join.
func (example *fanoutExample) prepareReport(_ context.Context, work *flow.Work[prepareReportArgs]) (prepareReportResult, error) {
	fmt.Fprintf(example.output, "preparing %d report analyses\n", len(work.Args.Parts))
	seen := make(map[int]struct{}, len(work.Args.Parts))
	join := flow.Execute(work, "analysis/join", joinAnalysis, joinAnalysisArgs{Parts: work.Args.Parts})
	for _, part := range work.Args.Parts {
		if _, duplicate := seen[part]; duplicate {
			return prepareReportResult{}, flow.Permanent(fmt.Errorf("duplicate report part %d", part))
		}
		seen[part] = struct{}{}
		key := fmt.Sprintf("analysis/%d", part)
		flow.Execute(work, key, analyzePart, analyzePartArgs{Part: part})
		join.WaitFor(partAnalyzed, key)
	}
	return prepareReportResult{Parts: len(work.Args.Parts)}, nil
}

func (example *fanoutExample) analyzePart(ctx context.Context, work *flow.Work[analyzePartArgs]) (flow.None, error) {
	delay := time.Duration(rand.IntN(4)+1) * time.Second // simulating random amount of work
	fmt.Fprintf(example.output, "analyzing part %d for %s\n", work.Args.Part, delay)
	select {
	case <-ctx.Done():
		return flow.None{}, ctx.Err()
	case <-time.After(delay):
	}
	key := fmt.Sprintf("analysis/%d", work.Args.Part)
	return flow.None{}, flow.Emit(work, partAnalyzed, key, analyzedPart{Part: work.Args.Part, Score: work.Args.Part + 1})
}

// joinAnalysis consumes only its declared event inputs, then stages the
// second fan-out and its next all-of join.
func (example *fanoutExample) joinAnalysis(_ context.Context, work *flow.Work[joinAnalysisArgs]) (joinAnalysisResult, error) {
	join := flow.Execute(work, "enrichment/join", joinEnrichment, joinEnrichmentArgs{Parts: work.Args.Parts})
	for _, part := range work.Args.Parts {
		analysisKey := fmt.Sprintf("analysis/%d", part)
		analyzed, err := flow.ReadEvent(work, partAnalyzed, analysisKey)
		if err != nil {
			return joinAnalysisResult{}, err
		}
		enrichmentKey := fmt.Sprintf("enrichment/%d", part)
		flow.Execute(work, enrichmentKey, enrichPart, enrichPartArgs{Part: analyzed.Part, Score: analyzed.Score})
		join.WaitFor(partEnriched, enrichmentKey)
	}
	return joinAnalysisResult{Parts: len(work.Args.Parts)}, nil
}

func (example *fanoutExample) enrichPart(_ context.Context, work *flow.Work[enrichPartArgs]) (flow.None, error) {
	fmt.Fprintf(example.output, "enriching part %d\n", work.Args.Part)
	key := fmt.Sprintf("enrichment/%d", work.Args.Part)
	return flow.None{}, flow.Emit(work, partEnriched, key, enrichedPart{Part: work.Args.Part, Score: work.Args.Score})
}

func (example *fanoutExample) joinEnrichment(_ context.Context, work *flow.Work[joinEnrichmentArgs]) (joinEnrichmentResult, error) {
	total := 0
	for _, part := range work.Args.Parts {
		value, err := flow.ReadEvent(work, partEnriched, fmt.Sprintf("enrichment/%d", part))
		if err != nil {
			return joinEnrichmentResult{}, err
		}
		total += value.Score
	}
	flow.Execute(work, "generate", generateReport, generateReportArgs{Total: total})
	return joinEnrichmentResult{Total: total}, nil
}

func (example *fanoutExample) generateReport(_ context.Context, work *flow.Work[generateReportArgs]) (generateReportResult, error) {
	fmt.Fprintf(example.output, "generated report with score %d\n", work.Args.Total)
	return generateReportResult{Total: work.Args.Total}, nil
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
