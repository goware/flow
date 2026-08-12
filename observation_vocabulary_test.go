package flow

import (
	"flag"
	"fmt"
	"os"
	"sort"
	"sync"
	"testing"
)

// emittedVocabulary accumulates every tuple this package emits while its tests
// run, so TestMain can hold the whole suite to the registry from both sides.
var emittedVocabulary = struct {
	mu     sync.Mutex
	tuples map[observationTuple]struct{}
}{tuples: make(map[observationTuple]struct{})}

func TestMain(m *testing.M) {
	tap := func(observation Observation) {
		tuple := observationTuple{observation.Kind, observation.Operation, observation.Outcome}
		emittedVocabulary.mu.Lock()
		emittedVocabulary.tuples[tuple] = struct{}{}
		emittedVocabulary.mu.Unlock()
	}
	observationTap.Store(&tap)
	code := m.Run()
	if code == 0 {
		if problems := vocabularyProblems(selectedTestRun()); len(problems) > 0 {
			for _, problem := range problems {
				fmt.Fprintln(os.Stderr, problem)
			}
			code = 1
		}
	}
	os.Exit(code)
}

func selectedTestRun() string {
	if selection := flag.Lookup("test.run"); selection != nil {
		return selection.Value.String()
	}
	return ""
}

// vocabularyProblems reports emitted tuples missing from the registry, and,
// when the whole suite ran, registered tuples that no test ever emitted.
func vocabularyProblems(selection string) []string {
	emittedVocabulary.mu.Lock()
	defer emittedVocabulary.mu.Unlock()
	var problems []string
	for tuple := range emittedVocabulary.tuples {
		if _, registered := observationVocabulary[tuple]; !registered {
			problems = append(problems, fmt.Sprintf("emitted observation %s/%s/%s is not in observationVocabulary",
				tuple.kind, tuple.operation, tuple.outcome))
		}
	}
	if selection == "" {
		for tuple := range observationVocabulary {
			if _, emitted := emittedVocabulary.tuples[tuple]; !emitted {
				problems = append(problems, fmt.Sprintf("registered observation %s/%s/%s is emitted by no test",
					tuple.kind, tuple.operation, tuple.outcome))
			}
		}
	}
	sort.Strings(problems)
	return problems
}
