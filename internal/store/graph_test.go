package store

import (
	"testing"

	"github.com/google/uuid"
)

func TestEvaluateDependencyGroup(t *testing.T) {
	t.Parallel()
	a, b, c := uuid.New(), uuid.New(), uuid.New()
	members := []graphMember{{id: a}, {id: b}, {id: c}}
	two := 2
	tests := []struct {
		name      string
		kind      string
		threshold *int
		states    map[uuid.UUID]string
		want      string
	}{
		{"all success pending", "all_succeeded", nil, map[uuid.UUID]string{a: "succeeded", b: "running", c: "succeeded"}, "unresolved"},
		{"all success satisfied", "all_succeeded", nil, map[uuid.UUID]string{a: "succeeded", b: "succeeded", c: "succeeded"}, "satisfied"},
		{"all success impossible", "all_succeeded", nil, map[uuid.UUID]string{a: "succeeded", b: "failed", c: "running"}, "unsatisfiable"},
		{"all settled pending", "all_settled", nil, map[uuid.UUID]string{a: "succeeded", b: "failed", c: "running"}, "unresolved"},
		{"all settled satisfied", "all_settled", nil, map[uuid.UUID]string{a: "succeeded", b: "failed", c: "skipped"}, "satisfied"},
		{"all failed satisfied", "all_failed", nil, map[uuid.UUID]string{a: "failed", b: "cancelled", c: "expired"}, "satisfied"},
		{"all failed impossible", "all_failed", nil, map[uuid.UUID]string{a: "failed", b: "succeeded", c: "running"}, "unsatisfiable"},
		{"threshold pending", "at_least", &two, map[uuid.UUID]string{a: "succeeded", b: "failed", c: "running"}, "unresolved"},
		{"threshold satisfied", "at_least", &two, map[uuid.UUID]string{a: "succeeded", b: "succeeded", c: "running"}, "satisfied"},
		{"threshold impossible", "at_least", &two, map[uuid.UUID]string{a: "succeeded", b: "failed", c: "cancelled"}, "unsatisfiable"},
	}
	for _, test := range tests {
		t.Run(test.name, func(t *testing.T) {
			group := graphGroup{kind: test.kind, threshold: test.threshold, members: members}
			if got := evaluateDependencyGroup(group, test.states); got != test.want {
				t.Fatalf("evaluateDependencyGroup() = %q, want %q", got, test.want)
			}
		})
	}
}
