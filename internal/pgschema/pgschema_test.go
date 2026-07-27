package pgschema

import "testing"

func TestIdentifiers(t *testing.T) {
	t.Parallel()

	for _, valid := range []string{"public", "flow_test_1", "_private"} {
		if err := Validate(valid); err != nil {
			t.Fatalf("Validate(%q) error = %v", valid, err)
		}
	}
	for _, invalid := range []string{"", "bad-name", "two words", "1starts_with_number"} {
		if err := Validate(invalid); err == nil {
			t.Fatalf("Validate(%q) succeeded", invalid)
		}
	}
	if Quote(`a"b`) != `"a""b"` {
		t.Fatalf("Quote() = %q", Quote(`a"b`))
	}
	if Table("public", "flow_journal") != `"public"."flow_journal"` {
		t.Fatalf("Table() = %q", Table("public", "flow_journal"))
	}
}
