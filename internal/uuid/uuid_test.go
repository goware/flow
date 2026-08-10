package uuid_test

import (
	"bytes"
	"testing"

	guuid "github.com/google/uuid"
	"github.com/goware/flow/internal/uuid"
)

func TestNewGeneratesMonotonicV7(t *testing.T) {
	previous := uuid.New()
	for range 1000 {
		next := uuid.New()
		if next.Version() != 7 {
			t.Fatalf("identifier version = %d, want 7", next.Version())
		}
		if next.Variant() != guuid.RFC4122 {
			t.Fatalf("identifier variant = %v, want RFC4122", next.Variant())
		}
		if bytes.Compare(previous[:], next[:]) >= 0 {
			t.Fatalf("identifier %s does not sort after predecessor %s", next, previous)
		}
		previous = next
	}
}
