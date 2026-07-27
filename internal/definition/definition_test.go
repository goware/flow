package definition

import (
	"errors"
	"reflect"
	"strings"
	"testing"

	"github.com/goware/flow/internal/canonical"
)

func TestCodec(t *testing.T) {
	t.Parallel()

	type payload struct {
		Value string `json:"value"`
	}
	codec := NewCodec[payload]()
	if codec.Type != reflect.TypeFor[payload]() || codec.Name != "definition.payload" {
		t.Fatalf("NewCodec() = %#v", codec)
	}
	encoded, err := codec.Encode(payload{Value: "ok"}, 0)
	if err != nil {
		t.Fatalf("Encode() error = %v", err)
	}
	decoded, err := codec.Decode(encoded.Bytes)
	if err != nil || decoded != (payload{Value: "ok"}) {
		t.Fatalf("Decode() = %#v, %v", decoded, err)
	}
	if _, err := codec.Encode("wrong", 0); err == nil {
		t.Fatal("Encode() accepted a wrong type")
	}
	if _, err := codec.Decode([]byte(`{"value":`)); err == nil {
		t.Fatal("Decode() accepted invalid JSON")
	}
	if !codec.Compatible(NewCodec[payload]()) || codec.Compatible(NewCodec[string]()) {
		t.Fatal("Compatible() returned an invalid result")
	}

	pointerCodec := NewCodec[*payload]()
	null, err := pointerCodec.Encode(nil, 0)
	if err != nil || string(null.Bytes) != "null" {
		t.Fatalf("pointer Encode(nil) = %s, %v", null.Bytes, err)
	}
	interfaceCodec := NewCodec[any]()
	if _, err := interfaceCodec.Encode(payload{Value: "assignable"}, 0); err != nil {
		t.Fatalf("interface Encode() error = %v", err)
	}
	if _, err := codec.Encode(payload{Value: strings.Repeat("x", 20)}, 5); !errors.Is(err, canonical.ErrTooLarge) {
		t.Fatalf("bounded Encode() error = %v", err)
	}
}

func TestBaseAndNameValidation(t *testing.T) {
	t.Parallel()

	base := Base{Kind: CommandKind, Name: "send_txn", Version: 3}
	if err := ValidateBase(base); err != nil {
		t.Fatalf("ValidateBase() error = %v", err)
	}
	if base.Key() != "command:send_txn:3" {
		t.Fatalf("Base.Key() = %q", base.Key())
	}
	tests := []string{"", " leading", "trailing ", "two words", "line\nbreak", strings.Repeat("x", 256)}
	for _, name := range tests {
		if err := ValidateName(name); err == nil {
			t.Fatalf("ValidateName(%q) succeeded", name)
		}
	}
	if err := ValidateBase(Base{Kind: EventKind, Name: "valid", Version: 0}); err == nil {
		t.Fatal("ValidateBase() accepted version zero")
	}
	if got := typeName(nil); got != "<nil>" {
		t.Fatalf("typeName(nil) = %q", got)
	}
}
