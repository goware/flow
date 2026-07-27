package journalcodec

import (
	"errors"
	"testing"
)

type body struct {
	Version int    `json:"v"`
	Name    string `json:"name"`
}

func TestEncodeDecode(t *testing.T) {
	t.Parallel()

	encoded, err := Encode(body{Version: 1, Name: "created"})
	if err != nil {
		t.Fatalf("Encode() error = %v", err)
	}
	if string(encoded.Bytes) != `{"name":"created","v":1}` {
		t.Fatalf("Encode() = %s", encoded.Bytes)
	}
	if version, err := Version(encoded.Bytes); err != nil || version != 1 {
		t.Fatalf("Version() = %d, %v", version, err)
	}
	decoded, err := Decode[body](encoded.Bytes)
	if err != nil || decoded != (body{Version: 1, Name: "created"}) {
		t.Fatalf("Decode() = %#v, %v", decoded, err)
	}
}

func TestVersionValidation(t *testing.T) {
	t.Parallel()

	for _, value := range []any{
		map[string]any{"name": "missing"},
		map[string]any{"v": 0},
		map[string]any{"v": "one"},
		[]any{1, 2},
	} {
		if _, err := Encode(value); !errors.Is(err, ErrVersion) {
			t.Fatalf("Encode(%#v) error = %v", value, err)
		}
	}
	if _, err := Version([]byte(`{"v":`)); err == nil {
		t.Fatal("Version() accepted malformed JSON")
	}
	if _, err := Decode[body]([]byte(`{"v":0}`)); !errors.Is(err, ErrVersion) {
		t.Fatalf("Decode() error = %v", err)
	}
}
