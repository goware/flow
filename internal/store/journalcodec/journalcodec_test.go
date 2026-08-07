package journalcodec

import (
	"errors"
	"strings"
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

func TestDecodeApplicationEvent(t *testing.T) {
	t.Parallel()

	decoded, err := DecodeApplicationEvent([]byte(`{"payload":{"value":"ok"},"v":1}`))
	if err != nil || decoded.V != ApplicationEventBodyVersion || string(decoded.Payload) != `{"value":"ok"}` {
		t.Fatalf("DecodeApplicationEvent() = %#v, %v", decoded, err)
	}

	tests := []struct {
		name string
		body string
	}{
		{name: "malformed", body: `{"payload":`},
		{name: "trailing", body: `{"payload":{},"v":1}{}`},
		{name: "duplicate payload", body: `{"payload":{},"payload":[],"v":1}`},
		{name: "nested duplicate key", body: `{"payload":{"key":1,"key":2},"v":1}`},
		{name: "noncanonical nested key order", body: `{"payload":{"z":1,"a":2},"v":1}`},
		{name: "noncanonical nested number", body: `{"payload":{"value":1.0},"v":1}`},
		{name: "noncanonical nested whitespace", body: `{"payload":{ "value":1},"v":1}`},
		{name: "missing version", body: `{"payload":{}}`},
		{name: "zero version", body: `{"payload":{},"v":0}`},
		{name: "unknown version", body: `{"payload":{},"v":2}`},
		{name: "missing payload", body: `{"v":1}`},
		{name: "noncanonical envelope", body: `{ "payload": {}, "v": 1 }`},
		{name: "oversized payload", body: `{"payload":"` + strings.Repeat("x", MaxApplicationEventPayloadBytes) + `","v":1}`},
	}
	for _, tt := range tests {
		t.Run(tt.name, func(t *testing.T) {
			t.Parallel()
			if _, err := DecodeApplicationEvent([]byte(tt.body)); err == nil {
				t.Fatal("DecodeApplicationEvent() accepted an invalid body")
			}
		})
	}
}
