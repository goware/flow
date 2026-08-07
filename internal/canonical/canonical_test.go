package canonical

import (
	"bytes"
	"errors"
	"strings"
	"testing"
)

func TestCanonicalRFC8785Vectors(t *testing.T) {
	t.Parallel()

	raw := []byte(`{
        "numbers": [333333333.33333329, 1E30, 4.50, 2e-3, 0.000000000000000000000000001],
        "string": "€$\u000f\nA'B\"\\\"/",
        "literals": [null, true, false]
    }`)
	want := `{"literals":[null,true,false],"numbers":[333333333.3333333,1e+30,4.5,0.002,1e-27],"string":"€$\u000f\nA'B\"\\\"/"}`

	got, err := Canonicalize(raw, 0)
	if err != nil {
		t.Fatalf("Canonicalize() error = %v", err)
	}
	if string(got.Bytes) != want {
		t.Fatalf("Canonicalize() = %s, want %s", got.Bytes, want)
	}
	if err := ValidateCanonical(got.Bytes, 0); err != nil {
		t.Fatalf("ValidateCanonical() error = %v", err)
	}

	again, err := Canonicalize(got.Bytes, 0)
	if err != nil {
		t.Fatalf("Canonicalize(canonical) error = %v", err)
	}
	if !got.Equal(again) || got.DigestHex() != again.DigestHex() {
		t.Fatal("canonical bytes and digest are not stable")
	}
	copy := got.BytesCopy()
	copy[0] = '['
	if bytes.Equal(copy, got.Bytes) {
		t.Fatal("BytesCopy aliases the canonical value")
	}
}

func TestCanonicalUTF16KeyOrder(t *testing.T) {
	t.Parallel()

	value, err := Canonicalize([]byte(`{"דּ":1,"😀":2,"€":3,"ö":4,"\u0080":5,"1":6,"\r":7}`), 0)
	if err != nil {
		t.Fatalf("Canonicalize() error = %v", err)
	}
	want := `{"\r":7,"1":6,"":5,"ö":4,"€":3,"😀":2,"דּ":1}`
	if string(value.Bytes) != want {
		t.Fatalf("UTF-16 key order = %s, want %s", value.Bytes, want)
	}
	if err := ValidateCanonical(value.Bytes, 0); err != nil {
		t.Fatalf("ValidateCanonical() error = %v", err)
	}
}

func TestCanonicalValidation(t *testing.T) {
	t.Parallel()

	tests := []struct {
		name string
		raw  string
		want error
	}{
		{name: "duplicate key", raw: `{"a":1,"a":2}`, want: ErrInvalidJSON},
		{name: "trailing value", raw: `{} []`, want: ErrInvalidJSON},
		{name: "number overflow", raw: `1e9999`, want: ErrInvalidJSON},
		{name: "unpaired high surrogate", raw: `"\ud800"`, want: ErrInvalidJSON},
		{name: "unpaired low surrogate", raw: `"\udc00"`, want: ErrInvalidJSON},
		{name: "invalid escape", raw: `"\x"`, want: ErrInvalidJSON},
		{name: "incomplete escape", raw: `"\u12"`, want: ErrInvalidJSON},
		{name: "invalid escape hex", raw: `"\u12xz"`, want: ErrInvalidJSON},
		{name: "unexpected delimiter", raw: `]`, want: ErrInvalidJSON},
		{name: "non number", raw: `NaN`, want: ErrInvalidJSON},
	}
	for _, tt := range tests {
		t.Run(tt.name, func(t *testing.T) {
			_, err := Canonicalize([]byte(tt.raw), 0)
			if !errors.Is(err, tt.want) {
				t.Fatalf("Canonicalize() error = %v, want %v", err, tt.want)
			}
		})
	}

	tooDeep := strings.Repeat("[", DefaultMaxDepth+2) + "0" + strings.Repeat("]", DefaultMaxDepth+2)
	if _, err := Canonicalize([]byte(tooDeep), 0); !errors.Is(err, ErrTooDeep) {
		t.Fatalf("deep Canonicalize() error = %v, want %v", err, ErrTooDeep)
	}
	if _, err := Canonicalize([]byte(`{"long":true}`), 5); !errors.Is(err, ErrTooLarge) {
		t.Fatalf("large Canonicalize() error = %v, want %v", err, ErrTooLarge)
	}
	if _, err := Canonicalize([]byte{'"', 0xff, '"'}, 0); !errors.Is(err, ErrInvalidJSON) {
		t.Fatalf("invalid UTF-8 error = %v", err)
	}
	paired, err := Canonicalize([]byte(`"\ud83d\ude00"`), 0)
	if err != nil || string(paired.Bytes) != `"😀"` {
		t.Fatalf("paired surrogate = %s, %v", paired.Bytes, err)
	}
}

func TestValidateCanonical(t *testing.T) {
	t.Parallel()

	valid := []string{
		`null`, `true`, `false`, `0`, `-1`, `0.000001`, `1e+21`,
		`"plain"`, `"quote\"slash\\controls\b\t\n\f\r\u000f"`,
		`[null,{"a":1,"b":[true,false]}]`,
		`{"\r":7,"1":6,"":5,"ö":4,"€":3,"😀":2,"דּ":1}`,
	}
	for _, raw := range valid {
		if err := ValidateCanonical([]byte(raw), 0); err != nil {
			t.Errorf("ValidateCanonical(%s) error = %v", raw, err)
		}
	}

	invalid := []string{
		``, ` `, `null `, `01`, `-0`, `1.0`, `1E+21`, `1e21`,
		`"\/"`, `"\u0061"`, `"\u0008"`, `"\u000F"`,
		`{"b":1,"a":2}`, `{"a":1,"a":2}`,
		`{"outer":{"nested":1,"nested":2}}`,
		`[1, 2]`, `[1,]`, `{"a":1,}`,
	}
	for _, raw := range invalid {
		if err := ValidateCanonical([]byte(raw), 0); !errors.Is(err, ErrInvalidJSON) {
			t.Errorf("ValidateCanonical(%s) error = %v, want ErrInvalidJSON", raw, err)
		}
	}
	if err := ValidateCanonical([]byte(`{"long":true}`), 5); !errors.Is(err, ErrTooLarge) {
		t.Fatalf("large ValidateCanonical() error = %v, want ErrTooLarge", err)
	}
	tooDeep := strings.Repeat("[", DefaultMaxDepth+2) + "0" + strings.Repeat("]", DefaultMaxDepth+2)
	if err := ValidateCanonical([]byte(tooDeep), 0); !errors.Is(err, ErrTooDeep) {
		t.Fatalf("deep ValidateCanonical() error = %v, want ErrTooDeep", err)
	}
}

func TestCanonicalRFC8785NumberSamples(t *testing.T) {
	t.Parallel()

	tests := map[string]string{
		`-0`:                       `0`,
		`5e-324`:                   `5e-324`,
		`-5e-324`:                  `-5e-324`,
		`1.7976931348623157e+308`:  `1.7976931348623157e+308`,
		`-1.7976931348623157e+308`: `-1.7976931348623157e+308`,
		`9007199254740992`:         `9007199254740992`,
		`-9007199254740992`:        `-9007199254740992`,
		`295147905179352830000`:    `295147905179352830000`,
		`999999999999999700000`:    `999999999999999700000`,
		`999999999999999900000`:    `999999999999999900000`,
		`1000000000000000000000`:   `1e+21`,
		`9.999999999999997e-7`:     `9.999999999999997e-7`,
		`0.000001`:                 `0.000001`,
		`1424953923781206.25`:      `1424953923781206.2`,
	}
	for input, want := range tests {
		input, want := input, want
		t.Run(input, func(t *testing.T) {
			t.Parallel()
			got, err := Canonicalize([]byte(input), 0)
			if err != nil {
				t.Fatalf("Canonicalize() error = %v", err)
			}
			if string(got.Bytes) != want {
				t.Fatalf("Canonicalize(%s) = %s, want %s", input, got.Bytes, want)
			}
			if err := ValidateCanonical(got.Bytes, 0); err != nil {
				t.Fatalf("ValidateCanonical(%s) error = %v", got.Bytes, err)
			}
		})
	}
}

func TestMarshalAndDecode(t *testing.T) {
	t.Parallel()

	type record struct {
		Z int    `json:"z"`
		A string `json:"a"`
	}
	value, err := Marshal(record{Z: 7, A: "ok"}, 0)
	if err != nil {
		t.Fatalf("Marshal() error = %v", err)
	}
	if string(value.Bytes) != `{"a":"ok","z":7}` {
		t.Fatalf("Marshal() = %s", value.Bytes)
	}
	var decoded record
	if err := Decode(value.Bytes, &decoded); err != nil {
		t.Fatalf("Decode() error = %v", err)
	}
	if decoded != (record{Z: 7, A: "ok"}) {
		t.Fatalf("Decode() = %#v", decoded)
	}
	if err := Decode([]byte(`{} {}`), &decoded); err == nil {
		t.Fatal("Decode() accepted trailing JSON")
	}
	if err := Decode([]byte(`{"z":`), &decoded); err == nil {
		t.Fatal("Decode() accepted malformed JSON")
	}
	if _, err := Marshal(make(chan int), 0); err == nil {
		t.Fatal("Marshal() accepted a channel")
	}
	different, err := Marshal(record{Z: 8, A: "ok"}, 0)
	if err != nil {
		t.Fatalf("Marshal(different) error = %v", err)
	}
	if value.Equal(different) {
		t.Fatal("different canonical values compare equal")
	}
}
