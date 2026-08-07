// Package canonical provides the deterministic JSON representation used by
// Flow's durable identities. It implements the JSON Canonicalization Scheme
// from RFC 8785 over I-JSON-compatible values.
package canonical

import (
	"bytes"
	"crypto/sha256"
	"encoding/hex"
	"encoding/json"
	"errors"
	"fmt"
	"io"
	"math"
	"slices"
	"strconv"
	"strings"
	"unicode/utf16"
	"unicode/utf8"
)

const DefaultMaxDepth = 100

var (
	ErrInvalidJSON = errors.New("invalid JSON")
	ErrTooDeep     = errors.New("JSON nesting exceeds limit")
	ErrTooLarge    = errors.New("canonical JSON exceeds size limit")
)

// Value is an immutable-by-convention canonical byte sequence and its digest.
// Call BytesCopy when bytes escape the owning operation.
type Value struct {
	Bytes  []byte
	Digest [sha256.Size]byte
}

func (v Value) BytesCopy() []byte { return slices.Clone(v.Bytes) }

func (v Value) DigestHex() string { return hex.EncodeToString(v.Digest[:]) }

func (v Value) Equal(other Value) bool {
	return v.Digest == other.Digest && bytes.Equal(v.Bytes, other.Bytes)
}

// Marshal encodes a Go value, canonicalizes it, and computes its SHA-256
// digest. A zero maxBytes disables the size bound.
func Marshal(v any, maxBytes int) (Value, error) {
	raw, err := json.Marshal(v)
	if err != nil {
		return Value{}, fmt.Errorf("marshal JSON: %w", err)
	}
	return Canonicalize(raw, maxBytes)
}

// Canonicalize validates and canonicalizes one complete JSON value. A zero
// maxBytes disables the size bound.
func Canonicalize(raw []byte, maxBytes int) (Value, error) {
	if !utf8.Valid(raw) {
		return Value{}, fmt.Errorf("%w: invalid UTF-8", ErrInvalidJSON)
	}
	if err := validateUnicodeEscapes(raw); err != nil {
		return Value{}, err
	}

	dec := json.NewDecoder(bytes.NewReader(raw))
	dec.UseNumber()
	value, err := decodeValue(dec, 0)
	if err != nil {
		return Value{}, err
	}
	if tok, err := dec.Token(); err == nil {
		return Value{}, fmt.Errorf("%w: trailing token %v", ErrInvalidJSON, tok)
	} else if !errors.Is(err, io.EOF) {
		return Value{}, fmt.Errorf("%w: trailing data: %v", ErrInvalidJSON, err)
	}

	out := make([]byte, 0, len(raw))
	out, err = appendValue(out, value)
	if err != nil {
		return Value{}, err
	}
	if maxBytes > 0 && len(out) > maxBytes {
		return Value{}, fmt.Errorf("%w: %d > %d", ErrTooLarge, len(out), maxBytes)
	}
	return Value{Bytes: out, Digest: sha256.Sum256(out)}, nil
}

// Decode decodes a canonical value into a typed destination.
func Decode(data []byte, dst any) error {
	dec := json.NewDecoder(bytes.NewReader(data))
	if err := dec.Decode(dst); err != nil {
		return fmt.Errorf("decode canonical JSON: %w", err)
	}
	var trailing any
	if err := dec.Decode(&trailing); !errors.Is(err, io.EOF) {
		if err == nil {
			return fmt.Errorf("decode canonical JSON: trailing value")
		}
		return fmt.Errorf("decode canonical JSON: trailing data: %w", err)
	}
	return nil
}

// ValidateCanonical validates one complete JSON value and requires that its
// bytes already use Flow's canonical representation. It does not construct a
// second canonical byte slice.
func ValidateCanonical(raw []byte, maxBytes int) error {
	if maxBytes > 0 && len(raw) > maxBytes {
		return fmt.Errorf("%w: %d > %d", ErrTooLarge, len(raw), maxBytes)
	}
	if !utf8.Valid(raw) {
		return fmt.Errorf("%w: invalid UTF-8", ErrInvalidJSON)
	}
	validator := canonicalValidator{raw: raw}
	if err := validator.value(0); err != nil {
		return err
	}
	if validator.offset != len(raw) {
		return fmt.Errorf("%w: trailing or noncanonical data", ErrInvalidJSON)
	}
	return nil
}

type canonicalValidator struct {
	raw    []byte
	offset int
}

func (v *canonicalValidator) value(depth int) error {
	if depth > DefaultMaxDepth {
		return ErrTooDeep
	}
	if v.offset >= len(v.raw) {
		return fmt.Errorf("%w: missing value", ErrInvalidJSON)
	}
	switch v.raw[v.offset] {
	case '{':
		return v.object(depth)
	case '[':
		return v.array(depth)
	case '"':
		_, err := v.string(false)
		return err
	case 'n':
		return v.literal("null")
	case 't':
		return v.literal("true")
	case 'f':
		return v.literal("false")
	default:
		return v.number()
	}
}

func (v *canonicalValidator) object(depth int) error {
	v.offset++
	if v.consume('}') {
		return nil
	}
	var previous string
	for index := 0; ; index++ {
		key, err := v.string(true)
		if err != nil {
			return err
		}
		if index > 0 && compareUTF16(previous, key) >= 0 {
			return fmt.Errorf("%w: object keys are duplicated or not canonical", ErrInvalidJSON)
		}
		previous = key
		if !v.consume(':') {
			return fmt.Errorf("%w: object key has no value", ErrInvalidJSON)
		}
		if err := v.value(depth + 1); err != nil {
			return err
		}
		if v.consume('}') {
			return nil
		}
		if !v.consume(',') {
			return fmt.Errorf("%w: object is not canonical", ErrInvalidJSON)
		}
	}
}

func (v *canonicalValidator) array(depth int) error {
	v.offset++
	if v.consume(']') {
		return nil
	}
	for {
		if err := v.value(depth + 1); err != nil {
			return err
		}
		if v.consume(']') {
			return nil
		}
		if !v.consume(',') {
			return fmt.Errorf("%w: array is not canonical", ErrInvalidJSON)
		}
	}
}

func (v *canonicalValidator) string(decode bool) (string, error) {
	if !v.consume('"') {
		return "", fmt.Errorf("%w: expected string", ErrInvalidJSON)
	}
	start := v.offset - 1
	for v.offset < len(v.raw) {
		current := v.raw[v.offset]
		v.offset++
		switch current {
		case '"':
			if !decode {
				return "", nil
			}
			decoded, err := strconv.Unquote(string(v.raw[start:v.offset]))
			if err != nil {
				return "", fmt.Errorf("%w: invalid string", ErrInvalidJSON)
			}
			return decoded, nil
		case '\\':
			if v.offset >= len(v.raw) {
				return "", fmt.Errorf("%w: incomplete escape", ErrInvalidJSON)
			}
			escape := v.raw[v.offset]
			v.offset++
			switch escape {
			case '"', '\\', 'b', 't', 'n', 'f', 'r':
			case 'u':
				if v.offset+4 > len(v.raw) {
					return "", fmt.Errorf("%w: incomplete unicode escape", ErrInvalidJSON)
				}
				digits := v.raw[v.offset : v.offset+4]
				code, err := parseHex4(digits)
				if err != nil || digits[0] != '0' || digits[1] != '0' || code >= 0x20 ||
					code == '\b' || code == '\t' || code == '\n' || code == '\f' || code == '\r' ||
					!lowerHex(digits[2]) || !lowerHex(digits[3]) {
					return "", fmt.Errorf("%w: noncanonical unicode escape", ErrInvalidJSON)
				}
				v.offset += 4
			default:
				return "", fmt.Errorf("%w: noncanonical escape", ErrInvalidJSON)
			}
		default:
			if current < 0x20 {
				return "", fmt.Errorf("%w: unescaped control character", ErrInvalidJSON)
			}
		}
	}
	return "", fmt.Errorf("%w: unterminated string", ErrInvalidJSON)
}

func (v *canonicalValidator) number() error {
	start := v.offset
	v.consume('-')
	if v.offset >= len(v.raw) {
		return fmt.Errorf("%w: incomplete number", ErrInvalidJSON)
	}
	if v.consume('0') {
		if v.offset < len(v.raw) && v.raw[v.offset] >= '0' && v.raw[v.offset] <= '9' {
			return fmt.Errorf("%w: leading zero", ErrInvalidJSON)
		}
	} else {
		if v.raw[v.offset] < '1' || v.raw[v.offset] > '9' {
			return fmt.Errorf("%w: invalid number", ErrInvalidJSON)
		}
		for v.offset < len(v.raw) && v.raw[v.offset] >= '0' && v.raw[v.offset] <= '9' {
			v.offset++
		}
	}
	if v.consume('.') {
		fractionStart := v.offset
		for v.offset < len(v.raw) && v.raw[v.offset] >= '0' && v.raw[v.offset] <= '9' {
			v.offset++
		}
		if v.offset == fractionStart {
			return fmt.Errorf("%w: incomplete fraction", ErrInvalidJSON)
		}
	}
	if v.offset < len(v.raw) && (v.raw[v.offset] == 'e' || v.raw[v.offset] == 'E') {
		v.offset++
		if v.offset < len(v.raw) && (v.raw[v.offset] == '+' || v.raw[v.offset] == '-') {
			v.offset++
		}
		exponentStart := v.offset
		for v.offset < len(v.raw) && v.raw[v.offset] >= '0' && v.raw[v.offset] <= '9' {
			v.offset++
		}
		if v.offset == exponentStart {
			return fmt.Errorf("%w: incomplete exponent", ErrInvalidJSON)
		}
	}
	rawNumber := string(v.raw[start:v.offset])
	canonical, err := canonicalNumber(json.Number(rawNumber))
	if err != nil {
		return err
	}
	if canonical != rawNumber {
		return fmt.Errorf("%w: noncanonical number", ErrInvalidJSON)
	}
	return nil
}

func (v *canonicalValidator) literal(literal string) error {
	if len(v.raw)-v.offset < len(literal) || string(v.raw[v.offset:v.offset+len(literal)]) != literal {
		return fmt.Errorf("%w: invalid literal", ErrInvalidJSON)
	}
	v.offset += len(literal)
	return nil
}

func (v *canonicalValidator) consume(want byte) bool {
	if v.offset >= len(v.raw) || v.raw[v.offset] != want {
		return false
	}
	v.offset++
	return true
}

func lowerHex(value byte) bool {
	return value >= '0' && value <= '9' || value >= 'a' && value <= 'f'
}

func decodeValue(dec *json.Decoder, depth int) (any, error) {
	if depth > DefaultMaxDepth {
		return nil, ErrTooDeep
	}
	tok, err := dec.Token()
	if err != nil {
		return nil, fmt.Errorf("%w: %v", ErrInvalidJSON, err)
	}
	delim, ok := tok.(json.Delim)
	if !ok {
		switch v := tok.(type) {
		case nil, bool, string, json.Number:
			return v, nil
		default:
			return nil, fmt.Errorf("%w: unsupported token %T", ErrInvalidJSON, tok)
		}
	}

	switch delim {
	case '{':
		obj := make(map[string]any)
		for dec.More() {
			keyTok, err := dec.Token()
			if err != nil {
				return nil, fmt.Errorf("%w: object key: %v", ErrInvalidJSON, err)
			}
			key, ok := keyTok.(string)
			if !ok {
				return nil, fmt.Errorf("%w: object key is %T", ErrInvalidJSON, keyTok)
			}
			if _, exists := obj[key]; exists {
				return nil, fmt.Errorf("%w: duplicate object key %q", ErrInvalidJSON, key)
			}
			value, err := decodeValue(dec, depth+1)
			if err != nil {
				return nil, err
			}
			obj[key] = value
		}
		end, err := dec.Token()
		if err != nil || end != json.Delim('}') {
			return nil, fmt.Errorf("%w: unterminated object", ErrInvalidJSON)
		}
		return obj, nil

	case '[':
		array := make([]any, 0)
		for dec.More() {
			value, err := decodeValue(dec, depth+1)
			if err != nil {
				return nil, err
			}
			array = append(array, value)
		}
		end, err := dec.Token()
		if err != nil || end != json.Delim(']') {
			return nil, fmt.Errorf("%w: unterminated array", ErrInvalidJSON)
		}
		return array, nil
	default:
		return nil, fmt.Errorf("%w: unexpected delimiter %q", ErrInvalidJSON, delim)
	}
}

func appendValue(dst []byte, value any) ([]byte, error) {
	switch v := value.(type) {
	case nil:
		return append(dst, "null"...), nil
	case bool:
		return strconv.AppendBool(dst, v), nil
	case string:
		return appendString(dst, v), nil
	case json.Number:
		n, err := canonicalNumber(v)
		if err != nil {
			return nil, err
		}
		return append(dst, n...), nil
	case []any:
		dst = append(dst, '[')
		for i, item := range v {
			if i > 0 {
				dst = append(dst, ',')
			}
			var err error
			dst, err = appendValue(dst, item)
			if err != nil {
				return nil, err
			}
		}
		return append(dst, ']'), nil
	case map[string]any:
		keys := make([]string, 0, len(v))
		for key := range v {
			keys = append(keys, key)
		}
		slices.SortFunc(keys, compareUTF16)
		dst = append(dst, '{')
		for i, key := range keys {
			if i > 0 {
				dst = append(dst, ',')
			}
			dst = appendString(dst, key)
			dst = append(dst, ':')
			var err error
			dst, err = appendValue(dst, v[key])
			if err != nil {
				return nil, err
			}
		}
		return append(dst, '}'), nil
	default:
		return nil, fmt.Errorf("%w: unsupported value %T", ErrInvalidJSON, value)
	}
}

func appendString(dst []byte, value string) []byte {
	dst = append(dst, '"')
	for _, r := range value {
		switch r {
		case '"', '\\':
			dst = append(dst, '\\', byte(r))
		case '\b':
			dst = append(dst, `\b`...)
		case '\t':
			dst = append(dst, `\t`...)
		case '\n':
			dst = append(dst, `\n`...)
		case '\f':
			dst = append(dst, `\f`...)
		case '\r':
			dst = append(dst, `\r`...)
		default:
			if r < 0x20 {
				dst = append(dst, `\u00`...)
				dst = append(dst, "0123456789abcdef"[byte(r)>>4], "0123456789abcdef"[byte(r)&0xf])
			} else {
				dst = utf8.AppendRune(dst, r)
			}
		}
	}
	return append(dst, '"')
}

func compareUTF16(a, b string) int {
	aa := utf16.Encode([]rune(a))
	bb := utf16.Encode([]rune(b))
	for i := 0; i < min(len(aa), len(bb)); i++ {
		if aa[i] < bb[i] {
			return -1
		}
		if aa[i] > bb[i] {
			return 1
		}
	}
	return len(aa) - len(bb)
}

func canonicalNumber(number json.Number) (string, error) {
	raw := string(number)
	f, err := strconv.ParseFloat(raw, 64)
	if err != nil || math.IsInf(f, 0) || math.IsNaN(f) {
		return "", fmt.Errorf("%w: invalid number %q", ErrInvalidJSON, raw)
	}
	if f == 0 {
		return "0", nil
	}
	abs := math.Abs(f)
	if abs >= 1e-6 && abs < 1e21 {
		return strconv.FormatFloat(f, 'f', -1, 64), nil
	}
	exp := strconv.FormatFloat(f, 'e', -1, 64)
	parts := strings.SplitN(exp, "e", 2)
	sign := ""
	digits := parts[1]
	if digits[0] == '+' || digits[0] == '-' {
		sign = digits[:1]
		digits = digits[1:]
	}
	digits = strings.TrimLeft(digits, "0")
	if digits == "" {
		digits = "0"
	}
	return parts[0] + "e" + sign + digits, nil
}

func validateUnicodeEscapes(raw []byte) error {
	inString := false
	for i := 0; i < len(raw); i++ {
		switch raw[i] {
		case '"':
			inString = !inString
		case '\\':
			if !inString || i+1 >= len(raw) {
				continue
			}
			i++
			if raw[i] != 'u' {
				continue
			}
			if i+4 >= len(raw) {
				return fmt.Errorf("%w: incomplete unicode escape", ErrInvalidJSON)
			}
			code, err := parseHex4(raw[i+1 : i+5])
			if err != nil {
				return err
			}
			i += 4
			if code >= 0xD800 && code <= 0xDBFF {
				if i+6 >= len(raw) || raw[i+1] != '\\' || raw[i+2] != 'u' {
					return fmt.Errorf("%w: unpaired high surrogate", ErrInvalidJSON)
				}
				low, err := parseHex4(raw[i+3 : i+7])
				if err != nil || low < 0xDC00 || low > 0xDFFF {
					return fmt.Errorf("%w: unpaired high surrogate", ErrInvalidJSON)
				}
				i += 6
			} else if code >= 0xDC00 && code <= 0xDFFF {
				return fmt.Errorf("%w: unpaired low surrogate", ErrInvalidJSON)
			}
		}
	}
	return nil
}

func parseHex4(value []byte) (uint16, error) {
	parsed, err := strconv.ParseUint(string(value), 16, 16)
	if err != nil {
		return 0, fmt.Errorf("%w: invalid unicode escape", ErrInvalidJSON)
	}
	return uint16(parsed), nil
}
