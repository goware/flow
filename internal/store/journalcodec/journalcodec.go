// Package journalcodec owns version validation for internal journal bodies.
// Application definition versions are separate from these schema versions.
package journalcodec

import (
	"encoding/json"
	"errors"
	"fmt"

	"github.com/goware/flow/internal/canonical"
)

var ErrVersion = errors.New("journal body requires a positive integer v")

func Encode(body any) (canonical.Value, error) {
	encoded, err := canonical.Marshal(body, 0)
	if err != nil {
		return canonical.Value{}, err
	}
	if _, err := Version(encoded.Bytes); err != nil {
		return canonical.Value{}, err
	}
	return encoded, nil
}

func Decode[T any](body []byte) (T, error) {
	var zero T
	if _, err := Version(body); err != nil {
		return zero, err
	}
	var decoded T
	if err := canonical.Decode(body, &decoded); err != nil {
		return zero, err
	}
	return decoded, nil
}

func Version(body []byte) (int, error) {
	var header struct {
		Version json.RawMessage `json:"v"`
	}
	if err := canonical.Decode(body, &header); err != nil {
		return 0, fmt.Errorf("%w: %v", ErrVersion, err)
	}
	if len(header.Version) == 0 {
		return 0, ErrVersion
	}
	var version int
	if err := json.Unmarshal(header.Version, &version); err != nil || version <= 0 {
		return 0, ErrVersion
	}
	return version, nil
}
