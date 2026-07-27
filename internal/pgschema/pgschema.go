package pgschema

import (
	"errors"
	"regexp"
	"strings"
)

var identifierPattern = regexp.MustCompile(`^[A-Za-z_][A-Za-z0-9_]*$`)

func Validate(identifier string) error {
	if identifier == "" || len(identifier) > 63 || !identifierPattern.MatchString(identifier) {
		return errors.New("must be a simple PostgreSQL identifier of at most 63 bytes")
	}
	return nil
}

func Quote(identifier string) string {
	return `"` + strings.ReplaceAll(identifier, `"`, `""`) + `"`
}

func Table(schema, table string) string { return Quote(schema) + `.` + Quote(table) }
