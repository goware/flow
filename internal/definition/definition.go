package definition

import (
	"fmt"
	"reflect"
	"strings"
	"unicode"

	"github.com/goware/flow/internal/canonical"
)

type Kind string

const (
	CommandKind     Kind = "command"
	EventKind       Kind = "event"
	PlanKind        Kind = "plan"
	CoordinatorKind Kind = "coordinator"
)

type Codec struct {
	Type reflect.Type
	Name string
}

func NewCodec[T any]() Codec {
	typeOf := reflect.TypeFor[T]()
	return Codec{Type: typeOf, Name: typeName(typeOf)}
}

func (c Codec) Encode(value any, maxBytes int) (canonical.Value, error) {
	if value == nil && permitsNil(c.Type) {
		return canonical.Marshal(value, maxBytes)
	}
	actual := reflect.TypeOf(value)
	if actual == nil || !actual.AssignableTo(c.Type) {
		return canonical.Value{}, fmt.Errorf("codec %s received %v", c.Name, actual)
	}
	return canonical.Marshal(value, maxBytes)
}

func (c Codec) Decode(data []byte) (any, error) {
	dst := reflect.New(c.Type)
	if err := canonical.Decode(data, dst.Interface()); err != nil {
		return nil, err
	}
	return dst.Elem().Interface(), nil
}

func (c Codec) Compatible(other Codec) bool { return c.Type == other.Type }

type Base struct {
	Kind    Kind
	Name    string
	Version int
}

func (b Base) Key() string { return string(b.Kind) + ":" + b.Name + ":" + fmt.Sprint(b.Version) }

type Command struct {
	Base
	Args   Codec
	Result Codec
}

type Event struct {
	Name      string
	Namespace string
	Payload   Codec
}

type Plan struct {
	Base
	Args Codec
}

type Coordinator struct {
	Base
	State Codec
}

func ValidateName(name string) error {
	if name == "" {
		return fmt.Errorf("name must not be empty")
	}
	if name != strings.TrimSpace(name) {
		return fmt.Errorf("name must not have surrounding whitespace")
	}
	if len(name) > 255 {
		return fmt.Errorf("name exceeds 255 bytes")
	}
	for _, r := range name {
		if unicode.IsControl(r) || unicode.IsSpace(r) {
			return fmt.Errorf("name contains whitespace or control characters")
		}
	}
	return nil
}

func ValidateBase(base Base) error {
	if err := ValidateName(base.Name); err != nil {
		return err
	}
	if base.Version <= 0 {
		return fmt.Errorf("version must be positive")
	}
	return nil
}

func typeName(value reflect.Type) string {
	if value == nil {
		return "<nil>"
	}
	return value.String()
}

func permitsNil(value reflect.Type) bool {
	switch value.Kind() {
	case reflect.Chan, reflect.Func, reflect.Interface, reflect.Map, reflect.Pointer, reflect.Slice:
		return true
	default:
		return false
	}
}
