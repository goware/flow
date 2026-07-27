package flowerr

import "errors"

var (
	ErrNotFound        = errors.New("flow: not found")
	ErrConflict        = errors.New("flow: conflict")
	ErrInvalid         = errors.New("flow: invalid")
	ErrInvalidState    = errors.New("flow: invalid state")
	ErrTerminal        = errors.New("flow: terminal")
	ErrLeaseLost       = errors.New("flow: lease lost")
	ErrPayloadTooLarge = errors.New("flow: payload too large")
	ErrClosed          = errors.New("flow: closed")
	ErrSchema          = errors.New("flow: incompatible schema")
)
