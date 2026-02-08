package errkind

import (
	"errors"
	"fmt"
)

// ErrRequired is a sentinel kind for missing-required-input validation errors.
var ErrRequired = errors.New("required")

// ErrInvalid is a sentinel kind for invalid-input validation errors.
var ErrInvalid = errors.New("invalid")

// ErrNotFound is a sentinel kind for not-found errors.
var ErrNotFound = errors.New("not found")

// ErrBusy is a sentinel kind for busy/locked errors.
var ErrBusy = errors.New("busy")

// ErrNil is a sentinel kind for nil-receiver/nil-argument errors.
var ErrNil = errors.New("nil")

// SentinelError is a stable sentinel error value (meant to be used as a package-level var).
type SentinelError string

// Error returns the sentinel message.
func (e SentinelError) Error() string { return string(e) }

// KindError preserves a stable message while allowing callers to match a stable Kind via errors.Is.
//
// Use this wrapper when you already have a package-level sentinel error (domain-level) and want
// a stable message/context at the call site.
type KindError struct {
	Kind  error
	Msg   string
	Cause error
}

// Error returns the stable message.
func (e *KindError) Error() string {
	if e == nil {
		return ""
	}
	return e.Msg
}

// Unwrap returns the underlying cause.
func (e *KindError) Unwrap() error {
	if e == nil {
		return nil
	}
	return e.Cause
}

// Is matches the error kind for errors.Is.
func (e *KindError) Is(target error) bool {
	if e == nil {
		return false
	}
	return target == e.Kind
}

// RequiredError is a typed error for missing-required inputs.
//
// It matches errors.Is(err, ErrRequired).
type RequiredError struct {
	What string
	Msg  string
}

// Error returns the user-facing message.
func (e *RequiredError) Error() string {
	if e == nil {
		return ""
	}
	if e.Msg != "" {
		return e.Msg
	}
	if e.What != "" {
		return fmt.Sprintf("%s is required", e.What)
	}
	return "required"
}

// Is matches the required kind for errors.Is.
func (e *RequiredError) Is(target error) bool {
	return target == ErrRequired
}

// InvalidError is a typed error for invalid inputs.
//
// It matches errors.Is(err, ErrInvalid).
type InvalidError struct {
	What  string
	Msg   string
	Cause error
}

// Error returns the user-facing message.
func (e *InvalidError) Error() string {
	if e == nil {
		return ""
	}
	if e.Msg != "" {
		return e.Msg
	}
	if e.What != "" {
		return fmt.Sprintf("invalid %s", e.What)
	}
	return "invalid"
}

// Unwrap returns the underlying cause.
func (e *InvalidError) Unwrap() error {
	if e == nil {
		return nil
	}
	return e.Cause
}

// Is matches the invalid kind for errors.Is.
func (e *InvalidError) Is(target error) bool {
	return target == ErrInvalid
}

// NotFoundError is a typed error for not-found conditions.
//
// It matches errors.Is(err, ErrNotFound).
type NotFoundError struct {
	What string
	Msg  string
}

// Error returns the user-facing message.
func (e *NotFoundError) Error() string {
	if e == nil {
		return ""
	}
	if e.Msg != "" {
		return e.Msg
	}
	if e.What != "" {
		return fmt.Sprintf("%s not found", e.What)
	}
	return "not found"
}

// Is matches the not-found kind for errors.Is.
func (e *NotFoundError) Is(target error) bool {
	return target == ErrNotFound
}

// BusyError is a typed error for busy/locked conditions.
//
// It matches errors.Is(err, ErrBusy).
type BusyError struct {
	What string
	Msg  string
}

// Error returns the user-facing message.
func (e *BusyError) Error() string {
	if e == nil {
		return ""
	}
	if e.Msg != "" {
		return e.Msg
	}
	if e.What != "" {
		return fmt.Sprintf("%s is busy", e.What)
	}
	return "busy"
}

// Is matches the busy kind for errors.Is.
func (e *BusyError) Is(target error) bool {
	return target == ErrBusy
}

// NilError is a typed error for nil receiver/argument conditions.
//
// It matches errors.Is(err, ErrNil).
type NilError struct {
	What string
	Msg  string
}

// Error returns the user-facing message.
func (e *NilError) Error() string {
	if e == nil {
		return ""
	}
	if e.Msg != "" {
		return e.Msg
	}
	if e.What != "" {
		return fmt.Sprintf("%s is nil", e.What)
	}
	return "nil"
}

// Is matches the nil kind for errors.Is.
func (e *NilError) Is(target error) bool {
	return target == ErrNil
}
