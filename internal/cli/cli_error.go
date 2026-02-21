package cli

import (
	"errors"
	"fmt"
	"os"
	"strings"
)

// CLIError represents a user-facing CLI error
type CLIError struct {
	Code     int
	Cmd      string
	Headline string
	Cause    error
	Hint     string
	Silent   bool
}

// Error implements the error interface.
func (e *CLIError) Error() string {
	if e == nil {
		return ""
	}
	if e.Cause != nil {
		return e.Cause.Error()
	}
	return e.Headline
}

// Unwrap enables errors.Is / errors.As.
func (e *CLIError) Unwrap() error {
	if e == nil {
		return nil
	}
	return e.Cause
}

// Lines returns the formatted error lines (max 3) for printing to stderr.
func (e *CLIError) Lines() []string {
	if e == nil {
		return nil
	}

	prefix := "error:"

	headline := strings.TrimSpace(e.Headline)
	if headline == "" {
		headline = "unexpected error"
	}

	lines := []string{prefix + " " + headline}
	if e.Cause != nil {
		lines = append(lines, "cause: "+e.Cause.Error())
	}
	if hint := strings.TrimSpace(e.Hint); hint != "" {
		lines = append(lines, "hint: "+hint)
	}
	if len(lines) > 3 {
		lines = lines[:3]
	}
	return lines
}

// rootCause unwraps err until it reaches the innermost error.
func rootCause(err error) error {
	if err == nil {
		return nil
	}
	for {
		// Preserve path context for user-facing causes.
		if pe, ok := errors.AsType[*os.PathError](err); ok {
			return pe
		}

		next := errors.Unwrap(err)
		if next == nil {
			return err
		}
		err = next
	}
}

// newConfigLoadCLIError formats a config load error (missing/permission/invalid) as a CLIError.
func newConfigLoadCLIError(cmd string, configPath string, err error) *CLIError {
	rc := rootCause(err)
	headline := fmt.Sprintf("invalid config: %s", configPath)
	hint := ""
	if errors.Is(err, os.ErrNotExist) {
		headline = fmt.Sprintf("config file not found: %s", configPath)
		hint = fmt.Sprintf("pass --config /path/to/pfs.yaml or create %s", configPath)
	} else if errors.Is(err, os.ErrPermission) {
		headline = fmt.Sprintf("permission denied: %s", configPath)
	}
	return &CLIError{
		Code:     ExitFail,
		Cmd:      cmd,
		Headline: headline,
		Cause:    rc,
		Hint:     hint,
	}
}
