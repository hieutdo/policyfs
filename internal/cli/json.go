package cli

import (
	"encoding/json"
	"errors"
	"fmt"
	"os"
)

// JSONScope describes the scope of a CLI command when emitting JSON.
type JSONScope struct {
	Mount  *string `json:"mount,omitempty"`
	Config *string `json:"config,omitempty"`
}

// JSONIssue describes one warning or error entry in JSON output.
type JSONIssue struct {
	Code    string `json:"code,omitempty"`
	Message string `json:"message"`
	Hint    string `json:"hint,omitempty"`
}

// JSONEnvelope is the common JSON envelope shared by CLI commands.
type JSONEnvelope struct {
	Command  string      `json:"command"`
	OK       bool        `json:"ok"`
	Scope    *JSONScope  `json:"scope,omitempty"`
	Issues   []JSONIssue `json:"issues,omitempty"`
	Warnings []JSONIssue `json:"warnings"`
	Errors   []JSONIssue `json:"errors"`
}

// jsonIssue creates a JsonIssue with an optional hint.
func jsonIssue(code string, message string, hint string) JSONIssue {
	return JSONIssue{Code: code, Message: message, Hint: hint}
}

// jsonIssueFromError creates a JsonIssue whose message is the root cause error string.
func jsonIssueFromError(code string, err error, hint string) JSONIssue {
	msg := ""
	if err != nil {
		msg = rootCause(err).Error()
	}
	return jsonIssue(code, msg, hint)
}

// jsonIssueFromConfigLoadError creates a JsonIssue for config load failures.
func jsonIssueFromConfigLoadError(configPath string, err error) JSONIssue {
	hint := ""
	if errors.Is(err, os.ErrNotExist) {
		hint = fmt.Sprintf("pass --config /path/to/pfs.yaml or create %s", configPath)
	}
	return jsonIssueFromError("CFG_LOAD", err, hint)
}

// writeJSON writes a single JSON value to stdout followed by a newline.
func writeJSON(v any) error {
	b, err := json.Marshal(v)
	if err != nil {
		return fmt.Errorf("failed to marshal json: %w", err)
	}
	if _, err := os.Stdout.Write(b); err != nil {
		return fmt.Errorf("failed to write json: %w", err)
	}
	if _, err := os.Stdout.Write([]byte("\n")); err != nil {
		return fmt.Errorf("failed to write json: %w", err)
	}
	return nil
}

// emitJSONAndExit writes JSON to stdout and exits with the provided code without printing extra human output.
func emitJSONAndExit(code int, v any) error {
	if err := writeJSON(v); err != nil {
		return &CLIError{
			Code:     ExitFail,
			Headline: "failed to write json",
			Cause:    rootCause(err),
		}
	}
	return &CLIError{Code: code, Silent: true}
}
