package cli

import (
	"encoding/json"
	"testing"

	"github.com/stretchr/testify/require"
)

// TestDoctor_JSON_OK verifies `pfs doctor --json` emits the common JSON envelope and exits 0 on valid config.
func TestDoctor_JSON_OK(t *testing.T) {
	cfg := writeTempConfig(t, `
mounts:
  media:
    mountpoint: "/mnt/pfs/media"
    storage_paths:
      - id: "ssd1"
        path: "/mnt/ssd1/media"
    routing_rules:
      - match: "**"
        targets: ["ssd1"]
`)

	code, stdout, stderr := runCLI(t, []string{"--config", cfg, "doctor", "--json"})
	require.Equal(t, ExitOK, code)
	require.Empty(t, stderr)

	var env JSONEnvelope
	require.NoError(t, json.Unmarshal([]byte(stdout), &env))
	require.Equal(t, "doctor", env.Command)
	require.True(t, env.OK)
	require.NotNil(t, env.Scope)
	require.NotNil(t, env.Scope.Config)
	require.NotNil(t, env.Warnings)
	require.NotNil(t, env.Errors)
}

// TestDoctor_JSON_Fail verifies `pfs doctor --json` exits 78 on invalid config and prints JSON only.
func TestDoctor_JSON_Fail(t *testing.T) {
	cfg := writeTempConfig(t, `
mounts:
  media:
    mountpoint: ""
    storage_paths:
      - id: "ssd1"
        path: "/mnt/ssd1/media"
    routing_rules:
      - match: "**"
        targets: ["ssd1"]
`)

	code, stdout, stderr := runCLI(t, []string{"--config", cfg, "doctor", "--json"})
	require.Equal(t, ExitDoctorFail, code)
	require.Empty(t, stderr)

	var env JSONEnvelope
	require.NoError(t, json.Unmarshal([]byte(stdout), &env))
	require.Equal(t, "doctor", env.Command)
	require.False(t, env.OK)
	require.NotEmpty(t, env.Issues)
	require.Empty(t, env.Warnings)
	require.Empty(t, env.Errors)
}

// TestDoctor_JSON_ConfigLoadErrorIsExecutionError verifies config load failures are reported as command errors (top-level `errors`), not doctor findings (`issues`).
func TestDoctor_JSON_ConfigLoadErrorIsExecutionError(t *testing.T) {
	code, stdout, stderr := runCLI(t, []string{"--config", "/does/not/exist.yaml", "doctor", "--json"})
	require.Equal(t, ExitFail, code)
	require.Empty(t, stderr)

	var env JSONEnvelope
	require.NoError(t, json.Unmarshal([]byte(stdout), &env))
	require.Equal(t, "doctor", env.Command)
	require.False(t, env.OK)
	require.Empty(t, env.Issues)
	require.NotEmpty(t, env.Errors)
	require.Equal(t, "CFG_LOAD", env.Errors[0].Code)
	require.Contains(t, env.Errors[0].Hint, "pass --config")
}

// TestDoctor_ShortFlag verifies `pfs doctor -j` works as alias for --json.
func TestDoctor_ShortFlag(t *testing.T) {
	cfg := writeTempConfig(t, `
mounts:
  media:
    mountpoint: "/mnt/pfs/media"
    storage_paths:
      - id: "ssd1"
        path: "/mnt/ssd1/media"
    routing_rules:
      - match: "**"
        targets: ["ssd1"]
`)

	code, stdout, stderr := runCLI(t, []string{"-c", cfg, "doctor", "-j"})
	require.Equal(t, ExitOK, code)
	require.Empty(t, stderr)

	var env JSONEnvelope
	require.NoError(t, json.Unmarshal([]byte(stdout), &env))
	require.Equal(t, "doctor", env.Command)
}

// TestDoctor_Text_FailListsAllIssues verifies `pfs doctor` (text) lists all discovered issues with ✗ marks.
func TestDoctor_Text_FailListsAllIssues(t *testing.T) {
	cfg := writeTempConfig(t, `
mounts:
  media:
    mountpoint: ""
`)

	code, stdout, stderr := runCLI(t, []string{"--config", cfg, "doctor"})
	require.Equal(t, ExitDoctorFail, code)
	require.Empty(t, stderr)

	require.Contains(t, stdout, "\u2713 Config loaded:")
	require.Contains(t, stdout, "\u2717 Mount \"media\": mountpoint is required")
	require.Contains(t, stdout, "\u2717 Mount \"media\": storage_paths must not be empty")
	require.Contains(t, stdout, "\u2717 Mount \"media\": routing_rules must not be empty")
	require.Contains(t, stdout, "3 issues")
}

// TestDoctor_Text_OK_showsCheckmarks verifies valid config prints ✓ for all checks.
func TestDoctor_Text_OK_showsCheckmarks(t *testing.T) {
	src := t.TempDir()
	cfg := writeTempConfig(t, `
mounts:
  media:
    mountpoint: "`+src+`"
    storage_paths:
      - id: "ssd1"
        path: "`+src+`"
    routing_rules:
      - match: "**"
        targets: ["ssd1"]
`)

	code, stdout, stderr := runCLI(t, []string{"--config", cfg, "doctor"})
	require.Equal(t, ExitOK, code)
	require.Empty(t, stderr)

	require.Contains(t, stdout, "\u2713 Config loaded:")
	require.Contains(t, stdout, "\u2713 Mount \"media\": config valid")
	require.Contains(t, stdout, "All checks passed.")
}

// TestDoctor_TooManyArgs_returnsUsage verifies `pfs doctor` rejects more than 2 args through the real CLI flow.
func TestDoctor_TooManyArgs_returnsUsage(t *testing.T) {
	code, _, stderr := runCLI(t, []string{"doctor", "a", "b", "c"})
	require.Equal(t, ExitUsage, code)
	require.Contains(t, stderr, "error: invalid arguments\n")
	require.Contains(t, stderr, "cause: requires at most 2 arguments")
	require.Contains(t, stderr, "hint: run 'pfs doctor --help'\n")
}

// TestDoctor_InvalidMountName_returnsUsage verifies mount name validation is enforced for `pfs doctor <mount>`.
func TestDoctor_InvalidMountName_returnsUsage(t *testing.T) {
	code, _, stderr := runCLI(t, []string{"doctor", "bad*name"})
	require.Equal(t, ExitUsage, code)
	require.Contains(t, stderr, "error: invalid arguments\n")
	require.Contains(t, stderr, "cause: invalid mount name\n")
	require.Contains(t, stderr, "hint: run 'pfs doctor --help'\n")
}
