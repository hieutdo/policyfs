package cli

import (
	"testing"

	"github.com/stretchr/testify/require"
)

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
