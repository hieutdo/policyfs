package cli

import (
	"testing"

	"github.com/stretchr/testify/require"
)

// TestMount_MissingArg_returnsUsage verifies `pfs mount` requires a mount name.
func TestMount_MissingArg_returnsUsage(t *testing.T) {
	code, _, stderr := runCLI(t, []string{"mount"})
	require.Equal(t, ExitUsage, code)
	require.Equal(t, "error: invalid arguments\ncause: requires exactly 1 argument: <mount>\nhint: run 'pfs mount --help'\n", stderr)
}

// TestMount_InvalidMountName_returnsUsage verifies mount name regex enforcement.
func TestMount_InvalidMountName_returnsUsage(t *testing.T) {
	code, _, stderr := runCLI(t, []string{"--config", "/does/not/exist.yaml", "mount", "bad*name"})
	require.Equal(t, ExitUsage, code)
	require.Equal(t, "error: invalid arguments\ncause: invalid mount name\nhint: run 'pfs mount --help'\n", stderr)
}

// TestMount_ConfigLoadError_returnsFail verifies config load errors map to ExitFail.
func TestMount_ConfigLoadError_returnsFail(t *testing.T) {
	code, _, stderr := runCLI(t, []string{"--config", "/does/not/exist.yaml", "mount", "media"})
	require.Equal(t, ExitFail, code)
	require.Contains(t, stderr, "error: config file not found: /does/not/exist.yaml\n")
	require.Contains(t, stderr, "cause: open /does/not/exist.yaml:")
	require.Contains(t, stderr, "hint: pass --config /path/to/pfs.yaml or create /does/not/exist.yaml\n")
}

// TestMount_InvalidConfig_returnsFail verifies config validation errors stop before mounting.
func TestMount_InvalidConfig_returnsFail(t *testing.T) {
	// missing mountpoint
	cfg := writeTempConfig(t, `
mounts:
  media:
    storage_paths:
      - id: "ssd1"
        path: "/mnt/ssd1/media"
    routing_rules:
      - match: "**"
        targets: ["ssd1"]
`)

	code, _, stderr := runCLI(t, []string{"--config", cfg, "mount", "media"})
	require.Equal(t, ExitFail, code)
	require.Equal(t, "error: invalid config: "+cfg+"\ncause: config: mount \"media\": mountpoint is required\n", stderr)
}

// TestMount_MountNotFound_returnsUsage verifies `pfs mount <mount>` returns usage when the mount is not in the config.
func TestMount_MountNotFound_returnsUsage(t *testing.T) {
	cfg := writeTempConfig(t, `
mounts:
  other:
    mountpoint: "/mnt/pfs/other"
    storage_paths:
      - id: "ssd1"
        path: "/mnt/ssd1/other"
    routing_rules:
      - match: "**"
        targets: ["ssd1"]
`)

	code, _, stderr := runCLI(t, []string{"--config", cfg, "mount", "media"})
	require.Equal(t, ExitUsage, code)
	require.Contains(t, stderr, "error: invalid arguments\n")
	require.Contains(t, stderr, "cause: config: mount \"media\" not found\n")
	require.Contains(t, stderr, "hint: run 'pfs mount --help'\n")
}
