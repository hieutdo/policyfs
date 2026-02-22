package cli

import (
	"bufio"
	"os"
	"os/exec"
	"path/filepath"
	"testing"

	"github.com/hieutdo/policyfs/internal/config"
	"github.com/stretchr/testify/require"
)

// TestMaint_Busy_shouldReturnExitBusy verifies maint returns ExitBusy when job.lock is held.
func TestMaint_Busy_shouldReturnExitBusy(t *testing.T) {
	if os.Getenv(config.EnvTestHelper) == "1" {
		return
	}

	runtimeDir := filepath.Join(t.TempDir(), "runtime")
	require.NoError(t, os.MkdirAll(runtimeDir, 0o755))
	t.Setenv(config.EnvRuntimeDir, runtimeDir)

	stateDir := filepath.Join(t.TempDir(), "state")
	require.NoError(t, os.MkdirAll(stateDir, 0o755))
	t.Setenv(config.EnvStateDir, stateDir)

	src := t.TempDir()
	dst := t.TempDir()

	cfg := writeTempConfig(t, `
mounts:
  media:
    mountpoint: "/mnt/pfs/media"
    storage_paths:
      - id: "ssd1"
        path: "`+src+`"
        indexed: false
      - id: "hdd1"
        path: "`+dst+`"
        indexed: true
    routing_rules:
      - match: "**"
        targets: ["ssd1"]
`)

	cmd := exec.Command(os.Args[0], "-test.run=TestHelperProcessIndexHoldJobLock")
	cmd.Env = append(
		os.Environ(),
		config.EnvTestHelper+"=1",
		config.EnvRuntimeDir+"="+runtimeDir,
		config.EnvStateDir+"="+stateDir,
		config.EnvTestMount+"=media",
		config.EnvTestLockFile+"="+config.DefaultJobLockFile,
	)
	stdout, err := cmd.StdoutPipe()
	require.NoError(t, err)
	stdin, err := cmd.StdinPipe()
	require.NoError(t, err)
	cmd.Stderr = os.Stderr
	require.NoError(t, cmd.Start())

	waited := false
	t.Cleanup(func() {
		_ = stdin.Close()
		if !waited {
			_ = cmd.Process.Kill()
			_ = cmd.Wait()
		}
	})

	r := bufio.NewReader(stdout)
	line, err := r.ReadString('\n')
	require.NoError(t, err)
	require.Equal(t, "ready\n", line)

	code, _, stderr := runCLI(t, []string{"--config", cfg, "maint", "media", "--quiet"})
	require.Equal(t, ExitBusy, code)
	require.Contains(t, stderr, "error: job already running")

	require.NoError(t, stdin.Close())
	require.NoError(t, cmd.Wait())
	waited = true
}

// TestMaint_invalidIndexMode_shouldReturnExitUsage verifies --index only accepts touch|all|off.
func TestMaint_invalidIndexMode_shouldReturnExitUsage(t *testing.T) {
	runtimeDir := filepath.Join(t.TempDir(), "runtime")
	require.NoError(t, os.MkdirAll(runtimeDir, 0o755))
	t.Setenv(config.EnvRuntimeDir, runtimeDir)

	stateDir := filepath.Join(t.TempDir(), "state")
	require.NoError(t, os.MkdirAll(stateDir, 0o755))
	t.Setenv(config.EnvStateDir, stateDir)

	src := t.TempDir()
	dst := t.TempDir()

	cfg := writeTempConfig(t, `
mounts:
  media:
    mountpoint: "/mnt/pfs/media"
    storage_paths:
      - id: "ssd1"
        path: "`+src+`"
        indexed: false
      - id: "hdd1"
        path: "`+dst+`"
        indexed: true
    routing_rules:
      - match: "**"
        targets: ["ssd1"]
`)

	code, _, stderr := runCLI(t, []string{"--config", cfg, "maint", "media", "--index=nope", "--quiet"})
	require.Equal(t, ExitUsage, code)
	require.Contains(t, stderr, "error: invalid arguments")
}

// TestMaint_indexTouch_noMoveWork_shouldSkipPruneAndIndex verifies maint skips prune and index when mover did no work.
func TestMaint_indexTouch_noTouchedIndexedStorages_shouldSkipIndex(t *testing.T) {
	runtimeDir := filepath.Join(t.TempDir(), "runtime")
	require.NoError(t, os.MkdirAll(runtimeDir, 0o755))
	t.Setenv(config.EnvRuntimeDir, runtimeDir)

	stateDir := filepath.Join(t.TempDir(), "state")
	require.NoError(t, os.MkdirAll(stateDir, 0o755))
	t.Setenv(config.EnvStateDir, stateDir)

	src := t.TempDir()
	dst := t.TempDir()

	cfg := writeTempConfig(t, `
mounts:
  media:
    mountpoint: "/mnt/pfs/media"
    storage_paths:
      - id: "ssd1"
        path: "`+src+`"
        indexed: false
      - id: "hdd1"
        path: "`+dst+`"
        indexed: true
    routing_rules:
      - match: "**"
        targets: ["ssd1"]
    mover:
      enabled: false
`)

	code, stdout, stderr := runCLI(t, []string{"--config", cfg, "maint", "media", "--index=touch"})
	require.Equal(t, ExitNoChanges, code, "expected exit_code=%d got=%d stderr=%s", ExitNoChanges, code, stderr)
	require.Contains(t, stdout, "Skipped: mover did no work")
}

// TestMaint_allPhasesNoWork_shouldReturnExitNoChanges verifies maint returns ExitNoChanges when nothing changed across all phases.
func TestMaint_allPhasesNoWork_shouldReturnExitNoChanges(t *testing.T) {
	runtimeDir := filepath.Join(t.TempDir(), "runtime")
	require.NoError(t, os.MkdirAll(runtimeDir, 0o755))
	t.Setenv(config.EnvRuntimeDir, runtimeDir)

	stateDir := filepath.Join(t.TempDir(), "state")
	require.NoError(t, os.MkdirAll(stateDir, 0o755))
	t.Setenv(config.EnvStateDir, stateDir)

	src := t.TempDir()
	dst := t.TempDir()

	cfg := writeTempConfig(t, `
mounts:
  media:
    mountpoint: "/mnt/pfs/media"
    storage_paths:
      - id: "ssd1"
        path: "`+src+`"
        indexed: false
      - id: "hdd1"
        path: "`+dst+`"
        indexed: true
    routing_rules:
      - match: "**"
        targets: ["ssd1"]
    mover:
      enabled: false
`)

	code, stdout, stderr := runCLI(t, []string{"--config", cfg, "maint", "media", "--index=touch"})
	require.Equal(t, ExitNoChanges, code, "expected exit_code=%d got=%d stderr=%s", ExitNoChanges, code, stderr)
	require.Empty(t, stderr)
	require.Contains(t, stdout, "pfs maint: mount=media")
	require.Contains(t, stdout, "Skipped: mover did no work")
}

// TestMaint_allPhasesNoWork_quiet_shouldReturnExitNoChanges verifies --quiet does not change the aggregate exit code.
func TestMaint_allPhasesNoWork_quiet_shouldReturnExitNoChanges(t *testing.T) {
	runtimeDir := filepath.Join(t.TempDir(), "runtime")
	require.NoError(t, os.MkdirAll(runtimeDir, 0o755))
	t.Setenv(config.EnvRuntimeDir, runtimeDir)

	stateDir := filepath.Join(t.TempDir(), "state")
	require.NoError(t, os.MkdirAll(stateDir, 0o755))
	t.Setenv(config.EnvStateDir, stateDir)

	src := t.TempDir()
	dst := t.TempDir()

	cfg := writeTempConfig(t, `
mounts:
  media:
    mountpoint: "/mnt/pfs/media"
    storage_paths:
      - id: "ssd1"
        path: "`+src+`"
        indexed: false
      - id: "hdd1"
        path: "`+dst+`"
        indexed: true
    routing_rules:
      - match: "**"
        targets: ["ssd1"]
    mover:
      enabled: false
`)

	code, stdout, stderr := runCLI(t, []string{"--config", cfg, "maint", "media", "--index=touch", "--quiet"})
	require.Equal(t, ExitNoChanges, code, "expected exit_code=%d got=%d stderr=%s", ExitNoChanges, code, stderr)
	require.Empty(t, stderr)
	require.Empty(t, stdout)
}
