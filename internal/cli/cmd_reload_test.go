package cli

import (
	"context"
	"errors"
	"os"
	"path/filepath"
	"testing"

	"github.com/hieutdo/policyfs/internal/config"
	"github.com/hieutdo/policyfs/internal/daemonctl"
	"github.com/rs/zerolog"
	"github.com/stretchr/testify/require"
)

// stubReloadServer implements the daemonctl providers needed for the reload socket.
type stubReloadServer struct {
	changed bool
	fields  []string
	err     error
}

// OpenCounts is unused by these tests.
func (s *stubReloadServer) OpenCounts(_ context.Context, _ []daemonctl.OpenFileID) ([]daemonctl.OpenStat, error) {
	return nil, nil
}

// Reload returns the configured result.
func (s *stubReloadServer) Reload(_ context.Context, configPath string) (bool, []string, error) {
	if configPath == "" {
		return false, nil, errors.New("config path is required")
	}
	if s.err != nil {
		return false, nil, s.err
	}
	return s.changed, s.fields, nil
}

// setShortRuntimeDir sets PFS_RUNTIME_DIR to a short /tmp path (macOS unix socket path limit).
func setShortRuntimeDir(t *testing.T) string {
	t.Helper()

	dir, err := os.MkdirTemp("/tmp", "pfs-runtime")
	require.NoError(t, err)
	old, had := os.LookupEnv(config.EnvRuntimeDir)
	require.NoError(t, os.Setenv(config.EnvRuntimeDir, dir))
	t.Cleanup(func() {
		_ = os.RemoveAll(dir)
		if had {
			_ = os.Setenv(config.EnvRuntimeDir, old)
			return
		}
		_ = os.Unsetenv(config.EnvRuntimeDir)
	})
	return dir
}

// TestReload_invalidArgs_shouldReturnUsage verifies argument validation.
func TestReload_invalidArgs_shouldReturnUsage(t *testing.T) {
	setShortRuntimeDir(t)

	code, _, stderr := runCLI(t, []string{"reload"})
	require.Equal(t, ExitUsage, code)
	require.Contains(t, stderr, "error: invalid arguments")
}

// TestReload_invalidMountName_shouldReturnUsage verifies mount name validation.
func TestReload_invalidMountName_shouldReturnUsage(t *testing.T) {
	setShortRuntimeDir(t)

	code, _, stderr := runCLI(t, []string{"reload", "!!!"})
	require.Equal(t, ExitUsage, code)
	require.Contains(t, stderr, "error: invalid arguments")
}

// TestReload_configMissing_shouldReturnConfigLoadError verifies missing config surfaces correctly.
func TestReload_configMissing_shouldReturnConfigLoadError(t *testing.T) {
	setShortRuntimeDir(t)

	cfgPath := filepath.Join(t.TempDir(), "missing.yaml")
	code, _, stderr := runCLI(t, []string{"--config", cfgPath, "reload", "media"})
	require.Equal(t, ExitFail, code)
	require.Contains(t, stderr, "error: config file not found")
}

// TestReload_noDaemon_shouldReturnConnectError verifies dial errors are mapped to a helpful CLI error.
func TestReload_noDaemon_shouldReturnConnectError(t *testing.T) {
	setShortRuntimeDir(t)

	cfgPath := writeTempConfig(t, `mounts:
  media:
    mountpoint: /mnt/unused
    storage_paths:
      - id: ssd1
        path: /tmp
`)

	code, _, stderr := runCLI(t, []string{"--config", cfgPath, "reload", "media"})
	require.Equal(t, ExitFail, code)
	require.Contains(t, stderr, "error: failed to connect to daemon")
}

// TestReload_remoteError_shouldReturnReloadFailed verifies daemon errors are surfaced to users.
func TestReload_remoteError_shouldReturnReloadFailed(t *testing.T) {
	runtimeDir := setShortRuntimeDir(t)

	cfgPath := writeTempConfig(t, `mounts:
  media:
    mountpoint: /mnt/unused
    storage_paths:
      - id: ssd1
        path: /tmp
`)

	sockPath := filepath.Join(runtimeDir, "media", "daemon.sock")

	ctx := t.Context()

	provider := &stubReloadServer{err: errors.New("reload requires restart: mountpoint changed")}
	srv, err := daemonctl.StartServer(ctx, sockPath, provider, zerolog.Nop())
	require.NoError(t, err)
	defer func() { _ = srv.Close() }()

	code, _, stderr := runCLI(t, []string{"--config", cfgPath, "reload", "media"})
	require.Equal(t, ExitFail, code)
	require.Contains(t, stderr, "error: reload failed")
	require.Contains(t, stderr, "cause: reload requires restart")
}

// TestReload_noChanges_shouldExit3 verifies reload no-ops return exit code 3 and are silent.
func TestReload_noChanges_shouldExit3(t *testing.T) {
	runtimeDir := setShortRuntimeDir(t)

	cfgPath := writeTempConfig(t, `mounts:
  media:
    mountpoint: /mnt/unused
    storage_paths:
      - id: ssd1
        path: /tmp
`)

	sockPath := filepath.Join(runtimeDir, "media", "daemon.sock")

	ctx := t.Context()

	provider := &stubReloadServer{changed: false}
	srv, err := daemonctl.StartServer(ctx, sockPath, provider, zerolog.Nop())
	require.NoError(t, err)
	defer func() { _ = srv.Close() }()

	code, stdout, stderr := runCLI(t, []string{"--config", cfgPath, "reload", "media"})
	require.Equal(t, ExitNoChanges, code)
	require.Contains(t, stdout, "reload no changes")
	require.Equal(t, "", stderr)
}

// TestReload_changed_shouldExit0 verifies successful changes exit 0 and are silent.
func TestReload_changed_shouldExit0(t *testing.T) {
	runtimeDir := setShortRuntimeDir(t)

	cfgPath := writeTempConfig(t, `mounts:
  media:
    mountpoint: /mnt/unused
    storage_paths:
      - id: ssd1
        path: /tmp
`)

	sockPath := filepath.Join(runtimeDir, "media", "daemon.sock")

	ctx := t.Context()

	provider := &stubReloadServer{changed: true, fields: []string{"mounts.media.routing_rules"}}
	srv, err := daemonctl.StartServer(ctx, sockPath, provider, zerolog.Nop())
	require.NoError(t, err)
	defer func() { _ = srv.Close() }()

	code, stdout, stderr := runCLI(t, []string{"--config", cfgPath, "reload", "media"})
	require.Equal(t, ExitOK, code)
	require.Contains(t, stdout, "reload applied")
	require.Contains(t, stdout, "\"changed_fields\"")
	require.Equal(t, "", stderr)
}
