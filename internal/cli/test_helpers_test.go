package cli

import (
	"io"
	"os"
	"path/filepath"
	"strings"
	"testing"

	"github.com/hieutdo/policyfs/internal/config"
	"github.com/stretchr/testify/require"
)

// runCLI runs the CLI with args, capturing stdout/stderr, and returns (exitCode, stdout, stderr).
func runCLI(t *testing.T, args []string) (int, string, string) {
	t.Helper()

	if cur, ok := os.LookupEnv("PFS_RUNTIME_DIR"); !ok || cur == config.DefaultRuntimeDir || cur == "/workspace/tmp/pfs" || strings.HasPrefix(cur, "/workspace/tmp/pfs/") {
		oldRuntime, hadRuntime := os.LookupEnv("PFS_RUNTIME_DIR")
		runtimeDir := filepath.Join(t.TempDir(), "runtime")
		require.NoError(t, os.MkdirAll(runtimeDir, 0o755))
		require.NoError(t, os.Setenv("PFS_RUNTIME_DIR", runtimeDir))
		t.Cleanup(func() {
			if hadRuntime {
				_ = os.Setenv("PFS_RUNTIME_DIR", oldRuntime)
				return
			}
			_ = os.Unsetenv("PFS_RUNTIME_DIR")
		})
	}

	if cur, ok := os.LookupEnv("PFS_STATE_DIR"); !ok || cur == config.DefaultStateDir || cur == "/workspace/tmp/pfs" || strings.HasPrefix(cur, "/workspace/tmp/pfs/") {
		oldState, hadState := os.LookupEnv("PFS_STATE_DIR")
		stateDir := filepath.Join(t.TempDir(), "state")
		require.NoError(t, os.MkdirAll(stateDir, 0o755))
		require.NoError(t, os.Setenv("PFS_STATE_DIR", stateDir))
		t.Cleanup(func() {
			if hadState {
				_ = os.Setenv("PFS_STATE_DIR", oldState)
				return
			}
			_ = os.Unsetenv("PFS_STATE_DIR")
		})
	}

	oldStdout := os.Stdout
	oldStderr := os.Stderr
	defer func() {
		os.Stdout = oldStdout
		os.Stderr = oldStderr
	}()

	rOut, wOut, err := os.Pipe()
	require.NoError(t, err)
	defer func() { _ = rOut.Close() }()

	rErr, wErr, err := os.Pipe()
	if err != nil {
		_ = wOut.Close()
	}
	require.NoError(t, err)
	defer func() { _ = rErr.Close() }()

	os.Stdout = wOut
	os.Stderr = wErr

	code := Execute(args)

	_ = wOut.Close()
	_ = wErr.Close()

	outB, _ := io.ReadAll(rOut)
	errB, _ := io.ReadAll(rErr)
	return code, string(outB), string(errB)
}

// writeTempConfig writes a YAML config file for tests and returns its path.
func writeTempConfig(t *testing.T, content string) string {
	t.Helper()

	p := filepath.Join(t.TempDir(), "pfs.yaml")
	require.NoError(t, os.WriteFile(p, []byte(content), 0o644))

	return p
}
