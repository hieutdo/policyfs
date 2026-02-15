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

	if cur, ok := os.LookupEnv(config.EnvRuntimeDir); !ok || cur == config.DefaultRuntimeDir || cur == "/workspace/tmp/pfs" || strings.HasPrefix(cur, "/workspace/tmp/pfs/") {
		oldRuntime, hadRuntime := os.LookupEnv(config.EnvRuntimeDir)
		runtimeDir := filepath.Join(t.TempDir(), "runtime")
		require.NoError(t, os.MkdirAll(runtimeDir, 0o755))
		require.NoError(t, os.Setenv(config.EnvRuntimeDir, runtimeDir))
		t.Cleanup(func() {
			if hadRuntime {
				_ = os.Setenv(config.EnvRuntimeDir, oldRuntime)
				return
			}
			_ = os.Unsetenv(config.EnvRuntimeDir)
		})
	}

	if cur, ok := os.LookupEnv(config.EnvStateDir); !ok || cur == config.DefaultStateDir || cur == "/workspace/tmp/pfs" || strings.HasPrefix(cur, "/workspace/tmp/pfs/") {
		oldState, hadState := os.LookupEnv(config.EnvStateDir)
		stateDir := filepath.Join(t.TempDir(), "state")
		require.NoError(t, os.MkdirAll(stateDir, 0o755))
		require.NoError(t, os.Setenv(config.EnvStateDir, stateDir))
		t.Cleanup(func() {
			if hadState {
				_ = os.Setenv(config.EnvStateDir, oldState)
				return
			}
			_ = os.Unsetenv(config.EnvStateDir)
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
