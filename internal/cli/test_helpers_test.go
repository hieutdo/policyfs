package cli

import (
	"io"
	"os"
	"path/filepath"
	"testing"

	"github.com/stretchr/testify/require"
)

// runCLI runs the CLI with args, capturing stdout/stderr, and returns (exitCode, stdout, stderr).
func runCLI(t *testing.T, args []string) (int, string, string) {
	t.Helper()

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
