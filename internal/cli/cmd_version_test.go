package cli

import (
	"encoding/json"
	"strings"
	"testing"

	"github.com/stretchr/testify/require"
)

// TestVersion_Text verifies `pfs version` outputs human-readable text.
func TestVersion_Text(t *testing.T) {
	code, stdout, _ := runCLI(t, []string{"version"})
	require.Equal(t, ExitOK, code)
	require.NotEmpty(t, stdout)
	// Should contain version info
	require.True(t, strings.Contains(stdout, "pfs"))
	require.True(t, strings.Contains(stdout, "commit"))
	require.True(t, strings.Contains(stdout, "built"))
	require.True(t, strings.Contains(stdout, "go version"))
	require.True(t, strings.Contains(stdout, "platform"))
}

// TestVersion_JSON verifies `pfs version --json` outputs valid JSON object per spec.
func TestVersion_JSON(t *testing.T) {
	code, stdout, _ := runCLI(t, []string{"version", "--json"})
	require.Equal(t, ExitOK, code)

	var out JSONVersionOutput
	require.NoError(t, json.Unmarshal([]byte(stdout), &out))
	require.Equal(t, "version", out.Command)
	require.True(t, out.OK)
	require.NotEmpty(t, out.Version)
	require.NotEmpty(t, out.Commit)
	require.NotEmpty(t, out.GoVersion)
	require.NotEmpty(t, out.BuildTime)
}

// TestVersion_ShortFlag verifies `pfs version -j` works as alias for --json.
func TestVersion_ShortFlag(t *testing.T) {
	code, stdout, _ := runCLI(t, []string{"version", "-j"})
	require.Equal(t, ExitOK, code)

	var out JSONVersionOutput
	require.NoError(t, json.Unmarshal([]byte(stdout), &out))
	require.Equal(t, "version", out.Command)
	require.True(t, out.OK)
	require.NotEmpty(t, out.Version)
	require.NotEmpty(t, out.Commit)
	require.NotEmpty(t, out.GoVersion)
	require.NotEmpty(t, out.BuildTime)
}
