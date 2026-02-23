package cli

import (
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
