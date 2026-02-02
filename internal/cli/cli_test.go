package cli

import (
	"testing"

	"github.com/stretchr/testify/require"
)

// TestExecute_UnknownCommand_returnsUsage verifies Execute() treats unknown commands as usage errors.
func TestExecute_UnknownCommand_returnsUsage(t *testing.T) {
	tests := []struct {
		name       string
		args       []string
		wantStderr string
	}{
		{
			name:       "plain unknown command",
			args:       []string{"nope"},
			wantStderr: "error: invalid arguments\ncause: unknown command \"nope\" for \"pfs\"\nhint: run 'pfs --help'\n",
		},
		{
			name:       "unknown command after --config",
			args:       []string{"--config", "/does/not/exist.yaml", "nope"},
			wantStderr: "error: invalid arguments\ncause: unknown command \"nope\" for \"pfs\"\nhint: run 'pfs --help'\n",
		},
		{
			name:       "unknown command after -c=...",
			args:       []string{"-c=/does/not/exist.yaml", "nope"},
			wantStderr: "error: invalid arguments\ncause: unknown command \"nope\" for \"pfs\"\nhint: run 'pfs --help'\n",
		},
	}

	for _, tt := range tests {
		t.Run(tt.name, func(t *testing.T) {
			code, stdout, stderr := runCLI(t, tt.args)
			require.Equal(t, ExitUsage, code)
			require.Empty(t, stdout)
			require.Equal(t, tt.wantStderr, stderr)
		})
	}
}

// TestExecute_InvalidFlag_returnsUsage verifies SetFlagErrorFunc produces proper CLIError.
func TestExecute_InvalidFlag_returnsUsage(t *testing.T) {
	tests := []struct {
		name           string
		args           []string
		wantContains   string
		wantHintPrefix string
	}{
		{
			name:           "unknown root flag",
			args:           []string{"--unknown-flag"},
			wantContains:   "unknown flag: --unknown-flag",
			wantHintPrefix: "run 'pfs --help'",
		},
		{
			name:           "unknown subcommand flag",
			args:           []string{"mount", "--unknown-flag"},
			wantContains:   "unknown flag: --unknown-flag",
			wantHintPrefix: "run 'pfs mount --help'",
		},
		{
			name:           "unknown doctor flag",
			args:           []string{"doctor", "--unknown"},
			wantContains:   "unknown flag: --unknown",
			wantHintPrefix: "run 'pfs doctor --help'",
		},
	}

	for _, tt := range tests {
		t.Run(tt.name, func(t *testing.T) {
			code, _, stderr := runCLI(t, tt.args)
			require.Equal(t, ExitUsage, code)
			require.Contains(t, stderr, tt.wantContains)
			require.Contains(t, stderr, tt.wantHintPrefix)
		})
	}
}

// TestExecute_Help_returnsOK verifies help flags work correctly.
func TestExecute_Help_returnsOK(t *testing.T) {
	code, stdout, _ := runCLI(t, []string{"--help"})
	require.Equal(t, ExitOK, code)
	require.Contains(t, stdout, "PolicyFS")
}
