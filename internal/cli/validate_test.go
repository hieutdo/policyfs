package cli

import (
	"strings"
	"testing"

	"github.com/stretchr/testify/require"
)

// TestMount_InvalidMountNames_returnsUsage verifies mount name validation is enforced via the real CLI flow.
func TestMount_InvalidMountNames_returnsUsage(t *testing.T) {
	tests := []struct {
		name      string
		args      []string
		wantCause string
	}{
		{
			name:      "invalid contains asterisk",
			args:      []string{"mount", "bad*name"},
			wantCause: "invalid mount name",
		},
		{
			name:      "invalid contains slash",
			args:      []string{"mount", "bad/name"},
			wantCause: "invalid mount name",
		},
		{
			name:      "invalid contains space",
			args:      []string{"mount", "bad name"},
			wantCause: "invalid mount name",
		},
		{
			name:      "invalid too long",
			args:      []string{"mount", "a1234567890123456789012345678901234567890123456789012345678901234"},
			wantCause: "invalid mount name",
		},
	}

	for _, tt := range tests {
		t.Run(tt.name, func(t *testing.T) {
			code, _, stderr := runCLI(t, tt.args)
			require.Equal(t, ExitUsage, code)
			require.Contains(t, stderr, "error: invalid arguments")
			require.Contains(t, stderr, "cause: "+tt.wantCause)
			require.Contains(t, stderr, "hint: run 'pfs mount --help'")
		})
	}
}

// TestMount_InvalidMountNameStartingWithHyphen verifies `--` allows passing hyphen-prefixed mount names as args.
func TestMount_InvalidMountNameStartingWithHyphen(t *testing.T) {
	code, _, stderr := runCLI(t, []string{"mount", "--", "-media"})
	require.Equal(t, ExitUsage, code)
	require.Contains(t, stderr, "cause: invalid mount name")
}

// TestDoctor_Text_IssueOrderIsDeterministic verifies `pfs doctor` prints issues in a deterministic order.
func TestDoctor_Text_IssueOrderIsDeterministic(t *testing.T) {
	cfg := writeTempConfig(t, `
mounts:
  zzz:
    mountpoint: ""
  aaa:
    mountpoint: ""
`)

	code, stdout, stderr := runCLI(t, []string{"--config", cfg, "doctor"})
	require.Equal(t, ExitDoctorFail, code)
	require.Empty(t, stderr)

	idxA := strings.Index(stdout, "config: mount \"aaa\": mountpoint is required\n")
	idxZ := strings.Index(stdout, "config: mount \"zzz\": mountpoint is required\n")
	require.NotEqual(t, -1, idxA)
	require.NotEqual(t, -1, idxZ)
	require.True(t, idxA < idxZ)
	require.Contains(t, stdout, "6 issues.\n")
}

// TestDoctor_MultipleCatchAll_shouldReportIssue verifies config validation rejects multiple catch-all rules.
func TestDoctor_MultipleCatchAll_shouldReportIssue(t *testing.T) {
	cfg := writeTempConfig(t, `
mounts:
  media:
    mountpoint: /mnt/pfs/media
    storage_paths:
      - id: ssd1
        path: /mnt/ssd1/media
    routing_rules:
      - match: '**'
        read_targets: [ssd1]
      - match: '**'
        read_targets: [ssd1]
`)

	code, stdout, stderr := runCLI(t, []string{"--config", cfg, "doctor"})
	require.Equal(t, ExitDoctorFail, code)
	require.Empty(t, stderr)
	require.Contains(t, stdout, "config: mount \"media\": multiple catch-all rules '**'\n")
}
