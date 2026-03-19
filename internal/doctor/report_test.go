package doctor

import (
	"bytes"
	"errors"
	"fmt"
	"io"
	"os"
	"path/filepath"
	"strings"
	"testing"
	"time"

	"github.com/stretchr/testify/require"
)

// TestRootCause_shouldReturnInnermostError verifies rootCause unwraps to the innermost error.
func TestRootCause_shouldReturnInnermostError(t *testing.T) {
	inner := errors.New("inner")
	err := fmt.Errorf("lvl2: %w", fmt.Errorf("lvl1: %w", inner))

	got := rootCause(err)
	require.Same(t, inner, got)
}

// TestParseSystemctlShow_shouldParseKeyValuePairs verifies parseSystemctlShow parses key=value lines
// and ignores empty/invalid lines.
func TestParseSystemctlShow_shouldParseKeyValuePairs(t *testing.T) {
	out := []byte("\nA=1\nB=two\ninvalid\nC=3\n\n")
	m := parseSystemctlShow(out)

	require.Equal(t, "1", m["A"])
	require.Equal(t, "two", m["B"])
	require.Equal(t, "3", m["C"])
}

// TestParseSystemdValueToken_shouldHandleQuotedAndUnquoted verifies parseSystemdValueToken handles
// quoted and unquoted tokens and stops at separators.
func TestParseSystemdValueToken_shouldHandleQuotedAndUnquoted(t *testing.T) {
	cases := []struct {
		name string
		in   string
		want string
	}{
		{name: "should parse unquoted", in: "abc def", want: "abc"},
		{name: "should stop at semicolon", in: "abc; rest", want: "abc"},
		{name: "should stop at close brace", in: "abc} rest", want: "abc"},
		{name: "should parse single quoted", in: "'a b' rest", want: "a b"},
		{name: "should parse double quoted", in: "\"a b\" rest", want: "a b"},
		{name: "should return empty on unterminated quote", in: "'nope", want: ""},
		{name: "should trim leading whitespace", in: "\t  abc", want: "abc"},
		{name: "should return empty on empty", in: "   ", want: ""},
	}

	for _, tc := range cases {
		t.Run(tc.name, func(t *testing.T) {
			require.Equal(t, tc.want, parseSystemdValueToken(tc.in))
		})
	}
}

// TestParseSystemdEnvironmentValue_shouldExtractValue verifies parseSystemdEnvironmentValue extracts
// one variable value from the Environment property.
func TestParseSystemdEnvironmentValue_shouldExtractValue(t *testing.T) {
	got := parseSystemdEnvironmentValue("A=1 B=2 PFS_LOG_FILE='/var/log/pfs/pfs.log' C=3", "PFS_LOG_FILE")
	require.Equal(t, "/var/log/pfs/pfs.log", got)

	got = parseSystemdEnvironmentValue("A=1", "PFS_LOG_FILE")
	require.Equal(t, "", got)
}

// TestParseSystemdExecStartFlagValue_shouldExtractFlagValue verifies parseSystemdExecStartFlagValue handles
// both --flag=value and --flag value shapes.
func TestParseSystemdExecStartFlagValue_shouldExtractFlagValue(t *testing.T) {
	cases := []struct {
		name string
		in   string
		want string
	}{
		{name: "should parse equals form", in: "argv[]=/usr/bin/pfs --log-file=/var/log/pfs/pfs.log --x=y", want: "/var/log/pfs/pfs.log"},
		{name: "should parse space form", in: "argv[]=/usr/bin/pfs --log-file /var/log/pfs/pfs.log --x=y", want: "/var/log/pfs/pfs.log"},
		{name: "should parse quoted", in: "argv[]=/usr/bin/pfs --log-file \"/var/log/pfs/pfs.log\";", want: "/var/log/pfs/pfs.log"},
		{name: "should return empty when flag missing", in: "argv[]=/usr/bin/pfs --x=y", want: ""},
	}

	for _, tc := range cases {
		t.Run(tc.name, func(t *testing.T) {
			require.Equal(t, tc.want, parseSystemdExecStartFlagValue(tc.in, "--log-file"))
		})
	}
}

// TestRedundantSystemdTimers_shouldReturnSortedRedundant verifies redundantSystemdTimers returns redundant
// timer units enabled alongside an active maint timer.
func TestRedundantSystemdTimers_shouldReturnSortedRedundant(t *testing.T) {
	timers := []SystemdTimerReport{
		{Unit: "pfs-maint@media.timer", UnitFileState: "enabled", ActiveState: "active"},
		{Unit: "pfs-move@media.timer", UnitFileState: "enabled", ActiveState: "inactive"},
		{Unit: "pfs-index@media.timer", UnitFileState: "enabled", ActiveState: "inactive"},
		{Unit: "pfs-prune@media.timer", UnitFileState: "disabled", ActiveState: "inactive"},
	}

	require.Equal(t, []string{"pfs-index@media.timer", "pfs-move@media.timer"}, redundantSystemdTimers(timers))
}

// TestRedundantSystemdTimers_shouldReturnNilWhenMaintNotActive verifies redundantSystemdTimers returns nil
// when the maint timer is not active.
func TestRedundantSystemdTimers_shouldReturnNilWhenMaintNotActive(t *testing.T) {
	timers := []SystemdTimerReport{
		{Unit: "pfs-maint@media.timer", UnitFileState: "enabled", ActiveState: "inactive"},
		{Unit: "pfs-move@media.timer", UnitFileState: "enabled", ActiveState: "inactive"},
	}

	require.Nil(t, redundantSystemdTimers(timers))
}

// TestCheckFile_shouldReturnMissingOrSize verifies checkFile reports missing for empty/missing paths
// and size for existing files.
func TestCheckFile_shouldReturnMissingOrSize(t *testing.T) {
	t.Run("should mark empty path as missing", func(t *testing.T) {
		r := checkFile("")
		require.True(t, r.Missing)
		require.Nil(t, r.SizeBytes)
	})

	t.Run("should mark non-existent file as missing", func(t *testing.T) {
		dir := t.TempDir()
		r := checkFile(filepath.Join(dir, "nope"))
		require.True(t, r.Missing)
		require.Nil(t, r.SizeBytes)
	})

	t.Run("should return file size", func(t *testing.T) {
		dir := t.TempDir()
		p := filepath.Join(dir, "x")
		require.NoError(t, os.WriteFile(p, []byte("abc"), 0o644))

		r := checkFile(p)
		require.False(t, r.Missing)
		require.NotNil(t, r.SizeBytes)
		require.Equal(t, int64(3), *r.SizeBytes)
	})
}

// errSeekReadSeeker is a ReadSeeker that always fails Seek; used to cover tailFile error paths.
type errSeekReadSeeker struct{}

// Read implements io.Reader.
func (r *errSeekReadSeeker) Read(p []byte) (int, error) { return 0, io.EOF }

// Seek implements io.Seeker.
func (r *errSeekReadSeeker) Seek(offset int64, whence int) (int64, error) {
	return 0, errors.New("seek failed")
}

// TestTailFile_shouldHandleEmptyAndZeroN verifies tailFile returns nil for empty inputs and
// returns an empty slice for n=0.
func TestTailFile_shouldHandleEmptyAndZeroN(t *testing.T) {
	t.Run("should return nil for empty file", func(t *testing.T) {
		r := bytes.NewReader(nil)
		require.Nil(t, tailFile(r, 10))
	})

	t.Run("should return nil when Seek fails", func(t *testing.T) {
		r := &errSeekReadSeeker{}
		require.Nil(t, tailFile(r, 10))
	})

	t.Run("should return empty slice for n=0", func(t *testing.T) {
		r := bytes.NewReader([]byte("a\nb\n"))
		lines := tailFile(r, 0)
		require.NotNil(t, lines)
		require.Len(t, lines, 0)
	})
}

// TestTailFile_shouldReturnLastNLines verifies tailFile returns the last N lines even when
// the file has no trailing newline.
func TestTailFile_shouldReturnLastNLines(t *testing.T) {
	r := bytes.NewReader([]byte("a\nb\nc"))
	require.Equal(t, []string{"b", "c"}, tailFile(r, 2))
}

// TestTailFile_shouldCrossChunkBoundary verifies tailFile works when the file is larger than
// the internal chunk size (multiple reads from end).
func TestTailFile_shouldCrossChunkBoundary(t *testing.T) {
	var b strings.Builder
	for i := 0; i < 6000; i++ {
		_, _ = fmt.Fprintf(&b, "line-%04d\n", i)
	}

	r := bytes.NewReader([]byte(b.String()))
	lines := tailFile(r, 3)
	require.Equal(t, []string{"line-5997", "line-5998", "line-5999"}, lines)
}

// TestGenerateSuggestions_shouldIncludeActionableItems verifies generateSuggestions emits the expected
// suggestions across the major branches.
func TestGenerateSuggestions_shouldIncludeActionableItems(t *testing.T) {
	now := time.Unix(1700000000, 0)
	report := &Report{Mounts: []MountReport{
		{
			Name:        "bad",
			ConfigValid: false,
		},
		{
			Name:        "media",
			ConfigValid: true,
			SystemdTimers: &SystemdTimersReport{
				Supported: true,
				Redundant: []string{"pfs-index@media.timer", "pfs-move@media.timer"},
			},
			Storages: []StorageReport{
				{ID: "hdd1", Path: "/mnt/hdd1", Accessible: false},
				{ID: "ssd1", Path: "/mnt/ssd1", Accessible: true, UsedPct: 90},
			},
			IndexStats: []IndexStatsReport{{StorageID: "hdd1", LastCompleted: nil}, {StorageID: "ssd1", LastCompleted: &now}},
		},
	}}

	s := generateSuggestions(report)
	require.Contains(t, s, "Mount \"bad\": fix config errors above before running doctor again")
	require.Contains(t, s, "Mount \"media\": maint timer is active; disable redundant timers: systemctl disable --now pfs-index@media.timer pfs-move@media.timer")
	require.Contains(t, s, "Mount \"media\": hdd1 (/mnt/hdd1) is not accessible — check disk/mount")
	require.Contains(t, s, "Mount \"media\": ssd1 is 90% full — consider freeing space or adding storage")
	require.Contains(t, s, "Mount \"media\": storage \"hdd1\" has never been indexed — run 'pfs index media'")
}
