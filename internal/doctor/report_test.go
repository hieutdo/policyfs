package doctor

import (
	"bytes"
	"errors"
	"fmt"
	"io"
	"os"
	"path/filepath"
	"strings"
	"syscall"
	"testing"
	"time"

	"github.com/hieutdo/policyfs/internal/config"
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
			Name:                 "media",
			ConfigValid:          true,
			FusePermissionErrors: CheckResult{Name: "fuse perms", Pass: false, Detail: "1 in last 15m"},
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
	foundPerm := false
	for _, item := range s {
		if strings.HasPrefix(item, "Mount \"media\": recent permission errors detected") {
			foundPerm = true
			break
		}
	}
	require.True(t, foundPerm, "expected permission suggestion prefix, got: %#v", s)
	require.Contains(t, s, "Mount \"media\": maint timer is active; disable redundant timers: systemctl disable --now pfs-index@media.timer pfs-move@media.timer")
	require.Contains(t, s, "Mount \"media\": hdd1 (/mnt/hdd1) is not accessible - check disk/mount")
	require.Contains(t, s, "Mount \"media\": ssd1 is 90% full - consider freeing space or adding storage")
	require.Contains(t, s, "Mount \"media\": storage \"hdd1\" has never been indexed - run 'pfs index media'")
}

// TestCheckRecentFusePermissionErrors_shouldApplyWindowAndSessionCutoff verifies we only report
// permission errors that are recent and belong to the current mount session (after the last
// "mount ready" log entry).
func TestCheckRecentFusePermissionErrors_shouldApplyWindowAndSessionCutoff(t *testing.T) {
	dir := t.TempDir()
	logPath := filepath.Join(dir, "pfs.log")

	now := time.Unix(1700000000, 0).UTC()
	oldButWithinWindow := now.Add(-10 * time.Minute)
	sessionStart := now.Add(-1 * time.Minute)
	recent := now.Add(-30 * time.Second)

	lines := []string{
		fmt.Sprintf(`{"time":%q,"level":"error","component":"fuse","mount":"media","msg":"failed to mkdir","error":"open x: permission denied"}`+"\n", oldButWithinWindow.Format(time.RFC3339Nano)),
		fmt.Sprintf(`{"time":%q,"level":"info","mount":"media","msg":"mount ready"}`+"\n", sessionStart.Format(time.RFC3339Nano)),
		fmt.Sprintf(`{"time":%q,"level":"error","component":"fuse","mount":"media","msg":"failed to create","error":"permission denied"}`+"\n", recent.Format(time.RFC3339Nano)),
		fmt.Sprintf(`{"time":%q,"level":"error","component":"fuse","mount":"media","msg":"failed to open","error":"file name too long"}`+"\n", recent.Format(time.RFC3339Nano)),
	}
	require.NoError(t, os.WriteFile(logPath, []byte(strings.Join(lines, "")), 0o644))

	c, err := checkRecentFusePermissionErrors(logPath, "media", 2000, now, 15*time.Minute)
	require.NoError(t, err)
	require.Equal(t, "fuse perms", c.Name)
	require.False(t, c.Pass)
	require.Contains(t, c.Detail, "1 in last")
	// last error was 30s ago (<1min) → format must be "just now", not "just now ago"
	require.Contains(t, c.Detail, "just now")
	require.NotContains(t, c.Detail, "ago")
}

func TestIsPermissionDeniedLogError(t *testing.T) {
	tests := []struct {
		name string
		err  string
		want bool
	}{
		{"empty", "", false},
		{"whitespace", "   ", false},
		{"exact EACCES", syscall.EACCES.Error(), true},
		{"exact EPERM", syscall.EPERM.Error(), true},
		{"wrapped EACCES", "open /mnt/x: " + syscall.EACCES.Error(), true},
		{"wrapped EPERM", "failed to chown: " + syscall.EPERM.Error(), true},
		{"unrelated error", "file name too long", false},
		{"prefix not suffix", syscall.EACCES.Error() + ": unexpected", false},
		{"substring not suffix", "got permission denied somehow", false},
	}
	for _, tt := range tests {
		t.Run(tt.name, func(t *testing.T) {
			require.Equal(t, tt.want, isPermissionDeniedLogError(tt.err))
		})
	}
}

func TestCheckRecentFusePermissionErrors_shouldPassWhenNoErrors(t *testing.T) {
	dir := t.TempDir()
	logPath := filepath.Join(dir, "pfs.log")
	now := time.Unix(1700000000, 0).UTC()
	recent := now.Add(-30 * time.Second)

	lines := []string{
		fmt.Sprintf(`{"time":%q,"level":"info","mount":"media","msg":"mount ready"}`+"\n", recent.Format(time.RFC3339Nano)),
		fmt.Sprintf(`{"time":%q,"level":"error","component":"fuse","mount":"media","msg":"failed to open","error":"file name too long"}`+"\n", recent.Format(time.RFC3339Nano)),
	}
	require.NoError(t, os.WriteFile(logPath, []byte(strings.Join(lines, "")), 0o644))

	c, err := checkRecentFusePermissionErrors(logPath, "media", 2000, now, 15*time.Minute)
	require.NoError(t, err)
	require.Equal(t, "fuse perms", c.Name)
	require.True(t, c.Pass)
}

func TestCheckRecentFusePermissionErrors_shouldReturnEmptyWhenLogMissing(t *testing.T) {
	c, err := checkRecentFusePermissionErrors("/nonexistent/pfs.log", "media", 2000, time.Now(), 15*time.Minute)
	require.NoError(t, err)
	require.Equal(t, "", c.Name)
}

func TestCheckRecentFusePermissionErrors_shouldReturnEmptyWhenPathEmpty(t *testing.T) {
	c, err := checkRecentFusePermissionErrors("", "media", 2000, time.Now(), 15*time.Minute)
	require.NoError(t, err)
	require.Equal(t, "", c.Name)
}

func TestCheckRecentFusePermissionErrors_shouldPassWhenLogEmpty(t *testing.T) {
	dir := t.TempDir()
	logPath := filepath.Join(dir, "pfs.log")
	require.NoError(t, os.WriteFile(logPath, []byte{}, 0o644))

	c, err := checkRecentFusePermissionErrors(logPath, "media", 2000, time.Now(), 15*time.Minute)
	require.NoError(t, err)
	// Empty log → tailFile returns nil → early return with pass.
	require.Equal(t, "fuse perms", c.Name)
	require.True(t, c.Pass)
}

func TestCheckRecentFusePermissionErrors_shouldNotCountOtherMounts(t *testing.T) {
	dir := t.TempDir()
	logPath := filepath.Join(dir, "pfs.log")
	now := time.Unix(1700000000, 0).UTC()
	recent := now.Add(-30 * time.Second)

	lines := []string{
		fmt.Sprintf(`{"time":%q,"level":"error","component":"fuse","mount":"other","msg":"failed to mkdir","error":"permission denied"}`+"\n", recent.Format(time.RFC3339Nano)),
	}
	require.NoError(t, os.WriteFile(logPath, []byte(strings.Join(lines, "")), 0o644))

	c, err := checkRecentFusePermissionErrors(logPath, "media", 2000, now, 15*time.Minute)
	require.NoError(t, err)
	require.True(t, c.Pass)
}

// TestComputePoolSizeBytes_shouldReturnNilWhenUnknown verifies pool size is unknown when there are
// no accessible storages or when any accessible storage lacks TotalBytes.
func TestComputePoolSizeBytes_shouldReturnNilWhenUnknown(t *testing.T) {
	t.Run("should return nil when no accessible", func(t *testing.T) {
		got := computePoolSizeBytes([]StorageReport{{ID: "ssd1", Accessible: false, TotalBytes: 100}})
		require.Nil(t, got)
	})

	t.Run("should return nil when any accessible total is unknown", func(t *testing.T) {
		got := computePoolSizeBytes([]StorageReport{{ID: "ssd1", Accessible: true, TotalBytes: 0}})
		require.Nil(t, got)
	})
}

// TestComputePoolSizeBytes_shouldSumTotals verifies pool size sums TotalBytes across accessible storages.
func TestComputePoolSizeBytes_shouldSumTotals(t *testing.T) {
	got := computePoolSizeBytes([]StorageReport{
		{ID: "ssd1", Accessible: true, TotalBytes: 10},
		{ID: "ssd2", Accessible: true, TotalBytes: 20},
		{ID: "hdd1", Accessible: false, TotalBytes: 999},
	})
	require.NotNil(t, got)
	require.Equal(t, uint64(30), *got)
}

// TestApplyUsageThresholdsForFirstUsageJob_shouldAnnotateSources verifies we annotate only source storages
// of the first usage-triggered mover job and only when storages are accessible.
func TestApplyUsageThresholdsForFirstUsageJob_shouldAnnotateSources(t *testing.T) {
	mountCfg := config.MountConfig{
		StorageGroups: map[string][]string{
			"ssds": {"ssd2"},
		},
		Mover: config.MoverConfig{
			Enabled: boolPtr(true),
			Jobs: []config.MoverJobConfig{
				{Name: "manual", Trigger: config.MoverTriggerConfig{Type: "manual"}},
				{
					Name:    "usage",
					Trigger: config.MoverTriggerConfig{Type: "usage", ThresholdStart: 90, ThresholdStop: 65},
					Source:  config.MoverSourceConfig{Paths: []string{"ssd1"}, Groups: []string{"ssds"}},
				},
			},
		},
	}

	storages := []StorageReport{
		{ID: "ssd1", Accessible: true},
		{ID: "ssd2", Accessible: true},
		{ID: "hdd1", Accessible: true},
		{ID: "ssd3", Accessible: false},
	}

	applyUsageThresholdsForFirstUsageJob(mountCfg, storages)

	require.NotNil(t, storages[0].ThresholdStartPct)
	require.NotNil(t, storages[0].ThresholdStopPct)
	require.Equal(t, 90, *storages[0].ThresholdStartPct)
	require.Equal(t, 65, *storages[0].ThresholdStopPct)

	require.NotNil(t, storages[1].ThresholdStartPct)
	require.NotNil(t, storages[1].ThresholdStopPct)

	require.Nil(t, storages[2].ThresholdStartPct)
	require.Nil(t, storages[2].ThresholdStopPct)

	require.Nil(t, storages[3].ThresholdStartPct)
	require.Nil(t, storages[3].ThresholdStopPct)
}

// TestApplyUsageThresholds_shouldNoopWhenMoverDisabled verifies thresholds are not applied
// when the mover is explicitly disabled.
func TestApplyUsageThresholds_shouldNoopWhenMoverDisabled(t *testing.T) {
	mountCfg := config.MountConfig{
		Mover: config.MoverConfig{
			Enabled: boolPtr(false),
			Jobs: []config.MoverJobConfig{
				{
					Name:    "usage",
					Trigger: config.MoverTriggerConfig{Type: "usage", ThresholdStart: 90, ThresholdStop: 65},
					Source:  config.MoverSourceConfig{Paths: []string{"ssd1"}},
				},
			},
		},
	}

	storages := []StorageReport{{ID: "ssd1", Accessible: true}}
	applyUsageThresholdsForFirstUsageJob(mountCfg, storages)
	require.Nil(t, storages[0].ThresholdStartPct)
}

// TestApplyUsageThresholds_shouldNoopWhenNoUsageJob verifies thresholds are not applied
// when no mover job has trigger.type=usage.
func TestApplyUsageThresholds_shouldNoopWhenNoUsageJob(t *testing.T) {
	mountCfg := config.MountConfig{
		Mover: config.MoverConfig{
			Enabled: boolPtr(true),
			Jobs: []config.MoverJobConfig{
				{Name: "manual", Trigger: config.MoverTriggerConfig{Type: "manual"}},
			},
		},
	}

	storages := []StorageReport{{ID: "ssd1", Accessible: true}}
	applyUsageThresholdsForFirstUsageJob(mountCfg, storages)
	require.Nil(t, storages[0].ThresholdStartPct)
}
