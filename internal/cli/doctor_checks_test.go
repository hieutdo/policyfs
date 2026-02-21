package cli

import (
	"bytes"
	"context"
	"os"
	"path/filepath"
	"strings"
	"testing"
	"time"

	"github.com/hieutdo/policyfs/internal/config"
	"github.com/hieutdo/policyfs/internal/eventlog"
	"github.com/hieutdo/policyfs/internal/lock"
	"github.com/stretchr/testify/require"
)

// --- checkConfigLoaded ---

func TestCheckConfigLoaded_OK(t *testing.T) {
	c := checkConfigLoaded("/etc/pfs/pfs.yaml", nil)
	require.True(t, c.Pass)
	require.Contains(t, c.Name, "/etc/pfs/pfs.yaml")
}

func TestCheckConfigLoaded_Error(t *testing.T) {
	c := checkConfigLoaded("/bad/path.yaml", os.ErrNotExist)
	require.False(t, c.Pass)
	require.NotEmpty(t, c.Detail)
}

// --- configChecksForMount ---

func TestConfigChecksForMount_NoErrors(t *testing.T) {
	checks := configChecksForMount("media", nil)
	require.Len(t, checks, 1)
	require.True(t, checks[0].Pass)
	require.Contains(t, checks[0].Name, "media")
	require.Contains(t, checks[0].Name, "config valid")
}

func TestConfigChecksForMount_WithErrors(t *testing.T) {
	errs := []error{
		&usageError{err: os.ErrNotExist}, // unrelated error
		&mountConfigError{Mount: "media", Msg: "mountpoint is required"},
		&mountConfigError{Mount: "media", Msg: "storage_paths must not be empty"},
		&mountConfigError{Mount: "photos", Msg: "routing_rules must not be empty"},
	}
	checks := configChecksForMount("media", errs)
	require.Len(t, checks, 2)
	require.False(t, checks[0].Pass)
	require.Contains(t, checks[0].Name, "mountpoint is required")
	require.False(t, checks[1].Pass)
	require.Contains(t, checks[1].Name, "storage_paths must not be empty")
}

// --- checkMountpointAccessible ---

func TestCheckMountpointAccessible_Exists(t *testing.T) {
	dir := t.TempDir()
	c := checkMountpointAccessible(dir)
	require.True(t, c.Pass)
	require.Contains(t, c.Detail, "exists")
}

func TestCheckMountpointAccessible_NotFound(t *testing.T) {
	c := checkMountpointAccessible("/does/not/exist")
	require.False(t, c.Pass)
	require.Contains(t, c.Detail, "not found")
}

func TestCheckMountpointAccessible_Empty(t *testing.T) {
	c := checkMountpointAccessible("")
	require.False(t, c.Pass)
	require.Contains(t, c.Detail, "not configured")
}

// --- checkStorage ---

func TestCheckStorage_Accessible(t *testing.T) {
	dir := t.TempDir()
	sp := config.StoragePath{ID: "ssd1", Path: dir, Indexed: true}
	r := checkStorage(sp)
	require.True(t, r.Accessible)
	require.Equal(t, "ssd1", r.ID)
	require.True(t, r.Indexed)
	require.True(t, r.TotalBytes > 0)
}

func TestCheckStorage_NotFound(t *testing.T) {
	sp := config.StoragePath{ID: "hdd1", Path: "/does/not/exist"}
	r := checkStorage(sp)
	require.False(t, r.Accessible)
	require.Equal(t, "not found", r.Error)
}

// --- checkDaemonLock / checkJobLock ---

func TestCheckDaemonLock_Free(t *testing.T) {
	runtimeDir := filepath.Join(t.TempDir(), "runtime")
	require.NoError(t, os.MkdirAll(runtimeDir, 0o755))
	t.Setenv(config.EnvRuntimeDir, runtimeDir)

	c := checkDaemonLock("testmount")
	require.True(t, c.Pass)
	require.Contains(t, c.Detail, "not running")
}

func TestCheckDaemonLock_Busy(t *testing.T) {
	runtimeDir := filepath.Join(t.TempDir(), "runtime")
	require.NoError(t, os.MkdirAll(runtimeDir, 0o755))
	t.Setenv(config.EnvRuntimeDir, runtimeDir)

	// Acquire lock to simulate daemon running.
	lk, err := lock.AcquireMountLock("testmount", config.DefaultDaemonLockFile)
	require.NoError(t, err)
	t.Cleanup(func() { _ = lk.Close() })

	c := checkDaemonLock("testmount")
	require.True(t, c.Pass) // busy is informational, not a failure
	require.Contains(t, c.Detail, "running")
}

func TestCheckJobLock_Free(t *testing.T) {
	runtimeDir := filepath.Join(t.TempDir(), "runtime")
	require.NoError(t, os.MkdirAll(runtimeDir, 0o755))
	t.Setenv(config.EnvRuntimeDir, runtimeDir)

	c := checkJobLock("testmount")
	require.True(t, c.Pass)
	require.Contains(t, c.Detail, "free")
}

func TestCheckJobLock_Busy(t *testing.T) {
	runtimeDir := filepath.Join(t.TempDir(), "runtime")
	require.NoError(t, os.MkdirAll(runtimeDir, 0o755))
	t.Setenv(config.EnvRuntimeDir, runtimeDir)

	lk, err := lock.AcquireMountLock("testmount", config.DefaultJobLockFile)
	require.NoError(t, err)
	t.Cleanup(func() { _ = lk.Close() })

	c := checkJobLock("testmount")
	require.True(t, c.Pass)
	require.Contains(t, c.Detail, "busy")
}

// --- countPendingEvents ---

func TestCountPendingEvents_NoEvents(t *testing.T) {
	stateDir := t.TempDir()
	t.Setenv(config.EnvStateDir, stateDir)

	pe, err := countPendingEvents("testmount", 100)
	require.NoError(t, err)
	require.Nil(t, pe)
}

func TestCountPendingEvents_WithEvents(t *testing.T) {
	stateDir := t.TempDir()
	t.Setenv(config.EnvStateDir, stateDir)

	mount := "testmount"
	ts := time.Now().Unix()

	// Write some events.
	require.NoError(t, eventlog.Append(context.TODO(), mount, eventlog.DeleteEvent{Type: eventlog.TypeDelete, StorageID: "hdd1", Path: "a.txt", TS: ts}))
	require.NoError(t, eventlog.Append(context.TODO(), mount, eventlog.RenameEvent{Type: eventlog.TypeRename, StorageID: "hdd1", OldPath: "b.txt", NewPath: "c.txt", TS: ts}))
	require.NoError(t, eventlog.Append(context.TODO(), mount, eventlog.DeleteEvent{Type: eventlog.TypeDelete, StorageID: "hdd1", Path: "d.txt", TS: ts}))

	// Offset at 0 → all 3 events should be pending.
	pe, err := countPendingEvents(mount, 100)
	require.NoError(t, err)
	require.NotNil(t, pe)
	require.Equal(t, 3, pe.Total)
	require.Equal(t, 2, pe.ByType[eventlog.TypeDelete])
	require.Equal(t, 1, pe.ByType[eventlog.TypeRename])
	require.Len(t, pe.Recent, 3)
}

// --- analyzeDiskAccess ---

func TestAnalyzeDiskAccess_NoLog(t *testing.T) {
	da, err := analyzeDiskAccess("/does/not/exist.log", 100)
	require.NoError(t, err)
	require.Nil(t, da)
}

func TestAnalyzeDiskAccess_WithEntries(t *testing.T) {
	logFile := filepath.Join(t.TempDir(), "pfs.log")
	lines := []string{
		`{"level":"info","msg":"disk_access","op":"open","storage_id":"hdd1","path":"movie.mkv","caller_pid":1234,"caller_name":"jellyfin"}`,
		`{"level":"info","msg":"disk_access","op":"open","storage_id":"hdd1","path":"show.mkv","caller_pid":1234,"caller_name":"jellyfin"}`,
		`{"level":"info","msg":"disk_access","op":"open","storage_id":"hdd2","path":"photo.jpg","caller_pid":5678,"caller_name":"plex"}`,
		`{"level":"info","msg":"some_other_event"}`,
	}
	var content strings.Builder
	for _, l := range lines {
		content.WriteString(l + "\n")
	}
	require.NoError(t, os.WriteFile(logFile, []byte(content.String()), 0o644))

	da, err := analyzeDiskAccess(logFile, 100)
	require.NoError(t, err)
	require.NotNil(t, da)

	// 3 disk_access entries found.
	require.NotEmpty(t, da.TopProcesses)
	require.Equal(t, 2, da.TopProcesses[0].Count) // jellyfin: 2
	require.Contains(t, da.TopProcesses[0].Label, "jellyfin")

	require.NotEmpty(t, da.TopStorages)
	require.Equal(t, "hdd1", da.TopStorages[0].Label) // hdd1: 2
	require.Equal(t, 2, da.TopStorages[0].Count)
}

// --- tailFile ---

func TestTailFile_LastNLines(t *testing.T) {
	content := "line1\nline2\nline3\nline4\nline5\n"
	r := bytes.NewReader([]byte(content))
	lines := tailFile(r, 3)
	require.Equal(t, []string{"line3", "line4", "line5"}, lines)
}

func TestTailFile_FewerThanN(t *testing.T) {
	content := "a\nb\n"
	r := bytes.NewReader([]byte(content))
	lines := tailFile(r, 10)
	require.Equal(t, []string{"a", "b"}, lines)
}

// TestRedundantSystemdTimers_shouldRequireMaintActive verifies redundancy is only reported when maint timer is active.
func TestRedundantSystemdTimers_shouldRequireMaintActive(t *testing.T) {
	t.Run("should_return_nil_when_maint_inactive", func(t *testing.T) {
		timers := []systemdTimerReport{
			{Unit: "pfs-maint@media.timer", UnitFileState: "enabled", ActiveState: "inactive"},
			{Unit: "pfs-index@media.timer", UnitFileState: "enabled", ActiveState: "inactive"},
			{Unit: "pfs-prune@media.timer", UnitFileState: "enabled", ActiveState: "inactive"},
		}
		require.Nil(t, redundantSystemdTimers(timers))
	})

	t.Run("should_return_redundant_when_maint_active", func(t *testing.T) {
		timers := []systemdTimerReport{
			{Unit: "pfs-maint@media.timer", UnitFileState: "enabled", ActiveState: "active"},
			{Unit: "pfs-index@media.timer", UnitFileState: "enabled", ActiveState: "inactive"},
			{Unit: "pfs-prune@media.timer", UnitFileState: "enabled", ActiveState: "inactive"},
		}
		require.Equal(t, []string{"pfs-index@media.timer", "pfs-prune@media.timer"}, redundantSystemdTimers(timers))
	})
}

// --- generateSuggestions ---

func TestGenerateSuggestions_InaccessibleStorage(t *testing.T) {
	report := &doctorReport{
		Mounts: []mountReport{
			{
				Name:        "media",
				ConfigValid: true,
				Storages: []storageReport{
					{ID: "hdd1", Path: "/mnt/hdd1", Accessible: false},
				},
			},
		},
	}

	suggestions := generateSuggestions(report)
	require.Len(t, suggestions, 1)
	require.Contains(t, suggestions[0], "hdd1")
	require.Contains(t, suggestions[0], "not accessible")
}

func TestGenerateSuggestions_InvalidConfig(t *testing.T) {
	report := &doctorReport{
		Mounts: []mountReport{
			{Name: "photos", ConfigValid: false},
		},
	}

	suggestions := generateSuggestions(report)
	require.Len(t, suggestions, 1)
	require.Contains(t, suggestions[0], "fix config errors")
}

// TestGenerateSuggestions_RedundantSystemdTimers verifies doctor suggests disabling redundant timers when maint timer is enabled.
func TestGenerateSuggestions_RedundantSystemdTimers(t *testing.T) {
	report := &doctorReport{
		Mounts: []mountReport{
			{
				Name:        "media",
				ConfigValid: true,
				SystemdTimers: &systemdTimersReport{
					Supported: true,
					Redundant: []string{
						"pfs-index@media.timer",
						"pfs-move@media.timer",
					},
				},
			},
		},
	}

	suggestions := generateSuggestions(report)
	require.Len(t, suggestions, 1)
	require.Contains(t, suggestions[0], "disable redundant timers")
	require.Contains(t, suggestions[0], "systemctl disable --now")
	require.Contains(t, suggestions[0], "pfs-index@media.timer")
	require.Contains(t, suggestions[0], "pfs-move@media.timer")
}

// --- printDoctorReport ---

func TestPrintDoctorReport_AllPassed(t *testing.T) {
	var buf bytes.Buffer
	report := doctorReport{
		ConfigPath: "/etc/pfs/pfs.yaml",
		ConfigChecks: []checkResult{
			{Name: "Config loaded: /etc/pfs/pfs.yaml", Pass: true},
			{Name: `Mount "media": config valid`, Pass: true},
		},
	}
	printDoctorReport(&buf, report)
	out := buf.String()
	require.Contains(t, out, "\u2713 Config loaded")
	require.Contains(t, out, "\u2713 Mount \"media\": config valid")
	require.Contains(t, out, "All checks passed.")
}

func TestPrintDoctorReport_WithIssues(t *testing.T) {
	var buf bytes.Buffer
	report := doctorReport{
		ConfigPath: "/etc/pfs/pfs.yaml",
		ConfigChecks: []checkResult{
			{Name: "Config loaded: /etc/pfs/pfs.yaml", Pass: true},
			{Name: `Mount "media": mountpoint is required`, Pass: false},
		},
		IssueCount: 1,
	}
	printDoctorReport(&buf, report)
	out := buf.String()
	require.Contains(t, out, "\u2717 Mount \"media\": mountpoint is required")
	require.Contains(t, out, "1 issue")
}

// TestPrintDoctorReport_FilesAndTimers verifies doctor prints per-mount file paths and systemd timer schedule.
func TestPrintDoctorReport_FilesAndTimers(t *testing.T) {
	var buf bytes.Buffer
	report := doctorReport{
		Mounts: []mountReport{
			{
				Name:        "media",
				ConfigValid: true,
				Daemon:      checkResult{Name: "daemon", Pass: true, Detail: "running"},
				Mountpoint:  checkResult{Name: "mountpoint", Pass: true, Detail: "/mnt/pfs/media (exists)"},
				JobLock:     checkResult{Name: "job lock", Pass: true, Detail: "free"},
				IndexDB:     fileReport{Path: "/var/lib/pfs/media/index.db", Missing: true},
				LogFile:     fileReport{Path: "/var/log/pfs/pfs.log", Missing: true},
				SystemdTimers: &systemdTimersReport{
					Supported: true,
					Timers: []systemdTimerReport{
						{Unit: "pfs-maint@media.timer", UnitFileState: "enabled", ActiveState: "active", OnCalendar: "hourly"},
					},
					Redundant: []string{"pfs-index@media.timer"},
				},
			},
		},
	}

	printDoctorReport(&buf, report)
	out := buf.String()
	require.Contains(t, out, "Mount: media")
	require.Contains(t, out, "  Files")
	require.Contains(t, out, "index db:")
	require.Contains(t, out, "(missing)")
	require.Contains(t, out, "log:")
	require.Contains(t, out, "  Systemd Timers")
	require.Contains(t, out, "pfs-maint@media.timer")
	require.Contains(t, out, "schedule=hourly")
	require.Contains(t, out, "warning: maint timer active")
}

// TestParseSystemdEnvironmentValue verifies systemd Environment parsing for daemon log file overrides.
func TestParseSystemdEnvironmentValue(t *testing.T) {
	t.Run("should_parse_quoted", func(t *testing.T) {
		got := parseSystemdEnvironmentValue(`PATH=/usr/bin PFS_LOG_FILE="/var/log/pfs/daemon.log"`, config.EnvLogFile)
		require.Equal(t, "/var/log/pfs/daemon.log", got)
	})

	t.Run("should_parse_unquoted", func(t *testing.T) {
		got := parseSystemdEnvironmentValue(`PFS_LOG_FILE=/var/log/pfs/daemon.log`, config.EnvLogFile)
		require.Equal(t, "/var/log/pfs/daemon.log", got)
	})
}

// TestParseSystemdExecStartFlagValue verifies systemd ExecStart parsing for --log-file.
func TestParseSystemdExecStartFlagValue(t *testing.T) {
	t.Run("should_parse_space_form", func(t *testing.T) {
		got := parseSystemdExecStartFlagValue(`/usr/bin/pfs mount media --log-file /var/log/pfs/daemon.log`, "--log-file")
		require.Equal(t, "/var/log/pfs/daemon.log", got)
	})

	t.Run("should_parse_equals_form", func(t *testing.T) {
		got := parseSystemdExecStartFlagValue(`/usr/bin/pfs mount media --log-file=/var/log/pfs/daemon.log`, "--log-file")
		require.Equal(t, "/var/log/pfs/daemon.log", got)
	})

	t.Run("should_parse_quoted", func(t *testing.T) {
		got := parseSystemdExecStartFlagValue(`/usr/bin/pfs mount media --log-file "/var/log/pfs/daemon.log"`, "--log-file")
		require.Equal(t, "/var/log/pfs/daemon.log", got)
	})
}

// --- humanizeDuration ---

func TestHumanizeDuration(t *testing.T) {
	tests := []struct {
		d    time.Duration
		want string
	}{
		{30 * time.Second, "just now"},
		{42 * time.Minute, "42m"},
		{2 * time.Hour, "2h"},
		{2*time.Hour + 30*time.Minute, "2h 30m"},
		{14*time.Hour + 22*time.Minute, "14h 22m"},
		{24 * time.Hour, "1d"},
		{3*24*time.Hour + 14*time.Hour, "3d 14h"},
		{7 * 24 * time.Hour, "1w"},
		{14*24*time.Hour + 3*24*time.Hour, "2w 3d"},
		{30 * 24 * time.Hour, "1mo"},
		{65 * 24 * time.Hour, "2mo 5d"},
		{365 * 24 * time.Hour, "1y"},
		{395 * 24 * time.Hour, "1y 1mo"},
	}
	for _, tt := range tests {
		t.Run(tt.want, func(t *testing.T) {
			got := humanizeDuration(tt.d)
			require.Equal(t, tt.want, got)
		})
	}
}

// --- printIndexStats with stale ---

func TestPrintIndexStats_WithStale(t *testing.T) {
	var buf bytes.Buffer
	stale := int64(1203)
	files := int64(42000)
	idx := indexStatsReport{
		StorageID:  "hdd1",
		FileCount:  &files,
		StaleFiles: &stale,
	}
	printIndexStats(&buf, idx)
	out := buf.String()
	require.Contains(t, out, "files:")
	require.Contains(t, out, "stale:")
	require.Contains(t, out, "1,203")
}

func TestPrintIndexStats_NoStale(t *testing.T) {
	var buf bytes.Buffer
	files := int64(100)
	idx := indexStatsReport{
		StorageID: "hdd1",
		FileCount: &files,
	}
	printIndexStats(&buf, idx)
	out := buf.String()
	require.NotContains(t, out, "stale:")
}

// --- checkDaemonLock with PID ---

func TestCheckDaemonLock_BusyWithPID(t *testing.T) {
	runtimeDir := filepath.Join(t.TempDir(), "runtime")
	require.NoError(t, os.MkdirAll(runtimeDir, 0o755))
	t.Setenv(config.EnvRuntimeDir, runtimeDir)

	// Acquire lock — now writes PID.
	lk, err := lock.AcquireMountLock("testmount", config.DefaultDaemonLockFile)
	require.NoError(t, err)
	t.Cleanup(func() { _ = lk.Close() })

	c := checkDaemonLock("testmount")
	require.True(t, c.Pass)
	require.Contains(t, c.Detail, "running")
	require.Contains(t, c.Detail, "pid")
}
