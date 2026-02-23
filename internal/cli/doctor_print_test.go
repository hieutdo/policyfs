package cli

import (
	"bytes"
	"testing"

	"github.com/hieutdo/policyfs/internal/doctor"
	"github.com/stretchr/testify/require"
)

// --- printDoctorReport ---

func TestPrintDoctorReport_AllPassed(t *testing.T) {
	var buf bytes.Buffer
	report := doctor.Report{
		ConfigPath: "/etc/pfs/pfs.yaml",
		ConfigChecks: []doctor.CheckResult{
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
	report := doctor.Report{
		ConfigPath: "/etc/pfs/pfs.yaml",
		ConfigChecks: []doctor.CheckResult{
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
	report := doctor.Report{
		Mounts: []doctor.MountReport{
			{
				Name:        "media",
				ConfigValid: true,
				Daemon:      doctor.CheckResult{Name: "daemon", Pass: true, Detail: "running"},
				Mountpoint:  doctor.CheckResult{Name: "mountpoint", Pass: true, Detail: "/mnt/pfs/media (exists)"},
				JobLock:     doctor.CheckResult{Name: "job lock", Pass: true, Detail: "free"},
				IndexDB:     doctor.FileReport{Path: "/var/lib/pfs/media/index.db", Missing: true},
				LogFile:     doctor.FileReport{Path: "/var/log/pfs/pfs.log", Missing: true},
				SystemdTimers: &doctor.SystemdTimersReport{
					Supported: true,
					Timers: []doctor.SystemdTimerReport{
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

// --- printIndexStats with stale ---

func TestPrintIndexStats_WithStale(t *testing.T) {
	var buf bytes.Buffer
	stale := int64(1203)
	files := int64(42000)
	idx := doctor.IndexStatsReport{
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
	idx := doctor.IndexStatsReport{
		StorageID: "hdd1",
		FileCount: &files,
	}
	printIndexStats(&buf, idx)
	out := buf.String()
	require.NotContains(t, out, "stale:")
}
