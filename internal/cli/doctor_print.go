package cli

import (
	"fmt"
	"io"
	"strings"
	"time"

	"github.com/dustin/go-humanize"
	"github.com/hieutdo/policyfs/internal/doctor"
	"github.com/hieutdo/policyfs/internal/eventlog"
	"github.com/hieutdo/policyfs/internal/humanfmt"
)

const (
	checkPass = "\u2713" // ✓
	checkFail = "\u2717" // ✗
)

// printDoctorReport renders the full doctor report to w.
func printDoctorReport(w io.Writer, r doctor.Report) {
	// --- Config section ---
	fmt.Fprintln(w, "Config")
	for _, c := range r.ConfigChecks {
		printCheck(w, c)
	}

	// --- Per-mount sections ---
	for _, m := range r.Mounts {
		fmt.Fprintln(w)
		fmt.Fprintf(w, "Mount: %s\n", m.Name)

		if !m.ConfigValid {
			fmt.Fprintf(w, "  %s Skipped: config validation failed\n", checkFail)
			continue
		}

		// Status
		fmt.Fprintln(w, "  Status")
		printStatusLine(w, m.Daemon)
		printStatusLine(w, m.Mountpoint)
		printStatusLine(w, m.JobLock)

		// Files
		if m.IndexDB.Path != "" || m.LogFile.Path != "" {
			fmt.Fprintln(w, "  Files")
			if m.IndexDB.Path != "" {
				printFileLine(w, "index db", m.IndexDB)
			}
			if m.LogFile.Path != "" {
				printFileLine(w, "log", m.LogFile)
			}
		}

		// Systemd timers
		if m.SystemdTimers != nil {
			printSystemdTimers(w, m.SystemdTimers)
		}

		// Storages
		if len(m.Storages) > 0 {
			fmt.Fprintln(w, "  Storages")
			for _, s := range m.Storages {
				printStorageLine(w, s)
			}
		}

		// Index
		for _, idx := range m.IndexStats {
			fmt.Fprintf(w, "  Index (%s)\n", idx.StorageID)
			printIndexStats(w, idx)
		}

		// Pending Events
		if m.PendingEvents != nil {
			printPendingEvents(w, m.PendingEvents)
		}

		// Disk Access
		if m.DiskAccess != nil {
			printDiskAccess(w, m.DiskAccess)
		}
	}

	// --- Suggestions ---
	if len(r.Suggestions) > 0 {
		fmt.Fprintln(w)
		fmt.Fprintf(w, "Suggestions (%d):\n", len(r.Suggestions))
		for _, s := range r.Suggestions {
			fmt.Fprintf(w, "  - %s\n", s)
		}
	}

	// --- Summary line ---
	fmt.Fprintln(w)
	parts := []string{}
	if r.IssueCount > 0 {
		parts = append(parts, fmt.Sprintf("%d %s", r.IssueCount, pluralize(r.IssueCount, "issue", "issues")))
	}
	if len(r.Suggestions) > 0 {
		parts = append(parts, fmt.Sprintf("%d %s", len(r.Suggestions), pluralize(len(r.Suggestions), "suggestion", "suggestions")))
	}
	if len(parts) == 0 {
		fmt.Fprintln(w, "All checks passed.")
	} else {
		fmt.Fprintf(w, "%s.\n", joinParts(parts))
	}
}

// printCheck prints one ✓ or ✗ check line.
func printCheck(w io.Writer, c doctor.CheckResult) {
	mark := checkPass
	if !c.Pass {
		mark = checkFail
	}
	if c.Detail != "" {
		fmt.Fprintf(w, "  %s %s: %s\n", mark, c.Name, c.Detail)
	} else {
		fmt.Fprintf(w, "  %s %s\n", mark, c.Name)
	}
}

// printStatusLine prints a status key/value line.
func printStatusLine(w io.Writer, c doctor.CheckResult) {
	detail := c.Detail
	if detail == "" {
		detail = "unknown"
	}
	fmt.Fprintf(w, "    %-12s %s\n", c.Name+":", detail)
}

// printStorageLine prints one storage accessibility line.
func printStorageLine(w io.Writer, s doctor.StorageReport) {
	indexed := ""
	if s.Indexed {
		indexed = " (indexed)"
	}

	if !s.Accessible {
		fmt.Fprintf(w, "    %-8s %-30s %s%s\n", s.ID, s.Path, s.Error, indexed)
		return
	}

	free := humanize.IBytes(s.FreeBytes)
	pct := 100 - s.UsedPct
	fmt.Fprintf(w, "    %-8s %-30s accessible  %s free (%d%%)%s\n", s.ID, s.Path, free, pct, indexed)
}

// printFileLine prints a single file path plus best-effort size/availability.
func printFileLine(w io.Writer, label string, f doctor.FileReport) {
	if f.Missing {
		fmt.Fprintf(w, "    %-12s %s (missing)\n", label+":", f.Path)
		return
	}
	if f.StatError != "" {
		fmt.Fprintf(w, "    %-12s %s (stat error: %s)\n", label+":", f.Path, f.StatError)
		return
	}
	if f.SizeBytes != nil {
		fmt.Fprintf(w, "    %-12s %s (%s)\n", label+":", f.Path, humanize.IBytes(uint64(*f.SizeBytes)))
		return
	}
	fmt.Fprintf(w, "    %-12s %s\n", label+":", f.Path)
}

// printSystemdTimers prints best-effort timer schedule info for maintenance jobs.
func printSystemdTimers(w io.Writer, s *doctor.SystemdTimersReport) {
	fmt.Fprintln(w, "  Systemd Timers")
	if !s.Supported {
		detail := "not supported"
		if s.Error != "" {
			detail = s.Error
		}
		fmt.Fprintf(w, "    %s\n", detail)
		return
	}
	if s.Error != "" {
		fmt.Fprintf(w, "    error: %s\n", s.Error)
	}

	for _, t := range s.Timers {
		if t.Error != "" {
			fmt.Fprintf(w, "    %s: error: %s\n", t.Unit, t.Error)
			continue
		}

		parts := []string{}
		if t.UnitFileState != "" {
			parts = append(parts, t.UnitFileState)
		}
		if t.ActiveState != "" {
			parts = append(parts, t.ActiveState)
		}
		if t.OnCalendar != "" {
			parts = append(parts, "schedule="+t.OnCalendar)
		}
		if t.Next != "" {
			parts = append(parts, "next="+t.Next)
		}
		if t.Last != "" {
			parts = append(parts, "last="+t.Last)
		}
		fmt.Fprintf(w, "    %s: %s\n", t.Unit, joinParts(parts))
	}

	if len(s.Redundant) > 0 {
		fmt.Fprintf(w, "    warning: maint timer active; redundant timers also enabled: %s\n", joinParts(s.Redundant))
	}
}

// printIndexStats prints index stats for one storage.
func printIndexStats(w io.Writer, idx doctor.IndexStatsReport) {
	if idx.LastCompleted != nil {
		ago := time.Since(*idx.LastCompleted).Truncate(time.Minute)
		fmt.Fprintf(w, "    last indexed: %s  (%s ago)\n", idx.LastCompleted.Format("2006-01-02 15:04"), humanfmt.HumanizeDuration(ago))
	} else {
		fmt.Fprintln(w, "    last indexed: never")
	}

	if idx.LastDurationMS != nil {
		dur := time.Duration(*idx.LastDurationMS) * time.Millisecond
		fmt.Fprintf(w, "    duration:     %s\n", dur.Round(100*time.Millisecond))
	}

	if idx.FileCount != nil {
		fmt.Fprintf(w, "    files:        %s\n", humanize.Comma(*idx.FileCount))
	}

	if idx.TotalBytes != nil {
		fmt.Fprintf(w, "    size:         %s\n", humanize.IBytes(uint64(*idx.TotalBytes)))
	}

	if idx.StaleFiles != nil && *idx.StaleFiles > 0 {
		fmt.Fprintf(w, "    stale:        %s\n", humanize.Comma(*idx.StaleFiles))
	}
}

// printPendingEvents prints the pending events section.
func printPendingEvents(w io.Writer, p *doctor.PendingEvents) {
	fmt.Fprintln(w, "  Pending Events")

	// Summary line: total (DELETE N, RENAME N, SETATTR N)
	parts := []string{}
	for _, t := range []eventlog.Type{eventlog.TypeDelete, eventlog.TypeRename, eventlog.TypeSetattr} {
		if c, ok := p.ByType[t]; ok && c > 0 {
			parts = append(parts, fmt.Sprintf("%s %d", string(t), c))
		}
	}
	fmt.Fprintf(w, "    total: %d", p.Total)
	if len(parts) > 0 {
		fmt.Fprintf(w, " (%s)", joinParts(parts))
	}
	fmt.Fprintln(w)

	// Recent events
	if len(p.Recent) > 0 {
		fmt.Fprintln(w, "    recent:")
		for _, e := range p.Recent {
			ts := e.TS.Format("2006-01-02 15:04")
			switch e.Type {
			case eventlog.TypeRename:
				fmt.Fprintf(w, "      %-8s storage_id=%-8s old=%s new=%s  %s\n", string(e.Type), e.StorageID, e.OldPath, e.NewPath, ts)
			default:
				fmt.Fprintf(w, "      %-8s storage_id=%-8s path=%s  %s\n", string(e.Type), e.StorageID, e.Path, ts)
			}
		}
	}
}

// printDiskAccess prints the disk access analysis section.
func printDiskAccess(w io.Writer, d *doctor.DiskAccessReport) {
	fmt.Fprintf(w, "  Disk Access (from %s, last %s lines)\n", d.LogPath, humanize.Comma(int64(d.LinesScanned)))

	if len(d.TopProcesses) > 0 {
		fmt.Fprintln(w, "    Top processes:")
		for _, p := range d.TopProcesses {
			fmt.Fprintf(w, "      %-28s %d accesses\n", p.Label, p.Count)
		}
	}

	if len(d.TopStorages) > 0 {
		fmt.Fprintln(w, "    Top storages:")
		for _, s := range d.TopStorages {
			fmt.Fprintf(w, "      %-28s %d accesses\n", s.Label, s.Count)
		}
	}
}

// pluralize returns singular or plural based on count.
func pluralize(n int, singular, plural string) string {
	if n == 1 {
		return singular
	}
	return plural
}

// joinParts joins strings with ", ".
func joinParts(parts []string) string {
	var result strings.Builder
	for i, p := range parts {
		if i > 0 {
			result.WriteString(", ")
		}
		result.WriteString(p)
	}
	return result.String()
}
