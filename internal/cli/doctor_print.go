package cli

import (
	"fmt"
	"io"
	"strings"
	"time"

	"github.com/dustin/go-humanize"
	"github.com/hieutdo/policyfs/internal/eventlog"
)

const (
	checkPass = "\u2713" // ✓
	checkFail = "\u2717" // ✗
)

// printDoctorReport renders the full doctor report to w.
func printDoctorReport(w io.Writer, r doctorReport) {
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
func printCheck(w io.Writer, c checkResult) {
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
func printStatusLine(w io.Writer, c checkResult) {
	detail := c.Detail
	if detail == "" {
		detail = "unknown"
	}
	fmt.Fprintf(w, "    %-12s %s\n", c.Name+":", detail)
}

// printStorageLine prints one storage accessibility line.
func printStorageLine(w io.Writer, s storageReport) {
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
func printFileLine(w io.Writer, label string, f fileReport) {
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
func printSystemdTimers(w io.Writer, s *systemdTimersReport) {
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
func printIndexStats(w io.Writer, idx indexStatsReport) {
	if idx.LastCompleted != nil {
		ago := time.Since(*idx.LastCompleted).Truncate(time.Minute)
		fmt.Fprintf(w, "    last indexed: %s  (%s ago)\n", idx.LastCompleted.Format("2006-01-02 15:04"), humanizeDuration(ago))
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
func printPendingEvents(w io.Writer, p *pendingEvents) {
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
func printDiskAccess(w io.Writer, d *diskAccessReport) {
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

// humanizeDuration formats a duration in a human-friendly way.
//
//	< 1m   → "just now"
//	< 1h   → "42m"
//	< 24h  → "14h 22m"
//	< 7d   → "3d 14h"
//	< 30d  → "2w 3d"
//	< 365d → "2mo 5d"
//	≥ 365d → "1y 2mo"
func humanizeDuration(d time.Duration) string {
	if d < time.Minute {
		return "just now"
	}

	totalMinutes := int(d.Minutes())
	totalHours := totalMinutes / 60
	totalDays := totalHours / 24

	if totalHours < 1 {
		return fmt.Sprintf("%dm", totalMinutes)
	}
	if totalDays < 1 {
		m := totalMinutes % 60
		if m == 0 {
			return fmt.Sprintf("%dh", totalHours)
		}
		return fmt.Sprintf("%dh %dm", totalHours, m)
	}
	if totalDays < 7 {
		h := totalHours % 24
		if h == 0 {
			return fmt.Sprintf("%dd", totalDays)
		}
		return fmt.Sprintf("%dd %dh", totalDays, h)
	}
	if totalDays < 30 {
		weeks := totalDays / 7
		days := totalDays % 7
		if days == 0 {
			return fmt.Sprintf("%dw", weeks)
		}
		return fmt.Sprintf("%dw %dd", weeks, days)
	}
	if totalDays < 365 {
		months := totalDays / 30
		days := totalDays % 30
		if days == 0 {
			return fmt.Sprintf("%dmo", months)
		}
		return fmt.Sprintf("%dmo %dd", months, days)
	}

	years := totalDays / 365
	remDays := totalDays % 365
	months := remDays / 30
	days := remDays % 30
	if months > 0 {
		return fmt.Sprintf("%dy %dmo", years, months)
	}
	if days > 0 {
		return fmt.Sprintf("%dy %dd", years, days)
	}
	return fmt.Sprintf("%dy", years)
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
