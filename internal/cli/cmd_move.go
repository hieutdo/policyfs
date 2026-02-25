package cli

import (
	"context"
	"errors"
	"fmt"
	"io"
	"sort"
	"strings"
	"syscall"
	"time"

	"github.com/dustin/go-humanize"
	"github.com/hieutdo/policyfs/internal/config"
	"github.com/hieutdo/policyfs/internal/errkind"
	"github.com/hieutdo/policyfs/internal/humanfmt"
	"github.com/hieutdo/policyfs/internal/lock"
	"github.com/hieutdo/policyfs/internal/mover"
	"github.com/spf13/cobra"
)

// newMoveCmd creates `pfs move`.
func newMoveCmd(configPath *string) *cobra.Command {
	var job string
	var dryRun bool
	var force bool
	var limit int
	var debug bool
	var debugMax int
	var quiet bool
	var progress string

	cmd := &cobra.Command{
		Use:   "move <mount>",
		Short: "Move files between storage paths",
		Long: `Move files between storage paths based on mover job configuration.

This command is typically scheduled via systemd timers for usage-triggered jobs.`,
		Example: `  pfs move media
  pfs move media --job archive-media
  pfs move media --dry-run --force --debug --limit 5
  pfs move media --quiet
  pfs move media --progress=plain`,
		Args: func(cmd *cobra.Command, args []string) error {
			if len(args) != 1 {
				return &CLIError{Code: ExitUsage, Cmd: "move", Headline: "invalid arguments", Cause: errors.New("requires exactly 1 argument: <mount>"), Hint: "run 'pfs move --help'"}
			}
			return nil
		},
		RunE: func(cmd *cobra.Command, args []string) error {
			if limit < 0 {
				return &CLIError{Code: ExitUsage, Cmd: "move", Headline: "invalid arguments", Cause: errors.New("--limit must be >= 0"), Hint: "run 'pfs move --help'"}
			}

			cfgPath := ""
			if configPath != nil {
				cfgPath = *configPath
			}

			mountName := args[0]
			if err := validateMountName(mountName); err != nil {
				return &CLIError{Code: ExitUsage, Cmd: "move", Headline: "invalid arguments", Cause: rootCause(err), Hint: "run 'pfs move --help'"}
			}

			progressMode := strings.TrimSpace(progress)
			if progressMode == "" {
				progressMode = "auto"
			}
			if quiet {
				progressMode = "off"
			}
			switch progressMode {
			case "auto", "tty", "plain", "off":
			default:
				return &CLIError{Code: ExitUsage, Cmd: "move", Headline: "invalid arguments", Cause: fmt.Errorf("invalid value for --progress: %q", progressMode), Hint: "run 'pfs move --help'"}
			}

			rootCfg, err := loadRootConfig(cfgPath)
			if err != nil {
				return newConfigLoadCLIError("move", cfgPath, err)
			}
			mountCfg, err := rootCfg.Mount(mountName)
			if err != nil {
				return &CLIError{Code: ExitUsage, Cmd: "move", Headline: "invalid arguments", Cause: rootCause(err), Hint: "run 'pfs move --help'"}
			}

			lk, err := lock.AcquireMountLock(mountName, config.DefaultJobLockFile)
			if err != nil {
				if errors.Is(err, errkind.ErrBusy) {
					return &CLIError{Code: ExitBusy, Cmd: "move", Headline: "job already running", Cause: rootCause(err)}
				}
				return &CLIError{Code: ExitFail, Cmd: "move", Headline: "unexpected error", Cause: rootCause(err)}
			}
			defer func() { _ = lk.Close() }()

			stdout := cmd.OutOrStdout()
			ctx := cmd.Context()
			if quiet {
				// Quiet mode suppresses all output; debug is ignored.
				debug = false
				debugMax = 0
			}
			opts := mover.Opts{Job: job, DryRun: dryRun, Force: force, Limit: limit, Debug: debug, DebugMax: debugMax}

			if !quiet {
				printMoveHeader(stdout, mountName, opts)
			}
			plan := mover.PlanResult{}
			needPlan := debug || progressMode != "off"
			if needPlan {
				pl, err := mover.Plan(ctx, mountName, mountCfg, opts)
				if err != nil {
					if errors.Is(err, errkind.ErrNotFound) {
						return &CLIError{Code: ExitUsage, Cmd: "move", Headline: "invalid arguments", Cause: rootCause(err), Hint: "run 'pfs move --help'"}
					}
					return &CLIError{Code: ExitFail, Cmd: "move", Headline: "unexpected error", Cause: rootCause(err)}
				}
				plan = pl
			}
			if debug && !quiet {
				printMoveDebug(stdout, plan.Debug)
			}
			if debug && !quiet {
				printMoveCandidates(stdout, plan)
			}
			if needPlan && plan.TotalCandidates == 0 {
				if !quiet {
					skip, reason, err := moveShouldSkipAllJobsForTriggers(mountCfg, opts, time.Now())
					if err != nil {
						return &CLIError{Code: ExitFail, Cmd: "move", Headline: "unexpected error", Cause: rootCause(err)}
					}
					if skip && strings.TrimSpace(reason) != "" {
						fmt.Fprintf(stdout, "Skipped: %s\n", reason)
					} else {
						fmt.Fprintln(stdout, "Done: nothing to move.")
					}
				}
				return &CLIError{Code: ExitNoChanges, Silent: true}
			}

			warningsHuman := []string{}
			sawCanceledCopy := false
			hooks := mover.Hooks{}
			hooks.Warn = func(jobName string, storageID string, rel string, err error) {
				if errors.Is(err, context.Canceled) {
					sawCanceledCopy = true
				}
				msg := simplifyError(err)
				if strings.TrimSpace(rel) != "" {
					msg = fmt.Sprintf("%s: %s", rel, msg)
				}
				msg = fmt.Sprintf("%s/%s: %s", jobName, storageID, msg)
				warningsHuman = append(warningsHuman, msg)
			}

			var progressUI moveProgressAdapter
			if progressMode != "off" {
				if isInteractiveWriter(stdout) && progressMode != "plain" {
					progressUI = startMpbProgress(stdout, opts, plan)
				} else {
					progressUI = startPlainProgress(stdout, opts, plan)
				}
				hooks.FileStart = progressUI.OnFileStart
				hooks.Progress = progressUI.OnProgress
				hooks.CopyProgress = progressUI.OnCopyProgress
			}

			if !needPlan {
				skip, reason, err := moveShouldSkipAllJobsForTriggers(mountCfg, opts, time.Now())
				if err != nil {
					return &CLIError{Code: ExitFail, Cmd: "move", Headline: "unexpected error", Cause: rootCause(err)}
				}
				if skip {
					if !quiet {
						if strings.TrimSpace(reason) != "" {
							fmt.Fprintf(stdout, "Skipped: %s\n", reason)
						} else {
							fmt.Fprintln(stdout, "Done: nothing to move.")
						}
					}
					return &CLIError{Code: ExitNoChanges, Silent: true}
				}
			}

			res, err := mover.RunOneshot(ctx, mountName, mountCfg, opts, hooks)
			if err != nil {
				if progressUI != nil {
					progressUI.Cancel()
				}
				if errors.Is(err, context.Canceled) {
					if !quiet {
						totalCandidates := plan.TotalCandidates
						if totalCandidates <= 0 {
							for _, jr := range res.Jobs {
								totalCandidates += jr.TotalCandidates
							}
						}
						printMoveCancelSummary(stdout, res, totalCandidates, sawCanceledCopy)
					}
					return &CLIError{Code: ExitInterrupted, Silent: true}
				}
				if errors.Is(err, errkind.ErrNotFound) {
					return &CLIError{Code: ExitUsage, Cmd: "move", Headline: "invalid arguments", Cause: rootCause(err), Hint: "run 'pfs move --help'"}
				}
				return &CLIError{Code: ExitFail, Cmd: "move", Headline: "unexpected error", Cause: rootCause(err)}
			}

			if progressUI != nil {
				progressUI.Finish()
			}

			totalSkippedOpen := int64(0)
			for _, jr := range res.Jobs {
				totalSkippedOpen += jr.FilesSkippedOpen
			}
			if res.TotalFilesMoved == 0 && len(warningsHuman) == 0 {
				if totalSkippedOpen > 0 {
					if !quiet {
						printMoveSummary(stdout, res, warningsHuman)
					}
					return &CLIError{Code: ExitNoChanges, Silent: true}
				}
				if !quiet {
					fmt.Fprintln(stdout, "Done: nothing to move.")
				}
				return &CLIError{Code: ExitNoChanges, Silent: true}
			}
			if !quiet {
				printMoveSummary(stdout, res, warningsHuman)
			}
			return nil
		},
	}

	cmd.Flags().StringVar(&job, "job", "", "run a specific mover job")
	cmd.Flags().BoolVar(&dryRun, "dry-run", false, "show what would be moved without making changes")
	cmd.Flags().BoolVar(&force, "force", false, "force run (ignore triggers)")
	cmd.Flags().IntVar(&limit, "limit", 0, "limit number of files moved")
	cmd.Flags().BoolVar(&debug, "debug", false, "print debug info about skipped entries")
	cmd.Flags().IntVar(&debugMax, "debug-max", 20, "maximum number of debug entries to print")
	cmd.Flags().BoolVar(&quiet, "quiet", false, "disable all output")
	cmd.Flags().StringVar(&progress, "progress", "auto", "progress output: auto|tty|plain|off")

	return cmd
}

// moveShouldSkipAllJobsForTriggers reports whether every selected job would be skipped due to usage trigger preconditions.
func moveShouldSkipAllJobsForTriggers(mountCfg *config.MountConfig, opts mover.Opts, now time.Time) (bool, string, error) {
	if mountCfg == nil {
		return false, "", &errkind.NilError{What: "mount config"}
	}

	enabled := true
	if mountCfg.Mover.Enabled != nil {
		enabled = *mountCfg.Mover.Enabled
	}
	if !enabled {
		return true, "mover disabled", nil
	}

	jobs := mountCfg.Mover.Jobs
	if strings.TrimSpace(opts.Job) != "" {
		name := strings.TrimSpace(opts.Job)
		filtered := make([]config.MoverJobConfig, 0, 1)
		for _, j := range jobs {
			if j.Name == name {
				filtered = append(filtered, j)
				break
			}
		}
		if len(filtered) == 0 {
			return false, "", nil
		}
		jobs = filtered
	}
	if len(jobs) == 0 {
		return true, "", nil
	}
	if opts.Force {
		return false, "", nil
	}

	storageByID := map[string]config.StoragePath{}
	for _, sp := range mountCfg.StoragePaths {
		storageByID[sp.ID] = sp
	}

	reasons := map[string]struct{}{}
	skippedJobs := 0
	for _, j := range jobs {
		trigType := strings.TrimSpace(j.Trigger.Type)
		if trigType != "usage" {
			return false, "", nil
		}
		aw := j.Trigger.AllowedWindow
		if aw != nil {
			inside, err := cliInAllowedWindow(now, aw.Start, aw.End)
			if err != nil {
				return false, "", err
			}
			if !inside {
				skippedJobs++
				reasons["outside allowed_window"] = struct{}{}
				continue
			}
		}

		srcIDs, err := cliExpandStorageRefs(storageByID, mountCfg.StorageGroups, j.Source.Paths, j.Source.Groups)
		if err != nil {
			return false, "", err
		}

		anyActive := false
		for _, id := range srcIDs {
			sp, ok := storageByID[id]
			if !ok {
				continue
			}
			pct, err := cliUsagePercent(sp.Path)
			if err != nil {
				return false, "", err
			}
			if pct >= float64(j.Trigger.ThresholdStart) {
				anyActive = true
				break
			}
		}
		if !anyActive {
			skippedJobs++
			reasons["usage below threshold_start"] = struct{}{}
			continue
		}

		return false, "", nil
	}

	if skippedJobs != len(jobs) {
		return false, "", nil
	}
	if len(reasons) == 0 {
		return true, "", nil
	}

	ordered := make([]string, 0, len(reasons))
	for r := range reasons {
		ordered = append(ordered, r)
	}
	sort.Strings(ordered)
	return true, strings.Join(ordered, "; "), nil
}

// cliInAllowedWindow is a local copy of the mover allowed window calculation, kept private to avoid widening APIs.
func cliInAllowedWindow(now time.Time, start string, end string) (bool, error) {
	st, err := time.Parse("15:04", start)
	if err != nil {
		return false, fmt.Errorf("invalid allowed_window.start: %w", err)
	}
	et, err := time.Parse("15:04", end)
	if err != nil {
		return false, fmt.Errorf("invalid allowed_window.end: %w", err)
	}

	startToday := time.Date(now.Year(), now.Month(), now.Day(), st.Hour(), st.Minute(), 0, 0, now.Location())
	endToday := time.Date(now.Year(), now.Month(), now.Day(), et.Hour(), et.Minute(), 0, 0, now.Location())

	var winStart time.Time
	var winEnd time.Time
	if !startToday.After(endToday) {
		winStart = startToday
		winEnd = endToday
	} else {
		if now.Equal(startToday) || now.After(startToday) {
			winStart = startToday
			winEnd = endToday.Add(24 * time.Hour)
		} else {
			winStart = startToday.Add(-24 * time.Hour)
			winEnd = endToday
		}
	}

	inside := (now.Equal(winStart) || now.After(winStart)) && now.Before(winEnd)
	return inside, nil
}

// cliUsagePercent calculates filesystem usage percent using statfs.
func cliUsagePercent(p string) (float64, error) {
	var st syscall.Statfs_t
	if err := syscall.Statfs(p, &st); err != nil {
		return 0, fmt.Errorf("failed to statfs %q: %w", p, err)
	}
	if st.Blocks == 0 {
		return 0, nil
	}
	used := float64(st.Blocks-st.Bavail) / float64(st.Blocks)
	return used * 100.0, nil
}

// cliExpandStorageRefs expands storage IDs and group names into storage IDs.
func cliExpandStorageRefs(storageByID map[string]config.StoragePath, groups map[string][]string, paths []string, groupNames []string) ([]string, error) {
	in := []string{}
	in = append(in, paths...)
	in = append(in, groupNames...)

	out := []string{}
	seen := map[string]struct{}{}
	for _, id := range in {
		id = strings.TrimSpace(id)
		if id == "" {
			continue
		}
		if _, ok := storageByID[id]; ok {
			if _, dup := seen[id]; !dup {
				seen[id] = struct{}{}
				out = append(out, id)
			}
			continue
		}
		if members, ok := groups[id]; ok {
			for _, m := range members {
				m = strings.TrimSpace(m)
				if m == "" {
					continue
				}
				if _, ok := storageByID[m]; !ok {
					return nil, &errkind.InvalidError{Msg: fmt.Sprintf("config: storage_groups %q references unknown storage id %q", id, m)}
				}
				if _, dup := seen[m]; dup {
					continue
				}
				seen[m] = struct{}{}
				out = append(out, m)
			}
			continue
		}
		return nil, &errkind.NotFoundError{Msg: fmt.Sprintf("unknown storage id or group: %s", id)}
	}
	return out, nil
}

// printMoveHeader prints a short human-readable header before moving.
func printMoveHeader(w io.Writer, mountName string, opts mover.Opts) {
	if w == nil {
		return
	}
	flags := []string{}
	if opts.Debug {
		flags = append(flags, "debug=ON")
	}
	if opts.DryRun {
		flags = append(flags, "dry-run")
	}
	if opts.Force {
		flags = append(flags, "force=ON")
	}
	if opts.Limit > 0 {
		flags = append(flags, fmt.Sprintf("limit=%d", opts.Limit))
	}
	if strings.TrimSpace(opts.Job) != "" {
		flags = append(flags, fmt.Sprintf("job=%s", strings.TrimSpace(opts.Job)))
	}
	line := fmt.Sprintf("pfs move: mount=%s", mountName)
	if len(flags) > 0 {
		line += "  " + strings.Join(flags, "  ")
	}
	fmt.Fprintln(w, line)
}

// printMoveDebug prints best-effort debug entries produced during planning.
func printMoveDebug(w io.Writer, dbg *mover.PlanDebug) {
	if w == nil {
		return
	}
	if dbg == nil {
		fmt.Fprintln(w, "\nDebug:")
		fmt.Fprintln(w, "  (no debug output)")
		return
	}
	if dbg.Max <= 0 {
		return
	}

	fmt.Fprintf(w, "\nDebug (max=%d entries):\n", dbg.Max)
	if len(dbg.Entries) == 0 {
		fmt.Fprintln(w, "  (no entries)")
	} else {
		for _, e := range dbg.Entries {
			loc := strings.TrimSpace(e.Path)
			if loc == "" {
				loc = "-"
			}
			if strings.TrimSpace(e.StorageID) != "" {
				loc = fmt.Sprintf("%s:%s", strings.TrimSpace(e.StorageID), loc)
			}
			if strings.TrimSpace(e.JobName) != "" {
				loc = fmt.Sprintf("job=%s, %s", strings.TrimSpace(e.JobName), loc)
			}
			if strings.TrimSpace(e.Detail) != "" {
				fmt.Fprintf(w, "  %s  %s  (%s)\n", loc, e.Reason, e.Detail)
				continue
			}
			fmt.Fprintf(w, "  %s  %s\n", loc, e.Reason)
		}
		if dbg.Dropped > 0 {
			fmt.Fprintf(w, "  ... (%d more)\n", dbg.Dropped)
		}
	}

	if len(dbg.Destinations) > 0 {
		for _, j := range dbg.Destinations {
			primary := j.PrimaryChoice
			if strings.TrimSpace(primary) == "" {
				primary = "-"
			}
			note := strings.TrimSpace(j.Note)
			if note != "" {
				fmt.Fprintf(w, "\nDestinations: job=%s  policy=%s  path_preserving=%v  note=%s\n", j.JobName, j.Policy, j.PathPreserving, note)
			} else {
				fmt.Fprintf(w, "\nDestinations: job=%s  policy=%s  path_preserving=%v\n", j.JobName, j.Policy, j.PathPreserving)
			}
			for _, d := range j.Destinations {
				freePct := 100.0 - d.UsePct
				if freePct < 0 {
					freePct = 0
				}
				if freePct > 100 {
					freePct = 100
				}
				elig := "no"
				if d.Eligible {
					elig = "yes"
				}
				minFree := "-"
				if d.MinFreeGB > 0 {
					minFree = fmt.Sprintf("%.1f GiB", d.MinFreeGB)
				}
				primaryMark := ""
				if d.StorageID == primary {
					primaryMark = "  \u25C4 primary"
				}
				fmt.Fprintf(w, "  %s  free=%.1f GiB (%.0f%%)  used=%.0f%%  min_free=%s  eligible=%s%s\n",
					d.StorageID,
					d.FreeGB,
					freePct,
					d.UsePct,
					minFree,
					elig,
					primaryMark,
				)
			}
			if len(j.OrderedEligible) > 0 {
				fmt.Fprintf(w, "  ordered: %s\n", strings.Join(j.OrderedEligible, ", "))
			}
		}
	}
}

// printMoveCandidates prints the planned move candidates when --verbose is enabled.
func printMoveCandidates(w io.Writer, plan mover.PlanResult) {
	if w == nil {
		return
	}

	totalBytes := int64(0)
	for _, pj := range plan.Jobs {
		for _, c := range pj.Candidates {
			totalBytes += c.SizeBytes
		}
	}
	totalSz := humanfmt.FormatBytesIEC(totalBytes, 1)
	fmt.Fprintf(w, "\nCandidates: %d files  %s\n", plan.TotalCandidates, totalSz)
	if plan.TotalCandidates == 0 {
		fmt.Fprintln(w, "  (none)")
		return
	}

	idx := 0
	for _, pj := range plan.Jobs {
		for _, c := range pj.Candidates {
			idx++
			sz := humanfmt.FormatBytesIEC(c.SizeBytes, 1)
			dst := c.DstStorageID
			if dst == "" {
				dst = "-"
			}
			fmt.Fprintf(w, "  [%d]  %s\n", idx, c.RelPath)
			fmt.Fprintf(w, "       %s  job=%s  %s \u2500\u25BA %s\n", sz, pj.Name, c.SrcStorageID, dst)
			if c.PathPreservingKept != nil {
				if len(c.PathPreservingKept) == 0 {
					fmt.Fprintf(w, "       path_preserving: parent dir not present on any destination\n")
				} else {
					fmt.Fprintf(w, "       path_preserving: %s\n", strings.Join(c.PathPreservingKept, ", "))
				}
			}
		}
	}
}

// printMoveCancelSummary prints a one-line summary when the move is interrupted by Ctrl+C.
func printMoveCancelSummary(w io.Writer, res mover.Result, totalCandidates int64, partialCleaned bool) {
	if w == nil {
		return
	}

	moved := res.TotalFilesMoved
	freed := humanfmt.FormatBytesIEC(res.TotalBytesFreed, 1)
	remaining := max(totalCandidates-moved, 0)

	parts := []string{}
	parts = append(parts, fmt.Sprintf("%d moved (%s freed)", moved, freed))

	// At most 1 file was in-flight when interrupted; its temp file is auto-cleaned.
	if remaining > 0 && partialCleaned {
		parts = append(parts, "1 partial (cleaned up)")
		remaining--
	}
	if remaining > 0 {
		parts = append(parts, fmt.Sprintf("%d pending", remaining))
	}

	fmt.Fprintf(w, "\nStopped early: %s\n", strings.Join(parts, ", "))
}

// printMoveSummary prints the human summary (per-job + totals + warnings).
func printMoveSummary(w io.Writer, res mover.Result, warningsHuman []string) {
	if w == nil {
		return
	}

	totalDur := time.Duration(res.TotalDurationMS) * time.Millisecond

	fmt.Fprintln(w, "\nSummary:")
	for _, jr := range res.Jobs {
		fmt.Fprintf(w, "  Job %s: moved %s files, %s, freed %s\n",
			jr.Name,
			humanize.Comma(jr.FilesMoved),
			humanfmt.FormatBytesIEC(jr.BytesMoved, 1),
			humanfmt.FormatBytesIEC(jr.BytesFreed, 1),
		)
		if jr.FilesSkippedOpen > 0 {
			fmt.Fprintf(w, "         skipped_open %s\n", humanize.Comma(jr.FilesSkippedOpen))
		}
	}
	var totalErrors int64
	for _, jr := range res.Jobs {
		totalErrors += jr.FilesErrored
	}
	fmt.Fprintf(w, "  Errors: %s\n", humanize.Comma(totalErrors))
	fmt.Fprintf(w, "Done: %s files, %s, freed %s, in %s\n",
		humanize.Comma(res.TotalFilesMoved),
		humanfmt.FormatBytesIEC(res.TotalBytesMoved, 1),
		humanfmt.FormatBytesIEC(res.TotalBytesFreed, 1),
		totalDur.Round(100*time.Millisecond),
	)

	if len(warningsHuman) > 0 {
		fmt.Fprintf(w, "\nWarnings (%d):\n", len(warningsHuman))
		for _, warn := range warningsHuman {
			fmt.Fprintf(w, "  - %s\n", warn)
		}
	}
}
