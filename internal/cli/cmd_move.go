package cli

import (
	"errors"
	"fmt"
	"io"
	"strings"
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
	var quiet bool
	var progress string

	cmd := &cobra.Command{
		Use:   "move <mount>",
		Short: "Move files between storage paths",
		Long: `Move files between storage paths based on mover job configuration.

This command is typically scheduled via systemd timers for usage-triggered jobs.`,
		Example: `  pfs move media
  pfs move media --job archive-media
  pfs move media --dry-run
  pfs move media --force
  pfs move media --limit 100
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
			printMoveHeader(stdout, mountName, job, dryRun)

			ctx := cmd.Context()
			warningsHuman := []string{}
			hooks := mover.Hooks{}
			hooks.Warn = func(jobName string, storageID string, rel string, err error) {
				msg := simplifyError(err)
				if strings.TrimSpace(rel) != "" {
					msg = fmt.Sprintf("%s: %s", rel, msg)
				}
				msg = fmt.Sprintf("%s/%s: %s", jobName, storageID, msg)
				warningsHuman = append(warningsHuman, msg)
			}

			opts := mover.Opts{Job: job, DryRun: dryRun, Force: force, Limit: limit}
			var progressUI *moveProgressUI
			if progressMode != "off" {
				pui, err := startMoveProgress(ctx, stdout, mountName, mountCfg, opts, progressMode)
				if err != nil {
					return &CLIError{Code: ExitFail, Cmd: "move", Headline: "unexpected error", Cause: rootCause(err)}
				}
				progressUI = pui
				hooks.Progress = progressUI.OnProgress
			}

			res, err := mover.RunOneshot(ctx, mountName, mountCfg, opts, hooks)
			if err != nil {
				if errors.Is(err, errkind.ErrNotFound) {
					return &CLIError{Code: ExitUsage, Cmd: "move", Headline: "invalid arguments", Cause: rootCause(err), Hint: "run 'pfs move --help'"}
				}
				return &CLIError{Code: ExitFail, Cmd: "move", Headline: "unexpected error", Cause: rootCause(err)}
			}

			if progressUI != nil {
				progressUI.Finish()
			}
			printMoveSummary(stdout, res, warningsHuman)
			return nil
		},
	}

	cmd.Flags().StringVar(&job, "job", "", "run a specific mover job")
	cmd.Flags().BoolVar(&dryRun, "dry-run", false, "show what would be moved without making changes")
	cmd.Flags().BoolVar(&force, "force", false, "force run (ignore triggers)")
	cmd.Flags().IntVar(&limit, "limit", 0, "limit number of files moved")
	cmd.Flags().BoolVar(&quiet, "quiet", false, "disable progress output")
	cmd.Flags().StringVar(&progress, "progress", "auto", "progress output: auto|tty|plain|off")

	return cmd
}

// printMoveHeader prints a short human-readable header before moving.
func printMoveHeader(w io.Writer, mountName string, jobName string, dryRun bool) {
	if w == nil {
		return
	}
	fmt.Fprintf(w, "pfs move: mount=%s\n", mountName)
	if strings.TrimSpace(jobName) != "" {
		fmt.Fprintf(w, "Job: %s\n", strings.TrimSpace(jobName))
	}
	if dryRun {
		fmt.Fprintln(w, "Mode: dry-run")
	}
}

// printMoveSummary prints the human summary (per-job + totals + warnings).
func printMoveSummary(w io.Writer, res mover.Result, warningsHuman []string) {
	if w == nil {
		return
	}

	fmt.Fprintln(w, "Summary:")
	for _, jr := range res.Jobs {
		dur := time.Duration(jr.DurationMS) * time.Millisecond
		fmt.Fprintf(w, "  %s  moved  %s files  %s  errors %s  (%s)\n",
			jr.Name,
			humanize.Comma(jr.FilesMoved),
			humanfmt.FormatBytesIEC(jr.BytesMoved, 1),
			humanize.Comma(jr.FilesErrored),
			dur.Round(100*time.Millisecond),
		)
	}

	totalDur := time.Duration(res.TotalDurationMS) * time.Millisecond
	fmt.Fprintf(w, "Total freed on sources: %s\n", humanfmt.FormatBytesIEC(res.TotalBytesFreed, 1))
	fmt.Fprintf(w, "Done: %s files, %s in %s\n",
		humanize.Comma(res.TotalFilesMoved),
		humanfmt.FormatBytesIEC(res.TotalBytesMoved, 1),
		totalDur.Round(100*time.Millisecond),
	)

	if len(warningsHuman) > 0 {
		fmt.Fprintf(w, "\nWarnings (%d):\n\n", len(warningsHuman))
		for _, warn := range warningsHuman {
			fmt.Fprintf(w, "- %s\n", warn)
		}
	}
}
