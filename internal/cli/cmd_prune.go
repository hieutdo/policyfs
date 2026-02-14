package cli

import (
	"context"
	"errors"
	"fmt"
	"io"
	"sort"
	"time"

	"github.com/dustin/go-humanize"
	"github.com/hieutdo/policyfs/internal/config"
	"github.com/hieutdo/policyfs/internal/errkind"
	"github.com/hieutdo/policyfs/internal/eventlog"
	"github.com/hieutdo/policyfs/internal/prune"
	"github.com/spf13/cobra"
)

// newPruneCmd creates `pfs prune`.
func newPruneCmd(configPath *string) *cobra.Command {
	var all bool
	var dryRun bool
	var limit int
	var quiet bool

	cmd := &cobra.Command{
		Use:   "prune [mount]",
		Short: "Apply deferred mutations (DELETE/RENAME/SETATTR) for indexed storages",
		Long: `Process deferred filesystem operations recorded in events.ndjson.

This command executes physical mutations (unlink/rmdir/rename/chmod/chown/utimens) that were deferred to avoid spinning up disks during normal filesystem operations.`,
		Example: `  pfs prune media
  pfs prune media --dry-run
  pfs prune media --limit 10
  pfs prune --all`,
		Args: func(cmd *cobra.Command, args []string) error {
			if all {
				if len(args) != 0 {
					return &CLIError{Code: ExitUsage, Cmd: "prune", Headline: "invalid arguments", Cause: errors.New("--all cannot be used with [mount]"), Hint: "run 'pfs prune --help'"}
				}
				return nil
			}
			if len(args) != 1 {
				return &CLIError{Code: ExitUsage, Cmd: "prune", Headline: "invalid arguments", Cause: errors.New("requires exactly 1 argument: <mount>"), Hint: "run 'pfs prune --help'"}
			}
			return nil
		},
		RunE: func(cmd *cobra.Command, args []string) error {
			if limit < 0 {
				return &CLIError{Code: ExitUsage, Cmd: "prune", Headline: "invalid arguments", Cause: errors.New("--limit must be >= 0"), Hint: "run 'pfs prune --help'"}
			}

			cfgPath := ""
			if configPath != nil {
				cfgPath = *configPath
			}

			rootCfg, err := loadRootConfig(cfgPath)
			if err != nil {
				return newConfigLoadCLIError("prune", cfgPath, err)
			}

			ctx := cmd.Context()
			stdout := cmd.OutOrStdout()
			actionHook := func(mountName string) prune.Hooks {
				if quiet {
					return prune.Hooks{}
				}
				return prune.Hooks{Verbose: func(e prune.VerboseEvent) {
					printPruneAction(stdout, mountName, e)
				}}
			}

			if all {
				mounts := make([]string, 0, len(rootCfg.Mounts))
				for name := range rootCfg.Mounts {
					mounts = append(mounts, name)
				}
				sort.Strings(mounts)

				anyFailed := false
				for _, mountName := range mounts {
					mountCfg, err := rootCfg.Mount(mountName)
					if err != nil {
						anyFailed = true
						continue
					}
					res, err := pruneOneshot(ctx, mountName, mountCfg, prune.Opts{DryRun: dryRun, Limit: limit}, actionHook(mountName))
					if err != nil {
						anyFailed = true
						if !quiet {
							fmt.Fprintf(stdout, "pfs prune: mount=%s failed: %s\n", mountName, rootCause(err).Error())
						}
						continue
					}
					printPruneSummary(stdout, res, quiet)
				}
				if anyFailed {
					return &CLIError{Code: ExitFail, Cmd: "prune", Headline: "unexpected error", Silent: true}
				}
				return nil
			}

			mountName := args[0]
			if err := validateMountName(mountName); err != nil {
				return &CLIError{Code: ExitUsage, Cmd: "prune", Headline: "invalid arguments", Cause: rootCause(err), Hint: "run 'pfs prune --help'"}
			}

			mountCfg, err := rootCfg.Mount(mountName)
			if err != nil {
				return &CLIError{Code: ExitUsage, Cmd: "prune", Headline: "invalid arguments", Cause: rootCause(err), Hint: "run 'pfs prune --help'"}
			}

			res, err := pruneOneshot(ctx, mountName, mountCfg, prune.Opts{DryRun: dryRun, Limit: limit}, actionHook(mountName))
			if err != nil {
				return &CLIError{Code: ExitFail, Cmd: "prune", Headline: "unexpected error", Cause: rootCause(err)}
			}
			printPruneSummary(stdout, res, quiet)
			return nil
		},
	}

	cmd.Flags().BoolVar(&all, "all", false, "process all mounts")
	cmd.Flags().BoolVar(&dryRun, "dry-run", false, "show what would be executed without making changes")
	cmd.Flags().IntVar(&limit, "limit", 0, "limit number of events processed")
	cmd.Flags().BoolVar(&quiet, "quiet", false, "suppress success output")

	return cmd
}

// printPruneSummary prints prune results and optional warnings, following the `pfs index` output style.
func printPruneSummary(w io.Writer, s prune.Summary, quiet bool) {
	if w == nil {
		return
	}
	if quiet {
		return
	}

	fmt.Fprintln(w, "Summary:")
	fmt.Fprintf(w, "  Events: processed %s  ok %s  skipped %s  failed %s\n",
		humanize.Comma(s.EventsProcessed),
		humanize.Comma(s.EventsSucceeded),
		humanize.Comma(s.EventsSkipped),
		humanize.Comma(s.EventsFailed),
	)
	fmt.Fprintf(w, "  Events log: truncated=%t\n", s.Truncated)

	if len(s.ByType) > 0 {
		del := s.ByType[eventlog.TypeDelete]
		ren := s.ByType[eventlog.TypeRename]
		set := s.ByType[eventlog.TypeSetattr]
		if del != 0 || ren != 0 || set != 0 {
			fmt.Fprintf(w, "  By type: DELETE %s  RENAME %s  SETATTR %s\n",
				humanize.Comma(del),
				humanize.Comma(ren),
				humanize.Comma(set),
			)
		}
	}

	dur := time.Duration(s.DurationMS) * time.Millisecond
	fmt.Fprintf(w, "Done: %s\n", dur.Round(100*time.Millisecond))

	if len(s.Warnings) > 0 {
		fmt.Fprintf(w, "\nWarnings (%d):\n\n", len(s.Warnings))
		for _, warn := range s.Warnings {
			fmt.Fprintf(w, "- %s\n", warn)
		}
	}
}

// printPruneAction prints one human-readable action line for a processed event.
func printPruneAction(w io.Writer, mountName string, e prune.VerboseEvent) {
	if w == nil {
		return
	}

	dryRun := ""
	if e.DryRun {
		dryRun = " dry_run=true"
	}

	switch e.Type {
	case eventlog.TypeDelete:
		fmt.Fprintf(w, "DELETE mount=%s storage_id=%s path=%s result=%s%s\n", mountName, e.StorageID, e.Path, e.Result, dryRun)
	case eventlog.TypeRename:
		fmt.Fprintf(w, "RENAME mount=%s storage_id=%s old_path=%s new_path=%s result=%s%s\n", mountName, e.StorageID, e.OldPath, e.NewPath, e.Result, dryRun)
	case eventlog.TypeSetattr:
		fmt.Fprintf(w, "SETATTR mount=%s storage_id=%s path=%s result=%s%s\n", mountName, e.StorageID, e.Path, e.Result, dryRun)
	default:
		fmt.Fprintf(w, "EVENT mount=%s type=%s result=%s%s\n", mountName, string(e.Type), e.Result, dryRun)
	}
}

func pruneOneshot(ctx context.Context, mountName string, mountCfg *config.MountConfig, opts prune.Opts, hooks prune.Hooks) (prune.Summary, error) {
	res, err := prune.RunOneshot(ctx, mountName, mountCfg, opts, hooks)
	if err != nil {
		if errors.Is(err, errkind.ErrBusy) {
			return res, &CLIError{Code: ExitBusy, Cmd: "prune", Headline: "job already running", Cause: err}
		}
		return res, fmt.Errorf("failed to prune: %w", err)
	}
	return res, nil
}
