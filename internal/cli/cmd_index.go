package cli

import (
	"errors"
	"fmt"
	"io"
	"os"
	"sort"
	"strings"
	"time"

	"github.com/dustin/go-humanize"
	"github.com/hieutdo/policyfs/internal/config"
	"github.com/hieutdo/policyfs/internal/errkind"
	"github.com/hieutdo/policyfs/internal/indexdb"
	"github.com/hieutdo/policyfs/internal/indexer"
	"github.com/hieutdo/policyfs/internal/lock"
	"github.com/spf13/cobra"
)

// indexResponder centralizes error/success output for `pfs index`.
type indexResponder struct {
	cmd        *cobra.Command
	configPath string
}

// newIndexResponder builds an index responder for the current command invocation.
func newIndexResponder(cmd *cobra.Command, configPath string) *indexResponder {
	return &indexResponder{
		cmd:        cmd,
		configPath: configPath,
	}
}

// fail returns a formatted CLIError (stderr).
func (r *indexResponder) fail(code int, headline string, cause error, hint string) error {
	return &CLIError{Code: code, Cmd: "index", Headline: headline, Cause: rootCause(cause), Hint: hint}
}

// handleLoadAndResolveMountError formats config/mount resolution errors consistently.
func (r *indexResponder) handleLoadAndResolveMountError(err error) error {
	if r == nil {
		return &CLIError{Code: ExitFail, Cmd: "index", Headline: "unexpected error", Cause: rootCause(err)}
	}
	if isUsageError(err) {
		return r.fail(ExitUsage, "invalid arguments", err, "run 'pfs index --help'")
	}
	if errors.Is(err, os.ErrNotExist) || errors.Is(err, os.ErrPermission) {
		return newConfigLoadCLIError("index", r.configPath, err)
	}
	return r.fail(ExitFail, fmt.Sprintf("invalid config: %s", r.configPath), err, "")
}

// handleAcquireJobLockError formats job.lock acquisition errors.
func (r *indexResponder) handleAcquireJobLockError(err error) error {
	if r == nil {
		return &CLIError{Code: ExitFail, Cmd: "index", Headline: "unexpected error", Cause: rootCause(err)}
	}
	if errors.Is(err, errkind.ErrBusy) {
		return r.fail(ExitBusy, "job already running", err, "")
	}
	return r.fail(ExitFail, "unexpected error", err, "")
}

// resetIndexDB handles --rebuild behavior (including daemon.lock).
func (r *indexResponder) resetIndexDB(mountName string) error {
	dlk, err := lock.AcquireMountLock(mountName, config.DefaultDaemonLockFile)
	if err != nil {
		if errors.Is(err, errkind.ErrBusy) {
			return r.fail(ExitFail, "cannot reset while daemon is running", err, "stop 'pfs mount' and try again")
		}
		return r.fail(ExitFail, "unexpected error", err, "")
	}
	defer func() { _ = dlk.Close() }()

	if err := indexdb.Reset(mountName); err != nil {
		return r.fail(ExitFail, "failed to reset index db", err, "")
	}
	return nil
}

// openIndexDB opens the per-mount index db or returns a formatted error.
func (r *indexResponder) openIndexDB(mountName string) (*indexdb.DB, error) {
	idxDB, err := indexdb.Open(mountName)
	if err != nil {
		return nil, r.fail(ExitFail, "failed to open index db", err, "")
	}
	return idxDB, nil
}

// filterMountCfgByStorageID narrows the mount config to a single storage id.
func (r *indexResponder) filterMountCfgByStorageID(mountCfg *config.MountConfig, storageID string) (*config.MountConfig, error) {
	if strings.TrimSpace(storageID) == "" {
		return mountCfg, nil
	}

	found := false
	out := *mountCfg
	out.StoragePaths = nil
	for _, sp := range mountCfg.StoragePaths {
		if sp.ID == storageID {
			found = true
			if !sp.Indexed {
				return nil, r.fail(ExitUsage, "invalid arguments", fmt.Errorf("storage %q is not indexed", storageID), "run 'pfs index --help'")
			}
			out.StoragePaths = append(out.StoragePaths, sp)
		}
	}
	if !found {
		return nil, r.fail(ExitUsage, "invalid arguments", fmt.Errorf("unknown storage id: %s", storageID), "run 'pfs index --help'")
	}
	return &out, nil
}

// newWarnHook builds the indexer warning hook.
func (r *indexResponder) newWarnHook(warningsHuman *[]string) func(storageID, rel string, err error) {
	return func(storageID, rel string, err error) {
		rel = strings.TrimSpace(rel)
		msg := simplifyError(err)
		if rel != "" {
			msg = fmt.Sprintf("%s: %s", rel, msg)
		}
		msg = fmt.Sprintf("%s: %s", storageID, msg)
		if warningsHuman != nil {
			*warningsHuman = append(*warningsHuman, msg)
		}
	}
}

// writeSuccess writes the success output.
func (r *indexResponder) writeSuccess(mountName string, mountCfg *config.MountConfig, res indexer.Result, warningsHuman []string) error {
	stdout := io.Discard
	if r != nil && r.cmd != nil {
		stdout = r.cmd.OutOrStdout()
	}
	printIndexSummary(stdout, res, warningsHuman)
	return nil
}

// printIndexHeader prints a short human-readable header before scanning.
func printIndexHeader(w io.Writer, mountName string, mountCfg *config.MountConfig) {
	if w == nil || mountCfg == nil {
		return
	}
	var ids []string
	for _, sp := range mountCfg.StoragePaths {
		if sp.Indexed {
			ids = append(ids, sp.ID)
		}
	}
	sort.Strings(ids)
	fmt.Fprintf(w, "pfs index: mount=%s\n", mountName)
	fmt.Fprintf(w, "Scanning storages: %s\n", strings.Join(ids, ", "))
}

// printIndexSummary prints the human summary (per-storage + totals + warnings).
func printIndexSummary(w io.Writer, res indexer.Result, warningsHuman []string) {
	if w == nil {
		return
	}

	fmt.Fprintln(w, "Summary:")

	totalUpserts := int64(0)
	totalDeletes := int64(0)
	for _, sr := range res.StoragePaths {
		dur := time.Duration(sr.DurationMS) * time.Millisecond
		fmt.Fprintf(w, "  %s  scanned  %s dirs  %s files  (%s)\n",
			sr.ID,
			humanize.Comma(sr.DirsScanned),
			humanize.Comma(sr.FilesScanned),
			dur.Round(100*time.Millisecond),
		)
		totalUpserts += sr.Upserts
		totalDeletes += sr.StaleRemoved
	}

	fmt.Fprintf(w, "Index DB: updated (%s upserts, %s stale removals)\n",
		humanize.Comma(totalUpserts),
		humanize.Comma(totalDeletes),
	)

	totalDur := time.Duration(res.TotalDurationMS) * time.Millisecond
	fmt.Fprintf(w, "Done: %s files, %s dirs, %s in %s\n",
		humanize.Comma(res.TotalFiles),
		humanize.Comma(res.TotalDirs),
		humanize.Bytes(uint64(res.TotalBytes)),
		totalDur.Round(100*time.Millisecond),
	)

	if len(warningsHuman) > 0 {
		fmt.Fprintf(w, "\nWarnings (%d):\n\n", len(warningsHuman))
		for _, warn := range warningsHuman {
			fmt.Fprintf(w, "- %s\n", warn)
		}
	}
}

// newIndexCmd creates `pfs index`.
func newIndexCmd(configPath *string) *cobra.Command {
	var quiet bool
	var verbose bool
	var progress string
	var storageID string
	var rebuild bool

	cmd := &cobra.Command{
		Use:   "index <mount>",
		Short: "Index metadata for indexed storage paths",
		Long: `Index indexed storage paths and write metadata to the per-mount SQLite database.

This enables metadata operations (lookup/readdir/getattr) to avoid touching disks for indexed paths.`,
		Example: `  pfs index media
  pfs index media --rebuild
  pfs index media --quiet
  pfs index media --progress=plain
  pfs index media --storage hdd1`,
		Args: func(cmd *cobra.Command, args []string) error {
			if len(args) != 1 {
				return &CLIError{Code: ExitUsage, Cmd: "index", Headline: "invalid arguments", Cause: errors.New("requires exactly 1 argument: <mount>"), Hint: "run 'pfs index --help'"}
			}
			return nil
		},
		RunE: func(cmd *cobra.Command, args []string) error {
			mountName := args[0]
			cfg := *configPath
			r := newIndexResponder(cmd, cfg)

			progressMode := strings.TrimSpace(progress)
			if progressMode == "" {
				progressMode = "auto"
			}
			if quiet {
				progressMode = "off"
			}
			if quiet && verbose {
				return r.fail(ExitUsage, "invalid arguments", errors.New("--quiet cannot be used with -v"), "run 'pfs index --help'")
			}
			switch progressMode {
			case "auto", "tty", "plain", "off":
			default:
				return r.fail(
					ExitUsage,
					"invalid arguments",
					fmt.Errorf("invalid value for --progress: %q", progressMode),
					"run 'pfs index --help'",
				)
			}
			if verbose {
				if cmd.Flags().Changed("progress") && progressMode != "off" {
					return r.fail(ExitUsage, "invalid arguments", errors.New("--progress cannot be used with -v"), "run 'pfs index --help'")
				}
				progressMode = "off"
			}

			warningsHuman := []string{}
			_, mountCfg, _, err := loadAndResolveMount(cfg, mountName)
			if err != nil {
				return r.handleLoadAndResolveMountError(err)
			}

			lk, err := lock.AcquireMountLock(mountName, config.DefaultJobLockFile)
			if err != nil {
				return r.handleAcquireJobLockError(err)
			}
			defer func() {
				_ = lk.Close()
			}()

			if rebuild {
				if err := r.resetIndexDB(mountName); err != nil {
					return err
				}
			}

			idxDB, err := r.openIndexDB(mountName)
			if err != nil {
				return err
			}
			defer func() {
				_ = idxDB.Close()
			}()

			mountCfg, err = r.filterMountCfgByStorageID(mountCfg, storageID)
			if err != nil {
				return err
			}

			ctx := cmd.Context()
			stdout := cmd.OutOrStdout()
			printIndexHeader(stdout, mountName, mountCfg)

			hooks := indexer.Hooks{}
			var progressUI *indexProgressUI
			if progressMode != "off" {
				p, err := startIndexProgress(ctx, stdout, mountName, mountCfg, progressMode)
				if err != nil {
					return r.fail(ExitFail, "unexpected error", err, "")
				}
				progressUI = p
				hooks.Progress = progressUI.OnProgress
			}
			if verbose {
				hooks.Progress = func(storageID string, rel string, isDir bool) {
					_ = isDir
					fmt.Fprintf(stdout, "%s  + %s\n", storageID, rel)
				}
			}
			hooks.Warn = r.newWarnHook(&warningsHuman)

			res, err := indexer.Run(ctx, mountName, mountCfg, idxDB.SQL(), hooks)
			if err != nil {
				return r.fail(ExitFail, "unexpected error", err, "")
			}

			if progressUI != nil {
				progressUI.Finish()
			}
			return r.writeSuccess(mountName, mountCfg, res, warningsHuman)
		},
	}
	cmd.Flags().BoolVarP(&verbose, "verbose", "v", false, "verbose output")
	cmd.Flags().BoolVar(&quiet, "quiet", false, "disable progress output")
	cmd.Flags().StringVar(&progress, "progress", "auto", "progress output: auto|tty|plain|off")
	cmd.Flags().StringVar(&storageID, "storage", "", "index a specific storage id")
	cmd.Flags().BoolVar(&rebuild, "rebuild", false, "delete index db before indexing")

	return cmd
}

// simplifyError simplifies error messages for user-friendly output.
func simplifyError(err error) string {
	if err == nil {
		return ""
	}
	var pe *os.PathError
	if errors.As(err, &pe) {
		if errors.Is(err, os.ErrNotExist) {
			return fmt.Sprintf("not found: %s", pe.Path)
		}
		if errors.Is(err, os.ErrPermission) {
			return fmt.Sprintf("permission denied: %s", pe.Path)
		}
		return pe.Err.Error()
	}
	return err.Error()
}
