package cli

import (
	"context"
	"errors"
	"fmt"
	"sort"
	"strings"

	"github.com/hieutdo/policyfs/internal/config"
	"github.com/hieutdo/policyfs/internal/errkind"
	"github.com/hieutdo/policyfs/internal/indexdb"
	"github.com/hieutdo/policyfs/internal/indexer"
	"github.com/hieutdo/policyfs/internal/lock"
	"github.com/hieutdo/policyfs/internal/mover"
	"github.com/hieutdo/policyfs/internal/prune"
	"github.com/spf13/cobra"
)

// maintIndexMode defines how `pfs maint` runs the index phase.
type maintIndexMode string

const (
	maintIndexModeTouch maintIndexMode = "touch"
	maintIndexModeAll   maintIndexMode = "all"
	maintIndexModeOff   maintIndexMode = "off"
)

// touchStorageID adds a storage id to a set.
func touchStorageID(set map[string]struct{}, storageID string) {
	storageID = strings.TrimSpace(storageID)
	if storageID == "" {
		return
	}
	set[storageID] = struct{}{}
}

// filterMountCfgByIndexedStorageIDs returns a shallow copy of mountCfg with StoragePaths narrowed
// to indexed storages whose IDs are present in ids.
func filterMountCfgByIndexedStorageIDs(mountCfg *config.MountConfig, ids map[string]struct{}) (*config.MountConfig, []string) {
	if mountCfg == nil {
		return nil, nil
	}
	out := *mountCfg
	out.StoragePaths = nil
	kept := []string{}
	for _, sp := range mountCfg.StoragePaths {
		if !sp.Indexed {
			continue
		}
		if ids != nil {
			if _, ok := ids[sp.ID]; !ok {
				continue
			}
		}
		out.StoragePaths = append(out.StoragePaths, sp)
		kept = append(kept, sp.ID)
	}
	sort.Strings(kept)
	return &out, kept
}

// newMaintCmd creates `pfs maint`.
func newMaintCmd(configPath *string) *cobra.Command {
	var job string
	var force bool
	var limit int
	var quiet bool
	var indexMode string

	cmd := &cobra.Command{
		Use:   "maint <mount>",
		Short: "Run maintenance jobs (move, prune, index) in sequence",
		Long: `Run mover, prune, and index for a mount while holding job.lock.

This command is intended for systemd timers to reduce disk wake-ups by batching maintenance work into a single window.`,
		Example: `  pfs maint media
  pfs maint media --job archive --force --limit 10
  pfs maint media --index=all
  pfs maint media --quiet`,
		Args: func(cmd *cobra.Command, args []string) error {
			if len(args) != 1 {
				return &CLIError{Code: ExitUsage, Cmd: "maint", Headline: "invalid arguments", Cause: errors.New("requires exactly 1 argument: <mount>"), Hint: "run 'pfs maint --help'"}
			}
			return nil
		},
		RunE: func(cmd *cobra.Command, args []string) error {
			if limit < 0 {
				return &CLIError{Code: ExitUsage, Cmd: "maint", Headline: "invalid arguments", Cause: errors.New("--limit must be >= 0"), Hint: "run 'pfs maint --help'"}
			}

			mode := maintIndexMode(strings.TrimSpace(indexMode))
			if mode == "" {
				mode = maintIndexModeTouch
			}
			switch mode {
			case maintIndexModeTouch, maintIndexModeAll, maintIndexModeOff:
			default:
				return &CLIError{Code: ExitUsage, Cmd: "maint", Headline: "invalid arguments", Cause: fmt.Errorf("invalid value for --index: %q", strings.TrimSpace(indexMode)), Hint: "run 'pfs maint --help'"}
			}

			cfgPath := ""
			if configPath != nil {
				cfgPath = *configPath
			}

			mountName := args[0]
			if err := validateMountName(mountName); err != nil {
				return &CLIError{Code: ExitUsage, Cmd: "maint", Headline: "invalid arguments", Cause: rootCause(err), Hint: "run 'pfs maint --help'"}
			}

			rootCfg, err := loadRootConfig(cfgPath)
			if err != nil {
				return newConfigLoadCLIError("maint", cfgPath, err)
			}
			mountCfg, err := rootCfg.Mount(mountName)
			if err != nil {
				return &CLIError{Code: ExitUsage, Cmd: "maint", Headline: "invalid arguments", Cause: rootCause(err), Hint: "run 'pfs maint --help'"}
			}

			lk, err := lock.AcquireMountLock(mountName, config.DefaultJobLockFile)
			if err != nil {
				if errors.Is(err, errkind.ErrBusy) {
					return &CLIError{Code: ExitBusy, Cmd: "maint", Headline: "job already running", Cause: rootCause(err)}
				}
				return &CLIError{Code: ExitFail, Cmd: "maint", Headline: "unexpected error", Cause: rootCause(err)}
			}
			defer func() { _ = lk.Close() }()

			stdout := cmd.OutOrStdout()
			ctx := cmd.Context()

			if !quiet {
				flags := []string{fmt.Sprintf("index=%s", mode)}
				if force {
					flags = append(flags, "force=ON")
				}
				if limit > 0 {
					flags = append(flags, fmt.Sprintf("limit=%d", limit))
				}
				if strings.TrimSpace(job) != "" {
					flags = append(flags, fmt.Sprintf("job=%s", strings.TrimSpace(job)))
				}
				fmt.Fprintf(stdout, "pfs maint: mount=%s  %s\n\n", mountName, strings.Join(flags, "  "))
			}

			touched := map[string]struct{}{}

			mvOpts := mover.Opts{Job: job, Force: force, Limit: limit}
			mvWarnings := []string{}
			mvHooks := mover.Hooks{}
			var mvPlan mover.PlanResult
			var mvProgressUI moveProgressAdapter
			sawCanceledCopy := false

			baseFileStart := func(_ string, srcStorageID string, dstStorageID string, _ string, _ int64) {
				touchStorageID(touched, srcStorageID)
				touchStorageID(touched, dstStorageID)
			}
			baseProgress := func(_ string, storageID string, _ string) {
				touchStorageID(touched, storageID)
			}

			mvHooks.Warn = func(jobName string, storageID string, rel string, err error) {
				touchStorageID(touched, storageID)
				if errors.Is(err, context.Canceled) {
					sawCanceledCopy = true
				}
				msg := simplifyError(err)
				if strings.TrimSpace(rel) != "" {
					msg = fmt.Sprintf("%s: %s", rel, msg)
				}
				msg = fmt.Sprintf("%s/%s: %s", jobName, storageID, msg)
				mvWarnings = append(mvWarnings, msg)
			}

			mvHooks.FileStart = func(jobName string, srcStorageID string, dstStorageID string, rel string, sizeBytes int64) {
				baseFileStart(jobName, srcStorageID, dstStorageID, rel, sizeBytes)
				if mvProgressUI != nil {
					mvProgressUI.OnFileStart(jobName, srcStorageID, dstStorageID, rel, sizeBytes)
				}
			}
			mvHooks.Progress = func(jobName string, storageID string, rel string) {
				baseProgress(jobName, storageID, rel)
				if mvProgressUI != nil {
					mvProgressUI.OnProgress(jobName, storageID, rel)
				}
			}
			mvHooks.CopyProgress = func(jobName string, storageID string, rel string, phase string, doneBytes int64, totalBytes int64) {
				touchStorageID(touched, storageID)
				if mvProgressUI != nil {
					mvProgressUI.OnCopyProgress(jobName, storageID, rel, phase, doneBytes, totalBytes)
				}
			}

			if !quiet {
				printMoveHeader(stdout, mountName, mvOpts)
			}

			if !quiet {
				pl, err := mover.Plan(ctx, mountName, mountCfg, mvOpts)
				if err != nil {
					if errors.Is(err, context.Canceled) {
						return &CLIError{Code: ExitInterrupted, Silent: true}
					}
					if errors.Is(err, errkind.ErrNotFound) {
						return &CLIError{Code: ExitUsage, Cmd: "maint", Headline: "invalid arguments", Cause: rootCause(err), Hint: "run 'pfs maint --help'"}
					}
					return &CLIError{Code: ExitFail, Cmd: "maint", Headline: "failed to plan move", Cause: rootCause(err)}
				}
				mvPlan = pl
				if isInteractiveWriter(stdout) {
					mvProgressUI = startMpbProgress(stdout, mvOpts, mvPlan)
				} else {
					mvProgressUI = startPlainProgress(stdout, mvOpts, mvPlan)
				}
			}

			mvRes, err := mover.RunOneshot(ctx, mountName, mountCfg, mvOpts, mvHooks)
			if err != nil {
				if mvProgressUI != nil {
					mvProgressUI.Cancel()
				}
				if errors.Is(err, context.Canceled) {
					if !quiet {
						totalCandidates := mvPlan.TotalCandidates
						if totalCandidates <= 0 {
							for _, jr := range mvRes.Jobs {
								totalCandidates += jr.TotalCandidates
							}
						}
						printMoveCancelSummary(stdout, mvRes, totalCandidates, sawCanceledCopy)
					}
					return &CLIError{Code: ExitInterrupted, Silent: true}
				}
				if errors.Is(err, errkind.ErrNotFound) {
					return &CLIError{Code: ExitUsage, Cmd: "maint", Headline: "invalid arguments", Cause: rootCause(err), Hint: "run 'pfs maint --help'"}
				}
				return &CLIError{Code: ExitFail, Cmd: "maint", Headline: "failed to move", Cause: rootCause(err)}
			}
			if mvProgressUI != nil {
				mvProgressUI.Finish()
			}
			if !quiet {
				printMoveSummary(stdout, mvRes, mvWarnings)
			}

			if !quiet {
				fmt.Fprintf(stdout, "\npfs prune: mount=%s\n", mountName)
			}
			prHooks := prune.Hooks{}
			prHooks.Verbose = func(e prune.VerboseEvent) {
				touchStorageID(touched, e.StorageID)
				if !quiet {
					printPruneAction(stdout, mountName, e)
				}
			}
			prRes, err := prune.RunOneshot(ctx, mountName, mountCfg, prune.Opts{}, prHooks)
			if err != nil {
				if errors.Is(err, context.Canceled) {
					return &CLIError{Code: ExitInterrupted, Silent: true}
				}
				return &CLIError{Code: ExitFail, Cmd: "maint", Headline: "failed to prune", Cause: rootCause(err)}
			}
			if !quiet {
				fmt.Fprintln(stdout)
				printPruneSummary(stdout, prRes, false)
			}

			if mode == maintIndexModeOff {
				if !quiet {
					fmt.Fprintf(stdout, "\npfs index: skipped (index=off)\n")
				}
				return nil
			}

			idxMountCfg := mountCfg
			if mode == maintIndexModeTouch {
				filtered, kept := filterMountCfgByIndexedStorageIDs(mountCfg, touched)
				if len(kept) == 0 {
					if !quiet {
						fmt.Fprintf(stdout, "\npfs index: skipped (no touched indexed storages)\n")
					}
					return nil
				}
				idxMountCfg = filtered
			}

			idxDB, err := indexdb.Open(mountName)
			if err != nil {
				return &CLIError{Code: ExitFail, Cmd: "maint", Headline: "failed to open index db", Cause: rootCause(err)}
			}
			defer func() { _ = idxDB.Close() }()

			idxWarnings := []string{}
			idxHooks := indexer.Hooks{}
			idxHooks.Warn = func(storageID string, rel string, err error) {
				rel = strings.TrimSpace(rel)
				msg := simplifyError(err)
				if rel != "" {
					msg = fmt.Sprintf("%s: %s", rel, msg)
				}
				msg = fmt.Sprintf("%s: %s", storageID, msg)
				idxWarnings = append(idxWarnings, msg)
			}

			if !quiet {
				fmt.Fprintln(stdout)
				printIndexHeader(stdout, mountName, idxMountCfg)
			}

			var idxProgressUI indexProgressAdapter
			if !quiet {
				p, err := startIndexProgress(ctx, stdout, mountName, idxMountCfg, "auto")
				if err != nil {
					return &CLIError{Code: ExitFail, Cmd: "maint", Headline: "failed to start index progress", Cause: rootCause(err)}
				}
				idxProgressUI = p
				idxHooks.Progress = idxProgressUI.OnProgress
			}

			idxRes, err := indexer.Run(ctx, mountName, idxMountCfg, idxDB.SQL(), idxHooks)
			if err != nil {
				if idxProgressUI != nil {
					idxProgressUI.Cancel()
				}
				if errors.Is(err, context.Canceled) {
					return &CLIError{Code: ExitInterrupted, Silent: true}
				}
				return &CLIError{Code: ExitFail, Cmd: "maint", Headline: "failed to index", Cause: rootCause(err)}
			}
			if idxProgressUI != nil {
				idxProgressUI.Finish()
			}
			if !quiet {
				printIndexSummary(stdout, idxRes, idxWarnings)
			}

			return nil
		},
	}

	cmd.Flags().StringVar(&job, "job", "", "run a specific mover job")
	cmd.Flags().BoolVar(&force, "force", false, "force run (ignore mover triggers)")
	cmd.Flags().IntVar(&limit, "limit", 0, "limit number of files moved")
	cmd.Flags().StringVar(&indexMode, "index", string(maintIndexModeTouch), "index mode: touch|all|off")
	cmd.Flags().BoolVar(&quiet, "quiet", false, "suppress success output")

	return cmd
}
