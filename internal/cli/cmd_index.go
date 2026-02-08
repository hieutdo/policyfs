package cli

import (
	"context"
	"errors"
	"fmt"
	"io"
	"os"
	"sort"

	"github.com/hieutdo/policyfs/internal/errkind"
	"github.com/hieutdo/policyfs/internal/indexdb"
	"github.com/hieutdo/policyfs/internal/indexer"
	"github.com/hieutdo/policyfs/internal/lock"
	"github.com/spf13/cobra"
)

// JSONIndexOutput is the JSON output for `pfs index <mount> --json`.
type JSONIndexOutput struct {
	Command  string          `json:"command"`
	OK       bool            `json:"ok"`
	Scope    *JSONScope      `json:"scope,omitempty"`
	Result   *indexer.Result `json:"result,omitempty"`
	Warnings []JSONIssue     `json:"warnings"`
	Errors   []JSONIssue     `json:"errors"`
}

// newIndexCmd creates `pfs index`.
func newIndexCmd(configPath *string) *cobra.Command {
	var jsonOut bool
	var logFile string
	var storageID string
	var rebuild bool

	cmd := &cobra.Command{
		Use:   "index <mount>",
		Short: "Index metadata for indexed storage paths",
		Long: `Index indexed storage paths and write metadata to the per-mount SQLite database.

This enables metadata operations (lookup/readdir/getattr) to avoid touching disks for indexed paths.`,
		Example: `  pfs index media
  pfs index media --json
  pfs index media --rebuild
  pfs index media --storage hdd1`,
		Args: func(cmd *cobra.Command, args []string) error {
			if len(args) != 1 {
				return &CLIError{Code: ExitUsage, Cmd: "index", Headline: "invalid arguments", Cause: errors.New("requires exactly 1 argument: <mount>"), Hint: "run 'pfs index --help'"}
			}
			return nil
		},
		RunE: func(cmd *cobra.Command, args []string) error {
			mountName := args[0]

			rootCfg, mountCfg, _, err := loadAndResolveMount(*configPath, mountName)
			if err != nil {
				if isUsageError(err) {
					return &CLIError{Code: ExitUsage, Cmd: "index", Headline: "invalid arguments", Cause: rootCause(err), Hint: "run 'pfs index --help'"}
				}
				if errors.Is(err, os.ErrNotExist) || errors.Is(err, os.ErrPermission) {
					return newConfigLoadCLIError("index", *configPath, err)
				}
				return &CLIError{Code: ExitFail, Cmd: "index", Headline: fmt.Sprintf("invalid config: %s", *configPath), Cause: rootCause(err)}
			}

			jobLog, closer, err := NewJobLogger(rootCfg.Log, logFile)
			if err != nil {
				var eolf *OpenLogFileError
				if errors.As(err, &eolf) {
					return &CLIError{Code: ExitFail, Cmd: "index", Headline: "failed to open log file", Cause: rootCause(err)}
				}
				return &CLIError{Code: ExitFail, Cmd: "index", Headline: fmt.Sprintf("invalid config: %s", *configPath), Cause: rootCause(err)}
			}
			if closer != nil {
				defer func() { _ = closer() }()
			}

			lk, err := lock.AcquireMountLock(mountName, "job.lock")
			if err != nil {
				if errors.Is(err, errkind.ErrBusy) {
					if jsonOut {
						cfg := *configPath
						scope := &JSONScope{Mount: &mountName, Config: &cfg}
						out := JSONIndexOutput{
							Command:  "index",
							OK:       false,
							Scope:    scope,
							Warnings: []JSONIssue{},
							Errors:   []JSONIssue{jsonIssue("LOCK_BUSY", "another job already running", "")},
						}
						return emitJSONAndExit(ExitBusy, out)
					}
					return &CLIError{Code: ExitBusy, Cmd: "index", Headline: "job already running", Cause: err}
				}
				return &CLIError{Code: ExitFail, Cmd: "index", Headline: "unexpected error", Cause: rootCause(err)}
			}
			defer func() { _ = lk.Close() }()

			if rebuild {
				dlk, err := lock.AcquireMountLock(mountName, "daemon.lock")
				if err != nil {
					if errors.Is(err, errkind.ErrBusy) {
						return &CLIError{Code: ExitFail, Cmd: "index", Headline: "cannot reset while daemon is running", Cause: err, Hint: "stop 'pfs mount' and try again"}
					}
					return &CLIError{Code: ExitFail, Cmd: "index", Headline: "unexpected error", Cause: rootCause(err)}
				}
				if err := indexdb.Reset(mountName); err != nil {
					_ = dlk.Close()
					return &CLIError{Code: ExitFail, Cmd: "index", Headline: "failed to reset index db", Cause: rootCause(err)}
				}
				_ = dlk.Close()
			}

			idxDB, err := indexdb.Open(mountName)
			if err != nil {
				return &CLIError{Code: ExitFail, Cmd: "index", Headline: "failed to open index db", Cause: rootCause(err)}
			}
			defer func() { _ = idxDB.Close() }()

			if storageID != "" {
				found := false
				out := *mountCfg
				out.StoragePaths = nil
				for _, sp := range mountCfg.StoragePaths {
					if sp.ID == storageID {
						found = true
						if !sp.Indexed {
							return &CLIError{Code: ExitUsage, Cmd: "index", Headline: "invalid arguments", Cause: fmt.Errorf("storage %q is not indexed", storageID), Hint: "run 'pfs index --help'"}
						}
						out.StoragePaths = append(out.StoragePaths, sp)
					}
				}
				if !found {
					return &CLIError{Code: ExitUsage, Cmd: "index", Headline: "invalid arguments", Cause: fmt.Errorf("unknown storage id: %s", storageID), Hint: "run 'pfs index --help'"}
				}
				mountCfg = &out
			}

			ctx := context.Background()
			res, err := indexer.Run(ctx, mountName, mountCfg, idxDB.SQL(), jobLog)
			if err != nil {
				return &CLIError{Code: ExitFail, Cmd: "index", Headline: "unexpected error", Cause: rootCause(err)}
			}

			if jsonOut {
				cfg := *configPath
				scope := &JSONScope{Mount: &mountName, Config: &cfg}
				out := JSONIndexOutput{Command: "index", OK: true, Scope: scope, Result: &res, Warnings: []JSONIssue{}, Errors: []JSONIssue{}}
				if err := writeJSON(out); err != nil {
					return &CLIError{Code: ExitFail, Cmd: "index", Headline: "failed to write json", Cause: rootCause(err)}
				}
				return nil
			}

			printIndexSummary(cmd.OutOrStdout(), res)
			return nil
		},
	}

	cmd.Flags().BoolVarP(&jsonOut, "json", "j", false, "output as JSON")
	cmd.Flags().StringVar(&logFile, "log-file", "", "path to log file (overrides PFS_LOG_FILE)")
	cmd.Flags().StringVar(&storageID, "storage", "", "index a specific storage id")
	cmd.Flags().BoolVar(&rebuild, "rebuild", false, "delete index db before indexing")
	cmd.Flags().BoolVar(&rebuild, "reset", false, "alias for --rebuild")

	return cmd
}

// printIndexSummary prints a deterministic, human-readable summary of an index run.
func printIndexSummary(w io.Writer, res indexer.Result) {
	if w == nil {
		return
	}

	fmt.Fprintln(w, "OK")
	fmt.Fprintf(w, "mount: %s\n", res.Mount)

	paths := append([]indexer.StorageResult{}, res.StoragePaths...)
	sort.Slice(paths, func(i, j int) bool { return paths[i].ID < paths[j].ID })

	for _, sp := range paths {
		fmt.Fprintf(
			w,
			"%s: files=%d bytes=%d stale_removed=%d dur_ms=%d warnings=%d errors=%d\n",
			sp.ID,
			sp.FilesIndexed,
			sp.BytesIndexed,
			sp.StaleRemoved,
			sp.DurationMS,
			sp.Warnings,
			sp.Errors,
		)
	}
	fmt.Fprintf(w, "total: files=%d bytes=%d dur_ms=%d\n", res.TotalFiles, res.TotalBytes, res.TotalDurationMS)
}
