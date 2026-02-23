package cli

import (
	"errors"
	"fmt"
	"os"
	"path/filepath"
	"slices"
	"sort"
	"strings"

	"github.com/hieutdo/policyfs/internal/config"
	"github.com/hieutdo/policyfs/internal/indexdb"
	"github.com/spf13/cobra"
)

// newDoctorCmd creates `pfs doctor`.
func newDoctorCmd(configPath *string) *cobra.Command {
	var jsonOut bool

	cmd := &cobra.Command{
		Use:   "doctor [mount] [path]",
		Short: "Run health checks on configuration and runtime",
		Long: `Run health checks to validate configuration and runtime state.

When called with just a mount name (or no arguments), runs health checks
including config validation, storage accessibility, daemon status, index
stats, pending events, and disk access analysis.

When called with a mount name and a file path, inspects that specific file
across all storages showing index metadata, pending events, and disk state.

Exit codes:
  0  - All checks passed (or file inspect succeeded)
  78 - Validation errors found`,
		Example: `  pfs doctor
  pfs doctor media
  pfs doctor media --json
  pfs doctor media library/movies/MovieA/MovieA.mkv`,
		Args: func(cmd *cobra.Command, args []string) error {
			if len(args) > 2 {
				return &CLIError{
					Code:     ExitUsage,
					Cmd:      "doctor",
					Headline: "invalid arguments",
					Cause:    errors.New("requires at most 2 arguments: [mount] [path]"),
					Hint:     "run 'pfs doctor --help'",
				}
			}
			return nil
		},
		// ValidArgsFunction provides dynamic shell completion.
		ValidArgsFunction: func(cmd *cobra.Command, args []string, toComplete string) ([]string, cobra.ShellCompDirective) {
			// Re-parse from COMP_LINE when available. Bash's COMP_WORDS
			// splits at COMP_WORDBREAKS (including space) without respecting
			// backslash escaping, so paths like "dir\ name" get split into
			// multiple words. Our patched bash script sets _PFS_COMP_LINE.
			if clArgs, clTC, ok := compLineArgs("doctor"); ok {
				args = clArgs
				toComplete = clTC
			}

			cfgPath, err := cmd.Root().PersistentFlags().GetString("config")
			if err != nil {
				return nil, cobra.ShellCompDirectiveNoFileComp
			}
			rootCfg, err := loadRootConfig(cfgPath)
			if err != nil {
				return nil, cobra.ShellCompDirectiveNoFileComp
			}

			// 1st arg: mount name.
			if len(args) == 0 {
				names := make([]string, 0, len(rootCfg.Mounts))
				for name := range rootCfg.Mounts {
					if strings.HasPrefix(name, toComplete) {
						names = append(names, name)
					}
				}
				sort.Strings(names)
				return names, cobra.ShellCompDirectiveNoFileComp
			}

			// 2nd arg: virtual path (completion from index DB).
			if len(args) == 1 {
				mountName := args[0]
				mountCfg, ok := rootCfg.Mounts[mountName]
				if !ok {
					return nil, cobra.ShellCompDirectiveNoFileComp
				}

				dirPath, namePrefix := splitVirtualDirAndPrefix(toComplete)
				children, err := indexdb.QueryVirtualChildren(mountName, dirPath, namePrefix, 200)
				if err != nil {
					return nil, cobra.ShellCompDirectiveNoFileComp
				}

				// Fallback: if the index DB has no children for this directory, try reading
				// from non-indexed storage roots on disk.
				if len(children) == 0 {
					if diskChildren, err := queryVirtualChildrenFromDiskNonIndexed(mountCfg, dirPath, namePrefix, 200); err == nil {
						children = diskChildren
					}
				}
				if len(children) == 0 {
					return nil, cobra.ShellCompDirectiveNoFileComp
				}

				out := make([]string, 0, len(children))
				for _, c := range children {
					p := c.Name
					if dirPath != "" {
						p = dirPath + "/" + c.Name
					}
					if c.IsDir {
						p += "/"
					}
					out = append(out, p)
				}
				return out, cobra.ShellCompDirectiveNoSpace | cobra.ShellCompDirectiveNoFileComp
			}

			return nil, cobra.ShellCompDirectiveNoFileComp
		},
		RunE: func(cmd *cobra.Command, args []string) error {
			var filterMount *string
			if len(args) >= 1 {
				m := args[0]
				if err := validateMountName(m); err != nil {
					cfg := *configPath
					scope := &JSONScope{Mount: &m, Config: &cfg}
					env := JSONEnvelope{
						Command:  "doctor",
						OK:       false,
						Scope:    scope,
						Warnings: []JSONIssue{},
						Errors:   []JSONIssue{jsonIssueFromError("ARG_MOUNT", err, "run 'pfs doctor --help'")},
					}
					if jsonOut {
						return emitJSONAndExit(ExitUsage, env)
					}
					return &CLIError{
						Code:     ExitUsage,
						Cmd:      "doctor",
						Headline: "invalid arguments",
						Cause:    rootCause(err),
						Hint:     "run 'pfs doctor --help'",
					}
				}
				filterMount = &m
			}

			cfgPath := *configPath
			stdout := cmd.OutOrStdout()

			// --- File inspect mode ---
			if len(args) == 2 {
				return runDoctorFileInspect(stdout, jsonOut, *filterMount, args[1], cfgPath)
			}

			// --- Build report ---
			report := doctorReport{ConfigPath: cfgPath}

			// 1. Load config.
			rootCfg, loadErr := loadRootConfig(cfgPath)
			report.ConfigChecks = append(report.ConfigChecks, checkConfigLoaded(cfgPath, loadErr))

			if loadErr != nil {
				report.IssueCount++

				if jsonOut {
					scope := &JSONScope{Mount: filterMount, Config: &cfgPath}
					env := JSONEnvelope{
						Command:  "doctor",
						OK:       false,
						Scope:    scope,
						Warnings: []JSONIssue{},
						Errors:   []JSONIssue{jsonIssueFromConfigLoadError(cfgPath, loadErr)},
					}
					return emitJSONAndExit(ExitFail, env)
				}

				printDoctorReport(stdout, report)
				return &CLIError{Code: ExitDoctorFail, Silent: true}
			}

			// 2. Validate config — per-mount checks.
			validateErrs := validateConfigAll(rootCfg)

			// Collect mount names in sorted order.
			mountNames := make([]string, 0, len(rootCfg.Mounts))
			for name := range rootCfg.Mounts {
				mountNames = append(mountNames, name)
			}
			sort.Strings(mountNames)

			// If a specific mount was requested, validate it exists.
			if filterMount != nil {
				found := slices.Contains(mountNames, *filterMount)
				if !found {
					if jsonOut {
						scope := &JSONScope{Mount: filterMount, Config: &cfgPath}
						env := JSONEnvelope{
							Command:  "doctor",
							OK:       false,
							Scope:    scope,
							Warnings: []JSONIssue{},
							Errors:   []JSONIssue{jsonIssueFromError("ARG_MOUNT", fmt.Errorf("mount %q not found", *filterMount), "run 'pfs doctor --help'")},
						}
						return emitJSONAndExit(ExitUsage, env)
					}
					return &CLIError{
						Code:     ExitUsage,
						Cmd:      "doctor",
						Headline: "invalid arguments",
						Cause:    fmt.Errorf("mount %q not found", *filterMount),
						Hint:     "run 'pfs doctor --help'",
					}
				}
				mountNames = []string{*filterMount}
			}

			// Per-mount config checks.
			for _, name := range mountNames {
				checks := configChecksForMount(name, validateErrs)
				report.ConfigChecks = append(report.ConfigChecks, checks...)
			}

			// Count config issues.
			for _, c := range report.ConfigChecks {
				if !c.Pass {
					report.IssueCount++
				}
			}

			// Determine which mounts have valid config.
			mountValid := map[string]bool{}
			for _, name := range mountNames {
				mountValid[name] = true
			}
			for _, e := range validateErrs {
				var me *mountConfigError
				if errors.As(e, &me) && me != nil {
					if _, ok := mountValid[me.Mount]; ok {
						mountValid[me.Mount] = false
					}
				}
			}

			// 3. Per-mount runtime checks.
			for _, name := range mountNames {
				m := mountReport{
					Name:        name,
					ConfigValid: mountValid[name],
				}

				if !m.ConfigValid {
					report.Mounts = append(report.Mounts, m)
					continue
				}

				mountCfg := rootCfg.Mounts[name]
				logPath := resolveDoctorLogFilePath(name, rootCfg.Log)

				// Status probes.
				m.Daemon = checkDaemonLock(name)
				m.Mountpoint = checkMountpointAccessible(mountCfg.MountPoint)
				m.JobLock = checkJobLock(name)

				// Files (informational).
				m.IndexDB = checkIndexDBFile(name)
				m.LogFile = checkLogFile(logPath)

				// Systemd timers (best-effort).
				m.SystemdTimers = querySystemdTimers(name)

				// Storage checks (informational — does not affect exit code).
				for _, sp := range mountCfg.StoragePaths {
					m.Storages = append(m.Storages, checkStorage(sp))
				}

				// Index stats for indexed storages.
				for _, sp := range mountCfg.StoragePaths {
					if !sp.Indexed {
						continue
					}
					if stats := queryIndexStats(name, sp.ID); stats != nil {
						m.IndexStats = append(m.IndexStats, *stats)
					}
				}

				// Pending events.
				if pe, err := countPendingEvents(name, 1000); err == nil && pe != nil {
					m.PendingEvents = pe
				}

				// Disk access.
				if da, err := analyzeDiskAccess(logPath, 1000); err == nil && da != nil {
					m.DiskAccess = da
				}

				report.Mounts = append(report.Mounts, m)
			}

			// 4. Suggestions.
			report.Suggestions = generateSuggestions(&report)

			// 5. Output.
			if jsonOut {
				issues := make([]JSONIssue, 0)
				for _, c := range report.ConfigChecks {
					if !c.Pass {
						msg := c.Name
						if c.Detail != "" {
							msg += ": " + c.Detail
						}
						issues = append(issues, JSONIssue{Message: msg})
					}
				}
				ok := report.IssueCount == 0
				scope := &JSONScope{Mount: filterMount, Config: &cfgPath}
				env := JSONEnvelope{
					Command:  "doctor",
					OK:       ok,
					Scope:    scope,
					Issues:   issues,
					Warnings: []JSONIssue{},
					Errors:   []JSONIssue{},
				}
				if ok {
					if err := writeJSON(env); err != nil {
						return &CLIError{
							Code:     ExitFail,
							Cmd:      "doctor",
							Headline: "failed to write json",
							Cause:    rootCause(err),
						}
					}
					return nil
				}
				return emitJSONAndExit(ExitDoctorFail, env)
			}

			printDoctorReport(stdout, report)

			if report.IssueCount > 0 {
				return &CLIError{Code: ExitDoctorFail, Silent: true}
			}
			return nil
		},
	}

	cmd.Flags().BoolVarP(&jsonOut, "json", "j", false, "output as JSON")

	return cmd
}

// splitVirtualDirAndPrefix splits a virtual path fragment into (dirPath, namePrefix)
// suitable for indexdb parent_dir + name prefix queries.
func splitVirtualDirAndPrefix(toComplete string) (string, string) {
	if toComplete == "" {
		return "", ""
	}
	if strings.HasSuffix(toComplete, "/") {
		return indexdb.NormalizeVirtualPath(toComplete), ""
	}

	norm := indexdb.NormalizeVirtualPath(toComplete)
	if norm == "" {
		return "", ""
	}

	idx := strings.LastIndex(norm, "/")
	if idx < 0 {
		return "", norm
	}
	return norm[:idx], norm[idx+1:]
}

// queryVirtualChildrenFromDiskNonIndexed lists distinct child entries for one virtual directory
// across non-indexed storage roots, using os.ReadDir.
//
// This is a completion-only fallback for mounts that have paths present on disk but absent
// in the index DB because the storage is configured with indexed=false.
func queryVirtualChildrenFromDiskNonIndexed(mountCfg config.MountConfig, dirPath string, namePrefix string, limit int) ([]indexdb.VirtualChildEntry, error) {
	if limit <= 0 {
		limit = 100
	}
	if limit > 1000 {
		limit = 1000
	}

	// Use a map for dedupe across storages. If any storage reports a name as dir,
	// treat it as dir.
	byName := make(map[string]bool)
	for _, sp := range mountCfg.StoragePaths {
		if sp.Indexed {
			continue
		}

		physicalDir := filepath.Join(sp.Path, dirPath)
		entries, err := os.ReadDir(physicalDir)
		if err != nil {
			continue
		}

		for _, e := range entries {
			name := e.Name()
			if name == "" {
				continue
			}
			if namePrefix != "" && !strings.HasPrefix(name, namePrefix) {
				continue
			}

			isDir := false
			if e.Type().IsDir() {
				isDir = true
			} else if e.Type() == 0 {
				// Best-effort fallback for filesystems that don't return type info
				// without an extra stat.
				isDir = e.IsDir()
			}

			if prev, ok := byName[name]; ok {
				byName[name] = prev || isDir
				continue
			}
			byName[name] = isDir
		}
	}

	if len(byName) == 0 {
		return nil, nil
	}

	out := make([]indexdb.VirtualChildEntry, 0, len(byName))
	for name, isDir := range byName {
		out = append(out, indexdb.VirtualChildEntry{Name: name, IsDir: isDir})
	}
	sort.Slice(out, func(i, j int) bool { return out[i].Name < out[j].Name })
	if len(out) > limit {
		out = out[:limit]
	}
	return out, nil
}
