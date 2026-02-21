package cli

import (
	"errors"
	"fmt"
	"slices"
	"sort"

	"github.com/spf13/cobra"
)

// newDoctorCmd creates `pfs doctor`.
func newDoctorCmd(configPath *string) *cobra.Command {
	var jsonOut bool

	cmd := &cobra.Command{
		Use:   "doctor [mount]",
		Short: "Run health checks on configuration and runtime",
		Long: `Run health checks to validate configuration and runtime state.

Checks include:
  - Configuration file syntax and structure
  - Storage path accessibility and free space
  - Mount daemon and lock status
  - Index stats (last indexed, file count, size)
  - Pending deferred events (DELETE/RENAME/SETATTR)
  - Disk access analysis (top processes and storages)

Exit codes:
  0  - All checks passed
  78 - Validation errors found`,
		Example: `  pfs doctor
  pfs doctor media
  pfs doctor --json
  pfs doctor media --json`,
		Args: func(cmd *cobra.Command, args []string) error {
			if len(args) > 1 {
				return &CLIError{
					Code:     ExitUsage,
					Cmd:      "doctor",
					Headline: "invalid arguments",
					Cause:    errors.New("requires at most 1 argument: none or <mount>"),
					Hint:     "run 'pfs doctor --help'",
				}
			}
			return nil
		},
		RunE: func(cmd *cobra.Command, args []string) error {
			var filterMount *string
			if len(args) == 1 {
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
