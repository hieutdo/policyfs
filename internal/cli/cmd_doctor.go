package cli

import (
	"fmt"

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
  - Storage path references and groups
  - Routing rule validity and catch-all presence
  - Mount-specific validation (if mount name provided)

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
					Cause:    fmt.Errorf("requires at most 1 argument: none or <mount>"),
					Hint:     "run 'pfs doctor --help'",
				}
			}
			return nil
		},
		RunE: func(cmd *cobra.Command, args []string) error {
			var mountName *string
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
					fmt.Fprintln(cmd.OutOrStdout(), "FAIL")
					return &CLIError{
						Code:     ExitUsage,
						Cmd:      "doctor",
						Headline: "invalid arguments",
						Cause:    rootCause(err),
						Hint:     "run 'pfs doctor --help'",
					}
				}
				mountName = &m
			}
			cfg := *configPath
			scope := &JSONScope{Mount: mountName, Config: &cfg}

			rootCfg, err := loadRootConfig(*configPath)
			if err != nil {
				if jsonOut {
					env := JSONEnvelope{
						Command:  "doctor",
						OK:       false,
						Scope:    scope,
						Warnings: []JSONIssue{},
						Errors:   []JSONIssue{jsonIssueFromConfigLoadError(*configPath, err)},
					}
					return emitJSONAndExit(ExitFail, env)
				}
				fmt.Fprintln(cmd.OutOrStdout(), "FAIL")
				return newConfigLoadCLIError("doctor", *configPath, err)
			}

			validateErrs := validateConfigAll(rootCfg)
			issues := make([]JSONIssue, 0, len(validateErrs))
			for _, e := range validateErrs {
				issues = append(issues, JSONIssue{Message: rootCause(e).Error()})
			}

			if len(issues) == 0 && mountName != nil {
				if _, _, err := resolveMount(rootCfg, *mountName); err != nil {
					if jsonOut {
						env := JSONEnvelope{
							Command:  "doctor",
							OK:       false,
							Scope:    scope,
							Warnings: []JSONIssue{},
							Errors:   []JSONIssue{jsonIssueFromError("ARG_MOUNT", err, "run 'pfs doctor --help'")},
						}
						return emitJSONAndExit(ExitUsage, env)
					}
					fmt.Fprintln(cmd.OutOrStdout(), "FAIL")
					return &CLIError{
						Code:     ExitUsage,
						Cmd:      "doctor",
						Headline: "invalid arguments",
						Cause:    rootCause(err),
						Hint:     "run 'pfs doctor --help'",
					}
				}
			}

			ok := len(issues) == 0
			if jsonOut {
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

			if ok {
				fmt.Fprintln(cmd.OutOrStdout(), "OK")
				return nil
			}

			fmt.Fprintln(cmd.OutOrStdout(), "FAIL")
			for _, issue := range issues {
				fmt.Fprintln(cmd.OutOrStdout(), issue.Message)
			}
			fmt.Fprintf(cmd.OutOrStdout(), "%d issues.\n", len(issues))

			return &CLIError{
				Code:   ExitDoctorFail,
				Silent: true,
			}
		},
	}

	cmd.Flags().BoolVarP(&jsonOut, "json", "j", false, "output as JSON")

	return cmd
}
