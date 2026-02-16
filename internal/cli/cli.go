package cli

import (
	"errors"
	"fmt"
	"os"
	"strings"

	"github.com/hieutdo/policyfs/internal/config"
	"github.com/spf13/cobra"
)

const (
	ExitOK         = 0
	ExitFail       = 1
	ExitUsage      = 2
	ExitBusy       = 75
	ExitDoctorFail = 78
)

// Execute runs the CLI with the given args and returns a process exit code.
func Execute(args []string) int {
	root := newRootCmd()
	root.SetArgs(args)

	if err := root.Execute(); err != nil {
		var ce *CLIError
		if errors.As(err, &ce) {
			code := ce.Code
			if code == 0 && !ce.Silent {
				code = ExitFail
			}
			if ce.Silent {
				return code
			}
			for _, line := range ce.Lines() {
				fmt.Fprintln(os.Stderr, line)
			}
			return code
		}

		code := ExitFail
		headline := "unexpected error"
		hint := ""

		if strings.HasPrefix(err.Error(), "unknown command") {
			code = ExitUsage
			headline = "invalid arguments"
			hint = "run 'pfs --help'"
		}

		ce = &CLIError{Code: code, Headline: headline, Cause: err, Hint: hint}

		for _, line := range ce.Lines() {
			fmt.Fprintln(os.Stderr, line)
		}
		return code
	}

	return ExitOK
}

// newRootCmd constructs the root cobra command for pfs.
func newRootCmd() *cobra.Command {
	var configPath string
	defaultConfigPath := config.ConfigFilePath()

	cmd := &cobra.Command{
		Use:   "pfs",
		Short: "PolicyFS (pfs) - rule based filesystem",
		Long: `A FUSE filesystem that merges multiple storage roots using matching rules.
Optional indexing reduces disk wake-ups during metadata reads.`,
		SilenceUsage:       true,
		SilenceErrors:      true,
		DisableSuggestions: true,
	}

	cmd.SetFlagErrorFunc(func(cmd *cobra.Command, err error) error {
		cmdName := strings.TrimSpace(strings.TrimPrefix(cmd.CommandPath(), "pfs"))
		cmdName = strings.TrimSpace(cmdName)
		hint := "run 'pfs --help'"
		if cmdName != "" {
			hint = fmt.Sprintf("run 'pfs %s --help'", cmdName)
		}
		return &CLIError{Code: ExitUsage, Cmd: cmdName, Headline: "invalid arguments", Cause: err, Hint: hint}
	})

	cmd.PersistentFlags().StringVarP(&configPath, "config", "c", defaultConfigPath, "path to config file")

	cmd.AddCommand(newMountCmd(&configPath))
	cmd.AddCommand(newIndexCmd(&configPath))
	cmd.AddCommand(newMoveCmd(&configPath))
	cmd.AddCommand(newPruneCmd(&configPath))
	cmd.AddCommand(newDoctorCmd(&configPath))
	cmd.AddCommand(newVersionCmd())

	return cmd
}
