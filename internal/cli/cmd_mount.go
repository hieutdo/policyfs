package cli

import (
	"errors"
	"fmt"
	"os"

	"github.com/hanwen/go-fuse/v2/fs"
	"github.com/hanwen/go-fuse/v2/fuse"
	"github.com/spf13/cobra"
)

// newMountCmd creates `pfs mount`.
func newMountCmd(configPath *string) *cobra.Command {
	var debug bool
	var logFile string

	cmd := &cobra.Command{
		Use:   "mount <mount>",
		Short: "Start a PolicyFS FUSE daemon for a mount",
		Long: `Start a PolicyFS FUSE daemon for the specified mount configuration.

The daemon will run in the foreground until terminated (SIGTERM/SIGINT).
This command is typically managed by systemd as a service.`,
		Example: `  pfs mount media
  pfs mount media --debug
  pfs mount media -c /path/to/config.yaml`,
		Args: func(cmd *cobra.Command, args []string) error {
			if len(args) != 1 {
				return &CLIError{Code: ExitUsage, Cmd: "mount", Headline: "invalid arguments", Cause: errors.New("requires exactly 1 argument: <mount>"), Hint: "run 'pfs mount --help'"}
			}
			return nil
		},
		RunE: func(cmd *cobra.Command, args []string) error {
			mountName := args[0]

			rootCfg, mountCfg, source, err := loadAndResolveMount(*configPath, mountName)
			if err != nil {
				if isUsageError(err) {
					return &CLIError{Code: ExitUsage, Cmd: "mount", Headline: "invalid arguments", Cause: rootCause(err), Hint: "run 'pfs mount --help'"}
				}
				if errors.Is(err, os.ErrNotExist) || errors.Is(err, os.ErrPermission) {
					return newConfigLoadCLIError("mount", *configPath, err)
				}
				return &CLIError{Code: ExitFail, Cmd: "mount", Headline: fmt.Sprintf("invalid config: %s", *configPath), Cause: rootCause(err)}
			}

			// Configure logging and create command logger.
			cfgLog, closer, err := NewLogger(rootCfg.Log, logFile)
			if err != nil {
				var eolf *OpenLogFileError
				if errors.As(err, &eolf) {
					return &CLIError{Code: ExitFail, Cmd: "mount", Headline: "failed to open log file", Cause: rootCause(err)}
				}
				return &CLIError{Code: ExitFail, Cmd: "mount", Headline: fmt.Sprintf("invalid config: %s", *configPath), Cause: rootCause(err)}
			}
			if closer != nil {
				defer func() { _ = closer() }()
			}
			cmdLog := cfgLog.With().Str("component", "cli").Str("op", "mount").Logger()

			root, err := fs.NewLoopbackRoot(source)
			if err != nil {
				return &CLIError{Code: ExitFail, Cmd: "mount", Headline: fmt.Sprintf("invalid config: %s", *configPath), Cause: rootCause(err)}
			}

			options := []string{}
			if rootCfg.Fuse.AllowOther {
				options = append(options, "allow_other")
			}

			server, err := fs.Mount(mountCfg.MountPoint, root, &fs.Options{
				MountOptions: fuse.MountOptions{
					Debug:   debug,
					Name:    "policyfs",
					Options: options,
				},
			})
			if err != nil {
				if errors.Is(err, os.ErrPermission) {
					return &CLIError{Code: ExitFail, Cmd: "mount", Headline: "permission denied", Cause: rootCause(err)}
				}
				return &CLIError{Code: ExitFail, Cmd: "mount", Headline: "unexpected error", Cause: rootCause(err)}
			}

			cmdLog.Info().Str("mount", mountName).Str("mountpoint", mountCfg.MountPoint).Msg("mount ready")
			server.Wait()
			return nil
		},
	}

	cmd.Flags().BoolVar(&debug, "debug", false, "enable FUSE debug logging")
	cmd.Flags().StringVar(&logFile, "log-file", "", "path to log file (overrides PFS_LOG_FILE)")

	return cmd
}
