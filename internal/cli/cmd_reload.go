package cli

import (
	"errors"
	"fmt"
	"os"
	"path/filepath"

	"github.com/hieutdo/policyfs/internal/config"
	"github.com/hieutdo/policyfs/internal/daemonctl"
	"github.com/spf13/cobra"
)

// newReloadCmd creates `pfs reload`.
func newReloadCmd(configPath *string) *cobra.Command {
	cmd := &cobra.Command{
		Use:   "reload <mount>",
		Short: "Hot-reload mount-scoped config in a running daemon",
		Long: `Request that a running PolicyFS daemon reloads mount-scoped configuration.

Reloadable fields include routing rules and mounts.<name>.log.level.
Non-reloadable changes (mountpoint, storage paths, FUSE options, log format/file) are rejected.`,
		Example: `  pfs reload media
  pfs reload media --config /etc/pfs/pfs.yaml`,
		Args: func(cmd *cobra.Command, args []string) error {
			if len(args) != 1 {
				return &CLIError{Code: ExitUsage, Cmd: "reload", Headline: "invalid arguments", Cause: errors.New("requires exactly 1 argument: <mount>"), Hint: "run 'pfs reload --help'"}
			}
			return nil
		},
		RunE: func(cmd *cobra.Command, args []string) error {
			cfgPath := ""
			if configPath != nil {
				cfgPath = *configPath
			}
			mountName := args[0]
			if err := validateMountName(mountName); err != nil {
				return &CLIError{Code: ExitUsage, Cmd: "reload", Headline: "invalid arguments", Cause: rootCause(err), Hint: "run 'pfs reload --help'"}
			}

			rootCfg, mountCfg, _, err := loadAndResolveMount(cfgPath, mountName)
			if err != nil {
				if isUsageError(err) {
					return &CLIError{Code: ExitUsage, Cmd: "reload", Headline: "invalid arguments", Cause: rootCause(err), Hint: "run 'pfs reload --help'"}
				}
				if errors.Is(err, os.ErrNotExist) || errors.Is(err, os.ErrPermission) {
					return newConfigLoadCLIError("reload", cfgPath, err)
				}
				return &CLIError{Code: ExitFail, Cmd: "reload", Headline: fmt.Sprintf("invalid config: %s", cfgPath), Cause: rootCause(err)}
			}

			effectiveLogCfg := mountCfg.EffectiveLogConfig(rootCfg.Log)
			cmdLogBase, closer, err := NewLogger(effectiveLogCfg, "")
			if err != nil {
				if _, ok := errors.AsType[*OpenLogFileError](err); ok {
					return &CLIError{Code: ExitFail, Cmd: "reload", Headline: "failed to open log file", Cause: rootCause(err)}
				}
				return &CLIError{Code: ExitFail, Cmd: "reload", Headline: fmt.Sprintf("invalid config: %s", cfgPath), Cause: rootCause(err)}
			}
			if closer != nil {
				defer func() { _ = closer() }()
			}
			cmdLog := cmdLogBase.With().Str("component", "cli").Str("op", "reload").Str("mount", mountName).Logger()

			sockPath := filepath.Join(config.MountRuntimeDir(mountName), "daemon.sock")
			changed, changedFields, err := daemonctl.Reload(cmd.Context(), sockPath, cfgPath)
			if err != nil {
				if errors.Is(err, daemonctl.ErrDialDaemonSocket) {
					return &CLIError{Code: ExitFail, Cmd: "reload", Headline: "failed to connect to daemon", Cause: rootCause(err), Hint: "ensure the daemon is running"}
				}
				if errors.Is(err, daemonctl.ErrRemote) {
					return &CLIError{Code: ExitFail, Cmd: "reload", Headline: "reload failed", Cause: rootCause(err)}
				}
				return &CLIError{Code: ExitFail, Cmd: "reload", Headline: "reload failed", Cause: rootCause(err)}
			}
			if !changed {
				cmdLog.Info().Msg("reload no changes")
				return &CLIError{Code: ExitNoChanges, Silent: true}
			}
			if len(changedFields) > 0 {
				cmdLog.Info().Strs("changed_fields", changedFields).Msg("reload applied")
				return nil
			}
			cmdLog.Info().Msg("reload applied")
			return nil
		},
	}

	return cmd
}
