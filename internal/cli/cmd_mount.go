package cli

import (
	"context"
	"errors"
	"fmt"
	"os"
	"path/filepath"
	"strings"
	"sync"
	"time"

	"github.com/hanwen/go-fuse/v2/fs"
	gofuse "github.com/hanwen/go-fuse/v2/fuse"
	"github.com/hieutdo/policyfs/internal/config"
	"github.com/hieutdo/policyfs/internal/daemonctl"
	"github.com/hieutdo/policyfs/internal/errkind"
	pfsfuse "github.com/hieutdo/policyfs/internal/fuse"
	"github.com/hieutdo/policyfs/internal/indexdb"
	"github.com/hieutdo/policyfs/internal/lock"
	"github.com/spf13/cobra"
)

// newMountCmd creates `pfs mount`.
func newMountCmd(configPath *string) *cobra.Command {
	var fuseDebug bool
	var logFile string
	var logDiskAccess bool
	var dedupTTLSec int
	var diskAccessSummarySec int

	cmd := &cobra.Command{
		Use:   "mount <mount>",
		Short: "Start a PolicyFS FUSE daemon for a mount",
		Long: `Start a PolicyFS FUSE daemon for the specified mount configuration.

The daemon will run in the foreground until terminated (SIGTERM/SIGINT).
This command is typically managed by systemd as a service.`,
		Example: `  pfs mount media
  pfs mount media --fuse-debug
  pfs mount media -c /path/to/config.yaml`,
		Args: func(cmd *cobra.Command, args []string) error {
			if len(args) != 1 {
				return &CLIError{Code: ExitUsage, Cmd: "mount", Headline: "invalid arguments", Cause: errors.New("requires exactly 1 argument: <mount>"), Hint: "run 'pfs mount --help'"}
			}
			return nil
		},
		RunE: func(cmd *cobra.Command, args []string) error {
			mountName := args[0]
			if err := validateMountName(mountName); err != nil {
				return &CLIError{Code: ExitUsage, Cmd: "mount", Headline: "invalid arguments", Cause: rootCause(err), Hint: "run 'pfs mount --help'"}
			}

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
			effectiveLogCfg := mountCfg.EffectiveLogConfig(rootCfg.Log)
			cfgLog, closer, err := NewLogger(effectiveLogCfg, logFile)
			if err != nil {
				if _, ok := errors.AsType[*OpenLogFileError](err); ok {
					return &CLIError{Code: ExitFail, Cmd: "mount", Headline: "failed to open log file", Cause: rootCause(err)}
				}
				return &CLIError{Code: ExitFail, Cmd: "mount", Headline: fmt.Sprintf("invalid config: %s", *configPath), Cause: rootCause(err)}
			}
			if closer != nil {
				defer func() { _ = closer() }()
			}
			cmdLog := cfgLog.With().Str("component", "cli").Str("op", "mount").Logger()

			if os.Geteuid() != 0 {
				return &CLIError{Code: ExitFail, Cmd: "mount", Headline: "permission denied", Cause: errors.New("must be run as root"), Hint: "run as root"}
			}

			dlk, err := lock.AcquireMountLock(mountName, config.DefaultDaemonLockFile)
			if err != nil {
				if errors.Is(err, errkind.ErrBusy) {
					return &CLIError{Code: ExitBusy, Cmd: "mount", Headline: "daemon already running", Cause: err}
				}
				return &CLIError{Code: ExitFail, Cmd: "mount", Headline: "unexpected error", Cause: rootCause(err)}
			}
			defer func() { _ = dlk.Close() }()

			dedupTTLChanged := false
			if f := cmd.Flags().Lookup("dedup-ttl"); f != nil {
				dedupTTLChanged = f.Changed
			}
			summaryChanged := false
			if f := cmd.Flags().Lookup("disk-access-summary"); f != nil {
				summaryChanged = f.Changed
			}

			if dedupTTLSec < 0 {
				return &CLIError{Code: ExitUsage, Cmd: "mount", Headline: "invalid arguments", Cause: errors.New("--dedup-ttl must be >= 0"), Hint: "run 'pfs mount --help'"}
			}
			if diskAccessSummarySec < 0 {
				return &CLIError{Code: ExitUsage, Cmd: "mount", Headline: "invalid arguments", Cause: errors.New("--disk-access-summary must be >= 0"), Hint: "run 'pfs mount --help'"}
			}

			envDiskAccess := strings.TrimSpace(os.Getenv(config.EnvLogDiskAccess))
			diskAccessEnabled := logDiskAccess
			if !diskAccessEnabled && envDiskAccess != "" {
				switch strings.ToLower(envDiskAccess) {
				case "0", "false", "no", "off":
					// disabled
				default:
					diskAccessEnabled = true
				}
			}
			if !diskAccessEnabled && (dedupTTLChanged || summaryChanged) {
				cmdLog.Warn().
					Int("dedup_ttl_sec", dedupTTLSec).
					Int("disk_access_summary_sec", diskAccessSummarySec).
					Msg("disk access logging disabled")
			}

			diskCfg := pfsfuse.DiskAccessConfig{
				Enabled:         diskAccessEnabled,
				DedupTTL:        time.Duration(dedupTTLSec) * time.Second,
				SummaryInterval: time.Duration(diskAccessSummarySec) * time.Second,
			}

			var idxDB *indexdb.DB
			for _, sp := range mountCfg.StoragePaths {
				if sp.Indexed {
					idxDB, err = indexdb.Open(mountName)
					if err != nil {
						return &CLIError{Code: ExitFail, Cmd: "mount", Headline: "failed to open index db", Cause: rootCause(err)}
					}
					break
				}
			}
			if idxDB != nil {
				defer func() { _ = idxDB.Close() }()
			}

			root, err := pfsfuse.NewRootWithReload(mountName, mountCfg, source, idxDB, cfgLog, diskCfg, rootCfg.Fuse.AllowOther, rootCfg.Log)
			if err != nil {
				return &CLIError{Code: ExitFail, Cmd: "mount", Headline: fmt.Sprintf("invalid config: %s", *configPath), Cause: rootCause(err)}
			}

			options := []string{"default_permissions"}
			if rootCfg.Fuse.AllowOther {
				options = append(options, "allow_other")
			}

			server, err := fs.Mount(mountCfg.MountPoint, root, &fs.Options{
				MountOptions: gofuse.MountOptions{
					Debug:   fuseDebug,
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

			// Start daemon control socket for open-file awareness.
			mountRuntimeDir := config.MountRuntimeDir(mountName)
			if err := os.MkdirAll(mountRuntimeDir, 0o755); err != nil {
				return &CLIError{Code: ExitFail, Cmd: "mount", Headline: "failed to ensure runtime dir", Cause: rootCause(err)}
			}
			sockPath := filepath.Join(mountRuntimeDir, "daemon.sock")
			controlLog := cfgLog.With().Str("component", "fuse").Str("mount", mountName).Logger()
			controlCtx, controlCancel := context.WithCancel(context.Background())
			var controlSrv *daemonctl.Server
			if provider, ok := root.(daemonctl.OpenCountsProvider); ok {
				controlSrv, err = daemonctl.StartServer(controlCtx, sockPath, provider, controlLog)
				if err != nil {
					controlCancel()
					return &CLIError{Code: ExitFail, Cmd: "mount", Headline: "failed to start daemon control socket", Cause: rootCause(err)}
				}
			} else {
				controlCancel()
				cmdLog.Warn().Str("mount", mountName).Msg("daemon control socket unavailable")
			}
			defer func() {
				controlCancel()
				if controlSrv != nil {
					_ = controlSrv.Close()
				}
			}()

			idxDBPath := ""
			if idxDB != nil {
				idxDBPath = idxDB.Path
			}
			storageIDs := make([]string, 0, len(mountCfg.StoragePaths))
			indexedStorageIDs := make([]string, 0, len(mountCfg.StoragePaths))
			for _, sp := range mountCfg.StoragePaths {
				storageIDs = append(storageIDs, sp.ID)
				if sp.Indexed {
					indexedStorageIDs = append(indexedStorageIDs, sp.ID)
				}
			}

			cmdLog.Debug().
				Str("config_path", *configPath).
				Str("mount", mountName).
				Str("mountpoint", mountCfg.MountPoint).
				Str("source_root", source).
				Str("state_dir", config.StateDir()).
				Str("runtime_dir", config.RuntimeDir()).
				Str("mount_state_dir", config.MountStateDir(mountName)).
				Str("mount_runtime_dir", config.MountRuntimeDir(mountName)).
				Str("log_level", effectiveLogCfg.Level).
				Str("log_format", effectiveLogCfg.Format).
				Str("log_file", effectiveLogFilePath(effectiveLogCfg, logFile)).
				Bool("disk_access_enabled", diskCfg.Enabled).
				Int("disk_access_dedup_ttl_sec", dedupTTLSec).
				Int("disk_access_summary_sec", diskAccessSummarySec).
				Bool("index_db_enabled", idxDB != nil).
				Str("index_db_path", idxDBPath).
				Int("storage_count", len(storageIDs)).
				Int("indexed_storage_count", len(indexedStorageIDs)).
				Str("storage_ids", strings.Join(storageIDs, ",")).
				Str("indexed_storage_ids", strings.Join(indexedStorageIDs, ",")).
				Bool("fuse_allow_other", rootCfg.Fuse.AllowOther).
				Bool("fuse_debug", fuseDebug).
				Int("pid", os.Getpid()).
				Int("uid", os.Getuid()).
				Int("gid", os.Getgid()).
				Msg("mount runtime")

			cmdLog.Info().Str("mount", mountName).Str("mountpoint", mountCfg.MountPoint).Msg("mount ready")

			waitDone := make(chan struct{})
			go func() {
				server.Wait()
				close(waitDone)
			}()

			var shutdownOnce sync.Once
			reqShutdown := func() {
				shutdownOnce.Do(func() {
					controlCancel()
					if controlSrv != nil {
						_ = controlSrv.Close()
					}
					cmdLog.Info().Str("mount", mountName).Str("mountpoint", mountCfg.MountPoint).Msg("unmount requested")

					// Do not block on Unmount(): go-fuse can hang here in some failure modes.
					go func() { _ = server.Unmount() }()

					// Fail-safe: if go-fuse does not return from Wait() in time, exit before
					// systemd has to SIGKILL us (which leaves ugly logs and a failed Result=timeout).
					go func() {
						ctx := context.Background()
						ctx, cancel := context.WithTimeout(ctx, 8*time.Second)
						defer cancel()

						select {
						case <-waitDone:
							return
						case <-ctx.Done():
						}

						mounted, supported, err := isMountpointMounted(mountCfg.MountPoint)
						if err != nil {
							cmdLog.Error().Err(err).Str("mount", mountName).Str("mountpoint", mountCfg.MountPoint).Msg("shutdown timeout")
							flushCoverageIfEnabled(mountName, mountCfg.MountPoint)
							os.Exit(1)
						}
						if supported && mounted {
							cmdLog.Error().Str("mount", mountName).Str("mountpoint", mountCfg.MountPoint).Msg("shutdown timeout")
							flushCoverageIfEnabled(mountName, mountCfg.MountPoint)
							os.Exit(1)
						}
						cmdLog.Warn().Str("mount", mountName).Str("mountpoint", mountCfg.MountPoint).Msg("shutdown timeout but mountpoint already unmounted")
						flushCoverageIfEnabled(mountName, mountCfg.MountPoint)
						os.Exit(0)
					}()
				})
			}

			cmdCtx := cmd.Context()
			go func() {
				<-cmdCtx.Done()
				reqShutdown()
			}()

			// If the mount is torn down externally (e.g., ExecStop `pfs unmount`), ensure
			// the daemon exits even if go-fuse Wait() fails to return.
			go func() {
				t := time.NewTicker(500 * time.Millisecond)
				defer t.Stop()
				for {
					select {
					case <-waitDone:
						return
					case <-t.C:
						mounted, supported, err := isMountpointMounted(mountCfg.MountPoint)
						if err != nil {
							continue
						}
						if supported && !mounted {
							reqShutdown()
							return
						}
					}
				}
			}()

			<-waitDone
			flushCoverageIfEnabled(mountName, mountCfg.MountPoint)
			return nil
		},
	}

	cmd.Flags().BoolVar(&fuseDebug, "fuse-debug", false, "enable go-fuse internal debug logging (raw FUSE request/response dump)")
	cmd.Flags().StringVar(&logFile, "log-file", "", fmt.Sprintf("path to log file (overrides %s)", config.EnvLogFile))
	cmd.Flags().BoolVar(&logDiskAccess, "log-disk-access", false, fmt.Sprintf("enable disk access logging for indexed storage (debugging; can also set %s=1)", config.EnvLogDiskAccess))
	cmd.Flags().IntVar(&dedupTTLSec, "dedup-ttl", 60, "disk access log dedup TTL in seconds (0=disabled)")
	cmd.Flags().IntVar(&diskAccessSummarySec, "disk-access-summary", 60, "disk access summary interval in seconds (0=disabled)")

	return cmd
}
