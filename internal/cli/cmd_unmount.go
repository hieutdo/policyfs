package cli

import (
	"context"
	"errors"
	"fmt"
	"io"
	"os"
	"os/exec"
	"path/filepath"
	"strconv"
	"strings"
	"time"

	"github.com/spf13/cobra"
)

// newUnmountCmd creates `pfs unmount`.
func newUnmountCmd(configPath *string) *cobra.Command {
	cmd := &cobra.Command{
		Use:   "unmount <mount>",
		Short: "Unmount a PolicyFS mount and request daemon shutdown",
		Long: `Request unmount and clean shutdown (best-effort).

This is designed for systemd ExecStop and for manual recovery when a mount is stuck.`,
		Example: `  pfs unmount media
  pfs unmount media --config /etc/pfs/pfs.yaml`,
		Args: func(cmd *cobra.Command, args []string) error {
			if len(args) != 1 {
				return &CLIError{Code: ExitUsage, Cmd: "unmount", Headline: "invalid arguments", Cause: errors.New("requires exactly 1 argument: <mount>"), Hint: "run 'pfs unmount --help'"}
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
				return &CLIError{Code: ExitUsage, Cmd: "unmount", Headline: "invalid arguments", Cause: rootCause(err), Hint: "run 'pfs unmount --help'"}
			}

			rootCfg, err := loadRootConfig(cfgPath)
			if err != nil {
				if errors.Is(err, os.ErrNotExist) || errors.Is(err, os.ErrPermission) {
					return newConfigLoadCLIError("unmount", cfgPath, err)
				}
				return &CLIError{Code: ExitFail, Cmd: "unmount", Headline: fmt.Sprintf("invalid config: %s", cfgPath), Cause: rootCause(err)}
			}

			mountCfg, err := rootCfg.Mount(mountName)
			if err != nil {
				return &CLIError{Code: ExitUsage, Cmd: "unmount", Headline: "invalid arguments", Cause: rootCause(err), Hint: "run 'pfs unmount --help'"}
			}

			mountPoint := strings.TrimSpace(mountCfg.MountPoint)
			if mountPoint == "" {
				return &CLIError{Code: ExitFail, Cmd: "unmount", Headline: "invalid config", Cause: errors.New("mountpoint is required")}
			}

			mounted, supported, err := isMountpointMounted(mountPoint)
			if err != nil {
				return &CLIError{Code: ExitFail, Cmd: "unmount", Headline: "failed to check mountpoint", Cause: rootCause(err)}
			}
			if supported && !mounted {
				return nil
			}

			succeeded, err := unmountMountpoint(cmd.Context(), mountPoint)
			if err != nil {
				return &CLIError{Code: ExitFail, Cmd: "unmount", Headline: "failed to unmount", Cause: rootCause(err)}
			}
			if succeeded {
				return nil
			}
			if !supported {
				return nil
			}

			mounted, supported, err = isMountpointMounted(mountPoint)
			if err != nil {
				return &CLIError{Code: ExitFail, Cmd: "unmount", Headline: "failed to check mountpoint", Cause: rootCause(err)}
			}
			if supported && !mounted {
				return nil
			}

			return &CLIError{Code: ExitFail, Cmd: "unmount", Headline: "failed to unmount", Cause: fmt.Errorf("mountpoint still mounted: %s", filepath.Clean(mountPoint))}
		},
	}

	return cmd
}

// isMountpointMounted returns whether mountPoint is mounted, and whether the check is supported.
func isMountpointMounted(mountPoint string) (mounted bool, supported bool, err error) {
	if strings.TrimSpace(mountPoint) == "" {
		return false, true, nil
	}
	if ok, supported, err := isMountpointMountedProc(mountPoint); supported {
		return ok, true, err
	}
	c := exec.Command("mountpoint", "-q", mountPoint)
	err = c.Run()
	if err == nil {
		return true, true, nil
	}
	var execErr *exec.Error
	if errors.As(err, &execErr) {
		if errors.Is(execErr.Err, exec.ErrNotFound) {
			return false, false, nil
		}
	}
	var exitErr *exec.ExitError
	if errors.As(err, &exitErr) {
		switch exitErr.ExitCode() {
		case 1, 32:
			return false, true, nil
		}
	}
	return false, true, fmt.Errorf("mountpoint failed: %w", err)
}

// isMountpointMountedProc checks mount state by reading /proc/self/mountinfo (Linux).
func isMountpointMountedProc(mountPoint string) (mounted bool, supported bool, err error) {
	mountPoint = filepath.Clean(strings.TrimSpace(mountPoint))
	if mountPoint == "" {
		return false, true, nil
	}

	f, err := os.Open("/proc/self/mountinfo")
	if err != nil {
		return false, false, nil
	}
	defer func() { _ = f.Close() }()

	b, err := io.ReadAll(f)
	if err != nil {
		return false, true, fmt.Errorf("failed to read mountinfo: %w", err)
	}

	lines := strings.Split(string(b), "\n")
	for _, line := range lines {
		line = strings.TrimSpace(line)
		if line == "" {
			continue
		}
		fields := strings.Fields(line)
		if len(fields) < 5 {
			continue
		}
		mp, err := unescapeMountinfoPath(fields[4])
		if err != nil {
			continue
		}
		if filepath.Clean(mp) == mountPoint {
			return true, true, nil
		}
	}
	return false, true, nil
}

// unescapeMountinfoPath decodes Linux mountinfo escape sequences (e.g., "\040" for space).
func unescapeMountinfoPath(s string) (string, error) {
	if strings.IndexByte(s, '\\') < 0 {
		return s, nil
	}

	var out strings.Builder
	out.Grow(len(s))
	for i := 0; i < len(s); i++ {
		ch := s[i]
		if ch != '\\' {
			out.WriteByte(ch)
			continue
		}
		if i+3 >= len(s) {
			return "", fmt.Errorf("invalid escape")
		}
		v, err := strconv.ParseInt(s[i+1:i+4], 8, 32)
		if err != nil {
			return "", fmt.Errorf("failed to parse mountinfo escape: %w", err)
		}
		out.WriteByte(byte(v))
		i += 3
	}
	return out.String(), nil
}

// unmountMountpoint attempts a best-effort unmount of mountPoint using common Linux tools.
func unmountMountpoint(ctx context.Context, mountPoint string) (succeeded bool, err error) {
	mountPoint = strings.TrimSpace(mountPoint)
	if mountPoint == "" {
		return true, nil
	}
	if ctx == nil {
		ctx = context.Background()
	}
	if _, ok := ctx.Deadline(); !ok {
		var cancel context.CancelFunc
		ctx, cancel = context.WithTimeout(ctx, 8*time.Second)
		defer cancel()
	}

	var lastErr error
	try := func(name string, args ...string) bool {
		p, err := exec.LookPath(name)
		if err != nil {
			return false
		}
		c := exec.CommandContext(ctx, p, args...)
		if err := c.Run(); err != nil {
			var exitErr *exec.ExitError
			if errors.As(err, &exitErr) {
				if exitErr.ExitCode() == 32 {
					succeeded = true
					return true
				}
			}
			lastErr = err
			return true
		}
		succeeded = true
		return true
	}

	anyTool := false
	if attempted := try("fusermount3", "-u", "-z", mountPoint); attempted {
		anyTool = true
		if succeeded {
			return true, nil
		}
	}
	if attempted := try("fusermount", "-u", "-z", mountPoint); attempted {
		anyTool = true
		if succeeded {
			return true, nil
		}
	}
	if attempted := try("umount", "-l", mountPoint); attempted {
		anyTool = true
		if succeeded {
			return true, nil
		}
	}
	if !anyTool {
		return false, fmt.Errorf("no unmount tool found")
	}
	if lastErr != nil {
		return false, lastErr
	}
	return false, fmt.Errorf("unmount did not succeed")
}
