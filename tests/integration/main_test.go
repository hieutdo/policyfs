//go:build integration

package integration

import (
	"context"
	"fmt"
	"os"
	"os/exec"
	"path/filepath"
	"syscall"
	"testing"
	"time"
)

const (
	pfsBin      = "/workspace/bin/pfs"
	pfsCfg      = "/workspace/configs/dev.yaml"
	mountPoint  = "/mnt/pfs/media"
	fuseHelper  = "/workspace/scripts/fuse.sh"
	buildTarget = "/workspace/cmd/pfs"
)

func TestMain(m *testing.M) {
	code := run(m)
	os.Exit(code)
}

func run(m *testing.M) int {
	if err := os.MkdirAll(mountPoint, 0o755); err != nil {
		fmt.Fprintln(os.Stderr, "failed to create mountpoint:", err)
		return 2
	}

	_ = exec.Command(fuseHelper, "umount").Run()

	buildCmd := exec.Command("go", "build", "-o", pfsBin, buildTarget)
	buildCmd.Stdout = os.Stdout
	buildCmd.Stderr = os.Stderr
	if err := buildCmd.Run(); err != nil {
		fmt.Fprintln(os.Stderr, "failed to build pfs:", err)
		return 2
	}

	ctx, cancel := context.WithCancel(context.Background())
	defer cancel()

	mountCmd := exec.CommandContext(ctx, pfsBin, "mount", "--config", pfsCfg)
	mountCmd.Stdout = os.Stdout
	mountCmd.Stderr = os.Stderr
	if err := mountCmd.Start(); err != nil {
		fmt.Fprintln(os.Stderr, "failed to start pfs mount:", err)
		return 2
	}

	if err := waitForMount(5 * time.Second); err != nil {
		_ = mountCmd.Process.Signal(syscall.SIGTERM)
		_, _ = mountCmd.Process.Wait()
		fmt.Fprintln(os.Stderr, err)
		return 2
	}

	exitCode := m.Run()

	_ = exec.Command(fuseHelper, "umount").Run()
	_ = mountCmd.Process.Signal(syscall.SIGTERM)
	_, _ = mountCmd.Process.Wait()
	_ = exec.Command(fuseHelper, "umount").Run()

	return exitCode
}

func waitForMount(timeout time.Duration) error {
	deadline := time.Now().Add(timeout)
	for time.Now().Before(deadline) {
		cmd := exec.Command("mountpoint", "-q", mountPoint)
		if err := cmd.Run(); err == nil {
			return nil
		}
		time.Sleep(100 * time.Millisecond)
	}
	return fmt.Errorf("mount did not become ready within %s (mountpoint=%s)", timeout, filepath.Clean(mountPoint))
}
