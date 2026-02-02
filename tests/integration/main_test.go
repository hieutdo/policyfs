//go:build integration

package integration

import (
	"context"
	"errors"
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
	pfsCfg      = "/workspace/tmp/pfs-integration.yaml"
	storageRoot = "/workspace/tmp/pfs-storage"
	mountPoint  = "/mnt/pfs/media"
	buildTarget = "./cmd/pfs"
	workspace   = "/workspace"
)

func TestMain(m *testing.M) {
	code := run(m)
	os.Exit(code)
}

func run(m *testing.M) int {
	if err := ensureUnmounted(2 * time.Second); err != nil {
		fmt.Fprintln(os.Stderr, "failed to unmount", err)
		return 2
	}
	if err := ensureMountpointDir(); err != nil {
		fmt.Fprintln(os.Stderr, "failed to ensure mountpoint", err)
		return 2
	}
	if err := os.MkdirAll("/workspace/tmp", 0o755); err != nil {
		fmt.Fprintln(os.Stderr, "failed to ensure tmp dir", err)
		return 2
	}
	if err := os.MkdirAll(storageRoot, 0o755); err != nil {
		fmt.Fprintln(os.Stderr, "failed to ensure storage root", err)
		return 2
	}
	if err := writeIntegrationConfig(pfsCfg); err != nil {
		fmt.Fprintln(os.Stderr, "failed to write integration config", err)
		return 2
	}

	buildCmd := exec.Command("go", "build", "-o", pfsBin, buildTarget)
	buildCmd.Dir = workspace
	buildCmd.Stdout = os.Stdout
	buildCmd.Stderr = os.Stderr
	if err := buildCmd.Run(); err != nil {
		fmt.Fprintln(os.Stderr, "failed to build pfs", err)
		return 2
	}

	ctx, cancel := context.WithCancel(context.Background())
	defer cancel()

	mountCmd := exec.CommandContext(ctx, pfsBin, "--config", pfsCfg, "mount", "media")
	mountCmd.Env = append(os.Environ(), "PFS_LOG_FILE=/workspace/tmp/pfs.log")
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
	_ = mountCmd.Process.Signal(syscall.SIGTERM)
	_, _ = mountCmd.Process.Wait()
	if err := ensureUnmounted(2 * time.Second); err != nil {
		fmt.Fprintln(os.Stderr, "failed to unmount", err)
		return 2
	}

	return exitCode
}

// writeIntegrationConfig writes a minimal, known-good config file for integration tests.
func writeIntegrationConfig(path string) error {
	cfg := fmt.Sprintf(`
mounts:
  media:
    mountpoint: %q
    storage_paths:
      - id: ssd1
        path: %q
        indexed: false
        min_free_gb: 0
    routing_rules:
      - match: "**"
        targets: [ssd1]
`, mountPoint, storageRoot)

	if err := os.WriteFile(path, []byte(cfg), 0o644); err != nil {
		return fmt.Errorf("failed to write config file: %w", err)
	}
	return nil
}

func ensureMountpointDir() error {
	fi, err := os.Lstat(mountPoint)
	if err != nil {
		if os.IsNotExist(err) {
			if err := os.MkdirAll(mountPoint, 0o755); err != nil {
				return fmt.Errorf("failed to create mountpoint dir: %w", err)
			}
			return nil
		}
		return fmt.Errorf("failed to stat mountpoint: %w", err)
	}
	if fi.IsDir() {
		return nil
	}
	if err := os.Remove(mountPoint); err != nil {
		return fmt.Errorf("failed to remove mountpoint path: %w", err)
	}
	if err := os.MkdirAll(mountPoint, 0o755); err != nil {
		return fmt.Errorf("failed to create mountpoint dir: %w", err)
	}
	return nil
}

func ensureUnmounted(timeout time.Duration) error {
	_ = tryUnmount()

	deadline := time.Now().Add(timeout)
	for time.Now().Before(deadline) {
		mounted, err := isMountpointMounted()
		if err != nil {
			return err
		}
		if !mounted {
			return nil
		}
		_ = tryUnmount()
		time.Sleep(100 * time.Millisecond)
	}
	return fmt.Errorf("mountpoint still mounted after %s (mountpoint=%s)", timeout, filepath.Clean(mountPoint))
}

func tryUnmount() error {
	// Best-effort: different environments prefer different tools.
	_ = exec.Command("umount", mountPoint).Run()
	_ = exec.Command("umount", "-l", mountPoint).Run()
	_ = exec.Command("fusermount3", "-u", mountPoint).Run()
	return nil
}

func isMountpointMounted() (bool, error) {
	if _, err := os.Lstat(mountPoint); err != nil {
		if os.IsNotExist(err) {
			return false, nil
		}
		return false, fmt.Errorf("failed to stat mountpoint: %w", err)
	}

	cmd := exec.Command("mountpoint", "-q", mountPoint)
	err := cmd.Run()
	if err == nil {
		return true, nil
	}
	var exitErr *exec.ExitError
	if errors.As(err, &exitErr) {
		switch exitErr.ExitCode() {
		case 1:
			return false, nil
		case 32:
			return false, nil
		}
	}
	return false, fmt.Errorf("failed to check mountpoint: %w", err)
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
