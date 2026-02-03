//go:build integration

package integration

import (
	"context"
	"errors"
	"fmt"
	"os"
	"os/exec"
	"path/filepath"
	"strings"
	"syscall"
	"testing"
	"time"
)

const (
	workspace    = "/workspace"
	pfsSrc       = "/workspace/cmd/pfs"
	pfsBin       = "/workspace/bin/pfs"
	tmpDir       = "/workspace/tmp/pfs-integration"
	mountBase    = "/mnt/pfs/pfs-integration"
	storageBase1 = "/mnt/ssd1/pfs-integration"
	storageBase2 = "/mnt/ssd2/pfs-integration"
)

// IntegrationConfig describes the filesystem setup needed for an integration test.
type IntegrationConfig struct {
	// Targets controls the default target list for the catch-all rule.
	Targets []string

	// ReadTargets controls the read target ordering for the catch-all rule.
	ReadTargets []string
}

// MountedFS describes a mounted PolicyFS instance used by an integration test.
type MountedFS struct {
	ConfigPath   string
	MountName    string
	MountPoint   string
	StorageRoot1 string
	StorageRoot2 string
}

// TestMain is the entry point for integration tests.
func TestMain(m *testing.M) {
	code := run(m)
	os.Exit(code)
}

// run prepares the environment and runs all integration tests.
func run(m *testing.M) int {
	if err := os.MkdirAll(tmpDir, 0o755); err != nil {
		fmt.Fprintln(os.Stderr, "failed to ensure tmp dir", err)
		return 2
	}
	if err := os.MkdirAll(mountBase, 0o755); err != nil {
		fmt.Fprintln(os.Stderr, "failed to ensure mount base dir", err)
		return 2
	}
	if err := os.MkdirAll(storageBase1, 0o755); err != nil {
		fmt.Fprintln(os.Stderr, "failed to ensure storage base1", err)
		return 2
	}
	if err := os.MkdirAll(storageBase2, 0o755); err != nil {
		fmt.Fprintln(os.Stderr, "failed to ensure storage root2", err)
		return 2
	}

	buildArgs := []string{"build", "-o", pfsBin}
	if strings.TrimSpace(os.Getenv("PFS_INTEGRATION_DEBUG_BUILD")) != "" {
		buildArgs = append(buildArgs, "-gcflags=all=-N -l")
	}
	buildArgs = append(buildArgs, pfsSrc)
	buildCmd := exec.Command("go", buildArgs...)
	buildCmd.Dir = workspace
	buildCmd.Stdout = os.Stdout
	buildCmd.Stderr = os.Stderr
	if err := buildCmd.Run(); err != nil {
		fmt.Fprintln(os.Stderr, "failed to build pfs", err)
		return 2
	}
	return m.Run()
}

// withMountedFS mounts a PolicyFS instance for the duration of a test.
func withMountedFS(t *testing.T, cfg IntegrationConfig, fn func(env *MountedFS)) {
	t.Helper()

	// Sanitize test name into a path-safe name.
	name := sanitizeName(t.Name())

	if strings.TrimSpace(os.Getenv("PFS_INTEGRATION_USE_EXISTING_MOUNT")) != "" {
		baseMountPoint := os.Getenv("PFS_INTEGRATION_MOUNTPOINT")
		baseStorageRoot1 := os.Getenv("PFS_INTEGRATION_STORAGE_ROOT1")
		baseStorageRoot2 := os.Getenv("PFS_INTEGRATION_STORAGE_ROOT2")
		configPath := os.Getenv("PFS_INTEGRATION_CONFIG")

		if strings.TrimSpace(baseMountPoint) == "" {
			t.Fatalf("missing PFS_INTEGRATION_MOUNTPOINT")
		}
		mounted, err := isMountpointMounted(baseMountPoint)
		if err != nil {
			t.Fatalf("failed to check mountpoint: %v", err)
		}
		if !mounted {
			t.Fatalf("mountpoint is not mounted: mountpoint=%s", baseMountPoint)
		}

		if strings.TrimSpace(baseStorageRoot1) == "" {
			t.Fatalf("missing PFS_INTEGRATION_STORAGE_ROOT1")
		}
		if strings.TrimSpace(baseStorageRoot2) == "" {
			t.Fatalf("missing PFS_INTEGRATION_STORAGE_ROOT2")
		}

		if err := os.MkdirAll(baseStorageRoot1, 0o755); err != nil {
			t.Fatalf("failed to ensure storage root1: %v", err)
		}
		if err := os.MkdirAll(baseStorageRoot2, 0o755); err != nil {
			t.Fatalf("failed to ensure storage root2: %v", err)
		}

		env := &MountedFS{
			MountName:    "integration",
			MountPoint:   filepath.Join(baseMountPoint, name),
			StorageRoot1: filepath.Join(baseStorageRoot1, name),
			StorageRoot2: filepath.Join(baseStorageRoot2, name),
			ConfigPath:   configPath,
		}

		fn(env)
		return
	}

	env := &MountedFS{
		MountName:    "integration",
		MountPoint:   filepath.Join(mountBase, name),
		StorageRoot1: filepath.Join(storageBase1, name),
		StorageRoot2: filepath.Join(storageBase2, name),
		ConfigPath:   filepath.Join(tmpDir, name+".yaml"),
	}

	if err := ensureUnmounted(env.MountPoint, 2*time.Second); err != nil {
		t.Fatalf("failed to ensure unmounted: %v", err)
	}
	if err := ensureMountpointDir(env.MountPoint); err != nil {
		t.Fatalf("failed to ensure mountpoint: %v", err)
	}
	if err := os.MkdirAll(env.StorageRoot1, 0o755); err != nil {
		t.Fatalf("failed to ensure storage root1: %v", err)
	}
	if err := os.MkdirAll(env.StorageRoot2, 0o755); err != nil {
		t.Fatalf("failed to ensure storage root2: %v", err)
	}
	if err := writeIntegrationConfig(env.ConfigPath, env.MountPoint, env.StorageRoot1, env.StorageRoot2, cfg); err != nil {
		t.Fatalf("failed to write integration config: %v", err)
	}

	ctx, cancel := context.WithCancel(context.Background())
	defer cancel()

	mountCmd := exec.CommandContext(ctx, pfsBin, "--config", env.ConfigPath, "mount", env.MountName)
	mountCmd.Env = append(os.Environ(), "PFS_LOG_FILE="+tmpDir+"/"+name+".log")
	mountCmd.Stdout = os.Stdout
	mountCmd.Stderr = os.Stderr
	if err := mountCmd.Start(); err != nil {
		t.Fatalf("failed to start pfs mount: %v", err)
	}

	if err := waitForMount(env.MountPoint, 5*time.Second); err != nil {
		_ = mountCmd.Process.Signal(syscall.SIGTERM)
		_, _ = mountCmd.Process.Wait()
		t.Fatalf("mount did not become ready: %v", err)
	}

	t.Cleanup(func() {
		_ = mountCmd.Process.Signal(syscall.SIGTERM)
		_, _ = mountCmd.Process.Wait()
		if err := ensureUnmounted(env.MountPoint, 2*time.Second); err != nil {
			t.Fatalf("failed to unmount: %v", err)
		}
	})

	fn(env)
}

// sanitizeName converts a test name into a stable path segment.
func sanitizeName(s string) string {
	s = strings.ReplaceAll(s, "/", "_")
	s = strings.ReplaceAll(s, " ", "_")
	s = strings.ReplaceAll(s, "\\", "_")
	s = strings.ReplaceAll(s, ":", "_")
	return s
}

func quoteList(ss []string) string {
	quoted := make([]string, 0, len(ss))
	for _, s := range ss {
		quoted = append(quoted, fmt.Sprintf("%q", s))
	}
	return strings.Join(quoted, ", ")
}

// writeIntegrationConfig writes a minimal, known-good config file for an integration test.
func writeIntegrationConfig(path string, mountPoint string, storageRoot1 string, storageRoot2 string, cfg IntegrationConfig) error {
	targets := cfg.Targets
	if len(targets) == 0 {
		targets = []string{"ssd1"}
	}
	readTargets := cfg.ReadTargets
	if len(readTargets) == 0 {
		readTargets = []string{"ssd2", "ssd1"}
	}

	cfgYAML := fmt.Sprintf(`
mounts:
  integration:
    mountpoint: %q
    storage_paths:
      - id: ssd1
        path: %q
        indexed: false
        min_free_gb: 0
      - id: ssd2
        path: %q
        indexed: false
        min_free_gb: 0
    routing_rules:
      - match: "**"
        targets: [%s]
        read_targets: [%s]
`, mountPoint, storageRoot1, storageRoot2, quoteList(targets), quoteList(readTargets))

	if err := os.WriteFile(path, []byte(cfgYAML), 0o644); err != nil {
		return fmt.Errorf("failed to write config file: %w", err)
	}
	return nil
}

func ensureMountpointDir(mountPoint string) error {
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

func ensureUnmounted(mountPoint string, timeout time.Duration) error {
	_ = tryUnmount(mountPoint)

	deadline := time.Now().Add(timeout)
	for time.Now().Before(deadline) {
		mounted, err := isMountpointMounted(mountPoint)
		if err != nil {
			return err
		}
		if !mounted {
			return nil
		}
		_ = tryUnmount(mountPoint)
		time.Sleep(100 * time.Millisecond)
	}
	return fmt.Errorf("mountpoint still mounted after %s (mountpoint=%s)", timeout, filepath.Clean(mountPoint))
}

func tryUnmount(mountPoint string) error {
	// Best-effort: different environments prefer different tools.
	_ = exec.Command("umount", mountPoint).Run()
	_ = exec.Command("umount", "-l", mountPoint).Run()
	_ = exec.Command("fusermount3", "-u", mountPoint).Run()
	return nil
}

func isMountpointMounted(mountPoint string) (bool, error) {
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

func waitForMount(mountPoint string, timeout time.Duration) error {
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
