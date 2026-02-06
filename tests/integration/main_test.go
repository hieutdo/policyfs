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

	"github.com/hieutdo/policyfs/internal/config"
	"gopkg.in/yaml.v3"
)

const (
	workspace = "/workspace"
	pfsSrc    = "/workspace/cmd/pfs"
	pfsBin    = "/workspace/bin/pfs"
	tmpDir    = "/workspace/tmp/pfs-integration"
	mountBase = "/mnt/pfs/pfs-integration"
)

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

	buildArgs := []string{"build", "-o", pfsBin}
	if strings.TrimSpace(os.Getenv("PFS_INTEGRATION_COVER")) != "" {
		buildArgs = append(buildArgs, "-tags=cover", "-cover", "-covermode=atomic", "-coverpkg=./cmd/...,./internal/...")
	}
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
		configPath := strings.TrimSpace(os.Getenv("PFS_INTEGRATION_CONFIG"))
		if configPath == "" {
			t.Fatalf("missing PFS_INTEGRATION_CONFIG")
		}

		mountName := strings.TrimSpace(os.Getenv("PFS_INTEGRATION_MOUNT_NAME"))
		if mountName == "" {
			mountName = "integration"
		}

		rootCfg, err := config.Load(configPath)
		if err != nil {
			t.Fatalf("failed to load config: %v", err)
		}
		mountCfg, err := rootCfg.Mount(mountName)
		if err != nil {
			t.Fatalf("failed to resolve mount: %v", err)
		}

		baseMountPoint := strings.TrimSpace(os.Getenv("PFS_INTEGRATION_MOUNTPOINT"))
		if baseMountPoint == "" {
			baseMountPoint = mountCfg.MountPoint
		}
		if strings.TrimSpace(baseMountPoint) == "" {
			t.Fatalf("missing mountpoint (PFS_INTEGRATION_MOUNTPOINT or config mountpoint)")
		}

		mounted, err := isMountpointMounted(baseMountPoint)
		if err != nil {
			t.Fatalf("failed to check mountpoint: %v", err)
		}
		if !mounted {
			t.Fatalf("mountpoint is not mounted: mountpoint=%s", baseMountPoint)
		}

		storageRoots, err := existingMountStorageRoots(cfg, mountCfg, name)
		if err != nil {
			t.Fatalf("failed to resolve storages: %v", err)
		}

		env := &MountedFS{
			ConfigPath:   configPath,
			MountName:    mountName,
			MountPoint:   filepath.Join(baseMountPoint, name),
			StorageRoots: storageRoots,
		}

		t.Cleanup(func() {
			if t.Failed() {
				return
			}
			if strings.TrimSpace(os.Getenv("PFS_INTEGRATION_KEEP_ARTIFACTS")) != "" {
				return
			}
			for _, root := range env.StorageRoots {
				_ = os.RemoveAll(root)
			}
		})
		fn(env)
		return
	}

	storages := effectiveStorages(cfg)
	storageRoots, err := localStorageRoots(storages, name)
	if err != nil {
		t.Fatalf("failed to create storage roots: %v", err)
	}

	env := &MountedFS{
		MountName:    "integration",
		MountPoint:   filepath.Join(mountBase, name),
		ConfigPath:   filepath.Join(tmpDir, name+".yaml"),
		StorageRoots: storageRoots,
	}

	if err := ensureUnmounted(env.MountPoint, 2*time.Second); err != nil {
		t.Fatalf("failed to ensure unmounted: %v", err)
	}
	if err := ensureMountpointDir(env.MountPoint); err != nil {
		t.Fatalf("failed to ensure mountpoint: %v", err)
	}
	if err := writeIntegrationConfig(env.ConfigPath, env.MountName, env.MountPoint, storages, env.StorageRoots, cfg); err != nil {
		t.Fatalf("failed to write integration config: %v", err)
	}

	// NOTE: Don't defer cancel here. exec.CommandContext kills the process on cancel (SIGKILL),
	// which prevents graceful shutdown and coverage flush.
	ctx, cancel := context.WithCancel(context.Background())

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
		cancel()
		t.Fatalf("mount did not become ready: %v", err)
	}

	t.Cleanup(func() {
		_ = mountCmd.Process.Signal(syscall.SIGTERM)
		done := make(chan struct{})
		go func() {
			_, _ = mountCmd.Process.Wait()
			close(done)
		}()
		select {
		case <-done:
		case <-time.After(5 * time.Second):
			cancel()
			_ = mountCmd.Process.Kill()
			<-done
		}
		cancel()
		if err := ensureUnmounted(env.MountPoint, 2*time.Second); err != nil {
			t.Fatalf("failed to unmount: %v", err)
		}

		if t.Failed() {
			return
		}
		// Opt-out: keep artifacts even on success, for debugging and manual inspection.
		if strings.TrimSpace(os.Getenv("PFS_INTEGRATION_KEEP_ARTIFACTS")) != "" {
			return
		}

		// Best-effort cleanup: keep artifacts on failure to make debugging easier.
		for _, root := range env.StorageRoots {
			_ = os.RemoveAll(root)
		}
		_ = os.RemoveAll(env.MountPoint)
		_ = os.Remove(env.ConfigPath)
		_ = os.Remove(filepath.Join(tmpDir, name+".log"))
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

// effectiveStorages returns the storage list for a test, with sane defaults.
func effectiveStorages(cfg IntegrationConfig) []IntegrationStorage {
	if len(cfg.Storages) > 0 {
		out := make([]IntegrationStorage, 0, len(cfg.Storages))
		for _, s := range cfg.Storages {
			if strings.TrimSpace(s.ID) == "" {
				continue
			}
			out = append(out, s)
		}
		return out
	}
	return []IntegrationStorage{
		{ID: "ssd1", BasePath: "/mnt/ssd1/pfs-integration"},
		{ID: "ssd2", BasePath: "/mnt/ssd2/pfs-integration"},
	}
}

// localStorageRoots creates per-test storage root directories.
func localStorageRoots(storages []IntegrationStorage, testName string) (map[string]string, error) {
	roots := map[string]string{}
	for _, s := range storages {
		id := strings.TrimSpace(s.ID)
		if id == "" {
			continue
		}
		base := strings.TrimSpace(s.BasePath)
		if base == "" {
			return nil, fmt.Errorf("missing base path for storage %q", id)
		}
		if err := os.MkdirAll(base, 0o755); err != nil {
			return nil, fmt.Errorf("failed to ensure storage base: %w", err)
		}
		root := filepath.Join(base, testName)
		if err := os.MkdirAll(root, 0o755); err != nil {
			return nil, fmt.Errorf("failed to ensure storage root: %w", err)
		}
		roots[id] = root
	}
	if len(roots) == 0 {
		return nil, errors.New("no storages configured")
	}
	return roots, nil
}

// existingMountStorageRoots returns per-test storage roots based on an existing mount config.
func existingMountStorageRoots(cfg IntegrationConfig, mountCfg *config.MountConfig, testName string) (map[string]string, error) {
	if mountCfg == nil {
		return nil, errors.New("mount config is nil")
	}

	requested := effectiveStorages(cfg)
	requestedIDs := map[string]struct{}{}
	for _, s := range requested {
		if strings.TrimSpace(s.ID) == "" {
			continue
		}
		requestedIDs[s.ID] = struct{}{}
	}

	roots := map[string]string{}
	for _, sp := range mountCfg.StoragePaths {
		id := strings.TrimSpace(sp.ID)
		if id == "" {
			continue
		}
		if _, ok := requestedIDs[id]; !ok {
			continue
		}
		base := strings.TrimSpace(sp.Path)
		if base == "" {
			return nil, fmt.Errorf("storage %q has empty path", id)
		}
		root := filepath.Join(base, testName)
		if err := os.MkdirAll(root, 0o755); err != nil {
			return nil, fmt.Errorf("failed to ensure storage root: %w", err)
		}
		roots[id] = root
	}
	if len(roots) == 0 {
		return nil, errors.New("no storages resolved from config")
	}
	for id := range requestedIDs {
		if _, ok := roots[id]; !ok {
			return nil, fmt.Errorf("config missing requested storage id %q", id)
		}
	}
	return roots, nil
}

// writeIntegrationConfig writes a minimal, known-good config file for an integration test.
func writeIntegrationConfig(path string, mountName string, mountPoint string, storages []IntegrationStorage, storageRoots map[string]string, cfg IntegrationConfig) error {
	if strings.TrimSpace(mountName) == "" {
		return errors.New("mount name is required")
	}
	if strings.TrimSpace(mountPoint) == "" {
		return errors.New("mountpoint is required")
	}

	storagePaths := make([]config.StoragePath, 0, len(storages))
	for _, s := range storages {
		id := strings.TrimSpace(s.ID)
		if id == "" {
			continue
		}
		root := ""
		if storageRoots != nil {
			root = strings.TrimSpace(storageRoots[id])
		}
		if root == "" {
			return fmt.Errorf("missing storage root for %q", id)
		}
		storagePaths = append(storagePaths, config.StoragePath{ID: id, Path: root, Indexed: s.Indexed, MinFreeGB: s.MinFreeGB})
	}
	if len(storagePaths) == 0 {
		return errors.New("no storage paths configured")
	}

	targets := cfg.Targets
	if len(targets) == 0 {
		if _, ok := storageRoots["ssd1"]; ok {
			targets = []string{"ssd1"}
		} else {
			targets = []string{storagePaths[0].ID}
		}
	}
	readTargets := cfg.ReadTargets
	if len(readTargets) == 0 {
		if _, ok1 := storageRoots["ssd1"]; ok1 {
			if _, ok2 := storageRoots["ssd2"]; ok2 {
				readTargets = []string{"ssd2", "ssd1"}
			}
		}
		if len(readTargets) == 0 {
			readTargets = append([]string{}, targets...)
		}
	}

	rules := cfg.RoutingRules
	if len(rules) == 0 {
		rules = []config.RoutingRule{{Match: "**", Targets: targets, ReadTargets: readTargets}}
	}

	rootCfg := config.RootConfig{
		Fuse: config.FuseConfig{AllowOther: cfg.AllowOther},
		Mounts: map[string]config.MountConfig{
			mountName: {
				MountPoint:    mountPoint,
				StoragePaths:  storagePaths,
				StorageGroups: cfg.StorageGroups,
				RoutingRules:  rules,
			},
		},
	}

	b, err := yaml.Marshal(&rootCfg)
	if err != nil {
		return fmt.Errorf("failed to marshal config: %w", err)
	}
	if err := os.WriteFile(path, b, 0o644); err != nil {
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
