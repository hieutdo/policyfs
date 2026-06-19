//go:build integration

package integration

import (
	"context"
	"encoding/json"
	"fmt"
	"os"
	"os/exec"
	"path/filepath"
	"strings"
	"syscall"
	"testing"
	"time"

	"github.com/hieutdo/policyfs/internal/config"
	"github.com/stretchr/testify/require"
)

// IntegrationConfig describes the filesystem setup needed for an integration test.
type IntegrationConfig struct {
	// Storages controls which storage_paths are generated for this test.
	// If empty, the harness defaults to 2 storages: ssd1 + ssd2.
	Storages []IntegrationStorage

	// LogLevel overrides root log.level for the generated integration config.
	LogLevel string

	// MountArgs are extra arguments passed to `pfs mount <mount_name>`.
	// Use this to enable optional debug features (e.g., disk access logging dedup/summary)
	// in a test-specific way.
	MountArgs []string

	// AllowOther controls whether the mount enables FUSE allow_other.
	AllowOther bool

	// StorageGroups configures mount.storage_groups.
	StorageGroups map[string][]string

	// RoutingRules overrides mount.routing_rules.
	// If empty, the harness generates a single catch-all rule "**".
	RoutingRules []config.RoutingRule

	// Targets controls the default target list for the catch-all rule.
	Targets []string

	// ReadTargets controls the read target ordering for the catch-all rule.
	ReadTargets []string

	// Mover optionally configures mount.mover.
	Mover *config.MoverConfig

	// Statfs optionally configures mount.statfs for the test.
	Statfs *config.StatfsConfig
}

// IntegrationStorage describes a single test storage backend.
type IntegrationStorage struct {
	ID        string
	Indexed   bool
	MinFreeGB float64
	BasePath  string
}

// MountedFS describes a mounted PolicyFS instance used by an integration test.
type MountedFS struct {
	ConfigPath   string
	LogPath      string
	MountName    string
	MountPoint   string
	RuntimeDir   string
	StateDir     string
	StorageRoots map[string]string

	// mountProc/mountCancel/mountDone are populated by the launcher (withMountedFS,
	// startNamedMountedFS). They let lifecycle tests stop a single daemon explicitly
	// via Stop() while leaving the t.Cleanup teardown a safe no-op.
	mountProc   *os.Process
	mountCancel context.CancelFunc
	mountDone   chan struct{}
	stopped     bool
}

// DaemonLogExpectation describes one structured daemon log entry that must appear in order.
type DaemonLogExpectation struct {
	Msg    string
	Fields map[string]string
}

// StorageRoot returns the physical root directory for a storage id.
func (m *MountedFS) StorageRoot(id string) string {
	if m == nil {
		panic("mounted fs is nil")
	}
	if m.StorageRoots == nil {
		panic("storage roots not initialized")
	}
	if _, ok := m.StorageRoots[id]; !ok {
		panic("storage root not found for id: " + id)
	}
	return m.StorageRoots[id]
}

// MountPath returns an absolute path under the mounted PolicyFS view.
func (m *MountedFS) MountPath(rel string) string {
	if m == nil {
		panic("mounted fs is nil")
	}
	return filepath.Join(m.MountPoint, filepath.FromSlash(rel))
}

// StoragePath returns an absolute path under a specific storage root.
func (m *MountedFS) StoragePath(storageID string, rel string) string {
	if m == nil {
		panic("mounted fs is nil")
	}
	return filepath.Join(m.StorageRoot(storageID), filepath.FromSlash(rel))
}

// MustStatT returns a syscall.Stat_t for a path or fails the test.
func (m *MountedFS) MustStatT(t testing.TB, path string) *syscall.Stat_t {
	t.Helper()
	fi, err := os.Lstat(path)
	require.NoError(t, err)
	st, ok := fi.Sys().(*syscall.Stat_t)
	require.True(t, ok)
	require.NotNil(t, st)
	return st
}

// ReadDaemonLog reads the daemon log file configured for this mounted filesystem.
func (m *MountedFS) ReadDaemonLog() ([]byte, error) {
	if m == nil {
		return nil, fmt.Errorf("mounted fs is nil")
	}
	if strings.TrimSpace(m.LogPath) == "" {
		return nil, fmt.Errorf("daemon log path is not configured")
	}
	b, err := os.ReadFile(m.LogPath)
	if err != nil {
		return nil, fmt.Errorf("failed to read daemon log %s: %w", m.LogPath, err)
	}
	return b, nil
}

// MustWaitForDaemonLogSequence polls until the daemon log contains the expected entry sequence.
func (m *MountedFS) MustWaitForDaemonLogSequence(t testing.TB, timeout time.Duration, expected []DaemonLogExpectation) []byte {
	t.Helper()
	require.NotEmpty(t, expected)

	deadline := time.Now().Add(timeout)
	var logData []byte
	for time.Now().Before(deadline) {
		b, err := m.ReadDaemonLog()
		if err == nil {
			logData = b
			if daemonLogContainsSequence(logData, expected) {
				return logData
			}
		}
		time.Sleep(100 * time.Millisecond)
	}

	require.NotEmpty(t, logData, "expected daemon log to be readable at %s", m.LogPath)
	t.Fatalf("expected daemon log sequence in %s:\n%s\nlog:\n%s", m.LogPath, formatDaemonLogExpectations(expected), string(logData))
	return nil
}

// RunAsUser runs a shell command as another user.
func (m *MountedFS) RunAsUser(t testing.TB, user string, cmd string) error {
	t.Helper()
	c := exec.Command("su", "-s", "/bin/sh", user, "-c", cmd)
	c.Stdout = os.Stdout
	c.Stderr = os.Stderr
	if err := c.Run(); err != nil {
		return fmt.Errorf("failed to run command as user %s: %w", user, err)
	}
	return nil
}

// daemonLogContainsSequence reports whether the expected structured log entries appear in order.
func daemonLogContainsSequence(logData []byte, expected []DaemonLogExpectation) bool {
	if len(expected) == 0 {
		return true
	}

	entries := parseDaemonLogEntries(logData)
	idx := 0
	for _, entry := range entries {
		if daemonLogEntryMatches(entry, expected[idx]) {
			idx++
			if idx == len(expected) {
				return true
			}
		}
	}
	return false
}

// parseDaemonLogEntries decodes newline-delimited JSON daemon logs into generic maps.
func parseDaemonLogEntries(logData []byte) []map[string]any {
	trimmed := strings.TrimSpace(string(logData))
	if trimmed == "" {
		return nil
	}

	lines := strings.Split(trimmed, "\n")
	entries := make([]map[string]any, 0, len(lines))
	for _, line := range lines {
		line = strings.TrimSpace(line)
		if line == "" {
			continue
		}
		var entry map[string]any
		if err := json.Unmarshal([]byte(line), &entry); err != nil {
			continue
		}
		entries = append(entries, entry)
	}
	return entries
}

// daemonLogEntryMatches reports whether one parsed JSON log entry matches the expectation.
func daemonLogEntryMatches(entry map[string]any, expected DaemonLogExpectation) bool {
	if entry == nil {
		return false
	}
	msg, _ := entry["msg"].(string)
	if msg != expected.Msg {
		return false
	}
	for key, want := range expected.Fields {
		got, ok := entry[key]
		if !ok {
			return false
		}
		if fmt.Sprint(got) != want {
			return false
		}
	}
	return true
}

// formatDaemonLogExpectations renders expected log entries for readable test failures.
func formatDaemonLogExpectations(expected []DaemonLogExpectation) string {
	parts := make([]string, 0, len(expected))
	for _, item := range expected {
		fields := make([]string, 0, len(item.Fields))
		for key, value := range item.Fields {
			fields = append(fields, fmt.Sprintf("%s=%s", key, value))
		}
		if len(fields) == 0 {
			parts = append(parts, item.Msg)
			continue
		}
		parts = append(parts, fmt.Sprintf("%s (%s)", item.Msg, strings.Join(fields, ", ")))
	}
	return strings.Join(parts, " -> ")
}

// CreateDirInStoragePath creates a directory tree under a storage root.
func (m *MountedFS) CreateDirInStoragePath(rootId string, rel string) error {
	storagePath := m.StoragePath(rootId, rel)
	if err := os.MkdirAll(storagePath, 0o755); err != nil {
		return fmt.Errorf("failed to create storage path %s: %w", storagePath, err)
	}
	return nil
}

// CreateFileInStoragePath creates a file under a storage root, creating parent directories if needed.
func (m *MountedFS) CreateFileInStoragePath(content []byte, rootId string, rel string) error {
	filePath := m.StoragePath(rootId, rel)
	if err := os.MkdirAll(filepath.Dir(filePath), 0o755); err != nil {
		return fmt.Errorf("failed to create directory for file %s: %w", filePath, err)
	}
	if err := os.WriteFile(filePath, content, 0o644); err != nil {
		return fmt.Errorf("failed to create file %s: %w", filePath, err)
	}
	return nil
}

// MustCreateDirInStoragePath creates a directory on a storage root or fails the test.
func (m *MountedFS) MustCreateDirInStoragePath(t testing.TB, rootId string, rel string) {
	t.Helper()
	require.NoError(t, m.CreateDirInStoragePath(rootId, rel))
}

// MustCreateFileInStoragePath creates a file on a storage root or fails the test.
func (m *MountedFS) MustCreateFileInStoragePath(t testing.TB, content []byte, rootId string, rel string) {
	t.Helper()
	require.NoError(t, m.CreateFileInStoragePath(content, rootId, rel))
}

// MkdirInMountPoint creates a directory tree through the mounted view.
func (m *MountedFS) MkdirInMountPoint(rel string, perm os.FileMode) error {
	p := m.MountPath(rel)
	if err := os.MkdirAll(p, perm); err != nil {
		return fmt.Errorf("failed to create mount directory %s: %w", p, err)
	}
	return nil
}

// MustMkdirInMountPoint is MkdirInMountPoint with test failure on error.
func (m *MountedFS) MustMkdirInMountPoint(t testing.TB, rel string) {
	t.Helper()
	require.NoError(t, m.MkdirInMountPoint(rel, 0o755))
}

// WriteFileInMountPoint writes a file through the mounted view, creating parent directories if needed.
func (m *MountedFS) WriteFileInMountPoint(rel string, content []byte, perm os.FileMode) error {
	full := m.MountPath(rel)
	if err := os.MkdirAll(filepath.Dir(full), 0o755); err != nil {
		return fmt.Errorf("failed to create mount directory for file %s: %w", full, err)
	}
	if err := os.WriteFile(full, content, perm); err != nil {
		return fmt.Errorf("failed to write mount file %s: %w", full, err)
	}
	return nil
}

// MustWriteFileInMountPoint is WriteFileInMountPoint with default perms and test failure on error.
func (m *MountedFS) MustWriteFileInMountPoint(t testing.TB, rel string, content []byte) {
	t.Helper()
	require.NoError(t, m.WriteFileInMountPoint(rel, content, 0o644))
}

// ReadFileInMountPoint reads a file through the mounted view.
func (m *MountedFS) ReadFileInMountPoint(rel string) ([]byte, error) {
	p := m.MountPath(rel)
	b, err := os.ReadFile(p)
	if err != nil {
		return nil, fmt.Errorf("failed to read mount file %s: %w", p, err)
	}
	return b, nil
}

// MustReadFileInMountPoint is ReadFileInMountPoint with test failure on error.
func (m *MountedFS) MustReadFileInMountPoint(t testing.TB, rel string) []byte {
	t.Helper()
	b, err := m.ReadFileInMountPoint(rel)
	require.NoError(t, err)
	return b
}

// ReadFileAtStorage reads a file directly from a storage root.
func (m *MountedFS) ReadFileAtStorage(storageID string, rel string) ([]byte, error) {
	p := m.StoragePath(storageID, rel)
	b, err := os.ReadFile(p)
	if err != nil {
		return nil, fmt.Errorf("failed to read storage file %s: %w", p, err)
	}
	return b, nil
}

// MustReadFileInStoragePath is ReadFileAtStorage with test failure on error.
func (m *MountedFS) MustReadFileInStoragePath(t testing.TB, storageID string, rel string) []byte {
	t.Helper()
	b, err := m.ReadFileAtStorage(storageID, rel)
	require.NoError(t, err)
	return b
}

// ReadDirInMountPoint reads a directory listing through the mounted view.
func (m *MountedFS) ReadDirInMountPoint(rel string) ([]os.DirEntry, error) {
	p := m.MountPath(rel)
	entries, err := os.ReadDir(p)
	if err != nil {
		return nil, fmt.Errorf("failed to read mount directory %s: %w", p, err)
	}
	return entries, nil
}

// MustReadDirInMountPoint is ReadDirInMountPoint with test failure on error.
func (m *MountedFS) MustReadDirInMountPoint(t testing.TB, rel string) []os.DirEntry {
	t.Helper()
	entries, err := m.ReadDirInMountPoint(rel)
	require.NoError(t, err)
	return entries
}

// RemoveFileInMountPoint removes a path through the mounted view.
func (m *MountedFS) RemoveFileInMountPoint(rel string) error {
	p := m.MountPath(rel)
	if err := os.Remove(p); err != nil {
		return fmt.Errorf("failed to remove mount path %s: %w", p, err)
	}
	return nil
}

// MustRemoveFileInMountPoint is RemoveFileInMountPoint with test failure on error.
func (m *MountedFS) MustRemoveFileInMountPoint(t testing.TB, rel string) {
	t.Helper()
	require.NoError(t, m.RemoveFileInMountPoint(rel))
}

// RenameFileInMountPoint renames a path through the mounted view.
func (m *MountedFS) RenameFileInMountPoint(oldRel string, newRel string) error {
	oldPath := m.MountPath(oldRel)
	newPath := m.MountPath(newRel)
	if err := os.Rename(oldPath, newPath); err != nil {
		return fmt.Errorf("failed to rename mount path %s to %s: %w", oldPath, newPath, err)
	}
	return nil
}

// MustRenameFileInMountPoint is RenameFileInMountPoint with test failure on error.
func (m *MountedFS) MustRenameFileInMountPoint(t testing.TB, oldRel string, newRel string) {
	t.Helper()
	require.NoError(t, m.RenameFileInMountPoint(oldRel, newRel))
}

// SymlinkInMountPoint creates a symbolic link through the mounted view.
func (m *MountedFS) SymlinkInMountPoint(target string, linkRel string) error {
	linkPath := m.MountPath(linkRel)
	if err := os.MkdirAll(filepath.Dir(linkPath), 0o755); err != nil {
		return fmt.Errorf("failed to create parent directory for symlink %s: %w", linkPath, err)
	}
	if err := os.Symlink(target, linkPath); err != nil {
		return fmt.Errorf("failed to create symlink %s -> %s: %w", linkPath, target, err)
	}
	return nil
}

// MustSymlinkInMountPoint is SymlinkInMountPoint with test failure on error.
func (m *MountedFS) MustSymlinkInMountPoint(t testing.TB, target string, linkRel string) {
	t.Helper()
	require.NoError(t, m.SymlinkInMountPoint(target, linkRel))
}

// ReadlinkInMountPoint reads a symbolic link target through the mounted view.
func (m *MountedFS) ReadlinkInMountPoint(rel string) (string, error) {
	p := m.MountPath(rel)
	target, err := os.Readlink(p)
	if err != nil {
		return "", fmt.Errorf("failed to read symlink %s: %w", p, err)
	}
	return target, nil
}

// MustReadlinkInMountPoint is ReadlinkInMountPoint with test failure on error.
func (m *MountedFS) MustReadlinkInMountPoint(t testing.TB, rel string) string {
	t.Helper()
	target, err := m.ReadlinkInMountPoint(rel)
	require.NoError(t, err)
	return target
}

// LstatInMountPoint returns file info for a path without following symlinks.
func (m *MountedFS) LstatInMountPoint(rel string) (os.FileInfo, error) {
	p := m.MountPath(rel)
	fi, err := os.Lstat(p)
	if err != nil {
		return nil, fmt.Errorf("failed to lstat %s: %w", p, err)
	}
	return fi, nil
}

// MustLstatInMountPoint is LstatInMountPoint with test failure on error.
func (m *MountedFS) MustLstatInMountPoint(t testing.TB, rel string) os.FileInfo {
	t.Helper()
	fi, err := m.LstatInMountPoint(rel)
	require.NoError(t, err)
	return fi
}

// FileExistsInMountPoint checks if a path exists through the mounted view.
func (m *MountedFS) FileExistsInMountPoint(rel string) bool {
	p := m.MountPath(rel)
	_, err := os.Lstat(p)
	return err == nil
}

// FileExistsInStoragePath checks if a path exists on a storage root.
func (m *MountedFS) FileExistsInStoragePath(storageID string, rel string) bool {
	p := m.StoragePath(storageID, rel)
	_, err := os.Lstat(p)
	return err == nil
}
