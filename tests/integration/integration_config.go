//go:build integration

package integration

import (
	"fmt"
	"os"
	"path/filepath"
	"testing"

	"github.com/hieutdo/policyfs/internal/config"
	"github.com/stretchr/testify/require"
)

// IntegrationConfig describes the filesystem setup needed for an integration test.
type IntegrationConfig struct {
	// Storages controls which storage_paths are generated for this test.
	// If empty, the harness defaults to 2 storages: ssd1 + ssd2.
	Storages []IntegrationStorage

	// StorageGroups configures mount.storage_groups.
	StorageGroups map[string][]string

	// RoutingRules overrides mount.routing_rules.
	// If empty, the harness generates a single catch-all rule "**".
	RoutingRules []config.RoutingRule

	// Targets controls the default target list for the catch-all rule.
	Targets []string

	// ReadTargets controls the read target ordering for the catch-all rule.
	ReadTargets []string
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
	MountName    string
	MountPoint   string
	StorageRoots map[string]string
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
