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

func (m *MountedFS) CreateDir(rootId string, path ...string) error {
	storageRoot := m.StorageRoot(rootId)
	storagePath := filepath.Join(append([]string{storageRoot}, path...)...)
	if err := os.MkdirAll(storagePath, 0o755); err != nil {
		return fmt.Errorf("failed to create storage path %s: %w", storagePath, err)
	}
	return nil
}

func (m *MountedFS) CreateFile(content []byte, rootId string, path ...string) error {
	storageRoot := m.StorageRoot(rootId)
	filePath := filepath.Join(append([]string{storageRoot}, path...)...)
	if err := os.MkdirAll(filepath.Dir(filePath), 0o755); err != nil {
		return fmt.Errorf("failed to create directory for file %s: %w", filePath, err)
	}
	if err := os.WriteFile(filePath, content, 0o644); err != nil {
		return fmt.Errorf("failed to create file %s: %w", filePath, err)
	}
	return nil
}

// MustCreateDir creates a directory on a storage root or fails the test.
func (m *MountedFS) MustCreateDir(t testing.TB, rootId string, path ...string) {
	t.Helper()
	require.NoError(t, m.CreateDir(rootId, path...))
}

// MustCreateFile creates a file on a storage root or fails the test.
func (m *MountedFS) MustCreateFile(t testing.TB, content []byte, rootId string, path ...string) {
	t.Helper()
	require.NoError(t, m.CreateFile(content, rootId, path...))
}
