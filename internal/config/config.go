package config

import (
	"fmt"
	"os"
	"path/filepath"
	"strings"

	"github.com/hieutdo/policyfs/internal/errkind"

	"gopkg.in/yaml.v3"
)

const (
	DefaultConfigFile     = "/etc/pfs/pfs.yaml"
	DefaultLogFile        = "/var/log/pfs/pfs.log"
	DefaultStateDir       = "/var/lib/pfs"
	DefaultRuntimeDir     = "/run/pfs"
	DefaultDaemonLockFile = "daemon.lock"
	DefaultJobLockFile    = "job.lock"
	DefaultWritePolicy    = "first_found"
)

const (
	EnvTZ                          = "TZ"
	EnvConfigFile                  = "PFS_CONFIG_FILE"
	EnvLogFile                     = "PFS_LOG_FILE"
	EnvStateDir                    = "PFS_STATE_DIR"
	EnvRuntimeDir                  = "PFS_RUNTIME_DIR"
	EnvIntegrationCover            = "PFS_INTEGRATION_COVER"
	EnvIntegrationDebugBuild       = "PFS_INTEGRATION_DEBUG_BUILD"
	EnvIntegrationUseExistingMount = "PFS_INTEGRATION_USE_EXISTING_MOUNT"
	EnvIntegrationConfig           = "PFS_INTEGRATION_CONFIG"
	EnvIntegrationMountName        = "PFS_INTEGRATION_MOUNT_NAME"
	EnvIntegrationMountpoint       = "PFS_INTEGRATION_MOUNTPOINT"
	EnvIntegrationKeepArtifacts    = "PFS_INTEGRATION_KEEP_ARTIFACTS"
	EnvTestHelper                  = "PFS_TEST_HELPER"
	EnvTestMount                   = "PFS_TEST_MOUNT"
	EnvTestLockFile                = "PFS_TEST_LOCK_FILE"
)

// ConfigFilePath returns the effective default config file path (PFS_CONFIG_FILE override).
func ConfigFilePath() string {
	p := strings.TrimSpace(os.Getenv(EnvConfigFile))
	if p == "" {
		return DefaultConfigFile
	}
	return p
}

// LogFilePath returns the effective default log file path (PFS_LOG_FILE override).
func LogFilePath() string {
	p := strings.TrimSpace(os.Getenv(EnvLogFile))
	if p == "" {
		return DefaultLogFile
	}
	return p
}

// StateDir returns the persistent state directory (PFS_STATE_DIR override).
func StateDir() string {
	base := strings.TrimSpace(os.Getenv(EnvStateDir))
	if base == "" {
		return DefaultStateDir
	}
	return base
}

// RuntimeDir returns the runtime directory (PFS_RUNTIME_DIR override).
func RuntimeDir() string {
	base := strings.TrimSpace(os.Getenv(EnvRuntimeDir))
	if base == "" {
		return DefaultRuntimeDir
	}
	return base
}

// MountStateDir returns the mount-scoped persistent state directory.
func MountStateDir(mountName string) string {
	return filepath.Join(StateDir(), mountName)
}

// MountRuntimeDir returns the mount-scoped runtime directory.
func MountRuntimeDir(mountName string) string {
	return filepath.Join(RuntimeDir(), mountName)
}

// MountLockDir returns the mount-scoped runtime locks directory.
func MountLockDir(mountName string) string {
	return filepath.Join(MountRuntimeDir(mountName), "locks")
}

// RootConfig represents the top-level YAML config file.
type RootConfig struct {
	Fuse   FuseConfig             `yaml:"fuse"`
	Log    LogConfig              `yaml:"log"`
	Mounts map[string]MountConfig `yaml:"mounts"`
}

// FuseConfig holds FUSE mount options.
type FuseConfig struct {
	AllowOther bool `yaml:"allow_other"`
}

// LogConfig controls CLI/daemon logging behavior.
type LogConfig struct {
	Level  string `yaml:"level"`
	Format string `yaml:"format"`
}

// MountConfig defines a single mount instance configuration.
type MountConfig struct {
	MountPoint    string              `yaml:"mountpoint"`
	StoragePaths  []StoragePath       `yaml:"storage_paths"`
	StorageGroups map[string][]string `yaml:"storage_groups"`
	RoutingRules  []RoutingRule       `yaml:"routing_rules"`
	Indexer       IndexerConfig       `yaml:"indexer"`
}

// IndexerConfig controls the indexer behavior for indexed storage paths.
type IndexerConfig struct {
	Ignore []string `yaml:"ignore"`
}

// RoutingRule routes virtual paths to storage targets.
type RoutingRule struct {
	Match          string   `yaml:"match"`
	Targets        []string `yaml:"targets"`
	ReadTargets    []string `yaml:"read_targets"`
	WriteTargets   []string `yaml:"write_targets"`
	WritePolicy    string   `yaml:"write_policy"`
	PathPreserving bool     `yaml:"path_preserving"`
}

// StoragePath is a single storage root referenced by routing rules.
type StoragePath struct {
	ID        string  `yaml:"id"`
	Path      string  `yaml:"path"`
	Indexed   bool    `yaml:"indexed"`
	MinFreeGB float64 `yaml:"min_free_gb"`
}

// Load reads a YAML config file from disk.
func Load(path string) (*RootConfig, error) {
	b, err := os.ReadFile(path)
	if err != nil {
		return nil, fmt.Errorf("read config %q: %w", path, err)
	}

	var cfg RootConfig
	if err := yaml.Unmarshal(b, &cfg); err != nil {
		return nil, fmt.Errorf("parse config %q: %w", path, err)
	}

	cfg.applyDefaults()

	return &cfg, nil
}

func (c *RootConfig) applyDefaults() {
	if c == nil {
		return
	}
	if c.Mounts == nil {
		return
	}

	for mountName, m := range c.Mounts {
		for i := range m.RoutingRules {
			wp := strings.TrimSpace(m.RoutingRules[i].WritePolicy)
			if wp == "" {
				m.RoutingRules[i].WritePolicy = DefaultWritePolicy
			}
		}
		c.Mounts[mountName] = m
	}
}

// Mount finds a mount configuration by name.
func (c *RootConfig) Mount(name string) (*MountConfig, error) {
	if c == nil {
		return nil, &errkind.NilError{What: "config"}
	}
	if name == "" {
		return nil, &errkind.RequiredError{Msg: "config: mount name is required"}
	}
	if c.Mounts == nil {
		return nil, &errkind.RequiredError{Msg: "config: mounts is required"}
	}
	m, ok := c.Mounts[name]
	if !ok {
		return nil, &errkind.NotFoundError{Msg: fmt.Sprintf("config: mount %q not found", name)}
	}
	return &m, nil
}

// FirstStoragePath returns the first configured storage path (used as a loopback source).
func (m *MountConfig) FirstStoragePath() (string, error) {
	if m == nil {
		return "", &errkind.NilError{What: "mount config"}
	}
	if len(m.StoragePaths) == 0 {
		return "", &errkind.InvalidError{Msg: "config: storage_paths must not be empty"}
	}
	if m.StoragePaths[0].Path == "" {
		return "", &errkind.RequiredError{Msg: "config: storage_paths[0].path is required"}
	}
	return m.StoragePaths[0].Path, nil
}

// GetIndexedStoragePaths returns only storage paths flagged as indexed.
func (m *MountConfig) GetIndexedStoragePaths() ([]StoragePath, error) {
	if m == nil {
		return nil, &errkind.NilError{What: "mount config"}
	}
	indexed := []StoragePath{}
	for _, sp := range m.StoragePaths {
		if sp.Indexed {
			indexed = append(indexed, sp)
		}
	}
	return indexed, nil
}
