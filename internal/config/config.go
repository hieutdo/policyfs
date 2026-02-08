package config

import (
	"fmt"
	"os"
	"path/filepath"
	"strings"

	"gopkg.in/yaml.v3"
)

const (
	DefaultConfigFilePath = "/etc/pfs/pfs.yaml"
	DefaultLogFilePath    = "/var/log/pfs/pfs.log"
	DefaultStateDir       = "/var/lib/pfs"
	DefaultRuntimeDir     = "/run/pfs"
	DefaultWritePolicy    = "first_found"
)

// KindError preserves a stable message while allowing callers to match a stable Kind via errors.Is.
type KindError struct {
	Kind error
	Msg  string
}

// Error returns the stable message.
func (e *KindError) Error() string { return e.Msg }

// Is matches the error kind for errors.Is.
func (e *KindError) Is(target error) bool { return target == e.Kind }

// ConfigFilePath returns the effective default config file path (PFS_CONFIG_FILE override).
func ConfigFilePath() string {
	p := strings.TrimSpace(os.Getenv("PFS_CONFIG_FILE"))
	if p == "" {
		return DefaultConfigFilePath
	}
	return p
}

// LogFilePath returns the effective default log file path (PFS_LOG_FILE override).
func LogFilePath() string {
	p := strings.TrimSpace(os.Getenv("PFS_LOG_FILE"))
	if p == "" {
		return DefaultLogFilePath
	}
	return p
}

// StateDir returns the persistent state directory (PFS_STATE_DIR override).
func StateDir() string {
	base := strings.TrimSpace(os.Getenv("PFS_STATE_DIR"))
	if base == "" {
		return DefaultStateDir
	}
	return base
}

// RuntimeDir returns the runtime directory (PFS_RUNTIME_DIR override).
func RuntimeDir() string {
	base := strings.TrimSpace(os.Getenv("PFS_RUNTIME_DIR"))
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
		return nil, &KindError{Kind: ErrConfigNil, Msg: "config is nil"}
	}
	if name == "" {
		return nil, &KindError{Kind: ErrMountNameRequired, Msg: "config: mount name is required"}
	}
	if c.Mounts == nil {
		return nil, &KindError{Kind: ErrMountsRequired, Msg: "config: mounts is required"}
	}
	m, ok := c.Mounts[name]
	if !ok {
		return nil, &KindError{Kind: ErrMountNotFound, Msg: fmt.Sprintf("config: mount %q not found", name)}
	}
	return &m, nil
}

// FirstStoragePath returns the first configured storage path (used as a loopback source).
func (m *MountConfig) FirstStoragePath() (string, error) {
	if m == nil {
		return "", &KindError{Kind: ErrMountConfigNil, Msg: "mount config is nil"}
	}
	if len(m.StoragePaths) == 0 {
		return "", &KindError{Kind: ErrStoragePathsEmpty, Msg: "config: storage_paths must not be empty"}
	}
	if m.StoragePaths[0].Path == "" {
		return "", &KindError{Kind: ErrStoragePath0Required, Msg: "config: storage_paths[0].path is required"}
	}
	return m.StoragePaths[0].Path, nil
}
