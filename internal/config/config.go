package config

import (
	"errors"
	"fmt"
	"os"

	"gopkg.in/yaml.v3"
)

var (
	// ErrConfigNil is returned when a config receiver is nil.
	ErrConfigNil = errors.New("config is nil")
	// ErrMountNameRequired is returned when a mount name is missing.
	ErrMountNameRequired = errors.New("mount name is required")
	// ErrMountsRequired is returned when mounts are not configured.
	ErrMountsRequired = errors.New("mounts is required")
	// ErrMountNotFound is returned when a mount name does not exist in the config.
	ErrMountNotFound = errors.New("mount not found")

	// ErrMountConfigNil is returned when a mount config receiver is nil.
	ErrMountConfigNil = errors.New("mount config is nil")
	// ErrStoragePathsEmpty is returned when storage_paths is empty.
	ErrStoragePathsEmpty = errors.New("storage_paths must not be empty")
	// ErrStoragePath0Required is returned when the first storage path has an empty path.
	ErrStoragePath0Required = errors.New("storage_paths[0].path is required")
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

	return &cfg, nil
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
