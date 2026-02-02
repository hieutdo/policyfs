package config

import (
	"errors"
	"fmt"
	"os"

	"gopkg.in/yaml.v3"
)

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
		return nil, errors.New("config is nil")
	}
	if name == "" {
		return nil, errors.New("config: mount name is required")
	}
	if c.Mounts == nil {
		return nil, errors.New("config: mounts is required")
	}
	m, ok := c.Mounts[name]
	if !ok {
		return nil, fmt.Errorf("config: mount %q not found", name)
	}
	return &m, nil
}

// FirstStoragePath returns the first configured storage path (used as a loopback source).
func (m *MountConfig) FirstStoragePath() (string, error) {
	if m == nil {
		return "", errors.New("mount config is nil")
	}
	if len(m.StoragePaths) == 0 {
		return "", errors.New("config: storage_paths must not be empty")
	}
	if m.StoragePaths[0].Path == "" {
		return "", errors.New("config: storage_paths[0].path is required")
	}
	return m.StoragePaths[0].Path, nil
}
