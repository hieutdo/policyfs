package config

import (
	"bytes"
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
	DefaultMovePolicy     = "most_free"
	DefaultMoveStartPct   = 80
	DefaultMoveStopPct    = 70
)

const (
	EnvTZ                          = "TZ"
	EnvConfigFile                  = "PFS_CONFIG_FILE"
	EnvLogFile                     = "PFS_LOG_FILE"
	EnvLogDiskAccess               = "PFS_LOG_DISK_ACCESS"
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
	File   string `yaml:"file"`
}

// MountLogConfig overrides global logging settings for a specific mount.
type MountLogConfig struct {
	Level string `yaml:"level"`
}

// MountConfig defines a single mount instance configuration.
type MountConfig struct {
	MountPoint    string              `yaml:"mountpoint"`
	Log           MountLogConfig      `yaml:"log"`
	StoragePaths  []StoragePath       `yaml:"storage_paths"`
	StorageGroups map[string][]string `yaml:"storage_groups"`
	RoutingRules  []RoutingRule       `yaml:"routing_rules"`
	Indexer       IndexerConfig       `yaml:"indexer"`
	Mover         MoverConfig         `yaml:"mover"`
}

// EffectiveLogConfig returns the log config that should be used for this mount.
//
// Mount-scoped fields override the root config when non-empty.
func (m *MountConfig) EffectiveLogConfig(root LogConfig) LogConfig {
	if m == nil {
		return root
	}
	out := root
	if strings.TrimSpace(m.Log.Level) != "" {
		out.Level = m.Log.Level
	}
	return out
}

// MoverConfig controls mover jobs for a mount.
type MoverConfig struct {
	Enabled *bool            `yaml:"enabled"`
	Jobs    []MoverJobConfig `yaml:"jobs"`
}

// MoverJobConfig defines a single move job.
type MoverJobConfig struct {
	Name           string                 `yaml:"name"`
	Description    string                 `yaml:"description"`
	Trigger        MoverTriggerConfig     `yaml:"trigger"`
	Source         MoverSourceConfig      `yaml:"source"`
	Destination    MoverDestinationConfig `yaml:"destination"`
	Conditions     MoverConditionsConfig  `yaml:"conditions"`
	DeleteSource   *bool                  `yaml:"delete_source"`
	DeleteEmptyDir *bool                  `yaml:"delete_empty_dir"`
	Verify         *bool                  `yaml:"verify"`
}

// MoverTriggerConfig defines when a job should run.
type MoverTriggerConfig struct {
	Type           string              `yaml:"type"`
	ThresholdStart int                 `yaml:"threshold_start"`
	ThresholdStop  int                 `yaml:"threshold_stop"`
	AllowedWindow  *MoverAllowedWindow `yaml:"allowed_window"`
}

// MoverAllowedWindow restricts usage-triggered runs to a time window.
type MoverAllowedWindow struct {
	Start         string `yaml:"start"`
	End           string `yaml:"end"`
	FinishCurrent *bool  `yaml:"finish_current"`
}

// MoverSourceConfig defines where candidates come from.
type MoverSourceConfig struct {
	Paths       []string `yaml:"paths"`
	Groups      []string `yaml:"groups"`
	Patterns    []string `yaml:"patterns"`
	Ignore      []string `yaml:"ignore"`
	IncludeFile string   `yaml:"include_file"`
	IgnoreFile  string   `yaml:"ignore_file"`
}

// MoverDestinationConfig defines where candidates are moved to.
type MoverDestinationConfig struct {
	Paths          []string `yaml:"paths"`
	Groups         []string `yaml:"groups"`
	Policy         string   `yaml:"policy"`
	PathPreserving bool     `yaml:"path_preserving"`
}

// MoverConditionsConfig filters candidates.
type MoverConditionsConfig struct {
	MinAge  string `yaml:"min_age"`
	MinSize string `yaml:"min_size"`
	MaxSize string `yaml:"max_size"`
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
	dec := yaml.NewDecoder(bytes.NewReader(b))
	dec.KnownFields(true)
	if err := dec.Decode(&cfg); err != nil {
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

		if m.Mover.Enabled == nil {
			en := true
			m.Mover.Enabled = &en
		}
		for i := range m.Mover.Jobs {
			j := &m.Mover.Jobs[i]

			if j.Trigger.AllowedWindow != nil && j.Trigger.AllowedWindow.FinishCurrent == nil {
				fc := true
				j.Trigger.AllowedWindow.FinishCurrent = &fc
			}
			if strings.TrimSpace(j.Destination.Policy) == "" {
				j.Destination.Policy = DefaultMovePolicy
			}
			if j.DeleteSource == nil {
				ds := true
				j.DeleteSource = &ds
			}
			if j.DeleteEmptyDir == nil {
				ded := true
				j.DeleteEmptyDir = &ded
			}
			if j.Verify == nil {
				v := false
				j.Verify = &v
			}

			tt := strings.TrimSpace(j.Trigger.Type)
			if tt == "usage" {
				if j.Trigger.ThresholdStart == 0 {
					j.Trigger.ThresholdStart = DefaultMoveStartPct
				}
				if j.Trigger.ThresholdStop == 0 {
					j.Trigger.ThresholdStop = DefaultMoveStopPct
				}
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
