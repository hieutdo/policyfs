package config

import (
	"os"
	"path/filepath"
	"testing"

	"github.com/hieutdo/policyfs/internal/errkind"
	"github.com/stretchr/testify/require"
)

// TestFirstStoragePath verifies FirstStoragePath returns the first configured storage path.
func TestFirstStoragePath(t *testing.T) {
	cfg := &MountConfig{StoragePaths: []StoragePath{{Path: "/mnt/ssd1/media"}}}
	got, err := cfg.FirstStoragePath()
	require.NoError(t, err)
	require.Equal(t, "/mnt/ssd1/media", got)
}

// TestRootConfig_applyDefaults_shouldFillExpectedDefaults verifies applyDefaults sets expected defaults
// for routing rules and mover job settings.
func TestRootConfig_applyDefaults_shouldFillExpectedDefaults(t *testing.T) {
	cfg := &RootConfig{Mounts: map[string]MountConfig{
		"m": {
			RoutingRules: []RoutingRule{{Match: "**", Targets: []string{"ssd1"}, WritePolicy: ""}},
			Mover: MoverConfig{
				Enabled: nil,
				Jobs: []MoverJobConfig{
					{
						Name: "j1",
						Trigger: MoverTriggerConfig{
							Type:           "usage",
							ThresholdStart: 0,
							ThresholdStop:  0,
							AllowedWindow:  &MoverAllowedWindow{Start: "01:00", End: "02:00", FinishCurrent: nil},
						},
						Destination:    MoverDestinationConfig{Policy: ""},
						DeleteSource:   nil,
						DeleteEmptyDir: nil,
						Verify:         nil,
					},
					{
						Name: "j2",
						Trigger: MoverTriggerConfig{
							Type:           "usage",
							ThresholdStart: 12,
							ThresholdStop:  34,
						},
						Destination:  MoverDestinationConfig{Policy: "first_found"},
						DeleteSource: func() *bool { b := false; return &b }(),
						Verify:       func() *bool { b := true; return &b }(),
					},
				},
			},
		},
	}}

	cfg.applyDefaults()

	m, err := cfg.Mount("m")
	require.NoError(t, err)

	require.Len(t, m.RoutingRules, 1)
	require.Equal(t, DefaultWritePolicy, m.RoutingRules[0].WritePolicy)
	require.Equal(t, DefaultStatfsReporting, m.Statfs.Reporting)
	require.Equal(t, DefaultStatfsOnError, m.Statfs.OnError)

	require.NotNil(t, m.Mover.Enabled)
	require.True(t, *m.Mover.Enabled)

	require.Len(t, m.Mover.Jobs, 2)

	j1 := m.Mover.Jobs[0]
	require.NotNil(t, j1.Trigger.AllowedWindow)
	require.NotNil(t, j1.Trigger.AllowedWindow.FinishCurrent)
	require.True(t, *j1.Trigger.AllowedWindow.FinishCurrent)
	require.Equal(t, DefaultMovePolicy, j1.Destination.Policy)
	require.NotNil(t, j1.DeleteSource)
	require.True(t, *j1.DeleteSource)
	require.NotNil(t, j1.DeleteEmptyDir)
	require.True(t, *j1.DeleteEmptyDir)
	require.NotNil(t, j1.Verify)
	require.False(t, *j1.Verify)
	require.Equal(t, DefaultMoveStartPct, j1.Trigger.ThresholdStart)
	require.Equal(t, DefaultMoveStopPct, j1.Trigger.ThresholdStop)

	j2 := m.Mover.Jobs[1]
	require.Equal(t, 12, j2.Trigger.ThresholdStart)
	require.Equal(t, 34, j2.Trigger.ThresholdStop)
	require.Equal(t, "first_found", j2.Destination.Policy)
	require.NotNil(t, j2.DeleteSource)
	require.False(t, *j2.DeleteSource)
	require.NotNil(t, j2.Verify)
	require.True(t, *j2.Verify)
}

// TestConfigPathHelpers_shouldRespectEnv verifies env overrides for config/log/state/runtime locations.
func TestConfigPathHelpers_shouldRespectEnv(t *testing.T) {
	t.Setenv(EnvConfigFile, "  /tmp/pfs.yaml  ")
	t.Setenv(EnvLogFile, " /tmp/pfs.log ")
	t.Setenv(EnvStateDir, " /var/tmp/pfs-state ")
	t.Setenv(EnvRuntimeDir, " /var/tmp/pfs-run ")

	require.Equal(t, "/tmp/pfs.yaml", ConfigFilePath())
	require.Equal(t, "/tmp/pfs.log", LogFilePath())
	require.Equal(t, "/var/tmp/pfs-state", StateDir())
	require.Equal(t, "/var/tmp/pfs-run", RuntimeDir())

	require.Equal(t, filepath.Join("/var/tmp/pfs-state", "media"), MountStateDir("media"))
	require.Equal(t, filepath.Join("/var/tmp/pfs-run", "media"), MountRuntimeDir("media"))
	require.Equal(t, filepath.Join("/var/tmp/pfs-run", "media", "locks"), MountLockDir("media"))
}

// TestRootConfig_Mount_shouldReturnTypedErrors verifies Mount returns typed errors for common failures.
func TestRootConfig_Mount_shouldReturnTypedErrors(t *testing.T) {
	_, err := (*RootConfig)(nil).Mount("media")
	require.Error(t, err)
	require.ErrorIs(t, err, errkind.ErrNil)

	_, err = (&RootConfig{}).Mount("")
	require.Error(t, err)
	require.ErrorIs(t, err, errkind.ErrRequired)

	_, err = (&RootConfig{}).Mount("media")
	require.Error(t, err)
	require.ErrorIs(t, err, errkind.ErrRequired)

	_, err = (&RootConfig{Mounts: map[string]MountConfig{}}).Mount("media")
	require.Error(t, err)
	require.ErrorIs(t, err, errkind.ErrNotFound)
}

// TestFirstStoragePath_shouldValidateInputs verifies FirstStoragePath returns typed errors for nil/empty.
func TestFirstStoragePath_shouldValidateInputs(t *testing.T) {
	_, err := (*MountConfig)(nil).FirstStoragePath()
	require.Error(t, err)
	require.ErrorIs(t, err, errkind.ErrNil)

	_, err = (&MountConfig{}).FirstStoragePath()
	require.Error(t, err)
	require.ErrorIs(t, err, errkind.ErrInvalid)

	_, err = (&MountConfig{StoragePaths: []StoragePath{{Path: ""}}}).FirstStoragePath()
	require.Error(t, err)
	require.ErrorIs(t, err, errkind.ErrRequired)
}

// TestGetIndexedStoragePaths_shouldFilter verifies GetIndexedStoragePaths filters only indexed storages.
func TestGetIndexedStoragePaths_shouldFilter(t *testing.T) {
	_, err := (*MountConfig)(nil).GetIndexedStoragePaths()
	require.Error(t, err)
	require.ErrorIs(t, err, errkind.ErrNil)

	m := &MountConfig{StoragePaths: []StoragePath{{ID: "a", Indexed: true}, {ID: "b", Indexed: false}}}
	got, err := m.GetIndexedStoragePaths()
	require.NoError(t, err)
	require.Len(t, got, 1)
	require.Equal(t, "a", got[0].ID)
}

// TestLoad_shouldDefaultMoverVerifyToFalse verifies Load applies mover job defaults via applyDefaults.
func TestLoad_shouldDefaultMoverVerifyToFalse(t *testing.T) {
	dir := t.TempDir()
	p := filepath.Join(dir, "pfs.yaml")

	content := `
mounts:
  media:
    mountpoint: "/mnt/pfs/media"
    storage_paths:
      - id: "ssd1"
        path: "/mnt/ssd1/media"
        indexed: false
      - id: "hdd1"
        path: "/mnt/hdd1/media"
        indexed: false
    routing_rules:
      - match: "**"
        targets: ["ssd1"]
    mover:
      enabled: true
      jobs:
        - name: "archive"
          trigger:
            type: manual
          source:
            paths: ["ssd1"]
            patterns: ["library/**"]
          destination:
            paths: ["hdd1"]
            policy: first_found
`

	if err := os.WriteFile(p, []byte(content), 0o644); err != nil {
		require.NoError(t, err)
	}

	cfg, err := Load(p)
	require.NoError(t, err)
	m, err := cfg.Mount("media")
	require.NoError(t, err)
	require.Len(t, m.Mover.Jobs, 1)
	require.NotNil(t, m.Mover.Jobs[0].Verify)
	require.False(t, *m.Mover.Jobs[0].Verify)
}
