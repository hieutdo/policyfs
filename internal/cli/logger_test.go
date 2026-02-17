package cli

import (
	"os"
	"path/filepath"
	"testing"

	"github.com/hieutdo/policyfs/internal/config"
	"github.com/stretchr/testify/require"
)

// TestMount_InvalidLogLevel_returnsFail verifies invalid log levels are rejected through the real CLI flow.
func TestMount_InvalidLogLevel_returnsFail(t *testing.T) {
	cfgDir := t.TempDir()
	cfgPath := filepath.Join(cfgDir, "pfs.yaml")
	mountpoint := filepath.Join(cfgDir, "missing-mount")

	require.NoError(t, os.WriteFile(cfgPath, []byte(`
log:
  level: nope
mounts:
  media:
    mountpoint: `+mountpoint+`
    storage_paths:
      - id: ssd1
        path: /tmp
`), 0o644))

	code, _, stderr := runCLI(t, []string{"--config", cfgPath, "mount", "media"})
	require.Equal(t, ExitFail, code)
	require.Contains(t, stderr, "error: invalid config")
	require.Contains(t, stderr, "cause: unsupported log level")
}

// TestMount_InvalidLogFormat_returnsFail verifies invalid log formats are rejected through the real CLI flow.
func TestMount_InvalidLogFormat_returnsFail(t *testing.T) {
	cfgDir := t.TempDir()
	cfgPath := filepath.Join(cfgDir, "pfs.yaml")
	mountpoint := filepath.Join(cfgDir, "missing-mount")

	require.NoError(t, os.WriteFile(cfgPath, []byte(`
log:
  format: nope
mounts:
  media:
    mountpoint: `+mountpoint+`
    storage_paths:
      - id: ssd1
        path: /tmp
`), 0o644))

	code, _, stderr := runCLI(t, []string{"--config", cfgPath, "mount", "media"})
	require.Equal(t, ExitFail, code)
	require.Contains(t, stderr, "error: invalid config")
	require.Contains(t, stderr, "cause: unsupported log format")
}

// TestMount_LogFileDirMissing_returnsFail verifies missing log file directories are reported as CLI errors.
func TestMount_LogFileDirMissing_returnsFail(t *testing.T) {
	cfgDir := t.TempDir()
	cfgPath := filepath.Join(cfgDir, "pfs.yaml")
	mountpoint := filepath.Join(cfgDir, "missing-mount")
	logPath := filepath.Join(cfgDir, "missing", "pfs.log")

	require.NoError(t, os.WriteFile(cfgPath, []byte(`
mounts:
  media:
    mountpoint: `+mountpoint+`
    storage_paths:
      - id: ssd1
        path: /tmp
`), 0o644))

	code, _, stderr := runCLI(t, []string{"--config", cfgPath, "mount", "media", "--log-file", logPath})
	require.Equal(t, ExitFail, code)
	require.Contains(t, stderr, "error: failed to open log file")
	require.Contains(t, stderr, "cause: log dir missing")
}

// TestMount_LogFile_EnvVarCreatesFile verifies PFS_LOG_FILE enables structured file output through the real CLI flow.
func TestMount_LogFile_EnvVarCreatesFile(t *testing.T) {
	cfgDir := t.TempDir()
	cfgPath := filepath.Join(cfgDir, "pfs.yaml")
	mountpoint := filepath.Join(cfgDir, "missing-mount")
	logPath := filepath.Join(cfgDir, "env.log")

	require.NoError(t, os.WriteFile(cfgPath, []byte(`
mounts:
  media:
    mountpoint: `+mountpoint+`
    storage_paths:
      - id: ssd1
        path: /tmp
`), 0o644))

	old := os.Getenv(config.EnvLogFile)
	require.NoError(t, os.Setenv(config.EnvLogFile, logPath))
	t.Cleanup(func() { _ = os.Setenv(config.EnvLogFile, old) })

	code, _, _ := runCLI(t, []string{"--config", cfgPath, "mount", "media"})
	require.Equal(t, ExitFail, code)

	_, err := os.Stat(logPath)
	require.NoError(t, err)
}

// TestMount_LogFile_FlagOverridesEnv verifies --log-file overrides PFS_LOG_FILE through the real CLI flow.
func TestMount_LogFile_FlagOverridesEnv(t *testing.T) {
	cfgDir := t.TempDir()
	cfgPath := filepath.Join(cfgDir, "pfs.yaml")
	mountpoint := filepath.Join(cfgDir, "missing-mount")
	flagPath := filepath.Join(cfgDir, "flag.log")
	envPath := filepath.Join(cfgDir, "env.log")

	require.NoError(t, os.WriteFile(cfgPath, []byte(`
mounts:
  media:
    mountpoint: `+mountpoint+`
    storage_paths:
      - id: ssd1
        path: /tmp
`), 0o644))

	old := os.Getenv(config.EnvLogFile)
	require.NoError(t, os.Setenv(config.EnvLogFile, envPath))
	t.Cleanup(func() { _ = os.Setenv(config.EnvLogFile, old) })

	code, _, _ := runCLI(t, []string{"--config", cfgPath, "mount", "media", "--log-file", flagPath})
	require.Equal(t, ExitFail, code)

	_, err := os.Stat(flagPath)
	require.NoError(t, err)

	_, err = os.Stat(envPath)
	require.True(t, os.IsNotExist(err))
}

// TestMount_LogFile_YAMLCreatesFile verifies log.file enables structured file output through the real CLI flow.
func TestMount_LogFile_YAMLCreatesFile(t *testing.T) {
	cfgDir := t.TempDir()
	cfgPath := filepath.Join(cfgDir, "pfs.yaml")
	mountpoint := filepath.Join(cfgDir, "missing-mount")
	logPath := filepath.Join(cfgDir, "yaml.log")

	oldEnv, hadEnv := os.LookupEnv(config.EnvLogFile)
	_ = os.Unsetenv(config.EnvLogFile)
	t.Cleanup(func() {
		if hadEnv {
			_ = os.Setenv(config.EnvLogFile, oldEnv)
			return
		}
		_ = os.Unsetenv(config.EnvLogFile)
	})

	require.NoError(t, os.WriteFile(cfgPath, []byte(`
log:
  file: `+logPath+`
mounts:
  media:
    mountpoint: `+mountpoint+`
    storage_paths:
      - id: ssd1
        path: /tmp
`), 0o644))

	code, _, _ := runCLI(t, []string{"--config", cfgPath, "mount", "media"})
	require.Equal(t, ExitFail, code)

	_, err := os.Stat(logPath)
	require.NoError(t, err)
}

// TestMount_LogFile_EnvOverridesYAML verifies PFS_LOG_FILE overrides log.file through the real CLI flow.
func TestMount_LogFile_EnvOverridesYAML(t *testing.T) {
	cfgDir := t.TempDir()
	cfgPath := filepath.Join(cfgDir, "pfs.yaml")
	mountpoint := filepath.Join(cfgDir, "missing-mount")
	yamlPath := filepath.Join(cfgDir, "yaml.log")
	envPath := filepath.Join(cfgDir, "env.log")

	require.NoError(t, os.WriteFile(cfgPath, []byte(`
log:
  file: `+yamlPath+`
mounts:
  media:
    mountpoint: `+mountpoint+`
    storage_paths:
      - id: ssd1
        path: /tmp
`), 0o644))

	old := os.Getenv(config.EnvLogFile)
	require.NoError(t, os.Setenv(config.EnvLogFile, envPath))
	t.Cleanup(func() { _ = os.Setenv(config.EnvLogFile, old) })

	code, _, _ := runCLI(t, []string{"--config", cfgPath, "mount", "media"})
	require.Equal(t, ExitFail, code)

	_, err := os.Stat(envPath)
	require.NoError(t, err)

	_, err = os.Stat(yamlPath)
	require.True(t, os.IsNotExist(err))
}

// TestMount_LogLevelOff_IgnoresEnvLogFile verifies log.level=off disables all log file side effects.
func TestMount_LogLevelOff_IgnoresEnvLogFile(t *testing.T) {
	cfgDir := t.TempDir()
	cfgPath := filepath.Join(cfgDir, "pfs.yaml")
	mountpoint := filepath.Join(cfgDir, "missing-mount")
	logPath := filepath.Join(cfgDir, "missing", "pfs.log")

	require.NoError(t, os.WriteFile(cfgPath, []byte(`
log:
  level: off
mounts:
  media:
    mountpoint: `+mountpoint+`
    storage_paths:
      - id: ssd1
        path: /tmp
`), 0o644))

	old := os.Getenv(config.EnvLogFile)
	require.NoError(t, os.Setenv(config.EnvLogFile, logPath))
	t.Cleanup(func() { _ = os.Setenv(config.EnvLogFile, old) })

	code, _, stderr := runCLI(t, []string{"--config", cfgPath, "mount", "media"})
	require.Equal(t, ExitFail, code)
	require.NotContains(t, stderr, "failed to open log file")
}
