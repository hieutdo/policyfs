package cli

import (
	"bufio"
	"os"
	"os/exec"
	"path/filepath"
	"testing"

	"github.com/hieutdo/policyfs/internal/config"
	"github.com/hieutdo/policyfs/internal/lock"
	"github.com/stretchr/testify/require"
)

// TestIndex_Text_shouldPrintSummary verifies `pfs index <mount>` prints a short human-readable summary to stdout.
func TestIndex_Text_shouldPrintSummary(t *testing.T) {
	if os.Getenv(config.EnvTestHelper) == "1" {
		return
	}

	runtimeDir := filepath.Join(t.TempDir(), "runtime")
	require.NoError(t, os.MkdirAll(runtimeDir, 0o755))
	t.Setenv(config.EnvRuntimeDir, runtimeDir)

	stateDir := filepath.Join(t.TempDir(), "state")
	require.NoError(t, os.MkdirAll(stateDir, 0o755))
	t.Setenv(config.EnvStateDir, stateDir)

	storageRoot := filepath.Join(t.TempDir(), "hdd1")
	require.NoError(t, os.MkdirAll(filepath.Join(storageRoot, "library"), 0o755))
	require.NoError(t, os.WriteFile(filepath.Join(storageRoot, "library", "a.txt"), []byte("hello"), 0o644))

	cfg := writeTempConfig(t, `
mounts:
  media:
    mountpoint: "/mnt/pfs/media"
    storage_paths:
      - id: "hdd1"
        path: "`+storageRoot+`"
        indexed: true
    routing_rules:
      - match: "**"
        targets: ["hdd1"]
`)

	code, stdout, _ := runCLI(t, []string{"--config", cfg, "index", "media"})
	require.Equal(t, ExitOK, code)
	require.Contains(t, stdout, "pfs index: mount=media")
	require.Contains(t, stdout, "Scanning: hdd1")
	require.Contains(t, stdout, "Summary:")
	require.Contains(t, stdout, "DB")
	require.Contains(t, stdout, "upserts")
	require.Contains(t, stdout, "Done:")
}

// TestIndex_Rebuild_shouldReplaceNonSQLiteDB verifies `pfs index --rebuild` deletes an existing DB file and recreates a fresh one.
func TestIndex_Rebuild_shouldReplaceNonSQLiteDB(t *testing.T) {
	if os.Getenv(config.EnvTestHelper) == "1" {
		return
	}

	runtimeDir := filepath.Join(t.TempDir(), "runtime")
	require.NoError(t, os.MkdirAll(runtimeDir, 0o755))
	t.Setenv(config.EnvRuntimeDir, runtimeDir)

	stateDir := filepath.Join(t.TempDir(), "state")
	require.NoError(t, os.MkdirAll(stateDir, 0o755))
	t.Setenv(config.EnvStateDir, stateDir)

	storageRoot := filepath.Join(t.TempDir(), "hdd1")
	require.NoError(t, os.MkdirAll(filepath.Join(storageRoot, "library"), 0o755))
	require.NoError(t, os.WriteFile(filepath.Join(storageRoot, "library", "a.txt"), []byte("hello"), 0o644))

	cfg := writeTempConfig(t, `
mounts:
  media:
    mountpoint: "/mnt/pfs/media"
    storage_paths:
      - id: "hdd1"
        path: "`+storageRoot+`"
        indexed: true
    routing_rules:
      - match: "**"
        targets: ["hdd1"]
`)

	dbPath := filepath.Join(stateDir, "media", "index.db")
	require.NoError(t, os.MkdirAll(filepath.Dir(dbPath), 0o755))
	require.NoError(t, os.WriteFile(dbPath, []byte("not a sqlite db"), 0o644))

	code, _, _ := runCLI(t, []string{"--config", cfg, "index", "media", "--rebuild"})
	require.Equal(t, ExitOK, code)

	b, err := os.ReadFile(dbPath)
	require.NoError(t, err)
	require.GreaterOrEqual(t, len(b), 16)
	require.Equal(t, "SQLite format 3\x00", string(b[:16]))
}

// TestIndex_Progress_shouldPrintProgress verifies `pfs index --progress` prints progress output.
func TestIndex_Progress_shouldPrintProgress(t *testing.T) {
	if os.Getenv(config.EnvTestHelper) == "1" {
		return
	}

	runtimeDir := filepath.Join(t.TempDir(), "runtime")
	require.NoError(t, os.MkdirAll(runtimeDir, 0o755))
	t.Setenv(config.EnvRuntimeDir, runtimeDir)

	stateDir := filepath.Join(t.TempDir(), "state")
	require.NoError(t, os.MkdirAll(stateDir, 0o755))
	t.Setenv(config.EnvStateDir, stateDir)

	storageRoot := filepath.Join(t.TempDir(), "hdd1")
	require.NoError(t, os.MkdirAll(filepath.Join(storageRoot, "library"), 0o755))
	require.NoError(t, os.WriteFile(filepath.Join(storageRoot, "library", "a.txt"), []byte("hello"), 0o644))

	cfg := writeTempConfig(t, `
mounts:
  media:
    mountpoint: "/mnt/pfs/media"
    storage_paths:
      - id: "hdd1"
        path: "`+storageRoot+`"
        indexed: true
    routing_rules:
      - match: "**"
        targets: ["hdd1"]
`)

	code, stdout, _ := runCLI(t, []string{"--config", cfg, "index", "media"})
	require.Equal(t, ExitOK, code)
	require.Contains(t, stdout, "Indexing")
}

// TestIndex_Quiet_shouldSuppressProgress verifies `pfs index --quiet` suppresses progress output.
func TestIndex_Quiet_shouldSuppressProgress(t *testing.T) {
	if os.Getenv(config.EnvTestHelper) == "1" {
		return
	}

	runtimeDir := filepath.Join(t.TempDir(), "runtime")
	require.NoError(t, os.MkdirAll(runtimeDir, 0o755))
	t.Setenv(config.EnvRuntimeDir, runtimeDir)

	stateDir := filepath.Join(t.TempDir(), "state")
	require.NoError(t, os.MkdirAll(stateDir, 0o755))
	t.Setenv(config.EnvStateDir, stateDir)

	storageRoot := filepath.Join(t.TempDir(), "hdd1")
	require.NoError(t, os.MkdirAll(filepath.Join(storageRoot, "library"), 0o755))
	require.NoError(t, os.WriteFile(filepath.Join(storageRoot, "library", "a.txt"), []byte("hello"), 0o644))

	cfg := writeTempConfig(t, `
mounts:
  media:
    mountpoint: "/mnt/pfs/media"
    storage_paths:
      - id: "hdd1"
        path: "`+storageRoot+`"
        indexed: true
    routing_rules:
      - match: "**"
        targets: ["hdd1"]
`)

	code, stdout, _ := runCLI(t, []string{"--config", cfg, "index", "media", "--quiet"})
	require.Equal(t, ExitOK, code)
	require.Contains(t, stdout, "Summary:")
	require.Contains(t, stdout, "Done:")
}

// TestIndex_noIndexedStorages_shouldReturnNoChanges verifies a mount with no indexed storages returns ExitNoChanges.
func TestIndex_noIndexedStorages_shouldReturnNoChanges(t *testing.T) {
	if os.Getenv(config.EnvTestHelper) == "1" {
		return
	}

	runtimeDir := filepath.Join(t.TempDir(), "runtime")
	require.NoError(t, os.MkdirAll(runtimeDir, 0o755))
	t.Setenv(config.EnvRuntimeDir, runtimeDir)

	stateDir := filepath.Join(t.TempDir(), "state")
	require.NoError(t, os.MkdirAll(stateDir, 0o755))
	t.Setenv(config.EnvStateDir, stateDir)

	storageRoot := filepath.Join(t.TempDir(), "hdd1")
	require.NoError(t, os.MkdirAll(storageRoot, 0o755))

	cfg := writeTempConfig(t, `
mounts:
  media:
    mountpoint: "/mnt/pfs/media"
    storage_paths:
      - id: "hdd1"
        path: "`+storageRoot+`"
        indexed: false
    routing_rules:
      - match: "**"
        targets: ["hdd1"]
`)

	code, stdout, stderr := runCLI(t, []string{"--config", cfg, "index", "media"})
	require.Equal(t, ExitNoChanges, code)
	require.Empty(t, stderr)
	require.Contains(t, stdout, "pfs index: mount=media")
	require.Contains(t, stdout, "Skipped: no indexed storages")
	require.NotContains(t, stdout, "Summary")
}

// TestIndex_emptyIndexedStorage_shouldReturnNoChanges verifies an indexed storage with no files returns ExitNoChanges.
func TestIndex_emptyIndexedStorage_shouldReturnNoChanges(t *testing.T) {
	if os.Getenv(config.EnvTestHelper) == "1" {
		return
	}

	runtimeDir := filepath.Join(t.TempDir(), "runtime")
	require.NoError(t, os.MkdirAll(runtimeDir, 0o755))
	t.Setenv(config.EnvRuntimeDir, runtimeDir)

	stateDir := filepath.Join(t.TempDir(), "state")
	require.NoError(t, os.MkdirAll(stateDir, 0o755))
	t.Setenv(config.EnvStateDir, stateDir)

	storageRoot := filepath.Join(t.TempDir(), "hdd1")
	require.NoError(t, os.MkdirAll(storageRoot, 0o755))

	cfg := writeTempConfig(t, `
mounts:
  media:
    mountpoint: "/mnt/pfs/media"
    storage_paths:
      - id: "hdd1"
        path: "`+storageRoot+`"
        indexed: true
    routing_rules:
      - match: "**"
        targets: ["hdd1"]
`)

	code, stdout, stderr := runCLI(t, []string{"--config", cfg, "index", "media", "--quiet"})
	require.Equal(t, ExitNoChanges, code)
	require.Empty(t, stderr)
	require.Contains(t, stdout, "nothing to index")
	require.NotContains(t, stdout, "Summary")
}

// TestIndex_InvalidArgs_shouldReturnUsage verifies missing mount arg returns ExitUsage.
func TestIndex_InvalidArgs_shouldReturnUsage(t *testing.T) {
	if os.Getenv(config.EnvTestHelper) == "1" {
		return
	}

	code, _, stderr := runCLI(t, []string{"index"})
	require.Equal(t, ExitUsage, code)
	require.Contains(t, stderr, "error: invalid arguments")
}

// TestIndex_InvalidMountName_shouldReturnUsage verifies invalid mount names are rejected.
func TestIndex_InvalidMountName_shouldReturnUsage(t *testing.T) {
	if os.Getenv(config.EnvTestHelper) == "1" {
		return
	}

	code, _, stderr := runCLI(t, []string{"index", "bad mount"})
	require.Equal(t, ExitUsage, code)
	require.Contains(t, stderr, "error: invalid arguments")
	require.Contains(t, stderr, "hint: run 'pfs index --help'")
}

// TestIndex_UnknownMount_shouldReturnUsage verifies unknown mounts are reported as usage errors.
func TestIndex_UnknownMount_shouldReturnUsage(t *testing.T) {
	if os.Getenv(config.EnvTestHelper) == "1" {
		return
	}

	cfg := writeTempConfig(t, `
mounts:
  media:
    mountpoint: "/mnt/pfs/media"
    storage_paths:
      - id: "hdd1"
        path: "/tmp/hdd1"
        indexed: true
    routing_rules:
      - match: "**"
        targets: ["hdd1"]
`)

	code, _, stderr := runCLI(t, []string{"--config", cfg, "index", "unknown"})
	require.Equal(t, ExitUsage, code)
	require.Contains(t, stderr, "error: invalid arguments")
}

// TestIndex_InvalidProgressValue_shouldReturnUsage verifies invalid --progress values are rejected.
func TestIndex_InvalidProgressFile_shouldReturnUsage(t *testing.T) {
	if os.Getenv(config.EnvTestHelper) == "1" {
		return
	}

	cfg := writeTempConfig(t, `
mounts:
  media:
    mountpoint: "/mnt/pfs/media"
    storage_paths:
      - id: "hdd1"
        path: "/tmp/hdd1"
        indexed: true
    routing_rules:
      - match: "**"
        targets: ["hdd1"]
`)

	code, _, stderr := runCLI(t, []string{"--config", cfg, "index", "media", "--progress=nope"})
	require.Equal(t, ExitUsage, code)
	require.Contains(t, stderr, "error: invalid arguments")
}

// TestIndex_UnknownStorageID_shouldReturnUsage verifies --storage rejects unknown storage ids.
func TestIndex_UnknownStorageID_shouldReturnUsage(t *testing.T) {
	if os.Getenv(config.EnvTestHelper) == "1" {
		return
	}

	cfg := writeTempConfig(t, `
mounts:
  media:
    mountpoint: "/mnt/pfs/media"
    storage_paths:
      - id: "hdd1"
        path: "/tmp/hdd1"
        indexed: true
    routing_rules:
      - match: "**"
        targets: ["hdd1"]
`)

	code, _, stderr := runCLI(t, []string{"--config", cfg, "index", "media", "--storage", "hdd2"})
	require.Equal(t, ExitUsage, code)
	require.Contains(t, stderr, "error: invalid arguments")
}

// TestIndex_StorageNotIndexed_shouldReturnUsage verifies --storage rejects non-indexed storage.
func TestIndex_StorageNotIndexed_shouldReturnUsage(t *testing.T) {
	if os.Getenv(config.EnvTestHelper) == "1" {
		return
	}

	cfg := writeTempConfig(t, `
mounts:
  media:
    mountpoint: "/mnt/pfs/media"
    storage_paths:
      - id: "hdd1"
        path: "/tmp/hdd1"
        indexed: false
    routing_rules:
      - match: "**"
        targets: ["hdd1"]
`)

	code, _, stderr := runCLI(t, []string{"--config", cfg, "index", "media", "--storage", "hdd1"})
	require.Equal(t, ExitUsage, code)
	require.Contains(t, stderr, "error: invalid arguments")
}

// TestIndex_JobLockBusy_shouldReturnExitBusy verifies index returns ExitBusy when job.lock is held.
func TestIndex_Busy_shouldReturnExitBusy(t *testing.T) {
	if os.Getenv(config.EnvTestHelper) == "1" {
		return
	}

	runtimeDir := filepath.Join(t.TempDir(), "runtime")
	require.NoError(t, os.MkdirAll(runtimeDir, 0o755))
	t.Setenv(config.EnvRuntimeDir, runtimeDir)

	stateDir := filepath.Join(t.TempDir(), "state")
	require.NoError(t, os.MkdirAll(stateDir, 0o755))
	t.Setenv(config.EnvStateDir, stateDir)

	storageRoot := filepath.Join(t.TempDir(), "hdd1")
	require.NoError(t, os.MkdirAll(filepath.Join(storageRoot, "library"), 0o755))
	require.NoError(t, os.WriteFile(filepath.Join(storageRoot, "library", "a.txt"), []byte("hello"), 0o644))

	cfg := writeTempConfig(t, `
mounts:
  media:
    mountpoint: "/mnt/pfs/media"
    storage_paths:
      - id: "hdd1"
        path: "`+storageRoot+`"
        indexed: true
    routing_rules:
      - match: "**"
        targets: ["hdd1"]
`)

	cmd := exec.Command(os.Args[0], "-test.run=TestHelperProcessIndexHoldJobLock")
	cmd.Env = append(
		os.Environ(),
		config.EnvTestHelper+"=1",
		config.EnvRuntimeDir+"="+runtimeDir,
		config.EnvStateDir+"="+stateDir,
		config.EnvTestMount+"=media",
		config.EnvTestLockFile+"="+config.DefaultJobLockFile,
	)
	stdout, err := cmd.StdoutPipe()
	require.NoError(t, err)
	stdin, err := cmd.StdinPipe()
	require.NoError(t, err)
	cmd.Stderr = os.Stderr
	require.NoError(t, cmd.Start())

	waited := false
	t.Cleanup(func() {
		_ = stdin.Close()
		if !waited {
			_ = cmd.Process.Kill()
			_ = cmd.Wait()
		}
	})

	r := bufio.NewReader(stdout)
	line, err := r.ReadString('\n')
	require.NoError(t, err)
	require.Equal(t, "ready\n", line)

	code, _, stderr := runCLI(t, []string{"--config", cfg, "index", "media", "--quiet"})
	require.Equal(t, ExitBusy, code)
	require.Contains(t, stderr, "error: job already running")

	require.NoError(t, stdin.Close())
	require.NoError(t, cmd.Wait())
	waited = true
}

// TestHelperProcessIndexHoldJobLock holds job.lock until stdin closes.
func TestHelperProcessIndexHoldJobLock(t *testing.T) {
	if os.Getenv(config.EnvTestHelper) != "1" {
		return
	}

	mountName := os.Getenv(config.EnvTestMount)
	lockFile := os.Getenv(config.EnvTestLockFile)

	lk, err := lock.AcquireMountLock(mountName, lockFile)
	if err != nil {
		os.Exit(1)
	}

	_, _ = os.Stdout.Write([]byte("ready\n"))
	_, _ = bufio.NewReader(os.Stdin).ReadBytes(0)
	_ = lk.Close()
	os.Exit(0)
}
