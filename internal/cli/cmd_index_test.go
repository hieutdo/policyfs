package cli

import (
	"encoding/json"
	"os"
	"path/filepath"
	"testing"

	"github.com/stretchr/testify/require"
)

// TestIndex_Text_shouldPrintSummary verifies `pfs index <mount>` prints a short human-readable summary to stdout.
func TestIndex_Text_shouldPrintSummary(t *testing.T) {
	runtimeDir := filepath.Join(t.TempDir(), "runtime")
	require.NoError(t, os.MkdirAll(runtimeDir, 0o755))
	t.Setenv("PFS_RUNTIME_DIR", runtimeDir)

	stateDir := filepath.Join(t.TempDir(), "state")
	require.NoError(t, os.MkdirAll(stateDir, 0o755))
	t.Setenv("PFS_STATE_DIR", stateDir)

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
	require.Contains(t, stdout, "OK\n")
	require.Contains(t, stdout, "mount: media\n")
	require.Contains(t, stdout, "hdd1:")
	require.Contains(t, stdout, "total:")
}

// TestIndex_Reset_shouldReplaceNonSQLiteDB verifies `pfs index --reset` deletes an existing DB file and recreates a fresh one.
func TestIndex_Reset_shouldReplaceNonSQLiteDB(t *testing.T) {
	runtimeDir := filepath.Join(t.TempDir(), "runtime")
	require.NoError(t, os.MkdirAll(runtimeDir, 0o755))
	t.Setenv("PFS_RUNTIME_DIR", runtimeDir)

	stateDir := filepath.Join(t.TempDir(), "state")
	require.NoError(t, os.MkdirAll(stateDir, 0o755))
	t.Setenv("PFS_STATE_DIR", stateDir)

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

	code, _, _ := runCLI(t, []string{"--config", cfg, "index", "media", "--reset"})
	require.Equal(t, ExitOK, code)

	b, err := os.ReadFile(dbPath)
	require.NoError(t, err)
	require.GreaterOrEqual(t, len(b), 16)
	require.Equal(t, "SQLite format 3\x00", string(b[:16]))
}

// TestIndex_JSON_shouldEmitValidJSON verifies `pfs index --json` emits valid JSON only on stdout.
func TestIndex_JSON_shouldEmitValidJSON(t *testing.T) {
	runtimeDir := filepath.Join(t.TempDir(), "runtime")
	require.NoError(t, os.MkdirAll(runtimeDir, 0o755))
	t.Setenv("PFS_RUNTIME_DIR", runtimeDir)

	stateDir := filepath.Join(t.TempDir(), "state")
	require.NoError(t, os.MkdirAll(stateDir, 0o755))
	t.Setenv("PFS_STATE_DIR", stateDir)

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

	code, stdout, _ := runCLI(t, []string{"--config", cfg, "index", "media", "--json"})
	require.Equal(t, ExitOK, code)

	var out JSONIndexOutput
	require.NoError(t, json.Unmarshal([]byte(stdout), &out))
	require.Equal(t, "index", out.Command)
	require.True(t, out.OK)
	require.NotNil(t, out.Scope)
	require.NotNil(t, out.Scope.Config)
	require.NotNil(t, out.Scope.Mount)
	require.NotNil(t, out.Result)
}
