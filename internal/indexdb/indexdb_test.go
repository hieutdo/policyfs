package indexdb

import (
	"database/sql"
	"os"
	"path/filepath"
	"strings"
	"testing"

	"github.com/pressly/goose/v3"
	"github.com/stretchr/testify/require"

	"github.com/hieutdo/policyfs/internal/config"
	_ "github.com/mattn/go-sqlite3"
)

// Test_mountStateDir_shouldUseDefaultWhenEnvUnset verifies config.MountStateDir uses DefaultStateDir when PFS_STATE_DIR is unset.
func Test_mountStateDir_shouldUseDefaultWhenEnvUnset(t *testing.T) {
	t.Setenv("PFS_STATE_DIR", "")
	require.Equal(t, filepath.Join(config.DefaultStateDir, "media"), config.MountStateDir("media"))
}

// Test_mountStateDir_shouldUseEnvOverride verifies config.MountStateDir uses PFS_STATE_DIR when provided.
func Test_mountStateDir_shouldUseEnvOverride(t *testing.T) {
	t.Setenv("PFS_STATE_DIR", "/tmp/pfs-state")
	require.Equal(t, filepath.Join("/tmp/pfs-state", "media"), config.MountStateDir("media"))
}

// TestOpen_shouldCreateDBUnderStateDir verifies Open creates the DB under the PFS_STATE_DIR override.
func TestOpen_shouldCreateDBUnderStateDir(t *testing.T) {
	base := t.TempDir()
	t.Setenv("PFS_STATE_DIR", base)

	db, err := Open("media")
	require.NoError(t, err)
	t.Cleanup(func() { _ = db.Close() })

	want := filepath.Join(base, "media", "index.db")
	require.Equal(t, want, db.Path)
	_, err = os.Stat(want)
	require.NoError(t, err)
}

// TestOpen_shouldBackupAndRecreateWhenDBVersion2 verifies Open treats DB version 2 as incompatible.
func TestOpen_shouldBackupAndRecreateWhenDBVersion2(t *testing.T) {
	base := t.TempDir()
	t.Setenv("PFS_STATE_DIR", base)

	mountName := "media"
	dbPath := filepath.Join(base, mountName, "index.db")
	require.NoError(t, os.MkdirAll(filepath.Dir(dbPath), 0o755))

	conn, err := sql.Open("sqlite3", "file:"+dbPath)
	require.NoError(t, err)
	t.Cleanup(func() { _ = conn.Close() })

	_, err = conn.Exec(`CREATE TABLE goose_db_version (
    id INTEGER PRIMARY KEY AUTOINCREMENT,
    version_id INTEGER NOT NULL,
    is_applied INTEGER NOT NULL,
    tstamp TIMESTAMP DEFAULT CURRENT_TIMESTAMP
);`)
	require.NoError(t, err)
	_, err = conn.Exec(`INSERT INTO goose_db_version (version_id, is_applied) VALUES (0, 1);`)
	require.NoError(t, err)
	_, err = conn.Exec(`INSERT INTO goose_db_version (version_id, is_applied) VALUES (2, 1);`)
	require.NoError(t, err)
	require.NoError(t, conn.Close())

	db, err := Open(mountName)
	require.NoError(t, err)
	t.Cleanup(func() { _ = db.Close() })

	ver, err := goose.EnsureDBVersion(db.SQL())
	require.NoError(t, err)
	require.Equal(t, int64(1), ver)

	entries, err := os.ReadDir(filepath.Dir(dbPath))
	require.NoError(t, err)

	found := false
	for _, e := range entries {
		if strings.HasPrefix(e.Name(), "index.db.incompatible.") {
			found = true
			break
		}
	}
	require.True(t, found)
}
