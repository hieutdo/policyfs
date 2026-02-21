package indexdb

import (
	"testing"

	"github.com/stretchr/testify/require"

	"github.com/hieutdo/policyfs/internal/config"
)

// TestQueryStaleCount_DBMissing_shouldReturnZero verifies QueryStaleCount returns 0 with no error when the index DB does not exist.
func TestQueryStaleCount_DBMissing_shouldReturnZero(t *testing.T) {
	base := t.TempDir()
	t.Setenv(config.EnvStateDir, base)

	count, err := QueryStaleCount("media", "hdd1")
	require.NoError(t, err)
	require.Equal(t, int64(0), count)
}

// TestQueryStaleCount_shouldCountDeletedEquals2 verifies QueryStaleCount counts only rows with deleted=2 for the given storage.
func TestQueryStaleCount_shouldCountDeletedEquals2(t *testing.T) {
	base := t.TempDir()
	t.Setenv(config.EnvStateDir, base)

	db, err := Open("media")
	require.NoError(t, err)
	t.Cleanup(func() { _ = db.Close() })

	// Insert rows. The files table requires (storage_id, path, parent_dir, name, mtime, mode, uid, gid).
	_, err = db.SQL().Exec(`INSERT INTO files (storage_id, path, parent_dir, name, is_dir, mtime, mode, uid, gid, deleted)
VALUES (?, ?, ?, ?, 0, 0, 0, 0, 0, 2);`, "hdd1", "a.txt", "", "a.txt")
	require.NoError(t, err)
	_, err = db.SQL().Exec(`INSERT INTO files (storage_id, path, parent_dir, name, is_dir, mtime, mode, uid, gid, deleted)
VALUES (?, ?, ?, ?, 0, 0, 0, 0, 0, 2);`, "hdd1", "b.txt", "", "b.txt")
	require.NoError(t, err)

	// Non-stale rows should not be counted.
	_, err = db.SQL().Exec(`INSERT INTO files (storage_id, path, parent_dir, name, is_dir, mtime, mode, uid, gid, deleted)
VALUES (?, ?, ?, ?, 0, 0, 0, 0, 0, 0);`, "hdd1", "c.txt", "", "c.txt")
	require.NoError(t, err)
	_, err = db.SQL().Exec(`INSERT INTO files (storage_id, path, parent_dir, name, is_dir, mtime, mode, uid, gid, deleted)
VALUES (?, ?, ?, ?, 0, 0, 0, 0, 0, 2);`, "hdd2", "d.txt", "", "d.txt")
	require.NoError(t, err)

	count, err := QueryStaleCount("media", "hdd1")
	require.NoError(t, err)
	require.Equal(t, int64(2), count)
}
