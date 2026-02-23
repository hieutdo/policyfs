package indexdb

import (
	"testing"

	"github.com/stretchr/testify/require"

	"github.com/hieutdo/policyfs/internal/config"
)

// --- QueryFileInspect ---

// TestQueryFileInspect_DBMissing_shouldReturnNil verifies QueryFileInspect returns nil when the index DB does not exist.
func TestQueryFileInspect_DBMissing_shouldReturnNil(t *testing.T) {
	base := t.TempDir()
	t.Setenv(config.EnvStateDir, base)

	rows, err := QueryFileInspect("media", "library/test.txt")
	require.NoError(t, err)
	require.Nil(t, rows)
}

// TestQueryFileInspect_NoMatch_shouldReturnEmpty verifies QueryFileInspect returns an empty slice when no rows match.
func TestQueryFileInspect_NoMatch_shouldReturnEmpty(t *testing.T) {
	base := t.TempDir()
	t.Setenv(config.EnvStateDir, base)

	db, err := Open("media")
	require.NoError(t, err)

	// Insert an unrelated file.
	_, err = db.SQL().Exec(`INSERT INTO files (storage_id, path, parent_dir, name, is_dir, mtime, mode, uid, gid, deleted)
VALUES ('hdd1', 'other.txt', '', 'other.txt', 0, 1000, 420, 1000, 1000, 0);`)
	require.NoError(t, err)
	require.NoError(t, db.Close())

	rows, err := QueryFileInspect("media", "library/test.txt")
	require.NoError(t, err)
	require.Empty(t, rows)
}

// TestQueryFileInspect_SingleStorage_shouldReturnRow verifies a single matching row is returned with all fields.
func TestQueryFileInspect_SingleStorage_shouldReturnRow(t *testing.T) {
	base := t.TempDir()
	t.Setenv(config.EnvStateDir, base)

	db, err := Open("media")
	require.NoError(t, err)

	_, err = db.SQL().Exec(`INSERT INTO files (storage_id, path, real_path, parent_dir, name, is_dir, size, mtime, mode, uid, gid, deleted, last_seen_run_id)
VALUES ('hdd1', 'library/test.txt', 'library/test.txt', 'library', 'test.txt', 0, 1024, 1700000000, 420, 1000, 1000, 0, 5);`)
	require.NoError(t, err)

	_, err = db.SQL().Exec(`INSERT INTO indexer_state (storage_id, current_run_id, last_completed) VALUES ('hdd1', 5, 1700001000);`)
	require.NoError(t, err)
	require.NoError(t, db.Close())

	rows, err := QueryFileInspect("media", "library/test.txt")
	require.NoError(t, err)
	require.Len(t, rows, 1)

	r := rows[0]
	require.Equal(t, "hdd1", r.StorageID)
	require.Equal(t, "library/test.txt", r.Path)
	require.Equal(t, "library/test.txt", r.RealPath)
	require.False(t, r.IsDir)
	require.NotNil(t, r.Size)
	require.Equal(t, int64(1024), *r.Size)
	require.Equal(t, int64(1700000000), r.MTimeSec)
	require.Equal(t, uint32(420), r.Mode)
	require.Equal(t, uint32(1000), r.UID)
	require.Equal(t, uint32(1000), r.GID)
	require.Equal(t, 0, r.Deleted)
	require.NotNil(t, r.LastSeenRunID)
	require.Equal(t, int64(5), *r.LastSeenRunID)
	require.Equal(t, int64(5), r.CurrentRunID)
	require.NotNil(t, r.LastCompleted)
	require.Equal(t, int64(1700001000), *r.LastCompleted)
}

// TestQueryFileInspect_MultipleStorages_shouldReturnBoth verifies rows from multiple storages are returned.
func TestQueryFileInspect_MultipleStorages_shouldReturnBoth(t *testing.T) {
	base := t.TempDir()
	t.Setenv(config.EnvStateDir, base)

	db, err := Open("media")
	require.NoError(t, err)

	_, err = db.SQL().Exec(`INSERT INTO files (storage_id, path, real_path, parent_dir, name, is_dir, mtime, mode, uid, gid, deleted)
VALUES ('hdd1', 'test.txt', 'test.txt', '', 'test.txt', 0, 1000, 420, 0, 0, 0);`)
	require.NoError(t, err)
	_, err = db.SQL().Exec(`INSERT INTO files (storage_id, path, real_path, parent_dir, name, is_dir, mtime, mode, uid, gid, deleted)
VALUES ('hdd2', 'test.txt', 'test.txt', '', 'test.txt', 0, 2000, 420, 0, 0, 0);`)
	require.NoError(t, err)
	require.NoError(t, db.Close())

	rows, err := QueryFileInspect("media", "test.txt")
	require.NoError(t, err)
	require.Len(t, rows, 2)
	require.Equal(t, "hdd1", rows[0].StorageID)
	require.Equal(t, "hdd2", rows[1].StorageID)
}

// TestQueryFileInspect_IncludesDeleted verifies deleted rows are included in results.
func TestQueryFileInspect_IncludesDeleted(t *testing.T) {
	base := t.TempDir()
	t.Setenv(config.EnvStateDir, base)

	db, err := Open("media")
	require.NoError(t, err)

	_, err = db.SQL().Exec(`INSERT INTO files (storage_id, path, real_path, parent_dir, name, is_dir, mtime, mode, uid, gid, deleted)
VALUES ('hdd1', 'deleted.txt', 'deleted.txt', '', 'deleted.txt', 0, 1000, 420, 0, 0, 1);`)
	require.NoError(t, err)
	_, err = db.SQL().Exec(`INSERT INTO files (storage_id, path, real_path, parent_dir, name, is_dir, mtime, mode, uid, gid, deleted)
VALUES ('hdd2', 'deleted.txt', 'deleted.txt', '', 'deleted.txt', 0, 1000, 420, 0, 0, 2);`)
	require.NoError(t, err)
	require.NoError(t, db.Close())

	rows, err := QueryFileInspect("media", "deleted.txt")
	require.NoError(t, err)
	require.Len(t, rows, 2)
	require.Equal(t, 1, rows[0].Deleted) // pending delete
	require.Equal(t, 2, rows[1].Deleted) // stale
}

// TestQueryFileInspect_WithMetaOverrides verifies file_meta overrides are returned.
func TestQueryFileInspect_WithMetaOverrides(t *testing.T) {
	base := t.TempDir()
	t.Setenv(config.EnvStateDir, base)

	db, err := Open("media")
	require.NoError(t, err)

	_, err = db.SQL().Exec(`INSERT INTO files (storage_id, path, real_path, parent_dir, name, is_dir, mtime, mode, uid, gid, deleted)
VALUES ('hdd1', 'meta.txt', 'meta.txt', '', 'meta.txt', 0, 1000, 420, 1000, 1000, 0);`)
	require.NoError(t, err)
	_, err = db.SQL().Exec(`INSERT INTO file_meta (storage_id, path, meta_mode, meta_uid)
VALUES ('hdd1', 'meta.txt', 493, 0);`)
	require.NoError(t, err)
	require.NoError(t, db.Close())

	rows, err := QueryFileInspect("media", "meta.txt")
	require.NoError(t, err)
	require.Len(t, rows, 1)

	r := rows[0]
	require.NotNil(t, r.MetaMode)
	require.Equal(t, uint32(493), *r.MetaMode) // 0o755
	require.NotNil(t, r.MetaUID)
	require.Equal(t, uint32(0), *r.MetaUID) // root
	require.Nil(t, r.MetaMTime)             // no mtime override
	require.Nil(t, r.MetaGID)               // no gid override
}

// --- QueryVirtualChildren ---

// TestQueryVirtualChildren_DBMissing_shouldReturnNil verifies QueryVirtualChildren returns nil when the index DB does not exist.
func TestQueryVirtualChildren_DBMissing_shouldReturnNil(t *testing.T) {
	base := t.TempDir()
	t.Setenv(config.EnvStateDir, base)

	rows, err := QueryVirtualChildren("media", "", "", 100)
	require.NoError(t, err)
	require.Nil(t, rows)
}

// TestQueryVirtualChildren_Root_shouldListDistinctChildren verifies root dir completion returns distinct child names.
func TestQueryVirtualChildren_Root_shouldListDistinctChildren(t *testing.T) {
	base := t.TempDir()
	t.Setenv(config.EnvStateDir, base)

	db, err := Open("media")
	require.NoError(t, err)

	// Two storages share the same child name; completion should dedupe.
	_, err = db.SQL().Exec(`INSERT INTO files (storage_id, path, real_path, parent_dir, name, is_dir, mtime, mode, uid, gid, deleted)
VALUES ('hdd1', 'library', 'library', '', 'library', 1, 1000, 493, 0, 0, 0);`)
	require.NoError(t, err)
	_, err = db.SQL().Exec(`INSERT INTO files (storage_id, path, real_path, parent_dir, name, is_dir, mtime, mode, uid, gid, deleted)
VALUES ('hdd2', 'library', 'library', '', 'library', 1, 1000, 493, 0, 0, 0);`)
	require.NoError(t, err)
	_, err = db.SQL().Exec(`INSERT INTO files (storage_id, path, real_path, parent_dir, name, is_dir, mtime, mode, uid, gid, deleted)
VALUES ('hdd1', 'other', 'other', '', 'other', 1, 1000, 493, 0, 0, 0);`)
	require.NoError(t, err)
	require.NoError(t, db.Close())

	rows, err := QueryVirtualChildren("media", "", "lib", 100)
	require.NoError(t, err)
	require.Len(t, rows, 1)
	require.Equal(t, "library", rows[0].Name)
	require.True(t, rows[0].IsDir)
}

// TestQueryVirtualChildren_Subdir_shouldListChildren verifies completion under a directory.
func TestQueryVirtualChildren_Subdir_shouldListChildren(t *testing.T) {
	base := t.TempDir()
	t.Setenv(config.EnvStateDir, base)

	db, err := Open("media")
	require.NoError(t, err)

	_, err = db.SQL().Exec(`INSERT INTO files (storage_id, path, real_path, parent_dir, name, is_dir, mtime, mode, uid, gid, deleted)
VALUES ('hdd1', 'library', 'library', '', 'library', 1, 1000, 493, 0, 0, 0);`)
	require.NoError(t, err)
	_, err = db.SQL().Exec(`INSERT INTO files (storage_id, path, real_path, parent_dir, name, is_dir, mtime, mode, uid, gid, deleted)
VALUES ('hdd1', 'library/test.txt', 'library/test.txt', 'library', 'test.txt', 0, 1000, 420, 0, 0, 0);`)
	require.NoError(t, err)
	_, err = db.SQL().Exec(`INSERT INTO files (storage_id, path, real_path, parent_dir, name, is_dir, mtime, mode, uid, gid, deleted)
VALUES ('hdd1', 'library/tool', 'library/tool', 'library', 'tool', 1, 1000, 493, 0, 0, 0);`)
	require.NoError(t, err)
	require.NoError(t, db.Close())

	rows, err := QueryVirtualChildren("media", "library", "t", 100)
	require.NoError(t, err)
	require.Len(t, rows, 2)
	require.Equal(t, "test.txt", rows[0].Name)
	require.False(t, rows[0].IsDir)
	require.Equal(t, "tool", rows[1].Name)
	require.True(t, rows[1].IsDir)
}

// --- QueryStaleCount ---

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
