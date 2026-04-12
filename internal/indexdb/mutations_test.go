package indexdb

import (
	"context"
	"database/sql"
	"errors"
	"path"
	"strings"
	"syscall"
	"testing"

	"github.com/hieutdo/policyfs/internal/config"
	"github.com/stretchr/testify/require"
)

// mustOpenTestDB creates a temp-backed indexdb.DB using the normal Open() migration path.
func mustOpenTestDB(t *testing.T) *DB {
	// This helper intentionally does not use t.Parallel() because it relies on env overrides.
	base := t.TempDir()
	t.Setenv(config.EnvStateDir, base)
	mountName := strings.ReplaceAll(strings.ToLower(t.Name()), "/", "_")

	db, err := Open(mountName)
	require.NoError(t, err)
	t.Cleanup(func() { _ = db.Close() })
	return db
}

// mustInsertEntry inserts a minimal files row for tests.
func mustInsertEntry(t *testing.T, db *DB, storageID string, p string, isDir bool, deleted int) {
	t.Helper()

	p = normalizeVirtualPath(p)
	parentDir, name := splitParentName(p)
	mode := uint32(syscall.S_IFREG | 0o644)
	size := any(int64(10))
	if isDir {
		mode = uint32(syscall.S_IFDIR | 0o755)
		size = nil
	}

	_, err := db.SQL().Exec(
		`INSERT INTO files (storage_id, path, real_path, parent_dir, name, is_dir, size, mtime, mode, uid, gid, deleted)
VALUES (?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?);`,
		storageID,
		p,
		p,
		parentDir,
		name,
		boolToInt(isDir),
		size,
		int64(1),
		mode,
		int64(0),
		int64(0),
		deleted,
	)
	require.NoError(t, err)
}

// boolToInt is a tiny test helper for sqlite bool columns.
func boolToInt(v bool) int {
	if v {
		return 1
	}
	return 0
}

// mustGetRow reads back key fields for a files row.
func mustGetRow(t *testing.T, db *DB, storageID string, p string) (path string, realPath string, parentDir string, name string, deleted int) {
	t.Helper()

	p = normalizeVirtualPath(p)
	err := db.SQL().QueryRow(
		`SELECT path, real_path, parent_dir, name, deleted FROM files WHERE storage_id = ? AND path = ?;`,
		storageID,
		p,
	).Scan(&path, &realPath, &parentDir, &name, &deleted)
	require.NoError(t, err)
	return path, realPath, parentDir, name, deleted
}

// TestMarkDeleted_shouldReturnFalseWhenMissing verifies MarkDeleted is a no-op for missing entries.
func TestMarkDeleted_shouldReturnFalseWhenMissing(t *testing.T) {
	db := mustOpenTestDB(t)
	ctx := context.Background()

	ok, err := db.MarkDeleted(ctx, "hdd1", "nope", false)
	require.NoError(t, err)
	require.False(t, ok)
}

// TestMarkDeleted_shouldFailForNonEmptyDir verifies MarkDeleted rejects directories with live children.
func TestMarkDeleted_shouldFailForNonEmptyDir(t *testing.T) {
	db := mustOpenTestDB(t)
	ctx := context.Background()

	mustInsertEntry(t, db, "hdd1", "d", true, 0)
	mustInsertEntry(t, db, "hdd1", "d/f", false, 0)

	ok, err := db.MarkDeleted(ctx, "hdd1", "d", true)
	require.False(t, ok)
	require.ErrorIs(t, err, syscall.ENOTEMPTY)
}

// TestMarkDeleted_shouldSucceedForEmptyDirWithOnlyTombstones verifies deleted children don't block rmdir semantics.
func TestMarkDeleted_shouldSucceedForEmptyDirWithOnlyTombstones(t *testing.T) {
	db := mustOpenTestDB(t)
	ctx := context.Background()

	mustInsertEntry(t, db, "hdd1", "d", true, 0)
	mustInsertEntry(t, db, "hdd1", "d/f", false, 1)
	mustInsertEntry(t, db, "hdd1", "d/g", false, 2)

	ok, err := db.MarkDeleted(ctx, "hdd1", "d", true)
	require.NoError(t, err)
	require.True(t, ok)

	_, _, _, _, deleted := mustGetRow(t, db, "hdd1", "d")
	require.Equal(t, 1, deleted)
	_, _, _, _, deleted = mustGetRow(t, db, "hdd1", "d/g")
	require.Equal(t, 1, deleted)
}

// TestMarkDeleted_shouldMarkFileDeleted verifies MarkDeleted sets deleted=1 for files.
func TestMarkDeleted_shouldMarkFileDeleted(t *testing.T) {
	db := mustOpenTestDB(t)
	ctx := context.Background()

	mustInsertEntry(t, db, "hdd1", "d", true, 0)
	mustInsertEntry(t, db, "hdd1", "d/f", false, 0)

	ok, err := db.MarkDeleted(ctx, "hdd1", "d/f", false)
	require.NoError(t, err)
	require.True(t, ok)

	_, _, _, _, deleted := mustGetRow(t, db, "hdd1", "d/f")
	require.Equal(t, 1, deleted)
}

// TestRenamePath_shouldReturnFalseWhenSourceMissing verifies RenamePath is a no-op for missing sources.
func TestRenamePath_shouldReturnFalseWhenSourceMissing(t *testing.T) {
	db := mustOpenTestDB(t)
	ctx := context.Background()

	mustInsertEntry(t, db, "hdd1", "b", true, 0)

	ok, err := db.RenamePath(ctx, "hdd1", "nope", "b/x")
	require.NoError(t, err)
	require.False(t, ok)
}

// TestRenamePath_shouldRenameFileAndKeepRealPath verifies file renames keep real_path pointing at the old physical path.
func TestRenamePath_shouldRenameFileAndKeepRealPath(t *testing.T) {
	db := mustOpenTestDB(t)
	ctx := context.Background()

	mustInsertEntry(t, db, "hdd1", "a", true, 0)
	mustInsertEntry(t, db, "hdd1", "b", true, 0)
	mustInsertEntry(t, db, "hdd1", "a/x", false, 0)

	// Add file_meta and ensure it follows the path update.
	_, err := db.SQL().Exec(`INSERT INTO file_meta (storage_id, path, meta_mode) VALUES (?, ?, ?);`, "hdd1", "a/x", int64(0o600))
	require.NoError(t, err)

	ok, err := db.RenamePath(ctx, "hdd1", "a/x", "b/y")
	require.NoError(t, err)
	require.True(t, ok)

	p, rp, parentDir, name, deleted := mustGetRow(t, db, "hdd1", "b/y")
	require.Equal(t, "b/y", p)
	require.Equal(t, "a/x", rp)
	require.Equal(t, "b", parentDir)
	require.Equal(t, "y", name)
	require.Equal(t, 0, deleted)

	var metaMode sql.NullInt64
	err = db.SQL().QueryRow(`SELECT meta_mode FROM file_meta WHERE storage_id = ? AND path = ?;`, "hdd1", "b/y").Scan(&metaMode)
	require.NoError(t, err)
	require.True(t, metaMode.Valid)
	require.Equal(t, int64(0o600), metaMode.Int64)
}

// TestRenamePath_shouldRenameDirSubtree verifies directory renames rewrite the subtree and keep real_path unchanged.
func TestRenamePath_shouldRenameDirSubtree(t *testing.T) {
	db := mustOpenTestDB(t)
	ctx := context.Background()

	mustInsertEntry(t, db, "hdd1", "a", true, 0)
	mustInsertEntry(t, db, "hdd1", "a/sub", true, 0)
	mustInsertEntry(t, db, "hdd1", "a/sub/f", false, 0)
	mustInsertEntry(t, db, "hdd1", "b", true, 0)

	ok, err := db.RenamePath(ctx, "hdd1", "a", "b/a2")
	require.NoError(t, err)
	require.True(t, ok)

	_, rp, _, _, _ := mustGetRow(t, db, "hdd1", "b/a2")
	require.Equal(t, "a", rp)
	_, rp, _, _, _ = mustGetRow(t, db, "hdd1", "b/a2/sub")
	require.Equal(t, "a/sub", rp)
	_, rp, _, _, _ = mustGetRow(t, db, "hdd1", "b/a2/sub/f")
	require.Equal(t, "a/sub/f", rp)
}

// TestRenamePath_overwriteSemantics verifies RenamePath overwrite behavior is POSIX-ish.
func TestRenamePath_overwriteSemantics(t *testing.T) {
	cases := []struct {
		name    string
		setup   func(t *testing.T, db *DB)
		oldPath string
		newPath string
		wantErr error
	}{
		{
			name: "file should overwrite existing file",
			setup: func(t *testing.T, db *DB) {
				mustInsertEntry(t, db, "hdd1", "a", true, 0)
				mustInsertEntry(t, db, "hdd1", "b", true, 0)
				mustInsertEntry(t, db, "hdd1", "a/src", false, 0)
				mustInsertEntry(t, db, "hdd1", "b/dst", false, 0)
			},
			oldPath: "a/src",
			newPath: "b/dst",
			wantErr: nil,
		},
		{
			name: "dir should overwrite empty dir",
			setup: func(t *testing.T, db *DB) {
				mustInsertEntry(t, db, "hdd1", "a", true, 0)
				mustInsertEntry(t, db, "hdd1", "b", true, 0)
				mustInsertEntry(t, db, "hdd1", "a/src", true, 0)
				mustInsertEntry(t, db, "hdd1", "b/dst", true, 0)
			},
			oldPath: "a/src",
			newPath: "b/dst",
			wantErr: nil,
		},
		{
			name: "dir should not overwrite non-empty dir",
			setup: func(t *testing.T, db *DB) {
				mustInsertEntry(t, db, "hdd1", "a", true, 0)
				mustInsertEntry(t, db, "hdd1", "b", true, 0)
				mustInsertEntry(t, db, "hdd1", "a/src", true, 0)
				mustInsertEntry(t, db, "hdd1", "b/dst", true, 0)
				mustInsertEntry(t, db, "hdd1", path.Join("b/dst", "child"), false, 0)
			},
			oldPath: "a/src",
			newPath: "b/dst",
			wantErr: syscall.ENOTEMPTY,
		},
	}

	for _, tc := range cases {
		t.Run(tc.name, func(t *testing.T) {
			db := mustOpenTestDB(t)
			ctx := context.Background()
			tc.setup(t, db)

			ok, err := db.RenamePath(ctx, "hdd1", tc.oldPath, tc.newPath)
			if tc.wantErr == nil {
				require.NoError(t, err)
				require.True(t, ok)
				_, rp, _, _, _ := mustGetRow(t, db, "hdd1", tc.newPath)
				require.Equal(t, normalizeVirtualPath(tc.oldPath), rp)
				return
			}
			require.False(t, ok)
			require.ErrorIs(t, err, tc.wantErr)
		})
	}
}

// TestUpsertMeta_shouldMergeFields verifies UpsertMeta merges fields rather than overwriting with NULL.
func TestUpsertMeta_shouldMergeFields(t *testing.T) {
	db := mustOpenTestDB(t)
	ctx := context.Background()

	mustInsertEntry(t, db, "hdd1", "a", true, 0)
	mustInsertEntry(t, db, "hdd1", "a/f", false, 0)

	m := uint32(0o600)
	ok, err := db.UpsertMeta(ctx, "hdd1", "a/f", &m, nil, nil, nil)
	require.NoError(t, err)
	require.True(t, ok)

	uid := uint32(123)
	ok, err = db.UpsertMeta(ctx, "hdd1", "a/f", nil, &uid, nil, nil)
	require.NoError(t, err)
	require.True(t, ok)

	var metaMode sql.NullInt64
	var metaUID sql.NullInt64
	err = db.SQL().QueryRow(`SELECT meta_mode, meta_uid FROM file_meta WHERE storage_id = ? AND path = ?;`, "hdd1", "a/f").Scan(&metaMode, &metaUID)
	require.NoError(t, err)
	require.True(t, metaMode.Valid)
	require.Equal(t, int64(0o600), metaMode.Int64)
	require.True(t, metaUID.Valid)
	require.Equal(t, int64(123), metaUID.Int64)
}

// TestFinalizeSetattr_shouldRemoveFileMeta verifies FinalizeSetattr clears file_meta rows.
func TestFinalizeSetattr_shouldRemoveFileMeta(t *testing.T) {
	db := mustOpenTestDB(t)
	ctx := context.Background()

	mustInsertEntry(t, db, "hdd1", "a", true, 0)
	mustInsertEntry(t, db, "hdd1", "a/f", false, 0)

	m := uint32(0o600)
	ok, err := db.UpsertMeta(ctx, "hdd1", "a/f", &m, nil, nil, nil)
	require.NoError(t, err)
	require.True(t, ok)

	require.NoError(t, db.FinalizeSetattr(ctx, "hdd1", "a/f"))

	var one int
	err = db.SQL().QueryRow(`SELECT 1 FROM file_meta WHERE storage_id = ? AND path = ?;`, "hdd1", "a/f").Scan(&one)
	require.True(t, errors.Is(err, sql.ErrNoRows))
}

// TestFinalizeDelete_shouldRemoveDeletedRows verifies FinalizeDelete deletes rows that were marked deleted=1.
func TestFinalizeDelete_shouldRemoveDeletedRows(t *testing.T) {
	db := mustOpenTestDB(t)
	ctx := context.Background()

	mustInsertEntry(t, db, "hdd1", "a", true, 0)
	mustInsertEntry(t, db, "hdd1", "a/f", false, 1)

	require.NoError(t, db.FinalizeDelete(ctx, "hdd1", "a/f", false))

	var one int
	err := db.SQL().QueryRow(`SELECT 1 FROM files WHERE storage_id = ? AND path = ?;`, "hdd1", "a/f").Scan(&one)
	require.True(t, errors.Is(err, sql.ErrNoRows))
}

// TestFinalizeRename_shouldRewriteRealPath verifies FinalizeRename rewrites pending real_path prefixes after physical rename.
func TestFinalizeRename_shouldRewriteRealPath(t *testing.T) {
	db := mustOpenTestDB(t)
	ctx := context.Background()

	mustInsertEntry(t, db, "hdd1", "a", true, 0)
	mustInsertEntry(t, db, "hdd1", "b", true, 0)
	mustInsertEntry(t, db, "hdd1", "a/x", false, 0)

	ok, err := db.RenamePath(ctx, "hdd1", "a/x", "b/y")
	require.NoError(t, err)
	require.True(t, ok)

	require.NoError(t, db.FinalizeRename(ctx, "hdd1", "a/x", "b/y"))

	_, rp, _, _, _ := mustGetRow(t, db, "hdd1", "b/y")
	require.Equal(t, "b/y", rp)
}

// TestFinalizeRename_shouldFixPathWhenRenamePathNotVisible verifies FinalizeRename correctly
// updates path/parent_dir/name even when a prior RenamePath commit was not visible (e.g. cross-process
// WAL visibility lag). This simulates calling FinalizeRename directly without RenamePath.
func TestFinalizeRename_shouldFixPathWhenRenamePathNotVisible(t *testing.T) {
	db := mustOpenTestDB(t)
	ctx := context.Background()

	mustInsertEntry(t, db, "hdd1", "a", true, 0)
	mustInsertEntry(t, db, "hdd1", "b", true, 0)
	mustInsertEntry(t, db, "hdd1", "a/x", false, 0)

	// Skip RenamePath to simulate the case where its commit is not visible.
	require.NoError(t, db.FinalizeRename(ctx, "hdd1", "a/x", "b/y"))

	p, rp, pd, n, _ := mustGetRow(t, db, "hdd1", "b/y")
	require.Equal(t, "b/y", p)
	require.Equal(t, "b/y", rp)
	require.Equal(t, "b", pd)
	require.Equal(t, "y", n)

	// Old path should no longer exist.
	var one int
	err := db.SQL().QueryRow(`SELECT 1 FROM files WHERE storage_id = ? AND path = ?;`, "hdd1", "a/x").Scan(&one)
	require.ErrorIs(t, err, sql.ErrNoRows)
}

// TestFinalizeRename_shouldFixDirSubtreeWhenRenamePathNotVisible verifies FinalizeRename correctly
// updates the subtree when a directory rename's RenamePath commit was not visible.
func TestFinalizeRename_shouldFixDirSubtreeWhenRenamePathNotVisible(t *testing.T) {
	db := mustOpenTestDB(t)
	ctx := context.Background()

	mustInsertEntry(t, db, "hdd1", "old", true, 0)
	mustInsertEntry(t, db, "hdd1", "old/child.txt", false, 0)

	// Skip RenamePath.
	require.NoError(t, db.FinalizeRename(ctx, "hdd1", "old", "new"))

	p, rp, pd, n, _ := mustGetRow(t, db, "hdd1", "new")
	require.Equal(t, "new", p)
	require.Equal(t, "new", rp)
	require.Equal(t, "", pd)
	require.Equal(t, "new", n)

	cp, crp, cpd, cn, _ := mustGetRow(t, db, "hdd1", "new/child.txt")
	require.Equal(t, "new/child.txt", cp)
	require.Equal(t, "new/child.txt", crp)
	require.Equal(t, "new", cpd)
	require.Equal(t, "child.txt", cn)
}

// TestFinalizeRename_shouldOverwriteTargetWhenRenamePathNotVisible verifies FinalizeRename
// deletes the overwrite target when RenamePath's commit was not visible, avoiding UNIQUE constraint violations.
func TestFinalizeRename_shouldOverwriteTargetWhenRenamePathNotVisible(t *testing.T) {
	db := mustOpenTestDB(t)
	ctx := context.Background()

	mustInsertEntry(t, db, "hdd1", "a", true, 0)
	mustInsertEntry(t, db, "hdd1", "b", true, 0)
	mustInsertEntry(t, db, "hdd1", "a/src.txt", false, 0)
	mustInsertEntry(t, db, "hdd1", "b/dst.txt", false, 0)

	// Skip RenamePath - both source and destination rows exist with real_path = path.
	require.NoError(t, db.FinalizeRename(ctx, "hdd1", "a/src.txt", "b/dst.txt"))

	p, rp, pd, n, _ := mustGetRow(t, db, "hdd1", "b/dst.txt")
	require.Equal(t, "b/dst.txt", p)
	require.Equal(t, "b/dst.txt", rp)
	require.Equal(t, "b", pd)
	require.Equal(t, "dst.txt", n)

	// Old path should no longer exist.
	var one int
	err := db.SQL().QueryRow(`SELECT 1 FROM files WHERE storage_id = ? AND path = ?;`, "hdd1", "a/src.txt").Scan(&one)
	require.ErrorIs(t, err, sql.ErrNoRows)
}

// TestUpsertFile_shouldCreateDirChain verifies UpsertFile creates parent directory rows so the file is visible.
func TestUpsertFile_shouldCreateDirChain(t *testing.T) {
	db := mustOpenTestDB(t)
	ctx := context.Background()

	sz := int64(123)
	require.NoError(t, db.UpsertFile(ctx, "hdd1", "a/b/c.txt", false, &sz, 10, uint32(syscall.S_IFREG|0o644), 0, 0))

	// Parents should exist.
	for _, p := range []string{"a", "a/b"} {
		var one int
		err := db.SQL().QueryRow(`SELECT 1 FROM files WHERE storage_id = ? AND path = ? AND is_dir = 1 AND deleted = 0;`, "hdd1", p).Scan(&one)
		require.NoError(t, err)
	}

	// File row should exist.
	var one int
	err := db.SQL().QueryRow(`SELECT 1 FROM files WHERE storage_id = ? AND path = ? AND is_dir = 0 AND deleted = 0;`, "hdd1", "a/b/c.txt").Scan(&one)
	require.NoError(t, err)
}

// TestUpsertFile_shouldReturnBusyForDeletedTombstone verifies UpsertFile does not resurrect deleted=1.
func TestUpsertFile_shouldReturnBusyForDeletedTombstone(t *testing.T) {
	db := mustOpenTestDB(t)
	ctx := context.Background()

	mustInsertEntry(t, db, "hdd1", "a", true, 0)
	mustInsertEntry(t, db, "hdd1", "a/f.txt", false, 1)

	sz := int64(1)
	err := db.UpsertFile(ctx, "hdd1", "a/f.txt", false, &sz, 10, uint32(syscall.S_IFREG|0o644), 0, 0)
	require.ErrorIs(t, err, syscall.EBUSY)
}

// TestUpsertFile_shouldPreserveLastSeenRunID verifies UpsertFile keeps last_seen_run_id for existing rows.
func TestUpsertFile_shouldPreserveLastSeenRunID(t *testing.T) {
	db := mustOpenTestDB(t)
	ctx := context.Background()

	mustInsertEntry(t, db, "hdd1", "a", true, 0)
	mustInsertEntry(t, db, "hdd1", "a/f.txt", false, 0)

	// Set an initial last_seen_run_id.
	_, err := db.SQL().Exec(`UPDATE files SET last_seen_run_id = 7 WHERE storage_id = ? AND path = ?;`, "hdd1", "a/f.txt")
	require.NoError(t, err)

	sz := int64(99)
	require.NoError(t, db.UpsertFile(ctx, "hdd1", "a/f.txt", false, &sz, 11, uint32(syscall.S_IFREG|0o600), 1, 2))

	var runID sql.NullInt64
	err = db.SQL().QueryRow(`SELECT last_seen_run_id FROM files WHERE storage_id = ? AND path = ?;`, "hdd1", "a/f.txt").Scan(&runID)
	require.NoError(t, err)
	require.True(t, runID.Valid)
	require.Equal(t, int64(7), runID.Int64)
}
