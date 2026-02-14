//go:build integration

package integration

import (
	"context"
	"os"
	"testing"
	"time"

	"github.com/hieutdo/policyfs/internal/config"
	_ "github.com/mattn/go-sqlite3"
	"github.com/stretchr/testify/require"
)

// TestIndexed_readdir_shouldHideDeletedEntries verifies that after deleting an indexed file,
// readdir on the parent directory does not include the deleted entry.
func TestIndexed_readdir_shouldHideDeletedEntries(t *testing.T) {
	withMountedFS(t, createIndexedCfg(), func(env *MountedFS) {
		dir := "indexed-ops/readdir-tombstone"
		env.MustCreateFileInStoragePath(t, []byte("keep"), "hdd1", dir+"/keep.txt")
		env.MustCreateFileInStoragePath(t, []byte("gone"), "hdd1", dir+"/gone.txt")

		mustRunPFS(t, env, "index", env.MountName)

		// Verify both files visible before delete.
		entries := env.MustReadDirInMountPoint(t, dir)
		names := dirEntryNames(entries)
		require.Contains(t, names, "keep.txt")
		require.Contains(t, names, "gone.txt")

		// Deferred delete.
		env.MustRemoveFileInMountPoint(t, dir+"/gone.txt")

		// Readdir should no longer include gone.txt.
		entries = env.MustReadDirInMountPoint(t, dir)
		names = dirEntryNames(entries)
		require.Contains(t, names, "keep.txt")
		require.NotContains(t, names, "gone.txt")

		// File is still on disk (deferred).
		require.FileExists(t, env.StoragePath("hdd1", dir+"/gone.txt"))
	})
}

// TestReaddir_root_shouldNotListEntriesNotReadableByRoutingRules verifies that directory listings
// do not include entries that cannot be resolved by read routing rules.
func TestReaddir_root_shouldNotListEntriesNotReadableByRoutingRules(t *testing.T) {
	cfg := IntegrationConfig{
		Storages: []IntegrationStorage{
			{ID: "ssd2", Indexed: false, BasePath: "/mnt/ssd2/pfs-integration"},
			{ID: "hdd1", Indexed: true, BasePath: "/mnt/hdd1/pfs-integration"},
		},
		RoutingRules: []config.RoutingRule{
			{Match: "library/**", ReadTargets: []string{"hdd1"}, WriteTargets: []string{"ssd2"}},
			{Match: "**", ReadTargets: []string{"ssd2"}, WriteTargets: []string{"ssd2"}},
		},
	}

	withMountedFS(t, cfg, func(env *MountedFS) {
		rel := "text1.txt"
		env.MustCreateFileInStoragePath(t, []byte("hello"), "hdd1", rel)
		mustRunPFS(t, env, "index", env.MountName)

		entries := env.MustReadDirInMountPoint(t, "")
		names := dirEntryNames(entries)
		require.NotContains(t, names, rel)

		_, err := os.Lstat(env.MountPath(rel))
		require.Error(t, err)
		require.ErrorIs(t, err, os.ErrNotExist)

		require.FileExists(t, env.StoragePath("hdd1", rel))
	})
}

// TestIndexed_readdir_shouldShowRenamedEntryAtNewLocation verifies that after renaming an indexed
// file, readdir shows the new name in the destination parent and hides it from the source parent.
func TestIndexed_readdir_shouldShowRenamedEntryAtNewLocation(t *testing.T) {
	withMountedFS(t, createIndexedCfg(), func(env *MountedFS) {
		srcDir := "indexed-ops/readdir-rename/src"
		dstDir := "indexed-ops/readdir-rename/dst"
		env.MustCreateFileInStoragePath(t, []byte("data"), "hdd1", srcDir+"/moved.txt")
		env.MustCreateDirInStoragePath(t, "hdd1", dstDir)

		mustRunPFS(t, env, "index", env.MountName)

		// Verify file visible in source dir.
		entries := env.MustReadDirInMountPoint(t, srcDir)
		require.Contains(t, dirEntryNames(entries), "moved.txt")

		// Deferred rename.
		env.MustRenameFileInMountPoint(t, srcDir+"/moved.txt", dstDir+"/moved.txt")

		// Source dir should no longer list moved.txt.
		entries = env.MustReadDirInMountPoint(t, srcDir)
		require.NotContains(t, dirEntryNames(entries), "moved.txt")

		// Destination dir should list moved.txt.
		entries = env.MustReadDirInMountPoint(t, dstDir)
		require.Contains(t, dirEntryNames(entries), "moved.txt")
	})
}

// TestIndexed_stat_shouldReturnENOENTForDeletedFile verifies that stat on a deleted indexed file
// returns ENOENT even though the physical file still exists.
func TestIndexed_stat_shouldReturnENOENTForDeletedFile(t *testing.T) {
	withMountedFS(t, createIndexedCfg(), func(env *MountedFS) {
		rel := "indexed-ops/stat-deleted/a.txt"
		env.MustCreateFileInStoragePath(t, []byte("data"), "hdd1", rel)

		mustRunPFS(t, env, "index", env.MountName)

		// Confirm stat works before delete.
		_, err := os.Stat(env.MountPath(rel))
		require.NoError(t, err)

		// Deferred delete.
		env.MustRemoveFileInMountPoint(t, rel)

		// Stat through mount should return ENOENT.
		_, err = os.Stat(env.MountPath(rel))
		require.ErrorIs(t, err, os.ErrNotExist)

		// Physical file still exists.
		require.FileExists(t, env.StoragePath("hdd1", rel))
	})
}

// TestIndexed_rename_chain_shouldTrackMultipleRenames verifies that chaining renames (A→B→C)
// correctly tracks real_path and prune applies the final physical rename.
func TestIndexed_rename_chain_shouldTrackMultipleRenames(t *testing.T) {
	withMountedFS(t, createIndexedCfg(), func(env *MountedFS) {
		dir := "indexed-ops/rename-chain"
		pathA := dir + "/a.txt"
		pathB := dir + "/b.txt"
		pathC := dir + "/c.txt"
		content := []byte("rename-chain-content")
		env.MustCreateFileInStoragePath(t, content, "hdd1", pathA)

		mustRunPFS(t, env, "index", env.MountName)

		// First rename: A→B.
		env.MustRenameFileInMountPoint(t, pathA, pathB)
		got := env.MustReadFileInMountPoint(t, pathB)
		require.Equal(t, content, got)
		require.False(t, env.FileExistsInMountPoint(pathA))

		// Second rename: B→C.
		env.MustRenameFileInMountPoint(t, pathB, pathC)
		got = env.MustReadFileInMountPoint(t, pathC)
		require.Equal(t, content, got)
		require.False(t, env.FileExistsInMountPoint(pathB))

		// Physical file still at original location.
		require.FileExists(t, env.StoragePath("hdd1", pathA))
		require.NoFileExists(t, env.StoragePath("hdd1", pathB))
		require.NoFileExists(t, env.StoragePath("hdd1", pathC))

		waitForEventsFile(t, env, 2*time.Second)

		// Prune should apply both renames: physical file ends up at C.
		mustRunPFS(t, env, "prune", env.MountName)

		require.NoFileExists(t, env.StoragePath("hdd1", pathA))
		require.NoFileExists(t, env.StoragePath("hdd1", pathB))
		require.FileExists(t, env.StoragePath("hdd1", pathC))
		got = env.MustReadFileInMountPoint(t, pathC)
		require.Equal(t, content, got)

		// DB should show real_path = path (finalized).
		db := openIndexDB(t, env)
		ctx, cancel := context.WithTimeout(context.Background(), 2*time.Second)
		defer cancel()

		var gotPath, gotRealPath string
		err := db.QueryRowContext(ctx,
			`SELECT path, real_path FROM files WHERE storage_id = ? AND path = ? AND deleted = 0 LIMIT 1;`,
			"hdd1", pathC).Scan(&gotPath, &gotRealPath)
		require.NoError(t, err)
		require.Equal(t, pathC, gotPath)
		require.Equal(t, gotPath, gotRealPath)
	})
}

// TestIndexed_delete_after_rename_shouldApplyBothOps verifies that renaming an indexed file and
// then deleting it produces correct mount view and prune applies both operations.
func TestIndexed_delete_after_rename_shouldApplyBothOps(t *testing.T) {
	withMountedFS(t, createIndexedCfg(), func(env *MountedFS) {
		dir := "indexed-ops/delete-after-rename"
		oldRel := dir + "/src.txt"
		newRel := dir + "/dst.txt"
		content := []byte("delete-after-rename")
		env.MustCreateFileInStoragePath(t, content, "hdd1", oldRel)

		mustRunPFS(t, env, "index", env.MountName)

		// Rename then delete.
		env.MustRenameFileInMountPoint(t, oldRel, newRel)
		require.True(t, env.FileExistsInMountPoint(newRel))
		env.MustRemoveFileInMountPoint(t, newRel)

		// Mount should not show either path.
		require.False(t, env.FileExistsInMountPoint(oldRel))
		require.False(t, env.FileExistsInMountPoint(newRel))

		// Physical file still at original location (deferred).
		require.FileExists(t, env.StoragePath("hdd1", oldRel))

		// Prune should handle both: rename then delete the physical file.
		mustRunPFS(t, env, "prune", env.MountName)

		require.NoFileExists(t, env.StoragePath("hdd1", oldRel))
		require.NoFileExists(t, env.StoragePath("hdd1", newRel))
	})
}

// TestIndexed_rename_dir_readdir_shouldShowSubtreeAtNewLocation verifies that renaming an indexed
// directory makes readdir show the subtree under the new parent.
func TestIndexed_rename_dir_readdir_shouldShowSubtreeAtNewLocation(t *testing.T) {
	withMountedFS(t, createIndexedCfg(), func(env *MountedFS) {
		oldDir := "indexed-ops/rename-dir/old"
		newDir := "indexed-ops/rename-dir/new"
		env.MustCreateFileInStoragePath(t, []byte("child1"), "hdd1", oldDir+"/c1.txt")
		env.MustCreateFileInStoragePath(t, []byte("child2"), "hdd1", oldDir+"/c2.txt")

		mustRunPFS(t, env, "index", env.MountName)

		// Rename directory.
		env.MustRenameFileInMountPoint(t, oldDir, newDir)

		// New dir should contain both children.
		entries := env.MustReadDirInMountPoint(t, newDir)
		names := dirEntryNames(entries)
		require.Contains(t, names, "c1.txt")
		require.Contains(t, names, "c2.txt")

		// Old dir should not exist.
		require.False(t, env.FileExistsInMountPoint(oldDir))

		// Children should be readable at new paths.
		got := env.MustReadFileInMountPoint(t, newDir+"/c1.txt")
		require.Equal(t, []byte("child1"), got)
		got = env.MustReadFileInMountPoint(t, newDir+"/c2.txt")
		require.Equal(t, []byte("child2"), got)

		waitForEventsFile(t, env, 2*time.Second)

		// Prune should finalize everything.
		mustRunPFS(t, env, "prune", env.MountName)

		require.NoDirExists(t, env.StoragePath("hdd1", oldDir))
		require.DirExists(t, env.StoragePath("hdd1", newDir))
		require.FileExists(t, env.StoragePath("hdd1", newDir+"/c1.txt"))
		require.FileExists(t, env.StoragePath("hdd1", newDir+"/c2.txt"))

		// Verify DB is fully finalized.
		db := openIndexDB(t, env)
		ctx, cancel := context.WithTimeout(context.Background(), 2*time.Second)
		defer cancel()

		rows, err := db.QueryContext(ctx,
			`SELECT path, real_path FROM files WHERE storage_id = ? AND path LIKE ? AND deleted = 0;`,
			"hdd1", "indexed-ops/rename-dir/new%")
		require.NoError(t, err)
		defer func() { _ = rows.Close() }()

		for rows.Next() {
			var p, rp string
			require.NoError(t, rows.Scan(&p, &rp))
			require.Equal(t, p, rp, "real_path should be finalized for %s", p)
		}
		require.NoError(t, rows.Err())
	})
}

// TestIndexed_reindex_shouldPreserveDeferredState verifies that re-indexing after deferred
// delete and rename preserves the deferred state (does not resurrect deleted files or clobber
// pending renames).
func TestIndexed_reindex_shouldPreserveDeferredState(t *testing.T) {
	withMountedFS(t, createIndexedCfg(), func(env *MountedFS) {
		dir := "indexed-ops/reindex-preserve"
		delFile := dir + "/deleted.txt"
		oldName := dir + "/old.txt"
		newName := dir + "/new.txt"
		keepFile := dir + "/keep.txt"
		env.MustCreateFileInStoragePath(t, []byte("del"), "hdd1", delFile)
		env.MustCreateFileInStoragePath(t, []byte("rename-me"), "hdd1", oldName)
		env.MustCreateFileInStoragePath(t, []byte("stay"), "hdd1", keepFile)

		mustRunPFS(t, env, "index", env.MountName)

		// Deferred ops.
		env.MustRemoveFileInMountPoint(t, delFile)
		env.MustRenameFileInMountPoint(t, oldName, newName)

		// Re-index.
		mustRunPFS(t, env, "index", env.MountName)

		// Mount view should still reflect deferred state.
		require.False(t, env.FileExistsInMountPoint(delFile), "deleted file should stay hidden after re-index")
		require.False(t, env.FileExistsInMountPoint(oldName), "old name should stay hidden after re-index")
		require.True(t, env.FileExistsInMountPoint(newName), "renamed file should remain at new path")
		require.True(t, env.FileExistsInMountPoint(keepFile), "untouched file should remain visible")

		got := env.MustReadFileInMountPoint(t, newName)
		require.Equal(t, []byte("rename-me"), got)

		// Readdir should show only keep.txt and new.txt.
		entries := env.MustReadDirInMountPoint(t, dir)
		names := dirEntryNames(entries)
		require.Contains(t, names, "keep.txt")
		require.Contains(t, names, "new.txt")
		require.NotContains(t, names, "deleted.txt")
		require.NotContains(t, names, "old.txt")
	})
}

// TestIndexed_setattr_shouldReflectInGetattr verifies that chmod/chtimes on an indexed file
// is immediately visible via stat (getattr) even though the physical file is unchanged.
func TestIndexed_setattr_shouldReflectInGetattr(t *testing.T) {
	withMountedFS(t, createIndexedCfg(), func(env *MountedFS) {
		rel := "indexed-ops/setattr-getattr/a.txt"
		env.MustCreateFileInStoragePath(t, []byte("data"), "hdd1", rel)

		mustRunPFS(t, env, "index", env.MountName)

		physical := env.StoragePath("hdd1", rel)
		physBefore, err := os.Stat(physical)
		require.NoError(t, err)

		// chmod through mount.
		newPerm := os.FileMode(0o600)
		require.NoError(t, os.Chmod(env.MountPath(rel), newPerm))

		// getattr through mount should reflect new perms.
		fiMount, err := os.Stat(env.MountPath(rel))
		require.NoError(t, err)
		require.Equal(t, newPerm, fiMount.Mode().Perm())

		// Physical file unchanged.
		physAfter, err := os.Stat(physical)
		require.NoError(t, err)
		require.Equal(t, physBefore.Mode().Perm(), physAfter.Mode().Perm())

		// chtimes through mount.
		newMTime := time.Date(2025, 6, 15, 12, 0, 0, 0, time.UTC)
		require.NoError(t, os.Chtimes(env.MountPath(rel), newMTime, newMTime))

		fiMount2, err := os.Stat(env.MountPath(rel))
		require.NoError(t, err)
		require.Equal(t, newMTime.Unix(), fiMount2.ModTime().Unix())

		// Physical mtime unchanged.
		physAfter2, err := os.Stat(physical)
		require.NoError(t, err)
		require.Equal(t, physBefore.ModTime().Unix(), physAfter2.ModTime().Unix())
	})
}

// TestIndexed_readRenamedFile_shouldUseRealPath verifies that reading a file at its new virtual
// path correctly follows real_path to the original physical location.
func TestIndexed_readRenamedFile_shouldUseRealPath(t *testing.T) {
	withMountedFS(t, createIndexedCfg(), func(env *MountedFS) {
		dir := "indexed-ops/read-renamed"
		oldRel := dir + "/original.txt"
		newRel := dir + "/renamed.txt"
		content := []byte("content-at-original-location")
		env.MustCreateFileInStoragePath(t, content, "hdd1", oldRel)

		mustRunPFS(t, env, "index", env.MountName)

		// Rename through mount.
		env.MustRenameFileInMountPoint(t, oldRel, newRel)

		// Physical file still at old path.
		require.FileExists(t, env.StoragePath("hdd1", oldRel))
		require.NoFileExists(t, env.StoragePath("hdd1", newRel))

		// Read through mount at new path should follow real_path.
		got := env.MustReadFileInMountPoint(t, newRel)
		require.Equal(t, content, got)

		// Read at old path should fail (ENOENT through mount).
		_, err := env.ReadFileInMountPoint(oldRel)
		require.Error(t, err)
	})
}
