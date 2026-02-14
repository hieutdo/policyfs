package prune

import (
	"context"
	"fmt"
	"os"
	"path/filepath"
	"testing"

	"github.com/hieutdo/policyfs/internal/config"
	"github.com/hieutdo/policyfs/internal/eventlog"
	"github.com/hieutdo/policyfs/internal/indexdb"
	"github.com/stretchr/testify/require"
)

// setupPruneTestEnv configures per-test runtime/state dirs for prune and returns the mount state dir.
func setupPruneTestEnv(t *testing.T, mountName string) string {
	t.Helper()

	runtimeDir := filepath.Join(t.TempDir(), "runtime")
	require.NoError(t, os.MkdirAll(runtimeDir, 0o755))
	t.Setenv("PFS_RUNTIME_DIR", runtimeDir)

	stateDir := filepath.Join(t.TempDir(), "state")
	require.NoError(t, os.MkdirAll(stateDir, 0o755))
	t.Setenv("PFS_STATE_DIR", stateDir)

	return filepath.Join(stateDir, mountName)
}

// mustWriteFile creates a file and its parent directories.
func mustWriteFile(t *testing.T, p string, content []byte) {
	t.Helper()
	require.NoError(t, os.MkdirAll(filepath.Dir(p), 0o755))
	require.NoError(t, os.WriteFile(p, content, 0o644))
}

// TestRunOneshot_DeleteFile_shouldDeletePhysicalAndTruncateLog verifies a successful DELETE unlinks the physical file and truncates the log.
func TestRunOneshot_DeleteFile_shouldDeletePhysicalAndTruncateLog(t *testing.T) {
	mount := "media"
	_ = setupPruneTestEnv(t, mount)

	storageRoot := filepath.Join(t.TempDir(), "hdd1")
	require.NoError(t, os.MkdirAll(storageRoot, 0o755))

	rel := "a.txt"
	physical := filepath.Join(storageRoot, rel)
	mustWriteFile(t, physical, []byte("hello"))

	require.NoError(t, eventlog.Append(context.Background(), mount, eventlog.DeleteEvent{Type: eventlog.TypeDelete, StorageID: "hdd1", Path: rel, IsDir: false, TS: 1}))

	res, err := RunOneshot(context.Background(), mount, &config.MountConfig{StoragePaths: []config.StoragePath{{ID: "hdd1", Path: storageRoot}}}, Opts{}, Hooks{})
	require.NoError(t, err)
	require.Equal(t, int64(1), res.EventsProcessed)
	require.Equal(t, int64(1), res.EventsSucceeded)
	require.Equal(t, int64(0), res.EventsFailed)
	require.True(t, res.Truncated)
	require.Empty(t, res.Warnings)

	require.NoFileExists(t, physical)

	logPath := filepath.Join(config.MountStateDir(mount), "events.ndjson")
	st, err := os.Stat(logPath)
	require.NoError(t, err)
	require.Equal(t, int64(0), st.Size())

	off, err := os.ReadFile(filepath.Join(config.MountStateDir(mount), "events.offset"))
	require.NoError(t, err)
	require.Equal(t, "0\n", string(off))
}

// TestRunOneshot_DeleteDirNotEmpty_shouldWarnAndAdvanceAndTruncate verifies a non-empty dir DELETE produces a warning and advances the offset.
func TestRunOneshot_DeleteDirNotEmpty_shouldWarnAndAdvanceAndTruncate(t *testing.T) {
	mount := "media"
	_ = setupPruneTestEnv(t, mount)

	storageRoot := filepath.Join(t.TempDir(), "hdd1")
	require.NoError(t, os.MkdirAll(storageRoot, 0o755))

	rel := "d"
	dirPath := filepath.Join(storageRoot, rel)
	require.NoError(t, os.MkdirAll(dirPath, 0o755))
	mustWriteFile(t, filepath.Join(dirPath, "child.txt"), []byte("x"))

	require.NoError(t, eventlog.Append(context.Background(), mount, eventlog.DeleteEvent{Type: eventlog.TypeDelete, StorageID: "hdd1", Path: rel, IsDir: true, TS: 1}))

	res, err := RunOneshot(context.Background(), mount, &config.MountConfig{StoragePaths: []config.StoragePath{{ID: "hdd1", Path: storageRoot}}}, Opts{}, Hooks{})
	require.NoError(t, err)
	require.Equal(t, int64(1), res.EventsProcessed)
	require.Equal(t, int64(0), res.EventsSucceeded)
	require.Equal(t, int64(1), res.EventsFailed)
	require.True(t, res.Truncated)
	require.NotEmpty(t, res.Warnings)
	require.Contains(t, res.Warnings[0], "rmdir not empty")

	require.DirExists(t, dirPath)
}

// TestRunOneshot_InvalidJSON_shouldRecordWarningAndWriteOffsetWithoutTruncateWhenLimit verifies invalid JSON lines become warnings and the offset is persisted.
func TestRunOneshot_InvalidJSON_shouldRecordWarningAndWriteOffsetWithoutTruncateWhenLimit(t *testing.T) {
	mount := "media"
	_ = setupPruneTestEnv(t, mount)

	storageRoot := filepath.Join(t.TempDir(), "hdd1")
	require.NoError(t, os.MkdirAll(storageRoot, 0o755))

	invalidLine := []byte("{not json}")
	validLine := []byte(`{"type":"DELETE","storage_id":"hdd1","path":"a","is_dir":false,"ts":1}`)

	logPath := filepath.Join(config.MountStateDir(mount), "events.ndjson")
	require.NoError(t, os.MkdirAll(filepath.Dir(logPath), 0o755))
	content := append(append(append([]byte(nil), invalidLine...), '\n'), append(validLine, '\n')...)
	require.NoError(t, os.WriteFile(logPath, content, eventlog.FileMode))

	expectedOff := int64(len(invalidLine) + 1)

	res, err := RunOneshot(context.Background(), mount, &config.MountConfig{StoragePaths: []config.StoragePath{{ID: "hdd1", Path: storageRoot}}}, Opts{Limit: 1}, Hooks{})
	require.NoError(t, err)
	require.Equal(t, int64(1), res.EventsProcessed)
	require.Equal(t, int64(1), res.EventsFailed)
	require.False(t, res.Truncated)
	require.Contains(t, res.Warnings, "invalid event json")

	off, err := os.ReadFile(filepath.Join(config.MountStateDir(mount), "events.offset"))
	require.NoError(t, err)
	require.Equal(t, fmt.Sprintf("%d\n", expectedOff), string(off))

	st, err := os.Stat(logPath)
	require.NoError(t, err)
	require.Greater(t, st.Size(), expectedOff)
}

// TestRunOneshot_DryRun_shouldNotModifyFilesystemOrOffsetOrTruncate verifies dry-run avoids touching disk state and offsets.
func TestRunOneshot_DryRun_shouldNotModifyFilesystemOrOffsetOrTruncate(t *testing.T) {
	mount := "media"
	_ = setupPruneTestEnv(t, mount)

	storageRoot := filepath.Join(t.TempDir(), "hdd1")
	require.NoError(t, os.MkdirAll(storageRoot, 0o755))

	rel := "a.txt"
	physical := filepath.Join(storageRoot, rel)
	mustWriteFile(t, physical, []byte("hello"))

	require.NoError(t, eventlog.Append(context.Background(), mount, eventlog.DeleteEvent{Type: eventlog.TypeDelete, StorageID: "hdd1", Path: rel, IsDir: false, TS: 1}))

	logPath := filepath.Join(config.MountStateDir(mount), "events.ndjson")
	stBefore, err := os.Stat(logPath)
	require.NoError(t, err)
	require.Greater(t, stBefore.Size(), int64(0))

	res, err := RunOneshot(context.Background(), mount, &config.MountConfig{StoragePaths: []config.StoragePath{{ID: "hdd1", Path: storageRoot}}}, Opts{DryRun: true}, Hooks{})
	require.NoError(t, err)
	require.Equal(t, int64(1), res.EventsProcessed)
	require.Equal(t, int64(1), res.EventsSucceeded)
	require.False(t, res.Truncated)

	require.FileExists(t, physical)

	off, err := eventlog.ReadOffset(mount)
	require.NoError(t, err)
	require.Equal(t, int64(0), off)

	stAfter, err := os.Stat(logPath)
	require.NoError(t, err)
	require.Equal(t, stBefore.Size(), stAfter.Size())
}

// TestRunOneshot_UnknownStorage_shouldSkipAndContinue verifies that events referencing a
// storage_id not present in the config are skipped with a warning but subsequent valid events
// are still processed.
func TestRunOneshot_UnknownStorage_shouldSkipAndContinue(t *testing.T) {
	mount := "media"
	_ = setupPruneTestEnv(t, mount)

	storageRoot := filepath.Join(t.TempDir(), "hdd1")
	require.NoError(t, os.MkdirAll(storageRoot, 0o755))

	rel := "a.txt"
	physical := filepath.Join(storageRoot, rel)
	mustWriteFile(t, physical, []byte("hello"))

	// Event 1: references a non-existent storage → should be skipped.
	require.NoError(t, eventlog.Append(context.Background(), mount,
		eventlog.DeleteEvent{Type: eventlog.TypeDelete, StorageID: "ghost", Path: "x.txt", IsDir: false, TS: 1}))
	// Event 2: valid DELETE → should succeed.
	require.NoError(t, eventlog.Append(context.Background(), mount,
		eventlog.DeleteEvent{Type: eventlog.TypeDelete, StorageID: "hdd1", Path: rel, IsDir: false, TS: 2}))

	res, err := RunOneshot(context.Background(), mount,
		&config.MountConfig{StoragePaths: []config.StoragePath{{ID: "hdd1", Path: storageRoot}}},
		Opts{}, Hooks{})
	require.NoError(t, err)
	require.Equal(t, int64(2), res.EventsProcessed)
	require.Equal(t, int64(1), res.EventsSucceeded, "valid event should succeed")
	require.Equal(t, int64(1), res.EventsFailed, "unknown storage event should be counted as failed")
	require.True(t, res.Truncated, "all events advanced past → log should be truncated")
	require.NotEmpty(t, res.Warnings)
	require.Contains(t, res.Warnings[0], "storage not found")

	require.NoFileExists(t, physical, "valid DELETE should have been applied")
}

// TestApplySetattr_partialFailure_chmodOK_chownEPERM verifies that when chmod succeeds but
// chown fails with EPERM, the event is marked as failed with a warning, but the offset still
// advances (no retry). Requires non-root to trigger EPERM on chown.
func TestApplySetattr_partialFailure_chmodOK_chownEPERM(t *testing.T) {
	if os.Getuid() == 0 {
		t.Skip("test requires non-root to trigger EPERM on chown")
	}

	mount := "media"
	_ = setupPruneTestEnv(t, mount)

	storageRoot := filepath.Join(t.TempDir(), "store1")
	require.NoError(t, os.MkdirAll(storageRoot, 0o755))

	rel := "test.txt"
	physical := filepath.Join(storageRoot, rel)
	mustWriteFile(t, physical, []byte("x"))

	// Open a test DB and seed a file entry so FinalizeSetattr has something to work on.
	db, err := indexdb.Open(mount)
	require.NoError(t, err)
	t.Cleanup(func() { _ = db.Close() })

	_, err = db.SQL().Exec(
		`INSERT INTO files (storage_id, path, real_path, parent_dir, name, is_dir, size, mtime, mode, uid, gid, deleted)
VALUES (?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?);`,
		"store1", rel, rel, "", rel, 0, 10, 1, 0o100644, os.Getuid(), os.Getgid(), 0,
	)
	require.NoError(t, err)

	// Craft a SETATTR event with both mode (chmod) and uid (chown-to-root).
	// As non-root, chmod on a file we own succeeds, but chown to root fails with EPERM.
	mode := uint32(0o600)
	uid := uint32(0) // chown to root → EPERM for non-root
	ev := eventlog.SetattrEvent{
		Type:      eventlog.TypeSetattr,
		StorageID: "store1",
		Path:      rel,
		Mode:      &mode,
		UID:       &uid,
		TS:        1,
	}

	storageRoots := map[string]string{"store1": storageRoot}
	res, warnings, advance, retryLater, applyErr := applySetattr(context.Background(), storageRoots, db, ev, false)

	// Should advance (not retry), report as failed due to partial failure, include warning.
	require.True(t, advance, "should advance past the event")
	require.False(t, retryLater, "EPERM is not retryable")
	require.NoError(t, applyErr, "should not return a hard error")
	require.Equal(t, int64(1), res.failed, "should report as failed due to partial EPERM")
	require.NotEmpty(t, warnings)
	require.Contains(t, warnings[0], "chown permission denied")

	// Verify: chmod was applied physically despite chown failure.
	fi, err := os.Stat(physical)
	require.NoError(t, err)
	require.Equal(t, os.FileMode(0o600), fi.Mode().Perm())

	// Verify: file_meta was still cleaned up (FinalizeSetattr runs even on partial failure).
	var metaCount int
	err = db.SQL().QueryRow(
		`SELECT COUNT(*) FROM file_meta WHERE storage_id = ? AND path = ?;`,
		"store1", rel,
	).Scan(&metaCount)
	require.NoError(t, err)
	require.Equal(t, 0, metaCount, "file_meta should be cleaned up after partial failure")
}
