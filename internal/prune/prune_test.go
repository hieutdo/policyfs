package prune

import (
	"context"
	"errors"
	"fmt"
	"os"
	"path/filepath"
	"syscall"
	"testing"

	"github.com/hieutdo/policyfs/internal/config"
	"github.com/hieutdo/policyfs/internal/errkind"
	"github.com/hieutdo/policyfs/internal/eventlog"
	"github.com/hieutdo/policyfs/internal/indexdb"
	"github.com/stretchr/testify/require"
)

// errFakeReader is a deterministic reader error used to exercise RunOneshot error branches.
var errFakeReader = errors.New("fake reader error")

// fakeErrReader is a minimal prune.eventReader that always returns a non-EOF error.
type fakeErrReader struct{}

// Next implements prune.eventReader.
func (fakeErrReader) Next() ([]byte, int64, error) { return nil, 0, errFakeReader }

// Offset implements prune.eventReader.
func (fakeErrReader) Offset() int64 { return 0 }

// Close implements prune.eventReader.
func (fakeErrReader) Close() error { return nil }

// setupPruneTestEnv configures per-test runtime/state dirs for prune and returns the mount state dir.
func setupPruneTestEnv(t *testing.T, mountName string) string {
	t.Helper()

	runtimeDir := filepath.Join(t.TempDir(), "runtime")
	require.NoError(t, os.MkdirAll(runtimeDir, 0o755))
	t.Setenv(config.EnvRuntimeDir, runtimeDir)

	stateDir := filepath.Join(t.TempDir(), "state")
	require.NoError(t, os.MkdirAll(stateDir, 0o755))
	t.Setenv(config.EnvStateDir, stateDir)

	return filepath.Join(stateDir, mountName)
}

// mustWriteFile creates a file and its parent directories.
func mustWriteFile(t *testing.T, p string, content []byte) {
	t.Helper()
	require.NoError(t, os.MkdirAll(filepath.Dir(p), 0o755))
	require.NoError(t, os.WriteFile(p, content, 0o644))
}

// fakeEvent is a minimal eventlog.Event implementation used to exercise unknown-type branches.
type fakeEvent struct{}

// EventType implements eventlog.Event.
func (fakeEvent) EventType() eventlog.Type { return "BOGUS" }

// TestValidateEventVirtualPath_shouldRejectInvalidAndAllowValid verifies validateEventVirtualPath
// rejects unsafe/invalid virtual paths and allows normal relative paths.
func TestValidateEventVirtualPath_shouldRejectInvalidAndAllowValid(t *testing.T) {
	t.Run("should accept normal relative", func(t *testing.T) {
		require.NoError(t, validateEventVirtualPath("a/b/c.txt"))
	})

	cases := []struct {
		name string
		path string
	}{
		{name: "empty", path: ""},
		{name: "dot", path: "."},
		{name: "absolute", path: "/abs"},
		{name: "trailing slash", path: "a/"},
		{name: "double slash", path: "a//b"},
		{name: "dot segment", path: "a/./b"},
		{name: "dotdot segment", path: "a/../b"},
		{name: "null byte", path: "a\x00b"},
	}

	for _, tc := range cases {
		t.Run(tc.name, func(t *testing.T) {
			err := validateEventVirtualPath(tc.path)
			require.Error(t, err)
			require.ErrorIs(t, err, errkind.ErrInvalid)
		})
	}
}

// TestPhysicalPathFor_shouldResolveAndValidate verifies physicalPathFor resolves the absolute
// physical path, validates the storage root, and rejects invalid storage/path inputs.
func TestPhysicalPathFor_shouldResolveAndValidate(t *testing.T) {
	t.Run("should return not found when storage id missing", func(t *testing.T) {
		_, err := physicalPathFor(map[string]string{"hdd1": t.TempDir()}, "ghost", "a.txt")
		require.Error(t, err)
		require.ErrorIs(t, err, errkind.ErrNotFound)
	})

	t.Run("should reject relative storage root", func(t *testing.T) {
		_, err := physicalPathFor(map[string]string{"hdd1": "relative"}, "hdd1", "a.txt")
		require.Error(t, err)
		require.ErrorIs(t, err, errkind.ErrInvalid)
	})

	t.Run("should reject invalid event path", func(t *testing.T) {
		_, err := physicalPathFor(map[string]string{"hdd1": t.TempDir()}, "hdd1", "../x")
		require.Error(t, err)
		require.ErrorIs(t, err, errkind.ErrInvalid)
	})

	t.Run("should resolve under root", func(t *testing.T) {
		root := t.TempDir()
		got, err := physicalPathFor(map[string]string{"hdd1": root}, "hdd1", "a/b.txt")
		require.NoError(t, err)
		require.Equal(t, filepath.Join(root, "a", "b.txt"), got)
	})
}

// TestBuildVerboseEvent_shouldSetResultAndFields verifies the verbose hook payload uses stable
// result strings and pulls the correct fields from each event type.
func TestBuildVerboseEvent_shouldSetResultAndFields(t *testing.T) {
	t.Run("should set ok for succeeded", func(t *testing.T) {
		v := buildVerboseEvent(eventlog.DeleteEvent{Type: eventlog.TypeDelete, StorageID: "hdd1", Path: "a"}, true, applyResult{succeeded: 1})
		require.Equal(t, "ok", v.Result)
		require.True(t, v.DryRun)
		require.Equal(t, eventlog.TypeDelete, v.Type)
		require.Equal(t, "hdd1", v.StorageID)
		require.Equal(t, "a", v.Path)
	})

	t.Run("should set skipped for skipped", func(t *testing.T) {
		v := buildVerboseEvent(eventlog.RenameEvent{Type: eventlog.TypeRename, StorageID: "hdd1", OldPath: "a", NewPath: "b"}, false, applyResult{skipped: 1})
		require.Equal(t, "skipped", v.Result)
		require.False(t, v.DryRun)
		require.Equal(t, eventlog.TypeRename, v.Type)
		require.Equal(t, "hdd1", v.StorageID)
		require.Equal(t, "a", v.OldPath)
		require.Equal(t, "b", v.NewPath)
	})

	t.Run("should default to failed", func(t *testing.T) {
		v := buildVerboseEvent(eventlog.SetattrEvent{Type: eventlog.TypeSetattr, StorageID: "hdd1", Path: "a"}, false, applyResult{failed: 1})
		require.Equal(t, "failed", v.Result)
		require.Equal(t, eventlog.TypeSetattr, v.Type)
		require.Equal(t, "hdd1", v.StorageID)
		require.Equal(t, "a", v.Path)
	})
}

// TestApplyOneEvent_shouldHandleNilDBAndUnknownType verifies dispatcher error handling does not
// rely on string matching and returns stable typed errors.
func TestApplyOneEvent_shouldHandleNilDBAndUnknownType(t *testing.T) {
	t.Run("should return nil error kind when db is nil", func(t *testing.T) {
		res, warnings, advance, retryLater, err := applyOneEvent(context.Background(), nil, nil, eventlog.DeleteEvent{Type: eventlog.TypeDelete, StorageID: "hdd1", Path: "a"}, false)
		require.Equal(t, int64(1), res.failed)
		require.Empty(t, warnings)
		require.False(t, advance)
		require.True(t, retryLater)
		require.Error(t, err)
		require.ErrorIs(t, err, errkind.ErrNil)
	})

	t.Run("should return invalid error kind for unknown event type", func(t *testing.T) {
		mount := "media"
		_ = setupPruneTestEnv(t, mount)

		db, err := indexdb.Open(mount)
		require.NoError(t, err)
		t.Cleanup(func() { _ = db.Close() })

		res, warnings, advance, retryLater, err := applyOneEvent(context.Background(), nil, db, fakeEvent{}, false)
		require.Equal(t, int64(1), res.failed)
		require.Empty(t, warnings)
		require.True(t, advance)
		require.False(t, retryLater)
		require.Error(t, err)
		require.ErrorIs(t, err, errkind.ErrInvalid)
	})
}

// TestMaybeTruncateEvents_shouldTruncateWhenOffsetMatches verifies truncation occurs only when
// the reader offset equals the file size, and writes offset=0.
func TestMaybeTruncateEvents_shouldTruncateWhenOffsetMatches(t *testing.T) {
	mount := "media"
	_ = setupPruneTestEnv(t, mount)

	logPath := filepath.Join(config.MountStateDir(mount), "events.ndjson")
	require.NoError(t, os.MkdirAll(filepath.Dir(logPath), 0o755))
	content := []byte("line1\nline2\n")
	require.NoError(t, os.WriteFile(logPath, content, eventlog.FileMode))

	truncated, err := maybeTruncateEvents(mount, int64(len(content)))
	require.NoError(t, err)
	require.True(t, truncated)

	st, err := os.Stat(logPath)
	require.NoError(t, err)
	require.Equal(t, int64(0), st.Size())

	off, err := eventlog.ReadOffset(mount)
	require.NoError(t, err)
	require.Equal(t, int64(0), off)
}

// TestMaybeTruncateEvents_shouldNotTruncateOnMismatchOrMissing verifies mismatch offsets or missing
// log files do not error and do not truncate.
func TestMaybeTruncateEvents_shouldNotTruncateOnMismatchOrMissing(t *testing.T) {
	mount := "media"
	_ = setupPruneTestEnv(t, mount)

	logPath := filepath.Join(config.MountStateDir(mount), "events.ndjson")
	require.NoError(t, os.MkdirAll(filepath.Dir(logPath), 0o755))
	content := []byte("line1\n")
	require.NoError(t, os.WriteFile(logPath, content, eventlog.FileMode))

	truncated, err := maybeTruncateEvents(mount, int64(len(content)-1))
	require.NoError(t, err)
	require.False(t, truncated)

	st, err := os.Stat(logPath)
	require.NoError(t, err)
	require.Equal(t, int64(len(content)), st.Size())

	require.NoError(t, os.Remove(logPath))
	truncated, err = maybeTruncateEvents(mount, 123)
	require.NoError(t, err)
	require.False(t, truncated)
}

// TestMaybeTruncateEvents_shouldReturnTypedErrors verifies invalid mount names are rejected and
// underlying open errors are wrapped.
func TestMaybeTruncateEvents_shouldReturnTypedErrors(t *testing.T) {
	_, err := maybeTruncateEvents("", 1)
	require.Error(t, err)
	require.ErrorIs(t, err, errkind.ErrRequired)

	mount := "media"
	_ = setupPruneTestEnv(t, mount)

	logPath := filepath.Join(config.MountStateDir(mount), "events.ndjson")
	require.NoError(t, os.MkdirAll(logPath, 0o755))

	_, err = maybeTruncateEvents(mount, 1)
	require.Error(t, err)
	require.ErrorIs(t, err, syscall.EISDIR)
}

// TestApplyDelete_shouldSkipWhenENOENT verifies DELETE treats missing files as already-deleted and
// advances the offset while finalizing DB state.
func TestApplyDelete_shouldSkipWhenENOENT(t *testing.T) {
	mount := "media"
	_ = setupPruneTestEnv(t, mount)

	root := t.TempDir()
	storageRoots := map[string]string{"hdd1": root}

	db, err := indexdb.Open(mount)
	require.NoError(t, err)
	t.Cleanup(func() { _ = db.Close() })

	ev := eventlog.DeleteEvent{Type: eventlog.TypeDelete, StorageID: "hdd1", Path: "missing.txt", IsDir: false, TS: 1}
	res, warnings, advance, retryLater, err := applyDelete(context.Background(), storageRoots, db, ev, false)

	require.NoError(t, err)
	require.True(t, advance)
	require.False(t, retryLater)
	require.Equal(t, int64(1), res.skipped)
	require.Empty(t, warnings)
}

// TestApplyDelete_shouldReturnHardErrorOnUnlinkDir verifies DELETE returns a hard error when
// unlink fails with an unhandled errno (commonly EISDIR/EPERM when attempting to unlink a dir).
func TestApplyDelete_shouldReturnHardErrorOnUnlinkDir(t *testing.T) {
	mount := "media"
	_ = setupPruneTestEnv(t, mount)

	root := t.TempDir()
	rel := "d"
	dir := filepath.Join(root, rel)
	require.NoError(t, os.MkdirAll(dir, 0o755))

	storageRoots := map[string]string{"hdd1": root}
	db, err := indexdb.Open(mount)
	require.NoError(t, err)
	t.Cleanup(func() { _ = db.Close() })

	ev := eventlog.DeleteEvent{Type: eventlog.TypeDelete, StorageID: "hdd1", Path: rel, IsDir: false, TS: 1}
	res, warnings, advance, retryLater, err := applyDelete(context.Background(), storageRoots, db, ev, false)

	require.Error(t, err)
	require.True(t, errors.Is(err, syscall.EISDIR) || errors.Is(err, syscall.EPERM), "expected EISDIR/EPERM, got: %v", err)
	require.False(t, advance)
	require.True(t, retryLater)
	require.Equal(t, int64(1), res.failed)
	require.Empty(t, warnings)

	require.DirExists(t, dir)
}

// TestApplyDelete_shouldWarnOnPermissionDeniedFile verifies DELETE emits a warning and advances
// when unlink fails with EPERM/EACCES. This typically requires running as non-root.
func TestApplyDelete_shouldWarnOnPermissionDeniedFile(t *testing.T) {
	if os.Getuid() == 0 {
		t.Skip("test requires non-root to trigger EPERM/EACCES on unlink")
	}

	mount := "media"
	_ = setupPruneTestEnv(t, mount)

	root := t.TempDir()
	targetRel := "a.txt"
	target := filepath.Join(root, targetRel)
	mustWriteFile(t, target, []byte("x"))

	// Make the storage root non-writable so unlink fails.
	require.NoError(t, os.Chmod(root, 0o555))
	t.Cleanup(func() { _ = os.Chmod(root, 0o755) })

	storageRoots := map[string]string{"hdd1": root}
	db, err := indexdb.Open(mount)
	require.NoError(t, err)
	t.Cleanup(func() { _ = db.Close() })

	ev := eventlog.DeleteEvent{Type: eventlog.TypeDelete, StorageID: "hdd1", Path: targetRel, IsDir: false, TS: 1}
	res, warnings, advance, retryLater, err := applyDelete(context.Background(), storageRoots, db, ev, false)

	require.NoError(t, err)
	require.True(t, advance)
	require.False(t, retryLater)
	require.Equal(t, int64(1), res.failed)
	require.Equal(t, []string{fmt.Sprintf("unlink permission denied: storage_id=%s path=%s", "hdd1", targetRel)}, warnings)

	require.FileExists(t, target)
}

// TestApplyDelete_shouldReturnHardErrorOnENOTDIR verifies dir DELETE returns a hard error when
// rmdir fails with an unhandled errno (e.g., ENOTDIR when the target is a file).
func TestApplyDelete_shouldReturnHardErrorOnENOTDIR(t *testing.T) {
	mount := "media"
	_ = setupPruneTestEnv(t, mount)

	root := t.TempDir()
	rel := "a.txt"
	mustWriteFile(t, filepath.Join(root, rel), []byte("x"))

	storageRoots := map[string]string{"hdd1": root}
	db, err := indexdb.Open(mount)
	require.NoError(t, err)
	t.Cleanup(func() { _ = db.Close() })

	ev := eventlog.DeleteEvent{Type: eventlog.TypeDelete, StorageID: "hdd1", Path: rel, IsDir: true, TS: 1}
	res, warnings, advance, retryLater, err := applyDelete(context.Background(), storageRoots, db, ev, false)

	require.Error(t, err)
	require.ErrorIs(t, err, syscall.ENOTDIR)
	require.False(t, advance)
	require.True(t, retryLater)
	require.Equal(t, int64(1), res.failed)
	require.Empty(t, warnings)
}

// TestApplyRename_shouldSkipWhenOldENOENT verifies RENAME treats missing old paths as already-applied
// and advances the offset after finalizing DB state.
func TestApplyRename_shouldSkipWhenOldENOENT(t *testing.T) {
	mount := "media"
	_ = setupPruneTestEnv(t, mount)

	root := t.TempDir()
	storageRoots := map[string]string{"hdd1": root}

	db, err := indexdb.Open(mount)
	require.NoError(t, err)
	t.Cleanup(func() { _ = db.Close() })

	ev := eventlog.RenameEvent{Type: eventlog.TypeRename, StorageID: "hdd1", OldPath: "missing.txt", NewPath: "new.txt", TS: 1}
	res, warnings, advance, retryLater, err := applyRename(context.Background(), storageRoots, db, ev, false)

	require.NoError(t, err)
	require.True(t, advance)
	require.False(t, retryLater)
	require.Equal(t, int64(1), res.skipped)
	require.Empty(t, warnings)
}

// TestApplyRename_shouldFailWhenMkdirAllFails verifies a hard failure occurs when destination parent
// cannot be created.
func TestApplyRename_shouldFailWhenMkdirAllFails(t *testing.T) {
	mount := "media"
	_ = setupPruneTestEnv(t, mount)

	root := t.TempDir()
	storageRoots := map[string]string{"hdd1": root}

	// Old file exists.
	oldRel := "a.txt"
	mustWriteFile(t, filepath.Join(root, oldRel), []byte("x"))

	// Block MkdirAll by placing a file where the destination parent dir should be.
	blocker := filepath.Join(root, "dest")
	mustWriteFile(t, blocker, []byte("not-a-dir"))

	db, err := indexdb.Open(mount)
	require.NoError(t, err)
	t.Cleanup(func() { _ = db.Close() })

	ev := eventlog.RenameEvent{Type: eventlog.TypeRename, StorageID: "hdd1", OldPath: oldRel, NewPath: "dest/new.txt", TS: 1}
	res, warnings, advance, retryLater, err := applyRename(context.Background(), storageRoots, db, ev, false)

	require.Error(t, err)
	var pe *os.PathError
	require.ErrorAs(t, err, &pe)

	require.False(t, advance)
	require.True(t, retryLater)
	require.Equal(t, int64(1), res.failed)
	require.Empty(t, warnings)
}

// TestApplyRename_shouldWarnOnNotEmpty verifies rename emits a warning and advances when renaming
// over a non-empty directory (ENOTEMPTY).
func TestApplyRename_shouldWarnOnNotEmpty(t *testing.T) {
	mount := "media"
	_ = setupPruneTestEnv(t, mount)

	root := t.TempDir()
	storageRoots := map[string]string{"hdd1": root}

	oldRel := "src"
	oldP := filepath.Join(root, oldRel)
	require.NoError(t, os.MkdirAll(oldP, 0o755))

	newRel := "dst"
	newP := filepath.Join(root, newRel)
	require.NoError(t, os.MkdirAll(newP, 0o755))
	mustWriteFile(t, filepath.Join(newP, "child.txt"), []byte("x"))

	db, err := indexdb.Open(mount)
	require.NoError(t, err)
	t.Cleanup(func() { _ = db.Close() })

	ev := eventlog.RenameEvent{Type: eventlog.TypeRename, StorageID: "hdd1", OldPath: oldRel, NewPath: newRel, TS: 1}
	res, warnings, advance, retryLater, err := applyRename(context.Background(), storageRoots, db, ev, false)

	require.NoError(t, err)
	require.True(t, advance)
	require.False(t, retryLater)
	require.Equal(t, int64(1), res.failed)
	require.Equal(t, []string{fmt.Sprintf("rename not empty: storage_id=%s old_path=%s new_path=%s", "hdd1", oldRel, newRel)}, warnings)

	require.DirExists(t, oldP)
}

// TestRunOneshot_shouldReturnErrorWhenReaderNextFails verifies the event loop returns a wrapped
// error when the event reader returns a non-EOF error.
func TestRunOneshot_shouldReturnErrorWhenReaderNextFails(t *testing.T) {
	mount := "media"
	_ = setupPruneTestEnv(t, mount)

	prev := openEventReader
	openEventReader = func(mountName string, offset int64) (eventReader, error) {
		_ = mountName
		_ = offset
		return fakeErrReader{}, nil
	}
	t.Cleanup(func() { openEventReader = prev })

	_, err := RunOneshot(context.Background(), mount, &config.MountConfig{}, Opts{}, Hooks{})
	require.Error(t, err)
	require.Contains(t, err.Error(), "failed to read next event")
	require.ErrorIs(t, err, errFakeReader)
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

// TestRunOneshot_InvalidPathTraversal_shouldWarnAndSkip verifies prune rejects escaping paths
// and still processes subsequent valid events.
func TestRunOneshot_InvalidPathTraversal_shouldWarnAndSkip(t *testing.T) {
	mount := "media"
	_ = setupPruneTestEnv(t, mount)

	storageRoot := filepath.Join(t.TempDir(), "hdd1")
	require.NoError(t, os.MkdirAll(storageRoot, 0o755))

	// A file outside the storage root should never be touched.
	outside := filepath.Join(t.TempDir(), "outside.txt")
	mustWriteFile(t, outside, []byte("do-not-touch"))

	// Event 1: escape-root attempt.
	require.NoError(t, eventlog.Append(context.Background(), mount,
		eventlog.DeleteEvent{Type: eventlog.TypeDelete, StorageID: "hdd1", Path: "../outside.txt", IsDir: false, TS: 1}))

	// Event 2: valid DELETE.
	insideRel := "ok.txt"
	insidePhysical := filepath.Join(storageRoot, insideRel)
	mustWriteFile(t, insidePhysical, []byte("hello"))
	require.NoError(t, eventlog.Append(context.Background(), mount,
		eventlog.DeleteEvent{Type: eventlog.TypeDelete, StorageID: "hdd1", Path: insideRel, IsDir: false, TS: 2}))

	res, err := RunOneshot(context.Background(), mount,
		&config.MountConfig{StoragePaths: []config.StoragePath{{ID: "hdd1", Path: storageRoot}}},
		Opts{}, Hooks{})
	require.NoError(t, err)
	require.Equal(t, int64(2), res.EventsProcessed)
	require.Equal(t, int64(1), res.EventsSucceeded)
	require.Equal(t, int64(1), res.EventsFailed)
	require.NotEmpty(t, res.Warnings)
	require.Contains(t, res.Warnings[0], "invalid path")

	// Invalid event must not touch outside file.
	require.FileExists(t, outside)
	// Valid event should apply.
	require.NoFileExists(t, insidePhysical)
}

// TestRunOneshot_InvalidPathForms_shouldWarnAndSkip verifies various invalid path encodings are rejected.
func TestRunOneshot_InvalidPathForms_shouldWarnAndSkip(t *testing.T) {
	mount := "media"
	_ = setupPruneTestEnv(t, mount)

	storageRoot := filepath.Join(t.TempDir(), "hdd1")
	require.NoError(t, os.MkdirAll(storageRoot, 0o755))

	cases := []struct {
		name string
		path string
	}{
		{name: "absolute", path: "/abs.txt"},
		{name: "trailing-slash", path: "dir/"},
		{name: "dot-segment", path: "a/./b"},
		{name: "dotdot-segment", path: "a/../b"},
		{name: "double-slash", path: "a//b"},
	}

	for i, tc := range cases {
		t.Run(tc.name, func(t *testing.T) {
			require.NoError(t, eventlog.Append(context.Background(), mount,
				eventlog.DeleteEvent{Type: eventlog.TypeDelete, StorageID: "hdd1", Path: tc.path, IsDir: false, TS: int64(i + 1)}))
			res, err := RunOneshot(context.Background(), mount,
				&config.MountConfig{StoragePaths: []config.StoragePath{{ID: "hdd1", Path: storageRoot}}},
				Opts{Limit: 1}, Hooks{})
			require.NoError(t, err)
			require.Equal(t, int64(1), res.EventsProcessed)
			require.Equal(t, int64(0), res.EventsSucceeded)
			require.Equal(t, int64(1), res.EventsFailed)
			require.NotEmpty(t, res.Warnings)
			require.Contains(t, res.Warnings[0], "invalid path")
		})
	}
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
