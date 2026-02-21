//go:build integration

package integration

import (
	"bufio"
	"bytes"
	"context"
	"database/sql"
	"errors"
	"fmt"
	"os"
	"os/exec"
	"path/filepath"
	"strconv"
	"testing"
	"time"

	sqlite3 "github.com/mattn/go-sqlite3"
	"github.com/stretchr/testify/require"

	"github.com/hieutdo/policyfs/internal/eventlog"
)

const exitNoChanges = 3

// mustRunPFS runs the pfs CLI with the integration test's runtime/state directories.
//
// It treats ExitNoChanges (3) as success to allow idempotent oneshot jobs.
func mustRunPFS(t *testing.T, env *MountedFS, args ...string) {
	t.Helper()
	cmd := exec.Command(pfsBin, append([]string{"--config", env.ConfigPath}, args...)...)
	cmd.Env = pfsTestEnv(env, "")
	cmd.Stdout = os.Stdout
	cmd.Stderr = os.Stderr
	if err := cmd.Run(); err != nil {
		if ee, ok := errors.AsType[*exec.ExitError](err); ok {
			if ee.ExitCode() == exitNoChanges {
				return
			}
		}
		require.NoError(t, err)
	}
}

// runPFSOutput runs the pfs CLI and returns the combined stdout+stderr for assertion.
func runPFSOutput(t *testing.T, env *MountedFS, args ...string) ([]byte, error) {
	t.Helper()
	cmd := exec.Command(pfsBin, append([]string{"--config", env.ConfigPath}, args...)...)
	cmd.Env = pfsTestEnv(env, "")
	out, err := cmd.CombinedOutput()
	if err != nil {
		if ee, ok := errors.AsType[*exec.ExitError](err); ok {
			if ee.ExitCode() == exitNoChanges {
				return out, nil
			}
		}
		return out, fmt.Errorf("pfs command failed: %w", err)
	}
	return out, nil
}

// readAllEvents parses all currently-written events from events.ndjson.
func readAllEvents(t *testing.T, env *MountedFS) []eventlog.Event {
	t.Helper()
	p := filepath.Join(env.StateDir, env.MountName, "events.ndjson")
	f, err := os.Open(p)
	require.NoError(t, err)
	defer func() { _ = f.Close() }()

	out := []eventlog.Event{}
	s := bufio.NewScanner(f)
	for s.Scan() {
		b := append([]byte(nil), s.Bytes()...)
		ev, err := eventlog.Parse(b)
		require.NoError(t, err)
		out = append(out, ev)
	}
	require.NoError(t, s.Err())
	return out
}

// waitForEventsFile waits until events.ndjson exists and is non-empty.
func waitForEventsFile(t *testing.T, env *MountedFS, timeout time.Duration) {
	t.Helper()
	p := filepath.Join(env.StateDir, env.MountName, "events.ndjson")
	deadline := time.Now().Add(timeout)
	for time.Now().Before(deadline) {
		st, err := os.Stat(p)
		if err == nil && st.Size() > 0 {
			return
		}
		time.Sleep(20 * time.Millisecond)
	}
	t.Fatalf("events file did not become ready: path=%s", p)
}

// openIndexDB opens the per-mount index.db for assertions in integration tests.
func openIndexDB(t testing.TB, env *MountedFS) *sql.DB {
	t.Helper()
	dbPath := filepath.Join(env.StateDir, env.MountName, "index.db")
	db, err := sql.Open("sqlite3", "file:"+dbPath+"?mode=ro&_foreign_keys=on&_busy_timeout=250")
	require.NoError(t, err)
	t.Cleanup(func() { _ = db.Close() })
	return db
}

// TestPrune_shouldApplyDeferredDeleteFile verifies indexed UNLINK produces a DELETE event and prune applies it.
func TestPrune_shouldApplyDeferredDeleteFile(t *testing.T) {
	cfg := createIndexedCfg()

	withMountedFS(t, cfg, func(env *MountedFS) {
		rel := "prune-flow/delete-file/a.txt"
		content := []byte("hello-prune-delete")
		env.MustCreateFileInStoragePath(t, content, "hdd1", rel)

		mustRunPFS(t, env, "index", env.MountName)

		// Action: unlink through the mount (indexed target) should defer physical deletion.
		env.MustRemoveFileInMountPoint(t, rel)

		// Verify: file still exists physically, but is gone from the mounted view.
		require.FileExists(t, env.StoragePath("hdd1", rel))
		require.False(t, env.FileExistsInMountPoint(rel))

		// Regression guard: re-index should not resurrect a deferred delete.
		mustRunPFS(t, env, "index", env.MountName)
		require.False(t, env.FileExistsInMountPoint(rel))

		waitForEventsFile(t, env, 2*time.Second)
		evs := readAllEvents(t, env)
		require.NotEmpty(t, evs)
		e0, ok := evs[len(evs)-1].(eventlog.DeleteEvent)
		require.True(t, ok)
		require.Equal(t, eventlog.TypeDelete, e0.Type)
		require.Equal(t, "hdd1", e0.StorageID)
		require.Equal(t, rel, e0.Path)
		require.False(t, e0.IsDir)

		// Action: prune should apply physical unlink and finalize the DB.
		mustRunPFS(t, env, "prune", env.MountName)

		require.NoFileExists(t, env.StoragePath("hdd1", rel))

		eventsPath := filepath.Join(env.StateDir, env.MountName, "events.ndjson")
		offsetPath := filepath.Join(env.StateDir, env.MountName, "events.offset")
		st, err := os.Stat(eventsPath)
		require.NoError(t, err)
		require.Equal(t, int64(0), st.Size())

		off, err := os.ReadFile(offsetPath)
		require.NoError(t, err)
		require.Equal(t, "0\n", string(off))

		// Idempotency: a second prune run should succeed and keep the log empty.
		mustRunPFS(t, env, "prune", env.MountName)
		st, err = os.Stat(eventsPath)
		require.NoError(t, err)
		require.Equal(t, int64(0), st.Size())
	})
}

// TestPrune_shouldApplyDeferredDeleteDir verifies indexed RMDIR produces a DELETE event and prune applies it.
func TestPrune_shouldApplyDeferredDeleteDir(t *testing.T) {
	cfg := createIndexedCfg()

	withMountedFS(t, cfg, func(env *MountedFS) {
		rel := "prune-flow/delete-dir/d"
		env.MustCreateDirInStoragePath(t, "hdd1", rel)

		mustRunPFS(t, env, "index", env.MountName)

		// Action: rmdir through the mount should defer physical deletion.
		env.MustRemoveFileInMountPoint(t, rel)

		require.DirExists(t, env.StoragePath("hdd1", rel))
		require.False(t, env.FileExistsInMountPoint(rel))

		waitForEventsFile(t, env, 2*time.Second)
		evs := readAllEvents(t, env)
		require.NotEmpty(t, evs)
		e0, ok := evs[len(evs)-1].(eventlog.DeleteEvent)
		require.True(t, ok)
		require.Equal(t, eventlog.TypeDelete, e0.Type)
		require.Equal(t, rel, e0.Path)
		require.True(t, e0.IsDir)

		mustRunPFS(t, env, "prune", env.MountName)
		require.NoDirExists(t, env.StoragePath("hdd1", rel))
	})
}

// TestPrune_shouldApplyDeferredRenameFile verifies indexed RENAME produces a RENAME event and prune applies it.
func TestPrune_shouldApplyDeferredRenameFile(t *testing.T) {
	cfg := createIndexedCfg()

	withMountedFS(t, cfg, func(env *MountedFS) {
		oldRel := "prune-flow/rename-file/src.txt"
		newRel := "prune-flow/rename-file/dst.txt"
		content := []byte("hello-prune-rename")
		env.MustCreateFileInStoragePath(t, content, "hdd1", oldRel)

		mustRunPFS(t, env, "index", env.MountName)

		// Action: rename through the mount should update the DB but not touch disk.
		env.MustRenameFileInMountPoint(t, oldRel, newRel)

		require.FileExists(t, env.StoragePath("hdd1", oldRel))
		require.NoFileExists(t, env.StoragePath("hdd1", newRel))

		got := env.MustReadFileInMountPoint(t, newRel)
		require.Equal(t, content, got)

		// Regression guard: re-index should not clobber pending rename state.
		mustRunPFS(t, env, "index", env.MountName)
		gotAfterIndex := env.MustReadFileInMountPoint(t, newRel)
		require.Equal(t, content, gotAfterIndex)

		waitForEventsFile(t, env, 2*time.Second)
		evs := readAllEvents(t, env)
		require.NotEmpty(t, evs)
		e0, ok := evs[len(evs)-1].(eventlog.RenameEvent)
		require.True(t, ok)
		require.Equal(t, eventlog.TypeRename, e0.Type)
		require.Equal(t, "hdd1", e0.StorageID)
		require.Equal(t, oldRel, e0.OldPath)
		require.Equal(t, newRel, e0.NewPath)

		// Action: prune should perform physical rename and finalize real_path.
		mustRunPFS(t, env, "prune", env.MountName)

		require.NoFileExists(t, env.StoragePath("hdd1", oldRel))
		require.FileExists(t, env.StoragePath("hdd1", newRel))
		gotFinal := env.MustReadFileInMountPoint(t, newRel)
		require.Equal(t, content, gotFinal)

		// Assert: prune must finalize real_path to match path (avoid relying on FUSE fallback).
		db := openIndexDB(t, env)
		ctx, cancel := context.WithTimeout(context.Background(), 2*time.Second)
		defer cancel()

		var gotPath string
		var gotRealPath string
		err := db.QueryRowContext(ctx, `SELECT path, real_path FROM files WHERE storage_id = ? AND path = ? AND deleted = 0 LIMIT 1;`, "hdd1", newRel).Scan(&gotPath, &gotRealPath)
		require.NoError(t, err)
		require.Equal(t, newRel, gotPath)
		require.Equal(t, gotPath, gotRealPath)

		var one int
		err = db.QueryRowContext(ctx, `SELECT 1 FROM files WHERE storage_id = ? AND path = ? AND deleted = 0 LIMIT 1;`, "hdd1", oldRel).Scan(&one)
		require.ErrorIs(t, err, sql.ErrNoRows)
	})
}

// TestPrune_shouldApplyDeferredRenameFile_whenPhysicalAlreadyRenamed verifies prune treats ENOENT(old) as OK,
// finalizes the DB, and truncates the log.
func TestPrune_shouldApplyDeferredRenameFile_whenPhysicalAlreadyRenamed(t *testing.T) {
	cfg := createIndexedCfg()

	withMountedFS(t, cfg, func(env *MountedFS) {
		oldRel := "prune-flow/rename-idempotent/src.txt"
		newRel := "prune-flow/rename-idempotent/dst.txt"
		content := []byte("hello-prune-rename-idempotent")
		env.MustCreateFileInStoragePath(t, content, "hdd1", oldRel)

		mustRunPFS(t, env, "index", env.MountName)
		env.MustRenameFileInMountPoint(t, oldRel, newRel)

		waitForEventsFile(t, env, 2*time.Second)

		// Simulate: physical rename already happened out-of-band.
		require.NoError(t, os.Rename(env.StoragePath("hdd1", oldRel), env.StoragePath("hdd1", newRel)))
		require.NoFileExists(t, env.StoragePath("hdd1", oldRel))
		require.FileExists(t, env.StoragePath("hdd1", newRel))

		mustRunPFS(t, env, "prune", env.MountName)

		// After prune, the mount should read without requiring a stale real_path.
		gotFinal := env.MustReadFileInMountPoint(t, newRel)
		require.Equal(t, content, gotFinal)

		db := openIndexDB(t, env)
		ctx, cancel := context.WithTimeout(context.Background(), 2*time.Second)
		defer cancel()

		var gotPath string
		var gotRealPath string
		err := db.QueryRowContext(ctx, `SELECT path, real_path FROM files WHERE storage_id = ? AND path = ? AND deleted = 0 LIMIT 1;`, "hdd1", newRel).Scan(&gotPath, &gotRealPath)
		require.NoError(t, err)
		require.Equal(t, newRel, gotPath)
		require.Equal(t, gotPath, gotRealPath)

		eventsPath := filepath.Join(env.StateDir, env.MountName, "events.ndjson")
		offsetPath := filepath.Join(env.StateDir, env.MountName, "events.offset")
		st, err := os.Stat(eventsPath)
		require.NoError(t, err)
		require.Equal(t, int64(0), st.Size())

		off, err := os.ReadFile(offsetPath)
		require.NoError(t, err)
		require.Equal(t, "0\n", string(off))
	})
}

// TestPrune_shouldApplyDeferredRenameFile_whenDestParentMissing verifies prune recreates the destination parent
// directory before performing the physical rename.
func TestPrune_shouldApplyDeferredRenameFile_whenDestParentMissing(t *testing.T) {
	cfg := createIndexedCfg()

	withMountedFS(t, cfg, func(env *MountedFS) {
		oldRel := "prune-flow/rename-mkdir/src.txt"
		newRel := "prune-flow/rename-mkdir/newparent/dst.txt"
		content := []byte("hello-prune-rename-mkdir")
		env.MustCreateFileInStoragePath(t, content, "hdd1", oldRel)
		env.MustCreateDirInStoragePath(t, "hdd1", "prune-flow/rename-mkdir/newparent")

		mustRunPFS(t, env, "index", env.MountName)
		env.MustRenameFileInMountPoint(t, oldRel, newRel)
		waitForEventsFile(t, env, 2*time.Second)

		// Simulate: destination parent directory missing at prune time.
		require.NoError(t, os.RemoveAll(env.StoragePath("hdd1", "prune-flow/rename-mkdir/newparent")))
		require.DirExists(t, filepath.Dir(env.StoragePath("hdd1", oldRel)))
		require.NoDirExists(t, env.StoragePath("hdd1", "prune-flow/rename-mkdir/newparent"))

		mustRunPFS(t, env, "prune", env.MountName)

		require.NoFileExists(t, env.StoragePath("hdd1", oldRel))
		require.FileExists(t, env.StoragePath("hdd1", newRel))
		gotFinal := env.MustReadFileInMountPoint(t, newRel)
		require.Equal(t, content, gotFinal)

		db := openIndexDB(t, env)

		var gotPath string
		var gotRealPath string
		deadline := time.Now().Add(2 * time.Second)
		for {
			err := db.QueryRowContext(context.Background(), `SELECT path, real_path FROM files WHERE storage_id = ? AND path = ? AND deleted = 0 LIMIT 1;`, "hdd1", newRel).Scan(&gotPath, &gotRealPath)
			if err == nil {
				break
			}
			se, seOK := errors.AsType[sqlite3.Error](err)
			if errors.Is(err, sql.ErrNoRows) || (seOK && (se.Code == sqlite3.ErrBusy || se.Code == sqlite3.ErrLocked)) {
				if time.Now().After(deadline) {
					require.NoError(t, err)
				}
				time.Sleep(20 * time.Millisecond)
				continue
			}
			require.NoError(t, err)
		}
		require.Equal(t, gotPath, gotRealPath)
	})
}

// TestPrune_shouldSkipInvalidJSONLines verifies prune skips invalid JSON lines that are not EOF-related and still
// applies subsequent valid events.
func TestPrune_shouldSkipInvalidJSONLines(t *testing.T) {
	cfg := createIndexedCfg()

	withMountedFS(t, cfg, func(env *MountedFS) {
		oldRel := "prune-flow/invalid-json/src.txt"
		newRel := "prune-flow/invalid-json/dst.txt"
		content := []byte("hello-prune-invalid-json")
		env.MustCreateFileInStoragePath(t, content, "hdd1", oldRel)

		mustRunPFS(t, env, "index", env.MountName)
		env.MustRenameFileInMountPoint(t, oldRel, newRel)
		waitForEventsFile(t, env, 2*time.Second)

		eventsPath := filepath.Join(env.StateDir, env.MountName, "events.ndjson")
		offsetPath := filepath.Join(env.StateDir, env.MountName, "events.offset")

		orig, err := os.ReadFile(eventsPath)
		require.NoError(t, err)
		require.True(t, bytes.Contains(orig, []byte("\n")))

		// Prepend an invalid line to exercise skip+advance semantics.
		corrupt := append([]byte("not-json\n"), orig...)
		require.NoError(t, os.WriteFile(eventsPath, corrupt, 0o644))
		require.NoError(t, os.WriteFile(offsetPath, []byte("0\n"), 0o644))

		mustRunPFS(t, env, "prune", env.MountName)
		require.NoFileExists(t, env.StoragePath("hdd1", oldRel))
		require.FileExists(t, env.StoragePath("hdd1", newRel))

		st, err := os.Stat(eventsPath)
		require.NoError(t, err)
		require.Equal(t, int64(0), st.Size())

		off, err := os.ReadFile(offsetPath)
		require.NoError(t, err)
		require.Equal(t, "0\n", string(off))
	})
}

// TestPrune_shouldNotAdvanceOffsetForPartialLastLine verifies prune stops on a partial last line (no newline),
// leaving the log untruncated and the offset pointing to the last successfully processed complete line.
func TestPrune_shouldNotAdvanceOffsetForPartialLastLine(t *testing.T) {
	cfg := createIndexedCfg()

	withMountedFS(t, cfg, func(env *MountedFS) {
		oldRel := "prune-flow/partial-line/src.txt"
		newRel := "prune-flow/partial-line/dst.txt"
		content := []byte("hello-prune-partial-line")
		env.MustCreateFileInStoragePath(t, content, "hdd1", oldRel)

		mustRunPFS(t, env, "index", env.MountName)
		env.MustRenameFileInMountPoint(t, oldRel, newRel)
		waitForEventsFile(t, env, 2*time.Second)

		eventsPath := filepath.Join(env.StateDir, env.MountName, "events.ndjson")
		offsetPath := filepath.Join(env.StateDir, env.MountName, "events.offset")

		orig, err := os.ReadFile(eventsPath)
		require.NoError(t, err)
		wantOff := int64(len(orig))

		f, err := os.OpenFile(eventsPath, os.O_WRONLY|os.O_APPEND, 0o644)
		require.NoError(t, err)
		_, err = f.WriteString("{\"type\":")
		require.NoError(t, err)
		require.NoError(t, f.Close())

		mustRunPFS(t, env, "prune", env.MountName)
		require.NoFileExists(t, env.StoragePath("hdd1", oldRel))
		require.FileExists(t, env.StoragePath("hdd1", newRel))

		offB, err := os.ReadFile(offsetPath)
		require.NoError(t, err)
		gotOff, err := strconv.ParseInt(string(bytes.TrimSpace(offB)), 10, 64)
		require.NoError(t, err)
		require.Equal(t, wantOff, gotOff)

		st, err := os.Stat(eventsPath)
		require.NoError(t, err)
		require.Greater(t, st.Size(), int64(0))

		// Idempotency: running prune again should keep offset stable until the last line is completed.
		mustRunPFS(t, env, "prune", env.MountName)
		offB2, err := os.ReadFile(offsetPath)
		require.NoError(t, err)
		gotOff2, err := strconv.ParseInt(string(bytes.TrimSpace(offB2)), 10, 64)
		require.NoError(t, err)
		require.Equal(t, wantOff, gotOff2)
	})
}

// TestPrune_shouldApplyDeferredRenameOverwriteFile verifies indexed RENAME over an existing dest keeps
// the old physical paths until prune, and after prune the destination contains the source content.
func TestPrune_shouldApplyDeferredRenameOverwriteFile(t *testing.T) {
	cfg := createIndexedCfg()

	withMountedFS(t, cfg, func(env *MountedFS) {
		srcRel := "prune-flow/rename-overwrite/src.txt"
		dstRel := "prune-flow/rename-overwrite/dst.txt"
		srcContent := []byte("hello-prune-rename-overwrite-src")
		dstContent := []byte("hello-prune-rename-overwrite-dst")
		env.MustCreateFileInStoragePath(t, srcContent, "hdd1", srcRel)
		env.MustCreateFileInStoragePath(t, dstContent, "hdd1", dstRel)

		mustRunPFS(t, env, "index", env.MountName)

		// Action: rename src -> dst through the mount should update DB but not touch disk.
		env.MustRenameFileInMountPoint(t, srcRel, dstRel)

		require.FileExists(t, env.StoragePath("hdd1", srcRel))
		require.FileExists(t, env.StoragePath("hdd1", dstRel))

		got := env.MustReadFileInMountPoint(t, dstRel)
		require.Equal(t, srcContent, got)

		// Regression guard: re-index should not clobber pending real_path.
		mustRunPFS(t, env, "index", env.MountName)
		gotAfterIndex := env.MustReadFileInMountPoint(t, dstRel)
		require.Equal(t, srcContent, gotAfterIndex)

		waitForEventsFile(t, env, 2*time.Second)
		evs := readAllEvents(t, env)
		require.NotEmpty(t, evs)
		e0, ok := evs[len(evs)-1].(eventlog.RenameEvent)
		require.True(t, ok)
		require.Equal(t, eventlog.TypeRename, e0.Type)
		require.Equal(t, "hdd1", e0.StorageID)
		require.Equal(t, srcRel, e0.OldPath)
		require.Equal(t, dstRel, e0.NewPath)

		// Action: prune should perform physical rename and overwrite destination.
		mustRunPFS(t, env, "prune", env.MountName)

		require.NoFileExists(t, env.StoragePath("hdd1", srcRel))
		require.FileExists(t, env.StoragePath("hdd1", dstRel))
		physicalGot := env.MustReadFileInStoragePath(t, "hdd1", dstRel)
		require.Equal(t, srcContent, physicalGot)
		gotFinal := env.MustReadFileInMountPoint(t, dstRel)
		require.Equal(t, srcContent, gotFinal)
	})
}

// TestPrune_shouldApplyDeferredSetattr verifies indexed SETATTR records metadata overrides and prune applies them.
func TestPrune_shouldApplyDeferredSetattr(t *testing.T) {
	cfg := createIndexedCfg()

	withMountedFS(t, cfg, func(env *MountedFS) {
		rel := "prune-flow/setattr/a.txt"
		env.MustCreateFileInStoragePath(t, []byte("hello-prune-setattr"), "hdd1", rel)

		mustRunPFS(t, env, "index", env.MountName)

		physical := env.StoragePath("hdd1", rel)
		fiBefore, err := os.Stat(physical)
		require.NoError(t, err)
		mtimeBefore := fiBefore.ModTime().Unix()
		permBefore := fiBefore.Mode().Perm()

		// Action: chmod + chtimes through the mount should not touch the physical file.
		newPerm := os.FileMode(0o600)
		require.NoError(t, os.Chmod(env.MountPath(rel), newPerm))
		newMTime := time.Unix(mtimeBefore+3600, 0)
		require.NoError(t, os.Chtimes(env.MountPath(rel), newMTime, newMTime))

		fiPhysicalAfter, err := os.Stat(physical)
		require.NoError(t, err)
		require.Equal(t, permBefore, fiPhysicalAfter.Mode().Perm())
		require.Equal(t, mtimeBefore, fiPhysicalAfter.ModTime().Unix())

		fiMountAfter, err := os.Stat(env.MountPath(rel))
		require.NoError(t, err)
		require.Equal(t, newPerm, fiMountAfter.Mode().Perm())
		require.Equal(t, newMTime.Unix(), fiMountAfter.ModTime().Unix())

		// Regression guard: re-index should preserve effective mounted attributes.
		mustRunPFS(t, env, "index", env.MountName)
		fiMountAfterIndex, err := os.Stat(env.MountPath(rel))
		require.NoError(t, err)
		require.Equal(t, newPerm, fiMountAfterIndex.Mode().Perm())
		require.Equal(t, newMTime.Unix(), fiMountAfterIndex.ModTime().Unix())

		mustRunPFS(t, env, "prune", env.MountName)

		fiPhysicalFinal, err := os.Stat(physical)
		require.NoError(t, err)
		require.Equal(t, newPerm, fiPhysicalFinal.Mode().Perm())
		require.Equal(t, newMTime.Unix(), fiPhysicalFinal.ModTime().Unix())

		fiMountFinal, err := os.Stat(env.MountPath(rel))
		require.NoError(t, err)
		require.Equal(t, newPerm, fiMountFinal.Mode().Perm())
		require.Equal(t, newMTime.Unix(), fiMountFinal.ModTime().Unix())
	})
}

// TestPrune_shouldSkipUnknownStorageAndContinue verifies prune skips events with a non-existent
// storage_id (warning + failed count) and still processes subsequent valid events.
func TestPrune_shouldSkipUnknownStorageAndContinue(t *testing.T) {
	cfg := createIndexedCfg()

	withMountedFS(t, cfg, func(env *MountedFS) {
		// Setup: create a file, index, unlink to produce a valid DELETE event.
		rel := "prune-flow/skip-storage/a.txt"
		content := []byte("hello-skip-storage")
		env.MustCreateFileInStoragePath(t, content, "hdd1", rel)
		mustRunPFS(t, env, "index", env.MountName)
		env.MustRemoveFileInMountPoint(t, rel)
		waitForEventsFile(t, env, 2*time.Second)

		// Prepend a DELETE event referencing a storage_id that doesn't exist in config.
		eventsPath := filepath.Join(env.StateDir, env.MountName, "events.ndjson")
		offsetPath := filepath.Join(env.StateDir, env.MountName, "events.offset")
		orig, err := os.ReadFile(eventsPath)
		require.NoError(t, err)

		fakeEvent := []byte(`{"type":"DELETE","storage_id":"nonexistent","path":"x.txt","is_dir":false,"ts":1}` + "\n")
		combined := append(fakeEvent, orig...)
		require.NoError(t, os.WriteFile(eventsPath, combined, 0o600))
		require.NoError(t, os.WriteFile(offsetPath, []byte("0\n"), 0o600))

		// Action: prune should skip the fake event and still process the real one.
		out, err := runPFSOutput(t, env, "prune", env.MountName)
		require.NoError(t, err, "prune failed: %s", string(out))

		// Verify: the real file was physically deleted.
		require.NoFileExists(t, env.StoragePath("hdd1", rel))

		// Verify: output contains a warning about the unknown storage.
		require.Contains(t, string(out), "storage not found")

		// Verify: events log was truncated (all events processed and advanced past).
		st, err := os.Stat(eventsPath)
		require.NoError(t, err)
		require.Equal(t, int64(0), st.Size())
	})
}

// TestPrune_shouldApplyDeferredSetattrOnDirectory verifies chmod on an indexed directory
// defers the physical change and prune applies it.
func TestPrune_shouldApplyDeferredSetattrOnDirectory(t *testing.T) {
	cfg := createIndexedCfg()

	withMountedFS(t, cfg, func(env *MountedFS) {
		rel := "prune-flow/setattr-dir/d"
		env.MustCreateDirInStoragePath(t, "hdd1", rel)
		mustRunPFS(t, env, "index", env.MountName)

		physical := env.StoragePath("hdd1", rel)
		fiBefore, err := os.Stat(physical)
		require.NoError(t, err)
		permBefore := fiBefore.Mode().Perm()

		// Action: chmod through the mount on the indexed directory.
		newPerm := os.FileMode(0o700)
		require.NoError(t, os.Chmod(env.MountPath(rel), newPerm))

		// Verify: physical dir unchanged, mount shows new perm.
		fiPhysical, err := os.Stat(physical)
		require.NoError(t, err)
		require.Equal(t, permBefore, fiPhysical.Mode().Perm())

		fiMount, err := os.Stat(env.MountPath(rel))
		require.NoError(t, err)
		require.Equal(t, newPerm, fiMount.Mode().Perm())

		// Action: prune applies the deferred chmod.
		mustRunPFS(t, env, "prune", env.MountName)

		// Verify: physical dir now has the new perm.
		fiPhysicalAfter, err := os.Stat(physical)
		require.NoError(t, err)
		require.Equal(t, newPerm, fiPhysicalAfter.Mode().Perm())

		// Verify: mount still shows the new perm.
		fiMountAfter, err := os.Stat(env.MountPath(rel))
		require.NoError(t, err)
		require.Equal(t, newPerm, fiMountAfter.Mode().Perm())
	})
}

// TestPrune_allFlag_shouldProcessAllMounts verifies `pfs prune --all` iterates over
// all mounts in the config and processes their events.
func TestPrune_allFlag_shouldProcessAllMounts(t *testing.T) {
	cfg := createIndexedCfg()

	withMountedFS(t, cfg, func(env *MountedFS) {
		rel := "prune-flow/all-flag/a.txt"
		content := []byte("hello-all-flag")
		env.MustCreateFileInStoragePath(t, content, "hdd1", rel)
		mustRunPFS(t, env, "index", env.MountName)
		env.MustRemoveFileInMountPoint(t, rel)
		waitForEventsFile(t, env, 2*time.Second)

		require.FileExists(t, env.StoragePath("hdd1", rel))

		// Action: prune with --all instead of specifying mount name.
		mustRunPFS(t, env, "prune", "--all")

		// Verify: file physically deleted.
		require.NoFileExists(t, env.StoragePath("hdd1", rel))

		// Verify: events log truncated.
		eventsPath := filepath.Join(env.StateDir, env.MountName, "events.ndjson")
		st, err := os.Stat(eventsPath)
		require.NoError(t, err)
		require.Equal(t, int64(0), st.Size())
	})
}

// TestPrune_shouldApplyChainRenames verifies two chained renames (A→B, B→C) before prune
// both get applied correctly: physical file ends up at C, DB path and real_path both equal C.
func TestPrune_shouldApplyChainRenames(t *testing.T) {
	cfg := createIndexedCfg()

	withMountedFS(t, cfg, func(env *MountedFS) {
		relA := "prune-flow/chain-rename/a.txt"
		relB := "prune-flow/chain-rename/b.txt"
		relC := "prune-flow/chain-rename/c.txt"
		content := []byte("hello-chain-rename")
		env.MustCreateFileInStoragePath(t, content, "hdd1", relA)
		mustRunPFS(t, env, "index", env.MountName)

		// Action: chain renames A→B→C through mount before prune runs.
		env.MustRenameFileInMountPoint(t, relA, relB)
		env.MustRenameFileInMountPoint(t, relB, relC)

		// Verify pre-prune: physical file still at A, mount only shows C.
		require.FileExists(t, env.StoragePath("hdd1", relA))
		require.NoFileExists(t, env.StoragePath("hdd1", relB))
		require.NoFileExists(t, env.StoragePath("hdd1", relC))

		got := env.MustReadFileInMountPoint(t, relC)
		require.Equal(t, content, got)
		require.False(t, env.FileExistsInMountPoint(relA))
		require.False(t, env.FileExistsInMountPoint(relB))

		waitForEventsFile(t, env, 2*time.Second)
		evs := readAllEvents(t, env)
		require.Len(t, evs, 2, "expected 2 RENAME events")

		// Action: prune processes both RENAME events in order.
		mustRunPFS(t, env, "prune", env.MountName)

		// Verify post-prune: physical file at C, A and B gone.
		require.NoFileExists(t, env.StoragePath("hdd1", relA))
		require.NoFileExists(t, env.StoragePath("hdd1", relB))
		require.FileExists(t, env.StoragePath("hdd1", relC))
		gotFinal := env.MustReadFileInMountPoint(t, relC)
		require.Equal(t, content, gotFinal)

		// Verify DB: path=C, real_path=C (no pending rename left).
		db := openIndexDB(t, env)
		ctx, cancel := context.WithTimeout(context.Background(), 2*time.Second)
		defer cancel()

		var gotPath, gotRealPath string
		err := db.QueryRowContext(ctx,
			`SELECT path, real_path FROM files WHERE storage_id = ? AND path = ? AND deleted = 0 LIMIT 1;`,
			"hdd1", relC,
		).Scan(&gotPath, &gotRealPath)
		require.NoError(t, err)
		require.Equal(t, relC, gotPath)
		require.Equal(t, gotPath, gotRealPath)

		// Old paths must not exist in the DB.
		var one int
		err = db.QueryRowContext(ctx,
			`SELECT 1 FROM files WHERE storage_id = ? AND path = ? AND deleted = 0 LIMIT 1;`,
			"hdd1", relA,
		).Scan(&one)
		require.ErrorIs(t, err, sql.ErrNoRows)
		err = db.QueryRowContext(ctx,
			`SELECT 1 FROM files WHERE storage_id = ? AND path = ? AND deleted = 0 LIMIT 1;`,
			"hdd1", relB,
		).Scan(&one)
		require.ErrorIs(t, err, sql.ErrNoRows)
	})
}

// TestFUSE_Unlink_indexed_shouldNotSucceedWhenEventlogAppendFails verifies that FUSE does not
// return success when eventlog.Append fails during an indexed unlink. This is a regression test
// for the fix that changed silent error swallowing to explicit EIO.
//
// Note: MarkDeleted commits deleted=1 to the DB before Append runs. If Append fails, the FUSE
// handler returns EIO, but the kernel may revalidate the dentry (seeing deleted=1 via LOOKUP)
// and report ENOENT instead. Either way, the operation does not succeed silently.
func TestFUSE_Unlink_indexed_shouldNotSucceedWhenEventlogAppendFails(t *testing.T) {
	cfg := createIndexedCfg()

	withMountedFS(t, cfg, func(env *MountedFS) {
		rel := "prune-flow/append-fail/a.txt"
		content := []byte("hello-append-fail")
		env.MustCreateFileInStoragePath(t, content, "hdd1", rel)
		mustRunPFS(t, env, "index", env.MountName)

		// Sabotage: replace events.ndjson with a directory so Append fails with EISDIR.
		eventsPath := filepath.Join(env.StateDir, env.MountName, "events.ndjson")
		_ = os.Remove(eventsPath)
		require.NoError(t, os.MkdirAll(eventsPath, 0o755))
		t.Cleanup(func() { _ = os.RemoveAll(eventsPath) })

		// Action: unlink through the mount should fail because eventlog.Append cannot write.
		// The exact errno may vary (EIO from handler, or ENOENT from kernel dentry revalidation
		// after MarkDeleted committed deleted=1 to the DB).
		err := os.Remove(env.MountPath(rel))
		require.Error(t, err, "expected error when eventlog.Append fails")

		// Verify: physical file still exists on disk (no physical deletion happened).
		require.FileExists(t, env.StoragePath("hdd1", rel))
	})
}
