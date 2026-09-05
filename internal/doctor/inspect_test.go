package doctor

import (
	"context"
	"errors"
	"io"
	"os"
	"path/filepath"
	"syscall"
	"testing"
	"time"

	"github.com/hieutdo/policyfs/internal/config"
	"github.com/hieutdo/policyfs/internal/eventlog"
	"github.com/hieutdo/policyfs/internal/indexdb"
	"github.com/stretchr/testify/require"
)

// fakePendingEventReader is a deterministic in-memory pendingEventReader for testing.
type fakePendingEventReader struct {
	lines [][]byte
	i     int
	err   error
}

// Next implements pendingEventReader.
func (r *fakePendingEventReader) Next() (line []byte, nextOffset int64, err error) {
	if r == nil {
		return nil, 0, io.EOF
	}
	if r.err != nil {
		return nil, 0, r.err
	}
	if r.i >= len(r.lines) {
		return nil, 0, io.EOF
	}
	b := r.lines[r.i]
	r.i++
	return b, int64(r.i), nil
}

// Close implements pendingEventReader.
func (r *fakePendingEventReader) Close() error { return nil }

// TestInspectFile_shouldSkipEveryIndexedStorageByDefault verifies one indexed hit does not stat other indexed disks.
func TestInspectFile_shouldSkipEveryIndexedStorageByDefault(t *testing.T) {
	stateDir := filepath.Join(t.TempDir(), "state")
	require.NoError(t, os.MkdirAll(stateDir, 0o755))
	t.Setenv(config.EnvStateDir, stateDir)

	db, err := indexdb.Open("media")
	require.NoError(t, err)

	size := int64(123)
	mtime := time.Now().Unix()
	mode := uint32(syscall.S_IFREG | 0o644)
	require.NoError(t, db.UpsertFile(context.Background(), "hdd4", "library/movie.mkv", false, &size, mtime, mode, 1000, 1000))
	require.NoError(t, db.Close())

	storageIDs := []string{"hdd1", "hdd2", "hdd3", "hdd4"}
	storagePaths := make([]config.StoragePath, 0, len(storageIDs))
	for _, storageID := range storageIDs {
		root := filepath.Join(t.TempDir(), "disk")
		require.NoError(t, os.WriteFile(root, nil, 0o644))
		storagePaths = append(storagePaths, config.StoragePath{ID: storageID, Path: root, Indexed: true})
	}

	report, err := InspectFile("media", "library/movie.mkv", config.MountConfig{StoragePaths: storagePaths}, false)
	require.NoError(t, err)
	require.Len(t, report.Storages, 4)
	for _, storage := range report.Storages {
		require.True(t, storage.DiskStatSkipped, "expected disk stat skipped for %s", storage.StorageID)
		require.Nil(t, storage.DiskExists, "expected no disk result for %s", storage.StorageID)
		require.Empty(t, storage.DiskError, "expected no disk error for %s", storage.StorageID)
	}
	require.True(t, report.Storages[3].InIndex)
}

// TestFindPendingEventsFromReader_shouldFilterMatchingEvents verifies findPendingEventsFromReader returns
// only events that match the requested path (including renames where either old/new match).
func TestFindPendingEventsFromReader_shouldFilterMatchingEvents(t *testing.T) {
	wantPath := "library/movies/a.mkv"

	r := &fakePendingEventReader{lines: [][]byte{
		[]byte(`{"type":"DELETE","storage_id":"hdd1","path":"library/movies/a.mkv","ts":1700000000}`),
		[]byte(`{"type":"DELETE","storage_id":"hdd1","path":"library/movies/b.mkv","ts":1700000001}`),
		[]byte(`{"type":"RENAME","storage_id":"hdd1","old_path":"library/movies/a.mkv","new_path":"library/movies/a2.mkv","ts":1700000002}`),
		[]byte(`{"type":"RENAME","storage_id":"hdd1","old_path":"library/movies/c.mkv","new_path":"library/movies/a.mkv","ts":1700000003}`),
		[]byte(`{"type":"SETATTR","storage_id":"hdd1","path":"library/movies/a.mkv","ts":1700000004}`),
		[]byte(`{not json}`),
	}}

	got, err := findPendingEventsFromReader(r, wantPath)
	require.NoError(t, err)
	require.Len(t, got, 4)

	require.Equal(t, eventlog.TypeDelete, got[0].Type)
	require.Equal(t, "hdd1", got[0].StorageID)
	require.Equal(t, wantPath, got[0].Path)

	require.Equal(t, eventlog.TypeRename, got[1].Type)
	require.Equal(t, "library/movies/a.mkv", got[1].OldPath)
	require.Equal(t, "library/movies/a2.mkv", got[1].NewPath)

	require.Equal(t, eventlog.TypeRename, got[2].Type)
	require.Equal(t, "library/movies/c.mkv", got[2].OldPath)
	require.Equal(t, "library/movies/a.mkv", got[2].NewPath)

	require.Equal(t, eventlog.TypeSetattr, got[3].Type)
	require.Equal(t, wantPath, got[3].Path)
}

// TestFindPendingEventsFromReader_shouldStopOnEOF verifies EOF stops the scan without error.
func TestFindPendingEventsFromReader_shouldStopOnEOF(t *testing.T) {
	r := &fakePendingEventReader{lines: [][]byte{
		[]byte(`{"type":"DELETE","storage_id":"hdd1","path":"x","ts":1}`),
	}}

	got, err := findPendingEventsFromReader(r, "x")
	require.NoError(t, err)
	require.Len(t, got, 1)
}

// TestFindPendingEventsFromReader_shouldReturnErrorOnReaderFailure verifies a non-EOF reader error
// is wrapped and returned.
func TestFindPendingEventsFromReader_shouldReturnErrorOnReaderFailure(t *testing.T) {
	r := &fakePendingEventReader{err: errors.New("boom")}

	got, err := findPendingEventsFromReader(r, "x")
	require.Error(t, err)
	require.Nil(t, got)
	require.ErrorIs(t, err, r.err)
	require.Same(t, r.err, errors.Unwrap(err))
}
