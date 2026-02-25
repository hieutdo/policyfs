package fuse

import (
	"context"
	"sync"
	"testing"

	"github.com/hieutdo/policyfs/internal/daemonctl"
	"github.com/stretchr/testify/require"
)

func fid(storage string, dev, ino uint64) daemonctl.OpenFileID {
	return daemonctl.OpenFileID{StorageID: storage, Dev: dev, Ino: ino}
}

// --- NewOpenTracker ---

func TestNewOpenTracker_shouldReturnNonNil(t *testing.T) {
	tr := NewOpenTracker()
	require.NotNil(t, tr)
}

// --- Inc / Dec ---

func TestOpenTracker_Inc_nil_shouldNotPanic(t *testing.T) {
	var tr *OpenTracker
	tr.Inc(fid("s", 1, 1), false) // must not panic
}

func TestOpenTracker_Dec_nil_shouldNotPanic(t *testing.T) {
	var tr *OpenTracker
	tr.Dec(fid("s", 1, 1), false) // must not panic
}

func TestOpenTracker_IncDec_readOnly_shouldTrackCount(t *testing.T) {
	tr := NewOpenTracker()
	id := fid("ssd1", 1, 100)

	tr.Inc(id, false)
	tr.Inc(id, false)

	stats, err := tr.OpenCounts(context.Background(), []daemonctl.OpenFileID{id})
	require.NoError(t, err)
	require.Len(t, stats, 1)
	require.Equal(t, int64(2), stats[0].OpenCount)
	require.Equal(t, int64(0), stats[0].OpenWriteCount)

	tr.Dec(id, false)
	stats, err = tr.OpenCounts(context.Background(), []daemonctl.OpenFileID{id})
	require.NoError(t, err)
	require.Equal(t, int64(1), stats[0].OpenCount)
}

func TestOpenTracker_IncDec_write_shouldTrackWriteCount(t *testing.T) {
	tr := NewOpenTracker()
	id := fid("ssd1", 1, 100)

	tr.Inc(id, true)
	tr.Inc(id, false)

	stats, err := tr.OpenCounts(context.Background(), []daemonctl.OpenFileID{id})
	require.NoError(t, err)
	require.Equal(t, int64(2), stats[0].OpenCount)
	require.Equal(t, int64(1), stats[0].OpenWriteCount)

	tr.Dec(id, true)
	stats, err = tr.OpenCounts(context.Background(), []daemonctl.OpenFileID{id})
	require.NoError(t, err)
	require.Equal(t, int64(1), stats[0].OpenCount)
	require.Equal(t, int64(0), stats[0].OpenWriteCount)
}

func TestOpenTracker_Dec_toZero_shouldRemoveEntry(t *testing.T) {
	tr := NewOpenTracker()
	id := fid("ssd1", 1, 100)

	tr.Inc(id, false)
	tr.Dec(id, false)

	stats, err := tr.OpenCounts(context.Background(), []daemonctl.OpenFileID{id})
	require.NoError(t, err)
	require.Equal(t, int64(0), stats[0].OpenCount)
}

func TestOpenTracker_Dec_unknownID_shouldNotPanic(t *testing.T) {
	tr := NewOpenTracker()
	tr.Dec(fid("ssd1", 1, 999), false) // must not panic
}

func TestOpenTracker_Dec_belowZero_shouldClampToZero(t *testing.T) {
	tr := NewOpenTracker()
	id := fid("ssd1", 1, 100)

	tr.Inc(id, false)
	tr.Dec(id, false)
	tr.Dec(id, false) // extra dec on non-existent entry

	stats, err := tr.OpenCounts(context.Background(), []daemonctl.OpenFileID{id})
	require.NoError(t, err)
	require.Equal(t, int64(0), stats[0].OpenCount)
}

// --- OpenCounts ---

func TestOpenCounts_nil_shouldReturnNil(t *testing.T) {
	var tr *OpenTracker
	stats, err := tr.OpenCounts(context.Background(), []daemonctl.OpenFileID{fid("s", 1, 1)})
	require.NoError(t, err)
	require.Nil(t, stats)
}

func TestOpenCounts_multipleIDs_shouldReturnAll(t *testing.T) {
	tr := NewOpenTracker()
	id1 := fid("ssd1", 1, 100)
	id2 := fid("ssd1", 1, 200)
	id3 := fid("hdd1", 2, 300)

	tr.Inc(id1, false)
	tr.Inc(id1, true)
	tr.Inc(id2, false)
	// id3 not incremented

	stats, err := tr.OpenCounts(context.Background(), []daemonctl.OpenFileID{id1, id2, id3})
	require.NoError(t, err)
	require.Len(t, stats, 3)

	require.Equal(t, int64(2), stats[0].OpenCount)
	require.Equal(t, int64(1), stats[0].OpenWriteCount)
	require.Equal(t, int64(1), stats[1].OpenCount)
	require.Equal(t, int64(0), stats[1].OpenWriteCount)
	require.Equal(t, int64(0), stats[2].OpenCount)
}

func TestOpenCounts_emptySlice_shouldReturnEmpty(t *testing.T) {
	tr := NewOpenTracker()
	stats, err := tr.OpenCounts(context.Background(), []daemonctl.OpenFileID{})
	require.NoError(t, err)
	require.Empty(t, stats)
}

// --- Concurrency ---

func TestOpenTracker_concurrent_shouldNotRace(t *testing.T) {
	tr := NewOpenTracker()
	id := fid("ssd1", 1, 42)
	const N = 100

	// Phase 1: concurrent Inc.
	var wg sync.WaitGroup
	wg.Add(N)
	for range N {
		go func() {
			defer wg.Done()
			tr.Inc(id, true)
		}()
	}
	wg.Wait()

	stats, err := tr.OpenCounts(context.Background(), []daemonctl.OpenFileID{id})
	require.NoError(t, err)
	require.Equal(t, int64(N), stats[0].OpenCount)

	// Phase 2: concurrent Dec.
	wg.Add(N)
	for range N {
		go func() {
			defer wg.Done()
			tr.Dec(id, true)
		}()
	}
	wg.Wait()

	stats, err = tr.OpenCounts(context.Background(), []daemonctl.OpenFileID{id})
	require.NoError(t, err)
	require.Equal(t, int64(0), stats[0].OpenCount)
}

func TestOpenTracker_concurrent_multipleIDs_shouldNotRace(t *testing.T) {
	tr := NewOpenTracker()
	ids := []daemonctl.OpenFileID{
		fid("ssd1", 1, 1),
		fid("ssd1", 1, 2),
		fid("hdd1", 2, 1),
	}
	const N = 50

	var wg sync.WaitGroup
	wg.Add(N * len(ids) * 2)

	for _, id := range ids {
		for range N {
			go func() {
				defer wg.Done()
				tr.Inc(id, false)
			}()
			go func() {
				defer wg.Done()
				tr.Dec(id, false)
			}()
		}
	}
	wg.Wait()
	// Just checking for races (no assertion needed beyond no panic/deadlock).
}
