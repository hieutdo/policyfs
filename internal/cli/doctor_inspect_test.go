package cli

import (
	"bytes"
	"context"
	"encoding/json"
	"errors"
	"io"
	"os"
	"path/filepath"
	"testing"
	"time"

	"github.com/hieutdo/policyfs/internal/config"
	"github.com/hieutdo/policyfs/internal/eventlog"
	"github.com/hieutdo/policyfs/internal/indexdb"
	"github.com/stretchr/testify/require"
)

// TestDoctor_FileInspect_NotInIndex verifies inspect shows "not found" when file is absent.
func TestDoctor_FileInspect_NotInIndex(t *testing.T) {
	src := t.TempDir()
	cfg := writeTempConfig(t, `
mounts:
  media:
    mountpoint: "`+src+`"
    storage_paths:
      - id: "ssd1"
        path: "`+src+`"
    routing_rules:
      - match: "**"
        targets: ["ssd1"]
`)

	code, stdout, stderr := runCLI(t, []string{"--config", cfg, "doctor", "media", "nonexistent.txt"})
	require.Equal(t, ExitOK, code)
	require.Empty(t, stderr)
	require.Contains(t, stdout, "virtual path: nonexistent.txt")
	require.Contains(t, stdout, "not found in any storage")
}

// TestDoctor_FileInspect_PathNormalization verifies leading/trailing slashes are normalized.
func TestDoctor_FileInspect_PathNormalization(t *testing.T) {
	src := t.TempDir()
	cfg := writeTempConfig(t, `
mounts:
  media:
    mountpoint: "`+src+`"
    storage_paths:
      - id: "ssd1"
        path: "`+src+`"
    routing_rules:
      - match: "**"
        targets: ["ssd1"]
`)

	code, stdout, _ := runCLI(t, []string{"--config", cfg, "doctor", "media", "/library/test.txt/"})
	require.Equal(t, ExitOK, code)
	require.Contains(t, stdout, "virtual path: library/test.txt")
}

// TestDoctor_FileInspect_InvalidMount verifies file inspect with unknown mount returns usage error.
func TestDoctor_FileInspect_InvalidMount(t *testing.T) {
	src := t.TempDir()
	cfg := writeTempConfig(t, `
mounts:
  media:
    mountpoint: "`+src+`"
    storage_paths:
      - id: "ssd1"
        path: "`+src+`"
    routing_rules:
      - match: "**"
        targets: ["ssd1"]
`)

	code, _, stderr := runCLI(t, []string{"--config", cfg, "doctor", "notreal", "test.txt"})
	require.Equal(t, ExitUsage, code)
	require.Contains(t, stderr, "mount \"notreal\" not found")
}

// TestDoctor_FileInspect_JSON verifies JSON output for file inspect.
func TestDoctor_FileInspect_JSON(t *testing.T) {
	src := t.TempDir()
	cfg := writeTempConfig(t, `
mounts:
  media:
    mountpoint: "`+src+`"
    storage_paths:
      - id: "ssd1"
        path: "`+src+`"
    routing_rules:
      - match: "**"
        targets: ["ssd1"]
`)

	code, stdout, stderr := runCLI(t, []string{"--config", cfg, "doctor", "media", "test.txt", "--json"})
	require.Equal(t, ExitOK, code)
	require.Empty(t, stderr)

	var env struct {
		Command string `json:"command"`
		OK      bool   `json:"ok"`
		Inspect struct {
			Mount string `json:"mount"`
			Path  string `json:"path"`
		} `json:"inspect"`
	}
	require.NoError(t, json.Unmarshal([]byte(stdout), &env))
	require.Equal(t, "doctor", env.Command)
	require.True(t, env.OK)
	require.Equal(t, "media", env.Inspect.Mount)
	require.Equal(t, "test.txt", env.Inspect.Path)
}

// --- Unit tests ---

// TestRunFileInspect_WithIndexedFile verifies runFileInspect returns correct data for an indexed file.
func TestRunFileInspect_WithIndexedFile(t *testing.T) {
	stateDir := t.TempDir()
	t.Setenv(config.EnvStateDir, stateDir)

	// Create an index DB and insert a file.
	db, err := indexdb.Open("media")
	require.NoError(t, err)

	_, err = db.SQL().Exec(`INSERT INTO files (storage_id, path, real_path, parent_dir, name, is_dir, size, mtime, mode, uid, gid, deleted, last_seen_run_id)
VALUES ('hdd1', 'library/movie.mkv', 'library/movie.mkv', 'library', 'movie.mkv', 0, 53687091200, 1706443140, 420, 1000, 1000, 0, 142);`)
	require.NoError(t, err)

	_, err = db.SQL().Exec(`INSERT INTO indexer_state (storage_id, current_run_id, last_completed) VALUES ('hdd1', 142, 1708300000);`)
	require.NoError(t, err)
	require.NoError(t, db.Close())

	storagePath := t.TempDir()
	mountCfg := config.MountConfig{
		StoragePaths: []config.StoragePath{
			{ID: "hdd1", Path: storagePath, Indexed: true},
		},
	}

	report, err := runFileInspect("media", "library/movie.mkv", mountCfg)
	require.NoError(t, err)
	require.Equal(t, "library/movie.mkv", report.Path)
	require.Len(t, report.Storages, 1)

	s := report.Storages[0]
	require.Equal(t, "hdd1", s.StorageID)
	require.True(t, s.InIndex)
	require.True(t, s.Indexed)
	require.NotNil(t, s.Size)
	require.Equal(t, int64(53687091200), *s.Size)
	require.NotNil(t, s.Deleted)
	require.Equal(t, 0, *s.Deleted)
	require.NotNil(t, s.LastSeenRunID)
	require.Equal(t, int64(142), *s.LastSeenRunID)
}

// TestRunFileInspect_RenamePending_shouldStatRealPath verifies runFileInspect stats disk using real_path
// when the index row is pending a physical rename (real_path != path).
func TestRunFileInspect_RenamePending_shouldStatRealPath(t *testing.T) {
	stateDir := t.TempDir()
	t.Setenv(config.EnvStateDir, stateDir)

	db, err := indexdb.Open("media")
	require.NoError(t, err)

	_, err = db.SQL().Exec(`INSERT INTO files (storage_id, path, real_path, parent_dir, name, is_dir, size, mtime, mode, uid, gid, deleted)
VALUES ('hdd1', 'library/new.txt', 'library/old.txt', 'library', 'new.txt', 0, 3, 1700000000, 420, 1000, 1000, 0);`)
	require.NoError(t, err)
	require.NoError(t, db.Close())

	storagePath := t.TempDir()
	require.NoError(t, os.MkdirAll(filepath.Join(storagePath, "library"), 0o755))
	require.NoError(t, os.WriteFile(filepath.Join(storagePath, "library", "old.txt"), []byte("hey"), 0o644))

	mountCfg := config.MountConfig{
		StoragePaths: []config.StoragePath{{ID: "hdd1", Path: storagePath, Indexed: true}},
	}

	report, err := runFileInspect("media", "library/new.txt", mountCfg)
	require.NoError(t, err)
	require.Len(t, report.Storages, 1)

	s := report.Storages[0]
	require.True(t, s.RenamePending)
	require.Equal(t, "library/old.txt", s.RealPath)
	require.Equal(t, filepath.Join(storagePath, "library", "old.txt"), s.PhysicalPath)
	require.NotNil(t, s.DiskExists)
	require.True(t, *s.DiskExists)
}

// TestRunFileInspect_NotIndexed verifies runFileInspect reports InIndex=false for missing files.
func TestRunFileInspect_NotIndexed(t *testing.T) {
	stateDir := t.TempDir()
	t.Setenv(config.EnvStateDir, stateDir)

	storagePath := t.TempDir()
	mountCfg := config.MountConfig{
		StoragePaths: []config.StoragePath{
			{ID: "hdd1", Path: storagePath, Indexed: true},
		},
	}

	report, err := runFileInspect("media", "missing.txt", mountCfg)
	require.NoError(t, err)
	require.Len(t, report.Storages, 1)
	require.False(t, report.Storages[0].InIndex)
}

// TestFindPendingEvents_MatchesPath verifies pending events are filtered by path.
func TestFindPendingEvents_MatchesPath(t *testing.T) {
	stateDir := t.TempDir()
	t.Setenv(config.EnvStateDir, stateDir)

	mount := "media"
	ts := time.Now().Unix()

	// Write events: one matching, two not matching.
	require.NoError(t, eventlog.Append(context.TODO(), mount, eventlog.DeleteEvent{Type: eventlog.TypeDelete, StorageID: "hdd1", Path: "target.txt", TS: ts}))
	require.NoError(t, eventlog.Append(context.TODO(), mount, eventlog.DeleteEvent{Type: eventlog.TypeDelete, StorageID: "hdd1", Path: "other.txt", TS: ts}))
	require.NoError(t, eventlog.Append(context.TODO(), mount, eventlog.RenameEvent{Type: eventlog.TypeRename, StorageID: "hdd1", OldPath: "target.txt", NewPath: "renamed.txt", TS: ts}))

	events, err := findPendingEvents(mount, "target.txt")
	require.NoError(t, err)
	require.Len(t, events, 2) // DELETE + RENAME
	require.Equal(t, eventlog.TypeDelete, events[0].Type)
	require.Equal(t, eventlog.TypeRename, events[1].Type)
}

// fakePendingEventReader is a test double for pendingEventReader.
type fakePendingEventReader struct {
	lines [][]byte
	err   error
	i     int
}

func (r *fakePendingEventReader) Next() ([]byte, int64, error) {
	if r.err != nil {
		return nil, 0, r.err
	}
	if r.i >= len(r.lines) {
		return nil, 0, io.EOF
	}
	line := r.lines[r.i]
	r.i++
	return line, int64(r.i), nil
}

func (r *fakePendingEventReader) Close() error { return nil }

// TestFindPendingEventsFromReader_NonEOFError_shouldReturnError verifies we do not treat non-EOF reader errors as EOF.
func TestFindPendingEventsFromReader_NonEOFError_shouldReturnError(t *testing.T) {
	r := &fakePendingEventReader{
		err: errors.New("boom"),
	}
	_, err := findPendingEventsFromReader(r, "target.txt")
	require.Error(t, err)
	require.Contains(t, err.Error(), "failed to read pending events")
}

// TestPrintFileInspect_NotFound verifies print output when file is not found.
func TestPrintFileInspect_NotFound(t *testing.T) {
	var buf bytes.Buffer
	report := fileInspectReport{
		Mount: "media",
		Path:  "missing.txt",
		Storages: []fileInspectStorage{
			{StorageID: "hdd1", PhysicalPath: "/mnt/hdd1/media/missing.txt", DiskExists: new(false)},
		},
	}
	printFileInspect(&buf, report)
	out := buf.String()
	require.Contains(t, out, "virtual path: missing.txt")
	require.Contains(t, out, "not found in any storage")
}

// TestPrintFileInspect_WithData verifies print output for indexed file.
func TestPrintFileInspect_WithData(t *testing.T) {
	var buf bytes.Buffer
	deleted := 0
	size := int64(1024)
	mtime := int64(1700000000)
	mode := uint32(0o100644)
	uid := uint32(1000)
	gid := uint32(1000)
	runID := int64(5)

	report := fileInspectReport{
		Mount: "media",
		Path:  "library/test.txt",
		Storages: []fileInspectStorage{
			{
				StorageID:     "hdd1",
				PhysicalPath:  "/mnt/hdd1/media/library/test.txt",
				Indexed:       true,
				InIndex:       true,
				Size:          &size,
				MTimeSec:      &mtime,
				Mode:          &mode,
				UID:           &uid,
				GID:           &gid,
				Deleted:       &deleted,
				LastSeenRunID: &runID,
				DiskExists:    new(true),
				DiskSize:      &size,
			},
		},
	}
	printFileInspect(&buf, report)
	out := buf.String()
	require.Contains(t, out, "virtual path: library/test.txt")
	require.Contains(t, out, "storage: hdd1 (indexed)")
	require.Contains(t, out, "real path: /mnt/hdd1/media/library/test.txt")
	require.Contains(t, out, "indexed: yes (run_id=5)")
	require.Contains(t, out, "deleted: no")
}

// TestPrintFileInspect_IndexedDirModeWithoutTypeBits_shouldRenderAsDir verifies directory mode is rendered with 'd'
// even if the stored mode only contains permission bits.
func TestPrintFileInspect_IndexedDirModeWithoutTypeBits_shouldRenderAsDir(t *testing.T) {
	var buf bytes.Buffer
	deleted := 0
	mtime := int64(1700000000)
	// Simulate older/buggy DB rows that only stored perms (no S_IFDIR).
	mode := uint32(0o775)
	uid := uint32(11000)
	gid := uint32(11000)
	runID := int64(5)

	report := fileInspectReport{
		Mount: "media",
		Path:  "library/Season 01",
		Storages: []fileInspectStorage{
			{
				StorageID:     "hdd2",
				PhysicalPath:  "/mnt/hdd2/media/library/Season 01",
				Indexed:       true,
				InIndex:       true,
				IsDir:         true,
				MTimeSec:      &mtime,
				Mode:          &mode,
				UID:           &uid,
				GID:           &gid,
				Deleted:       &deleted,
				LastSeenRunID: &runID,
			},
		},
	}

	printFileInspect(&buf, report)
	out := buf.String()
	require.Contains(t, out, "storage: hdd2 (indexed)")
	require.Contains(t, out, "indexed: yes (run_id=5)")
	require.Contains(t, out, "mode: drwxrwxr-x")
}

// TestDeletedLabel verifies the deletedLabel helper.
func TestDeletedLabel(t *testing.T) {
	require.Equal(t, "no", deletedLabel(0))
	require.Equal(t, "pending delete", deletedLabel(1))
	require.Equal(t, "stale", deletedLabel(2))
}
