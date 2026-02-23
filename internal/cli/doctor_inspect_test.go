package cli

import (
	"bytes"
	"testing"

	"github.com/hieutdo/policyfs/internal/doctor"
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

// TestPrintFileInspect_NotFound verifies print output when file is not found.
func TestPrintFileInspect_NotFound(t *testing.T) {
	var buf bytes.Buffer
	report := doctor.FileInspectReport{
		Mount: "media",
		Path:  "missing.txt",
		Storages: []doctor.FileInspectStorage{
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

	report := doctor.FileInspectReport{
		Mount: "media",
		Path:  "library/test.txt",
		Storages: []doctor.FileInspectStorage{
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

	report := doctor.FileInspectReport{
		Mount: "media",
		Path:  "library/Season 01",
		Storages: []doctor.FileInspectStorage{
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
