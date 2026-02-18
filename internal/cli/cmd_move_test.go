package cli

import (
	"os"
	"path/filepath"
	"strings"
	"testing"

	"github.com/hieutdo/policyfs/internal/mover"
	"github.com/stretchr/testify/require"
)

// TestMove_printMoveHeader_shouldIncludeMountJobAndDryRun verifies the header includes mount, job name, and dry-run marker.
func TestMove_printMoveHeader_shouldIncludeMountJobAndDryRun(t *testing.T) {
	var sb strings.Builder
	printMoveHeader(&sb, "media", mover.Opts{Job: "archive", DryRun: true})

	out := sb.String()
	require.Contains(t, out, "pfs move: mount=media")
	require.Contains(t, out, "job=archive")
	require.Contains(t, out, "dry-run")
}

// TestMove_printMoveHeader_debug_shouldIncludeDebugMarker verifies the header includes a debug marker.
func TestMove_printMoveHeader_debug_shouldIncludeDebugMarker(t *testing.T) {
	var sb strings.Builder
	printMoveHeader(&sb, "media", mover.Opts{Debug: true})
	out := sb.String()
	require.Contains(t, out, "pfs move: mount=media")
	require.Contains(t, out, "debug=ON")
}

// TestMove_printMoveSummary_shouldIncludeTotalsAndWarnings verifies the summary prints per-job lines, totals, and warnings.
func TestMove_printMoveSummary_shouldIncludeTotalsAndWarnings(t *testing.T) {
	var sb strings.Builder
	res := mover.Result{
		Jobs: []mover.JobResult{
			{Name: "archive", FilesMoved: 2, BytesMoved: 2048, FilesErrored: 1, DurationMS: 1000},
		},
		TotalFilesMoved: 2,
		TotalBytesMoved: 2048,
		TotalBytesFreed: 1024,
		TotalDurationMS: 1000,
	}
	printMoveSummary(&sb, res, []string{"archive/ssd1: boom"})
	out := sb.String()

	require.Contains(t, out, "Summary")
	require.Contains(t, out, "archive")
	require.Contains(t, out, "moved")
	require.Contains(t, out, "Errors")
	require.Contains(t, out, "Elapsed")
	require.Contains(t, out, "Warnings (1):")
	require.Contains(t, out, "archive/ssd1: boom")
}

// TestMove_invalidProgressValue_shouldReturnUsage verifies invalid --progress values are rejected.
func TestMove_invalidProgressValue_shouldReturnUsage(t *testing.T) {
	code, _, stderr := runCLI(t, []string{"move", "media", "--progress=nope"})
	require.Equal(t, ExitUsage, code)
	require.Contains(t, stderr, "error: invalid arguments")
	require.Contains(t, stderr, "invalid value for --progress")
}

// TestMove_negativeLimit_shouldReturnUsage verifies negative --limit is rejected.
func TestMove_negativeLimit_shouldReturnUsage(t *testing.T) {
	code, _, stderr := runCLI(t, []string{"move", "media", "--limit", "-1"})
	require.Equal(t, ExitUsage, code)
	require.Contains(t, stderr, "error: invalid arguments")
	require.Contains(t, stderr, "--limit must be >= 0")
}

// TestMove_unknownJob_shouldReturnUsage verifies specifying an unknown job returns ExitUsage.
func TestMove_unknownJob_shouldReturnUsage(t *testing.T) {
	src := t.TempDir()
	dst := t.TempDir()

	cfg := writeTempConfig(t, `
mounts:
  media:
    mountpoint: "/mnt/pfs/media"
    storage_paths:
      - id: "ssd1"
        path: "`+src+`"
        indexed: false
      - id: "hdd1"
        path: "`+dst+`"
        indexed: false
    routing_rules:
      - match: "**"
        targets: ["ssd1"]
    mover:
      enabled: true
      jobs:
        - name: "archive"
          trigger:
            type: manual
          source:
            paths: ["ssd1"]
            patterns: ["library/**"]
          destination:
            paths: ["hdd1"]
            policy: first_found
          delete_source: true
          verify: false
`)

	code, _, stderr := runCLI(t, []string{"--config", cfg, "move", "media", "--job", "nope", "--progress=off"})
	require.Equal(t, ExitUsage, code)
	require.Contains(t, stderr, "error: invalid arguments")
	require.Contains(t, stderr, "mover job \"nope\" not found")
}

// TestMove_success_shouldMoveOneFileAndPrintSummary verifies a basic move run copies file, deletes source, and prints a summary.
func TestMove_success_shouldMoveOneFileAndPrintSummary(t *testing.T) {
	src := t.TempDir()
	dst := t.TempDir()

	rel := filepath.Join(src, "library", "a.txt")
	require.NoError(t, os.MkdirAll(filepath.Dir(rel), 0o755))
	require.NoError(t, os.WriteFile(rel, []byte("hello"), 0o644))

	cfg := writeTempConfig(t, `
mounts:
  media:
    mountpoint: "/mnt/pfs/media"
    storage_paths:
      - id: "ssd1"
        path: "`+src+`"
        indexed: false
      - id: "hdd1"
        path: "`+dst+`"
        indexed: false
    routing_rules:
      - match: "**"
        targets: ["ssd1"]
    mover:
      enabled: true
      jobs:
        - name: "archive"
          trigger:
            type: manual
          source:
            paths: ["ssd1"]
            patterns: ["library/**"]
          destination:
            paths: ["hdd1"]
            policy: first_found
          delete_source: true
          verify: false
`)

	code, stdout, stderr := runCLI(t, []string{"--config", cfg, "move", "media", "--job", "archive", "--progress=off"})
	require.Equal(t, ExitOK, code)
	require.Empty(t, stderr)
	require.Contains(t, stdout, "pfs move: mount=media")
	require.Contains(t, stdout, "Summary")
	require.Contains(t, stdout, "Elapsed")

	require.NoFileExists(t, filepath.Join(src, "library", "a.txt"))
	require.FileExists(t, filepath.Join(dst, "library", "a.txt"))
}

// TestMove_progressPlain_shouldPrintCopyingAndDone verifies --progress=plain prints one Copying line and one Done line.
func TestMove_progressPlain_shouldPrintCopyingAndDone(t *testing.T) {
	src := t.TempDir()
	dst := t.TempDir()

	rel := filepath.Join(src, "library", "a.txt")
	require.NoError(t, os.MkdirAll(filepath.Dir(rel), 0o755))
	require.NoError(t, os.WriteFile(rel, []byte("hello"), 0o644))

	cfg := writeTempConfig(t, `
mounts:
  media:
    mountpoint: "/mnt/pfs/media"
    storage_paths:
      - id: "ssd1"
        path: "`+src+`"
        indexed: false
      - id: "hdd1"
        path: "`+dst+`"
        indexed: false
    routing_rules:
      - match: "**"
        targets: ["ssd1"]
    mover:
      enabled: true
      jobs:
        - name: "archive"
          trigger:
            type: manual
          source:
            paths: ["ssd1"]
            patterns: ["library/**"]
          destination:
            paths: ["hdd1"]
            policy: first_found
          delete_source: true
          verify: false
`)

	code, stdout, stderr := runCLI(t, []string{"--config", cfg, "move", "media", "--job", "archive", "--limit", "1", "--force", "--progress=plain"})
	require.Equal(t, ExitOK, code)
	require.Empty(t, stderr)
	require.Contains(t, stdout, "[1/1]  Copying  library/a.txt")
	require.Contains(t, stdout, "[1/1]  Done  library/a.txt")
}

// TestMove_progressAuto_nonTTYShouldNotSpam verifies non-TTY auto progress does not emit carriage returns or TTY bars.
func TestMove_progressAuto_nonTTYShouldNotSpam(t *testing.T) {
	src := t.TempDir()
	dst := t.TempDir()

	rel := filepath.Join(src, "library", "a.txt")
	require.NoError(t, os.MkdirAll(filepath.Dir(rel), 0o755))
	require.NoError(t, os.WriteFile(rel, []byte("hello"), 0o644))

	cfg := writeTempConfig(t, `
mounts:
  media:
    mountpoint: "/mnt/pfs/media"
    storage_paths:
      - id: "ssd1"
        path: "`+src+`"
        indexed: false
      - id: "hdd1"
        path: "`+dst+`"
        indexed: false
    routing_rules:
      - match: "**"
        targets: ["ssd1"]
    mover:
      enabled: true
      jobs:
        - name: "archive"
          trigger:
            type: manual
          source:
            paths: ["ssd1"]
            patterns: ["library/**"]
          destination:
            paths: ["hdd1"]
            policy: first_found
          delete_source: true
          verify: false
`)

	// Note: runCLI captures stdout via a pipe (non-TTY), so --progress=auto should use plain output.
	code, stdout, stderr := runCLI(t, []string{"--config", cfg, "move", "media", "--job", "archive", "--limit", "1", "--force"})
	require.Equal(t, ExitOK, code)
	require.Empty(t, stderr)
	require.NotContains(t, stdout, "\r")
	require.Contains(t, stdout, "Copying")
	require.Contains(t, stdout, "Done")
}

// TestMove_quiet_shouldSuppressAllOutput verifies --quiet suppresses all stdout output on success.
func TestMove_quiet_shouldSuppressAllOutput(t *testing.T) {
	src := t.TempDir()
	dst := t.TempDir()

	rel := filepath.Join(src, "library", "a.txt")
	require.NoError(t, os.MkdirAll(filepath.Dir(rel), 0o755))
	require.NoError(t, os.WriteFile(rel, []byte("hello"), 0o644))

	cfg := writeTempConfig(t, `
mounts:
  media:
    mountpoint: "/mnt/pfs/media"
    storage_paths:
      - id: "ssd1"
        path: "`+src+`"
        indexed: false
      - id: "hdd1"
        path: "`+dst+`"
        indexed: false
    routing_rules:
      - match: "**"
        targets: ["ssd1"]
    mover:
      enabled: true
      jobs:
        - name: "archive"
          trigger:
            type: manual
          source:
            paths: ["ssd1"]
            patterns: ["library/**"]
          destination:
            paths: ["hdd1"]
            policy: first_found
          delete_source: true
          verify: false
`)

	code, stdout, stderr := runCLI(t, []string{"--config", cfg, "move", "media", "--job", "archive", "--quiet"})
	require.Equal(t, ExitOK, code)
	require.Empty(t, stderr)
	require.Empty(t, stdout)

	require.NoFileExists(t, filepath.Join(src, "library", "a.txt"))
	require.FileExists(t, filepath.Join(dst, "library", "a.txt"))
}
