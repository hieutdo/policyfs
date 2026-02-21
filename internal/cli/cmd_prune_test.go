package cli

import (
	"bytes"
	"context"
	"errors"
	"os"
	"path/filepath"
	"testing"

	"github.com/hieutdo/policyfs/internal/config"
	"github.com/hieutdo/policyfs/internal/eventlog"
	"github.com/hieutdo/policyfs/internal/lock"
	"github.com/hieutdo/policyfs/internal/prune"
	"github.com/stretchr/testify/require"
)

// TestPruneCmd_Flags_shouldNotExposeVerbose verifies `pfs prune` does not expose a --verbose flag.
func TestPruneCmd_Flags_shouldNotExposeVerbose(t *testing.T) {
	cmd := newPruneCmd(nil)

	require.NotNil(t, cmd.Flags().Lookup("quiet"))
	require.Nil(t, cmd.Flags().Lookup("verbose"))
}

// TestPrintPruneAction_shouldPrintExpectedLines verifies action output matches the stable human format.
func TestPrintPruneAction_shouldPrintExpectedLines(t *testing.T) {
	var buf bytes.Buffer
	mount := "media"

	printPruneAction(&buf, mount, prune.VerboseEvent{Type: eventlog.TypeDelete, StorageID: "hdd1", Path: "a", Result: "ok"})
	printPruneAction(&buf, mount, prune.VerboseEvent{Type: eventlog.TypeRename, StorageID: "hdd1", OldPath: "a", NewPath: "b", Result: "skipped", DryRun: true})
	printPruneAction(&buf, mount, prune.VerboseEvent{Type: eventlog.TypeSetattr, StorageID: "hdd1", Path: "c", Result: "failed"})

	out := buf.String()
	require.Contains(t, out, "DELETE mount=media storage_id=hdd1 path=a result=ok")
	require.Contains(t, out, "RENAME mount=media storage_id=hdd1 old_path=a new_path=b result=skipped dry_run=true")
	require.Contains(t, out, "SETATTR mount=media storage_id=hdd1 path=c result=failed")
}

// TestPrintPruneSummary_shouldPrintSummaryAndWarnings verifies summary output includes warnings when present.
func TestPrintPruneSummary_shouldPrintSummaryAndWarnings(t *testing.T) {
	var buf bytes.Buffer
	printPruneSummary(&buf, prune.Summary{
		Mount:           "media",
		EventsProcessed: 3,
		EventsSucceeded: 1,
		EventsSkipped:   1,
		EventsFailed:    1,
		ByType: map[eventlog.Type]int64{
			eventlog.TypeDelete:  1,
			eventlog.TypeRename:  1,
			eventlog.TypeSetattr: 1,
			"UNKNOWN":            0,
		},
		DurationMS: 1200,
		Truncated:  true,
		Warnings:   []string{"w1", "w2"},
	}, false)

	out := buf.String()
	require.Contains(t, out, "Summary:\n")
	require.Contains(t, out, "Events: processed 3")
	require.Contains(t, out, "Events log: truncated=true")
	require.Contains(t, out, "By type: DELETE 1")
	require.Contains(t, out, "Warnings (2):")
	require.Contains(t, out, "- w1")
	require.Contains(t, out, "- w2")
}

// TestPrintPruneSummary_Quiet_shouldPrintNothing verifies `--quiet` suppresses summary output.
func TestPrintPruneSummary_Quiet_shouldPrintNothing(t *testing.T) {
	var buf bytes.Buffer
	printPruneSummary(&buf, prune.Summary{Mount: "media", EventsProcessed: 1}, true)
	require.Empty(t, buf.String())
}

// TestPrune_noEvents_shouldReturnNoChanges verifies a mount with no events returns ExitNoChanges.
func TestPrune_noEvents_shouldReturnNoChanges(t *testing.T) {
	src := t.TempDir()

	cfg := writeTempConfig(t, `
mounts:
  media:
    mountpoint: "/mnt/pfs/media"
    storage_paths:
      - id: "ssd1"
        path: "`+src+`"
        indexed: false
    routing_rules:
      - match: "**"
        targets: ["ssd1"]
`)

	code, stdout, stderr := runCLI(t, []string{"--config", cfg, "prune", "media"})
	require.Equal(t, ExitNoChanges, code)
	require.Empty(t, stderr)
	require.Contains(t, stdout, "pfs prune: mount=media")
	require.Contains(t, stdout, "nothing to prune")
}

// TestPrune_allNoEvents_shouldReturnNoChanges verifies --all with no events on any mount returns ExitNoChanges.
func TestPrune_allNoEvents_shouldReturnNoChanges(t *testing.T) {
	src1 := t.TempDir()
	src2 := t.TempDir()

	cfg := writeTempConfig(t, `
mounts:
  media:
    mountpoint: "/mnt/pfs/media"
    storage_paths:
      - id: "ssd1"
        path: "`+src1+`"
        indexed: false
    routing_rules:
      - match: "**"
        targets: ["ssd1"]
  photos:
    mountpoint: "/mnt/pfs/photos"
    storage_paths:
      - id: "ssd2"
        path: "`+src2+`"
        indexed: false
    routing_rules:
      - match: "**"
        targets: ["ssd2"]
`)

	code, stdout, stderr := runCLI(t, []string{"--config", cfg, "prune", "--all"})
	require.Equal(t, ExitNoChanges, code)
	require.Empty(t, stderr)
	require.Contains(t, stdout, "nothing to prune")
}

// TestPruneOneshot_Busy_shouldReturnExitBusy verifies ErrBusy maps to ExitBusy in the CLI wrapper.
func TestPruneOneshot_Busy_shouldReturnExitBusy(t *testing.T) {
	runtimeDir := filepath.Join(t.TempDir(), "runtime")
	require.NoError(t, os.MkdirAll(runtimeDir, 0o755))
	t.Setenv(config.EnvRuntimeDir, runtimeDir)
	t.Setenv(config.EnvStateDir, filepath.Join(t.TempDir(), "state"))

	mount := "media"
	lk, err := lock.AcquireMountLock(mount, config.DefaultJobLockFile)
	require.NoError(t, err)
	t.Cleanup(func() { _ = lk.Close() })

	_, err = pruneOneshot(context.Background(), mount, nil, prune.Opts{}, prune.Hooks{})
	require.Error(t, err)

	ce, ok := errors.AsType[*CLIError](err)
	require.True(t, ok)
	require.Equal(t, ExitBusy, ce.Code)
}
