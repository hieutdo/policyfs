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

// TestPruneOneshot_Busy_shouldReturnExitBusy verifies ErrBusy maps to ExitBusy in the CLI wrapper.
func TestPruneOneshot_Busy_shouldReturnExitBusy(t *testing.T) {
	runtimeDir := filepath.Join(t.TempDir(), "runtime")
	require.NoError(t, os.MkdirAll(runtimeDir, 0o755))
	t.Setenv("PFS_RUNTIME_DIR", runtimeDir)
	t.Setenv("PFS_STATE_DIR", filepath.Join(t.TempDir(), "state"))

	mount := "media"
	lk, err := lock.AcquireMountLock(mount, config.DefaultJobLockFile)
	require.NoError(t, err)
	t.Cleanup(func() { _ = lk.Close() })

	_, err = pruneOneshot(context.Background(), mount, nil, prune.Opts{}, prune.Hooks{})
	require.Error(t, err)

	var ce *CLIError
	require.True(t, errors.As(err, &ce))
	require.Equal(t, ExitBusy, ce.Code)
}
