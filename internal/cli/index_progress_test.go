package cli

import (
	"bytes"
	"testing"
	"time"

	"github.com/stretchr/testify/require"
)

// TestIndexProgressUI_Finish_shouldFlushFinal100Percent verifies we always print a final 100% state,
// even when OnProgress updates are throttled by the print interval.
func TestIndexProgressUI_Finish_shouldFlushFinal100Percent(t *testing.T) {
	var buf bytes.Buffer

	tracker := NewProgressTracker(ProgressTrackerConfig{
		Writer: &buf, Label: "Indexing", Total: 3, Mode: "tty", MinUpdates: 2,
	})
	ui := &indexProgressUI{tracker: tracker}

	ui.OnProgress("hdd1", "media/text1.txt", false)
	ui.OnProgress("hdd2", "media/text2.txt", false)
	ui.OnProgress("hdd3", "media/text3.txt", false)
	ui.Finish()

	out := buf.String()
	require.Contains(t, out, "100% 3/3")
	require.Contains(t, out, "File: hdd3: media/text3.txt")
}

// TestIndexProgressUI_TTY_shouldRewriteMultiLineRegion verifies the interactive renderer
// rewrites a fixed multi-line region (instead of relying on a single-line carriage return).
func TestIndexProgressUI_TTY_shouldRewriteMultiLineRegion(t *testing.T) {
	var buf bytes.Buffer

	tracker := NewProgressTracker(ProgressTrackerConfig{
		Writer: &buf, Label: "Indexing", Total: 2, Mode: "tty", MinUpdates: 2,
	})

	tracker.OnItem("hdd1: media/text1.txt")
	tracker.lastPrint = time.Time{} // bypass throttle
	tracker.OnItem("hdd2: media/text2.txt")

	out := buf.String()
	require.Contains(t, out, "\x1b[4A")
}

// TestIndexProgressUI_OnProgress_shouldCountDirs verifies directory entries count toward progress.
func TestIndexProgressUI_OnProgress_shouldCountDirs(t *testing.T) {
	var buf bytes.Buffer

	tracker := NewProgressTracker(ProgressTrackerConfig{
		Writer: &buf, Label: "Indexing", Total: 2, Mode: "plain", MinUpdates: 2,
	})
	ui := &indexProgressUI{tracker: tracker}

	ui.OnProgress("hdd1", "media/subdir", true)
	require.Equal(t, int64(1), tracker.done)

	ui.OnProgress("hdd1", "media/text1.txt", false)
	require.Equal(t, int64(2), tracker.done)
}
