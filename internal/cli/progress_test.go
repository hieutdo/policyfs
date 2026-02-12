package cli

import (
	"bytes"
	"testing"
	"time"

	"github.com/stretchr/testify/require"
)

// TestProgressTracker_Finish_shouldFlushFinal100Percent verifies we always print a final 100% state,
// even when OnItem updates are throttled by the print interval.
func TestProgressTracker_Finish_shouldFlushFinal100Percent(t *testing.T) {
	var buf bytes.Buffer

	p := NewProgressTracker(ProgressTrackerConfig{
		Writer: &buf, Label: "Indexing", Total: 3, Mode: "tty",
	})
	p.OnItem("hdd1: media/text1.txt")
	p.OnItem("hdd2: media/text2.txt")
	p.OnItem("hdd3: media/text3.txt")
	p.Finish()

	out := buf.String()
	require.Contains(t, out, "100% 3/3")
	require.Contains(t, out, "Current: hdd3: media/text3.txt")
}

// TestProgressTracker_TTY_shouldRewriteMultiLineRegion verifies the TTY renderer
// rewrites a fixed multi-line region using ANSI escape codes.
func TestProgressTracker_TTY_shouldRewriteMultiLineRegion(t *testing.T) {
	var buf bytes.Buffer

	p := NewProgressTracker(ProgressTrackerConfig{
		Writer: &buf, Label: "Moving", Total: 2, Mode: "tty",
	})

	p.OnItem("hdd1: media/text1.txt")
	p.lastPrint = time.Time{} // bypass throttle
	p.OnItem("hdd2: media/text2.txt")

	out := buf.String()
	require.Contains(t, out, "\x1b[4A")
}

// TestProgressTracker_Plain_shouldUseLabel verifies the label appears in plain mode output.
func TestProgressTracker_Plain_shouldUseLabel(t *testing.T) {
	var buf bytes.Buffer

	p := NewProgressTracker(ProgressTrackerConfig{
		Writer: &buf, Label: "Pruning", Total: 10, Mode: "plain",
	})
	p.OnItem("DELETE library/old.mkv")

	out := buf.String()
	require.Contains(t, out, "Pruning [")
}

// TestProgressTracker_NilSafe verifies nil receiver methods do not panic.
func TestProgressTracker_NilSafe(t *testing.T) {
	var p *ProgressTracker
	require.NotPanics(t, func() {
		p.OnItem("test")
		p.Finish()
	})
}
