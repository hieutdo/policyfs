package cli

import (
	"bytes"
	"testing"
	"time"

	"github.com/stretchr/testify/require"
)

// TestPlainIndexUI_shouldPrintProgress verifies plain output contains Indexing label and file paths.
func TestPlainIndexUI_shouldPrintProgress(t *testing.T) {
	var buf bytes.Buffer

	ui := startPlainIndexProgress(&buf, 3)

	ui.OnProgress("hdd1", "media/text1.txt", false)
	ui.lastPrint = time.Time{} // bypass throttle
	ui.OnProgress("hdd2", "media/text2.txt", false)
	ui.lastPrint = time.Time{}
	ui.OnProgress("hdd3", "media/text3.txt", false)
	ui.Finish()

	out := buf.String()
	require.Contains(t, out, "Indexing")
	require.Contains(t, out, "3/3 entries")
	require.Contains(t, out, "hdd3: media/text3.txt")
}

// TestMpbIndexUI_StartFinish_shouldNotPanic verifies the mpb UI can be created and torn down.
func TestMpbIndexUI_StartFinish_shouldNotPanic(t *testing.T) {
	var buf bytes.Buffer

	ui := startMpbIndexProgress(&buf, 3)
	ui.OnProgress("hdd1", "media/text1.txt", false)
	ui.OnProgress("hdd2", "media/text2.txt", false)
	ui.OnProgress("hdd3", "media/text3.txt", false)
	ui.Finish()

	require.NotEmpty(t, buf.String())
}

// TestPlainIndexUI_OnProgress_shouldCountDirs verifies directory entries count toward progress.
func TestPlainIndexUI_OnProgress_shouldCountDirs(t *testing.T) {
	var buf bytes.Buffer

	ui := startPlainIndexProgress(&buf, 2)

	ui.OnProgress("hdd1", "media/subdir", true)
	require.Equal(t, int64(1), ui.done)

	ui.OnProgress("hdd1", "media/text1.txt", false)
	require.Equal(t, int64(2), ui.done)
}
