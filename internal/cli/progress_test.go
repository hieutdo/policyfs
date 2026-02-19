package cli

import (
	"testing"

	"github.com/stretchr/testify/require"
)

func TestRenderProgressBar_shouldDrawFilledAndEmpty(t *testing.T) {
	bar := renderProgressBar(10, 5, 10)
	require.Equal(t, "█████░░░░░", bar)
}

func TestRenderProgressBar_shouldHandleZeroTotal(t *testing.T) {
	bar := renderProgressBar(10, 0, 0)
	require.Equal(t, "░░░░░░░░░░", bar)
}

func TestRenderProgressBar_shouldClampDoneAboveTotal(t *testing.T) {
	bar := renderProgressBar(10, 15, 10)
	require.Equal(t, "██████████", bar)
}

func TestTruncateForProgress_shouldEllipsizeLongPaths(t *testing.T) {
	s := truncateForProgress("abcdefghijklmnopqrstuvwxyz", 10)
	require.Len(t, s, 10)
	require.Contains(t, s, "...")
}

func TestTruncateForProgress_shouldReturnShortPathsUnchanged(t *testing.T) {
	s := truncateForProgress("short", 60)
	require.Equal(t, "short", s)
}
