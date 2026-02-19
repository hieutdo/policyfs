package cli

import (
	"io"
	"os"
	"strings"

	"golang.org/x/sys/unix"
)

// renderProgressBar draws a fixed-width bar.
func renderProgressBar(width int, done int64, total int64) string {
	if width <= 0 {
		return ""
	}
	if total <= 0 {
		return strings.Repeat("░", width)
	}
	if done < 0 {
		done = 0
	}
	if done > total {
		done = total
	}

	filled := int(float64(width) * float64(done) / float64(total))
	if filled < 0 {
		filled = 0
	}
	if filled > width {
		filled = width
	}

	return strings.Repeat("█", filled) + strings.Repeat("░", width-filled)
}

// truncateForProgress truncates long paths for progress display.
func truncateForProgress(s string, max int) string {
	if max <= 0 {
		return ""
	}
	s = strings.TrimSpace(s)
	if len(s) <= max {
		return s
	}
	if max <= 3 {
		return s[:max]
	}
	keep := max - 3
	left := keep / 2
	right := keep - left
	if left <= 0 || right <= 0 {
		return "..." + s[len(s)-(max-3):]
	}
	return s[:left] + "..." + s[len(s)-right:]
}

// terminalWidth returns the column width of w if it is a terminal, or 0 if unknown.
func terminalWidth(w io.Writer) int {
	f, ok := w.(*os.File)
	if !ok {
		return 0
	}
	ws, err := unix.IoctlGetWinsize(int(f.Fd()), unix.TIOCGWINSZ)
	if err != nil || ws.Col == 0 {
		return 0
	}
	return int(ws.Col)
}

// isInteractiveWriter returns true when the writer is very likely a terminal.
func isInteractiveWriter(w io.Writer) bool {
	f, ok := w.(*os.File)
	if !ok {
		return false
	}
	fi, err := f.Stat()
	if err != nil {
		return false
	}
	return fi.Mode()&os.ModeCharDevice != 0
}
