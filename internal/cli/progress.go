package cli

import (
	"fmt"
	"io"
	"os"
	"strings"
	"time"

	"github.com/dustin/go-humanize"
)

const (
	progressBarWidth  = 20
	progressInterval  = 200 * time.Millisecond
	minUpdateInterval = 60 * time.Millisecond
	progressLines     = 4
)

// progressState is a snapshot of tracker state passed to renderers.
type progressState struct {
	label    string
	done     int64
	total    int64
	current  string
	elapsed  time.Duration
	finished bool
}

// ProgressTrackerConfig holds options for creating a ProgressTracker.
type ProgressTrackerConfig struct {
	Writer io.Writer
	Label  string // e.g. "Indexing", "Moving", "Pruning"
	Total  int64  // total item count (0 = indeterminate)
	Mode   string // "tty", "plain", or "auto"
	// MinUpdates ensures a minimum number of renders even if work finishes quickly.
	MinUpdates int
}

// ProgressTracker is a generic item-counting progress bar.
type ProgressTracker struct {
	w           io.Writer
	label       string
	total       int64
	startedAt   time.Time
	lastPrint   time.Time
	interactive bool

	minUpdates int
	updates    int

	done     int64
	current  string
	finished bool

	render func(w io.Writer, state progressState)
}

// NewProgressTracker creates a progress tracker and renders the initial (0%) state.
func NewProgressTracker(cfg ProgressTrackerConfig) *ProgressTracker {
	mode := cfg.Mode
	if mode == "" || mode == "auto" {
		if isInteractiveWriter(cfg.Writer) {
			mode = "tty"
		} else {
			mode = "plain"
		}
	}
	interactive := mode == "tty" && isInteractiveWriter(cfg.Writer)

	var render func(io.Writer, progressState)
	switch mode {
	case "tty":
		render = newTTYRenderer()
	default:
		render = renderPlain
	}

	p := &ProgressTracker{
		w:           cfg.Writer,
		label:       cfg.Label,
		total:       cfg.Total,
		startedAt:   time.Now(),
		interactive: interactive,
		minUpdates:  cfg.MinUpdates,
		render:      render,
	}
	p.flush()
	if p.interactive {
		p.lastPrint = time.Now()
	}
	return p
}

// OnItem records one completed item. The display string is shown as the "Current" label.
// Calls are throttled to progressInterval (200ms).
func (p *ProgressTracker) OnItem(display string) {
	if p == nil {
		return
	}
	p.done++
	p.current = display

	now := time.Now()
	interval := progressInterval
	if p.interactive && p.minUpdates > 0 && p.updates < p.minUpdates {
		interval = minUpdateInterval
	}
	if !p.lastPrint.IsZero() && now.Sub(p.lastPrint) < interval {
		return
	}
	p.lastPrint = now
	p.flush()
}

// Finish forces a final render at 100%.
func (p *ProgressTracker) Finish() {
	if p == nil {
		return
	}
	if p.total > 0 && p.done < p.total {
		p.done = p.total
	}
	if p.interactive && p.minUpdates > 0 && p.updates < p.minUpdates && !p.lastPrint.IsZero() {
		now := time.Now()
		delta := now.Sub(p.lastPrint)
		if delta > 0 && delta < minUpdateInterval {
			time.Sleep(minUpdateInterval - delta)
		}
	}
	p.finished = true
	p.flush()
}

// flush renders the current state.
func (p *ProgressTracker) flush() {
	if p == nil {
		return
	}
	elapsed := time.Since(p.startedAt)
	if elapsed <= 0 {
		elapsed = time.Millisecond
	}
	p.render(p.w, progressState{
		label:    p.label,
		done:     p.done,
		total:    p.total,
		current:  p.current,
		elapsed:  elapsed,
		finished: p.finished,
	})
	p.updates++
}

// computeProgressMetrics computes display values from a progress state snapshot.
func computeProgressMetrics(s progressState) (pct float64, bar string, speed float64, eta string) {
	if s.total > 0 {
		pct = float64(s.done) / float64(s.total) * 100
		if pct > 100 {
			pct = 100
		}
	} else if s.finished {
		pct = 100
	}

	bar = renderProgressBar(progressBarWidth, s.done, s.total)
	if s.total <= 0 && s.finished {
		bar = strings.Repeat("█", progressBarWidth)
	}

	speed = float64(s.done) / s.elapsed.Seconds()

	eta = "?"
	if s.total > 0 && speed > 0 {
		remaining := float64(s.total - s.done)
		if remaining < 0 {
			remaining = 0
		}
		etaDur := time.Duration(remaining / speed * float64(time.Second))
		eta = etaDur.Round(time.Second).String()
	}
	return
}

// clampDoneTotal ensures done does not exceed total for display.
func clampDoneTotal(done int64, total int64) (int64, int64) {
	if total > 0 && done > total {
		done = total
	}
	return done, total
}

// newTTYRenderer returns a multi-line ANSI renderer.
// The returned closure captures cursor-rewind state and rewrites a fixed region using ANSI escape codes.
func newTTYRenderer() func(io.Writer, progressState) {
	rendered := false

	return func(w io.Writer, s progressState) {
		pct, bar, speed, eta := computeProgressMetrics(s)
		doneDisp, totalDisp := clampDoneTotal(s.done, s.total)

		lines := []string{
			fmt.Sprintf("%s [%s] %3.0f%% %s/%s", s.label, bar, pct, humanize.Comma(doneDisp), humanize.Comma(totalDisp)),
			fmt.Sprintf("Current: %s", truncateForProgress(s.current, 60)),
			fmt.Sprintf("Speed: %s items/sec", humanize.Comma(int64(speed))),
			fmt.Sprintf("ETA: %s", eta),
		}

		if rendered {
			fmt.Fprintf(w, "\x1b[%dA", progressLines)
		}
		for _, line := range lines {
			fmt.Fprintf(w, "\r\x1b[0K%s\n", line)
		}
		rendered = true
	}
}

// renderPlain is the plain (single-line) renderer.
func renderPlain(w io.Writer, s progressState) {
	pct, bar, _, _ := computeProgressMetrics(s)
	doneDisp, totalDisp := clampDoneTotal(s.done, s.total)
	line := fmt.Sprintf(
		"%s [%s] %3.0f%% %s/%s Current: %s",
		s.label,
		bar,
		pct,
		humanize.Comma(doneDisp),
		humanize.Comma(totalDisp),
		truncateForProgress(s.current, 60),
	)
	fmt.Fprintln(w, line)
}

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
	return "..." + s[len(s)-(max-3):]
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
