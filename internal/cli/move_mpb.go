package cli

import (
	"fmt"
	"io"
	"strings"
	"sync"
	"time"

	"github.com/vbauerster/mpb/v8"
	"github.com/vbauerster/mpb/v8/decor"

	"github.com/hieutdo/policyfs/internal/humanfmt"
	"github.com/hieutdo/policyfs/internal/mover"
)

// mpbMoveUI drives the mpb-based TTY progress display for `pfs move`.
type mpbMoveUI struct {
	p          *mpb.Progress
	bars       []*mpb.Bar // all bars including NopStyle - must abort all in Finish()
	overallBar *mpb.Bar

	mu sync.Mutex

	totalFiles int64
	doneFiles  int64
	startTime  time.Time

	// Last completed file.
	completed struct {
		idx       int64
		rel       string
		job       string
		src       string
		dst       string
		sizeBytes int64
		duration  time.Duration
		exists    bool
	}

	// Current file being processed.
	current struct {
		idx        int64
		rel        string
		job        string
		src        string
		dst        string
		phase      string
		sizeBytes  int64
		doneBytes  int64
		fileStart  time.Time
		phaseStart time.Time
		exists     bool
	}

	// Byte-level overall tracking for ETA.
	totalWorkBytes int64
	doneWorkBytes  int64
	committedBytes int64 // cumulative work bytes from fully completed files
	jobMult        map[string]int64
	overallByBytes bool
}

const mpbBarWidth = 38

// startMpbProgress creates the mpb container and all bars, returning the UI adapter.
func startMpbProgress(w io.Writer, opts mover.Opts, plan mover.PlanResult) *mpbMoveUI {
	totalFiles := plan.TotalCandidates
	totalWork := plan.TotalWorkBytes
	if opts.DryRun {
		totalWork = 0
	}

	jobMult := map[string]int64{}
	for _, pj := range plan.Jobs {
		m := int64(1)
		for _, c := range pj.Candidates {
			if c.SizeBytes <= 0 {
				continue
			}
			if c.WorkBytes > c.SizeBytes {
				m = 2
				break
			}
		}
		jobMult[pj.Name] = m
	}

	u := &mpbMoveUI{
		totalFiles:     totalFiles,
		startTime:      time.Now(),
		totalWorkBytes: totalWork,
		jobMult:        jobMult,
	}
	if totalWork > 0 {
		u.overallByBytes = true
	}

	cols := terminalWidth(w)
	if cols <= 0 {
		cols = 80
	}

	separator := strings.Repeat("\u2500", cols-1)

	p := mpb.New(
		mpb.WithOutput(w),
		mpb.WithRefreshRate(120*time.Millisecond),
		mpb.WithAutoRefresh(),
	)
	u.p = p

	addNop := func(fn func(decor.Statistics) string) *mpb.Bar {
		b := p.New(0, mpb.NopStyle(), mpb.PrependDecorators(decor.Any(fn)))
		u.bars = append(u.bars, b)
		return b
	}

	// Bar 0: top separator
	// addNop(func(_ decor.Statistics) string { return separator })

	// Bar 1: completed file line 1
	addNop(func(_ decor.Statistics) string {
		u.mu.Lock()
		defer u.mu.Unlock()
		if !u.completed.exists {
			return ""
		}
		rel := truncateForMpb(u.completed.rel, cols-20)
		return fmt.Sprintf("  \u2713  [%d/%d]  %s", u.completed.idx, u.totalFiles, rel)
	})

	// Bar 2: completed file line 2
	addNop(func(_ decor.Statistics) string {
		u.mu.Lock()
		defer u.mu.Unlock()
		if !u.completed.exists {
			return ""
		}
		sz := humanfmt.FormatBytesIEC(u.completed.sizeBytes, 1)
		dur := u.completed.duration.Round(time.Second).String()
		return fmt.Sprintf("            %s  %s \u2500\u25BA %s  done  %s", sz, u.completed.src, u.completed.dst, dur)
	})

	// Bar 3: current file line 1
	addNop(func(_ decor.Statistics) string {
		u.mu.Lock()
		defer u.mu.Unlock()
		if !u.current.exists {
			return ""
		}
		rel := truncateForMpb(u.current.rel, cols-20)
		return fmt.Sprintf("  \u2192  [%d/%d]  %s", u.current.idx, u.totalFiles, rel)
	})

	// Bar 5: current file line 2 (job, src -> dst, speed)
	addNop(func(_ decor.Statistics) string {
		u.mu.Lock()
		defer u.mu.Unlock()
		if !u.current.exists {
			return ""
		}
		speed := ""
		elapsed := time.Since(u.current.phaseStart)
		if elapsed > 0 && u.current.doneBytes > 0 {
			bps := float64(u.current.doneBytes) / elapsed.Seconds()
			if bps > 0 {
				speed = fmt.Sprintf("  %9s", humanfmt.FormatBytesIEC(int64(bps), 1)+"/s")
			}
		}
		return fmt.Sprintf("            job=%s  %s \u2500\u25BA %s  %s%s", u.current.job, u.current.src, u.current.dst, u.current.phase, speed)
	})

	// Bar 6: current file progress bar (manually rendered)
	addNop(func(_ decor.Statistics) string {
		u.mu.Lock()
		defer u.mu.Unlock()
		if !u.current.exists || u.current.sizeBytes <= 0 {
			return ""
		}
		done := u.current.doneBytes
		total := u.current.sizeBytes
		if done < 0 {
			done = 0
		}
		if done > total {
			done = total
		}

		pct := float64(done) / float64(total) * 100
		bar := renderProgressBar(mpbBarWidth, done, total)
		doneDisp := humanfmt.FormatBytesIEC(done, 1)
		totalDisp := humanfmt.FormatBytesIEC(total, 1)

		eta := "  -"
		elapsed := time.Since(u.current.phaseStart)
		if elapsed > 0 && done > 0 {
			bps := float64(done) / elapsed.Seconds()
			if bps > 0 {
				rem := float64(total - done)
				if rem < 0 {
					rem = 0
				}
				etaDur := time.Duration(rem / bps * float64(time.Second)).Round(time.Second)
				eta = fmt.Sprintf("%6s", etaDur.String())
			}
		}

		return fmt.Sprintf("            [%s]  %3.0f%%  %8s/%-8s ETA: %s", bar, pct, doneDisp, totalDisp, eta)
	})

	// Bar 7: bottom separator
	addNop(func(_ decor.Statistics) string { return separator })

	// Bar 8: overall progress (real bar)
	overallTotal := totalFiles
	if u.overallByBytes {
		overallTotal = u.totalWorkBytes
	}
	overallBar := p.New(overallTotal, mpb.NopStyle(),
		mpb.PrependDecorators(
			decor.Any(func(_ decor.Statistics) string {
				u.mu.Lock()
				defer u.mu.Unlock()

				doneFiles := u.doneFiles
				totalFiles := u.totalFiles
				if doneFiles < 0 {
					doneFiles = 0
				}
				if totalFiles > 0 && doneFiles > totalFiles {
					doneFiles = totalFiles
				}

				barDone := doneFiles
				barTotal := totalFiles
				if u.overallByBytes {
					barDone = u.doneWorkBytes
					barTotal = u.totalWorkBytes
					if barDone < 0 {
						barDone = 0
					}
					if barTotal > 0 && barDone > barTotal {
						barDone = barTotal
					}
				}

				pct := float64(0)
				if barTotal > 0 {
					pct = float64(barDone) / float64(barTotal) * 100
				}
				bar := renderProgressBar(mpbBarWidth, barDone, barTotal)

				eta := "  -"
				if u.totalWorkBytes > 0 && u.doneWorkBytes > 0 {
					elapsed := time.Since(u.startTime)
					if elapsed > 0 {
						bps := float64(u.doneWorkBytes) / elapsed.Seconds()
						if bps > 0 {
							rem := float64(u.totalWorkBytes - u.doneWorkBytes)
							if rem < 0 {
								rem = 0
							}
							etaDur := time.Duration(rem / bps * float64(time.Second)).Round(time.Second)
							eta = fmt.Sprintf("%6s", etaDur.String())
						}
					}
				}

				return fmt.Sprintf("Overall     [%s]  %3.0f%%  %d/%d files  ETA: %s", bar, pct, doneFiles, totalFiles, eta)
			}),
		),
	)
	u.bars = append(u.bars, overallBar)
	u.overallBar = overallBar

	return u
}

// OnFileStart is called when a file begins processing.
func (u *mpbMoveUI) OnFileStart(jobName string, srcStorageID string, dstStorageID string, rel string, sizeBytes int64) {
	if u == nil {
		return
	}
	u.mu.Lock()
	defer u.mu.Unlock()

	now := time.Now()

	// Promote current -> completed (if current exists).
	if u.current.exists {
		u.completed.exists = true
		u.completed.idx = u.current.idx
		u.completed.rel = u.current.rel
		u.completed.job = u.current.job
		u.completed.src = u.current.src
		u.completed.dst = u.current.dst
		u.completed.sizeBytes = u.current.sizeBytes
		u.completed.duration = now.Sub(u.current.fileStart)
	}

	// Set new current file.
	u.current.exists = true
	u.current.idx = u.doneFiles + 1
	u.current.rel = rel
	u.current.job = jobName
	u.current.src = srcStorageID
	u.current.dst = dstStorageID
	u.current.phase = ""
	u.current.sizeBytes = sizeBytes
	u.current.doneBytes = 0
	u.current.fileStart = now
	u.current.phaseStart = now
}

// OnCopyProgress updates byte-level progress for the current file.
func (u *mpbMoveUI) OnCopyProgress(jobName string, storageID string, rel string, phase string, doneBytes int64, totalBytes int64) {
	if u == nil {
		return
	}
	phase = strings.TrimSpace(phase)
	if phase == "" {
		phase = "copy"
	}
	verb := phase
	switch phase {
	case "copy":
		verb = "copying"
	case "verify":
		verb = "verifying"
	}

	u.mu.Lock()

	// Update phase.
	if u.current.phase != verb {
		u.current.phase = verb
		u.current.phaseStart = time.Now()
		u.current.doneBytes = 0
	}
	u.current.doneBytes = doneBytes
	if totalBytes > 0 {
		u.current.sizeBytes = totalBytes
	}

	// Update overall byte-level progress.
	if u.totalWorkBytes > 0 && totalBytes > 0 {
		switch phase {
		case "verify":
			// Verify work is the second pass: committed + copy pass + verify progress.
			u.doneWorkBytes = u.committedBytes + totalBytes + doneBytes
		default:
			u.doneWorkBytes = u.committedBytes + doneBytes
		}
	}
	newWorkDone := u.doneWorkBytes
	byBytes := u.overallByBytes
	overallTotal := u.totalWorkBytes
	u.mu.Unlock()

	if byBytes && u.overallBar != nil {
		if overallTotal > 0 && newWorkDone > overallTotal {
			newWorkDone = overallTotal
		}
		if newWorkDone < 0 {
			newWorkDone = 0
		}
		u.overallBar.SetCurrent(newWorkDone)
	}
}

// OnProgress is called when a file has been fully moved.
func (u *mpbMoveUI) OnProgress(jobName string, storageID string, rel string) {
	if u == nil {
		return
	}
	u.mu.Lock()

	// Commit work bytes for this file.
	if u.totalWorkBytes > 0 && u.jobMult != nil {
		if m, ok := u.jobMult[jobName]; ok && m > 0 && u.current.exists && u.current.sizeBytes > 0 {
			u.committedBytes += u.current.sizeBytes * m
			u.doneWorkBytes = u.committedBytes
		}
	}

	u.doneFiles++

	// Promote current -> completed.
	if u.current.exists {
		u.completed.exists = true
		u.completed.idx = u.current.idx
		u.completed.rel = u.current.rel
		u.completed.job = u.current.job
		u.completed.src = u.current.src
		u.completed.dst = u.current.dst
		u.completed.sizeBytes = u.current.sizeBytes
		u.completed.duration = time.Since(u.current.fileStart)
	}

	// Clear current.
	u.current.exists = false
	u.current.doneBytes = 0
	u.current.sizeBytes = 0

	newWorkDone := u.doneWorkBytes
	byBytes := u.overallByBytes
	totalWork := u.totalWorkBytes
	u.mu.Unlock()

	if u.overallBar == nil {
		return
	}
	if byBytes {
		if totalWork > 0 && newWorkDone > totalWork {
			newWorkDone = totalWork
		}
		if newWorkDone < 0 {
			newWorkDone = 0
		}
		u.overallBar.SetCurrent(newWorkDone)
		return
	}

	u.overallBar.Increment()
}

// Finish completes all bars (fills to 100%) and waits for mpb to release the terminal.
func (u *mpbMoveUI) Finish() {
	if u == nil {
		return
	}
	u.mu.Lock()
	remaining := u.totalFiles - u.doneFiles
	u.doneFiles = u.totalFiles
	u.doneWorkBytes = u.totalWorkBytes
	u.current.exists = false
	byBytes := u.overallByBytes
	totalWork := u.totalWorkBytes
	u.mu.Unlock()

	if u.overallBar != nil {
		if byBytes {
			u.overallBar.SetCurrent(totalWork)
		} else if remaining > 0 {
			u.overallBar.IncrBy(int(remaining))
		}
	}
	// Abort every bar so p.Wait() can return.
	for _, b := range u.bars {
		b.Abort(false)
	}
	u.p.Wait()
}

// Cancel shuts down the display without filling to 100%, preserving the active file info.
func (u *mpbMoveUI) Cancel() {
	if u == nil {
		return
	}
	// Don't touch doneFiles, doneWorkBytes, or current - keep them as-is
	// so the last rendered frame shows actual progress at time of cancel.
	for _, b := range u.bars {
		b.Abort(false)
	}
	u.p.Wait()
}

// truncateForMpb truncates a string to fit within maxWidth runes.
func truncateForMpb(s string, maxWidth int) string {
	if maxWidth <= 0 {
		return ""
	}
	runes := []rune(s)
	if len(runes) <= maxWidth {
		return s
	}
	if maxWidth <= 3 {
		return string(runes[:maxWidth])
	}
	keep := maxWidth - 3
	left := keep / 2
	right := keep - left
	return string(runes[:left]) + "..." + string(runes[len(runes)-right:])
}
