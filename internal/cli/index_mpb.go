package cli

import (
	"fmt"
	"io"
	"strings"
	"sync"
	"time"

	"github.com/dustin/go-humanize"
	"github.com/vbauerster/mpb/v8"
	"github.com/vbauerster/mpb/v8/decor"
)

// mpbIndexUI drives the mpb-based TTY progress display for `pfs index`.
type mpbIndexUI struct {
	p          *mpb.Progress
	bars       []*mpb.Bar
	overallBar *mpb.Bar
	curBar     *mpb.Bar

	mu    sync.Mutex
	total int64
	done  int64
	start time.Time

	currentLabel string // "storageID: rel/path"
}

// startMpbIndexProgress creates the mpb container and bars for index progress.
func startMpbIndexProgress(w io.Writer, total int64) *mpbIndexUI {
	u := &mpbIndexUI{
		total: total,
		start: time.Now(),
	}

	cols := terminalWidth(w)
	if cols <= 0 {
		cols = 80
	}

	p := mpb.New(
		mpb.WithOutput(w),
		mpb.WithRefreshRate(120*time.Millisecond),
		mpb.WithAutoRefresh(),
	)
	u.p = p

	// Bar 0: overall indexing progress.
	overallBar := p.New(total, mpb.NopStyle(),
		mpb.PrependDecorators(
			decor.Any(func(_ decor.Statistics) string {
				u.mu.Lock()
				defer u.mu.Unlock()

				done := u.done
				t := u.total
				if done < 0 {
					done = 0
				}
				if t > 0 && done > t {
					done = t
				}

				pct := float64(0)
				if t > 0 {
					pct = float64(done) / float64(t) * 100
				}
				bar := renderProgressBar(mpbBarWidth, done, t)

				eta := "  -"
				if t > 0 && done > 0 {
					elapsed := time.Since(u.start)
					if elapsed > 0 {
						rate := float64(done) / elapsed.Seconds()
						if rate > 0 {
							rem := float64(t - done)
							if rem < 0 {
								rem = 0
							}
							etaDur := time.Duration(rem / rate * float64(time.Second)).Round(time.Second)
							eta = etaDur.String()
						}
					}
				}

				return fmt.Sprintf("Indexing: [%s]  %3.0f%%  %s/%s entries  ETA: %s",
					bar, pct, humanize.Comma(done), humanize.Comma(t), eta)
			}),
		),
	)
	u.bars = append(u.bars, overallBar)
	u.overallBar = overallBar

	// Bar 1: current file being indexed.
	curBar := p.New(0, mpb.NopStyle(),
		mpb.PrependDecorators(
			decor.Any(func(_ decor.Statistics) string {
				u.mu.Lock()
				defer u.mu.Unlock()
				if u.currentLabel == "" {
					return ""
				}
				label := truncateForMpb(u.currentLabel, cols-10)
				return fmt.Sprintf("Current:  %s", label)
			}),
		),
	)
	u.bars = append(u.bars, curBar)
	u.curBar = curBar

	return u
}

// OnProgress is called for each scanned entry (file or directory).
func (u *mpbIndexUI) OnProgress(storageID string, rel string, isDir bool) {
	if u == nil {
		return
	}
	if strings.TrimSpace(rel) == "" {
		return
	}
	label := rel
	if isDir {
		label = rel + "/"
	}

	u.mu.Lock()
	u.done++
	u.currentLabel = fmt.Sprintf("%s: %s", storageID, label)
	u.mu.Unlock()

	u.overallBar.Increment()
}

// Finish completes all bars and waits for mpb to release the terminal.
func (u *mpbIndexUI) Finish() {
	if u == nil {
		return
	}
	u.mu.Lock()
	remaining := u.total - u.done
	u.done = u.total
	u.currentLabel = ""
	u.mu.Unlock()

	if remaining > 0 {
		u.overallBar.IncrBy(int(remaining))
	}
	// Drop the current-file bar so it doesn't leave a blank line.
	u.curBar.Abort(true)
	u.overallBar.Abort(false)
	u.p.Wait()
}

// Cancel shuts down the display without filling to 100%.
func (u *mpbIndexUI) Cancel() {
	if u == nil {
		return
	}
	if u.p == nil {
		return
	}
	for _, b := range u.bars {
		if b == nil {
			continue
		}
		b.Abort(false)
	}
	u.p.Wait()
}
