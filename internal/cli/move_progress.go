package cli

import (
	"context"
	"fmt"
	"io"
	"strings"
	"sync"
	"time"

	"github.com/hieutdo/policyfs/internal/humanfmt"
	"github.com/hieutdo/policyfs/internal/mover"
)

// moveProgressUI adapts a ProgressTracker to the mover.Hooks.Progress callback signature.
type moveProgressUI struct {
	tracker *ProgressTracker

	mu              sync.Mutex
	copyKey         string
	copyStarted     time.Time
	doneUnitsCommit int64
	curJobName      string
	curRel          string
	curTotalBytes   int64
	jobMult         map[string]int64
	totalUnits      int64
}

// OnProgress translates the mover callback to the generic tracker.
func (u *moveProgressUI) OnProgress(jobName string, storageID string, rel string) {
	if u == nil || u.tracker == nil {
		return
	}
	if strings.TrimSpace(rel) == "" {
		return
	}

	u.mu.Lock()
	if u.totalUnits > 0 && u.jobMult != nil {
		if m, ok := u.jobMult[jobName]; ok && m > 0 && u.curJobName == jobName && u.curRel == rel && u.curTotalBytes > 0 {
			u.doneUnitsCommit += u.curTotalBytes * m
			u.tracker.SetUnits(u.doneUnitsCommit, u.totalUnits)
		}
	}
	// Reset current file tracking after we commit.
	u.curJobName = ""
	u.curRel = ""
	u.curTotalBytes = 0
	u.mu.Unlock()

	file := rel
	status := fmt.Sprintf("target=%s moved", storageID)
	_ = jobName
	u.tracker.OnItem(file + progressFieldSep + status)
}

// OnCopyProgress updates the current progress label with byte-level copy/verify progress.
func (u *moveProgressUI) OnCopyProgress(jobName string, storageID string, rel string, phase string, doneBytes int64, totalBytes int64) {
	if u == nil || u.tracker == nil {
		return
	}
	if strings.TrimSpace(rel) == "" {
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

	key := fmt.Sprintf("%s/%s:%s:%s", jobName, storageID, rel, phase)

	u.mu.Lock()
	if u.copyKey != key || u.copyStarted.IsZero() || doneBytes < 0 {
		u.copyKey = key
		u.copyStarted = time.Now()
	}
	u.curJobName = jobName
	u.curRel = rel
	u.curTotalBytes = totalBytes
	started := u.copyStarted
	doneCommit := u.doneUnitsCommit
	totalUnits := u.totalUnits
	mult := int64(0)
	if u.jobMult != nil {
		mult = u.jobMult[jobName]
	}
	u.mu.Unlock()

	if totalUnits > 0 && mult > 0 && totalBytes > 0 {
		doneUnits := doneCommit
		sz := totalBytes
		switch phase {
		case "verify":
			// Verify work is the second pass over the file.
			doneUnits += sz + doneBytes
		default:
			doneUnits += doneBytes
		}
		u.tracker.SetUnits(doneUnits, totalUnits)
	}

	pct := "-"
	if totalBytes > 0 {
		p := float64(doneBytes) / float64(totalBytes) * 100
		if p < 0 {
			p = 0
		}
		if p > 100 {
			p = 100
		}
		pct = fmt.Sprintf("%.0f%%", p)
	}

	speed := "-"
	eta := "-"
	elapsed := time.Since(started)
	if elapsed > 0 && doneBytes > 0 {
		bps := float64(doneBytes) / elapsed.Seconds()
		if bps > 0 {
			speed = humanfmt.FormatBytesIEC(int64(bps), 1) + "/s"
			if totalBytes > 0 {
				rem := float64(totalBytes - doneBytes)
				if rem < 0 {
					rem = 0
				}
				etaDur := time.Duration(rem / bps * float64(time.Second)).Round(time.Second)
				eta = etaDur.String()
			}
		}
	}

	doneDisp := humanfmt.FormatBytesIEC(doneBytes, 1)
	totalDisp := "-"
	if totalBytes > 0 {
		totalDisp = humanfmt.FormatBytesIEC(totalBytes, 1)
	}
	_ = jobName

	file := rel
	status := fmt.Sprintf("target=%s %s %s/%s %s %s eta=%s", storageID, verb, doneDisp, totalDisp, pct, speed, eta)
	u.tracker.OnStatus(file + progressFieldSep + status)
}

// Finish delegates to the tracker.
func (u *moveProgressUI) Finish() {
	if u == nil || u.tracker == nil {
		return
	}
	u.tracker.Finish()
}

// startMoveProgress seeds the progress tracker from a planning result and returns a moveProgressUI for moving.
func startMoveProgress(ctx context.Context, w io.Writer, opts mover.Opts, plan mover.PlanResult, mode string) (*moveProgressUI, error) {
	if strings.TrimSpace(mode) == "tty" && !isInteractiveWriter(w) {
		mode = "plain"
	}
	_ = ctx

	label := "Moving"
	total := plan.TotalCandidates
	if opts.Limit > 0 {
		label = fmt.Sprintf("Moving (limit=%d)", opts.Limit)
	}

	totalUnits := plan.TotalWorkBytes
	if opts.DryRun {
		totalUnits = 0
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

	tracker := NewProgressTracker(ProgressTrackerConfig{
		Writer:     w,
		Label:      label,
		Total:      total,
		TotalUnits: totalUnits,
		MinUpdates: 4,
		Mode:       mode,
	})
	u := &moveProgressUI{tracker: tracker, jobMult: jobMult, totalUnits: totalUnits}
	tracker.SetUnits(0, totalUnits)
	return u, nil
}
