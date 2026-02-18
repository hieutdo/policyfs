package cli

import (
	"fmt"
	"io"
	"strings"
	"time"

	"github.com/hieutdo/policyfs/internal/humanfmt"
	"github.com/hieutdo/policyfs/internal/mover"
)

// moveProgressAdapter is the interface satisfied by both mpbMoveUI and plainMoveUI.
type moveProgressAdapter interface {
	OnFileStart(jobName string, srcStorageID string, dstStorageID string, rel string, sizeBytes int64)
	OnProgress(jobName string, storageID string, rel string)
	OnCopyProgress(jobName string, storageID string, rel string, phase string, doneBytes int64, totalBytes int64)
	// Finish completes all bars assuming normal completion (fills to 100%).
	Finish()
	// Cancel shuts down the display preserving current state (no fill to 100%).
	Cancel()
}

// plainMoveUI emits simple log lines for non-interactive (plain) output.
type plainMoveUI struct {
	w         io.Writer
	total     int64
	index     int64
	fileStart time.Time
	curSize   int64
	curDst    string
}

func startPlainProgress(w io.Writer, _ mover.Opts, plan mover.PlanResult) *plainMoveUI {
	return &plainMoveUI{
		w:     w,
		total: plan.TotalCandidates,
	}
}

func (u *plainMoveUI) OnFileStart(_ string, _ string, dstStorageID string, rel string, sizeBytes int64) {
	if u == nil || u.w == nil {
		return
	}
	u.index++
	u.fileStart = time.Now()
	u.curSize = sizeBytes
	u.curDst = dstStorageID

	sz := humanfmt.FormatBytesIEC(sizeBytes, 1)
	dst := strings.TrimSpace(dstStorageID)
	if dst == "" {
		dst = "-"
	}
	fmt.Fprintf(u.w, "[%d/%d]  Copying  %s  %s \u2500\u25BA %s\n", u.index, u.total, rel, sz, dst)
}

func (u *plainMoveUI) OnProgress(_ string, storageID string, rel string) {
	if u == nil || u.w == nil {
		return
	}
	dur := time.Since(u.fileStart).Round(100 * time.Millisecond)
	sz := humanfmt.FormatBytesIEC(u.curSize, 1)
	dst := strings.TrimSpace(u.curDst)
	if dst == "" {
		dst = strings.TrimSpace(storageID)
	}
	if dst == "" {
		dst = "-"
	}
	fmt.Fprintf(u.w, "[%d/%d]  Done  %s  %s \u2500\u25BA %s  %s\n", u.index, u.total, rel, sz, dst, dur.String())
}

func (u *plainMoveUI) OnCopyProgress(_ string, _ string, _ string, _ string, _ int64, _ int64) {
	// No byte-level progress in plain mode.
}

func (u *plainMoveUI) Finish() {
	// Nothing to clean up for plain mode.
}

func (u *plainMoveUI) Cancel() {
	// Nothing to clean up for plain mode.
}
