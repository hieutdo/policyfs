package cli

import (
	"context"
	"fmt"
	"io"
	"strings"
	"time"

	"github.com/dustin/go-humanize"
	"github.com/hieutdo/policyfs/internal/config"
	"github.com/hieutdo/policyfs/internal/indexer"
)

// indexProgressAdapter is the interface satisfied by both mpbIndexUI and plainIndexUI.
type indexProgressAdapter interface {
	OnProgress(storageID string, rel string, isDir bool)
	Finish()
	Cancel()
}

// plainIndexUI emits throttled single lines for non-interactive output.
type plainIndexUI struct {
	w               io.Writer
	total           int64
	done            int64
	lastPrint       time.Time
	lastPrintedDone int64

	lastStorageID string
	lastLabel     string
}

func startPlainIndexProgress(w io.Writer, total int64) *plainIndexUI {
	return &plainIndexUI{w: w, total: total}
}

// printLine prints one throttled progress line for plain mode.
func (u *plainIndexUI) printLine(storageID string, label string) {
	if u == nil || u.w == nil {
		return
	}
	fmt.Fprintf(u.w, "Indexing  %s/%s entries  %s: %s\n",
		humanize.Comma(u.done), humanize.Comma(u.total),
		storageID, truncateForProgress(label, 60))
}

func (u *plainIndexUI) OnProgress(storageID string, rel string, isDir bool) {
	if u == nil || u.w == nil {
		return
	}
	if strings.TrimSpace(rel) == "" {
		return
	}

	u.done++

	label := rel
	if isDir {
		label = rel + "/"
	}
	u.lastStorageID = storageID
	u.lastLabel = label

	now := time.Now()
	if !u.lastPrint.IsZero() && now.Sub(u.lastPrint) < 200*time.Millisecond {
		return
	}
	u.lastPrint = now

	u.printLine(storageID, label)
	u.lastPrintedDone = u.done
}

func (u *plainIndexUI) Finish() {
	// Nothing to clean up for plain mode.
	if u == nil {
		return
	}
	if u.done <= 0 {
		return
	}
	if u.lastPrintedDone >= u.done {
		return
	}
	if strings.TrimSpace(u.lastLabel) == "" {
		return
	}

	u.printLine(u.lastStorageID, u.lastLabel)
	u.lastPrintedDone = u.done
}

// Cancel stops progress output early (no-op for plain mode).
func (u *plainIndexUI) Cancel() {
}

// startIndexProgress runs the count phase silently and returns the appropriate progress adapter.
func startIndexProgress(ctx context.Context, w io.Writer, mountName string, mountCfg *config.MountConfig, mode string) (indexProgressAdapter, int64, error) {
	if strings.TrimSpace(mode) == "tty" && !isInteractiveWriter(w) {
		mode = "plain"
	}

	cr, err := indexer.Count(ctx, mountName, mountCfg)
	if err != nil {
		return nil, 0, fmt.Errorf("failed to count entries: %w", err)
	}

	if cr.TotalEntries == 0 {
		return nil, 0, nil
	}

	if isInteractiveWriter(w) && mode != "plain" {
		return startMpbIndexProgress(w, cr.TotalEntries), cr.TotalEntries, nil
	}
	return startPlainIndexProgress(w, cr.TotalEntries), cr.TotalEntries, nil
}
