package cli

import (
	"context"
	"fmt"
	"io"
	"strings"

	"github.com/hieutdo/policyfs/internal/config"
	"github.com/hieutdo/policyfs/internal/indexer"
)

// indexProgressUI adapts a ProgressTracker to the indexer.Hooks.Progress callback signature.
type indexProgressUI struct {
	tracker *ProgressTracker
}

// OnProgress translates the indexer callback to the generic tracker.
func (u *indexProgressUI) OnProgress(storageID string, rel string, isDir bool) {
	if u == nil || u.tracker == nil {
		return
	}
	if strings.TrimSpace(rel) == "" {
		return
	}
	label := rel
	if isDir {
		label = rel + "/"
	}
	u.tracker.OnItem(fmt.Sprintf("%s: %s", storageID, label))
}

// Finish delegates to the tracker.
func (u *indexProgressUI) Finish() {
	if u == nil || u.tracker == nil {
		return
	}
	u.tracker.Finish()
}

// startIndexProgress runs the count phase silently and returns an indexProgressUI for indexing.
func startIndexProgress(ctx context.Context, w io.Writer, mountName string, mountCfg *config.MountConfig, mode string) (*indexProgressUI, error) {
	if strings.TrimSpace(mode) == "tty" && !isInteractiveWriter(w) {
		mode = "plain"
	}

	cr, err := indexer.Count(ctx, mountName, mountCfg)
	if err != nil {
		return nil, fmt.Errorf("failed to count entries: %w", err)
	}
	tracker := NewProgressTracker(ProgressTrackerConfig{
		Writer:     w,
		Label:      "Indexing",
		Total:      cr.TotalEntries,
		TotalUnits: 0,
		MinUpdates: 4,
		Mode:       mode,
	})
	return &indexProgressUI{tracker: tracker}, nil
}
