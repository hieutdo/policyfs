package cli

import (
	"context"
	"fmt"
	"io"
	"strings"

	"github.com/hieutdo/policyfs/internal/config"
	"github.com/hieutdo/policyfs/internal/mover"
)

// moveProgressUI adapts a ProgressTracker to the mover.Hooks.Progress callback signature.
type moveProgressUI struct {
	tracker *ProgressTracker
}

// OnProgress translates the mover callback to the generic tracker.
func (u *moveProgressUI) OnProgress(jobName string, storageID string, rel string) {
	if u == nil || u.tracker == nil {
		return
	}
	if strings.TrimSpace(rel) == "" {
		return
	}
	u.tracker.OnItem(fmt.Sprintf("%s/%s: %s", jobName, storageID, rel))
}

// Finish delegates to the tracker.
func (u *moveProgressUI) Finish() {
	if u == nil || u.tracker == nil {
		return
	}
	u.tracker.Finish()
}

// startMoveProgress runs the count phase silently and returns a moveProgressUI for moving.
func startMoveProgress(ctx context.Context, w io.Writer, mountName string, mountCfg *config.MountConfig, opts mover.Opts, mode string) (*moveProgressUI, error) {
	if strings.TrimSpace(mode) == "tty" && !isInteractiveWriter(w) {
		mode = "plain"
	}

	cr, err := mover.Count(ctx, mountName, mountCfg, opts)
	if err != nil {
		return nil, fmt.Errorf("failed to count candidates: %w", err)
	}
	tracker := NewProgressTracker(ProgressTrackerConfig{
		Writer:     w,
		Label:      "Moving",
		Total:      cr.TotalCandidates,
		MinUpdates: 4,
		Mode:       mode,
	})
	return &moveProgressUI{tracker: tracker}, nil
}
