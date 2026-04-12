package mover

import (
	"context"
	"time"

	"github.com/hieutdo/policyfs/internal/daemonctl"
)

// openFileSet is a pre-queried set of open file IDs.
// A nil set means daemon.sock was unavailable (best-effort skip).
type openFileSet map[daemonctl.OpenFileID]struct{}

// queryOpenFileSet queries the daemon for all candidate file IDs in one batch.
//
// Returns (nil, nil) when daemon.sock is unavailable - callers treat this as "unknown, proceed".
func (p *planner) queryOpenFileSet(ctx context.Context, cands []candidate) (openFileSet, error) {
	if p == nil || p.daemonSockPath == "" {
		return nil, nil
	}

	ids := make([]daemonctl.OpenFileID, len(cands))
	for i, c := range cands {
		ids[i] = daemonctl.OpenFileID{StorageID: c.SrcStorageID, Dev: c.Dev, Ino: c.Ino}
	}

	qctx, cancel := context.WithTimeout(ctx, 2*time.Second)
	defer cancel()

	stats, err := daemonctl.QueryOpenCounts(qctx, p.daemonSockPath, ids)
	if err != nil {
		// Daemon unreachable for any reason → best-effort skip.
		return nil, nil
	}

	set := make(openFileSet, len(stats))
	for _, s := range stats {
		if s.OpenCount > 0 {
			set[s.OpenFileID] = struct{}{}
		}
	}
	return set, nil
}
