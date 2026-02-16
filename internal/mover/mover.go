package mover

import (
	"context"
	"fmt"
	"strings"
	"time"

	"github.com/hieutdo/policyfs/internal/config"
	"github.com/hieutdo/policyfs/internal/errkind"
	"github.com/hieutdo/policyfs/internal/indexdb"
)

// Result summarizes one mover invocation.
type Result struct {
	Mount           string
	Jobs            []JobResult
	TotalFilesMoved int64
	TotalBytesMoved int64
	TotalBytesFreed int64
	TotalDurationMS int64
	Warnings        []string
}

// JobResult summarizes one mover job.
type JobResult struct {
	Name            string
	FilesMoved      int64
	BytesMoved      int64
	BytesFreed      int64
	FilesSkipped    int64
	FilesErrored    int64
	DurationMS      int64
	TotalCandidates int64
}

// Opts controls mover behavior.
type Opts struct {
	Job    string
	DryRun bool
	Force  bool
	Limit  int
}

// Hooks provides optional callbacks for progress reporting.
type Hooks struct {
	// Progress is called for each candidate right before it is processed.
	Progress func(jobName string, storageID string, rel string)
	// Warn is called for non-fatal per-file issues.
	Warn func(jobName string, storageID string, rel string, err error)
}

// CountResult contains the discovery count used for progress bars.
type CountResult struct {
	TotalCandidates int64
}

// Count scans eligible jobs and counts move candidates.
func Count(ctx context.Context, mountName string, mountCfg *config.MountConfig, opts Opts) (CountResult, error) {
	if strings.TrimSpace(mountName) == "" {
		return CountResult{}, &errkind.RequiredError{What: "mount name"}
	}
	if mountCfg == nil {
		return CountResult{}, &errkind.NilError{What: "mount config"}
	}
	if opts.Limit < 0 {
		return CountResult{}, &errkind.InvalidError{Msg: "limit must be >= 0"}
	}

	p := newPlanner(mountName, mountCfg, opts)
	jobs, err := p.selectJobs()
	if err != nil {
		return CountResult{}, err
	}

	var total int64
	for _, j := range jobs {
		cands, err := p.discoverJobCandidates(ctx, j)
		if err != nil {
			return CountResult{}, err
		}
		total += int64(len(cands))
		if opts.Limit > 0 && total >= int64(opts.Limit) {
			total = int64(opts.Limit)
			break
		}
	}

	return CountResult{TotalCandidates: total}, nil
}

// RunOneshot executes mover jobs for one mount.
func RunOneshot(ctx context.Context, mountName string, mountCfg *config.MountConfig, opts Opts, hooks Hooks) (Result, error) {
	start := time.Now()
	out := Result{Mount: mountName}

	if strings.TrimSpace(mountName) == "" {
		return out, &errkind.RequiredError{What: "mount name"}
	}
	if mountCfg == nil {
		return out, &errkind.NilError{What: "mount config"}
	}
	if opts.Limit < 0 {
		return out, &errkind.InvalidError{Msg: "limit must be >= 0"}
	}

	enabled := true
	if mountCfg.Mover.Enabled != nil {
		enabled = *mountCfg.Mover.Enabled
	}
	if !enabled {
		out.TotalDurationMS = time.Since(start).Milliseconds()
		return out, nil
	}

	p := newPlanner(mountName, mountCfg, opts)
	jobs, err := p.selectJobs()
	if err != nil {
		return out, err
	}

	// Open index DB up-front if the mount has any indexed storage.
	// This avoids making physical moves without updating the DB (which would make the moved files invisible).
	var db *indexdb.DB
	needsDB := false
	for _, sp := range mountCfg.StoragePaths {
		if sp.Indexed {
			needsDB = true
			break
		}
	}
	if needsDB {
		idxDB, err := indexdb.Open(mountName)
		if err != nil {
			return out, fmt.Errorf("failed to open index db: %w", err)
		}
		db = idxDB
		defer func() { _ = db.Close() }()
	}

	moved := int64(0)
	limit := int64(opts.Limit)
	if limit <= 0 {
		limit = 0
	}

	for _, j := range jobs {
		if limit > 0 && moved >= limit {
			break
		}

		jr, err := p.runJob(ctx, j, hooks, db, limit, moved)
		if err != nil {
			return out, err
		}

		out.Jobs = append(out.Jobs, jr)
		out.TotalFilesMoved += jr.FilesMoved
		out.TotalBytesMoved += jr.BytesMoved
		out.TotalBytesFreed += jr.BytesFreed
		moved += jr.FilesMoved
		out.Warnings = append(out.Warnings, jrWarnings(jr)...)
	}

	out.TotalDurationMS = time.Since(start).Milliseconds()
	return out, nil
}
