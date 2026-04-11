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
	Name               string
	FilesMoved         int64
	BytesMoved         int64
	BytesFreed         int64
	FilesSkipped       int64
	FilesSkippedOpen   int64
	FilesSkippedExists int64
	FilesErrored       int64
	DurationMS         int64
	TotalCandidates    int64
}

// Opts controls mover behavior.
type Opts struct {
	Job    string
	DryRun bool
	Force  bool
	Limit  int
	// Debug enables best-effort debug output during planning/discovery.
	Debug bool
	// DebugMax caps the number of debug entries collected during planning.
	DebugMax int
}

// Hooks provides optional callbacks for progress reporting.
type Hooks struct {
	// FileStart is called when a file begins processing, before any copy/verify.
	FileStart func(jobName string, srcStorageID string, dstStorageID string, rel string, sizeBytes int64)
	// Progress is called for each candidate after it has been successfully moved.
	Progress     func(jobName string, storageID string, rel string)
	CopyProgress func(jobName string, storageID string, rel string, phase string, doneBytes int64, totalBytes int64)
	// Warn is called for non-fatal per-file issues.
	Warn func(jobName string, storageID string, rel string, err error)
}

// CountResult contains the discovery count used for progress bars.
type CountResult struct {
	TotalCandidates int64
}

// PlanCandidate describes one planned move candidate.
type PlanCandidate struct {
	JobName      string
	SrcStorageID string
	DstStorageID string // best-effort primary destination from planning
	RelPath      string
	SizeBytes    int64
	MTimeSec     int64
	WorkBytes    int64

	// PathPreservingKept lists destinations that passed the path_preserving filter
	// (i.e., already had the parent directory). Empty if path_preserving is off
	// or parentDir is empty. Nil means not computed (path_preserving off).
	PathPreservingKept []string
}

// PlanJob describes the planned candidates for one mover job.
type PlanJob struct {
	Name       string
	Candidates []PlanCandidate
	WorkBytes  int64
}

// PlanResult summarizes one planning pass used to seed progress/ETA and verbose output.
type PlanResult struct {
	Jobs            []PlanJob
	TotalCandidates int64
	TotalWorkBytes  int64
	// Debug holds optional debug output produced during discovery.
	Debug *PlanDebug
}

// Plan scans eligible jobs and discovers move candidates, capped by opts.Limit.
// It estimates total work bytes as size*(verify?2:1) so the CLI can compute an overall ETA.
func Plan(ctx context.Context, mountName string, mountCfg *config.MountConfig, opts Opts) (PlanResult, error) {
	if strings.TrimSpace(mountName) == "" {
		return PlanResult{}, &errkind.RequiredError{What: "mount name"}
	}
	if mountCfg == nil {
		return PlanResult{}, &errkind.NilError{What: "mount config"}
	}
	if opts.Limit < 0 {
		return PlanResult{}, &errkind.InvalidError{Msg: "limit must be >= 0"}
	}

	enabled := true
	if mountCfg.Mover.Enabled != nil {
		enabled = *mountCfg.Mover.Enabled
	}
	if !enabled {
		return PlanResult{}, nil
	}

	p := newPlanner(mountName, mountCfg, opts)
	jobs, err := p.selectJobs()
	if err != nil {
		return PlanResult{}, err
	}

	var dbg *debugCollector
	if opts.Debug {
		max := opts.DebugMax
		if max <= 0 {
			max = 20
		}
		dbg = newDebugCollector(max)
	}

	out := PlanResult{}
	for _, j := range jobs {
		if opts.Limit > 0 && out.TotalCandidates >= int64(opts.Limit) {
			break
		}
		if dbg != nil {
			if dj := p.debugDestinationsForJob(j); dj != nil {
				dbg.addDestinationDebug(*dj)
			}
		}
		cands, err := p.discoverJobCandidatesWithDebug(ctx, j, dbg)
		if err != nil {
			return PlanResult{}, err
		}
		if len(cands) == 0 {
			continue
		}
		verify := jobVerifyEnabled(j)
		mult := int64(1)
		if verify {
			mult = 2
		}

		// Best-effort destination resolution for display.
		// This can be expensive (statfs/stat); only do it in debug mode.
		dstIDs := []string(nil)
		if opts.Debug {
			dstIDs, _ = p.expandRefs(j.Destination.Paths, j.Destination.Groups)
		}

		pj := PlanJob{Name: j.Name}
		for _, c := range cands {
			if opts.Limit > 0 && out.TotalCandidates >= int64(opts.Limit) {
				break
			}
			wb := c.SizeBytes * mult
			dstID := ""
			var ppKept []string
			if len(dstIDs) > 0 {
				dr, err := p.selectDestinations(j, dstIDs, c)
				if err == nil && len(dr.choices) > 0 {
					dstID = dr.choices[0].id
				}
				ppKept = dr.pathPreservingKept
			}
			pj.Candidates = append(pj.Candidates, PlanCandidate{
				JobName:            j.Name,
				SrcStorageID:       c.SrcStorageID,
				DstStorageID:       dstID,
				RelPath:            c.RelPath,
				SizeBytes:          c.SizeBytes,
				MTimeSec:           c.MTimeSec,
				WorkBytes:          wb,
				PathPreservingKept: ppKept,
			})
			pj.WorkBytes += wb
			out.TotalCandidates++
			out.TotalWorkBytes += wb
		}
		if len(pj.Candidates) > 0 {
			out.Jobs = append(out.Jobs, pj)
		}
	}
	if dbg != nil {
		out.Debug = dbg.result()
	}

	return out, nil
}

// Count scans eligible jobs and counts move candidates.
func Count(ctx context.Context, mountName string, mountCfg *config.MountConfig, opts Opts) (CountResult, error) {
	pl, err := Plan(ctx, mountName, mountCfg, opts)
	if err != nil {
		return CountResult{}, err
	}
	return CountResult{TotalCandidates: pl.TotalCandidates}, nil
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
	limit := max(int64(opts.Limit), 0)

	for _, j := range jobs {
		if limit > 0 && moved >= limit {
			break
		}

		jr, err := p.runJob(ctx, j, hooks, db, limit, moved)
		// Always accumulate partial results, even on error.
		out.Jobs = append(out.Jobs, jr)
		out.TotalFilesMoved += jr.FilesMoved
		out.TotalBytesMoved += jr.BytesMoved
		out.TotalBytesFreed += jr.BytesFreed
		moved += jr.FilesMoved
		out.Warnings = append(out.Warnings, jrWarnings(jr)...)
		if err != nil {
			out.TotalDurationMS = time.Since(start).Milliseconds()
			return out, err
		}
	}

	out.TotalDurationMS = time.Since(start).Milliseconds()
	return out, nil
}
