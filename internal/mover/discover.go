package mover

import (
	"context"
	"fmt"
	"io/fs"
	"os"
	"path/filepath"
	"sort"
	"strings"
	"syscall"
	"time"

	"github.com/hieutdo/policyfs/internal/config"
	"github.com/hieutdo/policyfs/internal/errkind"
	"github.com/hieutdo/policyfs/internal/humanfmt"
	"github.com/hieutdo/policyfs/internal/pathmatch"
)

// candidate is one move candidate discovered on a source storage.
type candidate struct {
	SrcStorageID string
	RelPath      string
	SizeBytes    int64
	MTimeSec     int64
	Mode         uint32
	UID          uint32
	GID          uint32
	Dev          uint64
	Ino          uint64
}

// conditions is the parsed move conditions for filtering candidates.
type conditions struct {
	MinAge  *time.Duration
	MinSize *int64
	MaxSize *int64
}

// parseConditions parses job conditions.
func parseConditions(c config.MoverConditionsConfig) (conditions, error) {
	out := conditions{}
	if strings.TrimSpace(c.MinAge) != "" {
		d, err := humanfmt.ParseDuration(c.MinAge)
		if err != nil {
			return conditions{}, fmt.Errorf("invalid min_age: %w", err)
		}
		out.MinAge = &d
	}
	if strings.TrimSpace(c.MinSize) != "" {
		b, err := humanfmt.ParseBytes(c.MinSize)
		if err != nil {
			return conditions{}, fmt.Errorf("invalid min_size: %w", err)
		}
		out.MinSize = &b
	}
	if strings.TrimSpace(c.MaxSize) != "" {
		b, err := humanfmt.ParseBytes(c.MaxSize)
		if err != nil {
			return conditions{}, fmt.Errorf("invalid max_size: %w", err)
		}
		out.MaxSize = &b
	}
	return out, nil
}

// discoverJobCandidatesWithDebug discovers candidates across all active sources.
// When dbg is provided, it collects best-effort debug entries for skip reasons.
func (p *planner) discoverJobCandidatesWithDebug(ctx context.Context, j config.MoverJobConfig, dbg *debugCollector) ([]candidate, error) {
	trigType := strings.TrimSpace(j.Trigger.Type)
	aw := j.Trigger.AllowedWindow
	if aw != nil && trigType == "usage" && !p.opts.Force {
		inside, _, err := inAllowedWindow(p.now(), aw.Start, aw.End)
		if err != nil {
			return nil, err
		}
		if !inside {
			if dbg != nil {
				dbg.add(PlanDebugEntry{JobName: j.Name, Reason: "job_skipped", Detail: "outside allowed_window"})
			}
			return nil, nil
		}
	}

	srcIDs, err := p.expandRefs(j.Source.Paths, j.Source.Groups)
	if err != nil {
		return nil, err
	}
	activeSources, err := p.activeSourcesForJob(trigType, j, srcIDs)
	if err != nil {
		return nil, err
	}
	if len(activeSources) == 0 {
		if dbg != nil {
			if trigType == "usage" && !p.opts.Force {
				tStart := j.Trigger.ThresholdStart
				for _, id := range srcIDs {
					sp := p.storageByID[id]
					pct, err := p.usagePct(sp.Path)
					if err != nil {
						dbg.add(PlanDebugEntry{JobName: j.Name, StorageID: id, Reason: "usage_check_failed", Detail: err.Error()})
						continue
					}
					dbg.add(PlanDebugEntry{JobName: j.Name, StorageID: id, Reason: "usage_below_threshold", Detail: fmt.Sprintf("usage=%.0f%% threshold_start=%d%%", pct, tStart)})
				}
			} else {
				dbg.add(PlanDebugEntry{JobName: j.Name, Reason: "no_active_sources"})
			}
		}
		return nil, nil
	}

	matcher, err := pathmatch.NewMatcher(j.Source.Patterns)
	if err != nil {
		return nil, fmt.Errorf("failed to compile patterns: %w", err)
	}
	ignore, err := pathmatch.NewMatcher(j.Source.Ignore)
	if err != nil {
		return nil, fmt.Errorf("failed to compile ignore patterns: %w", err)
	}
	conds, err := parseConditions(j.Conditions)
	if err != nil {
		return nil, err
	}

	all := []candidate{}
	for _, srcID := range activeSources {
		cands, err := p.discoverCandidatesOneSource(ctx, j.Name, srcID, matcher, ignore, conds, dbg)
		if err != nil {
			return nil, err
		}
		all = append(all, cands...)
		if p.opts.Limit > 0 && int64(len(all)) >= int64(p.opts.Limit) {
			return all[:p.opts.Limit], nil
		}
	}
	return all, nil
}

// discoverCandidatesOneSource walks one source storage root and returns candidates.
// When dbg is provided, it collects best-effort debug entries for skip reasons.
func (p *planner) discoverCandidatesOneSource(ctx context.Context, jobName string, storageID string, matcher *pathmatch.Matcher, ignore *pathmatch.Matcher, conds conditions, dbg *debugCollector) ([]candidate, error) {
	sp, ok := p.storageByID[storageID]
	if !ok {
		return nil, &errkind.NotFoundError{Msg: fmt.Sprintf("unknown storage id: %s", storageID)}
	}
	root := strings.TrimSpace(sp.Path)
	if root == "" {
		return nil, &errkind.RequiredError{Msg: fmt.Sprintf("storage path is required: storage_id=%s", storageID)}
	}

	// Ensure source exists.
	if _, err := os.Stat(root); err != nil {
		if dbg != nil {
			dbg.add(PlanDebugEntry{JobName: jobName, StorageID: storageID, Path: "", Reason: "source_stat_failed", Detail: err.Error()})
		}
		return nil, fmt.Errorf("failed to stat source: %w", err)
	}

	now := p.now()
	out := []candidate{}

	walkFn := func(pth string, d fs.DirEntry, err error) error {
		if err != nil {
			// Best-effort: skip unreadable entries.
			if dbg != nil {
				dbg.add(PlanDebugEntry{JobName: jobName, StorageID: storageID, Path: filepath.ToSlash(strings.TrimPrefix(pth, root+string(filepath.Separator))), Reason: "walk_error", Detail: err.Error()})
			}
			//nolint:nilerr // v1 mover behavior: skip and continue.
			return nil
		}
		if err := ctx.Err(); err != nil {
			return fmt.Errorf("walk canceled: %w", err)
		}

		rel, err := filepath.Rel(root, pth)
		if err != nil {
			return fmt.Errorf("failed to compute relative path: %w", err)
		}
		rel = filepath.ToSlash(rel)
		if rel == "." {
			rel = ""
		}
		rel = strings.TrimPrefix(rel, "/")
		rel = strings.TrimSuffix(rel, "/")

		if rel == "" {
			return nil
		}

		// Prune directories that cannot match.
		if d.IsDir() {
			if ignore != nil && ignore.Match(rel) {
				if dbg != nil {
					dbg.add(PlanDebugEntry{JobName: jobName, StorageID: storageID, Path: rel, Reason: "ignored_by_ignore"})
				}
				return fs.SkipDir
			}
			if !matcher.CanMatchDescendant(rel) && !matcher.Match(rel) {
				if dbg != nil {
					dbg.add(PlanDebugEntry{JobName: jobName, StorageID: storageID, Path: rel, Reason: "dir_ignored", Detail: "cannot match descendant"})
				}
				return fs.SkipDir
			}
			return nil
		}

		if ignore != nil && ignore.Match(rel) {
			if dbg != nil {
				dbg.add(PlanDebugEntry{JobName: jobName, StorageID: storageID, Path: rel, Reason: "ignored_by_ignore"})
			}
			return nil
		}

		if !matcher.Match(rel) {
			if dbg != nil {
				dbg.add(PlanDebugEntry{JobName: jobName, StorageID: storageID, Path: rel, Reason: "pattern_mismatch"})
			}
			return nil
		}

		info, err := d.Info()
		if err != nil {
			if dbg != nil {
				dbg.add(PlanDebugEntry{JobName: jobName, StorageID: storageID, Path: rel, Reason: "info_failed", Detail: err.Error()})
			}
			//nolint:nilerr // v1 mover behavior: skip and continue.
			return nil
		}
		st, _ := info.Sys().(*syscall.Stat_t)
		if st == nil {
			if dbg != nil {
				dbg.add(PlanDebugEntry{JobName: jobName, StorageID: storageID, Path: rel, Reason: "stat_missing"})
			}
			//nolint:nilerr // v1 mover behavior: skip and continue.
			return nil
		}

		mode := uint32(st.Mode)
		if mode&syscall.S_IFMT != syscall.S_IFREG {
			if dbg != nil {
				dbg.add(PlanDebugEntry{JobName: jobName, StorageID: storageID, Path: rel, Reason: "not_regular_file"})
			}
			return nil
		}

		sz := info.Size()
		mtime := info.ModTime()
		if conds.MinAge != nil {
			if now.Sub(mtime) < *conds.MinAge {
				if dbg != nil {
					dbg.add(PlanDebugEntry{JobName: jobName, StorageID: storageID, Path: rel, Reason: "min_age_not_met"})
				}
				return nil
			}
		}
		if conds.MinSize != nil {
			if sz < *conds.MinSize {
				if dbg != nil {
					dbg.add(PlanDebugEntry{JobName: jobName, StorageID: storageID, Path: rel, Reason: "min_size_not_met"})
				}
				return nil
			}
		}
		if conds.MaxSize != nil {
			if sz > *conds.MaxSize {
				if dbg != nil {
					dbg.add(PlanDebugEntry{JobName: jobName, StorageID: storageID, Path: rel, Reason: "max_size_exceeded"})
				}
				return nil
			}
		}

		out = append(out, candidate{
			SrcStorageID: storageID,
			RelPath:      rel,
			SizeBytes:    sz,
			MTimeSec:     mtime.Unix(),
			Mode:         mode,
			UID:          st.Uid,
			GID:          st.Gid,
			Dev:          uint64(st.Dev),
			Ino:          st.Ino,
		})

		return nil
	}

	if err := filepath.WalkDir(root, walkFn); err != nil {
		return nil, fmt.Errorf("failed to walk source: %w", err)
	}

	// Smart-ish: move largest files first to reduce number of moves needed for freeing space.
	sort.Slice(out, func(i, j int) bool {
		if out[i].SizeBytes != out[j].SizeBytes {
			return out[i].SizeBytes > out[j].SizeBytes
		}
		return out[i].MTimeSec < out[j].MTimeSec
	})

	return out, nil
}
