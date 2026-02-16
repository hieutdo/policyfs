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

// discoverJobCandidates discovers candidates across all active sources for counting.
func (p *planner) discoverJobCandidates(ctx context.Context, j config.MoverJobConfig) ([]candidate, error) {
	trigType := strings.TrimSpace(j.Trigger.Type)
	aw := j.AllowedWindow
	if aw == nil {
		aw = j.Trigger.AllowedWindow
	}
	if aw != nil && trigType == "usage" && !p.opts.Force {
		inside, _, err := inAllowedWindow(p.now(), aw.Start, aw.End)
		if err != nil {
			return nil, err
		}
		if !inside {
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
		return nil, nil
	}

	matcher, err := pathmatch.NewMatcher(j.Source.Patterns)
	if err != nil {
		return nil, fmt.Errorf("failed to compile patterns: %w", err)
	}
	conds, err := parseConditions(j.Conditions)
	if err != nil {
		return nil, err
	}

	all := []candidate{}
	for _, srcID := range activeSources {
		cands, err := p.discoverCandidatesOneSource(ctx, srcID, matcher, conds)
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
func (p *planner) discoverCandidatesOneSource(ctx context.Context, storageID string, matcher *pathmatch.Matcher, conds conditions) ([]candidate, error) {
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
		return nil, fmt.Errorf("failed to stat source: %w", err)
	}

	now := p.now()
	out := []candidate{}

	walkFn := func(pth string, d fs.DirEntry, err error) error {
		if err != nil {
			// Best-effort: skip unreadable entries.
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
			if !matcher.CanMatchDescendant(rel) && !matcher.Match(rel) {
				return fs.SkipDir
			}
			return nil
		}

		if !matcher.Match(rel) {
			return nil
		}

		info, err := d.Info()
		if err != nil {
			//nolint:nilerr // v1 mover behavior: skip and continue.
			return nil
		}
		st, _ := info.Sys().(*syscall.Stat_t)
		if st == nil {
			//nolint:nilerr // v1 mover behavior: skip and continue.
			return nil
		}

		mode := uint32(st.Mode)
		if mode&syscall.S_IFMT != syscall.S_IFREG {
			return nil
		}

		sz := info.Size()
		mtime := info.ModTime()
		if conds.MinAge != nil {
			if now.Sub(mtime) < *conds.MinAge {
				return nil
			}
		}
		if conds.MinSize != nil {
			if sz < *conds.MinSize {
				return nil
			}
		}
		if conds.MaxSize != nil {
			if sz > *conds.MaxSize {
				return nil
			}
		}

		out = append(out, candidate{
			SrcStorageID: storageID,
			RelPath:      rel,
			SizeBytes:    sz,
			MTimeSec:     mtime.Unix(),
			Mode:         mode,
			UID:          uint32(st.Uid),
			GID:          uint32(st.Gid),
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
