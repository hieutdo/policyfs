package indexer

import (
	"context"
	"fmt"
	"io/fs"
	"os"
	"path/filepath"
	"strings"
	"time"

	"github.com/hieutdo/policyfs/internal/config"
	"github.com/hieutdo/policyfs/internal/errkind"
	"github.com/hieutdo/policyfs/internal/pathmatch"
)

// CountResult contains summary stats for the counting phase used by progress UI.
type CountResult struct {
	Mount           string         `json:"mount"`
	StoragePaths    []StorageCount `json:"storage_paths"`
	TotalFiles      int64          `json:"total_files"`
	TotalDurationMS int64          `json:"total_duration_ms"`
}

// StorageCount contains summary stats for counting one storage path.
type StorageCount struct {
	ID           string `json:"id"`
	FilesCounted int64  `json:"files_counted"`
	DurationMS   int64  `json:"duration_ms"`
}

// Count counts regular files across all indexed storage paths for a mount.
//
// This is a best-effort, "fast walk" phase used only for progress percentage/ETA.
// It intentionally avoids stat calls and does not emit warnings.
func Count(ctx context.Context, mountName string, mountCfg *config.MountConfig) (CountResult, error) {
	if mountName == "" {
		return CountResult{}, &errkind.RequiredError{What: "mount name"}
	}
	if mountCfg == nil {
		return CountResult{}, &errkind.NilError{What: "mount config"}
	}

	indexed, err := mountCfg.GetIndexedStoragePaths()
	if err != nil {
		return CountResult{}, fmt.Errorf("failed to get indexed storage paths: %w", err)
	}

	start := time.Now()
	out := CountResult{Mount: mountName, StoragePaths: make([]StorageCount, 0, len(indexed))}
	for _, sp := range indexed {
		sc, err := countOne(ctx, sp, mountCfg)
		if err != nil {
			return CountResult{}, err
		}
		out.StoragePaths = append(out.StoragePaths, sc)
		out.TotalFiles += sc.FilesCounted
	}
	out.TotalDurationMS = time.Since(start).Milliseconds()
	return out, nil
}

// countOne counts regular files in a single storage path.
func countOne(ctx context.Context, sp config.StoragePath, mountCfg *config.MountConfig) (StorageCount, error) {
	if mountCfg == nil {
		return StorageCount{}, &errkind.NilError{What: "mount config"}
	}
	if strings.TrimSpace(sp.ID) == "" {
		return StorageCount{}, &errkind.RequiredError{What: "storage id"}
	}
	if strings.TrimSpace(sp.Path) == "" {
		return StorageCount{}, &errkind.RequiredError{What: "storage path"}
	}

	// Best-effort: if the storage root is missing/unreadable, treat it as 0 files.
	if _, err := os.Stat(sp.Path); err != nil {
		return StorageCount{ID: sp.ID, FilesCounted: 0, DurationMS: 0}, nil //nolint:nilerr
	}

	ignore := mountCfg.Indexer.Ignore
	ignoreMatcher, err := pathmatch.NewMatcher(ignore)
	if err != nil {
		return StorageCount{}, fmt.Errorf("failed to compile ignore patterns: %w", err)
	}
	filesCounted := int64(0)
	start := time.Now()

	walkFn := func(p string, d fs.DirEntry, err error) error {
		if ctx != nil {
			if cerr := ctx.Err(); cerr != nil {
				return fmt.Errorf("count canceled: %w", cerr)
			}
		}
		if err != nil {
			// Best-effort: skip and continue.
			return nil //nolint:nilerr
		}

		rel, err := filepath.Rel(sp.Path, p)
		if err != nil {
			return fmt.Errorf("failed to compute relative path: %w", err)
		}
		rel = filepath.ToSlash(rel)
		if rel == "." {
			rel = ""
		}
		rel = strings.TrimPrefix(rel, "/")
		rel = strings.TrimSuffix(rel, "/")

		if ignoreMatcher.Match(rel) {
			if d.IsDir() {
				return fs.SkipDir
			}
			return nil
		}

		if d.IsDir() {
			return nil
		}

		// Count only regular files. DirEntry.Type() returns type bits when available.
		if d.Type()&fs.ModeType != 0 {
			return nil
		}
		filesCounted++
		return nil
	}

	if err := filepath.WalkDir(sp.Path, walkFn); err != nil {
		// Best-effort: if the walk fails at the root, treat it as 0.
		return StorageCount{ID: sp.ID, FilesCounted: 0, DurationMS: time.Since(start).Milliseconds()}, nil //nolint:nilerr
	}

	return StorageCount{ID: sp.ID, FilesCounted: filesCounted, DurationMS: time.Since(start).Milliseconds()}, nil
}
