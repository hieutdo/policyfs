package mover

import (
	"context"
	"errors"
	"fmt"
	"os"
	"path"
	"path/filepath"
	"sort"
	"strings"
	"syscall"
	"time"

	"github.com/hieutdo/policyfs/internal/config"
	"github.com/hieutdo/policyfs/internal/errkind"
	"github.com/hieutdo/policyfs/internal/eventlog"
	"github.com/hieutdo/policyfs/internal/indexdb"
	"github.com/hieutdo/policyfs/internal/pathmatch"
)

// runJob runs a single mover job.
func (p *planner) runJob(ctx context.Context, j config.MoverJobConfig, hooks Hooks, db *indexdb.DB, limit int64, movedSoFar int64) (JobResult, error) {
	jobStart := time.Now()
	jr := JobResult{Name: j.Name}

	if err := ctx.Err(); err != nil {
		return jr, fmt.Errorf("move canceled: %w", err)
	}

	trigType := strings.TrimSpace(j.Trigger.Type)
	if trigType == "" {
		return jr, &errkind.RequiredError{Msg: "config: mover job trigger.type is required"}
	}

	aw := j.Trigger.AllowedWindow
	var winEnd time.Time
	finishCurrent := true
	if aw != nil && aw.FinishCurrent != nil {
		finishCurrent = *aw.FinishCurrent
	}
	if aw != nil && trigType == "usage" && !p.opts.Force {
		inside, end, err := inAllowedWindow(p.now(), aw.Start, aw.End)
		if err != nil {
			return jr, err
		}
		winEnd = end
		if !inside {
			jr.DurationMS = time.Since(jobStart).Milliseconds()
			return jr, nil
		}
	}

	srcIDs, err := p.expandRefs(j.Source.Paths, j.Source.Groups)
	if err != nil {
		return jr, err
	}
	dstIDs, err := p.expandRefs(j.Destination.Paths, j.Destination.Groups)
	if err != nil {
		return jr, err
	}

	activeSources, err := p.activeSourcesForJob(trigType, j, srcIDs)
	if err != nil {
		return jr, err
	}
	if len(activeSources) == 0 {
		jr.DurationMS = time.Since(jobStart).Milliseconds()
		return jr, nil
	}

	matcher, err := pathmatch.NewMatcher(j.Source.Patterns)
	if err != nil {
		return jr, fmt.Errorf("failed to compile patterns: %w", err)
	}
	ignore, err := pathmatch.NewMatcher(j.Source.Ignore)
	if err != nil {
		return jr, fmt.Errorf("failed to compile ignore patterns: %w", err)
	}

	conds, err := parseConditions(j.Conditions)
	if err != nil {
		return jr, err
	}

	thresholdStop := j.Trigger.ThresholdStop
	stopJob := false

	for _, srcID := range activeSources {
		if limit > 0 && movedSoFar+jr.FilesMoved >= limit {
			break
		}

		cands, err := p.discoverCandidatesOneSource(ctx, j.Name, srcID, matcher, ignore, conds, nil)
		if err != nil {
			return jr, err
		}
		jr.TotalCandidates += int64(len(cands))
		for _, c := range cands {
			if err := ctx.Err(); err != nil {
				return jr, fmt.Errorf("move canceled: %w", err)
			}
			if stopJob {
				break
			}
			if aw != nil && trigType == "usage" && !p.opts.Force && !winEnd.IsZero() {
				now := p.now()
				if now.Equal(winEnd) || now.After(winEnd) {
					break
				}
			}

			if limit > 0 && movedSoFar+jr.FilesMoved >= limit {
				break
			}
			dr, err := p.selectDestinations(j, dstIDs, c)
			if err != nil {
				jr.FilesErrored++
				if hooks.Warn != nil {
					hooks.Warn(j.Name, c.SrcStorageID, c.RelPath, err)
				}
				continue
			}
			dests := dr.choices

			srcRoot := p.storageByID[c.SrcStorageID].Path
			srcPhys := filepath.Join(srcRoot, c.RelPath)

			if p.opts.DryRun {
				if hooks.FileStart != nil && len(dests) > 0 {
					hooks.FileStart(j.Name, c.SrcStorageID, dests[0].id, c.RelPath, c.SizeBytes)
				}
				jr.FilesMoved++
				jr.BytesMoved += c.SizeBytes
				if hooks.Progress != nil {
					hooks.Progress(j.Name, dests[0].id, c.RelPath)
				}
				continue
			}

			if hooks.FileStart != nil && len(dests) > 0 {
				hooks.FileStart(j.Name, c.SrcStorageID, dests[0].id, c.RelPath, c.SizeBytes)
			}

			movedThis := false
			var dstID string
			var dstRoot string
			var dstPhys string
			var lastErr error
			for _, d := range dests {
				dstID = d.id
				dstRoot = d.root
				dstPhys = filepath.Join(dstRoot, c.RelPath)
				fileCtx := ctx
				var cancel func()
				if aw != nil && trigType == "usage" && !p.opts.Force && !finishCurrent && !winEnd.IsZero() {
					fileCtx, cancel = context.WithDeadline(ctx, winEnd)
				}
				var copyProgress func(phase string, doneBytes int64, totalBytes int64)
				if hooks.CopyProgress != nil {
					copyProgress = func(phase string, doneBytes int64, totalBytes int64) {
						hooks.CopyProgress(j.Name, dstID, c.RelPath, phase, doneBytes, totalBytes)
					}
				}
				err := copyFileWithVerifyRetry(fileCtx, srcPhys, dstPhys, c, jobVerifyEnabled(j), defaultCopyRetries, copyProgress)
				if cancel != nil {
					cancel()
				}
				if err == nil {
					movedThis = true
					break
				}
				lastErr = err
				if errors.Is(err, context.DeadlineExceeded) {
					stopJob = true
					movedThis = false
					break
				}
				if errors.Is(err, syscall.ENOSPC) {
					// Try next destination.
					continue
				}
				if errors.Is(err, os.ErrNotExist) {
					// If the source itself disappeared, skip immediately instead of trying other dests.
					if _, serr := os.Stat(srcPhys); errors.Is(serr, os.ErrNotExist) {
						jr.FilesSkipped++
						if hooks.Warn != nil {
							hooks.Warn(j.Name, c.SrcStorageID, c.RelPath, fmt.Errorf("source disappeared: %w", err))
						}
						lastErr = nil
						movedThis = false
						break
					}
					continue
				}
				if _, ok := errors.AsType[*skipError](err); ok {
					jr.FilesSkipped++
					movedThis = false
					break
				}
				jr.FilesErrored++
				if hooks.Warn != nil {
					hooks.Warn(j.Name, c.SrcStorageID, c.RelPath, err)
				}
				movedThis = false
				break
			}
			if !movedThis {
				if stopJob {
					continue
				}
				if lastErr != nil {
					if errors.Is(lastErr, syscall.ENOSPC) || errors.Is(lastErr, os.ErrNotExist) {
						jr.FilesSkipped++
						if hooks.Warn != nil {
							hooks.Warn(j.Name, c.SrcStorageID, c.RelPath, lastErr)
						}
						continue
					}
				}
				continue
			}

			// Update index DB for destination if indexed.
			dstSP := p.storageByID[dstID]
			if dstSP.Indexed {
				if db == nil {
					return jr, &errkind.NilError{What: "index db"}
				}
				sz := c.SizeBytes
				err := db.UpsertFile(ctx, dstID, c.RelPath, false, &sz, c.MTimeSec, c.Mode, c.UID, c.GID)
				if err != nil {
					jr.FilesErrored++
					if hooks.Warn != nil {
						hooks.Warn(j.Name, c.SrcStorageID, c.RelPath, err)
					}
					_ = syscall.Unlink(dstPhys)
					continue
				}
			}

			// Delete source if configured.
			deletedSourcePhysical := false
			deletedSourceDeferred := false
			srcSP := p.storageByID[c.SrcStorageID]
			if jobDeleteSourceEnabled(j) {
				if srcSP.Indexed {
					if db == nil {
						return jr, &errkind.NilError{What: "index db"}
					}
					updated, err := db.MarkDeleted(ctx, c.SrcStorageID, c.RelPath, false)
					if err != nil {
						jr.FilesErrored++
						if hooks.Warn != nil {
							hooks.Warn(j.Name, c.SrcStorageID, c.RelPath, err)
						}
						continue
					}
					if updated {
						err := eventlog.Append(ctx, p.mountName, eventlog.DeleteEvent{Type: eventlog.TypeDelete, StorageID: c.SrcStorageID, Path: c.RelPath, IsDir: false, TS: p.now().Unix()})
						if err != nil {
							jr.FilesErrored++
							if hooks.Warn != nil {
								hooks.Warn(j.Name, c.SrcStorageID, c.RelPath, err)
							}
							continue
						}
						deletedSourceDeferred = true
						// Space will be freed when prune runs.
						jr.BytesFreed += c.SizeBytes
					}
				} else {
					if err := syscall.Unlink(srcPhys); err != nil {
						if !errors.Is(err, syscall.ENOENT) {
							jr.FilesErrored++
							if hooks.Warn != nil {
								hooks.Warn(j.Name, c.SrcStorageID, c.RelPath, err)
							}
							continue
						}
					} else {
						deletedSourcePhysical = true
						jr.BytesFreed += c.SizeBytes
					}
				}
			}

			if jobDeleteSourceEnabled(j) && jobDeleteEmptyDirEnabled(j) {
				if srcSP.Indexed {
					if deletedSourceDeferred {
						if db == nil {
							return jr, &errkind.NilError{What: "index db"}
						}
						if err := deleteEmptyDirsIndexed(ctx, db, p.mountName, c.SrcStorageID, c.RelPath, p.now); err != nil {
							if hooks.Warn != nil {
								hooks.Warn(j.Name, c.SrcStorageID, c.RelPath, err)
							}
						}
					}
				} else {
					if deletedSourcePhysical {
						if err := deleteEmptyDirsNonIndexed(srcSP.Path, filepath.Dir(srcPhys)); err != nil {
							if hooks.Warn != nil {
								hooks.Warn(j.Name, c.SrcStorageID, c.RelPath, err)
							}
						}
					}
				}
			}

			jr.FilesMoved++
			jr.BytesMoved += c.SizeBytes
			if hooks.Progress != nil {
				hooks.Progress(j.Name, dstID, c.RelPath)
			}

			// Hysteresis stop check: for usage triggers, stop moving from this source once it drops <= threshold_stop.
			if trigType == "usage" && !p.opts.Force {
				pct, err := p.usagePct(srcRoot)
				if err == nil {
					if pct <= float64(thresholdStop) {
						break
					}
				}
			}
		}
		if stopJob {
			break
		}
	}

	jr.DurationMS = time.Since(jobStart).Milliseconds()
	return jr, nil
}

// jobVerifyEnabled returns the effective verify bool for a job.
func jobVerifyEnabled(j config.MoverJobConfig) bool {
	if j.Verify == nil {
		return false
	}
	return *j.Verify
}

// jobDeleteSourceEnabled returns the effective delete_source bool for a job.
func jobDeleteSourceEnabled(j config.MoverJobConfig) bool {
	if j.DeleteSource == nil {
		return true
	}
	return *j.DeleteSource
}

// jobDeleteEmptyDirEnabled returns the effective delete_empty_dir bool for a job.
func jobDeleteEmptyDirEnabled(j config.MoverJobConfig) bool {
	if j.DeleteEmptyDir == nil {
		return true
	}
	return *j.DeleteEmptyDir
}

// deleteEmptyDirsNonIndexed removes empty directories upward from startDir until root.
// It is best-effort: ENOTEMPTY/ENOENT stop traversal without error.
func deleteEmptyDirsNonIndexed(root string, startDir string) error {
	root = filepath.Clean(strings.TrimSpace(root))
	startDir = filepath.Clean(strings.TrimSpace(startDir))
	if root == "" || startDir == "" {
		return nil
	}
	if root == startDir {
		return nil
	}

	for {
		if startDir == root {
			return nil
		}
		if !strings.HasPrefix(startDir, root+string(filepath.Separator)) {
			return nil
		}
		err := syscall.Rmdir(startDir)
		if err == nil {
			startDir = filepath.Dir(startDir)
			continue
		}
		if errors.Is(err, syscall.ENOENT) {
			startDir = filepath.Dir(startDir)
			continue
		}
		if errors.Is(err, syscall.ENOTEMPTY) {
			return nil
		}
		if errors.Is(err, syscall.EPERM) || errors.Is(err, syscall.EACCES) {
			return fmt.Errorf("failed to rmdir empty dir: %w", err)
		}
		return fmt.Errorf("failed to rmdir empty dir: %w", err)
	}
}

// deleteEmptyDirsIndexed marks empty dirs as deleted in indexdb and appends DELETE events (IsDir=true)
// so prune can remove them physically later.
func deleteEmptyDirsIndexed(ctx context.Context, db *indexdb.DB, mountName string, storageID string, relFilePath string, now func() time.Time) error {
	if db == nil {
		return &errkind.NilError{What: "index db"}
	}
	storageID = strings.TrimSpace(storageID)
	if storageID == "" {
		return &errkind.RequiredError{What: "storage id"}
	}

	// relFilePath uses '/' separators.
	dir := path.Dir(strings.TrimPrefix(strings.TrimSpace(relFilePath), "/"))
	if dir == "." || dir == "/" {
		dir = ""
	}
	for strings.TrimSpace(dir) != "" {
		updated, err := db.MarkDeleted(ctx, storageID, dir, true)
		if err != nil {
			if errors.Is(err, syscall.ENOTEMPTY) {
				return nil
			}
			return fmt.Errorf("failed to mark dir deleted: %w", err)
		}
		if !updated {
			return nil
		}
		ts := time.Now().Unix()
		if now != nil {
			ts = now().Unix()
		}
		if err := eventlog.Append(ctx, mountName, eventlog.DeleteEvent{Type: eventlog.TypeDelete, StorageID: storageID, Path: dir, IsDir: true, TS: ts}); err != nil {
			return fmt.Errorf("failed to append eventlog: %w", err)
		}

		parent := path.Dir(dir)
		if parent == "." || parent == "/" {
			parent = ""
		}
		dir = parent
	}
	return nil
}

// activeSourcesForJob returns the ordered set of sources to process given trigger mode.
func (p *planner) activeSourcesForJob(triggerType string, j config.MoverJobConfig, srcIDs []string) ([]string, error) {
	triggerType = strings.TrimSpace(triggerType)
	if p.opts.Force {
		return append([]string{}, srcIDs...), nil
	}
	if triggerType == "manual" {
		return append([]string{}, srcIDs...), nil
	}
	if triggerType != "usage" {
		return nil, &errkind.InvalidError{Msg: fmt.Sprintf("invalid mover trigger type: %s", triggerType)}
	}

	tStart := j.Trigger.ThresholdStart

	type srcUsage struct {
		id  string
		pct float64
	}
	usages := []srcUsage{}
	for _, id := range srcIDs {
		sp := p.storageByID[id]
		pct, err := p.usagePct(sp.Path)
		if err != nil {
			return nil, err
		}
		if pct >= float64(tStart) {
			usages = append(usages, srcUsage{id: id, pct: pct})
		}
	}

	sort.Slice(usages, func(i, j int) bool { return usages[i].pct > usages[j].pct })
	out := make([]string, 0, len(usages))
	for _, u := range usages {
		out = append(out, u.id)
	}
	return out, nil
}
