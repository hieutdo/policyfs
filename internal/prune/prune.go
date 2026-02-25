package prune

import (
	"context"
	"errors"
	"fmt"
	"io"
	"os"
	"path/filepath"
	"runtime"
	"strings"
	"syscall"
	"time"

	"github.com/hieutdo/policyfs/internal/config"
	"github.com/hieutdo/policyfs/internal/errkind"
	"github.com/hieutdo/policyfs/internal/eventlog"
	"github.com/hieutdo/policyfs/internal/indexdb"
	"golang.org/x/sys/unix"
)

const (
	utimeOmitNsec = int64((1 << 30) - 2)
	maxWarnings   = 1000
)

// Summary describes the outcome of one prune invocation.
type Summary struct {
	Mount           string
	EventsProcessed int64
	EventsSucceeded int64
	EventsSkipped   int64
	EventsFailed    int64
	ByType          map[eventlog.Type]int64
	DurationMS      int64
	Truncated       bool
	Warnings        []string
}

// Opts controls prune behavior.
type Opts struct {
	DryRun bool
	Limit  int
}

// VerboseEvent describes one processed event for verbose output.
type VerboseEvent struct {
	Type      eventlog.Type
	StorageID string
	Path      string
	OldPath   string
	NewPath   string
	DryRun    bool
	Result    string // ok|skipped|failed
}

// Hooks provides optional callbacks for prune progress reporting.
type Hooks struct {
	// Verbose is called after each parsed event is handled.
	Verbose func(e VerboseEvent)
}

// RunOneshot processes deferred event log mutations for a mount.
func RunOneshot(ctx context.Context, mountName string, mountCfg *config.MountConfig, opts Opts, hooks Hooks) (Summary, error) {
	start := time.Now()
	out := Summary{Mount: mountName, ByType: map[eventlog.Type]int64{}}

	storageRoots := map[string]string{}
	if mountCfg != nil {
		for _, sp := range mountCfg.StoragePaths {
			storageRoots[sp.ID] = sp.Path
		}
	}

	db, err := indexdb.Open(mountName)
	if err != nil {
		return out, fmt.Errorf("failed to open index db: %w", err)
	}
	defer func() { _ = db.Close() }()

	off, err := eventlog.ReadOffset(mountName)
	if err != nil {
		return out, fmt.Errorf("failed to read event offset: %w", err)
	}

	r, err := eventlog.OpenReader(mountName, off)
	if err != nil {
		return out, fmt.Errorf("failed to open event log reader: %w", err)
	}
	defer func() { _ = r.Close() }()

	processed := int64(0)
	for opts.Limit <= 0 || int(processed) < opts.Limit {
		line, nextOffset, err := r.Next()
		if err != nil {
			if errors.Is(err, io.EOF) {
				break
			}
			return out, fmt.Errorf("failed to read next event: %w", err)
		}

		processed++
		out.EventsProcessed++

		ev, parseErr := eventlog.Parse(line)
		if parseErr != nil {
			out.EventsFailed++
			if len(out.Warnings) < maxWarnings {
				out.Warnings = append(out.Warnings, "invalid event json")
			}
			if !opts.DryRun {
				if err := eventlog.WriteOffset(mountName, nextOffset); err != nil {
					return out, fmt.Errorf("failed to write event offset: %w", err)
				}
			}
			continue
		}

		typ := ev.EventType()
		out.ByType[typ]++

		res, warnings, advance, retryLater, handleErr := applyOneEvent(ctx, storageRoots, db, ev, opts.DryRun)
		out.EventsSucceeded += res.succeeded
		out.EventsSkipped += res.skipped
		out.EventsFailed += res.failed
		for _, w := range warnings {
			if len(out.Warnings) >= maxWarnings {
				break
			}
			out.Warnings = append(out.Warnings, w)
		}

		if hooks.Verbose != nil {
			hooks.Verbose(buildVerboseEvent(ev, opts.DryRun, res))
		}

		if retryLater {
			return out, handleErr
		}
		if !advance {
			return out, handleErr
		}
		if !opts.DryRun {
			if err := eventlog.WriteOffset(mountName, nextOffset); err != nil {
				return out, fmt.Errorf("failed to write event offset: %w", err)
			}
		}
	}

	if !opts.DryRun {
		truncated, err := maybeTruncateEvents(mountName, r.Offset())
		if err != nil {
			return out, err
		}
		out.Truncated = truncated
	}

	out.DurationMS = time.Since(start).Milliseconds()
	return out, nil
}

// buildVerboseEvent converts an event + result to the hook payload.
func buildVerboseEvent(ev eventlog.Event, dryRun bool, res applyResult) VerboseEvent {
	out := VerboseEvent{Type: ev.EventType(), DryRun: dryRun, Result: "failed"}
	if res.succeeded > 0 {
		out.Result = "ok"
	} else if res.skipped > 0 {
		out.Result = "skipped"
	}

	switch e := ev.(type) {
	case eventlog.DeleteEvent:
		out.StorageID = e.StorageID
		out.Path = e.Path
	case eventlog.RenameEvent:
		out.StorageID = e.StorageID
		out.OldPath = e.OldPath
		out.NewPath = e.NewPath
	case eventlog.SetattrEvent:
		out.StorageID = e.StorageID
		out.Path = e.Path
	}
	return out
}

// applyResult captures how one event was handled.
type applyResult struct {
	succeeded int64
	skipped   int64
	failed    int64
}

// applyOneEvent dispatches to per-event-type handlers.
func applyOneEvent(ctx context.Context, storageRoots map[string]string, db *indexdb.DB, ev eventlog.Event, dryRun bool) (res applyResult, warnings []string, advance bool, retryLater bool, err error) {
	if db == nil {
		return applyResult{failed: 1}, nil, false, true, &errkind.NilError{What: "index db"}
	}

	switch e := ev.(type) {
	case eventlog.DeleteEvent:
		return applyDelete(ctx, storageRoots, db, e, dryRun)
	case eventlog.RenameEvent:
		return applyRename(ctx, storageRoots, db, e, dryRun)
	case eventlog.SetattrEvent:
		return applySetattr(ctx, storageRoots, db, e, dryRun)
	default:
		return applyResult{failed: 1}, nil, true, false, &errkind.InvalidError{What: "event type"}
	}
}

// validateEventVirtualPath rejects any event path that could escape the storage root.
func validateEventVirtualPath(rel string) error {
	rel = strings.TrimSpace(rel)
	if rel == "" || rel == "." {
		return &errkind.InvalidError{What: "path"}
	}
	if strings.ContainsRune(rel, 0) {
		return &errkind.InvalidError{What: "path"}
	}
	if strings.HasPrefix(rel, "/") {
		return &errkind.InvalidError{What: "path"}
	}
	if strings.HasSuffix(rel, "/") {
		return &errkind.InvalidError{What: "path"}
	}
	if strings.Contains(rel, "//") {
		return &errkind.InvalidError{What: "path"}
	}
	for part := range strings.SplitSeq(rel, "/") {
		if part == "" || part == "." || part == ".." {
			return &errkind.InvalidError{What: "path"}
		}
	}
	return nil
}

// physicalPathFor resolves a storage_id + virtual path to an absolute physical path.
func physicalPathFor(storageRoots map[string]string, storageID string, rel string) (string, error) {
	root := ""
	if storageRoots != nil {
		root = strings.TrimSpace(storageRoots[storageID])
	}
	if strings.TrimSpace(root) == "" {
		return "", &errkind.NotFoundError{Msg: fmt.Sprintf("storage id not found: %s", storageID)}
	}
	if err := validateEventVirtualPath(rel); err != nil {
		return "", err
	}

	root = filepath.Clean(root)
	if runtime.GOOS != "windows" && !filepath.IsAbs(root) {
		return "", &errkind.InvalidError{What: "storage root"}
	}

	relOS := filepath.FromSlash(rel)
	p := filepath.Clean(filepath.Join(root, relOS))
	rootPrefix := root + string(os.PathSeparator)
	if p != root && !strings.HasPrefix(p, rootPrefix) {
		return "", &errkind.InvalidError{What: "path"}
	}
	return p, nil
}

// applyDelete applies one DELETE event.
func applyDelete(ctx context.Context, storageRoots map[string]string, db *indexdb.DB, e eventlog.DeleteEvent, dryRun bool) (applyResult, []string, bool, bool, error) {
	p, err := physicalPathFor(storageRoots, e.StorageID, e.Path)
	if err != nil {
		if errors.Is(err, errkind.ErrNotFound) {
			w := fmt.Sprintf("storage not found: storage_id=%s path=%s", e.StorageID, e.Path)
			return applyResult{failed: 1}, []string{w}, true, false, nil
		}
		if errors.Is(err, errkind.ErrInvalid) {
			w := fmt.Sprintf("invalid path: storage_id=%s path=%s", e.StorageID, e.Path)
			return applyResult{failed: 1}, []string{w}, true, false, nil
		}
		return applyResult{failed: 1}, nil, false, true, err
	}
	if dryRun {
		return applyResult{succeeded: 1}, nil, true, false, nil
	}

	if e.IsDir {
		err := syscall.Rmdir(p)
		if err != nil {
			if errors.Is(err, syscall.ENOENT) {
				if err := db.FinalizeDelete(ctx, e.StorageID, e.Path, e.IsDir); err != nil {
					return applyResult{failed: 1}, nil, false, true, fmt.Errorf("failed to finalize delete: %w", err)
				}
				return applyResult{skipped: 1}, nil, true, false, nil
			}
			if errors.Is(err, syscall.ENOTEMPTY) {
				w := fmt.Sprintf("rmdir not empty: storage_id=%s path=%s", e.StorageID, e.Path)
				return applyResult{failed: 1}, []string{w}, true, false, nil
			}
			if errors.Is(err, syscall.EPERM) || errors.Is(err, syscall.EACCES) {
				w := fmt.Sprintf("rmdir permission denied: storage_id=%s path=%s", e.StorageID, e.Path)
				return applyResult{failed: 1}, []string{w}, true, false, nil
			}
			return applyResult{failed: 1}, nil, false, true, fmt.Errorf("failed to rmdir: %w", err)
		}
	} else {
		err := syscall.Unlink(p)
		if err != nil {
			if errors.Is(err, syscall.ENOENT) {
				if err := db.FinalizeDelete(ctx, e.StorageID, e.Path, e.IsDir); err != nil {
					return applyResult{failed: 1}, nil, false, true, fmt.Errorf("failed to finalize delete: %w", err)
				}
				return applyResult{skipped: 1}, nil, true, false, nil
			}
			if errors.Is(err, syscall.EPERM) || errors.Is(err, syscall.EACCES) {
				w := fmt.Sprintf("unlink permission denied: storage_id=%s path=%s", e.StorageID, e.Path)
				return applyResult{failed: 1}, []string{w}, true, false, nil
			}
			return applyResult{failed: 1}, nil, false, true, fmt.Errorf("failed to unlink: %w", err)
		}
	}
	if err := db.FinalizeDelete(ctx, e.StorageID, e.Path, e.IsDir); err != nil {
		return applyResult{failed: 1}, nil, false, true, fmt.Errorf("failed to finalize delete: %w", err)
	}
	return applyResult{succeeded: 1}, nil, true, false, nil
}

// applyRename applies one RENAME event.
func applyRename(ctx context.Context, storageRoots map[string]string, db *indexdb.DB, e eventlog.RenameEvent, dryRun bool) (applyResult, []string, bool, bool, error) {
	oldP, err := physicalPathFor(storageRoots, e.StorageID, e.OldPath)
	if err != nil {
		if errors.Is(err, errkind.ErrNotFound) {
			w := fmt.Sprintf("storage not found: storage_id=%s old_path=%s new_path=%s", e.StorageID, e.OldPath, e.NewPath)
			return applyResult{failed: 1}, []string{w}, true, false, nil
		}
		if errors.Is(err, errkind.ErrInvalid) {
			w := fmt.Sprintf("invalid path: storage_id=%s old_path=%s new_path=%s", e.StorageID, e.OldPath, e.NewPath)
			return applyResult{failed: 1}, []string{w}, true, false, nil
		}
		return applyResult{failed: 1}, nil, false, true, err
	}
	newP, err := physicalPathFor(storageRoots, e.StorageID, e.NewPath)
	if err != nil {
		if errors.Is(err, errkind.ErrNotFound) {
			w := fmt.Sprintf("storage not found: storage_id=%s old_path=%s new_path=%s", e.StorageID, e.OldPath, e.NewPath)
			return applyResult{failed: 1}, []string{w}, true, false, nil
		}
		if errors.Is(err, errkind.ErrInvalid) {
			w := fmt.Sprintf("invalid path: storage_id=%s old_path=%s new_path=%s", e.StorageID, e.OldPath, e.NewPath)
			return applyResult{failed: 1}, []string{w}, true, false, nil
		}
		return applyResult{failed: 1}, nil, false, true, err
	}
	if dryRun {
		return applyResult{succeeded: 1}, nil, true, false, nil
	}

	if err := os.MkdirAll(filepath.Dir(newP), 0o755); err != nil {
		return applyResult{failed: 1}, nil, false, true, fmt.Errorf("failed to create destination parent: %w", err)
	}

	err = syscall.Rename(oldP, newP)
	if err != nil {
		if errors.Is(err, syscall.ENOENT) {
			if err := db.FinalizeRename(ctx, e.StorageID, e.OldPath, e.NewPath); err != nil {
				return applyResult{failed: 1}, nil, false, true, fmt.Errorf("failed to finalize rename: %w", err)
			}
			return applyResult{skipped: 1}, nil, true, false, nil
		}
		if errors.Is(err, syscall.EPERM) || errors.Is(err, syscall.EACCES) {
			w := fmt.Sprintf("rename permission denied: storage_id=%s old_path=%s new_path=%s", e.StorageID, e.OldPath, e.NewPath)
			return applyResult{failed: 1}, []string{w}, true, false, nil
		}
		if errors.Is(err, syscall.ENOTEMPTY) {
			w := fmt.Sprintf("rename not empty: storage_id=%s old_path=%s new_path=%s", e.StorageID, e.OldPath, e.NewPath)
			return applyResult{failed: 1}, []string{w}, true, false, nil
		}
		return applyResult{failed: 1}, nil, false, true, fmt.Errorf("failed to rename: %w", err)
	}
	if err := db.FinalizeRename(ctx, e.StorageID, e.OldPath, e.NewPath); err != nil {
		return applyResult{failed: 1}, nil, false, true, fmt.Errorf("failed to finalize rename: %w", err)
	}
	return applyResult{succeeded: 1}, nil, true, false, nil
}

// applySetattr applies one SETATTR event.
func applySetattr(ctx context.Context, storageRoots map[string]string, db *indexdb.DB, e eventlog.SetattrEvent, dryRun bool) (applyResult, []string, bool, bool, error) {
	p, err := physicalPathFor(storageRoots, e.StorageID, e.Path)
	if err != nil {
		if errors.Is(err, errkind.ErrNotFound) {
			w := fmt.Sprintf("storage not found: storage_id=%s path=%s", e.StorageID, e.Path)
			return applyResult{failed: 1}, []string{w}, true, false, nil
		}
		if errors.Is(err, errkind.ErrInvalid) {
			w := fmt.Sprintf("invalid path: storage_id=%s path=%s", e.StorageID, e.Path)
			return applyResult{failed: 1}, []string{w}, true, false, nil
		}
		return applyResult{failed: 1}, nil, false, true, err
	}
	if dryRun {
		return applyResult{succeeded: 1}, nil, true, false, nil
	}

	warnings := []string{}
	permFailed := false

	if e.Mode != nil {
		if err := syscall.Chmod(p, *e.Mode); err != nil {
			if errors.Is(err, syscall.ENOENT) {
				if err := db.FinalizeSetattr(ctx, e.StorageID, e.Path); err != nil {
					return applyResult{failed: 1}, nil, false, true, fmt.Errorf("failed to finalize setattr: %w", err)
				}
				return applyResult{skipped: 1}, nil, true, false, nil
			}
			if errors.Is(err, syscall.EPERM) || errors.Is(err, syscall.EACCES) {
				warnings = append(warnings, fmt.Sprintf("chmod permission denied: storage_id=%s path=%s", e.StorageID, e.Path))
				permFailed = true
			} else {
				return applyResult{failed: 1}, nil, false, true, fmt.Errorf("failed to chmod: %w", err)
			}
		}
	}

	if e.UID != nil || e.GID != nil {
		uid := -1
		gid := -1
		if e.UID != nil {
			uid = int(*e.UID)
		}
		if e.GID != nil {
			gid = int(*e.GID)
		}
		if err := syscall.Lchown(p, uid, gid); err != nil {
			if errors.Is(err, syscall.ENOENT) {
				if err := db.FinalizeSetattr(ctx, e.StorageID, e.Path); err != nil {
					return applyResult{failed: 1}, nil, false, true, fmt.Errorf("failed to finalize setattr: %w", err)
				}
				return applyResult{skipped: 1}, nil, true, false, nil
			}
			if errors.Is(err, syscall.EPERM) || errors.Is(err, syscall.EACCES) {
				warnings = append(warnings, fmt.Sprintf("chown permission denied: storage_id=%s path=%s", e.StorageID, e.Path))
				permFailed = true
			} else {
				return applyResult{failed: 1}, nil, false, true, fmt.Errorf("failed to chown: %w", err)
			}
		}
	}

	if e.ATime != nil || e.MTime != nil {
		ta := unix.Timespec{Nsec: utimeOmitNsec}
		tm := unix.Timespec{Nsec: utimeOmitNsec}
		if e.ATime != nil {
			ta.Sec = *e.ATime
			ta.Nsec = 0
		}
		if e.MTime != nil {
			tm.Sec = *e.MTime
			tm.Nsec = 0
		}
		if err := unix.UtimesNanoAt(unix.AT_FDCWD, p, []unix.Timespec{ta, tm}, unix.AT_SYMLINK_NOFOLLOW); err != nil {
			if errors.Is(err, syscall.ENOENT) {
				if err := db.FinalizeSetattr(ctx, e.StorageID, e.Path); err != nil {
					return applyResult{failed: 1}, nil, false, true, fmt.Errorf("failed to finalize setattr: %w", err)
				}
				return applyResult{skipped: 1}, nil, true, false, nil
			}
			if errors.Is(err, syscall.EPERM) || errors.Is(err, syscall.EACCES) {
				warnings = append(warnings, fmt.Sprintf("utimens permission denied: storage_id=%s path=%s", e.StorageID, e.Path))
				permFailed = true
			} else {
				return applyResult{failed: 1}, nil, false, true, fmt.Errorf("failed to utimens: %w", err)
			}
		}
	}

	if err := db.FinalizeSetattr(ctx, e.StorageID, e.Path); err != nil {
		return applyResult{failed: 1}, nil, false, true, fmt.Errorf("failed to finalize setattr: %w", err)
	}
	if permFailed {
		return applyResult{failed: 1}, warnings, true, false, nil
	}
	return applyResult{succeeded: 1}, warnings, true, false, nil
}

// maybeTruncateEvents truncates events.ndjson when offset indicates all events were processed.
//
// It uses flock(LOCK_EX) on the event log fd to coordinate with eventlog.Append (which holds
// LOCK_SH during writes). This eliminates the TOCTOU race where the daemon appends a new event
// between the size check and the truncation.
func maybeTruncateEvents(mountName string, offset int64) (bool, error) {
	if offset == 0 {
		return false, nil
	}

	p, err := eventlog.LogPath(mountName)
	if err != nil {
		return false, fmt.Errorf("failed to resolve event log path: %w", err)
	}

	f, err := os.OpenFile(p, os.O_RDWR, 0)
	if err != nil {
		if errors.Is(err, os.ErrNotExist) {
			return false, nil
		}
		return false, fmt.Errorf("failed to open event log for truncation: %w", err)
	}
	defer func() { _ = f.Close() }()

	// Exclusive lock blocks until all in-flight Appends (LOCK_SH) finish, and prevents
	// new Appends from starting until truncation completes.
	if err := syscall.Flock(int(f.Fd()), syscall.LOCK_EX); err != nil {
		return false, fmt.Errorf("failed to lock event log for truncation: %w", err)
	}

	// Re-stat under the lock so we see any data appended before we acquired it.
	st, err := f.Stat()
	if err != nil {
		return false, fmt.Errorf("failed to stat event log: %w", err)
	}
	if offset != st.Size() {
		return false, nil
	}

	if err := f.Truncate(0); err != nil {
		return false, fmt.Errorf("failed to truncate event log: %w", err)
	}
	if err := eventlog.WriteOffset(mountName, 0); err != nil {
		return false, fmt.Errorf("failed to write event offset: %w", err)
	}
	return true, nil
}
