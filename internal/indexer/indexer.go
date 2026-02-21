package indexer

import (
	"context"
	"database/sql"
	"errors"
	"fmt"
	"io/fs"
	"os"
	"path"
	"path/filepath"
	"strings"
	"syscall"
	"time"

	"github.com/hieutdo/policyfs/internal/config"
	"github.com/hieutdo/policyfs/internal/errkind"
	"github.com/hieutdo/policyfs/internal/pathmatch"
	"github.com/mattn/go-sqlite3"
)

const (
	batchSize         = 1000
	dbWriteMaxRetries = 3
	dbWriteRetryDelay = 50 * time.Millisecond
)

var (
	errMissingStat = errkind.SentinelError("missing stat")
)

// Result contains summary stats for one index run.
type Result struct {
	Mount           string          `json:"mount"`
	StoragePaths    []StorageResult `json:"storage_paths"`
	TotalDirs       int64           `json:"-"`
	TotalFiles      int64           `json:"total_files"`
	TotalBytes      int64           `json:"total_bytes"`
	TotalDurationMS int64           `json:"total_duration_ms"`
}

// StorageResult contains summary stats for one indexed storage path.
type StorageResult struct {
	ID           string `json:"id"`
	Upserts      int64  `json:"-"`
	DirsScanned  int64  `json:"dirs_scanned"`
	FilesScanned int64  `json:"files_scanned"`
	BytesScanned int64  `json:"bytes_scanned"`
	StaleRemoved int64  `json:"stale_removed"`
	DurationMS   int64  `json:"duration_ms"`
	Warnings     int64  `json:"warnings"`
	Errors       int64  `json:"errors"`
}

// Hooks provides optional callbacks for progress and warnings.
type Hooks struct {
	// Progress is called for each scanned entry when verbose mode is enabled.
	Progress func(storageID string, rel string, isDir bool)
	// Warn is called for non-fatal scan issues.
	Warn func(storageID string, rel string, err error)
}

// entryRow is the DB write payload for one filesystem entry.
type entryRow struct {
	path      string
	realPath  string
	parentDir string
	name      string
	isDir     bool
	size      sql.NullInt64
	mtimeSec  int64
	mode      uint32
	uid       uint32
	gid       uint32
}

// dirAgg is the in-memory aggregate state used to update directory aggregates.
type dirAgg struct {
	path       string
	fileCount  int64
	totalFiles int64
	totalBytes int64
}

// Run indexes all indexed storage paths for a mount.
func Run(ctx context.Context, mountName string, mountCfg *config.MountConfig, db *sql.DB, hooks Hooks) (Result, error) {
	if mountName == "" {
		return Result{}, &errkind.RequiredError{What: "mount name"}
	}
	if mountCfg == nil {
		return Result{}, &errkind.NilError{What: "mount config"}
	}
	if db == nil {
		return Result{}, &errkind.NilError{What: "db"}
	}

	indexed := []config.StoragePath{}
	for _, sp := range mountCfg.StoragePaths {
		if sp.Indexed {
			indexed = append(indexed, sp)
		}
	}

	res := Result{Mount: mountName, StoragePaths: make([]StorageResult, 0, len(indexed))}

	for _, sp := range indexed {
		sr, err := indexOne(ctx, db, sp, mountCfg, hooks)
		if err != nil {
			return Result{}, err
		}
		res.StoragePaths = append(res.StoragePaths, sr)
		res.TotalDirs += sr.DirsScanned
		res.TotalFiles += sr.FilesScanned
		res.TotalBytes += sr.BytesScanned
		res.TotalDurationMS += sr.DurationMS
	}

	return res, nil
}

// indexOne indexes a single storage path into the files table.
func indexOne(ctx context.Context, db *sql.DB, sp config.StoragePath, mountCfg *config.MountConfig, hooks Hooks) (StorageResult, error) {
	if mountCfg == nil {
		return StorageResult{}, &errkind.NilError{What: "mount config"}
	}
	if strings.TrimSpace(sp.ID) == "" {
		return StorageResult{}, &errkind.RequiredError{What: "storage id"}
	}
	if strings.TrimSpace(sp.Path) == "" {
		return StorageResult{}, &errkind.RequiredError{What: "storage path"}
	}

	if _, err := os.Stat(sp.Path); err != nil {
		if hooks.Warn != nil {
			hooks.Warn(sp.ID, "", err)
		}
		return StorageResult{
			ID:           sp.ID,
			DirsScanned:  0,
			Upserts:      0,
			FilesScanned: 0,
			BytesScanned: 0,
			StaleRemoved: 0,
			DurationMS:   0,
			Warnings:     1,
			Errors:       0,
		}, nil
	}

	start := time.Now()
	runID, err := allocateRunID(ctx, db, sp.ID)
	if err != nil {
		return StorageResult{}, err
	}

	aggs := map[string]*dirAgg{}
	ensureDirAgg := func(virtualDir string) {
		if virtualDir == "." {
			virtualDir = ""
		}
		if _, ok := aggs[virtualDir]; ok {
			return
		}
		aggs[virtualDir] = &dirAgg{path: virtualDir}
	}

	rows := make([]entryRow, 0, batchSize)
	flush := func() error {
		if len(rows) == 0 {
			return nil
		}
		if err := upsertEntriesBatch(ctx, db, sp.ID, runID, rows); err != nil {
			return err
		}
		rows = rows[:0]
		return nil
	}

	var dirsScanned int64
	var filesScanned int64
	var bytesScanned int64
	warnings := int64(0)
	tombstoneAllowed := true

	ignore := mountCfg.Indexer.Ignore
	ignoreMatcher, err := pathmatch.NewMatcher(ignore)
	if err != nil {
		return StorageResult{}, fmt.Errorf("failed to compile ignore patterns: %w", err)
	}

	warn := func(rel string, err error) {
		warnings++
		if shouldSkipTombstone(err) {
			tombstoneAllowed = false
		}
		if hooks.Warn != nil {
			hooks.Warn(sp.ID, rel, err)
		}
	}

	walkFn := func(p string, d fs.DirEntry, err error) error {
		if err != nil {
			rel := p
			if v, relErr := filepath.Rel(sp.Path, p); relErr == nil {
				rel = filepath.ToSlash(v)
				if rel == "." {
					rel = ""
				}
				rel = strings.TrimPrefix(rel, "/")
				rel = strings.TrimSuffix(rel, "/")
			}
			warn(rel, err)
			// v1 behavior: log warning, skip and continue indexing.
			return nil
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

		if d.Type()&os.ModeSymlink != 0 {
			if _, err := os.Stat(p); err != nil {
				warn(rel, err)
			}
			return nil
		}

		info, err := d.Info()
		if err != nil {
			warn(rel, err)
			//nolint:nilerr // v1 behavior: log warning, skip and continue indexing.
			return nil
		}
		st, _ := info.Sys().(*syscall.Stat_t)
		mtimeSec := info.ModTime().Unix()
		if st == nil {
			warn(rel, errMissingStat)
			return nil
		}
		uid := uint32(st.Uid)
		gid := uint32(st.Gid)

		mode := uint32(st.Mode)
		parent := path.Dir(rel)
		if parent == "." {
			parent = ""
		}
		name := path.Base(rel)
		if rel == "" {
			name = ""
		}

		if d.IsDir() {
			if rel != "" && hooks.Progress != nil {
				hooks.Progress(sp.ID, rel, true)
			}
			if rel != "" {
				dirsScanned++
			}
			ensureDirAgg(rel)
			rows = append(rows, entryRow{
				path:      rel,
				realPath:  rel,
				parentDir: parent,
				name:      name,
				isDir:     true,
				size:      sql.NullInt64{Valid: false},
				mtimeSec:  mtimeSec,
				mode:      mode,
				uid:       uid,
				gid:       gid,
			})
			return nil
		}

		if mode&syscall.S_IFMT != syscall.S_IFREG {
			// Index only regular files for v1.
			return nil
		}
		if rel != "" && hooks.Progress != nil {
			hooks.Progress(sp.ID, rel, false)
		}

		sz := info.Size()
		rows = append(rows, entryRow{
			path:      rel,
			realPath:  rel,
			parentDir: parent,
			name:      name,
			isDir:     false,
			size:      sql.NullInt64{Int64: sz, Valid: true},
			mtimeSec:  mtimeSec,
			mode:      mode,
			uid:       uid,
			gid:       gid,
		})
		filesScanned++
		bytesScanned += sz

		ensureDirAgg(parent)
		aggs[parent].fileCount++

		anc := parent
		for {
			ensureDirAgg(anc)
			aggs[anc].totalFiles++
			aggs[anc].totalBytes += sz
			if anc == "" {
				break
			}
			anc = path.Dir(anc)
			if anc == "." {
				anc = ""
			}
		}

		if len(rows) >= batchSize {
			if err := flush(); err != nil {
				return err
			}
		}

		return nil
	}

	if err := filepath.WalkDir(sp.Path, walkFn); err != nil {
		return StorageResult{}, fmt.Errorf("failed to walk storage: %w", err)
	}
	if err := flush(); err != nil {
		return StorageResult{}, err
	}

	staleRemoved := int64(0)
	if tombstoneAllowed {
		var err error
		staleRemoved, err = tombstoneStale(ctx, db, sp.ID, runID)
		if err != nil {
			return StorageResult{}, err
		}
	}
	if err := updateDirAggregates(ctx, db, sp.ID, runID, aggs); err != nil {
		return StorageResult{}, err
	}

	dur := time.Since(start)
	if err := updateIndexerState(ctx, db, sp.ID, runID, dur, filesScanned, bytesScanned); err != nil {
		return StorageResult{}, err
	}

	upserts := filesScanned + dirsScanned

	return StorageResult{
		ID:           sp.ID,
		Upserts:      upserts,
		DirsScanned:  dirsScanned,
		FilesScanned: filesScanned,
		BytesScanned: bytesScanned,
		StaleRemoved: staleRemoved,
		DurationMS:   dur.Milliseconds(),
		Warnings:     warnings,
		Errors:       0,
	}, nil
}

// shouldSkipTombstone reports whether a warning implies the scan was incomplete.
func shouldSkipTombstone(err error) bool {
	if err == nil {
		return false
	}
	if errors.Is(err, os.ErrNotExist) {
		return false
	}
	if errors.Is(err, os.ErrPermission) {
		return true
	}
	if errors.Is(err, errMissingStat) {
		return true
	}
	return true
}

// retryDBWrite retries a write operation unless the error is non-retryable.
func retryDBWrite(ctx context.Context, op string, fn func() error) error {
	var lastErr error
	for attempt := 1; attempt <= dbWriteMaxRetries; attempt++ {
		if err := ctx.Err(); err != nil {
			return fmt.Errorf("failed to %s: %w", op, err)
		}
		if err := fn(); err != nil {
			lastErr = err
			if !shouldRetryDBWrite(err) {
				return fmt.Errorf("failed to %s: %w", op, err)
			}
			if attempt == dbWriteMaxRetries {
				return fmt.Errorf("failed to %s after %d attempts: %w", op, attempt, err)
			}
			time.Sleep(dbWriteRetryDelay)
			continue
		}
		return nil
	}
	return fmt.Errorf("failed to %s: %w", op, lastErr)
}

// shouldRetryDBWrite reports whether a DB write error should be retried.
func shouldRetryDBWrite(err error) bool {
	if err == nil {
		return false
	}
	if errors.Is(err, context.Canceled) || errors.Is(err, context.DeadlineExceeded) {
		return false
	}
	if isSQLiteFull(err) {
		return false
	}
	return true
}

// isSQLiteFull reports whether a sqlite error indicates a disk-full condition.
func isSQLiteFull(err error) bool {
	sqliteErr, ok := errors.AsType[sqlite3.Error](err)
	if !ok {
		return false
	}
	return sqliteErr.Code == sqlite3.ErrFull
}

// allocateRunID increments indexer_state.current_run_id and returns the new run ID.
func allocateRunID(ctx context.Context, db *sql.DB, storageID string) (int64, error) {
	tx, err := db.BeginTx(ctx, nil)
	if err != nil {
		return 0, fmt.Errorf("failed to begin tx: %w", err)
	}
	defer func() { _ = tx.Rollback() }()

	_, err = tx.ExecContext(ctx, `INSERT INTO indexer_state (storage_id, current_run_id) VALUES (?, 0)
ON CONFLICT(storage_id) DO NOTHING;`, storageID)
	if err != nil {
		return 0, fmt.Errorf("failed to ensure indexer_state row: %w", err)
	}

	var cur int64
	if err := tx.QueryRowContext(ctx, `SELECT current_run_id FROM indexer_state WHERE storage_id = ?;`, storageID).Scan(&cur); err != nil {
		return 0, fmt.Errorf("failed to read run id: %w", err)
	}

	next := cur + 1
	if _, err := tx.ExecContext(ctx, `UPDATE indexer_state SET current_run_id = ? WHERE storage_id = ?;`, next, storageID); err != nil {
		return 0, fmt.Errorf("failed to update run id: %w", err)
	}

	if err := tx.Commit(); err != nil {
		return 0, fmt.Errorf("failed to commit run id: %w", err)
	}
	return next, nil
}

// upsertEntriesBatch inserts or updates entry rows in one transaction with retries.
func upsertEntriesBatch(ctx context.Context, db *sql.DB, storageID string, runID int64, rows []entryRow) error {
	return retryDBWrite(ctx, "upsert file batch", func() error {
		return upsertEntriesBatchOnce(ctx, db, storageID, runID, rows)
	})
}

// upsertEntriesBatchOnce inserts or updates entry rows in one transaction.
func upsertEntriesBatchOnce(ctx context.Context, db *sql.DB, storageID string, runID int64, rows []entryRow) error {
	tx, err := db.BeginTx(ctx, nil)
	if err != nil {
		return fmt.Errorf("failed to begin tx: %w", err)
	}
	defer func() { _ = tx.Rollback() }()

	updateStmt, err := tx.PrepareContext(ctx, `UPDATE files
SET
    size = ?,
    mtime = ?,
    mode = ?,
    uid = ?,
    gid = ?,
    is_dir = ?,
    last_seen_run_id = ?
WHERE storage_id = ?
  AND real_path = ?
  AND real_path != path;`)
	if err != nil {
		return fmt.Errorf("failed to prepare update: %w", err)
	}
	defer func() { _ = updateStmt.Close() }()

	upsertStmt, err := tx.PrepareContext(ctx, `INSERT INTO files (
	    storage_id, path, real_path, parent_dir, name, is_dir,
	    size, mtime, mode, uid, gid,
	    deleted, last_seen_run_id,
	    file_count, total_files, total_bytes
	)
	VALUES (
	    ?, ?, ?, ?, ?, ?,
	    ?, ?, ?, ?, ?,
	    0, ?,
	    0, 0, 0
	)
	ON CONFLICT (storage_id, path) DO UPDATE SET
	    parent_dir = excluded.parent_dir,
	    name = excluded.name,
	    real_path = CASE
	        WHEN files.real_path != '' AND files.real_path != files.path THEN files.real_path
	        ELSE excluded.real_path
	    END,
	    is_dir = excluded.is_dir,
	    size = excluded.size,
	    mtime = excluded.mtime,
	    mode = excluded.mode,
	    uid = excluded.uid,
	    gid = excluded.gid,
	    deleted = CASE
	        WHEN files.deleted = 1 THEN 1
	        ELSE 0
	    END,
	    last_seen_run_id = excluded.last_seen_run_id;`)
	if err != nil {
		return fmt.Errorf("failed to prepare upsert: %w", err)
	}
	defer func() { _ = upsertStmt.Close() }()

	for _, r := range rows {
		var size any
		if r.size.Valid {
			size = r.size.Int64
		} else {
			size = nil
		}
		isDir := 0
		if r.isDir {
			isDir = 1
		}

		res, err := updateStmt.ExecContext(ctx, size, r.mtimeSec, r.mode, r.uid, r.gid, isDir, runID, storageID, r.realPath)
		if err != nil {
			return fmt.Errorf("failed to update pending rename: %w", err)
		}
		n, _ := res.RowsAffected()
		if n > 0 {
			continue
		}

		if _, err := upsertStmt.ExecContext(
			ctx,
			storageID, r.path, r.realPath, r.parentDir, r.name, isDir,
			size, r.mtimeSec, r.mode, r.uid, r.gid,
			runID,
		); err != nil {
			return fmt.Errorf("failed to upsert file: %w", err)
		}
	}

	if err := tx.Commit(); err != nil {
		return fmt.Errorf("failed to commit file batch: %w", err)
	}
	return nil
}

// tombstoneStale marks entries from older runs as deleted and returns how many were changed.
func tombstoneStale(ctx context.Context, db *sql.DB, storageID string, runID int64) (int64, error) {
	r, err := db.ExecContext(ctx, `UPDATE files
SET deleted = 2
WHERE storage_id = ?
  AND deleted != 1
  AND (real_path = path OR real_path = '')
  AND (last_seen_run_id IS NULL OR last_seen_run_id < ?);`, storageID, runID)
	if err != nil {
		return 0, fmt.Errorf("failed to tombstone stale entries: %w", err)
	}
	n, _ := r.RowsAffected()
	return n, nil
}

// updateDirAggregates updates directory aggregate columns for a storage.
func updateDirAggregates(ctx context.Context, db *sql.DB, storageID string, runID int64, aggs map[string]*dirAgg) error {
	tx, err := db.BeginTx(ctx, nil)
	if err != nil {
		return fmt.Errorf("failed to begin tx: %w", err)
	}
	defer func() { _ = tx.Rollback() }()

	stmt, err := tx.PrepareContext(ctx, `UPDATE files
SET file_count = ?, total_files = ?, total_bytes = ?,
    deleted = CASE WHEN deleted = 1 THEN 1 ELSE 0 END,
    last_seen_run_id = ?
WHERE storage_id = ? AND path = ? AND is_dir = 1;`)
	if err != nil {
		return fmt.Errorf("failed to prepare dir aggregate update: %w", err)
	}
	defer func() { _ = stmt.Close() }()

	for _, a := range aggs {
		if _, err := stmt.ExecContext(ctx, a.fileCount, a.totalFiles, a.totalBytes, runID, storageID, a.path); err != nil {
			return fmt.Errorf("failed to update dir aggregates: %w", err)
		}
	}

	if err := tx.Commit(); err != nil {
		return fmt.Errorf("failed to commit dir aggregates: %w", err)
	}
	return nil
}

// updateIndexerState records summary for a completed run.
func updateIndexerState(ctx context.Context, db *sql.DB, storageID string, runID int64, dur time.Duration, fileCount int64, totalBytes int64) error {
	_, err := db.ExecContext(ctx, `UPDATE indexer_state
SET current_run_id = ?, last_completed = ?, last_duration_ms = ?, file_count = ?, total_bytes = ?
WHERE storage_id = ?;`, runID, time.Now().Unix(), dur.Milliseconds(), fileCount, totalBytes, storageID)
	if err != nil {
		return fmt.Errorf("failed to update indexer_state: %w", err)
	}
	return nil
}
