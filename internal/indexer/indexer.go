package indexer

import (
	"context"
	"database/sql"
	"errors"
	"fmt"
	"io/fs"
	"path"
	"path/filepath"
	"strings"
	"syscall"
	"time"

	"github.com/hieutdo/policyfs/internal/config"
	"github.com/rs/zerolog"
)

const (
	batchSize = 1000
)

// Result contains summary stats for one index run.
type Result struct {
	Mount           string          `json:"mount"`
	StoragePaths    []StorageResult `json:"storage_paths"`
	TotalFiles      int64           `json:"total_files"`
	TotalBytes      int64           `json:"total_bytes"`
	TotalDurationMS int64           `json:"total_duration_ms"`
}

// StorageResult contains summary stats for one indexed storage path.
type StorageResult struct {
	ID           string `json:"id"`
	FilesIndexed int64  `json:"files_indexed"`
	BytesIndexed int64  `json:"bytes_indexed"`
	StaleRemoved int64  `json:"stale_removed"`
	DurationMS   int64  `json:"duration_ms"`
	Warnings     int64  `json:"warnings"`
	Errors       int64  `json:"errors"`
}

// entryRow is the DB write payload for one filesystem entry.
type entryRow struct {
	path      string
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
func Run(ctx context.Context, mountName string, mountCfg *config.MountConfig, db *sql.DB, log zerolog.Logger) (Result, error) {
	if mountName == "" {
		return Result{}, errors.New("mount name is required")
	}
	if mountCfg == nil {
		return Result{}, errors.New("mount config is nil")
	}
	if db == nil {
		return Result{}, errors.New("db is nil")
	}

	indexed := []config.StoragePath{}
	for _, sp := range mountCfg.StoragePaths {
		if sp.Indexed {
			indexed = append(indexed, sp)
		}
	}

	jobLog := log.With().Str("component", "indexer").Str("op", "index").Str("mount", mountName).Logger()

	res := Result{Mount: mountName, StoragePaths: make([]StorageResult, 0, len(indexed))}
	start := time.Now()

	for _, sp := range indexed {
		sr, err := indexOne(ctx, db, sp, mountCfg, jobLog)
		if err != nil {
			return Result{}, err
		}
		res.StoragePaths = append(res.StoragePaths, sr)
		res.TotalFiles += sr.FilesIndexed
		res.TotalBytes += sr.BytesIndexed
		res.TotalDurationMS += sr.DurationMS
	}

	jobLog.Info().Int64("dur_ms", time.Since(start).Milliseconds()).Msg("index finished")
	return res, nil
}

// indexOne indexes a single storage path into the files table.
func indexOne(ctx context.Context, db *sql.DB, sp config.StoragePath, mountCfg *config.MountConfig, log zerolog.Logger) (StorageResult, error) {
	if mountCfg == nil {
		return StorageResult{}, errors.New("mount config is nil")
	}
	if strings.TrimSpace(sp.ID) == "" {
		return StorageResult{}, errors.New("storage id is required")
	}
	if strings.TrimSpace(sp.Path) == "" {
		return StorageResult{}, errors.New("storage path is required")
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

	var filesIndexed int64
	var bytesIndexed int64
	warnings := int64(0)

	ignore := mountCfg.Indexer.Ignore

	walkFn := func(p string, d fs.DirEntry, err error) error {
		if err != nil {
			return err
		}

		entryName := d.Name()
		for _, pat := range ignore {
			pat = strings.TrimSpace(pat)
			if pat == "" {
				continue
			}
			if ok, _ := filepath.Match(pat, entryName); ok {
				if d.IsDir() {
					return fs.SkipDir
				}
				return nil
			}
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

		info, err := d.Info()
		if err != nil {
			warnings++
			//nolint:nilerr // v1 behavior: log warning, skip and continue indexing.
			return nil
		}
		st, _ := info.Sys().(*syscall.Stat_t)
		mtimeSec := info.ModTime().Unix()
		if st == nil {
			warnings++
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
			ensureDirAgg(rel)
			rows = append(rows, entryRow{
				path:      rel,
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

		sz := info.Size()
		rows = append(rows, entryRow{
			path:      rel,
			parentDir: parent,
			name:      name,
			isDir:     false,
			size:      sql.NullInt64{Int64: sz, Valid: true},
			mtimeSec:  mtimeSec,
			mode:      mode,
			uid:       uid,
			gid:       gid,
		})
		filesIndexed++
		bytesIndexed += sz

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

	staleRemoved, err := tombstoneStale(ctx, db, sp.ID, runID)
	if err != nil {
		return StorageResult{}, err
	}
	if err := updateDirAggregates(ctx, db, sp.ID, runID, aggs); err != nil {
		return StorageResult{}, err
	}

	dur := time.Since(start)
	if err := updateIndexerState(ctx, db, sp.ID, runID, dur, filesIndexed, bytesIndexed); err != nil {
		return StorageResult{}, err
	}

	log.Info().Str("storage_id", sp.ID).Int64("dur_ms", dur.Milliseconds()).Msg("storage indexed")

	return StorageResult{
		ID:           sp.ID,
		FilesIndexed: filesIndexed,
		BytesIndexed: bytesIndexed,
		StaleRemoved: staleRemoved,
		DurationMS:   dur.Milliseconds(),
		Warnings:     warnings,
		Errors:       0,
	}, nil
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

// upsertEntriesBatch inserts or updates entry rows in one transaction.
func upsertEntriesBatch(ctx context.Context, db *sql.DB, storageID string, runID int64, rows []entryRow) error {
	tx, err := db.BeginTx(ctx, nil)
	if err != nil {
		return fmt.Errorf("failed to begin tx: %w", err)
	}
	defer func() { _ = tx.Rollback() }()

	stmt, err := tx.PrepareContext(ctx, `INSERT INTO files (
    storage_id, path, parent_dir, name, is_dir,
    size, mtime, mode, uid, gid,
    deleted, last_seen_run_id,
    file_count, total_files, total_bytes
)
VALUES (
    ?, ?, ?, ?, ?,
    ?, ?, ?, ?, ?,
    0, ?,
    0, 0, 0
)
ON CONFLICT (storage_id, path) DO UPDATE SET
    parent_dir = excluded.parent_dir,
    name = excluded.name,
    is_dir = excluded.is_dir,
    size = excluded.size,
    mtime = excluded.mtime,
    mode = excluded.mode,
    uid = excluded.uid,
    gid = excluded.gid,
    deleted = 0,
    last_seen_run_id = excluded.last_seen_run_id;`)
	if err != nil {
		return fmt.Errorf("failed to prepare upsert: %w", err)
	}
	defer func() { _ = stmt.Close() }()

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
		if _, err := stmt.ExecContext(ctx, storageID, r.path, r.parentDir, r.name, isDir, size, r.mtimeSec, r.mode, r.uid, r.gid, runID); err != nil {
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
SET deleted = 1
WHERE storage_id = ?
  AND deleted = 0
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
SET file_count = ?, total_files = ?, total_bytes = ?, deleted = 0, last_seen_run_id = ?
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
