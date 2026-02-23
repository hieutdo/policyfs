package indexdb

import (
	"context"
	"database/sql"
	"errors"
	"fmt"
	"os"
	"path/filepath"
	"strings"
	"syscall"

	"github.com/hieutdo/policyfs/internal/config"
	"github.com/hieutdo/policyfs/internal/errkind"
)

// IndexerStateRow holds summary stats from the indexer_state table.
type IndexerStateRow struct {
	StorageID      string
	CurrentRunID   int64
	LastCompleted  *int64 // unix timestamp, nil if never completed
	LastDurationMS *int64
	FileCount      *int64
	TotalBytes     *int64
}

// QueryIndexerState opens the index DB read-only and returns indexer_state for a storage.
// Returns nil (no error) if the DB file does not exist or the storage has no row.
func QueryIndexerState(mountName string, storageID string) (*IndexerStateRow, error) {
	if strings.TrimSpace(mountName) == "" {
		return nil, &errkind.RequiredError{What: "mount name"}
	}
	if strings.TrimSpace(storageID) == "" {
		return nil, &errkind.RequiredError{What: "storage id"}
	}

	dbPath := filepath.Join(config.MountStateDir(mountName), "index.db")
	if _, err := os.Stat(dbPath); err != nil {
		if errors.Is(err, os.ErrNotExist) {
			return nil, nil
		}
		return nil, fmt.Errorf("failed to stat index db: %w", err)
	}

	dsn := fmt.Sprintf("file:%s?mode=ro&_busy_timeout=%d", dbPath, busyTimeoutMS)
	conn, err := sql.Open("sqlite3", dsn)
	if err != nil {
		return nil, fmt.Errorf("failed to open index db: %w", err)
	}
	defer func() { _ = conn.Close() }()

	row := conn.QueryRowContext(context.Background(),
		`SELECT current_run_id, last_completed, last_duration_ms, file_count, total_bytes
		 FROM indexer_state WHERE storage_id = ?;`, storageID)

	var r IndexerStateRow
	r.StorageID = storageID
	if err := row.Scan(&r.CurrentRunID, &r.LastCompleted, &r.LastDurationMS, &r.FileCount, &r.TotalBytes); err != nil {
		if errors.Is(err, sql.ErrNoRows) {
			return nil, nil
		}
		return nil, fmt.Errorf("failed to query indexer_state: %w", err)
	}
	return &r, nil
}

// QueryStaleCount returns the number of stale (deleted=2) files for a storage.
// Returns 0 if the DB file does not exist.
func QueryStaleCount(mountName string, storageID string) (int64, error) {
	if strings.TrimSpace(mountName) == "" {
		return 0, &errkind.RequiredError{What: "mount name"}
	}
	if strings.TrimSpace(storageID) == "" {
		return 0, &errkind.RequiredError{What: "storage id"}
	}

	dbPath := filepath.Join(config.MountStateDir(mountName), "index.db")
	if _, err := os.Stat(dbPath); err != nil {
		if errors.Is(err, os.ErrNotExist) {
			return 0, nil
		}
		return 0, fmt.Errorf("failed to stat index db: %w", err)
	}

	dsn := fmt.Sprintf("file:%s?mode=ro&_busy_timeout=%d", dbPath, busyTimeoutMS)
	conn, err := sql.Open("sqlite3", dsn)
	if err != nil {
		return 0, fmt.Errorf("failed to open index db: %w", err)
	}
	defer func() { _ = conn.Close() }()

	var count int64
	if err := conn.QueryRowContext(context.Background(),
		`SELECT COUNT(*) FROM files WHERE storage_id = ? AND deleted = 2;`, storageID).Scan(&count); err != nil {
		return 0, fmt.Errorf("failed to count stale files: %w", err)
	}
	return count, nil
}

// InspectFileRow holds raw and meta-overlay data for one file entry.
// Unlike GetEffectiveFile, it does NOT filter by deleted status and
// searches across all storages for a given path.
type InspectFileRow struct {
	StorageID     string
	Path          string
	RealPath      string
	IsDir         bool
	Size          *int64
	MTimeSec      int64
	Mode          uint32
	UID           uint32
	GID           uint32
	Deleted       int    // 0=live, 1=pending-delete, 2=stale
	LastSeenRunID *int64 // which indexer run last saw this entry

	// file_meta overrides (nil = no override active).
	MetaMTime *int64
	MetaMode  *uint32
	MetaUID   *uint32
	MetaGID   *uint32

	// From indexer_state (for correlating last_seen_run_id).
	CurrentRunID  int64
	LastCompleted *int64 // unix timestamp of last completed index run
}

// QueryFileInspect opens the index DB read-only and returns all rows matching
// the given path across ALL storages, regardless of deleted status.
// It joins file_meta for overrides and indexer_state for run correlation.
// Returns nil (no error) if the DB file does not exist.
func QueryFileInspect(mountName string, path string) ([]InspectFileRow, error) {
	if strings.TrimSpace(mountName) == "" {
		return nil, &errkind.RequiredError{What: "mount name"}
	}
	path = normalizeVirtualPath(path)
	if path == "" {
		return nil, &errkind.RequiredError{What: "path"}
	}

	dbPath := filepath.Join(config.MountStateDir(mountName), "index.db")
	if _, err := os.Stat(dbPath); err != nil {
		if errors.Is(err, os.ErrNotExist) {
			return nil, nil
		}
		return nil, fmt.Errorf("failed to stat index db: %w", err)
	}

	dsn := fmt.Sprintf("file:%s?mode=ro&_busy_timeout=%d", dbPath, busyTimeoutMS)
	conn, err := sql.Open("sqlite3", dsn)
	if err != nil {
		return nil, fmt.Errorf("failed to open index db: %w", err)
	}
	defer func() { _ = conn.Close() }()

	q := `SELECT
    f.storage_id, f.path, f.real_path, f.is_dir, f.size,
    f.mtime, f.mode, f.uid, f.gid, f.deleted, f.last_seen_run_id,
    m.meta_mtime, m.meta_mode, m.meta_uid, m.meta_gid,
    COALESCE(s.current_run_id, 0), s.last_completed
FROM files f
LEFT JOIN file_meta m
    ON f.storage_id = m.storage_id AND f.path = m.path
LEFT JOIN indexer_state s
    ON f.storage_id = s.storage_id
WHERE f.path = ?
ORDER BY f.storage_id;`

	rows, err := conn.QueryContext(context.Background(), q, path)
	if err != nil {
		return nil, fmt.Errorf("failed to query file inspect: %w", err)
	}
	defer func() { _ = rows.Close() }()

	var results []InspectFileRow
	for rows.Next() {
		var r InspectFileRow
		var isDir int64
		var size sql.NullInt64
		var mode, uid, gid int64
		var deleted int64
		var metaMode, metaUID, metaGID sql.NullInt64
		if err := rows.Scan(
			&r.StorageID, &r.Path, &r.RealPath, &isDir, &size,
			&r.MTimeSec, &mode, &uid, &gid, &deleted, &r.LastSeenRunID,
			&r.MetaMTime, &metaMode, &metaUID, &metaGID,
			&r.CurrentRunID, &r.LastCompleted,
		); err != nil {
			return nil, fmt.Errorf("failed to scan file inspect row: %w", err)
		}
		r.IsDir = isDir != 0
		if size.Valid {
			r.Size = &size.Int64
		}
		r.Mode = uint32(mode)
		r.UID = uint32(uid)
		r.GID = uint32(gid)
		r.Deleted = int(deleted)
		if metaMode.Valid {
			v := uint32(metaMode.Int64)
			r.MetaMode = &v
		}
		if metaUID.Valid {
			v := uint32(metaUID.Int64)
			r.MetaUID = &v
		}
		if metaGID.Valid {
			v := uint32(metaGID.Int64)
			r.MetaGID = &v
		}
		if strings.TrimSpace(r.RealPath) == "" {
			r.RealPath = r.Path
		}
		results = append(results, r)
	}
	if err := rows.Err(); err != nil {
		return nil, fmt.Errorf("failed to iterate file inspect rows: %w", err)
	}
	return results, nil
}

// VirtualChildEntry describes one virtual directory child entry.
type VirtualChildEntry struct {
	Name  string
	IsDir bool
}

// QueryVirtualChildren lists distinct child entries under dirPath across all storages.
// Returns nil (no error) if the DB file does not exist.
func QueryVirtualChildren(mountName string, dirPath string, namePrefix string, limit int) ([]VirtualChildEntry, error) {
	if strings.TrimSpace(mountName) == "" {
		return nil, &errkind.RequiredError{What: "mount name"}
	}
	dirPath = normalizeVirtualPath(dirPath)
	namePrefix = strings.TrimSpace(namePrefix)
	if limit <= 0 {
		limit = 100
	}
	if limit > 1000 {
		limit = 1000
	}

	dbPath := filepath.Join(config.MountStateDir(mountName), "index.db")
	if _, err := os.Stat(dbPath); err != nil {
		if errors.Is(err, os.ErrNotExist) {
			return nil, nil
		}
		return nil, fmt.Errorf("failed to stat index db: %w", err)
	}

	dsn := fmt.Sprintf("file:%s?mode=ro&_busy_timeout=%d", dbPath, busyTimeoutMS)
	conn, err := sql.Open("sqlite3", dsn)
	if err != nil {
		return nil, fmt.Errorf("failed to open index db: %w", err)
	}
	defer func() { _ = conn.Close() }()

	likePrefix := escapeLike(namePrefix) + "%"
	q := `SELECT name, MAX(is_dir)
FROM files
WHERE parent_dir = ?
  AND deleted = 0
  AND name != ''
  AND name LIKE ? ESCAPE '\'
GROUP BY name
ORDER BY name
LIMIT ?;`

	rows, err := conn.QueryContext(context.Background(), q, dirPath, likePrefix, limit)
	if err != nil {
		return nil, fmt.Errorf("failed to query virtual children: %w", err)
	}
	defer func() { _ = rows.Close() }()

	results := []VirtualChildEntry{}
	for rows.Next() {
		var name string
		var isDir int64
		if err := rows.Scan(&name, &isDir); err != nil {
			return nil, fmt.Errorf("failed to scan virtual child: %w", err)
		}
		if name == "" {
			continue
		}
		results = append(results, VirtualChildEntry{Name: name, IsDir: isDir != 0})
	}
	if err := rows.Err(); err != nil {
		return nil, fmt.Errorf("failed to iterate virtual children: %w", err)
	}
	return results, nil
}

// escapeLike escapes SQLite LIKE wildcards in s so it can be used as a prefix.
func escapeLike(s string) string {
	s = strings.ReplaceAll(s, "\\", "\\\\")
	s = strings.ReplaceAll(s, "%", "\\%")
	s = strings.ReplaceAll(s, "_", "\\_")
	return s
}

// File describes one indexed filesystem entry.
type File struct {
	StorageID string
	Path      string
	RealPath  string
	IsDir     bool
	Size      int64
	MTimeSec  int64
	Mode      uint32
	UID       uint32
	GID       uint32
}

// DirEntry describes one directory entry returned from DB-backed READDIR.
type DirEntry struct {
	Name string
	Mode uint32
}

// GetEffectiveFile returns a row merged with any file_meta overrides.
func (d *DB) GetEffectiveFile(ctx context.Context, storageID string, path string) (File, bool, error) {
	if d == nil || d.sqlDB == nil {
		return File{}, false, &errkind.NilError{What: "index db"}
	}
	storageID = strings.TrimSpace(storageID)
	path = normalizeVirtualPath(path)
	if storageID == "" {
		return File{}, false, nil
	}

	q := `SELECT
    f.storage_id,
    f.path,
    f.real_path,
    f.is_dir,
    f.size,
    COALESCE(m.meta_mtime, f.mtime) AS mtime,
    COALESCE(m.meta_mode, f.mode) AS mode,
    COALESCE(m.meta_uid, f.uid) AS uid,
    COALESCE(m.meta_gid, f.gid) AS gid
FROM files f
LEFT JOIN file_meta m
    ON f.storage_id = m.storage_id AND f.path = m.path
WHERE f.storage_id = ?
  AND f.path = ?
  AND f.deleted = 0;`

	row := d.sqlDB.QueryRowContext(ctx, q, storageID, path)
	var out File
	var isDir int64
	var size sql.NullInt64
	var mode int64
	var uid int64
	var gid int64
	if err := row.Scan(&out.StorageID, &out.Path, &out.RealPath, &isDir, &size, &out.MTimeSec, &mode, &uid, &gid); err != nil {
		if errors.Is(err, sql.ErrNoRows) {
			return File{}, false, nil
		}
		return File{}, false, fmt.Errorf("failed to query file: %w", err)
	}
	out.IsDir = isDir != 0
	if strings.TrimSpace(out.RealPath) == "" {
		out.RealPath = out.Path
	}
	if size.Valid {
		out.Size = size.Int64
	} else {
		out.Size = 0
	}
	out.Mode = uint32(mode)
	out.UID = uint32(uid)
	out.GID = uint32(gid)
	return out, true, nil
}

// DirExists reports whether a directory exists in index DB.
func (d *DB) DirExists(ctx context.Context, storageID string, path string) (bool, error) {
	if d == nil || d.sqlDB == nil {
		return false, &errkind.NilError{What: "index db"}
	}
	storageID = strings.TrimSpace(storageID)
	path = normalizeVirtualPath(path)
	if storageID == "" {
		return false, nil
	}
	if path == "" {
		return true, nil
	}

	row := d.sqlDB.QueryRowContext(ctx, `SELECT 1 FROM files WHERE storage_id = ? AND path = ? AND is_dir = 1 AND deleted = 0 LIMIT 1;`, storageID, path)
	var one int
	if err := row.Scan(&one); err != nil {
		if errors.Is(err, sql.ErrNoRows) {
			return false, nil
		}
		return false, fmt.Errorf("failed to query dir: %w", err)
	}
	return true, nil
}

// ListDirEntries lists immediate children under a directory path, including subdirectories.
func (d *DB) ListDirEntries(ctx context.Context, storageID string, dirPath string) ([]DirEntry, bool, error) {
	if d == nil || d.sqlDB == nil {
		return nil, false, &errkind.NilError{What: "index db"}
	}
	storageID = strings.TrimSpace(storageID)
	dirPath = normalizeVirtualPath(dirPath)
	if storageID == "" {
		return nil, false, nil
	}

	// Root always exists from the mounted view.
	if dirPath != "" {
		exists, err := d.DirExists(ctx, storageID, dirPath)
		if err != nil {
			return nil, false, err
		}
		if !exists {
			return nil, false, nil
		}
	}

	entries := []DirEntry{}

	q := `SELECT name, mode FROM files
WHERE storage_id = ?
  AND parent_dir = ?
  AND deleted = 0
  AND name != '';`
	fRows, err := d.sqlDB.QueryContext(ctx, q, storageID, dirPath)
	if err != nil {
		return nil, false, fmt.Errorf("failed to query files: %w", err)
	}
	for fRows.Next() {
		var name string
		var mode int64
		if err := fRows.Scan(&name, &mode); err != nil {
			_ = fRows.Close()
			return nil, false, fmt.Errorf("failed to scan child row: %w", err)
		}
		if name == "" {
			continue
		}
		entries = append(entries, DirEntry{Name: name, Mode: uint32(mode) & uint32(syscall.S_IFMT)})
	}
	if err := fRows.Err(); err != nil {
		_ = fRows.Close()
		return nil, false, fmt.Errorf("failed to iterate files: %w", err)
	}
	_ = fRows.Close()

	return entries, true, nil
}
