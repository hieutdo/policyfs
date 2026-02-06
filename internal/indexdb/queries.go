package indexdb

import (
	"context"
	"database/sql"
	"errors"
	"fmt"
	"strings"
	"syscall"
)

// File describes one indexed filesystem entry.
type File struct {
	StorageID string
	Path      string
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
		return File{}, false, ErrIndexDBNil
	}
	storageID = strings.TrimSpace(storageID)
	path = strings.TrimSpace(path)
	if storageID == "" {
		return File{}, false, nil
	}

	q := `SELECT
    f.storage_id,
    f.path,
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
	if err := row.Scan(&out.StorageID, &out.Path, &isDir, &size, &out.MTimeSec, &mode, &uid, &gid); err != nil {
		if errors.Is(err, sql.ErrNoRows) {
			return File{}, false, nil
		}
		return File{}, false, fmt.Errorf("failed to query file: %w", err)
	}
	out.IsDir = isDir != 0
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
		return false, ErrIndexDBNil
	}
	storageID = strings.TrimSpace(storageID)
	path = strings.TrimSpace(path)
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
		return nil, false, ErrIndexDBNil
	}
	storageID = strings.TrimSpace(storageID)
	dirPath = strings.TrimSpace(dirPath)
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
