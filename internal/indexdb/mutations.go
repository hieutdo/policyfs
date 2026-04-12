package indexdb

import (
	"context"
	"database/sql"
	"errors"
	"fmt"
	"path"
	"strings"
	"syscall"

	"github.com/hieutdo/policyfs/internal/errkind"
)

// escapeLIKE escapes SQL LIKE metacharacters (%, _) in a path so it can be
// safely used in a LIKE pattern with the default escape character '\'.
func escapeLIKE(s string) string {
	s = strings.ReplaceAll(s, `\`, `\\`)
	s = strings.ReplaceAll(s, `%`, `\%`)
	s = strings.ReplaceAll(s, `_`, `\_`)
	return s
}

// NormalizeVirtualPath converts a user-provided virtual path into a stable DB key.
// It removes leading slashes, cleans dot segments, and returns "" for root.
func NormalizeVirtualPath(p string) string {
	return normalizeVirtualPath(p)
}

func normalizeVirtualPath(p string) string {
	p = strings.TrimSpace(p)
	p = path.Clean("/" + p)
	p = strings.TrimPrefix(p, "/")
	if p == "." {
		return ""
	}
	p = strings.TrimPrefix(p, "/")
	p = strings.TrimSuffix(p, "/")
	return p
}

// splitParentName returns the parent directory and base name for a normalized virtual path.
func splitParentName(p string) (parentDir string, name string) {
	if p == "" {
		return "", ""
	}
	parentDir = path.Dir(p)
	if parentDir == "." {
		parentDir = ""
	}
	name = path.Base(p)
	if name == "." {
		name = ""
	}
	return parentDir, name
}

// UpsertFile upserts a file/dir entry while preserving last_seen_run_id for existing rows.
//
// For new rows, it sets last_seen_run_id to the current indexer_state.current_run_id when available.
// It never resurrects deleted=1 tombstones; such conflicts are returned as syscall.EBUSY.
// It also ensures all parent directories exist as is_dir entries so the file becomes visible
// immediately via DB-backed lookup/readdir.
func (d *DB) UpsertFile(ctx context.Context, storageID string, p string, isDir bool, sizeBytes *int64, mtimeSec int64, mode uint32, uid uint32, gid uint32) error {
	if d == nil || d.sqlDB == nil {
		return &errkind.NilError{What: "index db"}
	}
	storageID = strings.TrimSpace(storageID)
	p = normalizeVirtualPath(p)
	if storageID == "" {
		return &errkind.RequiredError{What: "storage id"}
	}
	if p == "" {
		return syscall.EPERM
	}

	tx, err := d.sqlDB.BeginTx(ctx, nil)
	if err != nil {
		return fmt.Errorf("failed to begin tx: %w", err)
	}
	defer func() { _ = tx.Rollback() }()

	lastSeenAny, err := currentRunIDForInsertTx(ctx, tx, storageID)
	if err != nil {
		return err
	}

	parentDir, name := splitParentName(p)
	if parentDir != "" {
		if err := ensureDirChainTx(ctx, tx, storageID, parentDir, mtimeSec, mode, uid, gid); err != nil {
			return err
		}
	}

	if err := rejectDeletedOrTypeMismatchTx(ctx, tx, storageID, p, isDir); err != nil {
		return err
	}

	if err := upsertFileRowPreserveRunIDTx(ctx, tx, storageID, p, parentDir, name, isDir, sizeBytes, mtimeSec, mode, uid, gid, lastSeenAny); err != nil {
		return err
	}

	if err := tx.Commit(); err != nil {
		return fmt.Errorf("failed to commit upsert: %w", err)
	}
	return nil
}

// currentRunIDForInsertTx returns the last_seen_run_id value to use when inserting a new row.
func currentRunIDForInsertTx(ctx context.Context, tx *sql.Tx, storageID string) (any, error) {
	if tx == nil {
		return nil, &errkind.NilError{What: "tx"}
	}
	if strings.TrimSpace(storageID) == "" {
		return nil, &errkind.RequiredError{What: "storage id"}
	}

	_, err := tx.ExecContext(ctx, `INSERT INTO indexer_state (storage_id, current_run_id) VALUES (?, 0)
ON CONFLICT(storage_id) DO NOTHING;`, storageID)
	if err != nil {
		return nil, fmt.Errorf("failed to ensure indexer_state row: %w", err)
	}

	var cur int64
	if err := tx.QueryRowContext(ctx, `SELECT current_run_id FROM indexer_state WHERE storage_id = ?;`, storageID).Scan(&cur); err != nil {
		return nil, fmt.Errorf("failed to read current run id: %w", err)
	}
	if cur <= 0 {
		return nil, nil
	}
	return cur, nil
}

// ensureDirChainTx ensures a directory path and all its ancestors exist as live is_dir entries.
func ensureDirChainTx(ctx context.Context, tx *sql.Tx, storageID string, dir string, mtimeSec int64, mode uint32, uid uint32, gid uint32) error {
	dir = normalizeVirtualPath(dir)
	if dir == "" {
		return nil
	}

	dirMode := uint32(syscall.S_IFDIR | 0o755)

	parts := strings.Split(dir, "/")
	cur := ""
	for _, part := range parts {
		part = strings.TrimSpace(part)
		if part == "" {
			continue
		}
		if cur == "" {
			cur = part
		} else {
			cur = cur + "/" + part
		}

		if err := rejectDeletedOrTypeMismatchTx(ctx, tx, storageID, cur, true); err != nil {
			return err
		}
		parentDir, name := splitParentName(cur)
		if err := upsertFileRowPreserveRunIDTx(ctx, tx, storageID, cur, parentDir, name, true, nil, mtimeSec, dirMode, uid, gid, nil); err != nil {
			return err
		}
	}

	return nil
}

// rejectDeletedOrTypeMismatchTx returns syscall.EBUSY for deleted=1 entries and ENOTDIR/EISDIR for type mismatches.
func rejectDeletedOrTypeMismatchTx(ctx context.Context, tx *sql.Tx, storageID string, p string, wantDir bool) error {
	if tx == nil {
		return &errkind.NilError{What: "tx"}
	}
	storageID = strings.TrimSpace(storageID)
	p = normalizeVirtualPath(p)
	if storageID == "" {
		return &errkind.RequiredError{What: "storage id"}
	}
	if p == "" {
		return syscall.EPERM
	}

	row := tx.QueryRowContext(ctx, `SELECT is_dir, deleted FROM files WHERE storage_id = ? AND path = ? LIMIT 1;`, storageID, p)
	var isDirInt int64
	var deleted sql.NullInt64
	if err := row.Scan(&isDirInt, &deleted); err != nil {
		if errors.Is(err, sql.ErrNoRows) {
			return nil
		}
		return fmt.Errorf("failed to query entry: %w", err)
	}
	if deleted.Valid && deleted.Int64 == 1 {
		return syscall.EBUSY
	}
	if deleted.Valid && deleted.Int64 == 0 {
		isDir := isDirInt != 0
		if wantDir && !isDir {
			return syscall.ENOTDIR
		}
		if !wantDir && isDir {
			return syscall.EISDIR
		}
	}
	return nil
}

// upsertFileRowPreserveRunIDTx upserts one row while preserving last_seen_run_id for existing entries.
func upsertFileRowPreserveRunIDTx(
	ctx context.Context,
	tx *sql.Tx,
	storageID string,
	p string,
	parentDir string,
	name string,
	isDir bool,
	sizeBytes *int64,
	mtimeSec int64,
	mode uint32,
	uid uint32,
	gid uint32,
	insertLastSeen any,
) error {
	if tx == nil {
		return &errkind.NilError{What: "tx"}
	}
	storageID = strings.TrimSpace(storageID)
	p = normalizeVirtualPath(p)
	if storageID == "" {
		return &errkind.RequiredError{What: "storage id"}
	}
	if p == "" {
		return syscall.EPERM
	}

	var size any
	if !isDir && sizeBytes != nil {
		size = *sizeBytes
	} else {
		size = nil
	}
	isDirInt := 0
	if isDir {
		isDirInt = 1
	}

	_, err := tx.ExecContext(
		ctx,
		`INSERT INTO files (
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
		    last_seen_run_id = files.last_seen_run_id;`,
		storageID,
		p,
		p,
		parentDir,
		name,
		isDirInt,
		size,
		mtimeSec,
		mode,
		uid,
		gid,
		insertLastSeen,
	)
	if err != nil {
		return fmt.Errorf("failed to upsert file: %w", err)
	}
	return nil
}

// MarkDeleted soft-deletes a file or directory (optionally its subtree) by setting deleted=1.
// It enforces POSIX-ish semantics from the mounted view (ENOENT/ENOTEMPTY/ENOTDIR/EISDIR).
func (d *DB) MarkDeleted(ctx context.Context, storageID string, p string, isDir bool) (bool, error) {
	if d == nil || d.sqlDB == nil {
		return false, &errkind.NilError{What: "index db"}
	}
	storageID = strings.TrimSpace(storageID)
	p = normalizeVirtualPath(p)
	if storageID == "" {
		return false, &errkind.RequiredError{What: "storage id"}
	}
	if p == "" {
		return false, syscall.EPERM
	}

	tx, err := d.sqlDB.BeginTx(ctx, nil)
	if err != nil {
		return false, fmt.Errorf("failed to begin tx: %w", err)
	}
	defer func() { _ = tx.Rollback() }()

	row := tx.QueryRowContext(ctx, `SELECT is_dir FROM files WHERE storage_id = ? AND path = ? AND deleted = 0 LIMIT 1;`, storageID, p)
	var dbIsDir int64
	if err := row.Scan(&dbIsDir); err != nil {
		if errors.Is(err, sql.ErrNoRows) {
			return false, nil
		}
		return false, fmt.Errorf("failed to query entry: %w", err)
	}
	if isDir && dbIsDir == 0 {
		return false, syscall.ENOTDIR
	}
	if !isDir && dbIsDir != 0 {
		return false, syscall.EISDIR
	}

	if isDir {
		child := tx.QueryRowContext(ctx, `SELECT 1 FROM files WHERE storage_id = ? AND parent_dir = ? AND deleted = 0 AND name != '' LIMIT 1;`, storageID, p)
		var one int
		if err := child.Scan(&one); err == nil {
			return false, syscall.ENOTEMPTY
		} else if !errors.Is(err, sql.ErrNoRows) {
			return false, fmt.Errorf("failed to query children: %w", err)
		}
	}

	q := `UPDATE files SET deleted = 1 WHERE storage_id = ? AND (path = ? OR path LIKE ? ESCAPE '\') AND deleted != 1;`
	like := escapeLIKE(p) + "/%"
	if _, err := tx.ExecContext(ctx, q, storageID, p, like); err != nil {
		return false, fmt.Errorf("failed to mark deleted: %w", err)
	}

	if err := tx.Commit(); err != nil {
		return false, fmt.Errorf("failed to commit delete mark: %w", err)
	}
	return true, nil
}

// RenamePath updates virtual paths in the DB and records a pending physical rename by keeping real_path.
// It supports directory renames by rewriting the subtree.
func (d *DB) RenamePath(ctx context.Context, storageID string, oldPath string, newPath string) (bool, error) {
	if d == nil || d.sqlDB == nil {
		return false, &errkind.NilError{What: "index db"}
	}
	storageID = strings.TrimSpace(storageID)
	oldPath = normalizeVirtualPath(oldPath)
	newPath = normalizeVirtualPath(newPath)
	if storageID == "" {
		return false, &errkind.RequiredError{What: "storage id"}
	}
	if oldPath == "" || newPath == "" {
		return false, syscall.EPERM
	}
	if oldPath == newPath {
		return true, nil
	}

	tx, err := d.sqlDB.BeginTx(ctx, nil)
	if err != nil {
		return false, fmt.Errorf("failed to begin tx: %w", err)
	}
	defer func() { _ = tx.Rollback() }()

	row := tx.QueryRowContext(ctx, `SELECT is_dir FROM files WHERE storage_id = ? AND path = ? AND deleted = 0 LIMIT 1;`, storageID, oldPath)
	var oldIsDirInt int64
	if err := row.Scan(&oldIsDirInt); err != nil {
		if errors.Is(err, sql.ErrNoRows) {
			return false, nil
		}
		return false, fmt.Errorf("failed to query source entry: %w", err)
	}
	oldIsDir := oldIsDirInt != 0
	if oldIsDir && strings.HasPrefix(newPath, oldPath+"/") {
		return false, syscall.EINVAL
	}

	newParent, newName := splitParentName(newPath)
	if newParent != "" {
		pRow := tx.QueryRowContext(ctx, `SELECT 1 FROM files WHERE storage_id = ? AND path = ? AND is_dir = 1 AND deleted = 0 LIMIT 1;`, storageID, newParent)
		var one int
		if err := pRow.Scan(&one); err != nil {
			if errors.Is(err, sql.ErrNoRows) {
				return false, syscall.ENOENT
			}
			return false, fmt.Errorf("failed to query dest parent: %w", err)
		}
	}

	if oldIsDir {
		pending := tx.QueryRowContext(ctx, `SELECT 1 FROM files WHERE storage_id = ? AND deleted = 1 AND (path = ? OR path LIKE ? ESCAPE '\') LIMIT 1;`, storageID, newPath, escapeLIKE(newPath)+"/%")
		var one int
		if err := pending.Scan(&one); err == nil {
			return false, syscall.EBUSY
		} else if !errors.Is(err, sql.ErrNoRows) {
			return false, fmt.Errorf("failed to query pending deletes under dest: %w", err)
		}
	} else {
		pending := tx.QueryRowContext(ctx, `SELECT 1 FROM files WHERE storage_id = ? AND deleted = 1 AND path = ? LIMIT 1;`, storageID, newPath)
		var one int
		if err := pending.Scan(&one); err == nil {
			return false, syscall.EBUSY
		} else if !errors.Is(err, sql.ErrNoRows) {
			return false, fmt.Errorf("failed to query pending delete dest: %w", err)
		}
	}

	dRow := tx.QueryRowContext(ctx, `SELECT is_dir FROM files WHERE storage_id = ? AND path = ? AND deleted = 0 LIMIT 1;`, storageID, newPath)
	var dstIsDirInt int64
	dstExists := true
	if err := dRow.Scan(&dstIsDirInt); err != nil {
		if errors.Is(err, sql.ErrNoRows) {
			dstExists = false
		} else {
			return false, fmt.Errorf("failed to query dest entry: %w", err)
		}
	}
	if dstExists {
		dstIsDir := dstIsDirInt != 0
		if !oldIsDir && !dstIsDir {
			if _, err := tx.ExecContext(ctx, `DELETE FROM files WHERE storage_id = ? AND path = ?;`, storageID, newPath); err != nil {
				return false, fmt.Errorf("failed to remove overwrite target: %w", err)
			}
		} else if oldIsDir && !dstIsDir {
			return false, syscall.ENOTDIR
		} else if !oldIsDir && dstIsDir {
			return false, syscall.EISDIR
		} else {
			child := tx.QueryRowContext(ctx, `SELECT 1 FROM files WHERE storage_id = ? AND parent_dir = ? AND deleted = 0 AND name != '' LIMIT 1;`, storageID, newPath)
			var one int
			if err := child.Scan(&one); err == nil {
				return false, syscall.ENOTEMPTY
			} else if !errors.Is(err, sql.ErrNoRows) {
				return false, fmt.Errorf("failed to query overwrite dir children: %w", err)
			}
			if _, err := tx.ExecContext(ctx, `DELETE FROM files WHERE storage_id = ? AND path = ?;`, storageID, newPath); err != nil {
				return false, fmt.Errorf("failed to remove overwrite dir: %w", err)
			}
		}
	}

	if oldIsDir {
		child := tx.QueryRowContext(ctx, `SELECT 1 FROM files WHERE storage_id = ? AND path LIKE ? ESCAPE '\' AND deleted = 0 LIMIT 1;`, storageID, escapeLIKE(newPath)+"/%")
		var one int
		if err := child.Scan(&one); err == nil {
			return false, syscall.ENOTEMPTY
		} else if !errors.Is(err, sql.ErrNoRows) {
			return false, fmt.Errorf("failed to query dest subtree: %w", err)
		}
	}

	if oldIsDir {
		if _, err := tx.ExecContext(ctx, `DELETE FROM files WHERE storage_id = ? AND (path = ? OR path LIKE ? ESCAPE '\') AND deleted = 2;`, storageID, newPath, escapeLIKE(newPath)+"/%"); err != nil {
			return false, fmt.Errorf("failed to clear tombstone conflicts: %w", err)
		}
	} else {
		if _, err := tx.ExecContext(ctx, `DELETE FROM files WHERE storage_id = ? AND path = ? AND deleted = 2;`, storageID, newPath); err != nil {
			return false, fmt.Errorf("failed to clear tombstone conflict: %w", err)
		}
	}

	if _, err := tx.ExecContext(
		ctx,
		`UPDATE files
SET real_path = path
WHERE storage_id = ? AND path = ? AND (real_path = path OR real_path = '');`,
		storageID, oldPath,
	); err != nil {
		return false, fmt.Errorf("failed to set pending real_path: %w", err)
	}

	if _, err := tx.ExecContext(
		ctx,
		`UPDATE files
SET path = ?, parent_dir = ?, name = ?
WHERE storage_id = ? AND path = ?;`,
		newPath, newParent, newName, storageID, oldPath,
	); err != nil {
		return false, fmt.Errorf("failed to update entry path: %w", err)
	}

	if oldIsDir {
		if _, err := tx.ExecContext(
			ctx,
			`UPDATE files
SET real_path = path
WHERE storage_id = ? AND path LIKE ? ESCAPE '\' AND (real_path = path OR real_path = '');`,
			storageID, escapeLIKE(oldPath)+"/%",
		); err != nil {
			return false, fmt.Errorf("failed to set subtree pending real_path: %w", err)
		}

		oldLen := len(oldPath)
		start := oldLen + 1
		if _, err := tx.ExecContext(
			ctx,
			`UPDATE files
SET
    path = ? || substr(path, ?),
    parent_dir = ? || substr(parent_dir, ?)
WHERE storage_id = ? AND path LIKE ? ESCAPE '\';`,
			newPath, start, newPath, start, storageID, escapeLIKE(oldPath)+"/%",
		); err != nil {
			return false, fmt.Errorf("failed to update subtree paths: %w", err)
		}
	}

	if err := tx.Commit(); err != nil {
		return false, fmt.Errorf("failed to commit rename: %w", err)
	}
	return true, nil
}

// UpsertMeta writes file_meta overrides for SETATTR without touching the physical filesystem.
// Any nil field is treated as "no change".
func (d *DB) UpsertMeta(ctx context.Context, storageID string, p string, mode *uint32, uid *uint32, gid *uint32, mtimeSec *int64) (bool, error) {
	if d == nil || d.sqlDB == nil {
		return false, &errkind.NilError{What: "index db"}
	}
	storageID = strings.TrimSpace(storageID)
	p = normalizeVirtualPath(p)
	if storageID == "" {
		return false, &errkind.RequiredError{What: "storage id"}
	}
	if p == "" {
		return false, syscall.EPERM
	}

	tx, err := d.sqlDB.BeginTx(ctx, nil)
	if err != nil {
		return false, fmt.Errorf("failed to begin tx: %w", err)
	}
	defer func() { _ = tx.Rollback() }()

	row := tx.QueryRowContext(ctx, `SELECT 1 FROM files WHERE storage_id = ? AND path = ? AND deleted = 0 LIMIT 1;`, storageID, p)
	var one int
	if err := row.Scan(&one); err != nil {
		if errors.Is(err, sql.ErrNoRows) {
			return false, nil
		}
		return false, fmt.Errorf("failed to query entry: %w", err)
	}

	var mtime any
	if mtimeSec != nil {
		mtime = *mtimeSec
	}
	var mmode any
	if mode != nil {
		mmode = *mode
	}
	var muid any
	if uid != nil {
		muid = *uid
	}
	var mgid any
	if gid != nil {
		mgid = *gid
	}

	_, err = tx.ExecContext(
		ctx,
		`INSERT INTO file_meta (storage_id, path, meta_mtime, meta_mode, meta_uid, meta_gid)
VALUES (?, ?, ?, ?, ?, ?)
ON CONFLICT(storage_id, path) DO UPDATE SET
    meta_mtime = COALESCE(excluded.meta_mtime, file_meta.meta_mtime),
    meta_mode = COALESCE(excluded.meta_mode, file_meta.meta_mode),
    meta_uid = COALESCE(excluded.meta_uid, file_meta.meta_uid),
    meta_gid = COALESCE(excluded.meta_gid, file_meta.meta_gid);`,
		storageID, p, mtime, mmode, muid, mgid,
	)
	if err != nil {
		return false, fmt.Errorf("failed to upsert meta: %w", err)
	}

	if err := tx.Commit(); err != nil {
		return false, fmt.Errorf("failed to commit meta upsert: %w", err)
	}
	return true, nil
}

// FinalizeDelete removes rows that were previously marked deleted=1 after physical deletion succeeded.
func (d *DB) FinalizeDelete(ctx context.Context, storageID string, p string, isDir bool) error {
	if d == nil || d.sqlDB == nil {
		return &errkind.NilError{What: "index db"}
	}
	storageID = strings.TrimSpace(storageID)
	p = normalizeVirtualPath(p)
	if storageID == "" {
		return &errkind.RequiredError{What: "storage id"}
	}
	if p == "" {
		return syscall.EPERM
	}

	q := `DELETE FROM files WHERE storage_id = ? AND path = ? AND deleted = 1;`
	args := []any{storageID, p}
	if isDir {
		q = `DELETE FROM files WHERE storage_id = ? AND (path = ? OR path LIKE ? ESCAPE '\') AND deleted = 1;`
		args = []any{storageID, p, escapeLIKE(p) + "/%"}
	}
	if _, err := d.sqlDB.ExecContext(ctx, q, args...); err != nil {
		return fmt.Errorf("failed to finalize delete: %w", err)
	}
	return nil
}

// FinalizeRename updates real_path to match the new on-disk location after physical rename succeeded.
//
// It also ensures virtual paths (path/parent_dir/name) are at their new values. This is normally
// a no-op because RenamePath already set them during the FUSE operation, but acts as a safety net
// when the daemon's WAL commit is not yet visible to the prune process (e.g. cross-process WAL
// visibility lag on overlayfs).
func (d *DB) FinalizeRename(ctx context.Context, storageID string, oldPath string, newPath string) error {
	if d == nil || d.sqlDB == nil {
		return &errkind.NilError{What: "index db"}
	}
	storageID = strings.TrimSpace(storageID)
	oldPath = normalizeVirtualPath(oldPath)
	newPath = normalizeVirtualPath(newPath)
	if storageID == "" {
		return &errkind.RequiredError{What: "storage id"}
	}
	if oldPath == "" || newPath == "" {
		return syscall.EPERM
	}
	if oldPath == newPath {
		return nil
	}

	tx, err := d.sqlDB.BeginTx(ctx, nil)
	if err != nil {
		return fmt.Errorf("failed to begin tx: %w", err)
	}
	defer func() { _ = tx.Rollback() }()

	oldLen := len(oldPath)
	start := oldLen + 1

	// Phase 1: ensure virtual paths are at newPath.
	// Idempotent: if RenamePath already committed, WHERE path = oldPath won't match.

	// 1a. Preserve real_path for the root entry before moving path.
	if _, err := tx.ExecContext(ctx,
		`UPDATE files SET real_path = path
		 WHERE storage_id = ? AND path = ? AND (real_path = path OR real_path = '');`,
		storageID, oldPath,
	); err != nil {
		return fmt.Errorf("failed to preserve real_path: %w", err)
	}

	// 1b. Remove any existing overwrite target at the destination. RenamePath normally
	// deletes it, but if that commit isn't visible we must clear it to avoid a UNIQUE violation.
	// We identify the overwrite target by (real_path = path OR real_path = '') - a row that was
	// never itself renamed. The source row (if RenamePath committed) has real_path != path and
	// is left untouched.
	if _, err := tx.ExecContext(ctx,
		`DELETE FROM files WHERE storage_id = ? AND path = ? AND (real_path = path OR real_path = '');`,
		storageID, newPath,
	); err != nil {
		return fmt.Errorf("failed to clear overwrite target: %w", err)
	}
	// Also clear destination subtree for directory overwrites.
	if _, err := tx.ExecContext(ctx,
		`DELETE FROM files WHERE storage_id = ? AND path LIKE ? ESCAPE '\' AND (real_path = path OR real_path = '');`,
		storageID, escapeLIKE(newPath)+"/%",
	); err != nil {
		return fmt.Errorf("failed to clear overwrite subtree: %w", err)
	}

	// 1c. Move root entry path.
	newParent, newName := splitParentName(newPath)
	if _, err := tx.ExecContext(ctx,
		`UPDATE files SET path = ?, parent_dir = ?, name = ?
		 WHERE storage_id = ? AND path = ?;`,
		newPath, newParent, newName, storageID, oldPath,
	); err != nil {
		return fmt.Errorf("failed to finalize rename path: %w", err)
	}

	// 1d. Directory subtree: preserve real_path, then rewrite path/parent_dir.
	if _, err := tx.ExecContext(ctx,
		`UPDATE files SET real_path = path
		 WHERE storage_id = ? AND path LIKE ? ESCAPE '\' AND (real_path = path OR real_path = '');`,
		storageID, escapeLIKE(oldPath)+"/%",
	); err != nil {
		return fmt.Errorf("failed to preserve subtree real_path: %w", err)
	}
	if _, err := tx.ExecContext(ctx,
		`UPDATE files SET
		     path = ? || substr(path, ?),
		     parent_dir = ? || substr(parent_dir, ?)
		 WHERE storage_id = ? AND path LIKE ? ESCAPE '\';`,
		newPath, start, newPath, start, storageID, escapeLIKE(oldPath)+"/%",
	); err != nil {
		return fmt.Errorf("failed to finalize subtree paths: %w", err)
	}

	// Phase 2: update real_path to match the new physical location.
	if _, err := tx.ExecContext(ctx,
		`UPDATE files SET real_path = ? || substr(real_path, ?)
		 WHERE storage_id = ?
		   AND (real_path = ? OR real_path LIKE ? ESCAPE '\');`,
		newPath, start, storageID, oldPath, escapeLIKE(oldPath)+"/%",
	); err != nil {
		return fmt.Errorf("failed to finalize rename: %w", err)
	}

	// Safety net: the root renamed entry should no longer be pending after prune.
	if _, err := tx.ExecContext(ctx,
		`UPDATE files SET real_path = path
		 WHERE storage_id = ? AND path = ?;`,
		storageID, newPath,
	); err != nil {
		return fmt.Errorf("failed to finalize rename root: %w", err)
	}

	if err := tx.Commit(); err != nil {
		return fmt.Errorf("failed to commit finalize rename: %w", err)
	}
	return nil
}

// FinalizeSetattr clears file_meta overrides after they have been applied physically.
func (d *DB) FinalizeSetattr(ctx context.Context, storageID string, p string) error {
	if d == nil || d.sqlDB == nil {
		return &errkind.NilError{What: "index db"}
	}
	storageID = strings.TrimSpace(storageID)
	p = normalizeVirtualPath(p)
	if storageID == "" {
		return &errkind.RequiredError{What: "storage id"}
	}
	if p == "" {
		return syscall.EPERM
	}

	// Merge override values into the base row, then clear file_meta.
	// This keeps the DB consistent with the physical filesystem after prune.
	tx, err := d.sqlDB.BeginTx(ctx, nil)
	if err != nil {
		return fmt.Errorf("failed to begin tx: %w", err)
	}
	defer func() { _ = tx.Rollback() }()

	q := `UPDATE files
SET
    mtime = COALESCE((SELECT meta_mtime FROM file_meta WHERE storage_id = ? AND path = ?), mtime),
    mode = COALESCE((SELECT meta_mode FROM file_meta WHERE storage_id = ? AND path = ?), mode),
    uid = COALESCE((SELECT meta_uid FROM file_meta WHERE storage_id = ? AND path = ?), uid),
    gid = COALESCE((SELECT meta_gid FROM file_meta WHERE storage_id = ? AND path = ?), gid)
WHERE storage_id = ? AND path = ?;`
	if _, err := tx.ExecContext(ctx, q,
		storageID, p,
		storageID, p,
		storageID, p,
		storageID, p,
		storageID, p,
	); err != nil {
		return fmt.Errorf("failed to merge meta overrides: %w", err)
	}
	if _, err := tx.ExecContext(ctx, `DELETE FROM file_meta WHERE storage_id = ? AND path = ?;`, storageID, p); err != nil {
		return fmt.Errorf("failed to finalize setattr: %w", err)
	}
	if err := tx.Commit(); err != nil {
		return fmt.Errorf("failed to commit setattr finalize: %w", err)
	}
	return nil
}
