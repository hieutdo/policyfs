package indexdb

import (
	"context"
	"database/sql"
	"errors"
	"fmt"
	"io"
	"log"
	"os"
	"path/filepath"
	"strings"
	"time"

	_ "github.com/mattn/go-sqlite3"
	"github.com/pressly/goose/v3"

	"github.com/hieutdo/policyfs/internal/config"
	"github.com/hieutdo/policyfs/internal/errkind"
	"github.com/hieutdo/policyfs/migrations"
)

const (
	busyTimeoutMS            = 5000
	gooseIncompatibleVersion = 2
)

var (
	// ErrIndexDBSchemaIncompatible is returned when the index db schema is incompatible.
	ErrIndexDBSchemaIncompatible = errkind.SentinelError("index db schema is incompatible")
)

// DB provides access to the per-mount index database.
type DB struct {
	MountName string
	Path      string
	sqlDB     *sql.DB
}

// Reset deletes the per-mount index database so the next Open creates a fresh one.
func Reset(mountName string) error {
	if mountName == "" {
		return &errkind.RequiredError{What: "mount name"}
	}

	dbPath := filepath.Join(config.MountStateDir(mountName), "index.db")

	paths := []string{dbPath, dbPath + "-wal", dbPath + "-shm"}
	for _, p := range paths {
		if err := os.Remove(p); err != nil {
			if errors.Is(err, os.ErrNotExist) {
				continue
			}
			return fmt.Errorf("failed to delete index db: %w", err)
		}
	}
	return nil
}

// Open opens (or creates) the per-mount SQLite database and applies migrations.
func Open(mountName string) (*DB, error) {
	if mountName == "" {
		return nil, &errkind.RequiredError{What: "mount name"}
	}

	mountDir := config.MountStateDir(mountName)
	if err := os.MkdirAll(mountDir, 0o755); err != nil {
		return nil, fmt.Errorf("failed to ensure mount db dir: %w", err)
	}

	dbPath := filepath.Join(mountDir, "index.db")
	_, statErr := os.Stat(dbPath)
	if statErr != nil && !errors.Is(statErr, os.ErrNotExist) {
		return nil, fmt.Errorf("failed to stat index db: %w", statErr)
	}

	for attempt := 0; attempt < 2; attempt++ {
		dsn := fmt.Sprintf(
			"file:%s?_foreign_keys=on&_busy_timeout=%d&_journal_mode=WAL&_synchronous=NORMAL",
			dbPath,
			busyTimeoutMS,
		)
		conn, err := sql.Open("sqlite3", dsn)
		if err != nil {
			return nil, fmt.Errorf("failed to open index db: %w", err)
		}

		if err := conn.Ping(); err != nil {
			_ = conn.Close()
			return nil, fmt.Errorf("failed to ping index db: %w", err)
		}

		if err := applyPragmas(conn); err != nil {
			_ = conn.Close()
			return nil, err
		}
		if err := configureGoose(); err != nil {
			_ = conn.Close()
			return nil, err
		}
		ver, err := goose.EnsureDBVersion(conn)
		if err != nil {
			_ = conn.Close()
			return nil, fmt.Errorf("failed to read index db version: %w", err)
		}
		if ver == gooseIncompatibleVersion {
			_ = conn.Close()
			if err := backupIncompatibleDB(dbPath); err != nil {
				return nil, err
			}
			continue
		}
		if err := applyMigrations(conn); err != nil {
			_ = conn.Close()
			return nil, err
		}
		ok, err := schemaLooksMerged(conn)
		if err != nil {
			_ = conn.Close()
			return nil, err
		}
		if ok {
			return &DB{MountName: mountName, Path: dbPath, sqlDB: conn}, nil
		}

		_ = conn.Close()
		if err := backupIncompatibleDB(dbPath); err != nil {
			return nil, err
		}
	}

	return nil, ErrIndexDBSchemaIncompatible
}

// backupIncompatibleDB moves the current index db aside so the next open recreates it.
func backupIncompatibleDB(dbPath string) error {
	if strings.TrimSpace(dbPath) == "" {
		return &errkind.NilError{What: "index db path"}
	}

	backupPath := fmt.Sprintf("%s.incompatible.%d", dbPath, time.Now().Unix())
	if err := os.Rename(dbPath, backupPath); err != nil {
		return fmt.Errorf("failed to backup incompatible index db: %w", err)
	}

	walPath := dbPath + "-wal"
	if err := os.Rename(walPath, backupPath+"-wal"); err != nil && !errors.Is(err, os.ErrNotExist) {
		return fmt.Errorf("failed to backup incompatible index db wal: %w", err)
	}
	shmPath := dbPath + "-shm"
	if err := os.Rename(shmPath, backupPath+"-shm"); err != nil && !errors.Is(err, os.ErrNotExist) {
		return fmt.Errorf("failed to backup incompatible index db shm: %w", err)
	}

	return nil
}

// schemaLooksMerged returns true if required columns for the merged schema exist.
func schemaLooksMerged(db *sql.DB) (ok bool, retErr error) {
	q := `SELECT name FROM pragma_table_info('files');`
	rows, err := db.Query(q)
	if err != nil {
		return false, fmt.Errorf("failed to query files schema: %w", err)
	}
	defer func() {
		closeErr := rows.Close()
		if closeErr != nil && retErr == nil {
			retErr = fmt.Errorf("failed to close schema rows: %w", closeErr)
		}
	}()

	have := map[string]bool{}
	for rows.Next() {
		var name string
		if err := rows.Scan(&name); err != nil {
			return false, fmt.Errorf("failed to scan schema row: %w", err)
		}
		have[name] = true
	}
	if err := rows.Err(); err != nil {
		return false, fmt.Errorf("failed to iterate schema rows: %w", err)
	}

	required := []string{"parent_dir", "name", "is_dir", "uid", "gid"}
	for _, col := range required {
		if !have[col] {
			return false, nil
		}
	}
	return true, nil
}

// Close closes the underlying SQL database.
func (d *DB) Close() error {
	if d == nil || d.sqlDB == nil {
		return nil
	}
	err := d.sqlDB.Close()
	if err != nil {
		return fmt.Errorf("failed to close index db: %w", err)
	}
	return nil
}

// SQL returns the underlying sql.DB.
func (d *DB) SQL() *sql.DB {
	if d == nil {
		return nil
	}
	return d.sqlDB
}

// Ping validates DB connectivity.
func (d *DB) Ping(ctx context.Context) error {
	if d == nil || d.sqlDB == nil {
		return &errkind.NilError{What: "index db"}
	}
	err := d.sqlDB.PingContext(ctx)
	if err != nil {
		return fmt.Errorf("failed to ping index db: %w", err)
	}
	return nil
}

// applyPragmas configures SQLite connection settings required by spec.
func applyPragmas(db *sql.DB) error {
	if db == nil {
		return &errkind.NilError{What: "index db"}
	}

	stmts := []string{
		"PRAGMA journal_mode = WAL;",
		fmt.Sprintf("PRAGMA busy_timeout = %d;", busyTimeoutMS),
		"PRAGMA synchronous = NORMAL;",
		"PRAGMA foreign_keys = ON;",
	}
	for _, s := range stmts {
		if _, err := db.Exec(s); err != nil {
			return fmt.Errorf("failed to apply pragma: %w", err)
		}
	}
	return nil
}

// applyMigrations applies embedded goose migrations to the index database.
func applyMigrations(db *sql.DB) error {
	if db == nil {
		return &errkind.NilError{What: "index db"}
	}

	if err := configureGoose(); err != nil {
		return err
	}
	if err := goose.Up(db, "."); err != nil {
		return fmt.Errorf("failed to migrate index db: %w", err)
	}
	return nil
}

// configureGoose configures goose for PolicyFS's embedded index migrations.
func configureGoose() error {
	// Silence goose's default stdout/stderr noise.
	goose.SetLogger(log.New(io.Discard, "", 0))
	goose.SetBaseFS(migrations.FS)
	if err := goose.SetDialect("sqlite3"); err != nil {
		return fmt.Errorf("failed to set goose dialect: %w", err)
	}
	return nil
}
