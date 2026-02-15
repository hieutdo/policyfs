//go:build !windows

package indexer

import (
	"context"
	"errors"
	"os"
	"path/filepath"
	"testing"

	"github.com/hieutdo/policyfs/internal/config"
	"github.com/hieutdo/policyfs/internal/indexdb"
	"github.com/mattn/go-sqlite3"
	"github.com/stretchr/testify/require"
)

// newTestDB opens a fresh index database for indexer tests.
func newTestDB(t *testing.T, mountName string) *indexdb.DB {
	t.Helper()
	stateDir := t.TempDir()
	t.Setenv(config.EnvStateDir, stateDir)
	db, err := indexdb.Open(mountName)
	require.NoError(t, err)
	t.Cleanup(func() { _ = db.Close() })
	return db
}

// TestRun_StorageOffline_shouldWarnAndSkip verifies missing storage roots are reported as warnings.
func TestRun_StorageOffline_shouldWarnAndSkip(t *testing.T) {
	mountName := "media"
	missing := filepath.Join(t.TempDir(), "missing")
	db := newTestDB(t, mountName)

	mountCfg := &config.MountConfig{
		StoragePaths: []config.StoragePath{{ID: "hdd1", Path: missing, Indexed: true}},
	}

	warnings := []string{}
	hooks := Hooks{Warn: func(storageID, rel string, err error) {
		warnings = append(warnings, storageID)
	}}

	res, err := Run(context.Background(), mountName, mountCfg, db.SQL(), hooks)
	require.NoError(t, err)
	require.Len(t, res.StoragePaths, 1)
	require.Equal(t, int64(1), res.StoragePaths[0].Warnings)
	require.Equal(t, int64(0), res.StoragePaths[0].FilesScanned)
	require.Len(t, warnings, 1)
}

// TestRun_BrokenSymlink_shouldWarnAndSkip verifies broken symlinks are warned and ignored.
func TestRun_BrokenSymlink_shouldWarnAndSkip(t *testing.T) {
	mountName := "media"
	root := t.TempDir()
	link := filepath.Join(root, "broken")
	require.NoError(t, os.Symlink(filepath.Join(root, "missing"), link))

	db := newTestDB(t, mountName)
	mountCfg := &config.MountConfig{
		StoragePaths: []config.StoragePath{{ID: "hdd1", Path: root, Indexed: true}},
	}

	warnings := []string{}
	hooks := Hooks{Warn: func(storageID, rel string, err error) {
		warnings = append(warnings, rel)
	}}

	res, err := Run(context.Background(), mountName, mountCfg, db.SQL(), hooks)
	require.NoError(t, err)
	require.Len(t, res.StoragePaths, 1)
	require.Equal(t, int64(1), res.StoragePaths[0].Warnings)
	require.Equal(t, int64(0), res.StoragePaths[0].FilesScanned)
	require.Len(t, warnings, 1)
}

// TestRun_PermissionDenied_shouldWarnAndSkip verifies permission errors are warned and do not halt indexing.
func TestRun_PermissionDenied_shouldWarnAndSkip(t *testing.T) {
	if os.Geteuid() == 0 {
		t.Skip("permission denied test requires non-root user")
	}

	mountName := "media"
	root := t.TempDir()
	allowedFile := filepath.Join(root, "ok.txt")
	require.NoError(t, os.WriteFile(allowedFile, []byte("ok"), 0o644))

	secretDir := filepath.Join(root, "secret")
	require.NoError(t, os.MkdirAll(secretDir, 0o755))
	require.NoError(t, os.WriteFile(filepath.Join(secretDir, "hidden.txt"), []byte("nope"), 0o644))
	require.NoError(t, os.Chmod(secretDir, 0o000))
	t.Cleanup(func() { _ = os.Chmod(secretDir, 0o755) })

	db := newTestDB(t, mountName)
	mountCfg := &config.MountConfig{
		StoragePaths: []config.StoragePath{{ID: "hdd1", Path: root, Indexed: true}},
	}

	warnings := []string{}
	hooks := Hooks{Warn: func(storageID, rel string, err error) {
		warnings = append(warnings, rel)
	}}

	res, err := Run(context.Background(), mountName, mountCfg, db.SQL(), hooks)
	require.NoError(t, err)
	require.Len(t, res.StoragePaths, 1)
	require.Equal(t, int64(1), res.StoragePaths[0].Warnings)
	require.Equal(t, int64(1), res.StoragePaths[0].FilesScanned)
	require.Len(t, warnings, 1)
}

// TestRetryDBWrite_shouldRetryAndSucceed verifies transient DB errors are retried.
func TestRetryDBWrite_shouldRetryAndSucceed(t *testing.T) {
	ctx := context.Background()
	attempts := 0

	err := retryDBWrite(ctx, "test op", func() error {
		attempts++
		if attempts < 3 {
			return errors.New("boom")
		}
		return nil
	})

	require.NoError(t, err)
	require.Equal(t, 3, attempts)
}

// TestRetryDBWrite_shouldFailAfterMaxAttempts verifies retries stop after the limit.
func TestRetryDBWrite_shouldFailAfterMaxAttempts(t *testing.T) {
	ctx := context.Background()
	attempts := 0

	err := retryDBWrite(ctx, "test op", func() error {
		attempts++
		return errors.New("boom")
	})

	require.Error(t, err)
	require.Equal(t, 3, attempts)
}

// TestRetryDBWrite_DiskFull_shouldAbort verifies disk-full errors do not retry.
func TestRetryDBWrite_DiskFull_shouldAbort(t *testing.T) {
	ctx := context.Background()
	attempts := 0

	err := retryDBWrite(ctx, "test op", func() error {
		attempts++
		return sqlite3.Error{Code: sqlite3.ErrFull}
	})

	require.Error(t, err)
	require.Equal(t, 1, attempts)
}

// TestShouldSkipTombstone_shouldClassify verifies tombstone skipping only triggers for incomplete scans.
func TestShouldSkipTombstone_shouldClassify(t *testing.T) {
	cases := []struct {
		name string
		err  error
		want bool
	}{
		{name: "not-exist", err: os.ErrNotExist, want: false},
		{name: "permission", err: os.ErrPermission, want: true},
		{name: "missing-stat", err: errMissingStat, want: true},
		{name: "generic", err: errors.New("boom"), want: true},
	}

	for _, tc := range cases {
		t.Run(tc.name, func(t *testing.T) {
			require.Equal(t, tc.want, shouldSkipTombstone(tc.err))
		})
	}
}

// TestRun_IgnoreFullPath_shouldSkip verifies ignore patterns match full paths.
func TestRun_IgnoreFullPath_shouldSkip(t *testing.T) {
	mountName := "media"
	root := t.TempDir()

	require.NoError(t, os.MkdirAll(filepath.Join(root, "cache"), 0o755))
	require.NoError(t, os.WriteFile(filepath.Join(root, "cache", "keep.txt"), []byte("skip"), 0o644))
	require.NoError(t, os.MkdirAll(filepath.Join(root, "library", "movies"), 0o755))
	require.NoError(t, os.WriteFile(filepath.Join(root, "library", "movies", "a.mkv"), []byte("ok"), 0o644))
	require.NoError(t, os.WriteFile(filepath.Join(root, "library", "movies", ".DS_Store"), []byte("skip"), 0o644))

	db := newTestDB(t, mountName)
	mountCfg := &config.MountConfig{
		StoragePaths: []config.StoragePath{{ID: "hdd1", Path: root, Indexed: true}},
		Indexer: config.IndexerConfig{
			Ignore: []string{"cache/**", "**/.DS_Store"},
		},
	}

	res, err := Run(context.Background(), mountName, mountCfg, db.SQL(), Hooks{})
	require.NoError(t, err)
	require.Len(t, res.StoragePaths, 1)
	require.Equal(t, int64(1), res.StoragePaths[0].FilesScanned)
}

// TestCount_IgnoreFullPath_shouldSkip verifies Count applies ignore patterns against full relative paths.
func TestCount_IgnoreFullPath_shouldSkip(t *testing.T) {
	mountName := "media"
	root := t.TempDir()

	require.NoError(t, os.MkdirAll(filepath.Join(root, "cache"), 0o755))
	require.NoError(t, os.WriteFile(filepath.Join(root, "cache", "keep.txt"), []byte("skip"), 0o644))
	require.NoError(t, os.MkdirAll(filepath.Join(root, "library", "movies"), 0o755))
	require.NoError(t, os.WriteFile(filepath.Join(root, "library", "movies", "a.mkv"), []byte("ok"), 0o644))
	require.NoError(t, os.WriteFile(filepath.Join(root, "library", "movies", ".DS_Store"), []byte("skip"), 0o644))

	mountCfg := &config.MountConfig{
		StoragePaths: []config.StoragePath{{ID: "hdd1", Path: root, Indexed: true}},
		Indexer: config.IndexerConfig{
			Ignore: []string{"cache/**", "**/.DS_Store"},
		},
	}

	res, err := Count(context.Background(), mountName, mountCfg)
	require.NoError(t, err)
	require.Len(t, res.StoragePaths, 1)
	require.Equal(t, int64(2), res.TotalDirs)
	require.Equal(t, int64(1), res.TotalFiles)
	require.Equal(t, int64(3), res.TotalEntries)
	require.Equal(t, int64(2), res.StoragePaths[0].DirsCounted)
	require.Equal(t, int64(1), res.StoragePaths[0].FilesCounted)
	require.Equal(t, int64(3), res.StoragePaths[0].EntriesCounted)
}
