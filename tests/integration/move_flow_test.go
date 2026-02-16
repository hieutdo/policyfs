//go:build integration

package integration

import (
	"context"
	"database/sql"
	"os"
	"path/filepath"
	"strings"
	"testing"
	"time"

	"github.com/hieutdo/policyfs/internal/config"
	"github.com/stretchr/testify/require"
)

// boolPtr returns a pointer to v.
func boolPtr(v bool) *bool {
	return &v
}

// TestMove_shouldMoveFromNonIndexedToIndexed_andMountShouldExposeWithoutIndex verifies that moving a file
// into an indexed destination upserts the indexdb entries (including directory chain) so the running mount
// exposes the file immediately without an index run.
func TestMove_shouldMoveFromNonIndexedToIndexed_andMountShouldExposeWithoutIndex(t *testing.T) {
	if os.Getenv(config.EnvIntegrationUseExistingMount) != "" {
		t.Skip("skip move flow test when using an existing mount")
	}

	jobName := "archive"
	rel := "library/movies/a.txt"
	content := []byte("hello-move")

	mv := &config.MoverConfig{
		Enabled: boolPtr(true),
		Jobs: []config.MoverJobConfig{
			{
				Name:        jobName,
				Description: "integration move",
				Trigger: config.MoverTriggerConfig{
					Type: "manual",
				},
				Source: config.MoverSourceConfig{
					Paths:    []string{"ssd1"},
					Patterns: []string{"library/**"},
				},
				Destination: config.MoverDestinationConfig{
					Paths:          []string{"hdd1"},
					Policy:         "first_found",
					PathPreserving: true,
				},
				Conditions:   config.MoverConditionsConfig{},
				DeleteSource: boolPtr(true),
				Verify:       boolPtr(true),
			},
		},
	}

	cfg := IntegrationConfig{
		Storages: []IntegrationStorage{
			{ID: "ssd1", Indexed: false, BasePath: "/mnt/ssd1/pfs-integration"},
			{ID: "hdd1", Indexed: true, BasePath: "/mnt/hdd1/pfs-integration"},
		},
		Targets:     []string{"hdd1"},
		ReadTargets: []string{"hdd1"},
		Mover:       mv,
	}

	withMountedFS(t, cfg, func(env *MountedFS) {
		env.MustCreateFileInStoragePath(t, content, "ssd1", rel)

		// Guard: the mount should not expose non-indexed sources when routing only targets the indexed dest.
		require.False(t, env.FileExistsInMountPoint(rel))

		mustRunPFS(t, env, "move", env.MountName, "--job", jobName, "--progress=off")

		require.NoFileExists(t, env.StoragePath("ssd1", rel))
		require.FileExists(t, env.StoragePath("hdd1", rel))

		deadline := time.Now().Add(2 * time.Second)
		for time.Now().Before(deadline) {
			if env.FileExistsInMountPoint(rel) {
				break
			}
			time.Sleep(50 * time.Millisecond)
		}
		require.True(t, env.FileExistsInMountPoint(rel))
		require.Equal(t, content, env.MustReadFileInMountPoint(t, rel))

		db := openIndexDB(t, env)
		ctx, cancel := context.WithTimeout(context.Background(), 2*time.Second)
		defer cancel()

		var deleted int
		var isDir int
		err := db.QueryRowContext(ctx, `SELECT deleted, is_dir FROM files WHERE storage_id = ? AND path = ? LIMIT 1;`, "hdd1", rel).Scan(&deleted, &isDir)
		require.NoError(t, err)
		require.Equal(t, 0, deleted)
		require.Equal(t, 0, isDir)

		dirs := []string{"library", "library/movies"}
		for _, d := range dirs {
			deleted := 0
			isDir := 0
			err := db.QueryRowContext(ctx, `SELECT deleted, is_dir FROM files WHERE storage_id = ? AND path = ? LIMIT 1;`, "hdd1", d).Scan(&deleted, &isDir)
			require.NoError(t, err)
			require.Equal(t, 0, deleted)
			require.Equal(t, 1, isDir)
		}
	})
}

// TestMove_dryRun_shouldNotChangeFilesystemOrDB verifies dry-run mode does not write the destination,
// does not delete the source, and does not upsert indexdb entries.
func TestMove_dryRun_shouldNotChangeFilesystemOrDB(t *testing.T) {
	if os.Getenv(config.EnvIntegrationUseExistingMount) != "" {
		t.Skip("skip move flow test when using an existing mount")
	}

	jobName := "archive"
	rel := "library/movies/dry.txt"
	content := []byte("hello-move-dry")

	mv := &config.MoverConfig{
		Enabled: boolPtr(true),
		Jobs: []config.MoverJobConfig{
			{
				Name:        jobName,
				Description: "integration move dry",
				Trigger: config.MoverTriggerConfig{
					Type: "manual",
				},
				Source: config.MoverSourceConfig{
					Paths:    []string{"ssd1"},
					Patterns: []string{"library/**"},
				},
				Destination: config.MoverDestinationConfig{
					Paths:          []string{"hdd1"},
					Policy:         "first_found",
					PathPreserving: true,
				},
				Conditions:   config.MoverConditionsConfig{},
				DeleteSource: boolPtr(true),
				Verify:       boolPtr(true),
			},
		},
	}

	cfg := IntegrationConfig{
		Storages: []IntegrationStorage{
			{ID: "ssd1", Indexed: false, BasePath: "/mnt/ssd1/pfs-integration"},
			{ID: "hdd1", Indexed: true, BasePath: "/mnt/hdd1/pfs-integration"},
		},
		Targets:     []string{"hdd1"},
		ReadTargets: []string{"hdd1"},
		Mover:       mv,
	}

	withMountedFS(t, cfg, func(env *MountedFS) {
		env.MustCreateFileInStoragePath(t, content, "ssd1", rel)
		require.False(t, env.FileExistsInMountPoint(rel))

		mustRunPFS(t, env, "move", env.MountName, "--job", jobName, "--dry-run", "--progress=off")

		require.FileExists(t, env.StoragePath("ssd1", rel))
		require.NoFileExists(t, env.StoragePath("hdd1", rel))
		require.False(t, env.FileExistsInMountPoint(rel))

		db := openIndexDB(t, env)
		ctx, cancel := context.WithTimeout(context.Background(), 2*time.Second)
		defer cancel()

		var one int
		err := db.QueryRowContext(ctx, `SELECT 1 FROM files WHERE storage_id = ? AND path = ? LIMIT 1;`, "hdd1", rel).Scan(&one)
		require.Error(t, err)
		require.ErrorIs(t, err, sql.ErrNoRows)
	})
}

// TestMove_fromIndexedSource_shouldMarkDeletedAndAppendEventlog verifies that moving a file
// from an indexed source marks the source row as deleted=1 in indexdb and appends a delete event
// to the eventlog so the mount immediately hides the source path.
func TestMove_fromIndexedSource_shouldMarkDeletedAndAppendEventlog(t *testing.T) {
	if os.Getenv(config.EnvIntegrationUseExistingMount) != "" {
		t.Skip("skip move flow test when using an existing mount")
	}

	jobName := "archive"
	rel := "library/movies/indexed-src.txt"
	content := []byte("indexed-source-move")

	mv := &config.MoverConfig{
		Enabled: boolPtr(true),
		Jobs: []config.MoverJobConfig{
			{
				Name:        jobName,
				Description: "move from indexed src to non-indexed dst",
				Trigger:     config.MoverTriggerConfig{Type: "manual"},
				Source: config.MoverSourceConfig{
					Paths:    []string{"ssd1"},
					Patterns: []string{"library/**"},
				},
				Destination: config.MoverDestinationConfig{
					Paths:  []string{"hdd1"},
					Policy: "first_found",
				},
				DeleteSource: boolPtr(true),
				Verify:       boolPtr(true),
			},
		},
	}

	cfg := IntegrationConfig{
		Storages: []IntegrationStorage{
			{ID: "ssd1", Indexed: true, BasePath: "/mnt/ssd1/pfs-integration"},
			{ID: "hdd1", Indexed: false, BasePath: "/mnt/hdd1/pfs-integration"},
		},
		Targets:     []string{"ssd1"},
		ReadTargets: []string{"ssd1"},
		Mover:       mv,
	}

	withMountedFS(t, cfg, func(env *MountedFS) {
		env.MustCreateFileInStoragePath(t, content, "ssd1", rel)

		// Index so the mount sees the file.
		mustRunPFS(t, env, "index", env.MountName, "--progress=off")

		deadline := time.Now().Add(2 * time.Second)
		for time.Now().Before(deadline) {
			if env.FileExistsInMountPoint(rel) {
				break
			}
			time.Sleep(50 * time.Millisecond)
		}
		require.True(t, env.FileExistsInMountPoint(rel), "file should be visible in mount after index")

		// Run move.
		mustRunPFS(t, env, "move", env.MountName, "--job", jobName, "--progress=off")

		// Destination should have the file.
		require.FileExists(t, env.StoragePath("hdd1", rel))

		// Source physical file still exists (deferred deletion — prune hasn't run).
		require.FileExists(t, env.StoragePath("ssd1", rel))

		// Verify indexdb: source row should be deleted=1 (deferred tombstone).
		db := openIndexDB(t, env)
		ctx, cancel := context.WithTimeout(context.Background(), 2*time.Second)
		defer cancel()

		var deleted int
		err := db.QueryRowContext(ctx, `SELECT deleted FROM files WHERE storage_id = ? AND path = ? AND is_dir = 0 LIMIT 1;`, "ssd1", rel).Scan(&deleted)
		require.NoError(t, err, "source file row should exist in indexdb")
		require.Equal(t, 1, deleted, "source file should be marked deleted=1 (deferred)")

		// Mount should no longer expose the deleted source (eventlog delete event processed by FUSE).
		deadline = time.Now().Add(2 * time.Second)
		for time.Now().Before(deadline) {
			if !env.FileExistsInMountPoint(rel) {
				break
			}
			time.Sleep(50 * time.Millisecond)
		}
		require.False(t, env.FileExistsInMountPoint(rel), "mount should hide file after deferred delete")
	})
}

// TestMove_limit_shouldMoveOnlyNFiles verifies --limit restricts the number of files moved.
func TestMove_limit_shouldMoveOnlyNFiles(t *testing.T) {
	if os.Getenv(config.EnvIntegrationUseExistingMount) != "" {
		t.Skip("skip move flow test when using an existing mount")
	}

	jobName := "archive"

	mv := &config.MoverConfig{
		Enabled: boolPtr(true),
		Jobs: []config.MoverJobConfig{
			{
				Name:        jobName,
				Description: "limit test",
				Trigger:     config.MoverTriggerConfig{Type: "manual"},
				Source: config.MoverSourceConfig{
					Paths:    []string{"ssd1"},
					Patterns: []string{"library/**"},
				},
				Destination: config.MoverDestinationConfig{
					Paths:  []string{"hdd1"},
					Policy: "first_found",
				},
				DeleteSource: boolPtr(true),
				Verify:       boolPtr(false),
			},
		},
	}

	cfg := IntegrationConfig{
		Storages: []IntegrationStorage{
			{ID: "ssd1", Indexed: false, BasePath: "/mnt/ssd1/pfs-integration"},
			{ID: "hdd1", Indexed: false, BasePath: "/mnt/hdd1/pfs-integration"},
		},
		Targets:     []string{"ssd1"},
		ReadTargets: []string{"ssd1"},
		Mover:       mv,
	}

	withMountedFS(t, cfg, func(env *MountedFS) {
		// Create 3 files in source.
		for _, name := range []string{"a.txt", "b.txt", "c.txt"} {
			env.MustCreateFileInStoragePath(t, []byte("data-"+name), "ssd1", filepath.Join("library", name))
		}

		out, err := runPFSOutput(t, env, "move", env.MountName, "--job", jobName, "--limit", "2", "--progress=off")
		require.NoError(t, err, "pfs move --limit should succeed, output: %s", string(out))

		// Count how many files were actually moved to hdd1.
		moved := 0
		for _, name := range []string{"a.txt", "b.txt", "c.txt"} {
			if _, err := os.Stat(env.StoragePath("hdd1", filepath.Join("library", name))); err == nil {
				moved++
			}
		}
		require.Equal(t, 2, moved, "exactly 2 files should be moved with --limit 2")

		// Verify summary output mentions the move.
		require.True(t, strings.Contains(string(out), "Summary:"), "output should contain summary")
	})
}
