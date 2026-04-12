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
		Enabled: new(true),
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
				DeleteSource: new(true),
				Verify:       new(true),
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

// TestMove_skipIfExistsAny_shouldAvoidDuplicate verifies destination.skip_if_exists_any skips copying
// when the destination path already exists on any destination storage.
func TestMove_skipIfExistsAny_shouldAvoidDuplicate(t *testing.T) {
	if os.Getenv(config.EnvIntegrationUseExistingMount) != "" {
		t.Skip("skip move flow test when using an existing mount")
	}

	jobName := "mirror"
	rel := "library/dup/exists.txt"

	mv := &config.MoverConfig{
		Enabled: new(true),
		Jobs: []config.MoverJobConfig{
			{
				Name:        jobName,
				Description: "integration move skip_if_exists_any",
				Trigger:     config.MoverTriggerConfig{Type: "manual"},
				Source: config.MoverSourceConfig{
					Paths:    []string{"ssd1"},
					Patterns: []string{"library/**"},
				},
				Destination: config.MoverDestinationConfig{
					Paths:           []string{"hdd1", "hdd2"},
					Policy:          "first_found",
					SkipIfExistsAny: true,
					PathPreserving:  true,
				},
				DeleteSource: new(false),
				Verify:       new(false),
			},
		},
	}

	cfg := IntegrationConfig{
		Storages: []IntegrationStorage{
			{ID: "ssd1", Indexed: false, BasePath: "/mnt/ssd1/pfs-integration"},
			{ID: "hdd1", Indexed: false, BasePath: "/mnt/hdd1/pfs-integration"},
			{ID: "hdd2", Indexed: false, BasePath: "/mnt/hdd2/pfs-integration"},
		},
		Targets:     []string{"ssd1"},
		ReadTargets: []string{"ssd1"},
		Mover:       mv,
	}

	withMountedFS(t, cfg, func(env *MountedFS) {
		env.MustCreateFileInStoragePath(t, []byte("src"), "ssd1", rel)
		env.MustCreateFileInStoragePath(t, []byte("dst"), "hdd2", rel)

		mustRunPFS(t, env, "move", env.MountName, "--job", jobName, "--progress=off")

		require.FileExists(t, env.StoragePath("ssd1", rel), "source should remain (delete_source=false)")
		require.NoFileExists(t, env.StoragePath("hdd1", rel), "should not create duplicate on primary destination")
		require.FileExists(t, env.StoragePath("hdd2", rel), "existing destination should remain")
	})
}

// TestMove_shouldSkipOpenFileAndMoveAfterClose verifies open-file awareness:
// when a file is open via the mounted view, mover must skip it (skipped_open++)
// and only move it after the file handle is closed.
func TestMove_shouldSkipOpenFileAndMoveAfterClose(t *testing.T) {
	if os.Getenv(config.EnvIntegrationUseExistingMount) != "" {
		t.Skip("skip move flow test when using an existing mount")
	}

	jobName := "archive"
	rel := "library/open-aware.txt"

	mv := &config.MoverConfig{
		Enabled: new(true),
		Jobs: []config.MoverJobConfig{
			{
				Name:        jobName,
				Description: "integration open-file awareness",
				Trigger:     config.MoverTriggerConfig{Type: "manual"},
				Source: config.MoverSourceConfig{
					Paths:    []string{"ssd1"},
					Patterns: []string{"library/**"},
				},
				Destination: config.MoverDestinationConfig{
					Paths:  []string{"hdd1"},
					Policy: "first_found",
				},
				DeleteSource: new(true),
				Verify:       new(false),
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
		env.MustWriteFileInMountPoint(t, rel, []byte("hello-open"))

		f, err := os.Open(env.MountPath(rel))
		require.NoError(t, err)
		defer func() { _ = f.Close() }()

		out, err := runPFSOutput(t, env, "move", env.MountName, "--job", jobName, "--progress=off")
		require.NoError(t, err, "move should succeed (no-op) when skipping open files, output: %s", string(out))
		require.Contains(t, string(out), "skipped_open", "expected skipped_open in output, got: %s", string(out))

		require.FileExists(t, env.StoragePath("ssd1", rel))
		require.NoFileExists(t, env.StoragePath("hdd1", rel))

		require.NoError(t, f.Close())

		mustRunPFS(t, env, "move", env.MountName, "--job", jobName, "--progress=off")

		require.NoFileExists(t, env.StoragePath("ssd1", rel))
		require.FileExists(t, env.StoragePath("hdd1", rel))
	})
}

// TestMove_deleteEmptyDir_nonIndexed_shouldRemoveEmptySourceDirs verifies delete_empty_dir removes empty
// source directory chains after successful move when source is non-indexed.
func TestMove_deleteEmptyDir_nonIndexed_shouldRemoveEmptySourceDirs(t *testing.T) {
	if os.Getenv(config.EnvIntegrationUseExistingMount) != "" {
		t.Skip("skip move flow test when using an existing mount")
	}

	jobName := "archive"
	rel := filepath.Join("library", "a", "b", "c", "x.txt")

	mv := &config.MoverConfig{
		Enabled: new(true),
		Jobs: []config.MoverJobConfig{
			{
				Name:        jobName,
				Description: "delete empty dir test",
				Trigger:     config.MoverTriggerConfig{Type: "manual"},
				Source: config.MoverSourceConfig{
					Paths:    []string{"ssd1"},
					Patterns: []string{"library/**"},
				},
				Destination: config.MoverDestinationConfig{
					Paths:  []string{"hdd1"},
					Policy: "first_found",
				},
				DeleteSource:   new(true),
				DeleteEmptyDir: new(true),
				Verify:         new(false),
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
		env.MustCreateFileInStoragePath(t, []byte("hello"), "ssd1", rel)

		mustRunPFS(t, env, "move", env.MountName, "--job", jobName, "--progress=off")

		// The deep parent chain should be removed.
		require.NoDirExists(t, env.StoragePath("ssd1", filepath.Join("library", "a", "b", "c")))
		require.NoDirExists(t, env.StoragePath("ssd1", filepath.Join("library", "a", "b")))
		require.NoDirExists(t, env.StoragePath("ssd1", filepath.Join("library", "a")))
	})
}

// TestMove_debug_shouldPrintCandidates verifies candidates are printed when --debug is enabled.
func TestMove_debug_shouldPrintCandidates(t *testing.T) {
	if os.Getenv(config.EnvIntegrationUseExistingMount) != "" {
		t.Skip("skip move flow test when using an existing mount")
	}

	jobName := "archive"

	mv := &config.MoverConfig{
		Enabled: new(true),
		Jobs: []config.MoverJobConfig{
			{
				Name:        jobName,
				Description: "verbose test",
				Trigger:     config.MoverTriggerConfig{Type: "manual"},
				Source: config.MoverSourceConfig{
					Paths:    []string{"ssd1"},
					Patterns: []string{"library/**"},
				},
				Destination: config.MoverDestinationConfig{
					Paths:  []string{"hdd1"},
					Policy: "first_found",
				},
				DeleteSource: new(false),
				Verify:       new(false),
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
		env.MustCreateFileInStoragePath(t, []byte("data"), "ssd1", filepath.Join("library", "a.txt"))

		out, err := runPFSOutput(t, env, "move", env.MountName, "--job", jobName, "--dry-run", "--debug", "--progress=off")
		require.NoError(t, err, "pfs move should print candidates with --debug, output: %s", string(out))
		require.Contains(t, string(out), "Candidates:")
		require.Contains(t, string(out), "library/a.txt")
		require.Contains(t, string(out), "job=archive")
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
		Enabled: new(true),
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
				DeleteSource: new(true),
				Verify:       new(true),
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
		Enabled: new(true),
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
				DeleteSource: new(true),
				Verify:       new(true),
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

		// Source physical file still exists (deferred deletion - prune hasn't run).
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
		Enabled: new(true),
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
				DeleteSource: new(true),
				Verify:       new(false),
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
		require.True(t, strings.Contains(string(out), "Summary"), "output should contain summary")
	})
}

// TestMove_includeFile_shouldSelectOnlyListedFiles verifies that a job using only source.include_file
// (no source.patterns) moves exactly the files listed in the include file.
func TestMove_includeFile_shouldSelectOnlyListedFiles(t *testing.T) {
	if os.Getenv(config.EnvIntegrationUseExistingMount) != "" {
		t.Skip("skip move flow test when using an existing mount")
	}

	jobName := "promote"
	includeFile := filepath.Join(tmpDir, "include-"+sanitizeName(t.Name())+".txt")
	require.NoError(t, os.WriteFile(includeFile, []byte("library/a.txt\n"), 0o644))
	t.Cleanup(func() { _ = os.Remove(includeFile) })

	mv := &config.MoverConfig{
		Enabled: new(true),
		Jobs: []config.MoverJobConfig{
			{
				Name:    jobName,
				Trigger: config.MoverTriggerConfig{Type: "manual"},
				Source: config.MoverSourceConfig{
					Paths:       []string{"ssd1"},
					IncludeFile: includeFile,
				},
				Destination: config.MoverDestinationConfig{
					Paths:  []string{"hdd1"},
					Policy: "first_found",
				},
				DeleteSource: new(true),
				Verify:       new(false),
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
		env.MustCreateFileInStoragePath(t, []byte("aaa"), "ssd1", "library/a.txt")
		env.MustCreateFileInStoragePath(t, []byte("bbb"), "ssd1", "library/b.txt")

		mustRunPFS(t, env, "move", env.MountName, "--job", jobName, "--progress=off")

		// a.txt listed in include_file → moved.
		require.NoFileExists(t, env.StoragePath("ssd1", "library/a.txt"))
		require.FileExists(t, env.StoragePath("hdd1", "library/a.txt"))

		// b.txt not listed → stays.
		require.FileExists(t, env.StoragePath("ssd1", "library/b.txt"))
		require.NoFileExists(t, env.StoragePath("hdd1", "library/b.txt"))
	})
}

// TestMove_ignoreFile_shouldOverrideIncludeFile verifies that ignore_file always wins:
// a file matched by include_file but also listed in ignore_file is not moved.
func TestMove_ignoreFile_shouldOverrideIncludeFile(t *testing.T) {
	if os.Getenv(config.EnvIntegrationUseExistingMount) != "" {
		t.Skip("skip move flow test when using an existing mount")
	}

	jobName := "promote"
	base := sanitizeName(t.Name())
	includeFile := filepath.Join(tmpDir, "include-"+base+".txt")
	ignoreFile := filepath.Join(tmpDir, "ignore-"+base+".txt")
	require.NoError(t, os.WriteFile(includeFile, []byte("library/a.txt\n"), 0o644))
	require.NoError(t, os.WriteFile(ignoreFile, []byte("library/a.txt\n"), 0o644))
	t.Cleanup(func() { _ = os.Remove(includeFile); _ = os.Remove(ignoreFile) })

	mv := &config.MoverConfig{
		Enabled: new(true),
		Jobs: []config.MoverJobConfig{
			{
				Name:    jobName,
				Trigger: config.MoverTriggerConfig{Type: "manual"},
				Source: config.MoverSourceConfig{
					Paths:       []string{"ssd1"},
					IncludeFile: includeFile,
					IgnoreFile:  ignoreFile,
				},
				Destination: config.MoverDestinationConfig{
					Paths:  []string{"hdd1"},
					Policy: "first_found",
				},
				DeleteSource: new(true),
				Verify:       new(false),
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
		env.MustCreateFileInStoragePath(t, []byte("aaa"), "ssd1", "library/a.txt")

		mustRunPFS(t, env, "move", env.MountName, "--job", jobName, "--progress=off")

		// ignore_file wins → file stays on source.
		require.FileExists(t, env.StoragePath("ssd1", "library/a.txt"))
		require.NoFileExists(t, env.StoragePath("hdd1", "library/a.txt"))
	})
}

// TestMove_missingIncludeFile_shouldFailJob verifies that pointing include_file at a non-existent
// file causes the move command to fail rather than silently treating the list as empty.
func TestMove_missingIncludeFile_shouldFailJob(t *testing.T) {
	if os.Getenv(config.EnvIntegrationUseExistingMount) != "" {
		t.Skip("skip move flow test when using an existing mount")
	}

	jobName := "promote"

	mv := &config.MoverConfig{
		Enabled: new(true),
		Jobs: []config.MoverJobConfig{
			{
				Name:    jobName,
				Trigger: config.MoverTriggerConfig{Type: "manual"},
				Source: config.MoverSourceConfig{
					Paths:       []string{"ssd1"},
					IncludeFile: "/does/not/exist.txt",
				},
				Destination: config.MoverDestinationConfig{
					Paths:  []string{"hdd1"},
					Policy: "first_found",
				},
				DeleteSource: new(false),
				Verify:       new(false),
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
		env.MustCreateFileInStoragePath(t, []byte("data"), "ssd1", "library/a.txt")

		_, err := runPFSOutput(t, env, "move", env.MountName, "--job", jobName, "--progress=off")
		require.Error(t, err, "move should fail when include_file does not exist")
	})
}

// TestMove_missingIgnoreFile_shouldFailJob verifies that pointing ignore_file at a non-existent
// file causes the move command to fail rather than silently ignoring it.
func TestMove_missingIgnoreFile_shouldFailJob(t *testing.T) {
	if os.Getenv(config.EnvIntegrationUseExistingMount) != "" {
		t.Skip("skip move flow test when using an existing mount")
	}

	jobName := "promote"

	mv := &config.MoverConfig{
		Enabled: new(true),
		Jobs: []config.MoverJobConfig{
			{
				Name:    jobName,
				Trigger: config.MoverTriggerConfig{Type: "manual"},
				Source: config.MoverSourceConfig{
					Paths:      []string{"ssd1"},
					Patterns:   []string{"library/**"},
					IgnoreFile: "/does/not/exist.txt",
				},
				Destination: config.MoverDestinationConfig{
					Paths:  []string{"hdd1"},
					Policy: "first_found",
				},
				DeleteSource: new(false),
				Verify:       new(false),
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
		env.MustCreateFileInStoragePath(t, []byte("data"), "ssd1", "library/a.txt")

		_, err := runPFSOutput(t, env, "move", env.MountName, "--job", jobName, "--progress=off")
		require.Error(t, err, "move should fail when ignore_file does not exist")
	})
}

// TestMove_patternsAndIncludeFile_shouldMatchEither verifies that candidate selection is OR:
// match(source.patterns) OR match(source.include_file).
func TestMove_patternsAndIncludeFile_shouldMatchEither(t *testing.T) {
	if os.Getenv(config.EnvIntegrationUseExistingMount) != "" {
		t.Skip("skip move flow test when using an existing mount")
	}

	jobName := "promote"
	includeFile := filepath.Join(tmpDir, "include-"+sanitizeName(t.Name())+".txt")
	require.NoError(t, os.WriteFile(includeFile, []byte("library/a.txt\n"), 0o644))
	t.Cleanup(func() { _ = os.Remove(includeFile) })

	mv := &config.MoverConfig{
		Enabled: new(true),
		Jobs: []config.MoverJobConfig{
			{
				Name:    jobName,
				Trigger: config.MoverTriggerConfig{Type: "manual"},
				Source: config.MoverSourceConfig{
					Paths:       []string{"ssd1"},
					Patterns:    []string{"library/b.txt"},
					IncludeFile: includeFile,
				},
				Destination: config.MoverDestinationConfig{
					Paths:  []string{"hdd1"},
					Policy: "first_found",
				},
				DeleteSource: new(true),
				Verify:       new(false),
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
		env.MustCreateFileInStoragePath(t, []byte("aaa"), "ssd1", "library/a.txt")
		env.MustCreateFileInStoragePath(t, []byte("bbb"), "ssd1", "library/b.txt")
		env.MustCreateFileInStoragePath(t, []byte("ccc"), "ssd1", "library/c.txt")

		mustRunPFS(t, env, "move", env.MountName, "--job", jobName, "--progress=off")

		// a.txt matched by include_file → moved.
		require.NoFileExists(t, env.StoragePath("ssd1", "library/a.txt"))
		require.FileExists(t, env.StoragePath("hdd1", "library/a.txt"))

		// b.txt matched by patterns → moved.
		require.NoFileExists(t, env.StoragePath("ssd1", "library/b.txt"))
		require.FileExists(t, env.StoragePath("hdd1", "library/b.txt"))

		// c.txt matched by neither → stays.
		require.FileExists(t, env.StoragePath("ssd1", "library/c.txt"))
		require.NoFileExists(t, env.StoragePath("hdd1", "library/c.txt"))
	})
}

// TestMove_includeFileWithComments_shouldIgnoreCommentLines verifies that comment lines (starting
// with #) and blank lines in include_file are properly skipped during a real move flow.
func TestMove_includeFileWithComments_shouldIgnoreCommentLines(t *testing.T) {
	if os.Getenv(config.EnvIntegrationUseExistingMount) != "" {
		t.Skip("skip move flow test when using an existing mount")
	}

	jobName := "promote"
	includeFile := filepath.Join(tmpDir, "include-"+sanitizeName(t.Name())+".txt")
	require.NoError(t, os.WriteFile(includeFile, []byte("# pinned files for promotion\n\nlibrary/a.txt\n  # not this one\n"), 0o644))
	t.Cleanup(func() { _ = os.Remove(includeFile) })

	mv := &config.MoverConfig{
		Enabled: new(true),
		Jobs: []config.MoverJobConfig{
			{
				Name:    jobName,
				Trigger: config.MoverTriggerConfig{Type: "manual"},
				Source: config.MoverSourceConfig{
					Paths:       []string{"ssd1"},
					IncludeFile: includeFile,
				},
				Destination: config.MoverDestinationConfig{
					Paths:  []string{"hdd1"},
					Policy: "first_found",
				},
				DeleteSource: new(true),
				Verify:       new(false),
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
		env.MustCreateFileInStoragePath(t, []byte("aaa"), "ssd1", "library/a.txt")

		mustRunPFS(t, env, "move", env.MountName, "--job", jobName, "--progress=off")

		// Only library/a.txt should be moved; comments and blanks are skipped.
		require.NoFileExists(t, env.StoragePath("ssd1", "library/a.txt"))
		require.FileExists(t, env.StoragePath("hdd1", "library/a.txt"))
	})
}
