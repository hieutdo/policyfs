//go:build integration

package integration

import (
	"context"
	"database/sql"
	"os"
	"os/exec"
	"path/filepath"
	"strings"
	"syscall"
	"testing"
	"time"

	"github.com/hieutdo/policyfs/internal/config"
	"github.com/stretchr/testify/require"
)

// setMoveTestMetadata assigns synthetic ownership and exact permission bits to a move fixture path.
func setMoveTestMetadata(t *testing.T, path string, uid uint32, gid uint32, mode os.FileMode) {
	t.Helper()
	require.NoError(t, os.Chown(path, int(uid), int(gid)))
	require.NoError(t, syscall.Chmod(path, uint32(mode)))
}

// runMoveTestAsIDs executes one filesystem command with synthetic numeric credentials.
func runMoveTestAsIDs(t *testing.T, uid uint32, gid uint32, name string, args ...string) {
	t.Helper()
	cmd := exec.Command(name, args...)
	cmd.SysProcAttr = &syscall.SysProcAttr{Credential: &syscall.Credential{Uid: uid, Gid: gid}}
	output, err := cmd.CombinedOutput()
	require.NoError(t, err, "expected %s to succeed as uid=%d gid=%d, output=%s", name, uid, gid, string(output))
}

// TestMove_missingDestinationDirs_shouldPreserveMetadataAndAllowMediaUserOperations verifies root mover copies
// directory metadata, handles missing intermediate components, preserves existing directories, and permits rename/delete.
func TestMove_missingDestinationDirs_shouldPreserveMetadataAndAllowMediaUserOperations(t *testing.T) {
	if os.Geteuid() != 0 {
		t.Skip("synthetic ownership test requires root")
	}

	jobName := "metadata"
	movieRel := filepath.Join("library", "movies", "Test Movie (2026)", "Test Movie.mkv")
	newRel := filepath.Join("library", "new", "season", "Movie", "movie.mkv")
	existingRel := filepath.Join("library", "existing", "existing.mkv")
	uid := uint32(50000 + os.Getpid()%1000)
	gid := uid + 1

	mv := &config.MoverConfig{
		Enabled: new(true),
		Jobs: []config.MoverJobConfig{{
			Name:    jobName,
			Trigger: config.MoverTriggerConfig{Type: "manual"},
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
		}},
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
		env.MustCreateFileInStoragePath(t, []byte("movie"), "ssd1", movieRel)
		env.MustCreateFileInStoragePath(t, []byte("new"), "ssd1", newRel)
		env.MustCreateFileInStoragePath(t, []byte("existing"), "ssd1", existingRel)

		srcRoot := env.StorageRoot("ssd1")
		dstRoot := env.StorageRoot("hdd1")
		setMoveTestMetadata(t, filepath.Join(srcRoot, "library"), uid, gid, 0o755)
		setMoveTestMetadata(t, filepath.Join(srcRoot, "library", "movies"), uid, gid, 0o2775)
		setMoveTestMetadata(t, filepath.Join(srcRoot, "library", "movies", "Test Movie (2026)"), uid, gid, 0o2775)
		setMoveTestMetadata(t, filepath.Join(srcRoot, "library", "new"), uid, gid, 0o755)
		setMoveTestMetadata(t, filepath.Join(srcRoot, "library", "new", "season"), uid, gid, 0o2770)
		setMoveTestMetadata(t, filepath.Join(srcRoot, "library", "new", "season", "Movie"), uid, gid, 0o0750)
		setMoveTestMetadata(t, filepath.Join(srcRoot, "library", "existing"), uid, gid, 0o2775)
		for _, rel := range []string{movieRel, newRel, existingRel} {
			setMoveTestMetadata(t, filepath.Join(srcRoot, rel), uid, gid, 0o664)
		}

		dstMovieParent := filepath.Join(dstRoot, "library", "movies")
		require.NoError(t, os.MkdirAll(dstMovieParent, 0o755))
		setMoveTestMetadata(t, filepath.Join(dstRoot, "library"), 0, gid, 0o2775)
		setMoveTestMetadata(t, dstMovieParent, 0, gid, 0o2775)
		dstExisting := filepath.Join(dstRoot, "library", "existing")
		require.NoError(t, os.MkdirAll(dstExisting, 0o755))
		setMoveTestMetadata(t, dstExisting, 0, gid, 0o2701)
		existingBefore := env.MustStatT(t, dstExisting)

		mustRunPFS(t, env, "move", env.MountName, "--job", jobName, "--progress=off")

		require.NoFileExists(t, env.StoragePath("ssd1", movieRel))
		require.NoFileExists(t, env.StoragePath("ssd1", newRel))
		require.NoFileExists(t, env.StoragePath("ssd1", existingRel))
		require.Equal(t, []byte("movie"), mustReadMoveFile(t, env.StoragePath("hdd1", movieRel)))
		require.Equal(t, []byte("new"), mustReadMoveFile(t, env.StoragePath("hdd1", newRel)))

		for _, tc := range []struct {
			rel  string
			mode uint32
		}{
			{rel: filepath.Join("library", "movies", "Test Movie (2026)"), mode: 0o2775},
			{rel: filepath.Join("library", "new"), mode: 0o2775},
			{rel: filepath.Join("library", "new", "season"), mode: 0o2770},
			{rel: filepath.Join("library", "new", "season", "Movie"), mode: 0o2770},
		} {
			st := env.MustStatT(t, env.StoragePath("hdd1", tc.rel))
			require.Equal(t, uid, st.Uid, "expected new directory UID for %s, got %d", tc.rel, st.Uid)
			require.Equal(t, gid, st.Gid, "expected new directory GID for %s, got %d", tc.rel, st.Gid)
			require.Equal(t, tc.mode, uint32(st.Mode)&0o7777, "expected mode %04o for %s, got %04o", tc.mode, tc.rel, uint32(st.Mode)&0o7777)
		}

		movieFile := env.StoragePath("hdd1", movieRel)
		movieSt := env.MustStatT(t, movieFile)
		require.Equal(t, uid, movieSt.Uid, "expected destination file UID, got %d", movieSt.Uid)
		require.Equal(t, gid, movieSt.Gid, "expected destination file GID, got %d", movieSt.Gid)
		require.Equal(t, uint32(0o664), uint32(movieSt.Mode)&0o7777, "expected destination file mode 0664, got %04o", uint32(movieSt.Mode)&0o7777)

		existingAfter := env.MustStatT(t, dstExisting)
		require.Equal(t, existingBefore.Uid, existingAfter.Uid, "existing destination UID changed")
		require.Equal(t, existingBefore.Gid, existingAfter.Gid, "existing destination GID changed")
		require.Equal(t, uint32(existingBefore.Mode)&0o7777, uint32(existingAfter.Mode)&0o7777, "existing destination mode changed")

		renamed := movieFile + ".renamed"
		runMoveTestAsIDs(t, uid, gid, "mv", movieFile, renamed)
		runMoveTestAsIDs(t, uid, gid, "rm", renamed)
	})
}

// mustReadMoveFile reads a physical move fixture and fails the test on I/O errors.
func mustReadMoveFile(t *testing.T, path string) []byte {
	t.Helper()
	content, err := os.ReadFile(path)
	require.NoError(t, err, "expected moved file to be readable: %s", path)
	return content
}

// TestMove_deleteEmptyDir_shouldRecreateWritableDirectories verifies a later move recreates a deleted source chain correctly.
func TestMove_deleteEmptyDir_shouldRecreateWritableDirectories(t *testing.T) {
	if os.Geteuid() != 0 {
		t.Skip("synthetic ownership test requires root")
	}

	jobRel := filepath.Join("library", "roundtrip", "movie.mkv")
	uid := uint32(51000 + os.Getpid()%1000)
	gid := uid + 1
	jobs := []config.MoverJobConfig{
		{
			Name:    "forward",
			Trigger: config.MoverTriggerConfig{Type: "manual"},
			Source:  config.MoverSourceConfig{Paths: []string{"ssd1"}, Patterns: []string{"library/**"}},
			Destination: config.MoverDestinationConfig{
				Paths:  []string{"hdd1"},
				Policy: "first_found",
			},
			DeleteSource:   new(true),
			DeleteEmptyDir: new(true),
		},
		{
			Name:    "backward",
			Trigger: config.MoverTriggerConfig{Type: "manual"},
			Source:  config.MoverSourceConfig{Paths: []string{"hdd1"}, Patterns: []string{"library/**"}},
			Destination: config.MoverDestinationConfig{
				Paths:  []string{"ssd1"},
				Policy: "first_found",
			},
			DeleteSource:   new(true),
			DeleteEmptyDir: new(true),
		},
	}
	cfg := IntegrationConfig{
		Storages: []IntegrationStorage{
			{ID: "ssd1", Indexed: false, BasePath: "/mnt/ssd1/pfs-integration"},
			{ID: "hdd1", Indexed: false, BasePath: "/mnt/hdd1/pfs-integration"},
		},
		Targets:     []string{"ssd1"},
		ReadTargets: []string{"ssd1"},
		Mover:       &config.MoverConfig{Enabled: new(true), Jobs: jobs},
	}

	withMountedFS(t, cfg, func(env *MountedFS) {
		env.MustCreateFileInStoragePath(t, []byte("roundtrip"), "ssd1", jobRel)
		srcFile := env.StoragePath("ssd1", jobRel)
		srcDir := filepath.Dir(srcFile)
		setMoveTestMetadata(t, srcDir, uid, gid, 0o2775)
		setMoveTestMetadata(t, srcFile, uid, gid, 0o664)

		mustRunPFS(t, env, "move", env.MountName, "--job", "forward", "--progress=off")

		require.NoDirExists(t, srcDir)
		require.NoFileExists(t, env.StoragePath("ssd1", jobRel))
		require.FileExists(t, env.StoragePath("hdd1", jobRel))

		mustRunPFS(t, env, "move", env.MountName, "--job", "backward", "--progress=off")

		require.FileExists(t, env.StoragePath("ssd1", jobRel))
		require.NoFileExists(t, env.StoragePath("hdd1", jobRel))
		recreatedDir := filepath.Dir(env.StoragePath("ssd1", jobRel))
		st := env.MustStatT(t, recreatedDir)
		require.Equal(t, uid, st.Uid, "expected recreated directory UID, got %d", st.Uid)
		require.Equal(t, gid, st.Gid, "expected recreated directory GID, got %d", st.Gid)
		require.Equal(t, uint32(0o2775), uint32(st.Mode)&0o7777, "expected recreated directory mode 2775, got %04o", uint32(st.Mode)&0o7777)
	})
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

// TestMove_pathPreservingBelowMinFree_shouldFallbackToEligibleDestination verifies an ineligible existing parent falls back to another destination.
func TestMove_pathPreservingBelowMinFree_shouldFallbackToEligibleDestination(t *testing.T) {
	if os.Getenv(config.EnvIntegrationUseExistingMount) != "" {
		t.Skip("skip move flow test when using an existing mount")
	}

	jobName := "archive-fallback"
	rel := "library/tv/show/episode.mkv"
	content := []byte("path-preserving-fallback")

	mv := &config.MoverConfig{
		Enabled: new(true),
		Jobs: []config.MoverJobConfig{{
			Name:    jobName,
			Trigger: config.MoverTriggerConfig{Type: "manual"},
			Source: config.MoverSourceConfig{
				Paths:    []string{"ssd1"},
				Patterns: []string{"library/**"},
			},
			Destination: config.MoverDestinationConfig{
				Paths:          []string{"hdd1", "hdd2"},
				Policy:         "first_found",
				PathPreserving: true,
			},
			DeleteSource: new(true),
			Verify:       new(true),
		}},
	}

	cfg := IntegrationConfig{
		Storages: []IntegrationStorage{
			{ID: "ssd1", Indexed: false, BasePath: "/mnt/ssd1/pfs-integration"},
			{ID: "hdd1", Indexed: false, MinFreeGB: 1e9, BasePath: "/mnt/hdd1/pfs-integration"},
			{ID: "hdd2", Indexed: false, BasePath: "/mnt/hdd2/pfs-integration"},
		},
		Targets:     []string{"ssd1"},
		ReadTargets: []string{"ssd1"},
		Mover:       mv,
	}

	withMountedFS(t, cfg, func(env *MountedFS) {
		env.MustCreateDirInStoragePath(t, "hdd1", filepath.Dir(rel))
		env.MustCreateFileInStoragePath(t, content, "ssd1", rel)

		mustRunPFS(t, env, "move", env.MountName, "--job", jobName, "--progress=off")

		require.NoFileExists(t, env.StoragePath("ssd1", rel))
		require.NoFileExists(t, env.StoragePath("hdd1", rel))
		require.FileExists(t, env.StoragePath("hdd2", rel))
		require.Equal(t, content, env.MustReadFileInStoragePath(t, "hdd2", rel))
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
