//go:build integration

package integration

import (
	"os"
	"os/exec"
	"strings"
	"testing"
	"time"

	"github.com/hieutdo/policyfs/internal/config"
	"github.com/stretchr/testify/require"
	"golang.org/x/sys/unix"
)

// createIndexedOnlyCfg returns a minimal config with a single indexed storage.
func createIndexedOnlyCfg() IntegrationConfig {
	return IntegrationConfig{
		Storages: []IntegrationStorage{
			{ID: "hdd1", Indexed: true, BasePath: "/mnt/hdd1/pfs-integration"},
		},
		Targets:     []string{"hdd1"},
		ReadTargets: []string{"hdd1"},
	}
}

// TestIndexed_lookup_shouldResolveTopLevelDirectoryReturnedByReaddir verifies that an indexed-only
// top-level directory remains stat-able after readdir exposes it from the DB-backed view.
func TestIndexed_lookup_shouldResolveTopLevelDirectoryReturnedByReaddir(t *testing.T) {
	withMountedFS(t, createIndexedOnlyCfg(), func(env *MountedFS) {
		rel := "movies/existing/file.txt"
		content := []byte("indexed-only")
		env.MustCreateFileInStoragePath(t, content, "hdd1", rel)

		mustRunPFS(t, env, "index", env.MountName)

		rootEntries := env.MustReadDirInMountPoint(t, "")
		require.Contains(t, dirEntryNames(rootEntries), "movies")

		moviesInfo := env.MustLstatInMountPoint(t, "movies")
		require.True(t, moviesInfo.IsDir())

		moviesEntries := env.MustReadDirInMountPoint(t, "movies")
		require.Contains(t, dirEntryNames(moviesEntries), "existing")

		existingInfo := env.MustLstatInMountPoint(t, "movies/existing")
		require.True(t, existingInfo.IsDir())

		got := env.MustReadFileInMountPoint(t, rel)
		require.Equal(t, content, got)
	})
}

// TestMixed_lookup_shouldResolveIndexedTopLevelDirectoryReturnedByReaddir verifies that a top-level
// directory listed from an indexed backend remains stat-able when the first read target is unindexed.
func TestMixed_lookup_shouldResolveIndexedTopLevelDirectoryReturnedByReaddir(t *testing.T) {
	withMountedFS(t, createIndexedCfg(), func(env *MountedFS) {
		rel := "movies/existing/file.txt"
		content := []byte("mixed-indexed")
		env.MustCreateFileInStoragePath(t, content, "hdd1", rel)

		mustRunPFS(t, env, "index", env.MountName)

		rootEntries := env.MustReadDirInMountPoint(t, "")
		require.Contains(t, dirEntryNames(rootEntries), "movies")

		moviesInfo := env.MustLstatInMountPoint(t, "movies")
		require.True(t, moviesInfo.IsDir())

		moviesEntries := env.MustReadDirInMountPoint(t, "movies")
		require.Contains(t, dirEntryNames(moviesEntries), "existing")

		existingInfo := env.MustLstatInMountPoint(t, "movies/existing")
		require.True(t, existingInfo.IsDir())

		got := env.MustReadFileInMountPoint(t, rel)
		require.Equal(t, content, got)
	})
}

// TestMixed_lookup_shouldResolveIndexedChildrenWhenTopLevelDirExistsOnBothTargets verifies that a
// partially shadowed top-level directory still resolves indexed-only children after readdir.
func TestMixed_lookup_shouldResolveIndexedChildrenWhenTopLevelDirExistsOnBothTargets(t *testing.T) {
	withMountedFS(t, createIndexedCfg(), func(env *MountedFS) {
		rel := "movies/existing/file.txt"
		content := []byte("shadowed")
		env.MustCreateDirInStoragePath(t, "ssd1", "movies")
		env.MustCreateFileInStoragePath(t, content, "hdd1", rel)

		mustRunPFS(t, env, "index", env.MountName)

		rootEntries := env.MustReadDirInMountPoint(t, "")
		require.Contains(t, dirEntryNames(rootEntries), "movies")

		moviesInfo := env.MustLstatInMountPoint(t, "movies")
		require.True(t, moviesInfo.IsDir())

		moviesEntries := env.MustReadDirInMountPoint(t, "movies")
		require.Contains(t, dirEntryNames(moviesEntries), "existing")

		existingInfo := env.MustLstatInMountPoint(t, "movies/existing")
		require.True(t, existingInfo.IsDir())

		got := env.MustReadFileInMountPoint(t, rel)
		require.Equal(t, content, got)
	})
}

// TestMixed_lookup_shouldResolveIndexedFileWhenCatchAllUsesStorageGroups verifies that a catch-all
// rule using storage groups does not lose file visibility on indexed storage behind an unindexed
// SSD read target.
func TestMixed_lookup_shouldResolveIndexedFileWhenCatchAllUsesStorageGroups(t *testing.T) {
	cfg := IntegrationConfig{
		Storages: []IntegrationStorage{
			{ID: "ssd1", Indexed: false, BasePath: "/mnt/ssd1/pfs-integration"},
			{ID: "hdd1", Indexed: true, BasePath: "/mnt/hdd1/pfs-integration"},
		},
		StorageGroups: map[string][]string{
			"ssds": {"ssd1"},
			"hdds": {"hdd1"},
		},
		RoutingRules: []config.RoutingRule{{
			Match:          "**",
			ReadTargets:    []string{"ssds", "hdds"},
			WriteTargets:   []string{"ssds"},
			WritePolicy:    "most_free",
			PathPreserving: true,
		}},
	}

	withMountedFS(t, cfg, func(env *MountedFS) {
		rel := "library/movies/existing/file.txt"
		content := []byte("groups-regression")
		env.MustCreateFileInStoragePath(t, content, "hdd1", rel)

		mustRunPFS(t, env, "index", env.MountName)

		rootEntries := env.MustReadDirInMountPoint(t, "")
		require.Contains(t, dirEntryNames(rootEntries), "library")

		libraryInfo := env.MustLstatInMountPoint(t, "library")
		require.True(t, libraryInfo.IsDir())

		moviesInfo := env.MustLstatInMountPoint(t, "library/movies")
		require.True(t, moviesInfo.IsDir())

		existingEntries := env.MustReadDirInMountPoint(t, "library/movies/existing")
		require.Contains(t, dirEntryNames(existingEntries), "file.txt")

		fileInfo := env.MustLstatInMountPoint(t, rel)
		require.False(t, fileInfo.IsDir())

		got := env.MustReadFileInMountPoint(t, rel)
		require.Equal(t, content, got)
	})
}

// TestMixed_lookup_shouldNotExposeFileHiddenByFirstMatchReadRule verifies that lookup/getattr do
// not widen file visibility beyond first-match read routing, even when readdir unions descendant
// rules for parent directories.
func TestMixed_lookup_shouldNotExposeFileHiddenByFirstMatchReadRule(t *testing.T) {
	cfg := IntegrationConfig{
		Storages: []IntegrationStorage{
			{ID: "ssd2", Indexed: false, BasePath: "/mnt/ssd2/pfs-integration"},
			{ID: "hdd1", Indexed: true, BasePath: "/mnt/hdd1/pfs-integration"},
		},
		RoutingRules: []config.RoutingRule{
			{Match: "library/movies/**", ReadTargets: []string{"hdd1"}, WriteTargets: []string{"hdd1"}},
			{Match: "**", ReadTargets: []string{"ssd2"}, WriteTargets: []string{"ssd2"}},
		},
	}

	withMountedFS(t, cfg, func(env *MountedFS) {
		const rel = "library/movies/hidden-from-first-match.txt"
		env.MustCreateFileInStoragePath(t, []byte("hidden-on-catchall"), "ssd2", rel)

		mustRunPFS(t, env, "index", env.MountName)

		entries := env.MustReadDirInMountPoint(t, "library/movies")
		require.NotContains(t, dirEntryNames(entries), "hidden-from-first-match.txt")

		_, err := os.Lstat(env.MountPath(rel))
		require.Error(t, err)
		require.ErrorIs(t, err, os.ErrNotExist)

		_, err = env.ReadFileInMountPoint(rel)
		require.Error(t, err)
		require.ErrorIs(t, err, os.ErrNotExist)
	})
}

// TestMixed_ls_shouldNotShowQuestionMarksForIndexedDirectoryReturnedByReaddir verifies the shell
// symptom directly by asserting `ls -ld` succeeds without placeholder metadata.
func TestMixed_ls_shouldNotShowQuestionMarksForIndexedDirectoryReturnedByReaddir(t *testing.T) {
	cfg := IntegrationConfig{
		Storages: []IntegrationStorage{
			{ID: "ssd2", Indexed: false, BasePath: "/mnt/ssd2/pfs-integration"},
			{ID: "hdd1", Indexed: true, BasePath: "/mnt/hdd1/pfs-integration"},
		},
		RoutingRules: []config.RoutingRule{
			{Match: "library/**", ReadTargets: []string{"ssd2", "hdd1"}, WriteTargets: []string{"ssd2"}},
			{Match: "**", ReadTargets: []string{"ssd2"}, WriteTargets: []string{"ssd2"}},
		},
	}

	withMountedFS(t, cfg, func(env *MountedFS) {
		rel := "library/movies/existing/file.txt"
		env.MustCreateFileInStoragePath(t, []byte("ls-smoke"), "hdd1", rel)

		mustRunPFS(t, env, "index", env.MountName)

		entries := env.MustReadDirInMountPoint(t, "library")
		require.Contains(t, dirEntryNames(entries), "movies")

		// Descendant inside the specific rule resolves through Rule 1's read targets.
		cmd := exec.Command("ls", "-ld", env.MountPath("library/movies"))
		out, err := cmd.CombinedOutput()
		require.NoError(t, err, "expected ls -ld to succeed: out=%s", string(out))
		require.NotContains(t, string(out), "?????????")
		require.NotContains(t, strings.ToLower(string(out)), "cannot access")

		// The partition-boundary directory `library` itself must also stat cleanly.
		// `library/**` does not match `library` to first-match-wins, but readdir surfaces
		// it via descendant-rule union, so lookup must agree.
		cmd = exec.Command("ls", "-ld", env.MountPath("library"))
		out, err = cmd.CombinedOutput()
		require.NoError(t, err, "expected ls -ld on partition-boundary dir to succeed: out=%s", string(out))
		require.NotContains(t, string(out), "?????????")
		require.NotContains(t, strings.ToLower(string(out)), "cannot access")
	})
}

// TestMixed_lookup_shouldResolveIndexedDirAtRulePartitionBoundary reproduces the issue-5 symptom
// where a routing rule partitions the namespace so that the catch-all rule (which alone matches
// the partition-boundary directory under first-match-wins) does not include the indexed storage
// where the directory actually lives. readdir surfaces the entry via union over descendant-matching
// rules; lookup/getattr must also see the indexed target so `lstat` does not return ENOENT.
func TestMixed_lookup_shouldResolveIndexedDirAtRulePartitionBoundary(t *testing.T) {
	cfg := IntegrationConfig{
		Storages: []IntegrationStorage{
			{ID: "ssd2", Indexed: false, BasePath: "/mnt/ssd2/pfs-integration"},
			{ID: "hdd1", Indexed: true, BasePath: "/mnt/hdd1/pfs-integration"},
		},
		RoutingRules: []config.RoutingRule{
			// Specific rule routes only to the indexed storage. `library/movies/**` matches
			// descendants but does NOT match `library` itself under first-match-wins.
			{Match: "library/movies/**", ReadTargets: []string{"hdd1"}, WriteTargets: []string{"hdd1"}},
			// Catch-all routes only to the unindexed cache. Without ResolveLookupTargets, lookup
			// for `library` resolves here and fails because the cache has nothing under `library`.
			{Match: "**", ReadTargets: []string{"ssd2"}, WriteTargets: []string{"ssd2"}},
		},
	}

	withMountedFS(t, cfg, func(env *MountedFS) {
		rel := "library/movies/existing/file.txt"
		content := []byte("partition-boundary")
		env.MustCreateFileInStoragePath(t, content, "hdd1", rel)

		mustRunPFS(t, env, "index", env.MountName)

		rootEntries := env.MustReadDirInMountPoint(t, "")
		require.Contains(t, dirEntryNames(rootEntries), "library")

		libraryInfo := env.MustLstatInMountPoint(t, "library")
		require.True(t, libraryInfo.IsDir())

		libraryEntries := env.MustReadDirInMountPoint(t, "library")
		require.Contains(t, dirEntryNames(libraryEntries), "movies")

		moviesInfo := env.MustLstatInMountPoint(t, "library/movies")
		require.True(t, moviesInfo.IsDir())

		got := env.MustReadFileInMountPoint(t, rel)
		require.Equal(t, content, got)
	})
}

// TestMixed_statx_shouldResolveSecondaryTargetWhenPrimaryStorageLacksPath verifies that statx(2)
// requests (including STATX_BTIME, which triggers FUSE_STATX on Linux 6.9+) succeed for directories
// and files that exist only on a secondary read target and not on storage_paths[0].
func TestMixed_statx_shouldResolveSecondaryTargetWhenPrimaryStorageLacksPath(t *testing.T) {
	cfg := IntegrationConfig{
		Storages: []IntegrationStorage{
			{ID: "ssd1", Indexed: false, BasePath: "/mnt/ssd1/pfs-integration"},
			{ID: "ssd2", Indexed: false, BasePath: "/mnt/ssd2/pfs-integration"},
			{ID: "hdd1", Indexed: true, BasePath: "/mnt/hdd1/pfs-integration"},
		},
		StorageGroups: map[string][]string{
			"ssds": {"ssd1"},
			"hdds": {"ssd2", "hdd1"},
		},
		RoutingRules: []config.RoutingRule{{
			Match:          "**",
			ReadTargets:    []string{"ssds", "hdds"},
			WriteTargets:   []string{"ssds"},
			WritePolicy:    "first_found",
			PathPreserving: true,
		}},
	}

	withMountedFS(t, cfg, func(env *MountedFS) {
		unindexedContent := []byte("unindexed-secondary")
		indexedContent := []byte("indexed-secondary")
		env.MustCreateFileInStoragePath(t, unindexedContent, "ssd2", "download_services/tv/show.mkv")
		env.MustCreateFileInStoragePath(t, indexedContent, "hdd1", "library/movies/film.mkv")

		mustRunPFS(t, env, "index", env.MountName)

		t.Run("should preserve physical metadata for an unindexed sparse file", func(t *testing.T) {
			const rel = "sparse.bin"
			env.MustCreateFileInStoragePath(t, nil, "ssd1", rel)
			physicalPath := env.StoragePath("ssd1", rel)
			require.NoError(t, os.Truncate(physicalPath, 32<<20))
			atime := time.Unix(1_700_000_000, 123_456_789)
			require.NoError(t, os.Chtimes(physicalPath, atime, atime))

			want := env.MustStatT(t, physicalPath)
			got := env.MustStatT(t, env.MountPath(rel))
			require.Equal(t, want.Atim, got.Atim)
			require.Equal(t, want.Ctim, got.Ctim)
			require.Equal(t, want.Blocks, got.Blocks)
			require.Equal(t, want.Blksize, got.Blksize)
		})

		// Cached indexed metadata must remain readable without the backing directory.
		require.NoError(t, os.Rename(env.StoragePath("hdd1", "library"), env.StoragePath("hdd1", "offline-library")))

		cases := []struct {
			name     string
			rel      string
			wantType uint16
			wantSize uint64
		}{
			{name: "should resolve non-indexed secondary top-level dir", rel: "download_services", wantType: unix.S_IFDIR},
			{name: "should resolve non-indexed secondary nested dir", rel: "download_services/tv", wantType: unix.S_IFDIR},
			{name: "should resolve non-indexed secondary file", rel: "download_services/tv/show.mkv", wantType: unix.S_IFREG, wantSize: uint64(len(unindexedContent))},
			{name: "should resolve indexed secondary top-level dir", rel: "library", wantType: unix.S_IFDIR},
			{name: "should resolve indexed secondary nested dir", rel: "library/movies", wantType: unix.S_IFDIR},
			{name: "should resolve indexed secondary file", rel: "library/movies/film.mkv", wantType: unix.S_IFREG, wantSize: uint64(len(indexedContent))},
		}

		for _, tc := range cases {
			t.Run(tc.name, func(t *testing.T) {
				var stx unix.Statx_t
				err := unix.Statx(
					unix.AT_FDCWD,
					env.MountPath(tc.rel),
					unix.AT_SYMLINK_NOFOLLOW|unix.AT_STATX_FORCE_SYNC,
					unix.STATX_BASIC_STATS|unix.STATX_BTIME,
					&stx,
				)
				require.NoError(t, err, "expected statx to succeed for %s", tc.rel)
				require.Equal(t, tc.wantType, stx.Mode&unix.S_IFMT)
				if tc.wantType == unix.S_IFREG {
					require.Equal(t, tc.wantSize, stx.Size)
				}
				if strings.HasPrefix(tc.rel, "library") {
					require.Zero(t, stx.Mask&unix.STATX_BTIME)
				}
			})
		}

		t.Run("should list mount root and secondary parent directory with ls -la without placeholder metadata", func(t *testing.T) {
			for _, dir := range []string{env.MountPoint, env.MountPath("download_services"), env.MountPath("library")} {
				cmd := exec.Command("ls", "-la", dir)
				out, err := cmd.CombinedOutput()
				require.NoError(t, err, "expected ls -la %s to succeed: out=%s", dir, string(out))
				require.NotContains(t, string(out), "?????????")
				require.NotContains(t, strings.ToLower(string(out)), "cannot access")
			}
		})
	})
}

// TestFUSE_openFileMetadata verifies descriptor-based metadata survives unlink.
func TestFUSE_openFileMetadata(t *testing.T) {
	withMountedFS(t, IntegrationConfig{}, func(env *MountedFS) {
		for _, origin := range []string{"created through mount", "opened from storage"} {
			t.Run("should stat an unlinked file "+origin, func(t *testing.T) {
				const rel = "open-file"
				path := env.MountPath(rel)
				var f *os.File
				var err error
				if origin == "created through mount" {
					f, err = os.Create(path)
					require.NoError(t, err)
					_, err = f.WriteString("hello")
				} else {
					env.MustCreateFileInStoragePath(t, []byte("hello"), "ssd1", rel)
					f, err = os.Open(path)
				}
				require.NoError(t, err)
				t.Cleanup(func() { require.NoError(t, f.Close()) })
				env.MustRemoveFileInMountPoint(t, rel)
				env.MustWriteFileInMountPoint(t, rel, []byte("replacement"))

				var st unix.Statx_t
				require.NoError(t, unix.Statx(int(f.Fd()), "", unix.AT_EMPTY_PATH|unix.AT_STATX_FORCE_SYNC, unix.STATX_BASIC_STATS|unix.STATX_BTIME, &st))
				require.EqualValues(t, 5, st.Size)
				info, err := f.Stat()
				require.NoError(t, err)
				require.EqualValues(t, 5, info.Size())
				env.MustRemoveFileInMountPoint(t, rel)
			})
		}
	})
}

// TestFUSE_statxDuringPlacementChange keeps the secondary copy visible throughout.
func TestFUSE_statxDuringPlacementChange(t *testing.T) {
	withMountedFS(t, IntegrationConfig{ReadTargets: []string{"ssd1", "ssd2"}}, func(env *MountedFS) {
		t.Run("should stat a visible file while the primary copy moves", func(t *testing.T) {
			const rel = "moving"
			env.MustCreateFileInStoragePath(t, []byte("copy"), "ssd1", rel)
			env.MustCreateFileInStoragePath(t, []byte("copy"), "ssd2", rel)
			primary := env.StoragePath("ssd1", rel)
			stop := make(chan struct{})
			done := make(chan error, 1)
			go func() {
				for {
					select {
					case <-stop:
						done <- nil
						return
					default:
					}
					if err := os.Rename(primary, primary+".parked"); err != nil {
						done <- err
						return
					}
					if err := os.Rename(primary+".parked", primary); err != nil {
						done <- err
						return
					}
				}
			}()
			defer func() {
				close(stop)
				require.NoError(t, <-done)
			}()
			for range 256 {
				var st unix.Statx_t
				require.NoError(t, unix.Statx(unix.AT_FDCWD, env.MountPath(rel), unix.AT_STATX_FORCE_SYNC, unix.STATX_BASIC_STATS|unix.STATX_BTIME, &st))
				require.EqualValues(t, 4, st.Size)
			}
		})
	})
}
