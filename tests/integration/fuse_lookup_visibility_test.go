//go:build integration

package integration

import (
	"os"
	"os/exec"
	"strings"
	"testing"

	"github.com/hieutdo/policyfs/internal/config"
	"github.com/stretchr/testify/require"
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
