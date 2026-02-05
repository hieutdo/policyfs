//go:build integration

package integration

import (
	"os"
	"testing"

	"github.com/hieutdo/policyfs/internal/config"
	"github.com/stretchr/testify/require"
)

// TestFUSE_Readdir_MatchesAllRules verifies READDIR unions targets from all rules that can match descendants.
func TestFUSE_Readdir_MatchesAllRules(t *testing.T) {
	withMountedFS(t, IntegrationConfig{
		RoutingRules: []config.RoutingRule{
			{Match: "library/movies/**", WriteTargets: []string{"ssd1"}, ReadTargets: []string{"ssd1"}},
			{Match: "library/music/**", WriteTargets: []string{"ssd2"}, ReadTargets: []string{"ssd2"}},
			{Match: "**", WriteTargets: []string{"ssd2"}, ReadTargets: []string{"ssd2"}},
		},
	}, func(env *MountedFS) {
		// Setup: create files in different subtrees.
		env.MustCreateFileInStoragePath(t, []byte("a"), "ssd1", "library/movies/a.txt")
		env.MustCreateFileInStoragePath(t, []byte("b"), "ssd2", "library/music/b.txt")

		// Action: list the parent directory.
		entries := env.MustReadDirInMountPoint(t, "library")

		// Verify: both subdirectories appear.
		names := map[string]struct{}{}
		for _, e := range entries {
			names[e.Name()] = struct{}{}
		}
		_, okMovies := names["movies"]
		require.True(t, okMovies)
		_, okMusic := names["music"]
		require.True(t, okMusic)
	})
}

// TestFUSE_Readdir_smoke verifies directory listings work through PolicyFS.
func TestFUSE_Readdir_smoke(t *testing.T) {
	withMountedFS(t, IntegrationConfig{}, func(env *MountedFS) {
		// Setup: create directory tree on storage.
		env.MustCreateDirInStoragePath(t, "ssd1", "fuse-ops/readdir/a")
		env.MustCreateFileInStoragePath(t, []byte("x"), "ssd1", "fuse-ops/readdir/b.txt")

		// Action: list directory through the mount.
		entries := env.MustReadDirInMountPoint(t, "fuse-ops/readdir")

		// Verify: both entries appear.
		names := map[string]struct{}{}
		for _, e := range entries {
			names[e.Name()] = struct{}{}
		}
		_, okA := names["a"]
		require.True(t, okA)
		_, okB := names["b.txt"]
		require.True(t, okB)
	})
}

// TestFUSE_Readdir_MergesAndDedupes verifies READDIR unions entries across targets and dedupes by name.
func TestFUSE_Readdir_MergesAndDedupes(t *testing.T) {
	withMountedFS(t, IntegrationConfig{}, func(env *MountedFS) {
		// Setup: create entries on both targets with a name collision.
		path := "fuse-m2/readdir"

		env.MustCreateDirInStoragePath(t, "ssd2", path)
		env.MustCreateFileInStoragePath(t, []byte("a"), "ssd2", "fuse-m2/readdir/a.txt")
		env.MustCreateFileInStoragePath(t, []byte("file"), "ssd2", "fuse-m2/readdir/dup")

		env.MustCreateDirInStoragePath(t, "ssd1", "fuse-m2/readdir/dup")
		env.MustCreateFileInStoragePath(t, []byte("b"), "ssd1", "fuse-m2/readdir/b.txt")

		// Action: list directory through the mount.
		entries := env.MustReadDirInMountPoint(t, path)

		// Verify: both unique names appear; the collision resolves to the first target entry.
		got := map[string]os.DirEntry{}
		for _, e := range entries {
			got[e.Name()] = e
		}

		_, okA := got["a.txt"]
		_, okB := got["b.txt"]
		if !okA || !okB {
			names := make([]string, 0, len(got))
			for n := range got {
				names = append(names, n)
			}
			require.True(t, okA, "expected a.txt in readdir: got=%v", names)
			require.True(t, okB, "expected b.txt in readdir: got=%v", names)
		}

		dup, okDup := got["dup"]
		require.True(t, okDup)
		info, err := dup.Info()
		require.NoError(t, err)
		require.False(t, info.IsDir())
	})
}
