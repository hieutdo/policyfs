//go:build integration

package integration

import (
	"os"
	"path/filepath"
	"testing"

	"github.com/hieutdo/policyfs/internal/config"
	"github.com/stretchr/testify/require"
)

// TestFUSE_ReadFile_smoke verifies basic reads are served correctly through PolicyFS.
func TestFUSE_ReadFile_smoke(t *testing.T) {
	withMountedFS(t, IntegrationConfig{}, func(env *MountedFS) {
		want := []byte("hello from fuse read test")
		env.MustCreateFile(t, want, "ssd1", "fuse-ops", "read", "hello.txt")

		got, err := os.ReadFile(filepath.Join(env.MountPoint, "fuse-ops", "read", "hello.txt"))
		require.NoError(t, err)
		require.Equal(t, want, got)
	})
}

// TestFUSE_Readdir_MatchesAllRules verifies READDIR unions targets from all rules that can match descendants.
func TestFUSE_Readdir_MatchesAllRules(t *testing.T) {
	withMountedFS(t, IntegrationConfig{
		RoutingRules: []config.RoutingRule{
			{Match: "library/movies/**", WriteTargets: []string{"ssd1"}, ReadTargets: []string{"ssd1"}},
			{Match: "library/music/**", WriteTargets: []string{"ssd2"}, ReadTargets: []string{"ssd2"}},
			{Match: "**", WriteTargets: []string{"ssd2"}, ReadTargets: []string{"ssd2"}},
		},
	}, func(env *MountedFS) {
		env.MustCreateFile(t, []byte("a"), "ssd1", "library", "movies", "a.txt")
		env.MustCreateFile(t, []byte("b"), "ssd2", "library", "music", "b.txt")

		entries, err := os.ReadDir(filepath.Join(env.MountPoint, "library"))
		require.NoError(t, err)

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
		env.MustCreateDir(t, "ssd1", "fuse-ops", "readdir", "a")
		env.MustCreateFile(t, []byte("x"), "ssd1", "fuse-ops", "readdir", "b.txt")

		entries, err := os.ReadDir(filepath.Join(env.MountPoint, "fuse-ops", "readdir"))
		require.NoError(t, err)

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

// TestFUSE_Read_PrefersFirstReadTarget verifies reads prefer the first matching read target.
func TestFUSE_Read_PrefersFirstReadTarget(t *testing.T) {
	withMountedFS(t, IntegrationConfig{}, func(env *MountedFS) {
		path := filepath.Join("fuse-m2", "read-pref", "hello.txt")
		env.MustCreateDir(t, "ssd1", "fuse-m2", "read-pref")
		env.MustCreateDir(t, "ssd2", "fuse-m2", "read-pref")

		env.MustCreateFile(t, []byte("from ssd1"), "ssd1", "fuse-m2", "read-pref", "hello.txt")
		env.MustCreateFile(t, []byte("from ssd2"), "ssd2", "fuse-m2", "read-pref", "hello.txt")

		got, err := os.ReadFile(filepath.Join(env.MountPoint, path))
		require.NoError(t, err)
		require.Equal(t, []byte("from ssd2"), got)
	})
}

// TestFUSE_Readdir_MergesAndDedupes verifies READDIR unions entries across targets and dedupes by name.
func TestFUSE_Readdir_MergesAndDedupes(t *testing.T) {
	withMountedFS(t, IntegrationConfig{}, func(env *MountedFS) {
		path := filepath.Join("fuse-m2", "readdir")

		env.MustCreateDir(t, "ssd2", "fuse-m2", "readdir")
		env.MustCreateFile(t, []byte("a"), "ssd2", "fuse-m2", "readdir", "a.txt")
		env.MustCreateFile(t, []byte("file"), "ssd2", "fuse-m2", "readdir", "dup")

		env.MustCreateDir(t, "ssd1", "fuse-m2", "readdir", "dup")
		env.MustCreateFile(t, []byte("b"), "ssd1", "fuse-m2", "readdir", "b.txt")

		entries, err := os.ReadDir(filepath.Join(env.MountPoint, path))
		require.NoError(t, err)

		got := map[string]os.DirEntry{}
		for _, e := range entries {
			got[e.Name()] = e
		}

		_, okA := got["a.txt"]
		if !okA {
			names := make([]string, 0, len(got))
			for n := range got {
				names = append(names, n)
			}
			t.Fatalf("missing a.txt in readdir: got=%v", names)
		}
		_, okB := got["b.txt"]
		if !okB {
			names := make([]string, 0, len(got))
			for n := range got {
				names = append(names, n)
			}
			t.Fatalf("missing b.txt in readdir: got=%v", names)
		}

		dup, okDup := got["dup"]
		require.True(t, okDup)
		info, err := dup.Info()
		require.NoError(t, err)
		require.False(t, info.IsDir())
	})
}
