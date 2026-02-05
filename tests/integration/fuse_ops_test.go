//go:build integration

package integration

import (
	"errors"
	"os"
	"syscall"
	"testing"
	"time"

	"github.com/hieutdo/policyfs/internal/config"
	"github.com/stretchr/testify/require"
)

// TestFUSE_ReadFile_smoke verifies basic reads are served correctly through PolicyFS.
func TestFUSE_ReadFile_smoke(t *testing.T) {
	withMountedFS(t, IntegrationConfig{}, func(env *MountedFS) {
		want := []byte("hello from fuse read test")
		env.MustCreateFileInStoragePath(t, want, "ssd1", "fuse-ops/read/hello.txt")

		got := env.MustReadFileInMountPoint(t, "fuse-ops/read/hello.txt")
		require.Equal(t, want, got)
	})
}

// TestFUSE_Create_createsFileOnSelectedWriteTarget verifies CREATE selects the write target and writes content there.
func TestFUSE_Create_createsFileOnSelectedWriteTarget(t *testing.T) {
	withMountedFS(t, IntegrationConfig{
		RoutingRules: []config.RoutingRule{
			{Match: "fuse-mutate/create/**", WriteTargets: []string{"ssd1"}, ReadTargets: []string{"ssd1"}},
			{Match: "**", WriteTargets: []string{"ssd2"}, ReadTargets: []string{"ssd2"}},
		},
	}, func(env *MountedFS) {
		// Create file matching rule (ssd1)
		rel1 := "fuse-mutate/create/hello.txt"
		want := []byte("created")
		env.MustWriteFileInMountPoint(t, rel1, want)

		got := env.MustReadFileInStoragePath(t, "ssd1", rel1)
		require.Equal(t, want, got)
		require.NoFileExists(t, env.StoragePath("ssd2", rel1))

		// Create file not matching rule (ssd2)
		rel2 := "create/hello2.txt"
		want2 := []byte("created2")
		env.MustWriteFileInMountPoint(t, rel2, want2)

		got2 := env.MustReadFileInStoragePath(t, "ssd2", rel2)
		require.Equal(t, want2, got2)
		require.NoFileExists(t, env.StoragePath("ssd1", rel2))
	})
}

// TestFUSE_Mkdir_createsDirOnSelectedWriteTarget verifies MKDIR creates the directory on the selected write target.
func TestFUSE_Mkdir_createsDirOnSelectedWriteTarget(t *testing.T) {
	withMountedFS(t, IntegrationConfig{
		RoutingRules: []config.RoutingRule{
			{Match: "fuse-mutate/mkdir/**", WriteTargets: []string{"ssd2"}, ReadTargets: []string{"ssd2"}},
			{Match: "**", WriteTargets: []string{"ssd1"}, ReadTargets: []string{"ssd1"}},
		},
	}, func(env *MountedFS) {
		rel := "fuse-mutate/mkdir/a"
		env.MustMkdirInMountPoint(t, rel)

		// Verify directory was created on ssd2
		require.DirExists(t, env.StoragePath("ssd2", rel))

		// Verify directory was not created on ssd1
		require.NoDirExists(t, env.StoragePath("ssd1", rel))
	})
}

// TestFUSE_Unlink_removesFirstExistingReadTarget verifies UNLINK deletes from the first existing read target.
func TestFUSE_Unlink_removesFirstExistingReadTarget(t *testing.T) {
	withMountedFS(t, IntegrationConfig{
		ReadTargets: []string{"ssd2", "ssd1"},
		Targets:     []string{"ssd2", "ssd1"},
	}, func(env *MountedFS) {
		rel := "fuse-mutate/unlink/x.txt"
		env.MustCreateFileInStoragePath(t, []byte("from ssd1"), "ssd1", rel)
		env.MustCreateFileInStoragePath(t, []byte("from ssd2"), "ssd2", rel)

		env.MustRemoveFileInMountPoint(t, rel)
		require.NoFileExists(t, env.StoragePath("ssd2", rel))
		require.FileExists(t, env.StoragePath("ssd1", rel))
	})
}

// TestFUSE_Rmdir_removesFirstExistingReadTarget verifies RMDIR deletes from the first existing read target.
func TestFUSE_Rmdir_removesFirstExistingReadTarget(t *testing.T) {
	withMountedFS(t, IntegrationConfig{
		ReadTargets: []string{"ssd2", "ssd1"},
		Targets:     []string{"ssd2", "ssd1"},
	}, func(env *MountedFS) {
		rel := "fuse-mutate/rmdir/d"
		env.MustCreateDirInStoragePath(t, "ssd1", rel)
		env.MustCreateDirInStoragePath(t, "ssd2", rel)

		env.MustRemoveFileInMountPoint(t, rel)
		require.NoDirExists(t, env.StoragePath("ssd2", rel))
		require.DirExists(t, env.StoragePath("ssd1", rel))
	})
}

// TestFUSE_Rename_sameTarget verifies rename works when both old and new path resolve to the same target.
func TestFUSE_Rename_sameTarget(t *testing.T) {
	withMountedFS(t, IntegrationConfig{
		RoutingRules: []config.RoutingRule{
			{Match: "fuse-mutate/rename/**", WriteTargets: []string{"ssd1"}, ReadTargets: []string{"ssd1"}},
			{Match: "**", WriteTargets: []string{"ssd2"}, ReadTargets: []string{"ssd2"}},
		},
	}, func(env *MountedFS) {
		oldRel := "fuse-mutate/rename/old.txt"
		newRel := "fuse-mutate/rename/new.txt"

		env.MustWriteFileInMountPoint(t, oldRel, []byte("x"))
		env.MustRenameFileInMountPoint(t, oldRel, newRel)

		require.NoFileExists(t, env.StoragePath("ssd1", oldRel))
		require.NoFileExists(t, env.StoragePath("ssd2", oldRel))
		require.NoFileExists(t, env.StoragePath("ssd2", newRel))

		b := env.MustReadFileInStoragePath(t, "ssd1", newRel)
		require.Equal(t, []byte("x"), b)
	})
}

// TestFUSE_Rename_crossTarget_returnsEXDEV verifies cross-target rename returns EXDEV.
func TestFUSE_Rename_crossTarget_returnsEXDEV(t *testing.T) {
	withMountedFS(t, IntegrationConfig{
		RoutingRules: []config.RoutingRule{
			{Match: "fuse-mutate/rename-src/**", WriteTargets: []string{"ssd1"}, ReadTargets: []string{"ssd1"}},
			{Match: "fuse-mutate/rename-dst/**", WriteTargets: []string{"ssd2"}, ReadTargets: []string{"ssd2"}},
			{Match: "**", WriteTargets: []string{"ssd1"}, ReadTargets: []string{"ssd1"}},
		},
	}, func(env *MountedFS) {
		oldRel := "fuse-mutate/rename-src/a.txt"
		newRel := "fuse-mutate/rename-dst/b.txt"
		env.MustMkdirInMountPoint(t, "fuse-mutate/rename-src")
		env.MustMkdirInMountPoint(t, "fuse-mutate/rename-dst")
		env.MustWriteFileInMountPoint(t, oldRel, []byte("x"))

		err := env.RenameFileInMountPoint(oldRel, newRel)
		require.Error(t, err)
		require.True(t, errors.Is(err, syscall.EXDEV))

		require.NoFileExists(t, env.StoragePath("ssd2", newRel))
		require.NoFileExists(t, env.StoragePath("ssd1", newRel))

		b := env.MustReadFileInStoragePath(t, "ssd1", oldRel)
		require.Equal(t, []byte("x"), b)
	})
}

// TestFUSE_Setattr_truncateChmodUtimens verifies truncate, chmod, and utimens go through.
func TestFUSE_Setattr_truncateChmodUtimens(t *testing.T) {
	withMountedFS(t, IntegrationConfig{
		RoutingRules: []config.RoutingRule{
			{Match: "fuse-mutate/setattr/**", WriteTargets: []string{"ssd1"}, ReadTargets: []string{"ssd1"}},
			{Match: "**", WriteTargets: []string{"ssd2"}, ReadTargets: []string{"ssd2"}},
		},
	}, func(env *MountedFS) {
		rel := "fuse-mutate/setattr/f.txt"
		want := []byte("0123456789")
		env.MustWriteFileInMountPoint(t, rel, want)

		err := os.Chmod(env.MountPath(rel), 0o600)
		require.NoError(t, err)

		err = os.Truncate(env.MountPath(rel), 3)
		require.NoError(t, err)

		at := time.Unix(1700000000, 0)
		mt := time.Unix(1700000001, 0)
		err = os.Chtimes(env.MountPath(rel), at, mt)
		require.NoError(t, err)
		require.FileExists(t, env.StoragePath("ssd1", rel))
		require.NoFileExists(t, env.StoragePath("ssd2", rel))
		st, err := os.Stat(env.StoragePath("ssd1", rel))
		require.NoError(t, err)
		require.Equal(t, int64(3), st.Size())
		require.Equal(t, os.FileMode(0o600), st.Mode().Perm())
		require.WithinDuration(t, mt, st.ModTime(), 2*time.Second)
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
		env.MustCreateFileInStoragePath(t, []byte("a"), "ssd1", "library/movies/a.txt")
		env.MustCreateFileInStoragePath(t, []byte("b"), "ssd2", "library/music/b.txt")

		entries := env.MustReadDirInMountPoint(t, "library")

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
		env.MustCreateDirInStoragePath(t, "ssd1", "fuse-ops/readdir/a")
		env.MustCreateFileInStoragePath(t, []byte("x"), "ssd1", "fuse-ops/readdir/b.txt")

		entries := env.MustReadDirInMountPoint(t, "fuse-ops/readdir")

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
		path := "fuse-m2/read-pref/hello.txt"
		env.MustCreateDirInStoragePath(t, "ssd1", "fuse-m2/read-pref")
		env.MustCreateDirInStoragePath(t, "ssd2", "fuse-m2/read-pref")

		env.MustCreateFileInStoragePath(t, []byte("from ssd1"), "ssd1", path)
		env.MustCreateFileInStoragePath(t, []byte("from ssd2"), "ssd2", path)

		got := env.MustReadFileInMountPoint(t, path)
		require.Equal(t, []byte("from ssd2"), got)
	})
}

// TestFUSE_Readdir_MergesAndDedupes verifies READDIR unions entries across targets and dedupes by name.
func TestFUSE_Readdir_MergesAndDedupes(t *testing.T) {
	withMountedFS(t, IntegrationConfig{}, func(env *MountedFS) {
		path := "fuse-m2/readdir"

		env.MustCreateDirInStoragePath(t, "ssd2", path)
		env.MustCreateFileInStoragePath(t, []byte("a"), "ssd2", "fuse-m2/readdir/a.txt")
		env.MustCreateFileInStoragePath(t, []byte("file"), "ssd2", "fuse-m2/readdir/dup")

		env.MustCreateDirInStoragePath(t, "ssd1", "fuse-m2/readdir/dup")
		env.MustCreateFileInStoragePath(t, []byte("b"), "ssd1", "fuse-m2/readdir/b.txt")

		entries := env.MustReadDirInMountPoint(t, path)

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
