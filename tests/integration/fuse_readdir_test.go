//go:build integration

package integration

import (
	"fmt"
	"os"
	"os/exec"
	"strings"
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

// =============================================================================
// Readdir Edge Cases
// =============================================================================

// TestFUSE_Readdir_EmptyDirectory verifies empty directories return no entries.
func TestFUSE_Readdir_EmptyDirectory(t *testing.T) {
	withMountedFS(t, IntegrationConfig{}, func(env *MountedFS) {
		// Setup: create an empty directory.
		rel := "readdir-edge/empty"
		env.MustMkdirInMountPoint(t, rel)

		// Action: list the empty directory.
		entries := env.MustReadDirInMountPoint(t, rel)

		// Verify: no entries (os.ReadDir filters . and ..).
		require.Empty(t, entries)
	})
}

// TestFUSE_Readdir_LsDotEntries verifies `ls -al` shows `.` and `..` on PolicyFS.
func TestFUSE_Readdir_LsDotEntries(t *testing.T) {
	withMountedFS(t, IntegrationConfig{}, func(env *MountedFS) {
		// Setup: create an empty directory.
		rel := "readdir-edge/ls-dot"
		env.MustMkdirInMountPoint(t, rel)

		// Action: run `ls -al` through the mount.
		p := env.MountPath(rel)
		cmd := exec.Command("ls", "-al", p)
		out, err := cmd.CombinedOutput()
		require.NoError(t, err, "expected ls -al to succeed: path=%s out=%s", p, string(out))

		// Verify: both dot entries appear.
		lines := strings.Split(strings.TrimSpace(string(out)), "\n")
		names := map[string]struct{}{}
		for _, line := range lines {
			fields := strings.Fields(line)
			if len(fields) == 0 {
				continue
			}
			name := fields[len(fields)-1]
			names[name] = struct{}{}
		}
		require.Contains(t, names, ".")
		require.Contains(t, names, "..")
	})
}

// TestFUSE_Readdir_LargeDirectory verifies directories with many entries work.
func TestFUSE_Readdir_LargeDirectory(t *testing.T) {
	if testing.Short() {
		t.Skip("skipping large directory test in short mode")
	}

	withMountedFS(t, IntegrationConfig{}, func(env *MountedFS) {
		// Setup: create a directory with many files.
		const numFiles = 200
		rel := "readdir-edge/large"
		env.MustMkdirInMountPoint(t, rel)

		for i := 0; i < numFiles; i++ {
			fileName := rel + "/" + fmt.Sprintf("file%04d.txt", i)
			env.MustWriteFileInMountPoint(t, fileName, []byte("x"))
		}

		// Action: list the large directory.
		entries := env.MustReadDirInMountPoint(t, rel)

		// Verify: all files are present.
		require.Len(t, entries, numFiles)
	})
}

// TestFUSE_Readdir_HiddenFiles verifies dot-prefixed (hidden) files appear in listings.
func TestFUSE_Readdir_HiddenFiles(t *testing.T) {
	withMountedFS(t, IntegrationConfig{}, func(env *MountedFS) {
		// Setup: create hidden and non-hidden files.
		rel := "readdir-edge/hidden"
		env.MustWriteFileInMountPoint(t, rel+"/visible.txt", []byte("x"))
		env.MustWriteFileInMountPoint(t, rel+"/.hidden", []byte("x"))
		env.MustMkdirInMountPoint(t, rel+"/.hiddendir")

		// Action: list the directory.
		entries := env.MustReadDirInMountPoint(t, rel)

		// Verify: all entries appear including hidden ones.
		names := make(map[string]struct{})
		for _, e := range entries {
			names[e.Name()] = struct{}{}
		}

		require.Contains(t, names, "visible.txt")
		require.Contains(t, names, ".hidden")
		require.Contains(t, names, ".hiddendir")
	})
}

// TestFUSE_Readdir_SymlinksInListing verifies symlinks appear correctly in directory listings.
func TestFUSE_Readdir_SymlinksInListing(t *testing.T) {
	withMountedFS(t, IntegrationConfig{}, func(env *MountedFS) {
		// Setup: create a file and a symlink to it.
		rel := "readdir-edge/symlinks"
		env.MustWriteFileInMountPoint(t, rel+"/target.txt", []byte("content"))
		env.MustSymlinkInMountPoint(t, "target.txt", rel+"/link.txt")

		// Action: list the directory.
		entries := env.MustReadDirInMountPoint(t, rel)

		// Verify: both entries appear.
		entryMap := make(map[string]os.DirEntry)
		for _, e := range entries {
			entryMap[e.Name()] = e
		}

		require.Contains(t, entryMap, "target.txt")
		require.Contains(t, entryMap, "link.txt")

		// Verify: don't rely on readdir d_type; confirm via Lstat through the mount.
		linkInfo := env.MustLstatInMountPoint(t, rel+"/link.txt")
		require.True(t, linkInfo.Mode()&os.ModeSymlink != 0)
	})
}

// TestFUSE_Readdir_MixedTypes verifies directories with mixed entry types work.
func TestFUSE_Readdir_MixedTypes(t *testing.T) {
	withMountedFS(t, IntegrationConfig{}, func(env *MountedFS) {
		// Setup: create various entry types.
		rel := "readdir-edge/mixed"
		env.MustWriteFileInMountPoint(t, rel+"/file.txt", []byte("file content"))
		env.MustMkdirInMountPoint(t, rel+"/subdir")
		env.MustSymlinkInMountPoint(t, "file.txt", rel+"/link.txt")

		// Action: list the directory.
		entries := env.MustReadDirInMountPoint(t, rel)

		// Verify: all entries appear with correct types.
		entryMap := make(map[string]os.DirEntry)
		for _, e := range entries {
			entryMap[e.Name()] = e
		}

		require.Len(t, entryMap, 3)

		// File: confirm type via Lstat to avoid depending on readdir d_type.
		fileInfo := env.MustLstatInMountPoint(t, rel+"/file.txt")
		require.True(t, fileInfo.Mode().IsRegular())

		// Directory
		dirInfo := env.MustLstatInMountPoint(t, rel+"/subdir")
		require.True(t, dirInfo.IsDir())

		// Symlink
		linkInfo := env.MustLstatInMountPoint(t, rel+"/link.txt")
		require.True(t, linkInfo.Mode()&os.ModeSymlink != 0)
	})
}
