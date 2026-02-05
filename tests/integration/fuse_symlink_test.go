//go:build integration

package integration

import (
	"errors"
	"os"
	"syscall"
	"testing"

	"github.com/stretchr/testify/require"
)

// =============================================================================
// Basic Symlink Operations
// =============================================================================

// TestFUSE_Symlink_Create verifies creating a symlink through the mount.
func TestFUSE_Symlink_Create(t *testing.T) {
	withMountedFS(t, IntegrationConfig{}, func(env *MountedFS) {
		// Setup: create a target file.
		targetRel := "symlink/create/target.txt"
		linkRel := "symlink/create/link.txt"
		env.MustWriteFileInMountPoint(t, targetRel, []byte("target content"))

		// Action: create a symlink through the mount.
		env.MustSymlinkInMountPoint(t, "target.txt", linkRel)

		// Verify: symlink exists and points to the target.
		fi := env.MustLstatInMountPoint(t, linkRel)
		require.True(t, fi.Mode()&os.ModeSymlink != 0, "expected symlink mode")

		target := env.MustReadlinkInMountPoint(t, linkRel)
		require.Equal(t, "target.txt", target)
	})
}

// TestFUSE_Symlink_Readlink verifies reading a symlink target.
func TestFUSE_Symlink_Readlink(t *testing.T) {
	withMountedFS(t, IntegrationConfig{}, func(env *MountedFS) {
		// Setup: create a symlink on storage.
		targetRel := "symlink/readlink/target.txt"
		linkRel := "symlink/readlink/link.txt"
		env.MustCreateFileInStoragePath(t, []byte("content"), "ssd1", targetRel)
		require.NoError(t, os.Symlink("target.txt", env.StoragePath("ssd1", linkRel)))

		// Action: read the symlink target through the mount.
		target := env.MustReadlinkInMountPoint(t, linkRel)

		// Verify: target matches.
		require.Equal(t, "target.txt", target)
	})
}

// TestFUSE_Symlink_Follow verifies reading through a symlink returns target content.
func TestFUSE_Symlink_Follow(t *testing.T) {
	withMountedFS(t, IntegrationConfig{}, func(env *MountedFS) {
		// Setup: create target file and symlink.
		targetRel := "symlink/follow/target.txt"
		linkRel := "symlink/follow/link.txt"
		want := []byte("target content through symlink")
		env.MustWriteFileInMountPoint(t, targetRel, want)
		env.MustSymlinkInMountPoint(t, "target.txt", linkRel)

		// Action: read through the symlink.
		got := env.MustReadFileInMountPoint(t, linkRel)

		// Verify: content matches the target file.
		require.Equal(t, want, got)
	})
}

// TestFUSE_Symlink_BrokenLink verifies accessing a broken symlink returns ENOENT.
func TestFUSE_Symlink_BrokenLink(t *testing.T) {
	withMountedFS(t, IntegrationConfig{}, func(env *MountedFS) {
		// Setup: create a symlink to a non-existent target.
		linkRel := "symlink/broken/link.txt"
		env.MustSymlinkInMountPoint(t, "nonexistent.txt", linkRel)

		// Verify: symlink itself exists (lstat succeeds).
		fi := env.MustLstatInMountPoint(t, linkRel)
		require.True(t, fi.Mode()&os.ModeSymlink != 0)

		// Action: try to read through the broken symlink.
		_, err := env.ReadFileInMountPoint(linkRel)

		// Verify: error is ENOENT.
		require.Error(t, err)
		require.True(t, errors.Is(err, syscall.ENOENT), "expected ENOENT, got %v", err)
	})
}

// TestFUSE_Symlink_RelativeTarget verifies symlinks with relative paths work.
func TestFUSE_Symlink_RelativeTarget(t *testing.T) {
	withMountedFS(t, IntegrationConfig{}, func(env *MountedFS) {
		// Setup: create nested directory structure.
		env.MustWriteFileInMountPoint(t, "symlink/relative/a/target.txt", []byte("nested target"))

		// Create symlink in different directory pointing to ../a/target.txt
		env.MustMkdirInMountPoint(t, "symlink/relative/b")
		env.MustSymlinkInMountPoint(t, "../a/target.txt", "symlink/relative/b/link.txt")

		// Action: read through the symlink.
		got := env.MustReadFileInMountPoint(t, "symlink/relative/b/link.txt")

		// Verify: content matches.
		require.Equal(t, []byte("nested target"), got)
	})
}

// =============================================================================
// Symlink Edge Cases
// =============================================================================

// TestFUSE_Symlink_ToDirectory verifies symlinks to directories allow traversal.
func TestFUSE_Symlink_ToDirectory(t *testing.T) {
	withMountedFS(t, IntegrationConfig{}, func(env *MountedFS) {
		// Setup: create a directory with files.
		env.MustWriteFileInMountPoint(t, "symlink/dir/real/file1.txt", []byte("file1"))
		env.MustWriteFileInMountPoint(t, "symlink/dir/real/file2.txt", []byte("file2"))

		// Create symlink to the directory.
		env.MustSymlinkInMountPoint(t, "real", "symlink/dir/link")

		// Action: list directory through symlink.
		entries := env.MustReadDirInMountPoint(t, "symlink/dir/link")

		// Verify: can see files through the symlink.
		names := make([]string, len(entries))
		for i, e := range entries {
			names[i] = e.Name()
		}
		require.Contains(t, names, "file1.txt")
		require.Contains(t, names, "file2.txt")

		// Also verify reading a file through the dir symlink.
		got := env.MustReadFileInMountPoint(t, "symlink/dir/link/file1.txt")
		require.Equal(t, []byte("file1"), got)
	})
}

// TestFUSE_Symlink_Chain verifies symlink chains work (symlink -> symlink -> file).
func TestFUSE_Symlink_Chain(t *testing.T) {
	withMountedFS(t, IntegrationConfig{}, func(env *MountedFS) {
		// Setup: create file and chain of symlinks.
		want := []byte("content through chain")
		env.MustWriteFileInMountPoint(t, "symlink/chain/file.txt", want)
		env.MustSymlinkInMountPoint(t, "file.txt", "symlink/chain/link1.txt")
		env.MustSymlinkInMountPoint(t, "link1.txt", "symlink/chain/link2.txt")

		// Action: read through the chain.
		got := env.MustReadFileInMountPoint(t, "symlink/chain/link2.txt")

		// Verify: content matches.
		require.Equal(t, want, got)
	})
}

// TestFUSE_Symlink_CircularDetection verifies circular symlinks return ELOOP.
func TestFUSE_Symlink_CircularDetection(t *testing.T) {
	withMountedFS(t, IntegrationConfig{}, func(env *MountedFS) {
		// Setup: create circular symlinks.
		env.MustMkdirInMountPoint(t, "symlink/circular")
		env.MustSymlinkInMountPoint(t, "b.txt", "symlink/circular/a.txt")
		env.MustSymlinkInMountPoint(t, "a.txt", "symlink/circular/b.txt")

		// Action: try to read through the circular symlink.
		_, err := env.ReadFileInMountPoint("symlink/circular/a.txt")

		// Verify: error is ELOOP.
		require.Error(t, err)
		require.True(t, errors.Is(err, syscall.ELOOP), "expected ELOOP, got %v", err)
	})
}

// TestFUSE_Symlink_AbsoluteTarget verifies symlinks with absolute paths work.
func TestFUSE_Symlink_AbsoluteTarget(t *testing.T) {
	withMountedFS(t, IntegrationConfig{}, func(env *MountedFS) {
		// Setup: create target file and get absolute path.
		targetRel := "symlink/absolute/target.txt"
		want := []byte("absolute target content")
		env.MustWriteFileInMountPoint(t, targetRel, want)
		absTarget := env.MountPath(targetRel)

		// Create symlink with absolute target.
		linkRel := "symlink/absolute/link.txt"
		env.MustSymlinkInMountPoint(t, absTarget, linkRel)

		// Action: read through the symlink.
		got := env.MustReadFileInMountPoint(t, linkRel)

		// Verify: content matches.
		require.Equal(t, want, got)
	})
}

// TestFUSE_Symlink_Unlink verifies removing a symlink doesn't affect the target.
func TestFUSE_Symlink_Unlink(t *testing.T) {
	withMountedFS(t, IntegrationConfig{}, func(env *MountedFS) {
		// Setup: create target and symlink.
		targetRel := "symlink/unlink/target.txt"
		linkRel := "symlink/unlink/link.txt"
		want := []byte("target should survive")
		env.MustWriteFileInMountPoint(t, targetRel, want)
		env.MustSymlinkInMountPoint(t, "target.txt", linkRel)

		// Action: remove the symlink.
		env.MustRemoveFileInMountPoint(t, linkRel)

		// Verify: symlink is gone but target remains.
		require.False(t, env.FileExistsInMountPoint(linkRel))
		require.True(t, env.FileExistsInMountPoint(targetRel))
		got := env.MustReadFileInMountPoint(t, targetRel)
		require.Equal(t, want, got)
	})
}
