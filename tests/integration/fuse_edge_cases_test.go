//go:build integration

package integration

import (
	"errors"
	"os"
	"path/filepath"
	"strings"
	"syscall"
	"testing"

	"github.com/stretchr/testify/require"
)

// =============================================================================
// Empty File Handling
// =============================================================================

// TestFUSE_Read_EmptyFile verifies reading a 0-byte file returns an empty slice.
func TestFUSE_Read_EmptyFile(t *testing.T) {
	withMountedFS(t, IntegrationConfig{}, func(env *MountedFS) {
		// Setup: create an empty file on storage.
		rel := "edge/empty/read.txt"
		env.MustCreateFileInStoragePath(t, []byte{}, "ssd1", rel)

		// Action: read through the mount.
		got := env.MustReadFileInMountPoint(t, rel)

		// Verify: content is empty.
		require.Empty(t, got)
		require.Len(t, got, 0)
	})
}

// TestFUSE_Write_EmptyFile verifies creating a 0-byte file through the mount.
func TestFUSE_Write_EmptyFile(t *testing.T) {
	withMountedFS(t, IntegrationConfig{}, func(env *MountedFS) {
		// Action: write an empty file through the mount.
		rel := "edge/empty/write.txt"
		env.MustWriteFileInMountPoint(t, rel, []byte{})

		// Verify: file exists on storage with 0 bytes.
		require.True(t, env.FileExistsInStoragePath("ssd1", rel))
		content := env.MustReadFileInStoragePath(t, "ssd1", rel)
		require.Empty(t, content)
	})
}

// TestFUSE_Append_ToEmptyFile verifies appending to a 0-byte file grows it.
func TestFUSE_Append_ToEmptyFile(t *testing.T) {
	withMountedFS(t, IntegrationConfig{}, func(env *MountedFS) {
		// Setup: create an empty file through the mount.
		rel := "edge/empty/append.txt"
		env.MustWriteFileInMountPoint(t, rel, []byte{})

		// Action: append content to the file.
		p := env.MountPath(rel)
		f, err := os.OpenFile(p, os.O_APPEND|os.O_WRONLY, 0o644)
		require.NoError(t, err)
		_, err = f.Write([]byte("appended content"))
		require.NoError(t, err)
		require.NoError(t, f.Close())

		// Verify: file now has content.
		got := env.MustReadFileInMountPoint(t, rel)
		require.Equal(t, []byte("appended content"), got)
	})
}

// =============================================================================
// Error Conditions
// =============================================================================

// TestFUSE_Read_ENOENT verifies reading a non-existent file returns ENOENT.
func TestFUSE_Read_ENOENT(t *testing.T) {
	withMountedFS(t, IntegrationConfig{}, func(env *MountedFS) {
		// Action: try to read a file that doesn't exist.
		_, err := env.ReadFileInMountPoint("edge/errors/nonexistent.txt")

		// Verify: error is ENOENT.
		require.Error(t, err)
		require.True(t, errors.Is(err, syscall.ENOENT), "expected ENOENT, got %v", err)
	})
}

// TestFUSE_Mkdir_EEXIST verifies mkdir on existing directory returns EEXIST.
func TestFUSE_Mkdir_EEXIST(t *testing.T) {
	withMountedFS(t, IntegrationConfig{}, func(env *MountedFS) {
		// Setup: create a directory.
		rel := "edge/errors/existing-dir"
		env.MustMkdirInMountPoint(t, rel)

		// Action: try to create the same directory again (not MkdirAll).
		p := env.MountPath(rel)
		err := os.Mkdir(p, 0o755)

		// Verify: error is EEXIST.
		require.Error(t, err)
		require.True(t, errors.Is(err, syscall.EEXIST), "expected EEXIST, got %v", err)
	})
}

// TestFUSE_Rmdir_ENOTEMPTY verifies rmdir on non-empty directory returns ENOTEMPTY.
func TestFUSE_Rmdir_ENOTEMPTY(t *testing.T) {
	withMountedFS(t, IntegrationConfig{}, func(env *MountedFS) {
		// Setup: create a directory with a file inside.
		dirRel := "edge/errors/nonempty-dir"
		fileRel := dirRel + "/file.txt"
		env.MustWriteFileInMountPoint(t, fileRel, []byte("content"))

		// Action: try to remove the directory.
		err := env.RemoveFileInMountPoint(dirRel)

		// Verify: error is ENOTEMPTY.
		require.Error(t, err)
		require.True(t, errors.Is(err, syscall.ENOTEMPTY), "expected ENOTEMPTY, got %v", err)
	})
}

// TestFUSE_Unlink_ENOENT verifies unlink on non-existent file returns ENOENT.
func TestFUSE_Unlink_ENOENT(t *testing.T) {
	withMountedFS(t, IntegrationConfig{}, func(env *MountedFS) {
		// Action: try to remove a file that doesn't exist.
		err := env.RemoveFileInMountPoint("edge/errors/nonexistent-file.txt")

		// Verify: error is ENOENT.
		require.Error(t, err)
		require.True(t, errors.Is(err, syscall.ENOENT), "expected ENOENT, got %v", err)
	})
}

// TestFUSE_Unlink_EISDIR verifies syscall.Unlink on a directory returns EISDIR.
func TestFUSE_Unlink_EISDIR(t *testing.T) {
	withMountedFS(t, IntegrationConfig{}, func(env *MountedFS) {
		// Setup: create a directory.
		rel := "edge/errors/dir-for-unlink"
		env.MustMkdirInMountPoint(t, rel)

		// Action: try to unlink the directory using raw syscall (not os.Remove).
		// Note: os.Remove will call rmdir for directories, so we use syscall.Unlink directly.
		p := env.MountPath(rel)
		err := syscall.Unlink(p)

		// Verify: error is EISDIR (or EPERM on some systems).
		require.Error(t, err)
		// Note: Some systems return EPERM instead of EISDIR for unlink on directories.
		isExpected := errors.Is(err, syscall.EISDIR) || errors.Is(err, syscall.EPERM)
		require.True(t, isExpected, "expected EISDIR or EPERM, got %v", err)
	})
}

// TestFUSE_Rmdir_ENOTDIR verifies rmdir on a file returns ENOTDIR.
func TestFUSE_Rmdir_ENOTDIR(t *testing.T) {
	withMountedFS(t, IntegrationConfig{}, func(env *MountedFS) {
		// Setup: create a file.
		rel := "edge/errors/file-for-rmdir.txt"
		env.MustWriteFileInMountPoint(t, rel, []byte("content"))

		// Action: try to rmdir the file.
		p := env.MountPath(rel)
		err := syscall.Rmdir(p)

		// Verify: error is ENOTDIR.
		require.Error(t, err)
		require.True(t, errors.Is(err, syscall.ENOTDIR), "expected ENOTDIR, got %v", err)
	})
}

// =============================================================================
// Path Edge Cases
// =============================================================================

// TestFUSE_Create_MaxFilenameLength verifies files with 255-char names work.
func TestFUSE_Create_MaxFilenameLength(t *testing.T) {
	withMountedFS(t, IntegrationConfig{}, func(env *MountedFS) {
		// Setup: create a filename with exactly 255 characters.
		maxName := strings.Repeat("a", 255)
		rel := "edge/path/" + maxName

		// Action: create the file through the mount.
		env.MustWriteFileInMountPoint(t, rel, []byte("content"))

		// Verify: file exists and can be read.
		got := env.MustReadFileInMountPoint(t, rel)
		require.Equal(t, []byte("content"), got)
	})
}

// TestFUSE_Create_TooLongFilename verifies files with 256+ char names return ENAMETOOLONG.
func TestFUSE_Create_TooLongFilename(t *testing.T) {
	withMountedFS(t, IntegrationConfig{}, func(env *MountedFS) {
		// Setup: create a filename with 256 characters.
		tooLongName := strings.Repeat("a", 256)
		rel := "edge/path/" + tooLongName

		// Action: try to create the file.
		err := env.WriteFileInMountPoint(rel, []byte("content"), 0o644)

		// Verify: error is ENAMETOOLONG.
		require.Error(t, err)
		require.True(t, errors.Is(err, syscall.ENAMETOOLONG), "expected ENAMETOOLONG, got %v", err)
	})
}

// TestFUSE_Create_UnicodeFilename verifies files with UTF-8 names work.
func TestFUSE_Create_UnicodeFilename(t *testing.T) {
	withMountedFS(t, IntegrationConfig{}, func(env *MountedFS) {
		// Test various Unicode filenames.
		testCases := []string{
			"日本語ファイル.txt",
			"файл.txt",
			"αβγδ.txt",
			"emoji🎉file.txt",
			"mixed中文english.txt",
		}

		for _, name := range testCases {
			t.Run(name, func(t *testing.T) {
				rel := "edge/unicode/" + name
				content := []byte("content for " + name)

				// Action: create and read the file.
				env.MustWriteFileInMountPoint(t, rel, content)
				got := env.MustReadFileInMountPoint(t, rel)

				// Verify: content matches.
				require.Equal(t, content, got)
			})
		}
	})
}

// TestFUSE_Create_DeepNesting verifies deeply nested directories work.
func TestFUSE_Create_DeepNesting(t *testing.T) {
	withMountedFS(t, IntegrationConfig{}, func(env *MountedFS) {
		// Setup: create a path with 50 levels of nesting.
		parts := make([]string, 50)
		for i := range parts {
			parts[i] = "level"
		}
		deepPath := filepath.Join(append([]string{"edge", "deep"}, parts...)...)
		rel := deepPath + "/file.txt"

		// Action: create the file through the mount.
		env.MustWriteFileInMountPoint(t, rel, []byte("deep content"))

		// Verify: file exists and can be read.
		got := env.MustReadFileInMountPoint(t, rel)
		require.Equal(t, []byte("deep content"), got)
	})
}

// =============================================================================
// Large File Handling
// =============================================================================

// TestFUSE_Write_LargeFile verifies writing and reading a 10MB file.
func TestFUSE_Write_LargeFile(t *testing.T) {
	if testing.Short() {
		t.Skip("skipping large file test in short mode")
	}

	withMountedFS(t, IntegrationConfig{}, func(env *MountedFS) {
		// Setup: create 10MB of data.
		size := 10 * 1024 * 1024 // 10MB
		data := make([]byte, size)
		for i := range data {
			data[i] = byte(i % 256)
		}
		rel := "edge/large/10mb.bin"

		// Action: write through the mount.
		env.MustWriteFileInMountPoint(t, rel, data)

		// Verify: read back and compare.
		got := env.MustReadFileInMountPoint(t, rel)
		require.Len(t, got, size)
		require.Equal(t, data, got)
	})
}
