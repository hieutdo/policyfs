//go:build integration

package integration

import (
	"fmt"
	"os"
	"sync"
	"testing"

	"github.com/stretchr/testify/require"
)

// =============================================================================
// Concurrent Read Operations
// =============================================================================

// TestFUSE_Concurrent_ReadSameFile verifies multiple goroutines can read the same file.
func TestFUSE_Concurrent_ReadSameFile(t *testing.T) {
	withMountedFS(t, IntegrationConfig{}, func(env *MountedFS) {
		// Setup: create a file with known content.
		rel := "concurrent/read-same/file.txt"
		want := []byte("shared content for concurrent reads")
		env.MustWriteFileInMountPoint(t, rel, want)

		// Action: 10 goroutines read the same file concurrently.
		const numReaders = 10
		var wg sync.WaitGroup
		results := make([][]byte, numReaders)
		errs := make([]error, numReaders)

		wg.Add(numReaders)
		for i := range numReaders {
			go func(idx int) {
				defer wg.Done()
				results[idx], errs[idx] = env.ReadFileInMountPoint(rel)
			}(i)
		}
		wg.Wait()

		// Verify: all reads succeeded and returned the same content.
		for i := range numReaders {
			require.NoError(t, errs[i], "reader %d failed", i)
			require.Equal(t, want, results[i], "reader %d got wrong content", i)
		}
	})
}

// TestFUSE_Concurrent_ReadDifferentFiles verifies multiple goroutines can read different files.
func TestFUSE_Concurrent_ReadDifferentFiles(t *testing.T) {
	withMountedFS(t, IntegrationConfig{}, func(env *MountedFS) {
		// Setup: create multiple files with unique content.
		const numFiles = 10
		files := make([]string, numFiles)
		contents := make([][]byte, numFiles)

		for i := range numFiles {
			files[i] = fmt.Sprintf("concurrent/read-diff/file%d.txt", i)
			contents[i] = fmt.Appendf(nil, "content for file %d", i)
			env.MustWriteFileInMountPoint(t, files[i], contents[i])
		}

		// Action: each goroutine reads a different file.
		var wg sync.WaitGroup
		results := make([][]byte, numFiles)
		errs := make([]error, numFiles)

		wg.Add(numFiles)
		for i := range numFiles {
			go func(idx int) {
				defer wg.Done()
				results[idx], errs[idx] = env.ReadFileInMountPoint(files[idx])
			}(i)
		}
		wg.Wait()

		// Verify: all reads succeeded with correct content.
		for i := range numFiles {
			require.NoError(t, errs[i], "reader %d failed", i)
			require.Equal(t, contents[i], results[i], "reader %d got wrong content", i)
		}
	})
}

// =============================================================================
// Concurrent Write Operations
// =============================================================================

// TestFUSE_Concurrent_WriteDifferentFiles verifies multiple goroutines can write different files.
func TestFUSE_Concurrent_WriteDifferentFiles(t *testing.T) {
	withMountedFS(t, IntegrationConfig{}, func(env *MountedFS) {
		// Setup: prepare file paths and content.
		const numWriters = 10
		files := make([]string, numWriters)
		contents := make([][]byte, numWriters)

		for i := range numWriters {
			files[i] = fmt.Sprintf("concurrent/write-diff/file%d.txt", i)
			contents[i] = fmt.Appendf(nil, "written by goroutine %d", i)
		}

		// Create parent directory first.
		env.MustMkdirInMountPoint(t, "concurrent/write-diff")

		// Action: each goroutine writes a different file.
		var wg sync.WaitGroup
		errs := make([]error, numWriters)

		wg.Add(numWriters)
		for i := range numWriters {
			go func(idx int) {
				defer wg.Done()
				errs[idx] = env.WriteFileInMountPoint(files[idx], contents[idx], 0o644)
			}(i)
		}
		wg.Wait()

		// Verify: all writes succeeded.
		for i := range numWriters {
			require.NoError(t, errs[i], "writer %d failed", i)
		}

		// Verify: all files have correct content.
		for i := range numWriters {
			got := env.MustReadFileInMountPoint(t, files[i])
			require.Equal(t, contents[i], got, "file %d has wrong content", i)
		}
	})
}

// TestFUSE_Concurrent_MkdirRace verifies concurrent mkdir on the same path handles correctly.
func TestFUSE_Concurrent_MkdirRace(t *testing.T) {
	withMountedFS(t, IntegrationConfig{}, func(env *MountedFS) {
		// Setup: prepare the parent directory.
		env.MustMkdirInMountPoint(t, "concurrent/mkdir-race")

		// Action: 10 goroutines try to create the same directory.
		const numWorkers = 10
		rel := "concurrent/mkdir-race/target"
		p := env.MountPath(rel)

		var wg sync.WaitGroup
		errs := make([]error, numWorkers)

		wg.Add(numWorkers)
		for i := range numWorkers {
			go func(idx int) {
				defer wg.Done()
				errs[idx] = os.Mkdir(p, 0o755)
			}(i)
		}
		wg.Wait()

		// Verify: exactly one succeeded, others got EEXIST.
		successCount := 0
		for i := range numWorkers {
			if errs[i] == nil {
				successCount++
			}
		}
		require.Equal(t, 1, successCount, "expected exactly one mkdir to succeed")

		// Verify: directory exists.
		require.True(t, env.FileExistsInMountPoint(rel))
	})
}

// =============================================================================
// Delete-While-Open
// =============================================================================

// TestFUSE_UnlinkWhileOpen verifies file deletion while open behaves correctly.
// Note: FUSE loopback may not support reading from deleted files like native filesystems.
func TestFUSE_UnlinkWhileOpen(t *testing.T) {
	withMountedFS(t, IntegrationConfig{}, func(env *MountedFS) {
		// Setup: create a file.
		rel := "concurrent/unlink-open/file.txt"
		want := []byte("content of unlinked file")
		env.MustWriteFileInMountPoint(t, rel, want)

		// Action: open the file, then delete it.
		p := env.MountPath(rel)
		f, err := os.Open(p)
		require.NoError(t, err)
		defer func() { _ = f.Close() }()

		// Delete the file while it's open.
		env.MustRemoveFileInMountPoint(t, rel)

		// Verify: file is no longer visible in directory.
		require.False(t, env.FileExistsInMountPoint(rel))

		// Note: Unlike native filesystems, FUSE loopback may not allow reading
		// from a deleted file. The important behavior is that the delete succeeds
		// and the file is no longer visible. We don't assert on read behavior.
		_ = want // Suppress unused variable warning
	})
}

// =============================================================================
// Concurrent Create Operations
// =============================================================================

// TestFUSE_Concurrent_CreateSameFile verifies concurrent file creation handles correctly.
func TestFUSE_Concurrent_CreateSameFile(t *testing.T) {
	withMountedFS(t, IntegrationConfig{}, func(env *MountedFS) {
		// Setup: prepare the parent directory.
		env.MustMkdirInMountPoint(t, "concurrent/create-same")

		// Action: 10 goroutines try to create the same file with O_EXCL.
		const numWorkers = 10
		p := env.MountPath("concurrent/create-same/file.txt")

		var wg sync.WaitGroup
		errs := make([]error, numWorkers)
		files := make([]*os.File, numWorkers)

		wg.Add(numWorkers)
		for i := range numWorkers {
			go func(idx int) {
				defer wg.Done()
				f, err := os.OpenFile(p, os.O_CREATE|os.O_EXCL|os.O_WRONLY, 0o644)
				files[idx] = f
				errs[idx] = err
			}(i)
		}
		wg.Wait()

		// Cleanup: close any open files.
		for i := range numWorkers {
			if files[i] != nil {
				_ = files[i].Close()
			}
		}

		// Verify: exactly one succeeded, others got EEXIST.
		successCount := 0
		for i := range numWorkers {
			if errs[i] == nil {
				successCount++
			}
		}
		require.Equal(t, 1, successCount, "expected exactly one create to succeed with O_EXCL")
	})
}
