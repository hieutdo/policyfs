package fuse

import (
	"context"
	"os"
	"path/filepath"
	"syscall"
	"testing"

	"github.com/stretchr/testify/require"
)

// Test_materializeParentDirs verifies parent directories are created segment-by-segment.
func Test_materializeParentDirs(t *testing.T) {
	// This test pins basic mkdir -p behavior and ENOTDIR semantics.
	t.Run("should create all parent dirs", func(t *testing.T) {
		root := t.TempDir()
		virtualPath := filepath.FromSlash("a/b/c/file.txt")
		require.NoError(t, materializeParentDirs(context.Background(), root, virtualPath))

		for _, rel := range []string{"a", "a/b", "a/b/c"} {
			p := filepath.Join(root, filepath.FromSlash(rel))
			fi, err := os.Stat(p)
			require.NoError(t, err)
			require.True(t, fi.IsDir())
		}
	})

	t.Run("should return ENOTDIR when a path segment exists as a file", func(t *testing.T) {
		root := t.TempDir()
		// Setup: create root/a as a file.
		require.NoError(t, os.WriteFile(filepath.Join(root, "a"), []byte("x"), 0o644))

		err := materializeParentDirs(context.Background(), root, filepath.FromSlash("a/b/file.txt"))
		require.Error(t, err)
		require.ErrorIs(t, err, syscall.ENOTDIR)
	})
}

// Test_preserveOwnerForCreate verifies this helper is a no-op without caller identity.
func Test_preserveOwnerForCreate(t *testing.T) {
	// We intentionally avoid asserting chown behavior because CI/dev containers may run as non-root.
	require.NoError(t, preserveOwnerForCreate(context.Background(), "/", -1, "/"))
}
