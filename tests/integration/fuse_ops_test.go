//go:build integration

package integration

import (
	"os"
	"path/filepath"
	"testing"

	"github.com/stretchr/testify/require"
)

// TestFUSE_ReadFile_smoke verifies basic reads are served correctly through PolicyFS.
func TestFUSE_ReadFile_smoke(t *testing.T) {
	p := filepath.Join(storageRoot, "fuse-ops", "read", "hello.txt")
	require.NoError(t, os.MkdirAll(filepath.Dir(p), 0o755))

	want := []byte("hello from fuse read test")
	require.NoError(t, os.WriteFile(p, want, 0o644))

	got, err := os.ReadFile(filepath.Join(mountPoint, "fuse-ops", "read", "hello.txt"))
	require.NoError(t, err)
	require.Equal(t, want, got)
}

// TestFUSE_Readdir_smoke verifies directory listings work through PolicyFS.
func TestFUSE_Readdir_smoke(t *testing.T) {
	p := filepath.Join(storageRoot, "fuse-ops", "readdir")
	require.NoError(t, os.MkdirAll(filepath.Join(p, "a"), 0o755))
	require.NoError(t, os.WriteFile(filepath.Join(p, "b.txt"), []byte("x"), 0o644))

	entries, err := os.ReadDir(filepath.Join(mountPoint, "fuse-ops", "readdir"))
	require.NoError(t, err)

	names := map[string]struct{}{}
	for _, e := range entries {
		names[e.Name()] = struct{}{}
	}

	_, okA := names["a"]
	require.True(t, okA)
	_, okB := names["b.txt"]
	require.True(t, okB)
}
