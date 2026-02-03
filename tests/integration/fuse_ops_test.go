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
	withMountedFS(t, IntegrationConfig{}, func(env *MountedFS) {
		p := filepath.Join(env.StorageRoot1, "fuse-ops", "read", "hello.txt")
		require.NoError(t, os.MkdirAll(filepath.Dir(p), 0o755))

		want := []byte("hello from fuse read test")
		require.NoError(t, os.WriteFile(p, want, 0o644))

		got, err := os.ReadFile(filepath.Join(env.MountPoint, "fuse-ops", "read", "hello.txt"))
		require.NoError(t, err)
		require.Equal(t, want, got)
	})
}

// TestFUSE_Readdir_smoke verifies directory listings work through PolicyFS.
func TestFUSE_Readdir_smoke(t *testing.T) {
	withMountedFS(t, IntegrationConfig{}, func(env *MountedFS) {
		p := filepath.Join(env.StorageRoot1, "fuse-ops", "readdir")
		require.NoError(t, os.MkdirAll(filepath.Join(p, "a"), 0o755))
		require.NoError(t, os.WriteFile(filepath.Join(p, "b.txt"), []byte("x"), 0o644))

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

		p1 := filepath.Join(env.StorageRoot1, path)
		require.NoError(t, os.MkdirAll(filepath.Dir(p1), 0o755))
		p2 := filepath.Join(env.StorageRoot2, path)
		require.NoError(t, os.MkdirAll(filepath.Dir(p2), 0o755))

		fromSSD1 := []byte("from ssd1")
		fromSSD2 := []byte("from ssd2")
		require.NoError(t, os.WriteFile(p1, fromSSD1, 0o644))
		require.NoError(t, os.WriteFile(p2, fromSSD2, 0o644))

		got, err := os.ReadFile(filepath.Join(env.MountPoint, path))
		require.NoError(t, err)
		require.Equal(t, fromSSD2, got)
	})
}

// TestFUSE_Readdir_MergesAndDedupes verifies READDIR unions entries across targets and dedupes by name.
func TestFUSE_Readdir_MergesAndDedupes(t *testing.T) {
	withMountedFS(t, IntegrationConfig{}, func(env *MountedFS) {
		path := filepath.Join("fuse-m2", "readdir")

		ssd2Dir := filepath.Join(env.StorageRoot2, path)
		require.NoError(t, os.MkdirAll(ssd2Dir, 0o755))
		require.NoError(t, os.WriteFile(filepath.Join(ssd2Dir, "a.txt"), []byte("a"), 0o644))
		require.NoError(t, os.WriteFile(filepath.Join(ssd2Dir, "dup"), []byte("file"), 0o644))

		ssd1Dir := filepath.Join(env.StorageRoot1, path)
		require.NoError(t, os.MkdirAll(filepath.Join(ssd1Dir, "dup"), 0o755))
		require.NoError(t, os.WriteFile(filepath.Join(ssd1Dir, "b.txt"), []byte("b"), 0o644))

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
