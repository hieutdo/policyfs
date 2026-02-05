//go:build integration

package integration

import (
	"testing"

	"github.com/stretchr/testify/require"
)

// TestFUSE_ReadFile_smoke verifies basic reads are served correctly through PolicyFS.
func TestFUSE_ReadFile_smoke(t *testing.T) {
	withMountedFS(t, IntegrationConfig{}, func(env *MountedFS) {
		// Setup: create file on storage.
		want := []byte("hello from fuse read test")
		env.MustCreateFileInStoragePath(t, want, "ssd1", "fuse-ops/read/hello.txt")

		// Action: read through the mount.
		got := env.MustReadFileInMountPoint(t, "fuse-ops/read/hello.txt")

		// Verify: content matches.
		require.Equal(t, want, got)
	})
}

// TestFUSE_Read_PrefersFirstReadTarget verifies reads prefer the first matching read target.
func TestFUSE_Read_PrefersFirstReadTarget(t *testing.T) {
	withMountedFS(t, IntegrationConfig{}, func(env *MountedFS) {
		// Setup: same path exists on both targets, with different content.
		path := "fuse-m2/read-pref/hello.txt"
		env.MustCreateDirInStoragePath(t, "ssd1", "fuse-m2/read-pref")
		env.MustCreateDirInStoragePath(t, "ssd2", "fuse-m2/read-pref")

		env.MustCreateFileInStoragePath(t, []byte("from ssd1"), "ssd1", path)
		env.MustCreateFileInStoragePath(t, []byte("from ssd2"), "ssd2", path)

		// Action: read through the mount.
		got := env.MustReadFileInMountPoint(t, path)

		// Verify: content comes from the first read target (ssd2 by default).
		require.Equal(t, []byte("from ssd2"), got)
	})
}
