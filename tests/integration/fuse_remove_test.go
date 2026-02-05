//go:build integration

package integration

import (
	"testing"

	"github.com/stretchr/testify/require"
)

// TestFUSE_Unlink_removesFirstExistingReadTarget verifies UNLINK deletes from the first existing read target.
func TestFUSE_Unlink_removesFirstExistingReadTarget(t *testing.T) {
	withMountedFS(t, IntegrationConfig{
		ReadTargets: []string{"ssd2", "ssd1"},
		Targets:     []string{"ssd2", "ssd1"},
	}, func(env *MountedFS) {
		// Setup: create the same file on both targets so the first read target wins.
		rel := "fuse-mutate/unlink/x.txt"
		env.MustCreateFileInStoragePath(t, []byte("from ssd1"), "ssd1", rel)
		env.MustCreateFileInStoragePath(t, []byte("from ssd2"), "ssd2", rel)

		// Action: unlink through the mount.
		env.MustRemoveFileInMountPoint(t, rel)

		// Verify: removed only from the first existing read target.
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
		// Setup: create the same directory on both targets so the first read target wins.
		rel := "fuse-mutate/rmdir/d"
		env.MustCreateDirInStoragePath(t, "ssd1", rel)
		env.MustCreateDirInStoragePath(t, "ssd2", rel)

		// Action: rmdir through the mount.
		env.MustRemoveFileInMountPoint(t, rel)

		// Verify: removed only from the first existing read target.
		require.NoDirExists(t, env.StoragePath("ssd2", rel))
		require.DirExists(t, env.StoragePath("ssd1", rel))
	})
}
