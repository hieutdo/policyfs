//go:build integration

package integration

import (
	"os"
	"syscall"
	"testing"

	"github.com/hieutdo/policyfs/internal/config"
	"github.com/stretchr/testify/require"
)

// TestFUSE_Create_materializeParentDirs_inheritsSetgidAndGid verifies parent-dir materialization
// inherits setgid + gid from the physical parent directory on the selected target.
func TestFUSE_Create_materializeParentDirs_inheritsSetgidAndGid(t *testing.T) {
	withMountedFS(t, IntegrationConfig{}, func(env *MountedFS) {
		// Setup: create a setgid parent directory on ssd1 and ensure the virtual parent exists via ssd2.
		parentRel := "perm/sgid-parent"
		require.NoError(t, os.RemoveAll(env.StoragePath("ssd1", parentRel)))
		require.NoError(t, os.RemoveAll(env.StoragePath("ssd2", parentRel)))
		parentPhysical := env.StoragePath("ssd1", parentRel)
		env.MustCreateDirInStoragePath(t, "ssd1", parentRel)
		env.MustCreateDirInStoragePath(t, "ssd2", parentRel+"/childdir")

		wantGID := uint32(1234)
		if os.Getuid() == 0 {
			require.NoError(t, os.Chown(parentPhysical, 0, int(wantGID)))
		}
		require.NoError(t, syscall.Chmod(parentPhysical, 0o2770))
		if os.Getuid() == 0 {
			// chown may clear setgid; restore.
			require.NoError(t, syscall.Chmod(parentPhysical, 0o2770))
		}
		stParent := env.MustStatT(t, parentPhysical)
		require.NotZero(t, uint32(stParent.Mode)&syscall.S_ISGID)

		// Action: create a file under a parent path that must be materialized on ssd1.
		rel := parentRel + "/childdir/file.txt"
		require.NoError(t, os.WriteFile(env.MountPath(rel), []byte("x"), 0o644))

		// Verify: the missing parent dir was created with setgid + correct perms and the file exists on ssd1.
		childDirPhysical := env.StoragePath("ssd1", parentRel+"/childdir")
		stChildDir := env.MustStatT(t, childDirPhysical)
		require.Equal(t, uint32(syscall.S_IFDIR), uint32(stChildDir.Mode)&syscall.S_IFMT)
		require.NotZero(t, uint32(stChildDir.Mode)&syscall.S_ISGID)
		require.Equal(t, uint32(0o770), uint32(stChildDir.Mode)&0o777)
		if os.Getuid() == 0 {
			require.Equal(t, wantGID, uint32(stChildDir.Gid))
		}

		filePhysical := env.StoragePath("ssd1", rel)
		stFile := env.MustStatT(t, filePhysical)
		if os.Getuid() == 0 {
			require.Equal(t, wantGID, uint32(stFile.Gid))
		}
		require.NoFileExists(t, env.StoragePath("ssd2", rel))
	})
}

// TestFUSE_Mkdir_inheritsSetgidAndGid verifies MKDIR sets setgid and gid based on the physical parent.
func TestFUSE_Mkdir_inheritsSetgidAndGid(t *testing.T) {
	withMountedFS(t, IntegrationConfig{}, func(env *MountedFS) {
		// Setup: same shape as the create test, but for a mkdir under a materialized parent.
		parentRel := "perm/sgid-parent"
		require.NoError(t, os.RemoveAll(env.StoragePath("ssd1", parentRel)))
		require.NoError(t, os.RemoveAll(env.StoragePath("ssd2", parentRel)))
		parentPhysical := env.StoragePath("ssd1", parentRel)
		env.MustCreateDirInStoragePath(t, "ssd1", parentRel)
		env.MustCreateDirInStoragePath(t, "ssd2", parentRel+"/childdir")

		wantGID := uint32(1234)
		if os.Getuid() == 0 {
			require.NoError(t, os.Chown(parentPhysical, 0, int(wantGID)))
		}
		require.NoError(t, syscall.Chmod(parentPhysical, 0o2770))
		if os.Getuid() == 0 {
			// chown may clear setgid; restore.
			require.NoError(t, syscall.Chmod(parentPhysical, 0o2770))
		}
		stParent := env.MustStatT(t, parentPhysical)
		require.NotZero(t, uint32(stParent.Mode)&syscall.S_ISGID)

		// Action: mkdir under the virtual childdir.
		childRel := parentRel + "/childdir/newdir"
		require.NoError(t, os.Mkdir(env.MountPath(childRel), 0o755))

		// Verify: parent was materialized and setgid propagated.
		materializedParentPhysical := env.StoragePath("ssd1", parentRel+"/childdir")
		stMaterializedParent := env.MustStatT(t, materializedParentPhysical)
		require.Equal(t, uint32(syscall.S_IFDIR), uint32(stMaterializedParent.Mode)&syscall.S_IFMT)
		require.NotZero(t, uint32(stMaterializedParent.Mode)&syscall.S_ISGID)
		if os.Getuid() == 0 {
			require.Equal(t, wantGID, uint32(stMaterializedParent.Gid))
		}

		childPhysical := env.StoragePath("ssd1", childRel)
		stChild := env.MustStatT(t, childPhysical)
		require.Equal(t, uint32(syscall.S_IFDIR), uint32(stChild.Mode)&syscall.S_IFMT)
		require.NotZero(t, uint32(stChild.Mode)&syscall.S_ISGID)
		if os.Getuid() == 0 {
			require.Equal(t, wantGID, uint32(stChild.Gid))
		}
		require.NoDirExists(t, env.StoragePath("ssd2", childRel))
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
		// Action: create a file that matches the first rule.
		rel1 := "fuse-mutate/create/hello.txt"
		want := []byte("created")
		env.MustWriteFileInMountPoint(t, rel1, want)

		// Verify: file is on ssd1 only.
		got := env.MustReadFileInStoragePath(t, "ssd1", rel1)
		require.Equal(t, want, got)
		require.NoFileExists(t, env.StoragePath("ssd2", rel1))

		// Action: create a file that falls through to catch-all.
		rel2 := "create/hello2.txt"
		want2 := []byte("created2")
		env.MustWriteFileInMountPoint(t, rel2, want2)

		// Verify: file is on ssd2 only.
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
		// Action: create a directory that matches the first rule.
		rel := "fuse-mutate/mkdir/a"
		env.MustMkdirInMountPoint(t, rel)

		// Verify: directory exists only on ssd2.
		require.DirExists(t, env.StoragePath("ssd2", rel))
		require.NoDirExists(t, env.StoragePath("ssd1", rel))
	})
}
