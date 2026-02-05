//go:build integration

package integration

import (
	"errors"
	"os"
	"os/exec"
	"syscall"
	"testing"
	"time"

	"github.com/hieutdo/policyfs/internal/config"
	"github.com/stretchr/testify/require"
	"golang.org/x/sys/unix"
)

// TestFUSE_Setattr_chmod_nonRoot_denied verifies chmod through the mount is rejected for non-root callers.
func TestFUSE_Setattr_chmod_nonRoot_denied(t *testing.T) {
	withMountedFS(t, IntegrationConfig{
		AllowOther:   true,
		RoutingRules: []config.RoutingRule{{Match: "**", Targets: []string{"ssd1"}, ReadTargets: []string{"ssd1"}}},
	}, func(env *MountedFS) {
		// Setup: create the file on the physical target and ensure it's owned by root.
		rel := "fuse-mutate/setattr-nonroot/file.txt"
		physicalRoot := env.StoragePath("ssd1", "fuse-mutate/setattr-nonroot")
		require.NoError(t, os.RemoveAll(physicalRoot))
		env.MustCreateFileInStoragePath(t, []byte("x"), "ssd1", rel)

		physical := env.StoragePath("ssd1", rel)
		require.NoError(t, os.Chown(physical, 0, 0))
		require.NoError(t, syscall.Chmod(physical, 0o644))
		stBefore := env.MustStatT(t, physical)

		// Action: run chmod as a non-root user against the mount.
		err := env.RunAsUser(t, "nobody", "chmod 600 "+env.MountPath(rel))
		require.Error(t, err)
		var exitErr *exec.ExitError
		require.True(t, errors.As(err, &exitErr))
		require.NotEqual(t, 0, exitErr.ExitCode())

		// Verify: underlying storage permissions must not change.
		stAfter := env.MustStatT(t, physical)
		require.Equal(t, uint32(stBefore.Uid), uint32(stAfter.Uid))
		require.Equal(t, uint32(stBefore.Gid), uint32(stAfter.Gid))
		require.Equal(t, uint32(0o644), uint32(stAfter.Mode)&0o777)
	})
}

// TestFUSE_Setattr_chown_nonRoot_denied verifies chown through the mount is rejected for non-root callers.
func TestFUSE_Setattr_chown_nonRoot_denied(t *testing.T) {
	withMountedFS(t, IntegrationConfig{
		AllowOther:   true,
		RoutingRules: []config.RoutingRule{{Match: "**", Targets: []string{"ssd1"}, ReadTargets: []string{"ssd1"}}},
	}, func(env *MountedFS) {
		// Setup: create the file on the physical target and ensure it's owned by root.
		rel := "fuse-mutate/setattr-nonroot/file.txt"
		physicalRoot := env.StoragePath("ssd1", "fuse-mutate/setattr-nonroot")
		require.NoError(t, os.RemoveAll(physicalRoot))
		env.MustCreateFileInStoragePath(t, []byte("x"), "ssd1", rel)

		physical := env.StoragePath("ssd1", rel)
		require.NoError(t, os.Chown(physical, 0, 0))
		stBefore := env.MustStatT(t, physical)

		// Action: run chown as a non-root user against the mount.
		err := env.RunAsUser(t, "nobody", "chown 0:0 "+env.MountPath(rel))
		require.Error(t, err)
		var exitErr *exec.ExitError
		require.True(t, errors.As(err, &exitErr))
		require.NotEqual(t, 0, exitErr.ExitCode())

		// Verify: underlying uid/gid must not change.
		stAfter := env.MustStatT(t, physical)
		require.Equal(t, uint32(stBefore.Uid), uint32(stAfter.Uid))
		require.Equal(t, uint32(stBefore.Gid), uint32(stAfter.Gid))
	})
}

// TestFUSE_Setattr_chown_gidOnly_preservesUIDAndSetgid verifies Setattr chown gid-only does not
// change uid and does not clear setgid on directories.
func TestFUSE_Setattr_chown_gidOnly_preservesUIDAndSetgid(t *testing.T) {
	withMountedFS(t, IntegrationConfig{
		RoutingRules: []config.RoutingRule{{Match: "**", Targets: []string{"ssd1"}, ReadTargets: []string{"ssd1"}}},
	}, func(env *MountedFS) {
		// Setup: create a directory with setgid bit set.
		rel := "fuse-mutate/setattr-chown/sgid-dir"
		physical := env.StoragePath("ssd1", rel)
		require.NoError(t, os.RemoveAll(physical))
		env.MustCreateDirInStoragePath(t, "ssd1", rel)

		wantGID := 1234
		require.NoError(t, os.Chown(physical, 0, wantGID))
		require.NoError(t, syscall.Chmod(physical, 0o2770))
		stBefore := env.MustStatT(t, physical)
		require.NotZero(t, uint32(stBefore.Mode)&syscall.S_ISGID)

		// Action: chown with gid only (-1 for uid) through the mount.
		newGID := 2345
		require.NoError(t, os.Chown(env.MountPath(rel), -1, newGID))

		// Verify: uid unchanged + gid updated + setgid preserved.
		stAfter := env.MustStatT(t, physical)
		require.Equal(t, uint32(syscall.S_IFDIR), uint32(stAfter.Mode)&syscall.S_IFMT)
		require.Equal(t, uint32(stBefore.Uid), uint32(stAfter.Uid))
		require.Equal(t, uint32(newGID), uint32(stAfter.Gid))
		require.NotZero(t, uint32(stAfter.Mode)&syscall.S_ISGID)
	})
}

// TestFUSE_Setattr_chown_uidOnly_preservesGID verifies Setattr chown uid-only does not change gid.
func TestFUSE_Setattr_chown_uidOnly_preservesGID(t *testing.T) {
	withMountedFS(t, IntegrationConfig{
		RoutingRules: []config.RoutingRule{{Match: "**", Targets: []string{"ssd1"}, ReadTargets: []string{"ssd1"}}},
	}, func(env *MountedFS) {
		// Setup: create a file and set its gid explicitly.
		rel := "fuse-mutate/setattr-chown/file.txt"
		physical := env.StoragePath("ssd1", rel)
		require.NoError(t, os.RemoveAll(env.StoragePath("ssd1", "fuse-mutate/setattr-chown")))
		env.MustCreateFileInStoragePath(t, []byte("x"), "ssd1", rel)

		wantGID := 1234
		require.NoError(t, os.Chown(physical, 0, wantGID))
		stBefore := env.MustStatT(t, physical)

		// Action: chown with uid only (-1 for gid) through the mount.
		newUID := 2345
		require.NoError(t, os.Chown(env.MountPath(rel), newUID, -1))

		// Verify: uid updated + gid unchanged.
		stAfter := env.MustStatT(t, physical)
		require.Equal(t, uint32(newUID), uint32(stAfter.Uid))
		require.Equal(t, uint32(stBefore.Gid), uint32(stAfter.Gid))
	})
}

// TestFUSE_Setattr_lchown_symlink_changesLinkNotTarget verifies lchown through the mount affects
// the symlink itself, not the target.
func TestFUSE_Setattr_lchown_symlink_changesLinkNotTarget(t *testing.T) {
	withMountedFS(t, IntegrationConfig{
		RoutingRules: []config.RoutingRule{{Match: "**", Targets: []string{"ssd1"}, ReadTargets: []string{"ssd1"}}},
	}, func(env *MountedFS) {
		// Setup: create target file + symlink on the physical storage.
		relDir := "fuse-mutate/setattr-symlink"
		relTarget := relDir + "/target.txt"
		relLink := relDir + "/link.txt"
		physicalDir := env.StoragePath("ssd1", relDir)
		require.NoError(t, os.RemoveAll(physicalDir))
		env.MustCreateDirInStoragePath(t, "ssd1", relDir)
		env.MustCreateFileInStoragePath(t, []byte("x"), "ssd1", relTarget)
		require.NoError(t, os.Symlink("target.txt", env.StoragePath("ssd1", relLink)))

		stTargetBefore := env.MustStatT(t, env.StoragePath("ssd1", relTarget))
		stLinkBefore := env.MustStatT(t, env.StoragePath("ssd1", relLink))
		require.Equal(t, uint32(syscall.S_IFLNK), uint32(stLinkBefore.Mode)&syscall.S_IFMT)

		// Action: lchown the symlink through the mount.
		newGID := 1234
		require.NoError(t, os.Lchown(env.MountPath(relLink), -1, newGID))

		// Verify: target is unchanged; symlink gid is updated.
		stTargetAfter := env.MustStatT(t, env.StoragePath("ssd1", relTarget))
		stLinkAfter := env.MustStatT(t, env.StoragePath("ssd1", relLink))
		require.Equal(t, uint32(stTargetBefore.Gid), uint32(stTargetAfter.Gid))
		require.Equal(t, uint32(newGID), uint32(stLinkAfter.Gid))
	})
}

// TestFUSE_Setattr_utimens_symlink_noFollow verifies utimens applies to the symlink itself (no follow).
func TestFUSE_Setattr_utimens_symlink_noFollow(t *testing.T) {
	withMountedFS(t, IntegrationConfig{
		RoutingRules: []config.RoutingRule{{Match: "**", Targets: []string{"ssd1"}, ReadTargets: []string{"ssd1"}}},
	}, func(env *MountedFS) {
		// Setup: create target file + symlink on the physical storage.
		relDir := "fuse-mutate/setattr-utimens-symlink"
		relTarget := relDir + "/target.txt"
		relLink := relDir + "/link.txt"
		physicalDir := env.StoragePath("ssd1", relDir)
		require.NoError(t, os.RemoveAll(physicalDir))
		env.MustCreateDirInStoragePath(t, "ssd1", relDir)
		env.MustCreateFileInStoragePath(t, []byte("x"), "ssd1", relTarget)
		require.NoError(t, os.Symlink("target.txt", env.StoragePath("ssd1", relLink)))

		fiTargetBefore, err := os.Lstat(env.StoragePath("ssd1", relTarget))
		require.NoError(t, err)
		targetMTimeBefore := fiTargetBefore.ModTime()

		fiLinkBefore, err := os.Lstat(env.StoragePath("ssd1", relLink))
		require.NoError(t, err)
		linkMTimeBefore := fiLinkBefore.ModTime()
		linkMTimeAfter := linkMTimeBefore.Add(10 * time.Second)

		// Action: utimens on the symlink with no-follow through the mount.
		ta, err := unix.TimeToTimespec(linkMTimeBefore)
		require.NoError(t, err)
		tm, err := unix.TimeToTimespec(linkMTimeAfter)
		require.NoError(t, err)
		require.NoError(t, unix.UtimesNanoAt(unix.AT_FDCWD, env.MountPath(relLink), []unix.Timespec{ta, tm}, unix.AT_SYMLINK_NOFOLLOW))

		// Verify: target mtime unchanged; symlink mtime changed.
		fiTargetAfter, err := os.Lstat(env.StoragePath("ssd1", relTarget))
		require.NoError(t, err)
		require.Equal(t, targetMTimeBefore.Unix(), fiTargetAfter.ModTime().Unix())

		fiLinkAfter, err := os.Lstat(env.StoragePath("ssd1", relLink))
		require.NoError(t, err)
		require.Equal(t, linkMTimeAfter.Unix(), fiLinkAfter.ModTime().Unix())
	})
}

// TestFUSE_Setattr_truncateChmodUtimens verifies setattr operations reflect in the storage.
func TestFUSE_Setattr_truncateChmodUtimens(t *testing.T) {
	withMountedFS(t, IntegrationConfig{
		RoutingRules: []config.RoutingRule{
			{Match: "fuse-mutate/setattr/**", WriteTargets: []string{"ssd1"}, ReadTargets: []string{"ssd1"}},
			{Match: "**", WriteTargets: []string{"ssd2"}, ReadTargets: []string{"ssd2"}},
		},
	}, func(env *MountedFS) {
		// Setup: write the initial file through the mount.
		rel := "fuse-mutate/setattr/f.txt"
		want := []byte("0123456789")
		env.MustWriteFileInMountPoint(t, rel, want)

		// Action: apply chmod, truncate, and chtimes through the mount.
		err := os.Chmod(env.MountPath(rel), 0o600)
		require.NoError(t, err)

		err = os.Truncate(env.MountPath(rel), 3)
		require.NoError(t, err)

		at := time.Unix(1700000000, 0)
		mt := time.Unix(1700000001, 0)
		err = os.Chtimes(env.MountPath(rel), at, mt)
		require.NoError(t, err)

		// Verify: file exists on the selected target and attributes match.
		require.FileExists(t, env.StoragePath("ssd1", rel))
		require.NoFileExists(t, env.StoragePath("ssd2", rel))
		st, err := os.Stat(env.StoragePath("ssd1", rel))
		require.NoError(t, err)
		require.Equal(t, int64(3), st.Size())
		require.Equal(t, os.FileMode(0o600), st.Mode().Perm())
		require.WithinDuration(t, mt, st.ModTime(), 2*time.Second)
	})
}
