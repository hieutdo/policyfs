//go:build integration

package integration

import (
	"errors"
	"fmt"
	"os"
	"os/exec"
	"path/filepath"
	"syscall"
	"testing"

	"github.com/hieutdo/policyfs/internal/config"
	"github.com/stretchr/testify/require"
)

// TestFUSE_Create_nonRoot_parentNoWrite_denied verifies create is rejected when the parent directory is not writable.
func TestFUSE_Create_nonRoot_parentNoWrite_denied(t *testing.T) {
	withMountedFS(t, IntegrationConfig{
		AllowOther: true,
		RoutingRules: []config.RoutingRule{{
			Match:       "**",
			Targets:     []string{"ssd1"},
			ReadTargets: []string{"ssd1"},
		}},
	}, func(env *MountedFS) {
		relParent := "fuse-perms/create-no-write"
		relFile := relParent + "/x.txt"
		physicalParent := env.StoragePath("ssd1", relParent)
		require.NoError(t, os.RemoveAll(physicalParent))
		require.NoError(t, os.MkdirAll(physicalParent, 0o755))
		// Parent owned by root and not writable by others.
		require.NoError(t, os.Chown(physicalParent, 0, 0))
		require.NoError(t, os.Chmod(physicalParent, 0o555))

		cmd := "sh -c 'echo hi > " + env.MountPath(relFile) + "'"
		err := env.RunAsUser(t, "nobody", cmd)
		require.Error(t, err)
		exitErr, ok := errors.AsType[*exec.ExitError](err)
		require.True(t, ok)
		require.NotEqual(t, 0, exitErr.ExitCode())
		require.NoFileExists(t, env.StoragePath("ssd1", relFile))
	})
}

// TestFUSE_Unlink_nonRoot_stickyDir_notOwner_denied verifies sticky-bit semantics reject unlink by non-owner.
func TestFUSE_Unlink_nonRoot_stickyDir_notOwner_denied(t *testing.T) {
	withMountedFS(t, IntegrationConfig{
		AllowOther: true,
		RoutingRules: []config.RoutingRule{{
			Match:       "**",
			Targets:     []string{"ssd1"},
			ReadTargets: []string{"ssd1"},
		}},
	}, func(env *MountedFS) {
		relDir := "fuse-perms/sticky/d"
		relFile := relDir + "/x.txt"

		physicalDir := env.StoragePath("ssd1", relDir)
		require.NoError(t, os.RemoveAll(physicalDir))
		require.NoError(t, os.MkdirAll(physicalDir, 0o755))
		// Sticky dir owned by root.
		require.NoError(t, os.Chown(physicalDir, 0, 0))
		require.NoError(t, syscall.Chmod(physicalDir, 0o1777))

		// Create the file owned by root.
		physicalFile := env.StoragePath("ssd1", relFile)
		require.NoError(t, os.MkdirAll(filepath.Dir(physicalFile), 0o755))
		require.NoError(t, os.WriteFile(physicalFile, []byte("x"), 0o644))
		require.NoError(t, os.Chown(physicalFile, 0, 0))

		// Sanity: sticky bit must be present on the physical dir.
		dirSt := env.MustStatT(t, physicalDir)
		require.NotEqual(t, uint32(0), uint32(dirSt.Mode)&uint32(syscall.S_ISVTX))
		require.Equal(t, uint32(0), dirSt.Uid)
		fileSt := env.MustStatT(t, physicalFile)
		require.Equal(t, uint32(0), fileSt.Uid)

		err := env.RunAsUser(t, "nobody", "rm -f "+env.MountPath(relFile))
		require.Error(t, err)
		exitErr, ok := errors.AsType[*exec.ExitError](err)
		require.True(t, ok)
		require.NotEqual(t, 0, exitErr.ExitCode())
		require.FileExists(t, physicalFile)
	})
}

// TestFUSE_Rename_nonRoot_stickyDir_notOwner_denied verifies sticky-bit semantics reject rename by non-owner.
func TestFUSE_Rename_nonRoot_stickyDir_notOwner_denied(t *testing.T) {
	withMountedFS(t, IntegrationConfig{
		AllowOther: true,
		RoutingRules: []config.RoutingRule{{
			Match:       "**",
			Targets:     []string{"ssd1"},
			ReadTargets: []string{"ssd1"},
		}},
	}, func(env *MountedFS) {
		relDir := "fuse-perms/sticky-rename/d"
		oldRel := relDir + "/old.txt"
		newRel := relDir + "/new.txt"

		physicalDir := env.StoragePath("ssd1", relDir)
		require.NoError(t, os.RemoveAll(physicalDir))
		require.NoError(t, os.MkdirAll(physicalDir, 0o755))
		require.NoError(t, os.Chown(physicalDir, 0, 0))
		require.NoError(t, syscall.Chmod(physicalDir, 0o1777))

		physicalOld := env.StoragePath("ssd1", oldRel)
		require.NoError(t, os.WriteFile(physicalOld, []byte("x"), 0o644))
		require.NoError(t, os.Chown(physicalOld, 0, 0))

		// Sanity: sticky bit must be present on the physical dir.
		dirSt := env.MustStatT(t, physicalDir)
		require.NotEqual(t, uint32(0), uint32(dirSt.Mode)&uint32(syscall.S_ISVTX))
		oldSt := env.MustStatT(t, physicalOld)
		require.Equal(t, uint32(0), oldSt.Uid)

		err := env.RunAsUser(t, "nobody", "mv "+env.MountPath(oldRel)+" "+env.MountPath(newRel))
		require.Error(t, err)
		exitErr, ok := errors.AsType[*exec.ExitError](err)
		require.True(t, ok)
		require.NotEqual(t, 0, exitErr.ExitCode())
		require.FileExists(t, physicalOld)
		require.NoFileExists(t, env.StoragePath("ssd1", newRel))
	})
}

// TestFUSE_Rmdir_nonRoot_parentNoWrite_denied verifies rmdir is rejected when parent is not writable.
func TestFUSE_Rmdir_nonRoot_parentNoWrite_denied(t *testing.T) {
	withMountedFS(t, IntegrationConfig{
		AllowOther: true,
		RoutingRules: []config.RoutingRule{{
			Match:       "**",
			Targets:     []string{"ssd1"},
			ReadTargets: []string{"ssd1"},
		}},
	}, func(env *MountedFS) {
		relParent := "fuse-perms/rmdir-no-write"
		relDir := relParent + "/d"

		physicalParent := env.StoragePath("ssd1", relParent)
		require.NoError(t, os.RemoveAll(physicalParent))
		require.NoError(t, os.MkdirAll(physicalParent, 0o755))
		require.NoError(t, os.Chown(physicalParent, 0, 0))
		require.NoError(t, os.Chmod(physicalParent, 0o555))

		physicalDir := env.StoragePath("ssd1", relDir)
		require.NoError(t, os.MkdirAll(physicalDir, 0o755))

		err := env.RunAsUser(t, "nobody", "rmdir "+env.MountPath(relDir))
		require.Error(t, err)
		exitErr, ok := errors.AsType[*exec.ExitError](err)
		require.True(t, ok)
		require.NotEqual(t, 0, exitErr.ExitCode())
		require.DirExists(t, physicalDir)
	})
}

// TestFUSE_Link_nonRoot_parentNoWrite_denied verifies hardlink creation is rejected when parent is not writable.
func TestFUSE_Link_nonRoot_parentNoWrite_denied(t *testing.T) {
	withMountedFS(t, IntegrationConfig{
		AllowOther: true,
		RoutingRules: []config.RoutingRule{{
			Match:       "**",
			Targets:     []string{"ssd1"},
			ReadTargets: []string{"ssd1"},
		}},
	}, func(env *MountedFS) {
		relDir := "fuse-perms/link-no-write"
		relSrc := relDir + "/src.txt"
		relDst := relDir + "/dst.txt"

		physicalDir := env.StoragePath("ssd1", relDir)
		require.NoError(t, os.RemoveAll(physicalDir))
		require.NoError(t, os.MkdirAll(physicalDir, 0o755))
		require.NoError(t, os.Chown(physicalDir, 0, 0))
		require.NoError(t, os.Chmod(physicalDir, 0o555))

		physicalSrc := env.StoragePath("ssd1", relSrc)
		require.NoError(t, os.WriteFile(physicalSrc, []byte("x"), 0o644))

		err := env.RunAsUser(t, "nobody", "ln "+env.MountPath(relSrc)+" "+env.MountPath(relDst))
		require.Error(t, err)
		exitErr, ok := errors.AsType[*exec.ExitError](err)
		require.True(t, ok)
		require.NotEqual(t, 0, exitErr.ExitCode())
		require.NoFileExists(t, env.StoragePath("ssd1", relDst))
	})
}

// TestFUSE_Unlink_nonRoot_shouldStillWorkWhenWritable verifies non-root unlink succeeds when directory is writable and not sticky.
func TestFUSE_Unlink_nonRoot_shouldStillWorkWhenWritable(t *testing.T) {
	withMountedFS(t, IntegrationConfig{
		AllowOther: true,
		RoutingRules: []config.RoutingRule{{
			Match:       "**",
			Targets:     []string{"ssd1"},
			ReadTargets: []string{"ssd1"},
		}},
	}, func(env *MountedFS) {
		relDir := "fuse-perms/unlink-ok/d"
		relFile := relDir + "/x.txt"

		physicalDir := env.StoragePath("ssd1", relDir)
		require.NoError(t, os.RemoveAll(physicalDir))
		require.NoError(t, os.MkdirAll(physicalDir, 0o777))
		require.NoError(t, os.Chown(physicalDir, 0, 0))
		require.NoError(t, os.Chmod(physicalDir, 0o777))

		physicalFile := env.StoragePath("ssd1", relFile)
		require.NoError(t, os.WriteFile(physicalFile, []byte("x"), 0o644))
		require.NoError(t, os.Chown(physicalFile, 0, 0))

		err := env.RunAsUser(t, "nobody", "rm -f "+env.MountPath(relFile))
		require.NoError(t, err)
		require.NoFileExists(t, physicalFile)
	})
}

// TestFUSE_Create_nonRoot_shouldStillWorkWhenWritable verifies non-root create succeeds when parent is writable.
func TestFUSE_Create_nonRoot_shouldStillWorkWhenWritable(t *testing.T) {
	withMountedFS(t, IntegrationConfig{
		AllowOther: true,
		RoutingRules: []config.RoutingRule{{
			Match:       "**",
			Targets:     []string{"ssd1"},
			ReadTargets: []string{"ssd1"},
		}},
	}, func(env *MountedFS) {
		relParent := "fuse-perms/create-ok"
		relFile := relParent + "/x.txt"
		physicalParent := env.StoragePath("ssd1", relParent)
		require.NoError(t, os.RemoveAll(physicalParent))
		require.NoError(t, os.MkdirAll(physicalParent, 0o777))
		require.NoError(t, os.Chown(physicalParent, 0, 0))
		require.NoError(t, os.Chmod(physicalParent, 0o777))

		cmd := "sh -c 'echo hi > " + env.MountPath(relFile) + "'"
		err := env.RunAsUser(t, "nobody", cmd)
		require.NoError(t, err)
		require.FileExists(t, env.StoragePath("ssd1", relFile))
	})
}

// TestFUSE_Rename_nonRoot_shouldStillWorkWhenWritable verifies non-root rename succeeds when directory is writable and not sticky.
func TestFUSE_Rename_nonRoot_shouldStillWorkWhenWritable(t *testing.T) {
	withMountedFS(t, IntegrationConfig{
		AllowOther: true,
		RoutingRules: []config.RoutingRule{{
			Match:       "**",
			Targets:     []string{"ssd1"},
			ReadTargets: []string{"ssd1"},
		}},
	}, func(env *MountedFS) {
		relDir := "fuse-perms/rename-ok/d"
		oldRel := relDir + "/old.txt"
		newRel := relDir + "/new.txt"

		physicalDir := env.StoragePath("ssd1", relDir)
		require.NoError(t, os.RemoveAll(physicalDir))
		require.NoError(t, os.MkdirAll(physicalDir, 0o777))
		require.NoError(t, os.Chown(physicalDir, 0, 0))
		require.NoError(t, os.Chmod(physicalDir, 0o777))

		physicalOld := env.StoragePath("ssd1", oldRel)
		require.NoError(t, os.WriteFile(physicalOld, []byte("x"), 0o644))
		require.NoError(t, os.Chown(physicalOld, 0, 0))

		err := env.RunAsUser(t, "nobody", "mv "+env.MountPath(oldRel)+" "+env.MountPath(newRel))
		require.NoError(t, err)
		require.NoFileExists(t, physicalOld)
		require.FileExists(t, env.StoragePath("ssd1", newRel))
	})
}

// TestFUSE_Rmdir_nonRoot_shouldStillWorkWhenWritable verifies non-root rmdir succeeds when parent is writable.
func TestFUSE_Rmdir_nonRoot_shouldStillWorkWhenWritable(t *testing.T) {
	withMountedFS(t, IntegrationConfig{
		AllowOther: true,
		RoutingRules: []config.RoutingRule{{
			Match:       "**",
			Targets:     []string{"ssd1"},
			ReadTargets: []string{"ssd1"},
		}},
	}, func(env *MountedFS) {
		relParent := "fuse-perms/rmdir-ok"
		relDir := relParent + "/d"

		physicalParent := env.StoragePath("ssd1", relParent)
		require.NoError(t, os.RemoveAll(physicalParent))
		require.NoError(t, os.MkdirAll(physicalParent, 0o777))
		require.NoError(t, os.Chown(physicalParent, 0, 0))
		require.NoError(t, os.Chmod(physicalParent, 0o777))

		physicalDir := env.StoragePath("ssd1", relDir)
		require.NoError(t, os.MkdirAll(physicalDir, 0o755))

		err := env.RunAsUser(t, "nobody", "rmdir "+env.MountPath(relDir))
		require.NoError(t, err)
		require.NoDirExists(t, physicalDir)
	})
}

// TestFUSE_Link_nonRoot_shouldStillWorkWhenWritable verifies non-root hardlink succeeds when parent is writable.
func TestFUSE_Link_nonRoot_shouldStillWorkWhenWritable(t *testing.T) {
	withMountedFS(t, IntegrationConfig{
		AllowOther: true,
		RoutingRules: []config.RoutingRule{{
			Match:       "**",
			Targets:     []string{"ssd1"},
			ReadTargets: []string{"ssd1"},
		}},
	}, func(env *MountedFS) {
		relDir := "fuse-perms/link-ok"
		relSrc := relDir + "/src.txt"
		relDst := relDir + "/dst.txt"

		physicalDir := env.StoragePath("ssd1", relDir)
		require.NoError(t, os.RemoveAll(physicalDir))
		require.NoError(t, os.MkdirAll(physicalDir, 0o777))
		require.NoError(t, os.Chown(physicalDir, 0, 0))
		require.NoError(t, os.Chmod(physicalDir, 0o777))

		// Create the source file as nobody to avoid Linux protected_hardlinks restrictions.
		cmd := "sh -c 'echo x > " + env.MountPath(relSrc) + "'"
		require.NoError(t, env.RunAsUser(t, "nobody", cmd))

		err := env.RunAsUser(t, "nobody", "ln "+env.MountPath(relSrc)+" "+env.MountPath(relDst))
		require.NoError(t, err)
		require.FileExists(t, env.StoragePath("ssd1", relDst))
	})
}

// TestFUSE_Mkdir_nonRoot_parentNoWrite_denied verifies mkdir is rejected when parent directory is not writable.
func TestFUSE_Mkdir_nonRoot_parentNoWrite_denied(t *testing.T) {
	withMountedFS(t, IntegrationConfig{
		AllowOther: true,
		RoutingRules: []config.RoutingRule{{
			Match:       "**",
			Targets:     []string{"ssd1"},
			ReadTargets: []string{"ssd1"},
		}},
	}, func(env *MountedFS) {
		relParent := "fuse-perms/mkdir-no-write"
		relDir := relParent + "/d"
		physicalParent := env.StoragePath("ssd1", relParent)
		require.NoError(t, os.RemoveAll(physicalParent))
		require.NoError(t, os.MkdirAll(physicalParent, 0o755))
		require.NoError(t, os.Chown(physicalParent, 0, 0))
		require.NoError(t, os.Chmod(physicalParent, 0o555))

		err := env.RunAsUser(t, "nobody", "mkdir "+env.MountPath(relDir))
		require.Error(t, err)
		exitErr, ok := errors.AsType[*exec.ExitError](err)
		require.True(t, ok)
		require.NotEqual(t, 0, exitErr.ExitCode())
		require.NoDirExists(t, env.StoragePath("ssd1", relDir))
	})
}

// TestFUSE_Mkdir_nonRoot_shouldStillWorkWhenWritable verifies mkdir succeeds when parent is writable.
func TestFUSE_Mkdir_nonRoot_shouldStillWorkWhenWritable(t *testing.T) {
	withMountedFS(t, IntegrationConfig{
		AllowOther: true,
		RoutingRules: []config.RoutingRule{{
			Match:       "**",
			Targets:     []string{"ssd1"},
			ReadTargets: []string{"ssd1"},
		}},
	}, func(env *MountedFS) {
		relParent := "fuse-perms/mkdir-ok"
		relDir := relParent + "/d"
		physicalParent := env.StoragePath("ssd1", relParent)
		require.NoError(t, os.RemoveAll(physicalParent))
		require.NoError(t, os.MkdirAll(physicalParent, 0o755))
		require.NoError(t, os.Chown(physicalParent, 0, 0))
		require.NoError(t, os.Chmod(physicalParent, 0o777))

		err := env.RunAsUser(t, "nobody", "mkdir "+env.MountPath(relDir))
		require.NoError(t, err)
		require.DirExists(t, env.StoragePath("ssd1", relDir))
	})
}

// TestFUSE_Unlink_nonRoot_parentNoWrite_denied verifies unlink is rejected when parent directory is not writable.
func TestFUSE_Unlink_nonRoot_parentNoWrite_denied(t *testing.T) {
	withMountedFS(t, IntegrationConfig{
		AllowOther: true,
		RoutingRules: []config.RoutingRule{{
			Match:       "**",
			Targets:     []string{"ssd1"},
			ReadTargets: []string{"ssd1"},
		}},
	}, func(env *MountedFS) {
		relParent := "fuse-perms/unlink-no-write"
		relFile := relParent + "/x.txt"
		physicalParent := env.StoragePath("ssd1", relParent)
		require.NoError(t, os.RemoveAll(physicalParent))
		require.NoError(t, os.MkdirAll(physicalParent, 0o755))
		require.NoError(t, os.Chown(physicalParent, 0, 0))
		require.NoError(t, os.Chmod(physicalParent, 0o555))

		physicalFile := env.StoragePath("ssd1", relFile)
		require.NoError(t, os.WriteFile(physicalFile, []byte("x"), 0o644))
		require.NoError(t, os.Chown(physicalFile, 0, 0))

		err := env.RunAsUser(t, "nobody", "rm -f "+env.MountPath(relFile))
		require.Error(t, err)
		exitErr, ok := errors.AsType[*exec.ExitError](err)
		require.True(t, ok)
		require.NotEqual(t, 0, exitErr.ExitCode())
		require.FileExists(t, physicalFile)
	})
}

// TestFUSE_Rename_nonRoot_parentNoWrite_denied verifies rename is rejected when parent directory is not writable.
func TestFUSE_Rename_nonRoot_parentNoWrite_denied(t *testing.T) {
	withMountedFS(t, IntegrationConfig{
		AllowOther: true,
		RoutingRules: []config.RoutingRule{{
			Match:       "**",
			Targets:     []string{"ssd1"},
			ReadTargets: []string{"ssd1"},
		}},
	}, func(env *MountedFS) {
		relDir := "fuse-perms/rename-no-write"
		oldRel := relDir + "/old.txt"
		newRel := relDir + "/new.txt"

		physicalDir := env.StoragePath("ssd1", relDir)
		require.NoError(t, os.RemoveAll(physicalDir))
		require.NoError(t, os.MkdirAll(physicalDir, 0o755))
		require.NoError(t, os.Chown(physicalDir, 0, 0))
		require.NoError(t, os.Chmod(physicalDir, 0o555))

		physicalOld := env.StoragePath("ssd1", oldRel)
		require.NoError(t, os.WriteFile(physicalOld, []byte("x"), 0o644))
		require.NoError(t, os.Chown(physicalOld, 0, 0))

		err := env.RunAsUser(t, "nobody", "mv "+env.MountPath(oldRel)+" "+env.MountPath(newRel))
		require.Error(t, err)
		exitErr, ok := errors.AsType[*exec.ExitError](err)
		require.True(t, ok)
		require.NotEqual(t, 0, exitErr.ExitCode())
		require.FileExists(t, physicalOld)
	})
}

// TestFUSE_Link_nonRoot_inDirectory_noWrite_denied verifies hardlink is rejected when directory is not writable.
func TestFUSE_Link_nonRoot_inDirectory_noWrite_denied(t *testing.T) {
	withMountedFS(t, IntegrationConfig{
		AllowOther: true,
		RoutingRules: []config.RoutingRule{{
			Match:       "**",
			Targets:     []string{"ssd1"},
			ReadTargets: []string{"ssd1"},
		}},
	}, func(env *MountedFS) {
		relDir := "fuse-perms/link-no-write-2"
		relSrc := relDir + "/src.txt"
		relDst := relDir + "/dst.txt"

		physicalDir := env.StoragePath("ssd1", relDir)
		require.NoError(t, os.RemoveAll(physicalDir))
		require.NoError(t, os.MkdirAll(physicalDir, 0o755))
		require.NoError(t, os.Chown(physicalDir, 0, 0))
		require.NoError(t, os.Chmod(physicalDir, 0o555))

		physicalSrc := env.StoragePath("ssd1", relSrc)
		require.NoError(t, os.WriteFile(physicalSrc, []byte("x"), 0o644))

		err := env.RunAsUser(t, "nobody", "ln "+env.MountPath(relSrc)+" "+env.MountPath(relDst))
		require.Error(t, err)
		exitErr, ok := errors.AsType[*exec.ExitError](err)
		require.True(t, ok)
		require.NotEqual(t, 0, exitErr.ExitCode())
		require.NoFileExists(t, env.StoragePath("ssd1", relDst))
	})
}

// TestFUSE_Rmdir_nonRoot_stickyDir_notOwner_denied verifies sticky-bit semantics reject rmdir by non-owner.
func TestFUSE_Rmdir_nonRoot_stickyDir_notOwner_denied(t *testing.T) {
	withMountedFS(t, IntegrationConfig{
		AllowOther: true,
		RoutingRules: []config.RoutingRule{{
			Match:       "**",
			Targets:     []string{"ssd1"},
			ReadTargets: []string{"ssd1"},
		}},
	}, func(env *MountedFS) {
		relParent := "fuse-perms/sticky-rmdir"
		relDir := relParent + "/d"

		physicalParent := env.StoragePath("ssd1", relParent)
		require.NoError(t, os.RemoveAll(physicalParent))
		require.NoError(t, os.MkdirAll(physicalParent, 0o755))
		require.NoError(t, os.Chown(physicalParent, 0, 0))
		require.NoError(t, syscall.Chmod(physicalParent, 0o1777))

		physicalDir := env.StoragePath("ssd1", relDir)
		require.NoError(t, os.MkdirAll(physicalDir, 0o755))
		require.NoError(t, os.Chown(physicalDir, 0, 0))

		// Sanity: sticky bit must be present on the physical parent dir.
		pst := env.MustStatT(t, physicalParent)
		require.NotEqual(t, uint32(0), uint32(pst.Mode)&uint32(syscall.S_ISVTX))

		err := env.RunAsUser(t, "nobody", "rmdir "+env.MountPath(relDir))
		require.Error(t, err)
		exitErr, ok := errors.AsType[*exec.ExitError](err)
		require.True(t, ok)
		require.NotEqual(t, 0, exitErr.ExitCode())
		require.DirExists(t, physicalDir)
	})
}

// TestFUSE_permissions_sanity ensures our tests run with allow_other enabled.
func TestFUSE_permissions_sanity(t *testing.T) {
	withMountedFS(t, IntegrationConfig{AllowOther: true}, func(env *MountedFS) {
		// Sanity: try to list root as nobody.
		err := env.RunAsUser(t, "nobody", "ls -la "+env.MountPoint)
		require.NoError(t, err)
	})
}

// TestFUSE_permissions_note documents expected errno mapping for the integration harness.
func TestFUSE_permissions_note(t *testing.T) {
	// We don't assert exact errno here because userland tools (rm/mv/mkdir)
	// may map EACCES/EPERM differently by platform and flags.
	// The core invariant is: the operation must fail and no physical mutation occurs.
	_ = syscall.EACCES
	_ = syscall.EPERM
}

// TestFUSE_supplementaryGroup verifies shared-directory mutations use the caller's supplementary groups.
// The caller (nobody) gets access to root-owned directories only through group 1001.
func TestFUSE_supplementaryGroup(t *testing.T) {
	withMountedFS(t, IntegrationConfig{
		AllowOther: true,
		RoutingRules: []config.RoutingRule{
			{Match: "group-denied/**", ReadTargets: []string{"ssd1", "ssd2"}, WriteTargets: []string{"ssd2"}},
			{Match: "**", Targets: []string{"ssd1"}},
		},
	}, func(env *MountedFS) {
		const group = 1001
		// mustGroupDir creates a root-owned directory with the given mode and group.
		mustGroupDir := func(storage, rel string, mode os.FileMode) {
			dir := env.StoragePath(storage, rel)
			require.NoError(t, os.Mkdir(dir, mode))
			require.NoError(t, os.Chown(dir, 0, group))
			require.NoError(t, os.Chmod(dir, mode))
		}
		// runAsNobody runs the command with the supplied supplementary groups.
		runAsNobody := func(script string, groups ...uint32) ([]byte, error) {
			cmd := exec.Command("sh", "-c", script)
			cmd.SysProcAttr = &syscall.SysProcAttr{Credential: &syscall.Credential{Uid: 65534, Gid: 65534, Groups: groups}}
			return cmd.CombinedOutput()
		}

		mustGroupDir("ssd1", "shared", 0o770)
		env.MustCreateFileInStoragePath(t, []byte("x"), "ssd1", "shared/unlink-me")
		env.MustCreateFileInStoragePath(t, []byte("x"), "ssd1", "shared/rename-me")
		mustGroupDir("ssd1", "shared/rmdir-me", 0o770)

		tests := []struct {
			name    string
			script  string
			created string
			removed string
		}{
			{name: "create a file", script: "touch '%s/file'", created: "shared/file"},
			{name: "create a directory", script: "mkdir '%s/dir'", created: "shared/dir"},
			{name: "create a symlink", script: "ln -s target '%s/symlink'", created: "shared/symlink"},
			{name: "create a FIFO", script: "mkfifo '%s/fifo'", created: "shared/fifo"},
			{name: "unlink a file", script: "rm -f '%s/unlink-me'", removed: "shared/unlink-me"},
			{name: "remove a directory", script: "rmdir '%s/rmdir-me'", removed: "shared/rmdir-me"},
			{name: "rename a file", script: "mv '%[1]s/rename-me' '%[1]s/renamed'", created: "shared/renamed", removed: "shared/rename-me"},
		}
		for _, tc := range tests {
			t.Run("should "+tc.name+" using supplementary group access", func(t *testing.T) {
				output, err := runAsNobody(fmt.Sprintf(tc.script, env.MountPath("shared")), group)
				require.NoError(t, err, "%s", output)
				if tc.created != "" {
					_, err := os.Lstat(env.StoragePath("ssd1", tc.created))
					require.NoError(t, err)
				}
				if tc.removed != "" {
					_, err := os.Lstat(env.StoragePath("ssd1", tc.removed))
					require.ErrorIs(t, err, os.ErrNotExist)
				}
			})
		}

		t.Run("should deny a group member when group bits deny even if other bits allow", func(t *testing.T) {
			// The kernel sees the readable parent on ssd1. Only the daemon checks ssd2.
			const dir = "group-denied"
			mustGroupDir("ssd1", dir, 0o777)
			mustGroupDir("ssd2", dir, 0o777)
			parent := env.MustLstatInMountPoint(t, dir)
			require.Equal(t, os.FileMode(0o777), parent.Mode().Perm())

			output, err := runAsNobody(fmt.Sprintf("touch '%s/control'", env.MountPath(dir)), group)
			require.NoError(t, err, "%s", output)
			require.FileExists(t, env.StoragePath("ssd2", dir+"/control"))

			require.NoError(t, os.Chmod(env.StoragePath("ssd2", dir), 0o707))
			output, err = runAsNobody(fmt.Sprintf("touch '%s/nonmember'", env.MountPath(dir)))
			require.NoError(t, err, "%s", output)
			require.FileExists(t, env.StoragePath("ssd2", dir+"/nonmember"))

			output, err = runAsNobody(fmt.Sprintf("touch '%s/file'", env.MountPath(dir)), group)
			require.Error(t, err, "%s", output)
			_, err = os.Lstat(env.StoragePath("ssd2", dir+"/file"))
			require.ErrorIs(t, err, os.ErrNotExist)
		})
	})
}
