//go:build integration

package integration

import (
	"context"
	"fmt"
	"os"
	"os/exec"
	"path/filepath"
	"sort"
	"strings"
	"sync"
	"syscall"
	"testing"
	"time"

	"github.com/hieutdo/policyfs/internal/config"
	"github.com/stretchr/testify/require"
)

const nestedMountChildRel = "anime"

// nestedMountName returns a short, stable mount name that avoids per-mount lock collisions.
func nestedMountName(testName string, mountLabel string) string {
	return fmt.Sprintf("it%s_%s", runtimeID(testName), mountLabel)
}

// Stop terminates the daemon backing this mount and waits for the mountpoint to be
// unmounted. Subsequent calls are no-ops, and the harness t.Cleanup will skip its
// signal/wait/unmount phase. Use for tests that need to unmount one daemon mid-test
// (e.g. nested-mount lifecycle scenarios).
func (m *MountedFS) Stop(t testing.TB) {
	t.Helper()
	if m == nil || m.stopped {
		return
	}
	m.stopped = true
	if m.mountProc != nil {
		_ = m.mountProc.Signal(syscall.SIGTERM)
		select {
		case <-m.mountDone:
		case <-time.After(5 * time.Second):
			if m.mountCancel != nil {
				m.mountCancel()
			}
			_ = m.mountProc.Kill()
			<-m.mountDone
		}
	}
	if m.mountCancel != nil {
		m.mountCancel()
	}
	if err := ensureUnmounted(m.MountPoint, 2*time.Second); err != nil {
		t.Fatalf("failed to unmount: %v", err)
	}
}

// withNestedMountedFS starts a parent PolicyFS mount and a child PolicyFS mount nested under it.
func withNestedMountedFS(t *testing.T, parentCfg IntegrationConfig, childCfg IntegrationConfig, prepareParent func(parent *MountedFS), fn func(parent *MountedFS, child *MountedFS)) {
	t.Helper()
	if strings.TrimSpace(os.Getenv(config.EnvIntegrationUseExistingMount)) != "" {
		t.Skip("skip nested mount tests when using an existing mount")
	}

	testName := sanitizeName(t.Name())
	nestedRoot := filepath.Join(mountBase, testName)
	parentMountPoint := filepath.Join(nestedRoot, "media")
	childMountPoint := filepath.Join(parentMountPoint, nestedMountChildRel)

	t.Cleanup(func() {
		if t.Failed() {
			return
		}
		if strings.TrimSpace(os.Getenv(config.EnvIntegrationKeepArtifacts)) != "" {
			return
		}
		_ = os.RemoveAll(nestedRoot)
	})

	parent := startNamedMountedFS(t, testName, "parent", nestedMountName(testName, "parent"), parentMountPoint, parentCfg, false)
	parent.MustMkdirInMountPoint(t, nestedMountChildRel)
	if prepareParent != nil {
		prepareParent(parent)
	}

	child := startNamedMountedFS(t, testName, "child", nestedMountName(testName, "child"), childMountPoint, childCfg, true)
	fn(parent, child)
}

// startNamedMountedFS mounts one PolicyFS daemon with a caller-provided mount name and mountpoint.
func startNamedMountedFS(t *testing.T, testName string, mountLabel string, mountName string, mountPoint string, cfg IntegrationConfig, preserveMountpointContents bool) *MountedFS {
	t.Helper()

	storages := effectiveStorages(cfg)
	storageRoots, err := localStorageRoots(storages, testName+"-"+mountLabel)
	if err != nil {
		t.Fatalf("failed to create storage roots: %v", err)
	}

	rid := runtimeID(testName + "-" + mountLabel)
	runtimeDir := filepath.Join(tmpDir, "run-"+rid)
	_ = os.RemoveAll(runtimeDir)
	if err := os.MkdirAll(runtimeDir, 0o755); err != nil {
		t.Fatalf("failed to ensure runtime dir: %v", err)
	}
	stateDir := filepath.Join(tmpDir, "state-"+rid)
	_ = os.RemoveAll(stateDir)
	if err := os.MkdirAll(stateDir, 0o755); err != nil {
		t.Fatalf("failed to ensure state dir: %v", err)
	}

	env := &MountedFS{
		MountName:    mountName,
		MountPoint:   mountPoint,
		ConfigPath:   filepath.Join(tmpDir, testName+"-"+mountLabel+".yaml"),
		RuntimeDir:   runtimeDir,
		StateDir:     stateDir,
		StorageRoots: storageRoots,
	}

	if err := ensureUnmounted(env.MountPoint, 2*time.Second); err != nil {
		t.Fatalf("failed to ensure unmounted: %v", err)
	}
	if !preserveMountpointContents {
		_ = os.RemoveAll(env.MountPoint)
	}
	if err := ensureMountpointDir(env.MountPoint); err != nil {
		t.Fatalf("failed to ensure mountpoint: %v", err)
	}
	if err := writeIntegrationConfig(env.ConfigPath, env.MountName, env.MountPoint, storages, env.StorageRoots, cfg); err != nil {
		t.Fatalf("failed to write integration config: %v", err)
	}

	ctx, cancel := context.WithCancel(context.Background())
	args := []string{"--config", env.ConfigPath, "mount", env.MountName}
	args = append(args, cfg.MountArgs...)
	logFile := filepath.Join(tmpDir, testName+"-"+mountLabel+".log")
	mountCmd := exec.CommandContext(ctx, pfsBin, args...)
	mountCmd.Env = pfsTestEnv(env, logFile)
	mountCmd.Stdout = os.Stdout
	mountCmd.Stderr = os.Stderr
	if err := mountCmd.Start(); err != nil {
		t.Fatalf("failed to start pfs mount: %v", err)
	}

	mountDone := make(chan struct{})
	go func() {
		_, _ = mountCmd.Process.Wait()
		close(mountDone)
	}()
	env.mountProc = mountCmd.Process
	env.mountCancel = cancel
	env.mountDone = mountDone

	if err := waitForMount(env.MountPoint, 5*time.Second); err != nil {
		_ = mountCmd.Process.Signal(syscall.SIGTERM)
		<-mountDone
		cancel()
		t.Fatalf("mount did not become ready: %v", err)
	}

	t.Cleanup(func() {
		if !env.stopped {
			_ = mountCmd.Process.Signal(syscall.SIGTERM)
			select {
			case <-mountDone:
			case <-time.After(5 * time.Second):
				cancel()
				_ = mountCmd.Process.Kill()
				<-mountDone
			}
			cancel()
			if err := ensureUnmounted(env.MountPoint, 2*time.Second); err != nil {
				t.Fatalf("failed to unmount: %v", err)
			}
		}

		if t.Failed() {
			return
		}
		if strings.TrimSpace(os.Getenv(config.EnvIntegrationKeepArtifacts)) != "" {
			return
		}

		for _, root := range env.StorageRoots {
			_ = os.RemoveAll(root)
		}
		if !preserveMountpointContents {
			_ = os.RemoveAll(env.MountPoint)
		}
		_ = os.Remove(env.ConfigPath)
		_ = os.RemoveAll(env.RuntimeDir)
		_ = os.RemoveAll(env.StateDir)
		_ = os.Remove(logFile)
	})

	return env
}

// TestNestedMount_shouldStartParentAndChildDaemons verifies that two PolicyFS daemons can run
// concurrently when the child mountpoint is nested under the parent mountpoint.
func TestNestedMount_shouldStartParentAndChildDaemons(t *testing.T) {
	parentCfg := IntegrationConfig{
		Storages: []IntegrationStorage{{ID: "ssd1", BasePath: "/mnt/ssd1/pfs-integration"}},
		Targets:  []string{"ssd1"},
	}
	childCfg := IntegrationConfig{
		Storages: []IntegrationStorage{{ID: "ssd2", BasePath: "/mnt/ssd2/pfs-integration"}},
		Targets:  []string{"ssd2"},
	}

	withNestedMountedFS(t, parentCfg, childCfg, nil, func(parent *MountedFS, child *MountedFS) {
		parent.MustWriteFileInMountPoint(t, "parent.txt", []byte("from-parent"))
		child.MustWriteFileInMountPoint(t, "child.txt", []byte("from-child"))

		require.Equal(t, []byte("from-parent"), parent.MustReadFileInMountPoint(t, "parent.txt"))
		require.Equal(t, []byte("from-child"), child.MustReadFileInMountPoint(t, "child.txt"))

		childInfo := parent.MustLstatInMountPoint(t, nestedMountChildRel)
		require.True(t, childInfo.IsDir())
	})
}

// TestNestedMount_shouldRouteChildSubtreeThroughChildMount verifies that paths below the nested
// child mountpoint resolve through the child mount, while sibling paths still resolve through the parent.
func TestNestedMount_shouldRouteChildSubtreeThroughChildMount(t *testing.T) {
	parentCfg := IntegrationConfig{
		Storages: []IntegrationStorage{{ID: "ssd1", BasePath: "/mnt/ssd1/pfs-integration"}},
		Targets:  []string{"ssd1"},
	}
	childCfg := IntegrationConfig{
		Storages: []IntegrationStorage{{ID: "ssd2", BasePath: "/mnt/ssd2/pfs-integration"}},
		Targets:  []string{"ssd2"},
	}

	withNestedMountedFS(t, parentCfg, childCfg, func(parent *MountedFS) {
		parent.MustWriteFileInMountPoint(t, filepath.Join(nestedMountChildRel, "shared.txt"), []byte("from-parent"))
		parent.MustWriteFileInMountPoint(t, "sibling.txt", []byte("parent-sibling"))
	}, func(parent *MountedFS, child *MountedFS) {
		child.MustWriteFileInMountPoint(t, "shared.txt", []byte("from-child"))
		child.MustWriteFileInMountPoint(t, "child-only.txt", []byte("child-only"))

		gotShared := parent.MustReadFileInMountPoint(t, filepath.Join(nestedMountChildRel, "shared.txt"))
		require.Equal(t, []byte("from-child"), gotShared)

		gotChildOnly := parent.MustReadFileInMountPoint(t, filepath.Join(nestedMountChildRel, "child-only.txt"))
		require.Equal(t, []byte("child-only"), gotChildOnly)

		gotSibling := parent.MustReadFileInMountPoint(t, "sibling.txt")
		require.Equal(t, []byte("parent-sibling"), gotSibling)

		require.Equal(t, []byte("from-parent"), parent.MustReadFileInStoragePath(t, "ssd1", filepath.Join(nestedMountChildRel, "shared.txt")))
		require.Equal(t, []byte("from-child"), child.MustReadFileInStoragePath(t, "ssd2", "shared.txt"))
	})
}

// TestNestedMount_shouldReturnEXDEVOnRenameAcrossBoundary verifies that renames whose
// source and destination straddle the child mount boundary return EXDEV (POSIX cross-device).
func TestNestedMount_shouldReturnEXDEVOnRenameAcrossBoundary(t *testing.T) {
	parentCfg := IntegrationConfig{
		Storages: []IntegrationStorage{{ID: "ssd1", BasePath: "/mnt/ssd1/pfs-integration"}},
		Targets:  []string{"ssd1"},
	}
	childCfg := IntegrationConfig{
		Storages: []IntegrationStorage{{ID: "ssd2", BasePath: "/mnt/ssd2/pfs-integration"}},
		Targets:  []string{"ssd2"},
	}

	withNestedMountedFS(t, parentCfg, childCfg, func(parent *MountedFS) {
		parent.MustWriteFileInMountPoint(t, "sibling.txt", []byte("p"))
	}, func(parent *MountedFS, child *MountedFS) {
		child.MustWriteFileInMountPoint(t, "local.txt", []byte("c"))

		errIn := os.Rename(parent.MountPath("sibling.txt"), parent.MountPath(filepath.Join(nestedMountChildRel, "moved.txt")))
		require.ErrorIs(t, errIn, syscall.EXDEV)

		errOut := os.Rename(parent.MountPath(filepath.Join(nestedMountChildRel, "local.txt")), parent.MountPath("lifted.txt"))
		require.ErrorIs(t, errOut, syscall.EXDEV)

		require.True(t, parent.FileExistsInStoragePath("ssd1", "sibling.txt"))
		require.True(t, child.FileExistsInStoragePath("ssd2", "local.txt"))
		require.False(t, parent.FileExistsInStoragePath("ssd1", "lifted.txt"))
		require.False(t, child.FileExistsInStoragePath("ssd2", "moved.txt"))
	})
}

// TestNestedMount_shouldReturnEXDEVOnHardlinkAcrossBoundary verifies that hardlink(2)
// across the child mount boundary returns EXDEV.
func TestNestedMount_shouldReturnEXDEVOnHardlinkAcrossBoundary(t *testing.T) {
	parentCfg := IntegrationConfig{
		Storages: []IntegrationStorage{{ID: "ssd1", BasePath: "/mnt/ssd1/pfs-integration"}},
		Targets:  []string{"ssd1"},
	}
	childCfg := IntegrationConfig{
		Storages: []IntegrationStorage{{ID: "ssd2", BasePath: "/mnt/ssd2/pfs-integration"}},
		Targets:  []string{"ssd2"},
	}

	withNestedMountedFS(t, parentCfg, childCfg, func(parent *MountedFS) {
		parent.MustWriteFileInMountPoint(t, "src.txt", []byte("p"))
	}, func(parent *MountedFS, child *MountedFS) {
		child.MustWriteFileInMountPoint(t, "child-src.txt", []byte("c"))

		errIn := os.Link(parent.MountPath("src.txt"), parent.MountPath(filepath.Join(nestedMountChildRel, "linked.txt")))
		require.ErrorIs(t, errIn, syscall.EXDEV)

		errOut := os.Link(parent.MountPath(filepath.Join(nestedMountChildRel, "child-src.txt")), parent.MountPath("linked-out.txt"))
		require.ErrorIs(t, errOut, syscall.EXDEV)

		require.False(t, parent.FileExistsInStoragePath("ssd1", filepath.Join(nestedMountChildRel, "linked.txt")))
		require.False(t, child.FileExistsInStoragePath("ssd2", "linked.txt"))
		require.False(t, parent.FileExistsInStoragePath("ssd1", "linked-out.txt"))
	})
}

// TestNestedMount_shouldListOnlyChildContentAtChildMountpoint verifies that readdir at
// the child mountpoint returns only child content; parent-storage entries at the same
// relpath are shadowed (preserved on disk but not visible while child is mounted).
func TestNestedMount_shouldListOnlyChildContentAtChildMountpoint(t *testing.T) {
	parentCfg := IntegrationConfig{
		Storages: []IntegrationStorage{{ID: "ssd1", BasePath: "/mnt/ssd1/pfs-integration"}},
		Targets:  []string{"ssd1"},
	}
	childCfg := IntegrationConfig{
		Storages: []IntegrationStorage{{ID: "ssd2", BasePath: "/mnt/ssd2/pfs-integration"}},
		Targets:  []string{"ssd2"},
	}

	withNestedMountedFS(t, parentCfg, childCfg, func(parent *MountedFS) {
		parent.MustWriteFileInMountPoint(t, filepath.Join(nestedMountChildRel, "hidden_a.txt"), []byte("a"))
		parent.MustWriteFileInMountPoint(t, filepath.Join(nestedMountChildRel, "hidden_b.txt"), []byte("b"))
		parent.MustWriteFileInMountPoint(t, filepath.Join(nestedMountChildRel, "hidden_c.txt"), []byte("c"))
	}, func(parent *MountedFS, child *MountedFS) {
		child.MustWriteFileInMountPoint(t, "visible_x.txt", []byte("x"))
		child.MustWriteFileInMountPoint(t, "visible_y.txt", []byte("y"))

		viaParent := dirEntryNames(parent.MustReadDirInMountPoint(t, nestedMountChildRel))
		sort.Strings(viaParent)
		require.Equal(t, []string{"visible_x.txt", "visible_y.txt"}, viaParent)

		viaChild := dirEntryNames(child.MustReadDirInMountPoint(t, ""))
		sort.Strings(viaChild)
		require.Equal(t, []string{"visible_x.txt", "visible_y.txt"}, viaChild)

		require.True(t, parent.FileExistsInStoragePath("ssd1", filepath.Join(nestedMountChildRel, "hidden_a.txt")))
		require.True(t, parent.FileExistsInStoragePath("ssd1", filepath.Join(nestedMountChildRel, "hidden_b.txt")))
		require.True(t, parent.FileExistsInStoragePath("ssd1", filepath.Join(nestedMountChildRel, "hidden_c.txt")))
	})
}

// TestNestedMount_shouldRecoverShadowedContentAfterChildUnmount verifies that unmounting
// the child restores the parent's underlying view at the child-relpath, while the
// child's storage retains its data for a future remount.
func TestNestedMount_shouldRecoverShadowedContentAfterChildUnmount(t *testing.T) {
	parentCfg := IntegrationConfig{
		Storages: []IntegrationStorage{{ID: "ssd1", BasePath: "/mnt/ssd1/pfs-integration"}},
		Targets:  []string{"ssd1"},
	}
	childCfg := IntegrationConfig{
		Storages: []IntegrationStorage{{ID: "ssd2", BasePath: "/mnt/ssd2/pfs-integration"}},
		Targets:  []string{"ssd2"},
	}

	withNestedMountedFS(t, parentCfg, childCfg, func(parent *MountedFS) {
		parent.MustWriteFileInMountPoint(t, filepath.Join(nestedMountChildRel, "shared.txt"), []byte("A"))
	}, func(parent *MountedFS, child *MountedFS) {
		child.MustWriteFileInMountPoint(t, "shared.txt", []byte("B"))
		require.Equal(t, []byte("B"), parent.MustReadFileInMountPoint(t, filepath.Join(nestedMountChildRel, "shared.txt")))

		child.Stop(t)

		mounted, err := isMountpointMounted(child.MountPoint)
		require.NoError(t, err)
		require.False(t, mounted)

		require.Equal(t, []byte("A"), parent.MustReadFileInMountPoint(t, filepath.Join(nestedMountChildRel, "shared.txt")))
		require.Equal(t, []byte("A"), parent.MustReadFileInStoragePath(t, "ssd1", filepath.Join(nestedMountChildRel, "shared.txt")))
		require.Equal(t, []byte("B"), child.MustReadFileInStoragePath(t, "ssd2", "shared.txt"))
	})
}

// TestNestedMount_shouldReportIndependentStatfs verifies that parent and child mounts
// are reachable via distinct kernel mount devices - the strongest portable guarantee
// that statfs at each mountpoint is scoped to its own daemon. (FUSE fsid often defaults
// to zero, so we rely on the kernel-level Dev instead.)
func TestNestedMount_shouldReportIndependentStatfs(t *testing.T) {
	parentCfg := IntegrationConfig{
		Storages: []IntegrationStorage{{ID: "ssd1", BasePath: "/mnt/ssd1/pfs-integration"}},
		Targets:  []string{"ssd1"},
	}
	childCfg := IntegrationConfig{
		Storages: []IntegrationStorage{{ID: "ssd2", BasePath: "/mnt/ssd2/pfs-integration"}},
		Targets:  []string{"ssd2"},
	}

	withNestedMountedFS(t, parentCfg, childCfg, nil, func(parent *MountedFS, child *MountedFS) {
		pStat := mustStatfs(t, parent.MountPoint)
		cStat := mustStatfs(t, child.MountPoint)
		require.NotZero(t, pStat.Bsize)
		require.NotZero(t, cStat.Bsize)

		pDev := parent.MustStatT(t, parent.MountPoint).Dev
		cDev := parent.MustStatT(t, child.MountPoint).Dev
		require.NotEqual(t, pDev, cDev)
	})
}

// TestNestedMount_shouldExposeChildMountAttrsAtMountpointDir verifies that lstat of the
// child mountpoint via the parent returns the child mount's device, not the underlying
// parent-storage directory's device.
func TestNestedMount_shouldExposeChildMountAttrsAtMountpointDir(t *testing.T) {
	parentCfg := IntegrationConfig{
		Storages: []IntegrationStorage{{ID: "ssd1", BasePath: "/mnt/ssd1/pfs-integration"}},
		Targets:  []string{"ssd1"},
	}
	childCfg := IntegrationConfig{
		Storages: []IntegrationStorage{{ID: "ssd2", BasePath: "/mnt/ssd2/pfs-integration"}},
		Targets:  []string{"ssd2"},
	}

	var stUnderlying *syscall.Stat_t
	withNestedMountedFS(t, parentCfg, childCfg, func(parent *MountedFS) {
		parent.MustCreateFileInStoragePath(t, []byte("p"), "ssd1", filepath.Join(nestedMountChildRel, "marker.txt"))
		stUnderlying = parent.MustStatT(t, parent.StoragePath("ssd1", nestedMountChildRel))
	}, func(parent *MountedFS, child *MountedFS) {
		stMount := parent.MustStatT(t, parent.MountPath(nestedMountChildRel))
		require.NotEqual(t, stUnderlying.Dev, stMount.Dev)

		names := dirEntryNames(parent.MustReadDirInMountPoint(t, nestedMountChildRel))
		require.NotContains(t, names, "marker.txt")

		require.Equal(t, []byte("p"), parent.MustReadFileInStoragePath(t, "ssd1", filepath.Join(nestedMountChildRel, "marker.txt")))
	})
}

// TestNestedMount_shouldHandleConcurrentMutationsAcrossMounts verifies that simultaneous
// independent writes against parent and child mounts complete without error and produce
// disjoint storage outputs.
func TestNestedMount_shouldHandleConcurrentMutationsAcrossMounts(t *testing.T) {
	parentCfg := IntegrationConfig{
		Storages: []IntegrationStorage{{ID: "ssd1", BasePath: "/mnt/ssd1/pfs-integration"}},
		Targets:  []string{"ssd1"},
	}
	childCfg := IntegrationConfig{
		Storages: []IntegrationStorage{{ID: "ssd2", BasePath: "/mnt/ssd2/pfs-integration"}},
		Targets:  []string{"ssd2"},
	}

	withNestedMountedFS(t, parentCfg, childCfg, nil, func(parent *MountedFS, child *MountedFS) {
		const n = 50
		parent.MustMkdirInMountPoint(t, "conc")
		child.MustMkdirInMountPoint(t, "conc")

		var wg sync.WaitGroup
		errCh := make(chan error, 2)
		wg.Add(2)
		go func() {
			defer wg.Done()
			for i := 0; i < n; i++ {
				rel := filepath.Join("conc", fmt.Sprintf("parent-%d.txt", i))
				if err := parent.WriteFileInMountPoint(rel, []byte(fmt.Sprintf("p%d", i)), 0o644); err != nil {
					errCh <- err
					return
				}
			}
			errCh <- nil
		}()
		go func() {
			defer wg.Done()
			for i := 0; i < n; i++ {
				rel := filepath.Join("conc", fmt.Sprintf("child-%d.txt", i))
				if err := child.WriteFileInMountPoint(rel, []byte(fmt.Sprintf("c%d", i)), 0o644); err != nil {
					errCh <- err
					return
				}
			}
			errCh <- nil
		}()
		wg.Wait()
		for i := 0; i < 2; i++ {
			require.NoError(t, <-errCh)
		}

		for i := 0; i < n; i++ {
			rel := filepath.Join("conc", fmt.Sprintf("parent-%d.txt", i))
			require.Equal(t, []byte(fmt.Sprintf("p%d", i)), parent.MustReadFileInStoragePath(t, "ssd1", rel))
			require.False(t, child.FileExistsInStoragePath("ssd2", rel))
		}
		for i := 0; i < n; i++ {
			rel := filepath.Join("conc", fmt.Sprintf("child-%d.txt", i))
			require.Equal(t, []byte(fmt.Sprintf("c%d", i)), child.MustReadFileInStoragePath(t, "ssd2", rel))
			require.False(t, parent.FileExistsInStoragePath("ssd1", rel))
		}

		require.Len(t, dirEntryNames(parent.MustReadDirInMountPoint(t, "conc")), n)
		require.Len(t, dirEntryNames(child.MustReadDirInMountPoint(t, "conc")), n)
	})
}

// TestNestedMount_shouldKeepParentHealthyAfterChildUnmount verifies that unmounting the
// child leaves the parent fully operational and that anime/ reverts to a regular parent
// directory backed by parent's storage.
func TestNestedMount_shouldKeepParentHealthyAfterChildUnmount(t *testing.T) {
	parentCfg := IntegrationConfig{
		Storages: []IntegrationStorage{{ID: "ssd1", BasePath: "/mnt/ssd1/pfs-integration"}},
		Targets:  []string{"ssd1"},
	}
	childCfg := IntegrationConfig{
		Storages: []IntegrationStorage{{ID: "ssd2", BasePath: "/mnt/ssd2/pfs-integration"}},
		Targets:  []string{"ssd2"},
	}

	withNestedMountedFS(t, parentCfg, childCfg, func(parent *MountedFS) {
		parent.MustWriteFileInMountPoint(t, "before.txt", []byte("before"))
	}, func(parent *MountedFS, child *MountedFS) {
		child.MustWriteFileInMountPoint(t, "nested.txt", []byte("nested"))

		child.Stop(t)

		mounted, err := isMountpointMounted(child.MountPoint)
		require.NoError(t, err)
		require.False(t, mounted)

		require.Equal(t, []byte("before"), parent.MustReadFileInMountPoint(t, "before.txt"))

		parent.MustWriteFileInMountPoint(t, "after.txt", []byte("after"))
		require.Equal(t, []byte("after"), parent.MustReadFileInMountPoint(t, "after.txt"))
		require.True(t, parent.FileExistsInStoragePath("ssd1", "after.txt"))

		parent.MustWriteFileInMountPoint(t, filepath.Join(nestedMountChildRel, "post-unmount.txt"), []byte("post"))
		require.Equal(t, []byte("post"), parent.MustReadFileInMountPoint(t, filepath.Join(nestedMountChildRel, "post-unmount.txt")))
		require.True(t, parent.FileExistsInStoragePath("ssd1", filepath.Join(nestedMountChildRel, "post-unmount.txt")))

		require.True(t, child.FileExistsInStoragePath("ssd2", "nested.txt"))
	})
}
