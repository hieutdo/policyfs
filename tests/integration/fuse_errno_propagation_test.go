//go:build integration && linux

package integration

import (
	"os"
	"path/filepath"
	"strings"
	"syscall"
	"testing"

	"github.com/hieutdo/policyfs/internal/config"
	"github.com/stretchr/testify/require"
)

// TestFUSE_Mkdir_shouldPropagateWrappedErrno verifies that mkdir returns the correct errno
// even when the underlying failure is wrapped by internal helper errors.
//
// We use ENAMETOOLONG (triggered by a very long physical path on the write target) rather than
// EACCES because integration tests typically run as root, which bypasses permission checks.
// ENAMETOOLONG exercises the same toErrno unwrap path through materializeParentDirs → fmt.Errorf(%w).
func TestFUSE_Mkdir_shouldPropagateWrappedErrno(t *testing.T) {
	withMountedFS(t, IntegrationConfig{
		Storages: []IntegrationStorage{
			{ID: "ssd1", Indexed: false, BasePath: makeLongBasePath("/mnt/ssd1/pfs-integration-long", "base", 30, 50)},
			{ID: "ssd2", Indexed: false, BasePath: "/mnt/ssd2/pfs-integration"},
		},
		RoutingRules: []config.RoutingRule{{
			Match:        "**",
			Targets:      []string{"ssd1"},
			ReadTargets:  []string{"ssd2", "ssd1"},
			WriteTargets: []string{"ssd1"},
		}},
	}, func(env *MountedFS) {
		const pathMax = 4096
		mountPrefix := len(env.MountPoint) + 1
		ssd1Prefix := len(env.StorageRoot("ssd1")) + 1
		ssd2Prefix := len(env.StorageRoot("ssd2")) + 1

		// The parent dirs must exist in the mount view for the kernel to issue Mkdir on the child.
		// We create the deep dir tree on ssd2 (short base path) so it is visible, while writes route to
		// ssd1 (very long base path) so parent dir materialization fails with ENAMETOOLONG.
		relLen := pathMax - mountPrefix - 256
		if max2 := pathMax - ssd2Prefix - 256; max2 < relLen {
			relLen = max2
		}
		if relLen <= 0 {
			t.Logf("relLen=%d, mountPrefix=%d, ssd2Prefix=%d", relLen, mountPrefix, ssd2Prefix)
			t.Skip("paths too long to safely trigger ENAMETOOLONG")
		}
		deepDir := makeDeepRelativePath(relLen, "deep", 50)
		if ssd1Prefix+len(deepDir) <= pathMax+32 {
			t.Logf("ssd1 physical len=%d, pathMax=%d", ssd1Prefix+len(deepDir), pathMax)
			t.Skip("ssd1 root path not long enough to force physical ENAMETOOLONG")
		}
		require.NoError(t, os.MkdirAll(env.StoragePath("ssd2", deepDir), 0o755))
		_, err := os.Stat(env.MountPath(deepDir))
		require.NoError(t, err)

		child := filepath.Join(deepDir, "child")
		err = os.Mkdir(env.MountPath(child), 0o755)
		require.Error(t, err)
		require.ErrorIs(t, err, syscall.ENAMETOOLONG)
	})
}

// TestFUSE_Create_shouldPropagateWrappedErrno verifies that create/touch returns the correct errno
// even when the underlying failure is wrapped by internal helper errors.
// See TestFUSE_Mkdir_shouldPropagateWrappedErrno for rationale on using ENAMETOOLONG.
func TestFUSE_Create_shouldPropagateWrappedErrno(t *testing.T) {
	withMountedFS(t, IntegrationConfig{
		Storages: []IntegrationStorage{
			{ID: "ssd1", Indexed: false, BasePath: makeLongBasePath("/mnt/ssd1/pfs-integration-long", "base", 30, 50)},
			{ID: "ssd2", Indexed: false, BasePath: "/mnt/ssd2/pfs-integration"},
		},
		RoutingRules: []config.RoutingRule{{
			Match:        "**",
			Targets:      []string{"ssd1"},
			ReadTargets:  []string{"ssd2", "ssd1"},
			WriteTargets: []string{"ssd1"},
		}},
	}, func(env *MountedFS) {
		const pathMax = 4096
		mountPrefix := len(env.MountPoint) + 1
		ssd1Prefix := len(env.StorageRoot("ssd1")) + 1
		ssd2Prefix := len(env.StorageRoot("ssd2")) + 1

		relLen := pathMax - mountPrefix - 256
		if max2 := pathMax - ssd2Prefix - 256; max2 < relLen {
			relLen = max2
		}
		if relLen <= 0 {
			t.Logf("relLen=%d, mountPrefix=%d, ssd2Prefix=%d", relLen, mountPrefix, ssd2Prefix)
			t.Skip("paths too long to safely trigger ENAMETOOLONG")
		}
		deepDir := makeDeepRelativePath(relLen, "deep", 50)
		if ssd1Prefix+len(deepDir) <= pathMax+32 {
			t.Logf("ssd1 physical len=%d, pathMax=%d", ssd1Prefix+len(deepDir), pathMax)
			t.Skip("ssd1 root path not long enough to force physical ENAMETOOLONG")
		}
		require.NoError(t, os.MkdirAll(env.StoragePath("ssd2", deepDir), 0o755))
		_, err := os.Stat(env.MountPath(deepDir))
		require.NoError(t, err)

		relFile := filepath.Join(deepDir, "x.txt")
		f, err := os.OpenFile(env.MountPath(relFile), os.O_CREATE|os.O_WRONLY, 0o644)
		if f != nil {
			_ = f.Close()
		}
		require.Error(t, err)
		require.ErrorIs(t, err, syscall.ENAMETOOLONG)
	})
}

// makeLongBasePath builds a long base path consisting of nested directories.
func makeLongBasePath(root string, prefix string, segments int, segmentLen int) string {
	seg := prefix + strings.Repeat("x", segmentLen)
	p := root
	for i := 0; i < segments; i++ {
		p = filepath.Join(p, seg)
	}
	return p
}

// makeDeepRelativePath builds a deep relative path with many segments and a minimum string length.
func makeDeepRelativePath(minLen int, prefix string, segmentLen int) string {
	seg := prefix + strings.Repeat("y", segmentLen)
	n := (minLen / (len(seg) + 1)) + 1
	parts := make([]string, n)
	for i := range parts {
		parts[i] = seg
	}
	return strings.Join(parts, "/")
}
