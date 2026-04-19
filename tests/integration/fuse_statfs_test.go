//go:build integration

package integration

import (
	"os"
	"syscall"
	"testing"

	"github.com/hieutdo/policyfs/internal/config"
	"github.com/stretchr/testify/require"
)

// statfsDelta is the maximum block count difference allowed between two consecutive
// syscall.Statfs calls on the same or pooled paths. Generous to absorb CI noise.
const statfsDelta = uint64(2048)

// mustStatfs calls syscall.Statfs on path and fails the test on error.
func mustStatfs(t *testing.T, path string) syscall.Statfs_t {
	t.Helper()
	var st syscall.Statfs_t
	require.NoError(t, syscall.Statfs(path, &st))
	return st
}

func blocksDiff(a, b uint64) uint64 {
	if a > b {
		return a - b
	}
	return b - a
}

// TestStatfs_mountPooledTargets_shouldPoolUnionAcrossRules verifies mount_pooled_targets
// pools the union of write targets across all routing rules.
func TestStatfs_mountPooledTargets_shouldPoolUnionAcrossRules(t *testing.T) {
	withMountedFS(t, IntegrationConfig{
		// Statfs: nil → defaults to mount_pooled_targets + ignore_failed.
		RoutingRules: []config.RoutingRule{
			{
				Match:        "library/**",
				ReadTargets:  []string{"ssd2", "ssd1"},
				WriteTargets: []string{"ssd2"},
			},
			{
				Match:        "**",
				ReadTargets:  []string{"ssd1", "ssd2"},
				WriteTargets: []string{"ssd1"},
			},
		},
	}, func(env *MountedFS) {
		ssd2 := mustStatfs(t, env.StorageRoot("ssd2"))
		ssd1 := mustStatfs(t, env.StorageRoot("ssd1"))

		unit := uint64(ssd2.Bsize)
		if unit == 0 {
			unit = 4096
		}
		wantBlocks := (ssd2.Blocks*uint64(ssd2.Bsize) + ssd1.Blocks*uint64(ssd1.Bsize)) / unit

		got := mustStatfs(t, env.MountPoint)
		require.LessOrEqual(t, blocksDiff(got.Blocks, wantBlocks), statfsDelta,
			"mount_pooled_targets: Blocks want≈%d got=%d", wantBlocks, got.Blocks)
	})
}

// TestStatfs_pathPooledTargets_shouldRespectQueriedPath verifies path_pooled_targets
// uses the write targets resolved for the specific path being queried.
func TestStatfs_pathPooledTargets_shouldRespectQueriedPath(t *testing.T) {
	withMountedFS(t, IntegrationConfig{
		Statfs: &config.StatfsConfig{Reporting: "path_pooled_targets", OnError: "ignore_failed"},
		RoutingRules: []config.RoutingRule{
			{
				Match:        "library/**",
				ReadTargets:  []string{"ssd2", "ssd1"},
				WriteTargets: []string{"ssd2"},
			},
			{
				Match:        "**",
				ReadTargets:  []string{"ssd1", "ssd2"},
				WriteTargets: []string{"ssd1"},
			},
		},
	}, func(env *MountedFS) {
		// Ensure the directory exists so syscall.Statfs can be called on it.
		env.MustCreateDirInStoragePath(t, "ssd2", "library")

		ssd1 := mustStatfs(t, env.StorageRoot("ssd1"))
		ssd2 := mustStatfs(t, env.StorageRoot("ssd2"))

		gotRoot := mustStatfs(t, env.MountPoint)
		require.LessOrEqual(t, blocksDiff(gotRoot.Blocks, ssd1.Blocks), statfsDelta,
			"path_pooled_targets(root): Blocks want≈%d got=%d", ssd1.Blocks, gotRoot.Blocks)

		gotLibrary := mustStatfs(t, env.MountPath("library"))
		require.LessOrEqual(t, blocksDiff(gotLibrary.Blocks, ssd2.Blocks), statfsDelta,
			"path_pooled_targets(library): Blocks want≈%d got=%d", ssd2.Blocks, gotLibrary.Blocks)
	})
}

// TestStatfs_onError_ignoreFailed verifies ignore_failed skips a failed target and
// returns partial pooled stats from the remaining accessible targets.
func TestStatfs_onError_ignoreFailed(t *testing.T) {
	withMountedFS(t, IntegrationConfig{
		Statfs: &config.StatfsConfig{Reporting: "mount_pooled_targets", OnError: "ignore_failed"},
		RoutingRules: []config.RoutingRule{{
			Match:        "**",
			WriteTargets: []string{"ssd1", "ssd2"},
			ReadTargets:  []string{"ssd1", "ssd2"},
		}},
	}, func(env *MountedFS) {
		ssd2 := mustStatfs(t, env.StorageRoot("ssd2"))

		// Make ssd1 inaccessible - rename so syscall.Statfs on it returns ENOENT.
		gone := env.StorageRoot("ssd1") + ".gone"
		require.NoError(t, os.Rename(env.StorageRoot("ssd1"), gone))
		t.Cleanup(func() { _ = os.Rename(gone, env.StorageRoot("ssd1")) })

		var got syscall.Statfs_t
		err := syscall.Statfs(env.MountPoint, &got)
		require.NoError(t, err, "ignore_failed should succeed despite one target failure")
		require.LessOrEqual(t, blocksDiff(got.Blocks, ssd2.Blocks), statfsDelta,
			"ignore_failed: should report only ssd2 blocks, want≈%d got=%d", ssd2.Blocks, got.Blocks)
	})
}

// TestStatfs_onError_failEIO verifies fail_eio causes statfs to return EIO when
// any write target's statfs call fails.
func TestStatfs_onError_failEIO(t *testing.T) {
	withMountedFS(t, IntegrationConfig{
		Statfs: &config.StatfsConfig{Reporting: "mount_pooled_targets", OnError: "fail_eio"},
		RoutingRules: []config.RoutingRule{{
			Match:        "**",
			WriteTargets: []string{"ssd1", "ssd2"},
			ReadTargets:  []string{"ssd1", "ssd2"},
		}},
	}, func(env *MountedFS) {
		// Make ssd1 inaccessible.
		gone := env.StorageRoot("ssd1") + ".gone"
		require.NoError(t, os.Rename(env.StorageRoot("ssd1"), gone))
		t.Cleanup(func() { _ = os.Rename(gone, env.StorageRoot("ssd1")) })

		var st syscall.Statfs_t
		err := syscall.Statfs(env.MountPoint, &st)
		require.Error(t, err, "fail_eio should return an error when any target fails")
		require.Equal(t, syscall.EIO, err, "fail_eio should return EIO, got: %v", err)
	})
}

// TestStatfs_onError_fallbackEffectiveTarget verifies fallback_effective_target falls back
// gracefully when pooled computation cannot complete.
//
// ssd1 is the primaryRootPath (loopback) - it must stay intact.
// ssd2 is the sole write target - we rename it to force pooling failure.
// The fallback tries the effective write target (ssd2, also gone) then falls back to
// the loopback (ssd1, still accessible). Either way, statfs must not error.
func TestStatfs_onError_fallbackEffectiveTarget(t *testing.T) {
	withMountedFS(t, IntegrationConfig{
		Statfs: &config.StatfsConfig{Reporting: "mount_pooled_targets", OnError: "fallback_effective_target"},
		// ssd1 is first storage → primaryRootPath (loopback). Keep it intact.
		// ssd2 is the only write target; removing it forces complete pooling failure.
		RoutingRules: []config.RoutingRule{{
			Match:        "**",
			WriteTargets: []string{"ssd2"},
			ReadTargets:  []string{"ssd1", "ssd2"},
		}},
	}, func(env *MountedFS) {
		gone := env.StorageRoot("ssd2") + ".gone"
		require.NoError(t, os.Rename(env.StorageRoot("ssd2"), gone))
		t.Cleanup(func() { _ = os.Rename(gone, env.StorageRoot("ssd2")) })

		var st syscall.Statfs_t
		err := syscall.Statfs(env.MountPoint, &st)
		require.NoError(t, err, "fallback_effective_target should not return an error")
		require.Greater(t, st.Blocks, uint64(0), "fallback_effective_target: got zero blocks")
	})
}

// TestStatfs_onError_fallbackLoopback verifies fallback_loopback falls back to the
// loopback (primaryRootPath = ssd1) when pooled computation cannot complete.
//
// ssd1 is kept intact as the loopback; ssd2 is the sole write target that we remove.
func TestStatfs_onError_fallbackLoopback(t *testing.T) {
	withMountedFS(t, IntegrationConfig{
		Statfs: &config.StatfsConfig{Reporting: "mount_pooled_targets", OnError: "fallback_loopback"},
		// ssd1 is first storage → primaryRootPath (loopback). Keep it intact.
		// ssd2 is the only write target; removing it forces complete pooling failure.
		RoutingRules: []config.RoutingRule{{
			Match:        "**",
			WriteTargets: []string{"ssd2"},
			ReadTargets:  []string{"ssd1", "ssd2"},
		}},
	}, func(env *MountedFS) {
		gone := env.StorageRoot("ssd2") + ".gone"
		require.NoError(t, os.Rename(env.StorageRoot("ssd2"), gone))
		t.Cleanup(func() { _ = os.Rename(gone, env.StorageRoot("ssd2")) })

		var st syscall.Statfs_t
		err := syscall.Statfs(env.MountPoint, &st)
		require.NoError(t, err, "fallback_loopback should not return an error")
		require.Greater(t, st.Blocks, uint64(0), "fallback_loopback: got zero blocks")
	})
}
