package fuse

import (
	"syscall"
	"testing"

	gofuse "github.com/hanwen/go-fuse/v2/fuse"
	"github.com/hieutdo/policyfs/internal/config"
	"github.com/hieutdo/policyfs/internal/router"
	"github.com/stretchr/testify/require"
)

// Test_statfsWriteTarget_shouldReturnWriteTargetStats verifies that statfsWriteTarget
// reports the filesystem stats of the primary write target.
func Test_statfsWriteTarget_shouldReturnWriteTargetStats(t *testing.T) {
	root1 := t.TempDir()
	root2 := t.TempDir()

	mountCfg := &config.MountConfig{
		MountPoint: "/mnt/unused",
		StoragePaths: []config.StoragePath{
			{ID: "s1", Path: root1, Indexed: false},
			{ID: "s2", Path: root2, Indexed: false},
		},
		RoutingRules: []config.RoutingRule{{
			Match:        "**",
			ReadTargets:  []string{"s1", "s2"},
			WriteTargets: []string{"s2"},
		}},
	}

	rt, err := router.New(mountCfg)
	require.NoError(t, err)

	// Get expected stats by converting through the same FromStatfsT path.
	var raw syscall.Statfs_t
	require.NoError(t, syscall.Statfs(root2, &raw))
	want := &gofuse.StatfsOut{}
	want.FromStatfsT(&raw)

	got := &gofuse.StatfsOut{}
	ok := statfsWriteTarget(rt, "", got)
	require.True(t, ok)

	// The reported blocks should match the write target (root2).
	require.Equal(t, want.Blocks, got.Blocks)
	require.Equal(t, want.Bfree, got.Bfree)
}

// Test_statfsWriteTarget_nilRouter_shouldReturnFalse verifies that statfsWriteTarget
// returns false when the router is nil, allowing the caller to fall back.
func Test_statfsWriteTarget_nilRouter_shouldReturnFalse(t *testing.T) {
	out := &gofuse.StatfsOut{}
	ok := statfsWriteTarget(nil, "", out)
	require.False(t, ok)
}
