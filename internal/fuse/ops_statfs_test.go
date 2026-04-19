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

	// Statfs values can change between two consecutive syscalls due to OS
	// background activity, temp file cleanup, etc. Allow a delta of up to 64
	// blocks - large enough to absorb noise, small enough to catch wrong-device bugs.
	const maxDelta = uint64(64)
	{
		var diff uint64
		if want.Blocks > got.Blocks {
			diff = want.Blocks - got.Blocks
		} else {
			diff = got.Blocks - want.Blocks
		}
		require.LessOrEqual(t, diff, maxDelta, "Blocks mismatch: want=%d got=%d", want.Blocks, got.Blocks)
	}
	{
		var diff uint64
		if want.Bfree > got.Bfree {
			diff = want.Bfree - got.Bfree
		} else {
			diff = got.Bfree - want.Bfree
		}
		require.LessOrEqual(t, diff, maxDelta, "Bfree mismatch: want=%d got=%d", want.Bfree, got.Bfree)
	}
}

// Test_statfsWriteTarget_nilRouter_shouldReturnFalse verifies that statfsWriteTarget
// returns false when the router is nil, allowing the caller to fall back.
func Test_statfsWriteTarget_nilRouter_shouldReturnFalse(t *testing.T) {
	out := &gofuse.StatfsOut{}
	ok := statfsWriteTarget(nil, "", out)
	require.False(t, ok)
}

// Test_statfsPooledRoots_shouldSumRoots verifies statfsPooledRoots pools stats across multiple roots.
func Test_statfsPooledRoots_shouldSumRoots(t *testing.T) {
	root1 := t.TempDir()
	root2 := t.TempDir()

	var st1 syscall.Statfs_t
	require.NoError(t, syscall.Statfs(root1, &st1))
	var st2 syscall.Statfs_t
	require.NoError(t, syscall.Statfs(root2, &st2))

	bs1 := uint64(0)
	if st1.Bsize > 0 {
		bs1 = uint64(st1.Bsize)
	}
	if bs1 == 0 {
		bs1 = 4096
	}
	bs2 := uint64(0)
	if st2.Bsize > 0 {
		bs2 = uint64(st2.Bsize)
	}
	if bs2 == 0 {
		bs2 = 4096
	}
	unit := bs1

	wantBlocks := (st1.Blocks*bs1 + st2.Blocks*bs2) / unit
	wantBfree := (st1.Bfree*bs1 + st2.Bfree*bs2) / unit

	got := &gofuse.StatfsOut{}
	ok, hadErr := statfsPooledRoots([]string{root1, root2}, got)
	require.True(t, ok)
	require.False(t, hadErr)

	// Statfs values can change between syscalls. Allow a larger delta since pooling
	// multiplies noise across roots.
	const maxDelta = uint64(256)
	{
		var diff uint64
		if wantBlocks > got.Blocks {
			diff = wantBlocks - got.Blocks
		} else {
			diff = got.Blocks - wantBlocks
		}
		require.LessOrEqual(t, diff, maxDelta, "Blocks mismatch: want=%d got=%d", wantBlocks, got.Blocks)
	}
	{
		var diff uint64
		if wantBfree > got.Bfree {
			diff = wantBfree - got.Bfree
		} else {
			diff = got.Bfree - wantBfree
		}
		require.LessOrEqual(t, diff, maxDelta, "Bfree mismatch: want=%d got=%d", wantBfree, got.Bfree)
	}
}

// Test_statfsPooledRoots_shouldReportErrors verifies statfsPooledRoots reports failures while still pooling successes.
func Test_statfsPooledRoots_shouldReportErrors(t *testing.T) {
	root1 := t.TempDir()

	got := &gofuse.StatfsOut{}
	ok, hadErr := statfsPooledRoots([]string{root1, "/this/path/should/not/exist"}, got)
	require.True(t, ok)
	require.True(t, hadErr)
}
