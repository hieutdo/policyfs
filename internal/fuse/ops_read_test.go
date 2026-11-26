package fuse

import (
	"context"
	"os"
	"path/filepath"
	"syscall"
	"testing"

	"github.com/hanwen/go-fuse/v2/fs"
	gofuse "github.com/hanwen/go-fuse/v2/fuse"
	"github.com/hieutdo/policyfs/internal/config"
	"github.com/hieutdo/policyfs/internal/router"
	"github.com/rs/zerolog"
	"github.com/stretchr/testify/require"
	"golang.org/x/sys/unix"
)

// Test_listDirEntriesForVirtualPath verifies union+dedupe behavior for directory listings.
func Test_listDirEntriesForVirtualPath(t *testing.T) {
	// This test exercises the core PolicyFS readdir behavior without mounting FUSE.
	root1 := t.TempDir()
	root2 := t.TempDir()

	mountCfg := &config.MountConfig{
		MountPoint: "/mnt/unused",
		StoragePaths: []config.StoragePath{
			{ID: "ssd1", Path: root1, Indexed: false},
			{ID: "ssd2", Path: root2, Indexed: false},
		},
		RoutingRules: []config.RoutingRule{{
			Match:       "**",
			Targets:     []string{"ssd1", "ssd2"},
			ReadTargets: []string{"ssd1", "ssd2"},
		}},
	}

	rt, err := router.New(mountCfg)
	require.NoError(t, err)

	t.Run("should merge and dedupe by name", func(t *testing.T) {
		virtualDir := "readdir"

		// ssd1: dup is a file.
		dir1 := filepath.Join(root1, virtualDir)
		require.NoError(t, os.MkdirAll(dir1, 0o755))
		require.NoError(t, os.WriteFile(filepath.Join(dir1, "a.txt"), []byte("a"), 0o644))
		require.NoError(t, os.WriteFile(filepath.Join(dir1, "dup"), []byte("x"), 0o644))

		// ssd2: dup is a directory; should be ignored because ssd1 is first.
		dir2 := filepath.Join(root2, virtualDir)
		require.NoError(t, os.MkdirAll(dir2, 0o755))
		require.NoError(t, os.WriteFile(filepath.Join(dir2, "b.txt"), []byte("b"), 0o644))
		require.NoError(t, os.MkdirAll(filepath.Join(dir2, "dup"), 0o755))

		entries, errno := listDirEntriesForVirtualPath(context.Background(), virtualDir, rt, nil, zerolog.Logger{})
		require.Equal(t, syscall.Errno(0), errno)

		got := map[string]uint32{}
		for _, e := range entries {
			got[e.Name] = e.Mode
		}
		require.Contains(t, got, ".")
		require.Contains(t, got, "..")
		require.Contains(t, got, "a.txt")
		require.Contains(t, got, "b.txt")
		require.Contains(t, got, "dup")

		// dup should come from ssd1 where it's a regular file.
		require.Equal(t, uint32(syscall.S_IFREG), got["dup"]&uint32(syscall.S_IFMT))
	})

	t.Run("should ignore targets where the directory does not exist", func(t *testing.T) {
		virtualDir := "only-on-ssd2"
		dir2 := filepath.Join(root2, virtualDir)
		require.NoError(t, os.MkdirAll(dir2, 0o755))
		require.NoError(t, os.WriteFile(filepath.Join(dir2, "b.txt"), []byte("b"), 0o644))

		entries, errno := listDirEntriesForVirtualPath(context.Background(), virtualDir, rt, nil, zerolog.Logger{})
		require.Equal(t, syscall.Errno(0), errno)
		got := map[string]struct{}{}
		for _, e := range entries {
			got[e.Name] = struct{}{}
		}
		require.Contains(t, got, ".")
		require.Contains(t, got, "..")
		require.Contains(t, got, "b.txt")
	})

	t.Run("should return ENOENT when no directory exists on any target", func(t *testing.T) {
		entries, errno := listDirEntriesForVirtualPath(context.Background(), "missing-dir", rt, nil, zerolog.Logger{})
		require.Nil(t, entries)
		require.Equal(t, syscall.ENOENT, errno)
	})
}

// TestNode_Statx verifies Statx resolves through router read targets and returns physical
// statx fields (blocks, ctime, btime) from the target that holds the path.
func TestNode_Statx(t *testing.T) {
	root1 := t.TempDir()
	root2 := t.TempDir()

	mountCfg := &config.MountConfig{
		MountPoint: "/mnt/unused",
		StoragePaths: []config.StoragePath{
			{ID: "ssd1", Path: root1, Indexed: false},
			{ID: "hdd1", Path: root2, Indexed: false},
		},
		RoutingRules: []config.RoutingRule{{
			Match:       "**",
			Targets:     []string{"ssd1", "hdd1"},
			ReadTargets: []string{"ssd1", "hdd1"},
		}},
	}

	root, err := NewRoot("media", mountCfg, root1, nil, zerolog.Logger{}, DiskAccessConfig{})
	require.NoError(t, err)
	raw := fs.NewNodeFS(root, &fs.Options{})

	t.Run("should report physical statx metadata from a secondary read target", func(t *testing.T) {
		const name = "sparse.bin"
		physicalPath := filepath.Join(root2, name)
		require.NoError(t, os.WriteFile(physicalPath, nil, 0o600))
		require.NoError(t, os.Truncate(physicalPath, 32<<20))
		const mask = unix.STATX_BASIC_STATS | unix.STATX_BTIME
		var want unix.Statx_t
		require.NoError(t, unix.Statx(unix.AT_FDCWD, physicalPath, unix.AT_SYMLINK_NOFOLLOW, mask, &want))

		var entry gofuse.EntryOut
		require.Equal(t, gofuse.OK, raw.Lookup(nil, &gofuse.InHeader{NodeId: gofuse.FUSE_ROOT_ID}, name, &entry))
		in := gofuse.StatxIn{InHeader: gofuse.InHeader{NodeId: entry.NodeId}, SxMask: mask}
		var got gofuse.StatxOut
		require.Equal(t, gofuse.OK, raw.Statx(nil, &in, &got))
		require.Equal(t, want.Size, got.Size)
		require.Equal(t, want.Blocks, got.Blocks)
		require.Equal(t, want.Blksize, got.Blksize)
		require.Equal(t, want.Ctime.Sec, int64(got.Ctime.Sec))
		require.Equal(t, want.Mask&unix.STATX_BTIME, got.Mask&unix.STATX_BTIME)
		require.Equal(t, want.Btime.Sec, int64(got.Btime.Sec))
		require.Equal(t, want.Btime.Nsec, got.Btime.Nsec)
	})
}
