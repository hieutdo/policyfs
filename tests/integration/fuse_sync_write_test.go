//go:build integration

package integration

import (
	"bytes"
	"encoding/json"
	"fmt"
	"os"
	"path/filepath"
	"strconv"
	"strings"
	"sync/atomic"
	"testing"

	"github.com/hanwen/go-fuse/v2/fs"
	"github.com/hanwen/go-fuse/v2/fuse"
	"github.com/hieutdo/policyfs/internal/config"
	"github.com/stretchr/testify/require"
	"golang.org/x/sys/unix"
)

// TestFUSE_syncWrite proves O_SYNC/O_DSYNC writes keep the sync flag on the client fd
// but not on the backing fd, so the kernel's single FSYNC per write does the syncing.
func TestFUSE_syncWrite(t *testing.T) {
	if os.Getenv(config.EnvIntegrationUseExistingMount) != "" {
		t.Skip("requires a test-owned daemon to inspect backing fd flags")
	}

	t.Run("should sync a large write to a new file", func(t *testing.T) {
		withMountedFS(t, IntegrationConfig{Targets: []string{"ssd1"}}, func(env *MountedFS) {
			const name = "sync.bin"
			f, err := os.OpenFile(env.MountPath(name), os.O_WRONLY|os.O_CREATE|os.O_EXCL|unix.O_SYNC, 0o600)
			require.NoError(t, err)
			defer func() { _ = f.Close() }()

			flags, err := unix.FcntlInt(f.Fd(), unix.F_GETFL, 0)
			require.NoError(t, err)
			require.Equal(t, unix.O_SYNC, flags&unix.O_SYNC)

			data := bytes.Repeat([]byte{0x5a}, 16<<20)
			n, err := unix.Write(int(f.Fd()), data)
			require.NoError(t, err)
			require.Equal(t, len(data), n)
			require.Zero(t, backingFDFlags(t, env, env.StoragePath("ssd1", name))&(unix.O_SYNC|unix.O_DSYNC))

			got, err := os.ReadFile(env.MountPath(name))
			require.NoError(t, err)
			require.True(t, bytes.Equal(data, got), "readback differs from the synchronous write")
		})
	})

	t.Run("should sync an offset write to an existing sparse file", func(t *testing.T) {
		withMountedFS(t, IntegrationConfig{Targets: []string{"ssd1"}}, func(env *MountedFS) {
			const name = "sparse.bin"
			const offset = 1 << 20
			physicalPath := env.StoragePath("ssd1", name)
			require.NoError(t, os.WriteFile(physicalPath, []byte("head"), 0o600))

			f, err := os.OpenFile(env.MountPath(name), os.O_WRONLY|unix.O_DSYNC, 0)
			require.NoError(t, err)
			defer func() { _ = f.Close() }()

			flags, err := unix.FcntlInt(f.Fd(), unix.F_GETFL, 0)
			require.NoError(t, err)
			require.Equal(t, unix.O_DSYNC, flags&(unix.O_SYNC|unix.O_DSYNC))

			data := []byte("tail")
			n, err := f.WriteAt(data, offset)
			require.NoError(t, err)
			require.Equal(t, len(data), n)
			require.Zero(t, backingFDFlags(t, env, physicalPath)&(unix.O_SYNC|unix.O_DSYNC))
			require.NoError(t, unix.Fdatasync(int(f.Fd())))
			require.NoError(t, f.Sync())

			got, err := os.ReadFile(env.MountPath(name))
			require.NoError(t, err)
			require.Len(t, got, offset+len(data))
			require.Equal(t, "head", string(got[:4]))
			require.True(t, bytes.Equal(make([]byte, offset-4), got[4:offset]), "sparse gap is not zero-filled")
			require.Equal(t, data, got[offset:])
		})
	})
}

// syncTestFS counts each kind of FSYNC and injects the configured errors.
type syncTestFS struct {
	fuse.RawFileSystem
	full     *atomic.Int32
	datasync *atomic.Int32
	fullErr  fuse.Status
	dataErr  fuse.Status
}

// Fsync records whether the backing sync was an fdatasync (FUSE_FSYNC_FDATASYNC bit).
func (f syncTestFS) Fsync(cancel <-chan struct{}, in *fuse.FsyncIn) fuse.Status {
	status := f.fullErr
	if in.FsyncFlags&1 != 0 {
		f.datasync.Add(1)
		status = f.dataErr
	} else {
		f.full.Add(1)
	}
	if status != fuse.OK {
		return status
	}
	return f.RawFileSystem.Fsync(cancel, in)
}

// mountSyncTestFS exposes a loopback subtree with configurable errors for each sync kind.
func mountSyncTestFS(t *testing.T, env *MountedFS, fullErr, dataErr fuse.Status) (string, syncTestFS) {
	t.Helper()
	source := t.TempDir()
	root, err := fs.NewLoopbackRoot(source)
	require.NoError(t, err)
	mountpoint := env.StoragePath("ssd1", "fault")
	require.NoError(t, os.Mkdir(mountpoint, 0o755))

	backing := syncTestFS{
		RawFileSystem: fs.NewNodeFS(root, &fs.Options{}),
		full:          new(atomic.Int32),
		datasync:      new(atomic.Int32),
		fullErr:       fullErr,
		dataErr:       dataErr,
	}
	server, err := fuse.NewServer(backing, mountpoint, &fuse.MountOptions{})
	require.NoError(t, err)
	go server.Serve()
	t.Cleanup(func() { require.NoError(t, server.Unmount()) })
	require.NoError(t, server.WaitMount())

	return source, backing
}

// TestFUSE_syncWriteError requires the sync error on the write itself, before close.
func TestFUSE_syncWriteError(t *testing.T) {
	if os.Getenv(config.EnvIntegrationUseExistingMount) != "" {
		t.Skip("requires test-owned backing storage")
	}

	withMountedFS(t, IntegrationConfig{Targets: []string{"ssd1"}}, func(env *MountedFS) {
		source, backing := mountSyncTestFS(t, env, fuse.EIO, fuse.EIO)

		t.Run("should return backing fsync error from O_SYNC create and write", func(t *testing.T) {
			f, err := os.OpenFile(env.MountPath("fault/new"), os.O_WRONLY|os.O_CREATE|os.O_EXCL|unix.O_SYNC, 0o600)
			require.NoError(t, err)
			defer func() { _ = f.Close() }()

			_, err = f.Write([]byte("new"))
			require.ErrorIs(t, err, unix.EIO)
			// O_SYNC needs metadata too, so the backing sync must be a full fsync.
			require.Positive(t, backing.full.Load())
			require.Zero(t, backing.datasync.Load())
			got, err := os.ReadFile(filepath.Join(source, "new"))
			require.NoError(t, err)
			require.Equal(t, "new", string(got))
		})

		t.Run("should return backing fsync error from O_DSYNC open and offset write", func(t *testing.T) {
			path := filepath.Join(source, "existing")
			require.NoError(t, os.WriteFile(path, []byte("head"), 0o600))
			f, err := os.OpenFile(env.MountPath("fault/existing"), os.O_WRONLY|unix.O_DSYNC, 0)
			require.NoError(t, err)
			defer func() { _ = f.Close() }()

			_, err = f.WriteAt([]byte("tail"), 4)
			require.ErrorIs(t, err, unix.EIO)
			// O_DSYNC only needs data integrity; Release fsyncs are full, so count datasync exactly.
			require.Equal(t, int32(1), backing.datasync.Load())
			got, err := os.ReadFile(path)
			require.NoError(t, err)
			require.Equal(t, "headtail", string(got))
		})
	})
}

func TestFUSE_syncUnsupportedDatasync(t *testing.T) {
	if os.Getenv(config.EnvIntegrationUseExistingMount) != "" {
		t.Skip("requires test-owned backing storage")
	}

	t.Run("should use full fsync when backing storage rejects fdatasync", func(t *testing.T) {
		withMountedFS(t, IntegrationConfig{Targets: []string{"ssd1"}}, func(env *MountedFS) {
			_, backing := mountSyncTestFS(t, env, fuse.OK, fuse.Status(unix.EOPNOTSUPP))
			f, err := os.OpenFile(env.MountPath("fault/sync"), os.O_CREATE|os.O_WRONLY|unix.O_DSYNC, 0o600)
			require.NoError(t, err)
			defer func() { _ = f.Close() }()

			n, err := f.Write([]byte("data"))
			require.NoError(t, err)
			require.Equal(t, 4, n)
			require.NoError(t, unix.Fdatasync(int(f.Fd())))
			require.Equal(t, int32(2), backing.datasync.Load())
			require.Equal(t, int32(2), backing.full.Load())
		})
	})
}

// TestFUSE_syncENOSYS injects at the daemon syscall boundary. A backing FUSE
// mount would consume ENOSYS itself before PolicyFS could see it.
func TestFUSE_syncENOSYS(t *testing.T) {
	if os.Getenv(config.EnvIntegrationUseExistingMount) != "" {
		t.Skip("requires a test-owned daemon for syscall injection")
	}
	dir := t.TempDir()
	overlay, err := json.Marshal(map[string]map[string]string{"Replace": {
		filepath.Join(workspace, "internal/fuse/sync_syscall.go"): filepath.Join(workspace, "tests/integration/testdata/sync_enosys.go"),
	}})
	require.NoError(t, err)
	overlayPath := filepath.Join(dir, "overlay.json")
	require.NoError(t, os.WriteFile(overlayPath, overlay, 0o600))
	binary := filepath.Join(dir, "pfs")
	// Coverage instrumentation builds the original syscall source instead of the overlay.
	require.NoError(t, buildPFSBinary(binary, false, "-overlay", overlayPath))

	withMountedFS(t, IntegrationConfig{Targets: []string{"ssd1"}, mountBinary: binary}, func(env *MountedFS) {
		_, backing := mountSyncTestFS(t, env, fuse.EIO, fuse.EIO)

		t.Run("should map final backing ENOSYS to EIO without disabling mount FSYNC", func(t *testing.T) {
			f, err := os.OpenFile(env.MountPath("unsupported"), os.O_CREATE|os.O_WRONLY|unix.O_DSYNC, 0o600)
			require.NoError(t, err)
			defer func() { _ = f.Close() }()
			// Both backing calls return ENOSYS for this file.
			_, err = f.Write([]byte("data"))
			require.ErrorIs(t, err, unix.EIO)
			require.ErrorIs(t, f.Sync(), unix.EIO)
		})

		t.Run("should fall back from ENOSYS fdatasync to full fsync on subsequent files", func(t *testing.T) {
			// The kernel must still issue FSYNC on another file on the same mount.
			// Only fdatasync is unsupported here. The full fallback reaches loopback.
			g, err := os.OpenFile(env.MountPath("fault/fallback"), os.O_CREATE|os.O_WRONLY|unix.O_DSYNC, 0o600)
			require.NoError(t, err)
			defer func() { _ = g.Close() }()
			_, err = g.Write([]byte("data"))
			require.ErrorIs(t, err, unix.EIO)
			require.Equal(t, int32(1), backing.full.Load())
			require.Zero(t, backing.datasync.Load())

			// On ordinary storage the same fallback succeeds.
			h, err := os.OpenFile(env.MountPath("healthy"), os.O_CREATE|os.O_WRONLY|unix.O_DSYNC, 0o600)
			require.NoError(t, err)
			defer func() { _ = h.Close() }()
			_, err = h.Write([]byte("data"))
			require.NoError(t, err)
		})
	})
}

// backingFDFlags reads the test daemon's kernel fd state while the mounted file is open.
func backingFDFlags(t *testing.T, env *MountedFS, path string) int {
	t.Helper()
	proc := fmt.Sprintf("/proc/%d", env.mountProc.Pid)
	entries, err := os.ReadDir(filepath.Join(proc, "fd"))
	require.NoError(t, err)
	for _, entry := range entries {
		descriptor := entry.Name()
		target, err := os.Readlink(filepath.Join(proc, "fd", descriptor))
		if err != nil || target != path {
			continue
		}
		info, err := os.ReadFile(filepath.Join(proc, "fdinfo", descriptor))
		require.NoError(t, err)
		for line := range strings.SplitSeq(string(info), "\n") {
			if value, ok := strings.CutPrefix(line, "flags:"); ok {
				flags, err := strconv.ParseInt(strings.TrimSpace(value), 8, 0)
				require.NoError(t, err)
				return int(flags)
			}
		}
	}
	t.Fatalf("backing fd flags missing for %s", path)
	return 0
}
