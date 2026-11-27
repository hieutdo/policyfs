package fuse

import (
	"context"
	"syscall"

	gofuse "github.com/hanwen/go-fuse/v2/fuse"
	"github.com/hieutdo/policyfs/internal/daemonctl"
)

// FileHandle caches open-time resolution for performance.
//
// The key invariant is: READ/WRITE must not redo any path/routing work; they only use
// the cached `fd`.
type FileHandle struct {
	virtualPath  string
	physicalPath string
	storageID    string
	indexed      bool
	fallback     bool // true when stale real_path triggered fallback to virtualPath
	fd           int
	flags        uint32 // client open flags; fd may lack O_SYNC/O_DSYNC (see backingOpenFlags)

	openTracker *OpenTracker
	openID      daemonctl.OpenFileID
	openWrite   bool
	openTracked bool
}

// Read reads bytes from the already-open file descriptor.
func (h *FileHandle) Read(ctx context.Context, dest []byte, off int64) (gofuse.ReadResult, syscall.Errno) {
	n, err := syscall.Pread(h.fd, dest, off)
	if err != nil {
		return nil, toErrno(err)
	}
	return gofuse.ReadResultData(dest[:n]), 0
}

// Write writes bytes to the already-open file descriptor.
func (h *FileHandle) Write(ctx context.Context, data []byte, off int64) (uint32, syscall.Errno) {
	n, err := syscall.Pwrite(h.fd, data, off)
	if err != nil {
		return 0, toErrno(err)
	}
	return uint32(n), 0
}

// fuseFsyncFdatasync is FUSE_FSYNC_FDATASYNC: the kernel sets it for fdatasync(2)
// and O_DSYNC writes, which only need data (not full metadata) on stable storage.
const fuseFsyncFdatasync = 1

// Fsync flushes writes to stable storage, using fdatasync when the kernel asks for it.
//
// It also carries O_SYNC/O_DSYNC durability, because backingOpenFlags strips those
// flags from the backing fd. Never return ENOSYS here: the kernel would then disable
// FSYNC for the whole mount and sync writes would silently stop being durable.
func (h *FileHandle) Fsync(_ context.Context, flags uint32) syscall.Errno {
	dataOnly := flags&fuseFsyncFdatasync != 0
	errno := syncBackingFD(h.fd, dataOnly)
	if dataOnly && (errno == syscall.ENOSYS || errno == syscall.EOPNOTSUPP) {
		errno = syncBackingFD(h.fd, false)
	}
	if errno == syscall.ENOSYS {
		return syscall.EIO
	}
	return errno
}

// Release closes the underlying file descriptor.
func (h *FileHandle) Release(ctx context.Context) syscall.Errno {
	if h.openTracker != nil && h.openTracked {
		defer h.openTracker.Dec(h.openID, h.openWrite)
	}

	if h.flags&gofuse.O_ANYWRITE != 0 {
		if err := syscall.Fsync(h.fd); err != nil {
			_ = syscall.Close(h.fd)
			return toErrno(err)
		}
	}
	if err := syscall.Close(h.fd); err != nil {
		return toErrno(err)
	}
	_ = ctx
	return 0
}
