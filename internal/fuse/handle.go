package fuse

import (
	"context"
	"syscall"

	"github.com/hanwen/go-fuse/v2/fs"
	gofuse "github.com/hanwen/go-fuse/v2/fuse"
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
	fd           int
	flags        uint32
}

// Read reads bytes from the already-open file descriptor.
func (h *FileHandle) Read(ctx context.Context, dest []byte, off int64) (gofuse.ReadResult, syscall.Errno) {
	n, err := syscall.Pread(h.fd, dest, off)
	if err != nil {
		return nil, fs.ToErrno(err)
	}
	return gofuse.ReadResultData(dest[:n]), 0
}

// Write writes bytes to the already-open file descriptor.
func (h *FileHandle) Write(ctx context.Context, data []byte, off int64) (uint32, syscall.Errno) {
	n, err := syscall.Pwrite(h.fd, data, off)
	if err != nil {
		return 0, fs.ToErrno(err)
	}
	return uint32(n), 0
}

// Fsync flushes writes to stable storage.
func (h *FileHandle) Fsync(ctx context.Context, flags uint32) syscall.Errno {
	if err := syscall.Fsync(h.fd); err != nil {
		return fs.ToErrno(err)
	}
	_ = ctx
	_ = flags
	return 0
}

// Release closes the underlying file descriptor.
func (h *FileHandle) Release(ctx context.Context) syscall.Errno {
	if h.flags&gofuse.O_ANYWRITE != 0 {
		if err := syscall.Fsync(h.fd); err != nil {
			_ = syscall.Close(h.fd)
			return fs.ToErrno(err)
		}
	}
	if err := syscall.Close(h.fd); err != nil {
		return fs.ToErrno(err)
	}
	_ = ctx
	return 0
}
