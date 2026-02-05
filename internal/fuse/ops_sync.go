package fuse

import (
	"context"
	"syscall"

	"github.com/hanwen/go-fuse/v2/fs"
)

// Flush is called on close(2). We keep this lightweight for indexed=false.
func (n *Node) Flush(ctx context.Context, f fs.FileHandle) syscall.Errno {
	_ = ctx
	_ = f
	// We don't need to do anything here; actual fd close happens in Release on FileHandle.
	return 0
}

// Fsync flushes file content to stable storage.
func (n *Node) Fsync(ctx context.Context, f fs.FileHandle, flags uint32) syscall.Errno {
	if fh, ok := f.(*FileHandle); ok && fh != nil {
		return fh.Fsync(ctx, flags)
	}
	// Without a FileHandle we don't know which physical fd to sync.
	_ = ctx
	_ = flags
	return syscall.ENOTSUP
}
