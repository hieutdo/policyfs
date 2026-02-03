package fuse

import (
	"context"
	"syscall"

	gofuse "github.com/hanwen/go-fuse/v2/fuse"
)

// DirHandle is a directory handle backed by a fixed list of entries.
type DirHandle struct {
	entries []gofuse.DirEntry
	idx     int
}

// Readdirent returns directory entries one by one.
func (h *DirHandle) Readdirent(ctx context.Context) (*gofuse.DirEntry, syscall.Errno) {
	if h.idx >= len(h.entries) {
		return nil, 0
	}
	e := h.entries[h.idx]
	h.idx++
	e.Off = uint64(h.idx)
	return &e, 0
}

// Seekdir seeks to an opaque directory offset.
func (h *DirHandle) Seekdir(ctx context.Context, off uint64) syscall.Errno {
	idx := int(off)
	if idx < 0 || idx > len(h.entries) {
		return syscall.EINVAL
	}
	h.idx = idx
	return 0
}

// Releasedir releases any resources held by the directory handle.
func (h *DirHandle) Releasedir(ctx context.Context, releaseFlags uint32) {
	// No resources to free.
}
