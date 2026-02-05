package fuse

import (
	"context"
	"fmt"
	"syscall"

	"github.com/hanwen/go-fuse/v2/fs"
	gofuse "github.com/hanwen/go-fuse/v2/fuse"
	"github.com/hieutdo/policyfs/internal/config"
	"github.com/hieutdo/policyfs/internal/router"
)

// Node is a PolicyFS inode implementation (including the root inode).
type Node struct {
	*fs.LoopbackNode
	rt *router.Router
}

// NewRoot creates the PolicyFS root node for mounting.
//
// Currently this is a thin wrapper around go-fuse's loopback root to keep behavior
// identical while we incrementally add PolicyFS operations.
func NewRoot(m *config.MountConfig, primaryRootPath string) (fs.InodeEmbedder, error) {
	rt, err := router.New(m)
	if err != nil {
		return nil, fmt.Errorf("failed to create router: %w", err)
	}

	op, err := fs.NewLoopbackRoot(primaryRootPath)
	if err != nil {
		return nil, fmt.Errorf("failed to create loopback root: %w", err)
	}

	lb, ok := op.(*fs.LoopbackNode)
	if !ok {
		// Fallback: preserve behavior if go-fuse changes the concrete type.
		return op, nil
	}

	return &Node{LoopbackNode: lb, rt: rt}, nil
}

// WrapChild wraps descendant nodes.
func (n *Node) WrapChild(ctx context.Context, ops fs.InodeEmbedder) fs.InodeEmbedder {
	lb, ok := ops.(*fs.LoopbackNode)
	if !ok {
		return ops
	}
	return &Node{LoopbackNode: lb, rt: n.rt}
}

// Lookup resolves a child entry using the router's read target order.
func (n *Node) Lookup(ctx context.Context, name string, out *gofuse.EntryOut) (*fs.Inode, syscall.Errno) {
	return lookupChild(ctx, n.EmbeddedInode(), n.RootData, n.rt, name, out)
}

// Getattr reads attributes using the router's read target order.
func (n *Node) Getattr(ctx context.Context, f fs.FileHandle, out *gofuse.AttrOut) syscall.Errno {
	return getattrPath(ctx, n.EmbeddedInode(), n.rt, out)
}

// Readdir returns a union of directory entries across read targets, deduped by name.
func (n *Node) Readdir(ctx context.Context) (fs.DirStream, syscall.Errno) {
	return readdirPath(ctx, n.EmbeddedInode(), n.rt)
}

// OpendirHandle returns a directory handle that merges entries across read targets.
func (n *Node) OpendirHandle(ctx context.Context, flags uint32) (fs.FileHandle, uint32, syscall.Errno) {
	entries, errno := listDirEntries(ctx, n.EmbeddedInode(), n.rt)
	if errno != 0 {
		return nil, 0, errno
	}
	return &DirHandle{entries: entries}, 0, 0
}

// Open opens a file and returns a cached FileHandle.
func (n *Node) Open(ctx context.Context, flags uint32) (fs.FileHandle, uint32, syscall.Errno) {
	virtualPath := n.Path(n.Root())

	if flags&gofuse.O_ANYWRITE != 0 {
		return openFirst(ctx, n.rt, virtualPath, int(flags), true)
	}
	return openFirst(ctx, n.rt, virtualPath, int(flags), false)
}

// Release closes any file handles we created.
func (n *Node) Release(ctx context.Context, f fs.FileHandle) syscall.Errno {
	if r, ok := f.(interface {
		Release(ctx context.Context) syscall.Errno
	}); ok {
		return r.Release(ctx)
	}
	return 0
}
