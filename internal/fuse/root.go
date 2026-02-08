package fuse

import (
	"context"
	"fmt"
	"syscall"

	"github.com/hanwen/go-fuse/v2/fs"
	gofuse "github.com/hanwen/go-fuse/v2/fuse"
	"github.com/hieutdo/policyfs/internal/config"
	"github.com/hieutdo/policyfs/internal/indexdb"
	"github.com/hieutdo/policyfs/internal/router"
)

// Node is a PolicyFS inode implementation (including the root inode).
type Node struct {
	*fs.LoopbackNode
	rt *router.Router
	db *indexdb.DB
}

// NewRoot creates the PolicyFS root node for mounting.
//
// Currently this is a thin wrapper around go-fuse's loopback root to keep behavior
// identical while we incrementally add PolicyFS operations.
func NewRoot(m *config.MountConfig, primaryRootPath string, db *indexdb.DB) (fs.InodeEmbedder, error) {
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

	n := &Node{LoopbackNode: lb, rt: rt, db: db}
	if lb.RootData != nil {
		lb.RootData.RootNode = n
	}
	return n, nil
}

// WrapChild wraps descendant nodes.
func (n *Node) WrapChild(ctx context.Context, ops fs.InodeEmbedder) fs.InodeEmbedder {
	lb, ok := ops.(*fs.LoopbackNode)
	if !ok {
		return ops
	}
	return &Node{LoopbackNode: lb, rt: n.rt, db: n.db}
}

// Lookup resolves a child entry using the router's read target order.
func (n *Node) Lookup(ctx context.Context, name string, out *gofuse.EntryOut) (*fs.Inode, syscall.Errno) {
	return lookupChild(ctx, n.EmbeddedInode(), n.RootData, n.rt, n.db, name, out)
}

// Getattr reads attributes using the router's read target order.
func (n *Node) Getattr(ctx context.Context, f fs.FileHandle, out *gofuse.AttrOut) syscall.Errno {
	return getattrPath(ctx, n.EmbeddedInode(), n.rt, n.db, out)
}

// Readdir returns a union of directory entries across read targets, deduped by name.
func (n *Node) Readdir(ctx context.Context) (fs.DirStream, syscall.Errno) {
	return readdirPath(ctx, n.EmbeddedInode(), n.rt, n.db)
}

// OpendirHandle returns a directory handle that merges entries across read targets.
func (n *Node) OpendirHandle(ctx context.Context, flags uint32) (fs.FileHandle, uint32, syscall.Errno) {
	entries, errno := listDirEntries(ctx, n.EmbeddedInode(), n.rt, n.db)
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

// Getxattr reports no supported xattrs to avoid ls ACL probing showing '?'.
func (n *Node) Getxattr(ctx context.Context, attr string, dest []byte) (uint32, syscall.Errno) {
	return 0, fs.ENOATTR
}

// Listxattr returns an empty list to indicate no supported xattrs.
func (n *Node) Listxattr(ctx context.Context, dest []byte) (uint32, syscall.Errno) {
	return 0, 0
}

// Setxattr rejects all xattrs on the virtual mount.
func (n *Node) Setxattr(ctx context.Context, attr string, data []byte, flags uint32) syscall.Errno {
	return fs.ENOATTR
}

// Removexattr rejects all xattrs on the virtual mount.
func (n *Node) Removexattr(ctx context.Context, attr string) syscall.Errno {
	return fs.ENOATTR
}
