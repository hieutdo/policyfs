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
	"github.com/rs/zerolog"
)

// Node is a PolicyFS inode implementation (including the root inode).
type Node struct {
	*fs.LoopbackNode
	mountName string
	rt        *router.Router
	db        *indexdb.DB
	log       zerolog.Logger
	disk      *diskAccessLogger
}

// NewRoot creates the PolicyFS root node for mounting.
//
// Currently this is a thin wrapper around go-fuse's loopback root to keep behavior
// identical while we incrementally add PolicyFS operations.
func NewRoot(mountName string, m *config.MountConfig, primaryRootPath string, db *indexdb.DB, baseLog zerolog.Logger, diskCfg DiskAccessConfig) (fs.InodeEmbedder, error) {
	rt, err := router.New(m)
	if err != nil {
		return nil, fmt.Errorf("failed to create router: %w", err)
	}

	fuseLog := baseLog.With().Str("component", "fuse").Str("mount", mountName).Logger()
	diskLog := newDiskAccessLogger(fuseLog, diskCfg)

	op, err := fs.NewLoopbackRoot(primaryRootPath)
	if err != nil {
		return nil, fmt.Errorf("failed to create loopback root: %w", err)
	}

	lb, ok := op.(*fs.LoopbackNode)
	if !ok {
		// Fallback: preserve behavior if go-fuse changes the concrete type.
		return op, nil
	}

	n := &Node{LoopbackNode: lb, mountName: mountName, rt: rt, db: db, log: fuseLog, disk: diskLog}
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
	return &Node{LoopbackNode: lb, mountName: n.mountName, rt: n.rt, db: n.db, log: n.log, disk: n.disk}
}

// Lookup resolves a child entry using the router's read target order.
func (n *Node) Lookup(ctx context.Context, name string, out *gofuse.EntryOut) (*fs.Inode, syscall.Errno) {
	return lookupChild(ctx, n.EmbeddedInode(), n.RootData, n.mountName, n.rt, n.db, n.log, n.disk, name, out)
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

	var fh fs.FileHandle
	var openFlags uint32
	var errno syscall.Errno
	if flags&gofuse.O_ANYWRITE != 0 {
		fh, openFlags, errno = openFirst(ctx, n.rt, n.db, virtualPath, int(flags), true)
	} else {
		fh, openFlags, errno = openFirst(ctx, n.rt, n.db, virtualPath, int(flags), false)
	}
	if errno == 0 && n.disk != nil {
		if h, ok := fh.(*FileHandle); ok {
			n.disk.RecordOpen(ctx, h.storageID, virtualPath, h.indexed)
		}
	}
	return fh, openFlags, errno
}

// Statfs returns filesystem stats based on the write target for the current path.
//
// The default loopback Statfs reports stats for the primaryRootPath filesystem,
// which may differ from where writes actually land. This override resolves write
// targets via the router so that tools like df and sabnzbd see the correct free
// space for the filesystem that will receive writes at this path.
func (n *Node) Statfs(ctx context.Context, out *gofuse.StatfsOut) syscall.Errno {
	virtualPath := n.Path(n.Root())
	if statfsWriteTarget(n.rt, virtualPath, out) {
		return 0
	}
	// Fallback: delegate to loopback (uses primaryRootPath).
	return n.LoopbackNode.Statfs(ctx, out)
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
