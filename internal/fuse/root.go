package fuse

import (
	"context"
	"fmt"
	"path/filepath"
	"syscall"
	"time"

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
	ch, errno := lookupChild(ctx, n.EmbeddedInode(), n.RootData, n.mountName, n.rt, n.db, n.log, n.disk, name, out)
	if errno != 0 && errno != syscall.ENOENT {
		n.log.Error().Str("op", "lookup").Str("path", filepath.Join(n.Path(n.Root()), name)).Err(errno).Msg("failed to lookup")
	}
	return ch, errno
}

// Getattr reads attributes using the router's read target order.
func (n *Node) Getattr(ctx context.Context, f fs.FileHandle, out *gofuse.AttrOut) syscall.Errno {
	return getattrPath(ctx, n.EmbeddedInode(), n.rt, n.db, out)
}

// Readdir returns a union of directory entries across read targets, deduped by name.
func (n *Node) Readdir(ctx context.Context) (fs.DirStream, syscall.Errno) {
	start := time.Now()
	ds, errno := readdirPath(ctx, n.EmbeddedInode(), n.rt, n.db)
	if errno != 0 {
		n.log.Error().Str("op", "readdir").Str("path", n.Path(n.Root())).Err(errno).Msg("failed to readdir")
	} else {
		n.log.Debug().Str("op", "readdir").Str("path", n.Path(n.Root())).Int64("duration_ms", time.Since(start).Milliseconds()).Msg("readdir")
	}
	return ds, errno
}

// OpendirHandle returns a directory handle that merges entries across read targets.
func (n *Node) OpendirHandle(ctx context.Context, flags uint32) (fs.FileHandle, uint32, syscall.Errno) {
	start := time.Now()
	entries, errno := listDirEntries(ctx, n.EmbeddedInode(), n.rt, n.db)
	if errno != 0 {
		n.log.Error().Str("op", "opendir").Str("path", n.Path(n.Root())).Err(errno).Msg("failed to opendir")
		return nil, 0, errno
	}
	n.log.Debug().Str("op", "opendir").Str("path", n.Path(n.Root())).Int("entries", len(entries)).Int64("duration_ms", time.Since(start).Milliseconds()).Msg("opendir")
	return &DirHandle{entries: entries}, 0, 0
}

// Open opens a file and returns a cached FileHandle.
func (n *Node) Open(ctx context.Context, flags uint32) (fs.FileHandle, uint32, syscall.Errno) {
	virtualPath := n.Path(n.Root())
	write := flags&gofuse.O_ANYWRITE != 0

	start := time.Now()
	fh, openFlags, errno := openFirst(ctx, n.rt, n.db, virtualPath, int(flags), write)
	if errno != 0 {
		if errno == syscall.EROFS {
			n.log.Debug().Str("op", "open").Str("path", virtualPath).Bool("write", write).Msg("open blocked: indexed target is read-only")
		} else if errno != syscall.ENOENT {
			n.log.Error().Str("op", "open").Str("path", virtualPath).Bool("write", write).Err(errno).Msg("failed to open")
		}
		return nil, 0, errno
	}
	if h, ok := fh.(*FileHandle); ok {
		if h.fallback {
			n.log.Warn().Str("op", "open").Str("path", virtualPath).Str("storage_id", h.storageID).Msg("open: stale real_path fallback triggered")
		}
		n.log.Debug().Str("op", "open").Str("path", virtualPath).Str("storage_id", h.storageID).Bool("indexed", h.indexed).Bool("write", write).Int64("duration_ms", time.Since(start).Milliseconds()).Msg("open")
		if n.disk != nil {
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
	if errno := validateVirtualPath(virtualPath); errno != 0 {
		return errno
	}
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
