package fuse

import (
	"context"
	"fmt"
	"path/filepath"
	"strings"
	"syscall"
	"time"

	"github.com/hanwen/go-fuse/v2/fs"
	gofuse "github.com/hanwen/go-fuse/v2/fuse"
	"github.com/hieutdo/policyfs/internal/config"
	"github.com/hieutdo/policyfs/internal/daemonctl"
	"github.com/hieutdo/policyfs/internal/indexdb"
	"github.com/hieutdo/policyfs/internal/router"
	"github.com/rs/zerolog"
)

// Node is a PolicyFS inode implementation (including the root inode).
type Node struct {
	*fs.LoopbackNode
	mountName string
	state     *runtimeState
	reload    *reloadState
	db        *indexdb.DB
	disk      *diskAccessLogger
	open      *OpenTracker
}

// runtime returns a stable snapshot of the current router/logger for this mount.
func (n *Node) runtime() (*router.Router, zerolog.Logger) {
	if n == nil || n.state == nil {
		return nil, zerolog.Logger{}
	}
	return n.state.Snapshot()
}

// NewRoot creates the PolicyFS root node for mounting.
//
// Currently this is a thin wrapper around go-fuse's loopback root to keep behavior
// identical while we incrementally add PolicyFS operations.
func NewRoot(mountName string, m *config.MountConfig, primaryRootPath string, db *indexdb.DB, baseLog zerolog.Logger, diskCfg DiskAccessConfig) (fs.InodeEmbedder, error) {
	return NewRootWithReload(mountName, m, primaryRootPath, db, baseLog, diskCfg, false, config.LogConfig{})
}

// NewRootWithReload creates the PolicyFS root node for mounting, including reload state.
func NewRootWithReload(mountName string, m *config.MountConfig, primaryRootPath string, db *indexdb.DB, baseLog zerolog.Logger, diskCfg DiskAccessConfig, fuseAllowOther bool, rootLogCfg config.LogConfig) (fs.InodeEmbedder, error) {
	rt, err := router.New(m)
	if err != nil {
		return nil, fmt.Errorf("failed to create router: %w", err)
	}

	fuseLog := baseLog.With().Str("component", "fuse").Str("mount", mountName).Logger()
	state := newRuntimeState(rt, fuseLog)
	reload := newReloadState(mountName, m, primaryRootPath, fuseAllowOther, rootLogCfg)
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

	n := &Node{LoopbackNode: lb, mountName: mountName, state: state, reload: reload, db: db, disk: diskLog, open: NewOpenTracker()}
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
	return &Node{LoopbackNode: lb, mountName: n.mountName, state: n.state, reload: n.reload, db: n.db, disk: n.disk, open: n.open}
}

// OpenCounts implements daemonctl.OpenCountsProvider for the daemon control socket.
func (n *Node) OpenCounts(ctx context.Context, files []daemonctl.OpenFileID) ([]daemonctl.OpenStat, error) {
	if n == nil || n.open == nil {
		return nil, nil
	}
	return n.open.OpenCounts(ctx, files)
}

// Lookup resolves a child entry using the router's read target order.
func (n *Node) Lookup(ctx context.Context, name string, out *gofuse.EntryOut) (*fs.Inode, syscall.Errno) {
	rt, log := n.runtime()
	ch, errno := lookupChild(ctx, n.EmbeddedInode(), n.RootData, n.mountName, n.state, n.reload, rt, n.db, log, n.disk, n.open, name, out)
	if errno != 0 && errno != syscall.ENOENT {
		log.Error().Str("op", "lookup").Str("path", filepath.Join(n.Path(n.Root()), name)).Err(errno).Msg("failed to lookup")
	}
	return ch, errno
}

// Getattr reads attributes using the router's read target order.
func (n *Node) Getattr(ctx context.Context, f fs.FileHandle, out *gofuse.AttrOut) syscall.Errno {
	rt, _ := n.runtime()
	return getattrPath(ctx, n.EmbeddedInode(), rt, n.db, out)
}

// Readdir returns a union of directory entries across read targets, deduped by name.
func (n *Node) Readdir(ctx context.Context) (fs.DirStream, syscall.Errno) {
	rt, log := n.runtime()
	start := time.Now()
	ds, errno := readdirPath(ctx, n.EmbeddedInode(), rt, n.db)
	if errno != 0 {
		log.Error().Str("op", "readdir").Str("path", n.Path(n.Root())).Err(errno).Msg("failed to readdir")
	} else {
		log.Debug().Str("op", "readdir").Str("path", n.Path(n.Root())).Int64("duration_ms", time.Since(start).Milliseconds()).Msg("readdir")
	}
	return ds, errno
}

// OpendirHandle returns a directory handle that merges entries across read targets.
func (n *Node) OpendirHandle(ctx context.Context, flags uint32) (fs.FileHandle, uint32, syscall.Errno) {
	rt, log := n.runtime()
	start := time.Now()
	entries, errno := listDirEntries(ctx, n.EmbeddedInode(), rt, n.db)
	if errno != 0 {
		log.Error().Str("op", "opendir").Str("path", n.Path(n.Root())).Err(errno).Msg("failed to opendir")
		return nil, 0, errno
	}
	log.Debug().Str("op", "opendir").Str("path", n.Path(n.Root())).Int("entries", len(entries)).Int64("duration_ms", time.Since(start).Milliseconds()).Msg("opendir")
	return &DirHandle{entries: entries}, 0, 0
}

// Open opens a file and returns a cached FileHandle.
func (n *Node) Open(ctx context.Context, flags uint32) (fs.FileHandle, uint32, syscall.Errno) {
	rt, log := n.runtime()
	virtualPath := n.Path(n.Root())
	write := flags&gofuse.O_ANYWRITE != 0

	start := time.Now()
	fh, openFlags, errno := openFirst(ctx, rt, n.db, virtualPath, int(flags), write)
	if errno != 0 {
		if errno == syscall.EROFS {
			log.Debug().Str("op", "open").Str("path", virtualPath).Bool("write", write).Msg("open blocked: indexed target is read-only")
		} else if errno != syscall.ENOENT {
			log.Error().Str("op", "open").Str("path", virtualPath).Bool("write", write).Err(errno).Msg("failed to open")
		}
		return nil, 0, errno
	}
	if h, ok := fh.(*FileHandle); ok {
		attachOpenTracking(ctx, n, virtualPath, h, write)
		if h.fallback {
			log.Warn().Str("op", "open").Str("path", virtualPath).Str("storage_id", h.storageID).Msg("open: stale real_path fallback triggered")
		}
		log.Debug().Str("op", "open").Str("path", virtualPath).Str("storage_id", h.storageID).Bool("indexed", h.indexed).Bool("write", write).Int64("duration_ms", time.Since(start).Milliseconds()).Msg("open")
		if n.disk != nil {
			n.disk.RecordOpen(ctx, h.storageID, virtualPath, h.indexed)
		}
	}
	return fh, openFlags, errno
}

// Statfs returns filesystem stats for the mount.
//
// The default loopback Statfs reports stats for the primaryRootPath filesystem,
// which may differ from where writes actually land. This override resolves write
// targets via the router so that tools like df and sabnzbd see the correct free
// space for the filesystem that will receive writes at this path.
func (n *Node) Statfs(ctx context.Context, out *gofuse.StatfsOut) syscall.Errno {
	rt, _ := n.runtime()
	virtualPath := n.Path(n.Root())
	if virtualPath == "." {
		virtualPath = ""
	}
	if errno := validateVirtualPath(virtualPath); errno != 0 {
		return errno
	}

	reporting := config.DefaultStatfsReporting
	onError := config.DefaultStatfsOnError
	if n.reload != nil {
		if v := strings.TrimSpace(n.reload.statfs.Reporting); v != "" {
			reporting = v
		}
		if v := strings.TrimSpace(n.reload.statfs.OnError); v != "" {
			onError = v
		}
	}

	pooled := func(roots []string) syscall.Errno {
		if len(roots) == 0 {
			switch onError {
			case "fail_eio":
				return syscall.EIO
			case "fallback_effective_target":
				if statfsWriteTarget(rt, virtualPath, out) {
					return 0
				}
				return n.LoopbackNode.Statfs(ctx, out)
			case "fallback_loopback", "ignore_failed":
				return n.LoopbackNode.Statfs(ctx, out)
			default:
				return n.LoopbackNode.Statfs(ctx, out)
			}
		}

		ok, hadErr := statfsPooledRoots(roots, out)
		if ok && !hadErr {
			return 0
		}
		switch onError {
		case "ignore_failed":
			if ok {
				return 0
			}
			return n.LoopbackNode.Statfs(ctx, out)
		case "fail_eio":
			return syscall.EIO
		case "fallback_effective_target":
			if statfsWriteTarget(rt, virtualPath, out) {
				return 0
			}
			return n.LoopbackNode.Statfs(ctx, out)
		case "fallback_loopback":
			return n.LoopbackNode.Statfs(ctx, out)
		default:
			if ok {
				return 0
			}
			return n.LoopbackNode.Statfs(ctx, out)
		}
	}

	switch reporting {
	case "mount_pooled_targets":
		if rt == nil {
			return pooled(nil)
		}
		targets, err := rt.ResolveMountWriteTargets()
		if err != nil || len(targets) == 0 {
			return pooled(nil)
		}
		roots := make([]string, 0, len(targets))
		for _, t := range targets {
			roots = append(roots, t.Root)
		}
		return pooled(roots)
	case "path_pooled_targets":
		if rt == nil {
			return pooled(nil)
		}
		targets, err := rt.ResolveWriteTargets(virtualPath)
		if err != nil || len(targets) == 0 {
			return pooled(nil)
		}
		roots := make([]string, 0, len(targets))
		for _, t := range targets {
			roots = append(roots, t.Root)
		}
		return pooled(roots)
	default:
		if statfsWriteTarget(rt, virtualPath, out) {
			return 0
		}
		return n.LoopbackNode.Statfs(ctx, out)
	}
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
