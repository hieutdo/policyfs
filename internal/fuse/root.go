package fuse

import (
	"context"
	"errors"
	"fmt"
	"path/filepath"
	"strings"
	"sync"
	"syscall"
	"time"

	"github.com/hanwen/go-fuse/v2/fs"
	gofuse "github.com/hanwen/go-fuse/v2/fuse"
	"github.com/hieutdo/policyfs/internal/config"
	"github.com/hieutdo/policyfs/internal/daemonctl"
	"github.com/hieutdo/policyfs/internal/indexdb"
	"github.com/hieutdo/policyfs/internal/router"
	"github.com/rs/zerolog"
	"golang.org/x/sys/unix"
)

// Node is a PolicyFS inode implementation (including the root inode).
type Node struct {
	fs.Inode
	mountName        string
	state            *runtimeState
	reload           *reloadState
	db               *indexdb.DB
	disk             *diskAccessLogger
	open             *OpenTracker
	handlesMu        sync.Mutex
	unindexedHandles map[*FileHandle]struct{}
}

// runtime returns a stable snapshot of the current router/logger for this mount.
func (n *Node) runtime() (*router.Router, zerolog.Logger) {
	if n == nil || n.state == nil {
		return nil, zerolog.Logger{}
	}
	return n.state.Snapshot()
}

// NewRoot creates the PolicyFS root node for mounting.
func NewRoot(mountName string, m *config.MountConfig, primaryRootPath string, db *indexdb.DB, baseLog zerolog.Logger, diskCfg DiskAccessConfig) (fs.InodeEmbedder, error) {
	return NewRootWithReload(mountName, m, primaryRootPath, db, baseLog, diskCfg, false, config.LogConfig{})
}

// NewRootWithReload creates the PolicyFS root node for mounting, including reload state.
func NewRootWithReload(mountName string, m *config.MountConfig, primaryRootPath string, db *indexdb.DB, baseLog zerolog.Logger, diskCfg DiskAccessConfig, fuseAllowOther bool, rootLogCfg config.LogConfig) (fs.InodeEmbedder, error) {
	rt, err := router.New(m)
	if err != nil {
		return nil, fmt.Errorf("failed to create router: %w", err)
	}

	var st syscall.Stat_t
	if err := syscall.Stat(primaryRootPath, &st); err != nil {
		return nil, fmt.Errorf("failed to stat primary root: %w", err)
	}

	fuseLog := baseLog.With().Str("component", "fuse").Str("mount", mountName).Logger()
	state := newRuntimeState(rt, fuseLog)
	reload := newReloadState(mountName, m, primaryRootPath, fuseAllowOther, rootLogCfg)
	diskLog := newDiskAccessLogger(fuseLog, diskCfg)

	n := &Node{mountName: mountName, state: state, reload: reload, db: db, disk: diskLog, open: NewOpenTracker()}
	return n, nil
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
	ch, errno := lookupChild(ctx, n.EmbeddedInode(), n.mountName, n.state, n.reload, rt, n.db, log, n.disk, n.open, name, out)
	if errno != 0 && errno != syscall.ENOENT {
		log.Error().Str("op", "lookup").Str("path", filepath.Join(n.Path(n.Root()), name)).Err(errno).Msg("failed to lookup")
	}
	return ch, errno
}

// Getattr reads attributes using the router's read target order.
func (n *Node) Getattr(ctx context.Context, f fs.FileHandle, out *gofuse.AttrOut) syscall.Errno {
	if h, ok := f.(*FileHandle); ok && !h.indexed {
		var st syscall.Stat_t
		if err := syscall.Fstat(h.fd, &st); err != nil {
			return toErrno(err)
		}
		out.FromStat(&st)
		out.Ino = n.StableAttr().Ino
		return 0
	}
	rt, log := n.runtime()
	resolved, errno := resolveInodeMetadata(ctx, n.EmbeddedInode(), rt, n.db, log, metadataRequest{op: "getattr", readPhysical: lstatMetadata})
	if errno != 0 {
		if errno != syscall.ENOENT {
			log.Error().Str("op", "getattr").Str("path", n.Path(n.Root())).Err(errno).Msg("failed to getattr")
		}
		return errno
	}
	out.Attr = resolved.attr
	return 0
}

// Statx resolves metadata through the router and keeps indexed reads in the database.
func (n *Node) Statx(ctx context.Context, f fs.FileHandle, flags uint32, mask uint32, out *gofuse.StatxOut) syscall.Errno {
	syncFlags := int(flags) & (unix.AT_STATX_FORCE_SYNC | unix.AT_STATX_DONT_SYNC)
	if h, ok := f.(*FileHandle); ok && !h.indexed {
		return n.statxHandle(h, syncFlags, mask, out)
	}
	// Linux can omit the handle on FUSE_STATX, even for AT_EMPTY_PATH.
	// A detached inode has no pathname left to resolve.
	if f == nil && !n.IsRoot() {
		if _, parent := n.Parent(); parent == nil {
			n.handlesMu.Lock()
			defer n.handlesMu.Unlock()
			for h := range n.unindexedHandles {
				return n.statxHandle(h, syncFlags, mask, out)
			}
			return syscall.ENOENT
		}
	}
	rt, log := n.runtime()
	req := metadataRequest{op: "statx", readPhysical: func(path string) (*resolvedLookupPath, syscall.Errno) {
		var st unix.Statx_t
		// Directory-only fallback needs the type even when the caller requests only birth time.
		if err := unix.Statx(unix.AT_FDCWD, path, unix.AT_SYMLINK_NOFOLLOW|syncFlags, int(mask)|unix.STATX_TYPE, &st); err != nil {
			return nil, toErrno(err)
		}
		return &resolvedLookupPath{attr: gofuse.Attr{Mode: uint32(st.Mode)}, statx: &st}, 0
	}}
	resolved, errno := resolveInodeMetadata(ctx, n.EmbeddedInode(), rt, n.db, log, req)
	if errno != 0 {
		return errno
	}
	if resolved.statx != nil {
		out.FromStatx(resolved.statx)
		out.Ino = n.StableAttr().Ino
		return 0
	}

	attr := resolved.attr
	out.Statx = gofuse.Statx{
		Mask:  unix.STATX_TYPE | unix.STATX_MODE | unix.STATX_NLINK | unix.STATX_UID | unix.STATX_GID | unix.STATX_MTIME | unix.STATX_INO | unix.STATX_SIZE,
		Mode:  uint16(attr.Mode),
		Nlink: attr.Nlink,
		Uid:   attr.Uid,
		Gid:   attr.Gid,
		Ino:   n.StableAttr().Ino,
		Mtime: gofuse.SxTime{Sec: attr.Mtime, Nsec: attr.Mtimensec},
		Size:  attr.Size,
	}
	return 0
}

// statxHandle reads the backing inode held open by the caller.
func (n *Node) statxHandle(h *FileHandle, flags int, mask uint32, out *gofuse.StatxOut) syscall.Errno {
	var st unix.Statx_t
	if err := unix.Statx(h.fd, "", unix.AT_EMPTY_PATH|flags, int(mask), &st); err != nil {
		return toErrno(err)
	}
	out.FromStatx(&st)
	out.Ino = n.StableAttr().Ino
	return 0
}

// rememberHandle keeps unindexed descriptors available for detached inode metadata.
func (n *Node) rememberHandle(h *FileHandle) {
	if h.indexed {
		return
	}
	n.handlesMu.Lock()
	defer n.handlesMu.Unlock()
	if n.unindexedHandles == nil {
		n.unindexedHandles = make(map[*FileHandle]struct{})
	}
	n.unindexedHandles[h] = struct{}{}
}

// Readdir returns a union of directory entries across read targets, deduped by name.
func (n *Node) Readdir(ctx context.Context) (fs.DirStream, syscall.Errno) {
	rt, log := n.runtime()
	start := time.Now()
	ds, errno := readdirPath(ctx, n.EmbeddedInode(), rt, n.db, log)
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
	entries, errno := listDirEntries(ctx, n.EmbeddedInode(), rt, n.db, log)
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
	fh, openFlags, errno := openFirst(ctx, rt, n.db, log, virtualPath, int(flags), write)
	if errno != 0 {
		if errno == syscall.EROFS {
			log.Debug().Str("op", "open").Str("path", virtualPath).Bool("write", write).Msg("open blocked: indexed target is read-only")
		} else if errno != syscall.ENOENT {
			log.Error().Str("op", "open").Str("path", virtualPath).Bool("write", write).Err(errno).Msg("failed to open")
		}
		return nil, 0, errno
	}
	if h, ok := fh.(*FileHandle); ok {
		n.rememberHandle(h)
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
				return n.statfsLoopback(virtualPath, out)
			case "fallback_loopback", "ignore_failed":
				return n.statfsLoopback(virtualPath, out)
			default:
				return n.statfsLoopback(virtualPath, out)
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
			return n.statfsLoopback(virtualPath, out)
		case "fail_eio":
			return syscall.EIO
		case "fallback_effective_target":
			if statfsWriteTarget(rt, virtualPath, out) {
				return 0
			}
			return n.statfsLoopback(virtualPath, out)
		case "fallback_loopback":
			return n.statfsLoopback(virtualPath, out)
		default:
			if ok {
				return 0
			}
			return n.statfsLoopback(virtualPath, out)
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
		return n.statfsLoopback(virtualPath, out)
	}
}

// statfsLoopback reports filesystem stats from primaryRootPath when pooled or target statfs falls back.
func (n *Node) statfsLoopback(virtualPath string, out *gofuse.StatfsOut) syscall.Errno {
	if n == nil || n.reload == nil || strings.TrimSpace(n.reload.primaryRootPath) == "" {
		return syscall.EIO
	}
	primaryRoot := n.reload.primaryRootPath
	p := filepath.Join(primaryRoot, virtualPath)
	var st syscall.Statfs_t
	err := syscall.Statfs(p, &st)
	if err != nil && errors.Is(err, syscall.ENOENT) && p != primaryRoot {
		err = syscall.Statfs(primaryRoot, &st)
	}
	if err != nil {
		return toErrno(err)
	}
	out.FromStatfsT(&st)
	return 0
}

// Release closes any file handles we created.
func (n *Node) Release(ctx context.Context, f fs.FileHandle) syscall.Errno {
	_, log := n.runtime()
	start := time.Now()
	fh, fhOK := f.(*FileHandle)
	if fhOK && fh != nil && !fh.indexed {
		n.handlesMu.Lock()
		delete(n.unindexedHandles, fh)
		n.handlesMu.Unlock()
	}
	if r, ok := f.(interface {
		Release(ctx context.Context) syscall.Errno
	}); ok {
		errno := r.Release(ctx)
		if fhOK && fh != nil && fh.flags&gofuse.O_ANYWRITE != 0 {
			durMs := time.Since(start).Milliseconds()
			if errno != 0 {
				log.Error().Str("op", "release").Str("path", fh.virtualPath).Str("storage_id", fh.storageID).Bool("indexed", fh.indexed).Bool("write", true).Int64("duration_ms", durMs).Err(errno).Msg("failed to release")
				return errno
			}
			log.Debug().Str("op", "release").Str("path", fh.virtualPath).Str("storage_id", fh.storageID).Bool("indexed", fh.indexed).Bool("write", true).Int64("duration_ms", durMs).Msg("release")
		}
		return errno
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
