package fuse

import (
	"context"
	"path/filepath"
	"syscall"

	"github.com/hanwen/go-fuse/v2/fs"
	gofuse "github.com/hanwen/go-fuse/v2/fuse"
	"github.com/hieutdo/policyfs/internal/errkind"
)

// Link creates a hardlink to an existing inode.
//
// Cross-target hardlinks are not supported and must return EXDEV.
func (n *Node) Link(ctx context.Context, target fs.InodeEmbedder, name string, out *gofuse.EntryOut) (*fs.Inode, syscall.Errno) {
	if n == nil {
		return nil, toErrno(&errkind.NilError{What: "node"})
	}
	rt, log := n.runtime()
	if rt == nil {
		return nil, toErrno(&errkind.NilError{What: "router"})
	}
	if target == nil {
		return nil, toErrno(&errkind.NilError{What: "target"})
	}

	if tn, ok := target.(*Node); ok {
		if tn.state != n.state {
			return nil, syscall.EXDEV
		}
	}

	tino := target.EmbeddedInode()
	if tino == nil {
		return nil, toErrno(&errkind.NilError{What: "target inode"})
	}

	caller, callerOK := gofuse.FromContext(ctx)
	if !callerOK {
		return nil, syscall.EPERM
	}

	parentVirtualPath := n.Path(n.Root())
	if parentVirtualPath == "." {
		parentVirtualPath = ""
	}
	newVirtualPath, errno := joinVirtualPath(parentVirtualPath, name)
	if errno != 0 {
		return nil, errno
	}

	oldVirtualPath := tino.Path(tino.Root())
	if oldVirtualPath == "." {
		oldVirtualPath = ""
	}
	// Source must exist on some read target; we hardlink from the first existing physical file.
	srcTarget, srcPhysicalPath, errno := firstExistingPhysical(rt, oldVirtualPath)
	if errno != 0 {
		return nil, errno
	}
	// Indexed targets are not writable yet.
	if srcTarget.Indexed {
		log.Debug().Str("op", "link").Str("path", newVirtualPath).Str("storage_id", srcTarget.ID).Msg("link blocked: indexed target is read-only")
		return nil, syscall.EROFS
	}

	// Cross-target hardlinks are not allowed; destination path must be writable on the same target.
	allowed, err := rt.ResolveWriteTargets(newVirtualPath)
	if err != nil {
		return nil, toErrno(err)
	}
	allowedSameTarget := false
	for _, t := range allowed {
		if t.ID == srcTarget.ID {
			allowedSameTarget = true
			break
		}
	}
	if !allowedSameTarget {
		log.Debug().Str("op", "link").Str("old_path", oldVirtualPath).Str("new_path", newVirtualPath).Str("storage_id", srcTarget.ID).Msg("link blocked: cross-target")
		return nil, syscall.EXDEV
	}

	dstPhysicalPath := filepath.Join(srcTarget.Root, newVirtualPath)
	// Ensure the destination parent dirs exist on the source target.
	if err := materializeParentDirs(ctx, srcTarget.Root, newVirtualPath); err != nil {
		return nil, toErrno(err)
	}
	if callerOK {
		parentPhysical := filepath.Dir(dstPhysicalPath)
		pst := syscall.Stat_t{}
		if err := syscall.Lstat(parentPhysical, &pst); err != nil {
			return nil, toErrno(err)
		}
		if uint32(pst.Mode)&syscall.S_IFMT != syscall.S_IFDIR {
			return nil, syscall.ENOTDIR
		}
		if errno := dirWriteExecPermErrno(caller, uint32(pst.Mode), pst.Uid, pst.Gid); errno != 0 {
			return nil, errno
		}
	}
	if err := syscall.Link(srcPhysicalPath, dstPhysicalPath); err != nil {
		log.Error().Str("op", "link").Str("old_path", oldVirtualPath).Str("new_path", newVirtualPath).Str("storage_id", srcTarget.ID).Err(err).Msg("failed to link")
		return nil, toErrno(err)
	}

	st := syscall.Stat_t{}
	if err := syscall.Lstat(dstPhysicalPath, &st); err != nil {
		return nil, toErrno(err)
	}
	out.FromStat(&st)

	ch := newChildInode(ctx, n.EmbeddedInode(), n.RootData, n.mountName, n.state, n.reload, n.db, n.disk, n.open, srcTarget.ID, newVirtualPath, uint32(st.Mode))
	log.Debug().Str("op", "link").Str("old_path", oldVirtualPath).Str("new_path", newVirtualPath).Str("storage_id", srcTarget.ID).Msg("link")
	return ch, 0
}
