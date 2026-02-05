package fuse

import (
	"context"
	"errors"
	"path/filepath"
	"syscall"

	"github.com/hanwen/go-fuse/v2/fs"
	gofuse "github.com/hanwen/go-fuse/v2/fuse"
)

// Link creates a hardlink to an existing inode.
//
// Cross-target hardlinks are not supported and must return EXDEV.
func (n *Node) Link(ctx context.Context, target fs.InodeEmbedder, name string, out *gofuse.EntryOut) (*fs.Inode, syscall.Errno) {
	if n == nil {
		return nil, fs.ToErrno(errors.New("node is nil"))
	}
	if n.rt == nil {
		return nil, fs.ToErrno(errors.New("router is nil"))
	}
	if target == nil {
		return nil, fs.ToErrno(errors.New("target is nil"))
	}

	if tn, ok := target.(*Node); ok {
		if tn.rt != n.rt {
			return nil, syscall.EXDEV
		}
	}

	tino := target.EmbeddedInode()
	if tino == nil {
		return nil, fs.ToErrno(errors.New("target inode is nil"))
	}

	parentVirtualPath := n.Path(n.Root())
	newVirtualPath := filepath.Join(parentVirtualPath, name)

	oldVirtualPath := tino.Path(tino.Root())
	// Source must exist on some read target; we hardlink from the first existing physical file.
	srcTarget, srcPhysicalPath, errno := firstExistingPhysical(n.rt, oldVirtualPath)
	if errno != 0 {
		return nil, errno
	}
	// Indexed targets are not writable yet.
	if srcTarget.Indexed {
		return nil, syscall.EROFS
	}

	// Cross-target hardlinks are not allowed; destination path must be writable on the same target.
	allowed, err := n.rt.ResolveWriteTargets(newVirtualPath)
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
		return nil, syscall.EXDEV
	}

	dstPhysicalPath := filepath.Join(srcTarget.Root, newVirtualPath)
	// Ensure the destination parent dirs exist on the source target.
	if err := materializeParentDirs(ctx, srcTarget.Root, newVirtualPath); err != nil {
		return nil, fs.ToErrno(err)
	}
	if err := syscall.Link(srcPhysicalPath, dstPhysicalPath); err != nil {
		return nil, fs.ToErrno(err)
	}

	st := syscall.Stat_t{}
	if err := syscall.Lstat(dstPhysicalPath, &st); err != nil {
		return nil, fs.ToErrno(err)
	}
	out.FromStat(&st)

	ch := newChildInode(ctx, n.EmbeddedInode(), n.RootData, n.rt, uint32(st.Mode))
	return ch, 0
}
