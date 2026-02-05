package fuse

import (
	"context"
	"errors"
	"path/filepath"
	"syscall"

	"github.com/hanwen/go-fuse/v2/fs"
)

// Rename renames a child within the same underlying target.
//
// Cross-target renames return EXDEV.
func (n *Node) Rename(ctx context.Context, name string, newParent fs.InodeEmbedder, newName string, flags uint32) syscall.Errno {
	if n == nil {
		return fs.ToErrno(errors.New("node is nil"))
	}
	if n.rt == nil {
		return fs.ToErrno(errors.New("router is nil"))
	}
	if flags != 0 {
		// go-fuse uses flags for RENAME_EXCHANGE/RENAME_NOREPLACE; we don't support those yet.
		return syscall.ENOTSUP
	}

	np, ok := newParent.(*Node)
	if !ok {
		return syscall.EXDEV
	}
	if np.rt != n.rt {
		// Different router == different logical filesystem.
		return syscall.EXDEV
	}

	oldParentVirtualPath := n.Path(n.Root())
	oldVirtualPath := filepath.Join(oldParentVirtualPath, name)

	srcTarget, srcPhysicalPath, errno := firstExistingPhysical(n.rt, oldVirtualPath)
	if errno != 0 {
		return errno
	}
	// Indexed targets are not writable yet.
	if srcTarget.Indexed {
		return syscall.EROFS
	}

	newParentVirtualPath := np.Path(np.Root())
	newVirtualPath := filepath.Join(newParentVirtualPath, newName)

	allowed, err := n.rt.ResolveWriteTargets(newVirtualPath)
	if err != nil {
		return toErrno(err)
	}
	allowedSameTarget := false
	for _, t := range allowed {
		if t.ID == srcTarget.ID {
			allowedSameTarget = true
			break
		}
	}
	if !allowedSameTarget {
		// Cross-target rename is not supported.
		return syscall.EXDEV
	}

	dstPhysicalPath := filepath.Join(srcTarget.Root, newVirtualPath)
	// Ensure destination parent dirs exist on the source target.
	if err := materializeParentDirs(ctx, srcTarget.Root, newVirtualPath); err != nil {
		return fs.ToErrno(err)
	}
	_ = ctx
	return fs.ToErrno(syscall.Rename(srcPhysicalPath, dstPhysicalPath))
}
