package fuse

import (
	"context"
	"errors"
	"fmt"
	"path/filepath"
	"syscall"
	"time"

	"github.com/hanwen/go-fuse/v2/fs"
	"github.com/hieutdo/policyfs/internal/errkind"
	"github.com/hieutdo/policyfs/internal/eventlog"
	"github.com/hieutdo/policyfs/internal/router"
)

// Rename renames a child within the same underlying target.
//
// Cross-target renames return EXDEV.
func (n *Node) Rename(ctx context.Context, name string, newParent fs.InodeEmbedder, newName string, flags uint32) syscall.Errno {
	if n == nil {
		return fs.ToErrno(&errkind.NilError{What: "node"})
	}
	if n.rt == nil {
		return fs.ToErrno(&errkind.NilError{What: "router"})
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

	srcTarget := router.Target{}
	srcPhysicalPath := ""
	srcWasIndexed := false
	{
		targets, err := n.rt.ResolveReadTargets(oldVirtualPath)
		if err != nil {
			return toErrno(err)
		}
		found := false
		for _, t := range targets {
			if !t.Indexed {
				p := filepath.Join(t.Root, oldVirtualPath)
				st := syscall.Stat_t{}
				if err := syscall.Lstat(p, &st); err != nil {
					if errors.Is(err, syscall.ENOENT) {
						continue
					}
					return fs.ToErrno(err)
				}
				srcTarget = t
				srcPhysicalPath = p
				srcWasIndexed = false
				found = true
				break
			}

			if n.db == nil {
				return syscall.EIO
			}
			_, ok, err := n.db.GetEffectiveFile(ctx, t.ID, oldVirtualPath)
			if err != nil {
				return toErrno(fmt.Errorf("failed to lookup indexed source: %w", err))
			}
			if ok {
				srcTarget = t
				srcWasIndexed = true
				found = true
				break
			}
			dirOK, err := n.db.DirExists(ctx, t.ID, oldVirtualPath)
			if err != nil {
				return toErrno(fmt.Errorf("failed to lookup indexed source dir: %w", err))
			}
			if dirOK {
				srcTarget = t
				srcWasIndexed = true
				found = true
				break
			}
		}
		if !found {
			return syscall.ENOENT
		}
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

	if srcWasIndexed {
		if n.db == nil {
			return syscall.EIO
		}
		updated, err := n.db.RenamePath(ctx, srcTarget.ID, oldVirtualPath, newVirtualPath)
		if err != nil {
			return fs.ToErrno(err)
		}
		if !updated {
			return syscall.ENOENT
		}
		if err := eventlog.Append(ctx, n.mountName, eventlog.RenameEvent{Type: eventlog.TypeRename, StorageID: srcTarget.ID, OldPath: oldVirtualPath, NewPath: newVirtualPath, TS: time.Now().Unix()}); err != nil {
			return syscall.EIO
		}
		return 0
	}

	dstPhysicalPath := filepath.Join(srcTarget.Root, newVirtualPath)
	// Ensure destination parent dirs exist on the source target.
	if err := materializeParentDirs(ctx, srcTarget.Root, newVirtualPath); err != nil {
		return fs.ToErrno(err)
	}
	_ = ctx
	return fs.ToErrno(syscall.Rename(srcPhysicalPath, dstPhysicalPath))
}
