package fuse

import (
	"context"
	"errors"
	"fmt"
	"path/filepath"
	"syscall"
	"time"

	"github.com/hanwen/go-fuse/v2/fs"
	gofuse "github.com/hanwen/go-fuse/v2/fuse"
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
	rt, log := n.runtime()
	if rt == nil {
		return fs.ToErrno(&errkind.NilError{What: "router"})
	}
	if flags != 0 {
		// go-fuse uses flags for RENAME_EXCHANGE/RENAME_NOREPLACE; we don't support those yet.
		return syscall.ENOTSUP
	}

	caller, callerOK := gofuse.FromContext(ctx)
	if !callerOK {
		return syscall.EPERM
	}

	np, ok := newParent.(*Node)
	if !ok {
		return syscall.EXDEV
	}
	if np.state != n.state {
		// Different runtime state == different logical filesystem.
		return syscall.EXDEV
	}

	oldParentVirtualPath := n.Path(n.Root())
	if oldParentVirtualPath == "." {
		oldParentVirtualPath = ""
	}
	oldVirtualPath, errno := joinVirtualPath(oldParentVirtualPath, name)
	if errno != 0 {
		return errno
	}

	srcTarget := router.Target{}
	srcPhysicalPath := ""
	srcWasIndexed := false
	srcSt := syscall.Stat_t{}
	haveSrcSt := false
	{
		targets, err := rt.ResolveReadTargets(oldVirtualPath)
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
				srcSt = st
				haveSrcSt = true
				found = true
				break
			}

			if n.db == nil {
				log.Error().Str("op", "rename").Str("path", oldVirtualPath).Str("storage_id", t.ID).Msg("failed to rename: db is nil for indexed target")
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
	if newParentVirtualPath == "." {
		newParentVirtualPath = ""
	}
	newVirtualPath, errno := joinVirtualPath(newParentVirtualPath, newName)
	if errno != 0 {
		return errno
	}

	if !srcWasIndexed && callerOK {
		if !haveSrcSt {
			return syscall.EIO
		}
		oldParentPhysical := filepath.Dir(srcPhysicalPath)
		pst := syscall.Stat_t{}
		if err := syscall.Lstat(oldParentPhysical, &pst); err != nil {
			return fs.ToErrno(err)
		}
		if uint32(pst.Mode)&syscall.S_IFMT != syscall.S_IFDIR {
			return syscall.ENOTDIR
		}
		if errno := dirWriteExecPermErrno(caller, uint32(pst.Mode), pst.Uid, pst.Gid); errno != 0 {
			return errno
		}
		if errno := stickyDirMayRemoveErrno(caller, uint32(pst.Mode), pst.Uid, srcSt.Uid); errno != 0 {
			return errno
		}
	}

	if srcWasIndexed {
		if n.db == nil {
			log.Error().Str("op", "rename").Str("old_path", oldVirtualPath).Str("new_path", newVirtualPath).Msg("failed to rename: db is nil for indexed target")
			return syscall.EIO
		}
		// For indexed targets, rename is a metadata update + deferred physical rename.
		// We still reject renames that would move the entry outside of routing visibility.
		readable, err := rt.ResolveReadTargets(newVirtualPath)
		if err != nil {
			return toErrno(err)
		}
		readableSameTarget := false
		for _, t := range readable {
			if t.ID == srcTarget.ID {
				readableSameTarget = true
				break
			}
		}
		if !readableSameTarget {
			// Treat moves out of the source target's routing domain as cross-device.
			log.Debug().Str("op", "rename").Str("old_path", oldVirtualPath).Str("new_path", newVirtualPath).Str("storage_id", srcTarget.ID).Msg("rename blocked: cross-target")
			return syscall.EXDEV
		}

		if callerOK {
			srcEntry, ok, err := n.db.GetEffectiveFile(ctx, srcTarget.ID, oldVirtualPath)
			if err != nil {
				return toErrno(fmt.Errorf("failed to lookup indexed source: %w", err))
			}
			if !ok {
				return syscall.ENOENT
			}
			if oldParentVirtualPath != "" {
				pdir, ok, err := n.db.GetEffectiveFile(ctx, srcTarget.ID, oldParentVirtualPath)
				if err != nil {
					return toErrno(fmt.Errorf("failed to lookup indexed src parent: %w", err))
				}
				if !ok || !pdir.IsDir {
					return syscall.ENOTDIR
				}
				if errno := dirWriteExecPermErrno(caller, pdir.Mode, pdir.UID, pdir.GID); errno != 0 {
					return errno
				}
				if errno := stickyDirMayRemoveErrno(caller, pdir.Mode, pdir.UID, srcEntry.UID); errno != 0 {
					return errno
				}
			}
			if newParentVirtualPath != "" {
				pdir, ok, err := n.db.GetEffectiveFile(ctx, srcTarget.ID, newParentVirtualPath)
				if err != nil {
					return toErrno(fmt.Errorf("failed to lookup indexed dst parent: %w", err))
				}
				if !ok || !pdir.IsDir {
					return syscall.ENOTDIR
				}
				if errno := dirWriteExecPermErrno(caller, pdir.Mode, pdir.UID, pdir.GID); errno != 0 {
					return errno
				}
				dstEntry, ok, err := n.db.GetEffectiveFile(ctx, srcTarget.ID, newVirtualPath)
				if err != nil {
					return toErrno(fmt.Errorf("failed to lookup indexed overwrite target: %w", err))
				}
				if ok {
					if errno := stickyDirMayRemoveErrno(caller, pdir.Mode, pdir.UID, dstEntry.UID); errno != 0 {
						return errno
					}
				}
			}
		}

		updated, err := n.db.RenamePath(ctx, srcTarget.ID, oldVirtualPath, newVirtualPath)
		if err != nil {
			return fs.ToErrno(err)
		}
		if !updated {
			return syscall.ENOENT
		}
		if err := eventlog.Append(ctx, n.mountName, eventlog.RenameEvent{Type: eventlog.TypeRename, StorageID: srcTarget.ID, OldPath: oldVirtualPath, NewPath: newVirtualPath, TS: time.Now().Unix()}); err != nil {
			log.Error().Str("op", "rename").Str("old_path", oldVirtualPath).Str("new_path", newVirtualPath).Str("storage_id", srcTarget.ID).Err(err).Msg("failed to append eventlog")
			return syscall.EIO
		}
		log.Debug().Str("op", "rename").Str("old_path", oldVirtualPath).Str("new_path", newVirtualPath).Str("storage_id", srcTarget.ID).Bool("indexed", true).Msg("rename")
		return 0
	}

	allowed, err := rt.ResolveWriteTargets(newVirtualPath)
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
		log.Debug().Str("op", "rename").Str("old_path", oldVirtualPath).Str("new_path", newVirtualPath).Str("storage_id", srcTarget.ID).Msg("rename blocked: cross-target")
		return syscall.EXDEV
	}

	dstPhysicalPath := filepath.Join(srcTarget.Root, newVirtualPath)
	// Ensure destination parent dirs exist on the source target.
	if err := materializeParentDirs(ctx, srcTarget.Root, newVirtualPath); err != nil {
		log.Error().Str("op", "rename").Str("old_path", oldVirtualPath).Str("new_path", newVirtualPath).Str("storage_id", srcTarget.ID).Err(err).Msg("failed to materialize parent dirs")
		return fs.ToErrno(err)
	}
	if callerOK {
		newParentPhysical := filepath.Dir(dstPhysicalPath)
		pst := syscall.Stat_t{}
		if err := syscall.Lstat(newParentPhysical, &pst); err != nil {
			return fs.ToErrno(err)
		}
		if uint32(pst.Mode)&syscall.S_IFMT != syscall.S_IFDIR {
			return syscall.ENOTDIR
		}
		if errno := dirWriteExecPermErrno(caller, uint32(pst.Mode), pst.Uid, pst.Gid); errno != 0 {
			return errno
		}
		dstSt := syscall.Stat_t{}
		if err := syscall.Lstat(dstPhysicalPath, &dstSt); err == nil {
			if errno := stickyDirMayRemoveErrno(caller, uint32(pst.Mode), pst.Uid, dstSt.Uid); errno != 0 {
				return errno
			}
		} else if !errors.Is(err, syscall.ENOENT) {
			return fs.ToErrno(err)
		}
	}
	renameErrno := fs.ToErrno(syscall.Rename(srcPhysicalPath, dstPhysicalPath))
	if renameErrno != 0 {
		log.Error().Str("op", "rename").Str("old_path", oldVirtualPath).Str("new_path", newVirtualPath).Str("storage_id", srcTarget.ID).Err(renameErrno).Msg("failed to rename")
	} else {
		log.Debug().Str("op", "rename").Str("old_path", oldVirtualPath).Str("new_path", newVirtualPath).Str("storage_id", srcTarget.ID).Bool("indexed", false).Msg("rename")
	}
	return renameErrno
}
