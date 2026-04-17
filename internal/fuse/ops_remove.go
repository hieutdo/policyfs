package fuse

import (
	"context"
	"errors"
	"fmt"
	"path/filepath"
	"syscall"
	"time"

	gofuse "github.com/hanwen/go-fuse/v2/fuse"
	"github.com/hieutdo/policyfs/internal/errkind"
	"github.com/hieutdo/policyfs/internal/eventlog"
)

// Unlink removes a child file on the first existing read target.
func (n *Node) Unlink(ctx context.Context, name string) syscall.Errno {
	if n == nil {
		return toErrno(&errkind.NilError{What: "node"})
	}
	rt, log := n.runtime()
	if rt == nil {
		return toErrno(&errkind.NilError{What: "router"})
	}

	caller, callerOK := gofuse.FromContext(ctx)
	if !callerOK {
		return syscall.EPERM
	}

	parentVirtualPath := n.Path(n.Root())
	if parentVirtualPath == "." {
		parentVirtualPath = ""
	}
	virtualPath, errno := joinVirtualPath(parentVirtualPath, name)
	if errno != 0 {
		return errno
	}

	targets, err := rt.ResolveReadTargets(virtualPath)
	if err != nil {
		return toErrno(err)
	}

	for _, t := range targets {
		if !t.Indexed {
			physicalPath := filepath.Join(t.Root, virtualPath)
			st := syscall.Stat_t{}
			if err := syscall.Lstat(physicalPath, &st); err != nil {
				if errors.Is(err, syscall.ENOENT) {
					continue
				}
				return toErrno(err)
			}
			if uint32(st.Mode)&syscall.S_IFMT == syscall.S_IFDIR {
				return syscall.EISDIR
			}
			if callerOK {
				parentPhysical := filepath.Dir(physicalPath)
				pst := syscall.Stat_t{}
				if err := syscall.Lstat(parentPhysical, &pst); err != nil {
					return toErrno(err)
				}
				if uint32(pst.Mode)&syscall.S_IFMT != syscall.S_IFDIR {
					return syscall.ENOTDIR
				}
				if errno := dirWriteExecPermErrno(caller, uint32(pst.Mode), pst.Uid, pst.Gid); errno != 0 {
					return errno
				}
				if errno := stickyDirMayRemoveErrno(caller, uint32(pst.Mode), pst.Uid, st.Uid); errno != 0 {
					return errno
				}
			}
			errno := toErrno(syscall.Unlink(physicalPath))
			if errno != 0 {
				log.Error().Str("op", "unlink").Str("path", virtualPath).Str("storage_id", t.ID).Err(errno).Msg("failed to unlink")
			} else {
				log.Debug().Str("op", "unlink").Str("path", virtualPath).Str("storage_id", t.ID).Bool("indexed", false).Msg("unlink")
			}
			return errno
		}

		if n.db == nil {
			log.Error().Str("op", "unlink").Str("path", virtualPath).Str("storage_id", t.ID).Msg("failed to unlink: db is nil for indexed target")
			return syscall.EIO
		}
		f, ok, err := n.db.GetEffectiveFile(ctx, t.ID, virtualPath)
		if err != nil {
			return toErrno(fmt.Errorf("failed to lookup indexed file: %w", err))
		}
		if !ok {
			continue
		}
		if f.IsDir {
			return syscall.EISDIR
		}
		if callerOK && parentVirtualPath != "" {
			pdir, ok, err := n.db.GetEffectiveFile(ctx, t.ID, parentVirtualPath)
			if err != nil {
				return toErrno(fmt.Errorf("failed to lookup indexed parent dir: %w", err))
			}
			if !ok {
				return syscall.ENOENT
			}
			if !pdir.IsDir {
				return syscall.ENOTDIR
			}
			if errno := dirWriteExecPermErrno(caller, pdir.Mode, pdir.UID, pdir.GID); errno != 0 {
				return errno
			}
			if errno := stickyDirMayRemoveErrno(caller, pdir.Mode, pdir.UID, f.UID); errno != 0 {
				return errno
			}
		}
		updated, err := n.db.MarkDeleted(ctx, t.ID, virtualPath, false)
		if err != nil {
			return toErrno(err)
		}
		if !updated {
			continue
		}
		if err := eventlog.Append(ctx, n.mountName, eventlog.DeleteEvent{Type: eventlog.TypeDelete, StorageID: t.ID, Path: virtualPath, IsDir: false, TS: time.Now().Unix()}); err != nil {
			log.Error().Str("op", "unlink").Str("path", virtualPath).Str("storage_id", t.ID).Err(err).Msg("failed to append eventlog")
			return syscall.EIO
		}
		log.Debug().Str("op", "unlink").Str("path", virtualPath).Str("storage_id", t.ID).Bool("indexed", true).Msg("unlink")
		return 0
	}

	return syscall.ENOENT
}

// Rmdir removes a child directory on the first existing read target.
func (n *Node) Rmdir(ctx context.Context, name string) syscall.Errno {
	if n == nil {
		return toErrno(&errkind.NilError{What: "node"})
	}
	rt, log := n.runtime()
	if rt == nil {
		return toErrno(&errkind.NilError{What: "router"})
	}

	caller, callerOK := gofuse.FromContext(ctx)
	if !callerOK {
		return syscall.EPERM
	}

	parentVirtualPath := n.Path(n.Root())
	if parentVirtualPath == "." {
		parentVirtualPath = ""
	}
	virtualPath, errno := joinVirtualPath(parentVirtualPath, name)
	if errno != 0 {
		return errno
	}

	targets, err := rt.ResolveReadTargets(virtualPath)
	if err != nil {
		return toErrno(err)
	}

	for _, t := range targets {
		if !t.Indexed {
			physicalPath := filepath.Join(t.Root, virtualPath)
			st := syscall.Stat_t{}
			if err := syscall.Lstat(physicalPath, &st); err != nil {
				if errors.Is(err, syscall.ENOENT) {
					continue
				}
				return toErrno(err)
			}
			if uint32(st.Mode)&syscall.S_IFMT != syscall.S_IFDIR {
				return syscall.ENOTDIR
			}
			if callerOK {
				parentPhysical := filepath.Dir(physicalPath)
				pst := syscall.Stat_t{}
				if err := syscall.Lstat(parentPhysical, &pst); err != nil {
					return toErrno(err)
				}
				if uint32(pst.Mode)&syscall.S_IFMT != syscall.S_IFDIR {
					return syscall.ENOTDIR
				}
				if errno := dirWriteExecPermErrno(caller, uint32(pst.Mode), pst.Uid, pst.Gid); errno != 0 {
					return errno
				}
				if errno := stickyDirMayRemoveErrno(caller, uint32(pst.Mode), pst.Uid, st.Uid); errno != 0 {
					return errno
				}
			}
			errno := toErrno(syscall.Rmdir(physicalPath))
			if errno != 0 {
				log.Error().Str("op", "rmdir").Str("path", virtualPath).Str("storage_id", t.ID).Err(errno).Msg("failed to rmdir")
			} else {
				log.Debug().Str("op", "rmdir").Str("path", virtualPath).Str("storage_id", t.ID).Bool("indexed", false).Msg("rmdir")
			}
			return errno
		}

		if n.db == nil {
			log.Error().Str("op", "rmdir").Str("path", virtualPath).Str("storage_id", t.ID).Msg("failed to rmdir: db is nil for indexed target")
			return syscall.EIO
		}
		f, ok, err := n.db.GetEffectiveFile(ctx, t.ID, virtualPath)
		if err != nil {
			return toErrno(fmt.Errorf("failed to lookup indexed dir: %w", err))
		}
		if !ok {
			continue
		}
		if !f.IsDir {
			return syscall.ENOTDIR
		}
		if callerOK && parentVirtualPath != "" {
			pdir, ok, err := n.db.GetEffectiveFile(ctx, t.ID, parentVirtualPath)
			if err != nil {
				return toErrno(fmt.Errorf("failed to lookup indexed parent dir: %w", err))
			}
			if !ok {
				return syscall.ENOENT
			}
			if !pdir.IsDir {
				return syscall.ENOTDIR
			}
			if errno := dirWriteExecPermErrno(caller, pdir.Mode, pdir.UID, pdir.GID); errno != 0 {
				return errno
			}
			if errno := stickyDirMayRemoveErrno(caller, pdir.Mode, pdir.UID, f.UID); errno != 0 {
				return errno
			}
		}
		updated, err := n.db.MarkDeleted(ctx, t.ID, virtualPath, true)
		if err != nil {
			return toErrno(err)
		}
		if !updated {
			continue
		}
		if err := eventlog.Append(ctx, n.mountName, eventlog.DeleteEvent{Type: eventlog.TypeDelete, StorageID: t.ID, Path: virtualPath, IsDir: true, TS: time.Now().Unix()}); err != nil {
			log.Error().Str("op", "rmdir").Str("path", virtualPath).Str("storage_id", t.ID).Err(err).Msg("failed to append eventlog")
			return syscall.EIO
		}
		log.Debug().Str("op", "rmdir").Str("path", virtualPath).Str("storage_id", t.ID).Bool("indexed", true).Msg("rmdir")
		return 0
	}

	return syscall.ENOENT
}
