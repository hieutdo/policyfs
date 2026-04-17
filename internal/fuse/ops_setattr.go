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
	"golang.org/x/sys/unix"
)

// Setattr applies attribute changes to the underlying storage.
func (n *Node) Setattr(ctx context.Context, f fs.FileHandle, in *gofuse.SetAttrIn, out *gofuse.AttrOut) syscall.Errno {
	if n == nil {
		return toErrno(&errkind.NilError{What: "node"})
	}
	rt, log := n.runtime()
	if rt == nil {
		return toErrno(&errkind.NilError{What: "router"})
	}

	caller, callerOK := gofuse.FromContext(ctx)

	virtualPath := n.Path(n.Root())
	if errno := validateVirtualPath(virtualPath); errno != 0 {
		return errno
	}
	physicalPath := ""
	indexed := false
	indexedStorageID := ""
	var fh *FileHandle

	// Resolve the physical path.
	// - If we have a file handle, we must operate on that exact underlying fd/path.
	// - Otherwise we operate on the first existing physical entry from read targets.
	if h, ok := f.(*FileHandle); ok && h != nil {
		fh = h
		physicalPath = h.physicalPath
		indexed = h.indexed
		indexedStorageID = h.storageID
	} else {
		targets, err := rt.ResolveReadTargets(virtualPath)
		if err != nil {
			return toErrno(err)
		}
		found := false
		for _, t := range targets {
			if !t.Indexed {
				p := filepath.Join(t.Root, virtualPath)
				st := syscall.Stat_t{}
				if err := syscall.Lstat(p, &st); err != nil {
					if errors.Is(err, syscall.ENOENT) {
						continue
					}
					return toErrno(err)
				}
				indexed = false
				indexedStorageID = t.ID
				physicalPath = p
				found = true
				break
			}

			if n.db == nil {
				log.Error().Str("op", "setattr").Str("path", virtualPath).Str("storage_id", t.ID).Msg("failed to setattr: db is nil for indexed target")
				return syscall.EIO
			}
			_, ok, err := n.db.GetEffectiveFile(ctx, t.ID, virtualPath)
			if err != nil {
				return toErrno(fmt.Errorf("failed to getattr indexed entry: %w", err))
			}
			if ok {
				indexed = true
				indexedStorageID = t.ID
				found = true
				break
			}
			dirOK, err := n.db.DirExists(ctx, t.ID, virtualPath)
			if err != nil {
				return toErrno(fmt.Errorf("failed to getattr indexed dir: %w", err))
			}
			if dirOK {
				indexed = true
				indexedStorageID = t.ID
				found = true
				break
			}
		}
		if !found {
			return syscall.ENOENT
		}
	}
	// Indexed targets use deferred SETATTR (no physical ops).
	if indexed {
		if n.db == nil {
			log.Error().Str("op", "setattr").Str("path", virtualPath).Str("storage_id", indexedStorageID).Msg("failed to setattr: db is nil for indexed target")
			return syscall.EIO
		}
		cur, ok, err := n.db.GetEffectiveFile(ctx, indexedStorageID, virtualPath)
		if err != nil {
			return toErrno(fmt.Errorf("failed to getattr indexed file: %w", err))
		}
		if !ok {
			return syscall.ENOENT
		}

		if _, ok := in.GetSize(); ok {
			log.Debug().Str("op", "setattr").Str("path", virtualPath).Str("storage_id", indexedStorageID).Msg("setattr blocked: truncate on indexed target")
			return syscall.EROFS
		}

		uid, uok := in.GetUID()
		gid, gok := in.GetGID()
		if uok || gok {
			if callerOK && caller.Uid != 0 {
				return syscall.EPERM
			}
		}

		var modePtr *uint32
		if m, ok := in.GetMode(); ok {
			if callerOK && caller.Uid != 0 {
				if cur.UID != caller.Uid {
					return syscall.EPERM
				}
			}
			newMode := (cur.Mode & ^uint32(0o7777)) | (m & uint32(0o7777))
			modePtr = &newMode
		}

		var uidPtr *uint32
		var gidPtr *uint32
		if uok {
			u := uint32(uid)
			uidPtr = &u
		}
		if gok {
			g := uint32(gid)
			gidPtr = &g
		}

		var mtimePtr *int64
		if mtime, mok := in.GetMTime(); mok {
			if callerOK && caller.Uid != 0 {
				if cur.UID != caller.Uid {
					return syscall.EPERM
				}
			}
			mt := mtime.Unix()
			mtimePtr = &mt
		}

		haveAny := modePtr != nil || uidPtr != nil || gidPtr != nil || mtimePtr != nil
		if haveAny {
			updated, err := n.db.UpsertMeta(ctx, indexedStorageID, virtualPath, modePtr, uidPtr, gidPtr, mtimePtr)
			if err != nil {
				return toErrno(err)
			}
			if updated {
				if err := eventlog.Append(ctx, n.mountName, eventlog.SetattrEvent{Type: eventlog.TypeSetattr, StorageID: indexedStorageID, Path: virtualPath, Mode: modePtr, UID: uidPtr, GID: gidPtr, MTime: mtimePtr, TS: time.Now().Unix()}); err != nil {
					log.Error().Str("op", "setattr").Str("path", virtualPath).Str("storage_id", indexedStorageID).Err(err).Msg("failed to append eventlog")
					return syscall.EIO
				}
			}
		}

		cur, ok, err = n.db.GetEffectiveFile(ctx, indexedStorageID, virtualPath)
		if err != nil {
			return toErrno(fmt.Errorf("failed to getattr indexed file: %w", err))
		}
		if !ok {
			return syscall.ENOENT
		}
		out.Size = uint64(cur.Size)
		out.Mtime = uint64(cur.MTimeSec)
		out.Mtimensec = 0
		out.Mode = cur.Mode
		out.Nlink = 1
		out.Uid = cur.UID
		out.Gid = cur.GID
		ev := log.Debug().Str("op", "setattr").Str("path", virtualPath).Str("storage_id", indexedStorageID).Bool("indexed", true)
		if modePtr != nil {
			ev = ev.Bool("chmod", true)
		}
		if uidPtr != nil || gidPtr != nil {
			ev = ev.Bool("chown", true)
		}
		if mtimePtr != nil {
			ev = ev.Bool("utimens", true)
		}
		ev.Msg("setattr")
		return 0
	}

	old := syscall.Stat_t{}
	oldOK := false
	if err := syscall.Lstat(physicalPath, &old); err == nil {
		oldOK = true
	}

	uid, uok := in.GetUID()
	gid, gok := in.GetGID()
	if uok || gok {
		// Kernel-level permission checks are not guaranteed to run (we don't rely on default_permissions),
		// so we enforce the key POSIX rule here: only root may change ownership.
		if callerOK && caller.Uid != 0 {
			return syscall.EPERM
		}
		suid := -1
		sgid := -1
		if uok {
			suid = int(uid)
		}
		if gok {
			sgid = int(gid)
		}
		if fh != nil {
			// If we already have an fd, prefer fchown (avoids path races).
			if err := syscall.Fchown(fh.fd, suid, sgid); err != nil {
				return toErrno(err)
			}
		} else {
			// NOTE: For symlinks, lchown must affect the symlink itself (no-follow).
			if err := syscall.Lchown(physicalPath, suid, sgid); err != nil {
				return toErrno(err)
			}
		}
		if oldOK && uint32(old.Mode)&syscall.S_IFMT == syscall.S_IFDIR && uint32(old.Mode)&syscall.S_ISGID != 0 {
			// NOTE: On Linux, chown may clear setgid on directories. Restore it.
			if _, ok := in.GetMode(); !ok {
				mode := uint32(old.Mode) & 0o7777
				if err := syscall.Chmod(physicalPath, mode); err != nil {
					return toErrno(err)
				}
			}
		}
	}

	if m, ok := in.GetMode(); ok {
		// POSIX: non-root can chmod only if they own the file. Since the daemon often runs as root,
		// we must enforce this in userspace.
		if callerOK && caller.Uid != 0 {
			modeOwnerUID := uint32(0)
			if fh != nil {
				st := syscall.Stat_t{}
				if err := syscall.Fstat(fh.fd, &st); err != nil {
					return toErrno(err)
				}
				modeOwnerUID = st.Uid
			} else if oldOK {
				if uint32(old.Mode)&syscall.S_IFMT == syscall.S_IFLNK {
					st := syscall.Stat_t{}
					if err := syscall.Stat(physicalPath, &st); err != nil {
						return toErrno(err)
					}
					modeOwnerUID = st.Uid
				} else {
					modeOwnerUID = old.Uid
				}
			}
			if modeOwnerUID != caller.Uid {
				return syscall.EPERM
			}
		}
		if fh != nil {
			// If we already have an fd, prefer fchmod (avoids path races).
			if err := syscall.Fchmod(fh.fd, m); err != nil {
				return toErrno(err)
			}
		} else {
			if err := syscall.Chmod(physicalPath, m); err != nil {
				return toErrno(err)
			}
		}
	}

	mtime, mok := in.GetMTime()
	atime, aok := in.GetATime()
	if mok || aok {
		// utimens: always no-follow, matching lstat-style behavior for symlinks.
		ta := unix.Timespec{Nsec: utimeOmitNsec}
		tm := unix.Timespec{Nsec: utimeOmitNsec}
		if aok {
			ts, err := unix.TimeToTimespec(atime)
			if err != nil {
				return toErrno(err)
			}
			ta = ts
		}
		if mok {
			ts, err := unix.TimeToTimespec(mtime)
			if err != nil {
				return toErrno(err)
			}
			tm = ts
		}
		if err := unix.UtimesNanoAt(unix.AT_FDCWD, physicalPath, []unix.Timespec{ta, tm}, unix.AT_SYMLINK_NOFOLLOW); err != nil {
			return toErrno(err)
		}
	}

	if sz, ok := in.GetSize(); ok {
		// truncate: prefer ftruncate when we have a file handle.
		if fh != nil {
			if err := syscall.Ftruncate(fh.fd, int64(sz)); err != nil {
				return toErrno(err)
			}
		} else {
			if err := syscall.Truncate(physicalPath, int64(sz)); err != nil {
				return toErrno(err)
			}
		}
	}

	st := syscall.Stat_t{}
	if err := syscall.Lstat(physicalPath, &st); err != nil {
		return toErrno(err)
	}
	out.FromStat(&st)
	ev := log.Debug().Str("op", "setattr").Str("path", virtualPath).Bool("indexed", false)
	if _, ok := in.GetMode(); ok {
		ev = ev.Bool("chmod", true)
	}
	if uok || gok {
		ev = ev.Bool("chown", true)
	}
	if mok || aok {
		ev = ev.Bool("utimens", true)
	}
	if _, ok := in.GetSize(); ok {
		ev = ev.Bool("truncate", true)
	}
	ev.Msg("setattr")
	return 0
}
