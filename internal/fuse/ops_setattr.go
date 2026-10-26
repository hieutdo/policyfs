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
	"github.com/hieutdo/policyfs/internal/indexdb"
	"github.com/rs/zerolog"
	"golang.org/x/sys/unix"
)

// setattrTarget describes one resolved physical or indexed entry to mutate during Setattr.
type setattrTarget struct {
	storageID    string
	physicalPath string
	indexed      bool
	isDir        bool
	oldStat      syscall.Stat_t
	oldStatOK    bool
	indexedCur   indexdb.File
}

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
	var fh *FileHandle
	matchedTargets := []setattrTarget{}

	// Resolve target(s):
	// - If we have a file handle, we must operate on that exact underlying fd/path.
	// - If the first existing entry from read targets is a file/symlink, operate on that first target.
	// - If the first existing entry is a directory, collect all read targets where that directory exists.
	if h, ok := f.(*FileHandle); ok && h != nil {
		fh = h
		log.Debug().Str("op", "setattr").Str("path", virtualPath).Str("storage_id", h.storageID).Str("real_path", h.physicalPath).Bool("indexed", h.indexed).Msg("setattr resolved from open handle")
		matchedTargets = append(matchedTargets, setattrTarget{
			storageID:    h.storageID,
			physicalPath: h.physicalPath,
			indexed:      h.indexed,
		})
	} else {
		targets, err := rt.ResolveReadTargets(virtualPath)
		if err != nil {
			return toErrno(err)
		}
		log.Debug().Str("op", "setattr").Str("path", virtualPath).Msg("setattr resolved read targets")
		resolvingDirs := false
		for _, t := range targets {
			log.Debug().Str("op", "setattr").Str("path", virtualPath).Str("storage_id", t.ID).Bool("indexed", t.Indexed).Msg("setattr scanning target")
			if !t.Indexed {
				p := filepath.Join(t.Root, virtualPath)
				st := syscall.Stat_t{}
				if err := syscall.Lstat(p, &st); err != nil {
					if errors.Is(err, syscall.ENOENT) {
						log.Debug().Str("op", "setattr").Str("path", virtualPath).Str("storage_id", t.ID).Str("real_path", p).Msg("setattr missed on non-indexed target")
						continue
					}
					return toErrno(err)
				}
				isDir := uint32(st.Mode)&syscall.S_IFMT == syscall.S_IFDIR
				if resolvingDirs && !isDir {
					continue
				}
				log.Debug().Str("op", "setattr").Str("path", virtualPath).Str("storage_id", t.ID).Str("real_path", p).Msg("setattr resolved non-indexed target")
				matchedTargets = append(matchedTargets, setattrTarget{
					storageID:    t.ID,
					physicalPath: p,
					indexed:      false,
					isDir:        isDir,
					oldStat:      st,
					oldStatOK:    true,
				})
				if !isDir {
					break
				}
				resolvingDirs = true
				continue
			}

			if n.db == nil {
				log.Error().Str("op", "setattr").Str("path", virtualPath).Str("storage_id", t.ID).Msg("failed to setattr: db is nil for indexed target")
				return syscall.EIO
			}
			cur, ok, err := n.db.GetEffectiveFile(ctx, t.ID, virtualPath)
			if err != nil {
				return toErrno(fmt.Errorf("failed to getattr indexed entry: %w", err))
			}
			if ok {
				if resolvingDirs && !cur.IsDir {
					continue
				}
				if cur.IsDir {
					log.Debug().Str("op", "setattr").Str("path", virtualPath).Str("storage_id", t.ID).Msg("setattr resolved indexed directory")
				} else {
					log.Debug().Str("op", "setattr").Str("path", virtualPath).Str("storage_id", t.ID).Msg("setattr resolved indexed file")
				}
				matchedTargets = append(matchedTargets, setattrTarget{
					storageID:  t.ID,
					indexed:    true,
					isDir:      cur.IsDir,
					indexedCur: cur,
				})
				if !cur.IsDir {
					break
				}
				resolvingDirs = true
				continue
			}
			log.Debug().Str("op", "setattr").Str("path", virtualPath).Str("storage_id", t.ID).Msg("setattr indexed file missed; probing directory")
			dirOK, err := n.db.DirExists(ctx, t.ID, virtualPath)
			if err != nil {
				return toErrno(fmt.Errorf("failed to getattr indexed dir: %w", err))
			}
			if dirOK {
				log.Debug().Str("op", "setattr").Str("path", virtualPath).Str("storage_id", t.ID).Msg("setattr resolved indexed directory")
				curDir, okDir, err := n.db.GetEffectiveFile(ctx, t.ID, virtualPath)
				if err != nil {
					return toErrno(fmt.Errorf("failed to getattr indexed file: %w", err))
				}
				if !okDir {
					continue
				}
				matchedTargets = append(matchedTargets, setattrTarget{
					storageID:  t.ID,
					indexed:    true,
					isDir:      true,
					indexedCur: curDir,
				})
				resolvingDirs = true
				continue
			}
			log.Debug().Str("op", "setattr").Str("path", virtualPath).Str("storage_id", t.ID).Msg("setattr missed on indexed target")
		}
		if len(matchedTargets) == 0 {
			log.Debug().Str("op", "setattr").Str("path", virtualPath).Msg("setattr missed on all targets")
			return syscall.ENOENT
		}
	}

	// Pre-populate state and validate permissions across all matched targets before mutating.
	for i := range matchedTargets {
		mt := &matchedTargets[i]
		if mt.indexed {
			if n.db == nil {
				log.Error().Str("op", "setattr").Str("path", virtualPath).Str("storage_id", mt.storageID).Msg("failed to setattr: db is nil for indexed target")
				return syscall.EIO
			}
			if mt.indexedCur.StorageID == "" {
				cur, ok, err := n.db.GetEffectiveFile(ctx, mt.storageID, virtualPath)
				if err != nil {
					return toErrno(fmt.Errorf("failed to getattr indexed file: %w", err))
				}
				if !ok {
					log.Debug().Str("op", "setattr").Str("path", virtualPath).Str("storage_id", mt.storageID).Msg("setattr indexed entry missing after resolution")
					return syscall.ENOENT
				}
				mt.indexedCur = cur
			}
			if errno := validateIndexedSetattrPerms(in, caller, callerOK, mt.indexedCur, virtualPath, mt.storageID, log); errno != 0 {
				return errno
			}
			continue
		}

		if !mt.oldStatOK {
			if err := syscall.Lstat(mt.physicalPath, &mt.oldStat); err == nil {
				mt.oldStatOK = true
			}
		}
		if errno := validateNonIndexedSetattrPerms(in, caller, callerOK, fh, mt); errno != 0 {
			return errno
		}
	}

	// Apply attribute changes across all matched targets; populate AttrOut from the first target.
	for i, mt := range matchedTargets {
		var targetOut gofuse.AttrOut
		var errno syscall.Errno
		if mt.indexed {
			errno = n.applyIndexedSetattr(ctx, in, virtualPath, mt, log, &targetOut)
		} else {
			errno = applyNonIndexedSetattr(in, fh, virtualPath, mt, log, &targetOut)
		}
		if errno != 0 {
			return errno
		}
		if i == 0 {
			*out = targetOut
		}
	}
	return 0
}

// validateIndexedSetattrPerms checks EROFS and EPERM constraints for an indexed target before mutation.
func validateIndexedSetattrPerms(in *gofuse.SetAttrIn, caller *gofuse.Caller, callerOK bool, cur indexdb.File, virtualPath string, storageID string, log zerolog.Logger) syscall.Errno {
	if _, ok := in.GetSize(); ok {
		log.Debug().Str("op", "setattr").Str("path", virtualPath).Str("storage_id", storageID).Msg("setattr blocked: truncate on indexed target")
		return syscall.EROFS
	}
	_, uok := in.GetUID()
	_, gok := in.GetGID()
	if uok || gok {
		if callerOK && caller.Uid != 0 {
			return syscall.EPERM
		}
	}
	if _, ok := in.GetMode(); ok {
		if callerOK && caller.Uid != 0 && cur.UID != caller.Uid {
			return syscall.EPERM
		}
	}
	if _, mok := in.GetMTime(); mok {
		if callerOK && caller.Uid != 0 && cur.UID != caller.Uid {
			return syscall.EPERM
		}
	}
	return 0
}

// validateNonIndexedSetattrPerms checks EPERM constraints for a non-indexed target before mutation.
func validateNonIndexedSetattrPerms(in *gofuse.SetAttrIn, caller *gofuse.Caller, callerOK bool, fh *FileHandle, mt *setattrTarget) syscall.Errno {
	_, uok := in.GetUID()
	_, gok := in.GetGID()
	if uok || gok {
		// Kernel-level permission checks are not guaranteed to run (we don't rely on default_permissions),
		// so we enforce the key POSIX rule here: only root may change ownership.
		if callerOK && caller.Uid != 0 {
			return syscall.EPERM
		}
	}
	if _, ok := in.GetMode(); ok {
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
			} else if mt.oldStatOK {
				if uint32(mt.oldStat.Mode)&syscall.S_IFMT == syscall.S_IFLNK {
					st := syscall.Stat_t{}
					if err := syscall.Stat(mt.physicalPath, &st); err != nil {
						return toErrno(err)
					}
					modeOwnerUID = st.Uid
				} else {
					modeOwnerUID = mt.oldStat.Uid
				}
			}
			if modeOwnerUID != caller.Uid {
				return syscall.EPERM
			}
		}
	}
	return 0
}

// applyIndexedSetattr writes deferred metadata overrides and appends a SETATTR event for one indexed target.
func (n *Node) applyIndexedSetattr(ctx context.Context, in *gofuse.SetAttrIn, virtualPath string, mt setattrTarget, log zerolog.Logger, out *gofuse.AttrOut) syscall.Errno {
	cur := mt.indexedCur
	indexedStorageID := mt.storageID

	uid, uok := in.GetUID()
	gid, gok := in.GetGID()

	var modePtr *uint32
	if m, ok := in.GetMode(); ok {
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
		mtSec := mtime.Unix()
		mtimePtr = &mtSec
	}

	haveAny := modePtr != nil || uidPtr != nil || gidPtr != nil || mtimePtr != nil
	if haveAny {
		updated, err := n.db.UpsertMeta(ctx, indexedStorageID, virtualPath, modePtr, uidPtr, gidPtr, mtimePtr)
		if err != nil {
			return toErrno(err)
		}
		if !updated {
			log.Debug().Str("op", "setattr").Str("path", virtualPath).Str("storage_id", indexedStorageID).Msg("setattr indexed metadata update missed")
		}
		if updated {
			log.Debug().Str("op", "setattr").Str("path", virtualPath).Str("storage_id", indexedStorageID).Msg("setattr updated indexed metadata")
			if err := eventlog.Append(ctx, n.mountName, eventlog.SetattrEvent{Type: eventlog.TypeSetattr, StorageID: indexedStorageID, Path: virtualPath, Mode: modePtr, UID: uidPtr, GID: gidPtr, MTime: mtimePtr, TS: time.Now().Unix()}); err != nil {
				log.Error().Str("op", "setattr").Str("path", virtualPath).Str("storage_id", indexedStorageID).Err(err).Msg("failed to append eventlog")
				return syscall.EIO
			}
			log.Debug().Str("op", "setattr").Str("path", virtualPath).Str("storage_id", indexedStorageID).Msg("setattr appended deferred event")
		}
	}

	cur, ok, err := n.db.GetEffectiveFile(ctx, indexedStorageID, virtualPath)
	if err != nil {
		return toErrno(fmt.Errorf("failed to getattr indexed file: %w", err))
	}
	if !ok {
		log.Debug().Str("op", "setattr").Str("path", virtualPath).Str("storage_id", indexedStorageID).Msg("setattr indexed entry missing after refresh")
		return syscall.ENOENT
	}
	log.Debug().Str("op", "setattr").Str("path", virtualPath).Str("storage_id", indexedStorageID).Msg("setattr refreshed indexed attributes")
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

// applyNonIndexedSetattr applies immediate physical attribute changes for one non-indexed target.
func applyNonIndexedSetattr(in *gofuse.SetAttrIn, fh *FileHandle, virtualPath string, mt setattrTarget, log zerolog.Logger, out *gofuse.AttrOut) syscall.Errno {
	physicalPath := mt.physicalPath
	old := mt.oldStat
	oldOK := mt.oldStatOK

	uid, uok := in.GetUID()
	gid, gok := in.GetGID()
	if uok || gok {
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
	ev := log.Debug().Str("op", "setattr").Str("path", virtualPath).Str("storage_id", mt.storageID).Str("real_path", physicalPath).Bool("indexed", false)
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
