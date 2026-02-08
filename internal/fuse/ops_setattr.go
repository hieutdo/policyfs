package fuse

import (
	"context"
	"syscall"

	"github.com/hanwen/go-fuse/v2/fs"
	gofuse "github.com/hanwen/go-fuse/v2/fuse"
	"github.com/hieutdo/policyfs/internal/errkind"
	"golang.org/x/sys/unix"
)

// Setattr applies attribute changes to the underlying storage.
func (n *Node) Setattr(ctx context.Context, f fs.FileHandle, in *gofuse.SetAttrIn, out *gofuse.AttrOut) syscall.Errno {
	if n == nil {
		return fs.ToErrno(&errkind.NilError{What: "node"})
	}
	if n.rt == nil {
		return fs.ToErrno(&errkind.NilError{What: "router"})
	}

	caller, callerOK := gofuse.FromContext(ctx)

	virtualPath := n.Path(n.Root())
	physicalPath := ""
	indexed := false
	var fh *FileHandle

	// Resolve the physical path.
	// - If we have a file handle, we must operate on that exact underlying fd/path.
	// - Otherwise we operate on the first existing physical entry from read targets.
	if h, ok := f.(*FileHandle); ok && h != nil {
		fh = h
		physicalPath = h.physicalPath
		indexed = h.indexed
	} else {
		target, p, errno := firstExistingPhysical(n.rt, virtualPath)
		if errno != 0 {
			return errno
		}
		indexed = target.Indexed
		physicalPath = p
	}
	// Indexed targets are not writable yet.
	if indexed {
		return syscall.EROFS
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
				return fs.ToErrno(err)
			}
		} else {
			// NOTE: For symlinks, lchown must affect the symlink itself (no-follow).
			if err := syscall.Lchown(physicalPath, suid, sgid); err != nil {
				return fs.ToErrno(err)
			}
		}
		if oldOK && uint32(old.Mode)&syscall.S_IFMT == syscall.S_IFDIR && uint32(old.Mode)&syscall.S_ISGID != 0 {
			// NOTE: On Linux, chown may clear setgid on directories. Restore it.
			if _, ok := in.GetMode(); !ok {
				mode := uint32(old.Mode) & 0o7777
				if err := syscall.Chmod(physicalPath, mode); err != nil {
					return fs.ToErrno(err)
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
					return fs.ToErrno(err)
				}
				modeOwnerUID = st.Uid
			} else if oldOK {
				if uint32(old.Mode)&syscall.S_IFMT == syscall.S_IFLNK {
					st := syscall.Stat_t{}
					if err := syscall.Stat(physicalPath, &st); err != nil {
						return fs.ToErrno(err)
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
				return fs.ToErrno(err)
			}
		} else {
			if err := syscall.Chmod(physicalPath, m); err != nil {
				return fs.ToErrno(err)
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
				return fs.ToErrno(err)
			}
			ta = ts
		}
		if mok {
			ts, err := unix.TimeToTimespec(mtime)
			if err != nil {
				return fs.ToErrno(err)
			}
			tm = ts
		}
		if err := unix.UtimesNanoAt(unix.AT_FDCWD, physicalPath, []unix.Timespec{ta, tm}, unix.AT_SYMLINK_NOFOLLOW); err != nil {
			return fs.ToErrno(err)
		}
	}

	if sz, ok := in.GetSize(); ok {
		// truncate: prefer ftruncate when we have a file handle.
		if fh != nil {
			if err := syscall.Ftruncate(fh.fd, int64(sz)); err != nil {
				return fs.ToErrno(err)
			}
		} else {
			if err := syscall.Truncate(physicalPath, int64(sz)); err != nil {
				return fs.ToErrno(err)
			}
		}
	}

	st := syscall.Stat_t{}
	if err := syscall.Lstat(physicalPath, &st); err != nil {
		return fs.ToErrno(err)
	}
	out.FromStat(&st)
	_ = ctx
	return 0
}
