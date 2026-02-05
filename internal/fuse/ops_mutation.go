package fuse

import (
	"context"
	"errors"
	"os"
	"path/filepath"
	"syscall"

	"github.com/hanwen/go-fuse/v2/fs"
	gofuse "github.com/hanwen/go-fuse/v2/fuse"
	"golang.org/x/sys/unix"
)

// Create creates a new file on a selected write target.
func (n *Node) Create(ctx context.Context, name string, flags uint32, mode uint32, out *gofuse.EntryOut) (*fs.Inode, fs.FileHandle, uint32, syscall.Errno) {
	if n == nil {
		return nil, nil, 0, fs.ToErrno(errors.New("node is nil"))
	}
	if n.rt == nil {
		return nil, nil, 0, fs.ToErrno(errors.New("router is nil"))
	}

	parentVirtualPath := n.Path(n.Root())
	virtualPath := filepath.Join(parentVirtualPath, name)

	target, err := n.rt.SelectWriteTarget(virtualPath)
	if err != nil {
		return nil, nil, 0, toErrno(err)
	}
	if target.Indexed {
		return nil, nil, 0, syscall.EROFS
	}

	physicalPath := filepath.Join(target.Root, virtualPath)
	if err := os.MkdirAll(filepath.Dir(physicalPath), 0o755); err != nil {
		return nil, nil, 0, fs.ToErrno(err)
	}

	openFlags := int(flags) &^ syscall.O_APPEND
	fd, err := syscall.Open(physicalPath, openFlags|syscall.O_CREAT, mode)
	if err != nil {
		return nil, nil, 0, fs.ToErrno(err)
	}

	st := syscall.Stat_t{}
	if err := syscall.Fstat(fd, &st); err != nil {
		_ = syscall.Close(fd)
		return nil, nil, 0, fs.ToErrno(err)
	}
	out.FromStat(&st)

	ch := newChildInode(ctx, n.EmbeddedInode(), n.RootData, n.rt, uint32(st.Mode))

	fh := &FileHandle{virtualPath: virtualPath, physicalPath: physicalPath, storageID: target.ID, indexed: target.Indexed, fd: fd, flags: flags}
	return ch, fh, 0, 0
}

// Mkdir creates a new directory on a selected write target.
func (n *Node) Mkdir(ctx context.Context, name string, mode uint32, out *gofuse.EntryOut) (*fs.Inode, syscall.Errno) {
	if n == nil {
		return nil, fs.ToErrno(errors.New("node is nil"))
	}
	if n.rt == nil {
		return nil, fs.ToErrno(errors.New("router is nil"))
	}

	parentVirtualPath := n.Path(n.Root())
	virtualPath := filepath.Join(parentVirtualPath, name)

	target, err := n.rt.SelectWriteTarget(virtualPath)
	if err != nil {
		return nil, toErrno(err)
	}
	if target.Indexed {
		return nil, syscall.EROFS
	}

	physicalPath := filepath.Join(target.Root, virtualPath)
	if err := os.MkdirAll(filepath.Dir(physicalPath), 0o755); err != nil {
		return nil, fs.ToErrno(err)
	}
	if err := os.Mkdir(physicalPath, os.FileMode(mode)); err != nil {
		return nil, fs.ToErrno(err)
	}

	st := syscall.Stat_t{}
	if err := syscall.Lstat(physicalPath, &st); err != nil {
		_ = syscall.Rmdir(physicalPath)
		return nil, fs.ToErrno(err)
	}
	out.FromStat(&st)

	ch := newChildInode(ctx, n.EmbeddedInode(), n.RootData, n.rt, uint32(st.Mode))
	return ch, 0
}

// Unlink removes a child file on the first existing read target.
func (n *Node) Unlink(ctx context.Context, name string) syscall.Errno {
	if n == nil {
		return fs.ToErrno(errors.New("node is nil"))
	}
	if n.rt == nil {
		return fs.ToErrno(errors.New("router is nil"))
	}

	parentVirtualPath := n.Path(n.Root())
	virtualPath := filepath.Join(parentVirtualPath, name)

	target, physicalPath, errno := firstExistingPhysical(n.rt, virtualPath)
	if errno != 0 {
		return errno
	}
	if target.Indexed {
		return syscall.EROFS
	}
	_ = ctx
	return fs.ToErrno(syscall.Unlink(physicalPath))
}

// Rmdir removes a child directory on the first existing read target.
func (n *Node) Rmdir(ctx context.Context, name string) syscall.Errno {
	if n == nil {
		return fs.ToErrno(errors.New("node is nil"))
	}
	if n.rt == nil {
		return fs.ToErrno(errors.New("router is nil"))
	}

	parentVirtualPath := n.Path(n.Root())
	virtualPath := filepath.Join(parentVirtualPath, name)

	target, physicalPath, errno := firstExistingPhysical(n.rt, virtualPath)
	if errno != 0 {
		return errno
	}
	if target.Indexed {
		return syscall.EROFS
	}
	_ = ctx
	return fs.ToErrno(syscall.Rmdir(physicalPath))
}

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
		return syscall.ENOTSUP
	}

	np, ok := newParent.(*Node)
	if !ok {
		return syscall.EXDEV
	}
	if np.rt != n.rt {
		return syscall.EXDEV
	}

	oldParentVirtualPath := n.Path(n.Root())
	oldVirtualPath := filepath.Join(oldParentVirtualPath, name)

	srcTarget, srcPhysicalPath, errno := firstExistingPhysical(n.rt, oldVirtualPath)
	if errno != 0 {
		return errno
	}
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
		return syscall.EXDEV
	}

	dstPhysicalPath := filepath.Join(srcTarget.Root, newVirtualPath)
	if err := os.MkdirAll(filepath.Dir(dstPhysicalPath), 0o755); err != nil {
		return fs.ToErrno(err)
	}
	_ = ctx
	return fs.ToErrno(syscall.Rename(srcPhysicalPath, dstPhysicalPath))
}

// Setattr applies attribute changes to the underlying storage.
func (n *Node) Setattr(ctx context.Context, f fs.FileHandle, in *gofuse.SetAttrIn, out *gofuse.AttrOut) syscall.Errno {
	if n == nil {
		return fs.ToErrno(errors.New("node is nil"))
	}
	if n.rt == nil {
		return fs.ToErrno(errors.New("router is nil"))
	}

	virtualPath := n.Path(n.Root())
	physicalPath := ""
	indexed := false

	if fh, ok := f.(*FileHandle); ok && fh != nil {
		physicalPath = fh.physicalPath
		indexed = fh.indexed
	} else {
		target, p, errno := firstExistingPhysical(n.rt, virtualPath)
		if errno != 0 {
			return errno
		}
		indexed = target.Indexed
		physicalPath = p
	}
	if indexed {
		return syscall.EROFS
	}

	if m, ok := in.GetMode(); ok {
		if err := syscall.Chmod(physicalPath, m); err != nil {
			return fs.ToErrno(err)
		}
	}

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
		if err := syscall.Chown(physicalPath, suid, sgid); err != nil {
			return fs.ToErrno(err)
		}
	}

	mtime, mok := in.GetMTime()
	atime, aok := in.GetATime()
	if mok || aok {
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
		if fh, ok := f.(*FileHandle); ok && fh != nil {
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

// Flush is called on close(2). We keep this lightweight for indexed=false.
func (n *Node) Flush(ctx context.Context, f fs.FileHandle) syscall.Errno {
	_ = ctx
	_ = f
	return 0
}

// Fsync flushes file content to stable storage.
func (n *Node) Fsync(ctx context.Context, f fs.FileHandle, flags uint32) syscall.Errno {
	if fh, ok := f.(*FileHandle); ok && fh != nil {
		return fh.Fsync(ctx, flags)
	}
	_ = ctx
	_ = flags
	return syscall.ENOTSUP
}
