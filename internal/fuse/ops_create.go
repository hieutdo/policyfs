package fuse

import (
	"context"
	"os"
	"path/filepath"
	"syscall"

	"github.com/hanwen/go-fuse/v2/fs"
	gofuse "github.com/hanwen/go-fuse/v2/fuse"
	"github.com/hieutdo/policyfs/internal/errkind"
)

// Create creates a new file on a selected write target.
func (n *Node) Create(ctx context.Context, name string, flags uint32, mode uint32, out *gofuse.EntryOut) (*fs.Inode, fs.FileHandle, uint32, syscall.Errno) {
	if n == nil {
		return nil, nil, 0, fs.ToErrno(&errkind.NilError{What: "node"})
	}
	rt, log := n.runtime()
	if rt == nil {
		return nil, nil, 0, fs.ToErrno(&errkind.NilError{What: "router"})
	}

	caller, callerOK := gofuse.FromContext(ctx)
	if !callerOK {
		return nil, nil, 0, syscall.EPERM
	}

	parentVirtualPath := n.Path(n.Root())
	if parentVirtualPath == "." {
		parentVirtualPath = ""
	}
	virtualPath, errno := joinVirtualPath(parentVirtualPath, name)
	if errno != 0 {
		return nil, nil, 0, errno
	}

	// Route: choose a single write target for this create.
	target, err := rt.SelectWriteTarget(virtualPath)
	if err != nil {
		log.Error().Str("op", "create").Str("path", virtualPath).Err(err).Msg("failed to select write target")
		return nil, nil, 0, toErrno(err)
	}
	// Indexed targets are not writable yet.
	if target.Indexed {
		log.Debug().Str("op", "create").Str("path", virtualPath).Str("storage_id", target.ID).Msg("create blocked: indexed target is read-only")
		return nil, nil, 0, syscall.EROFS
	}

	physicalPath := filepath.Join(target.Root, virtualPath)
	// Ensure the parent directory exists on the chosen target.
	// This also applies setgid/gid inheritance based on the physical parent directory.
	if err := materializeParentDirs(ctx, target.Root, virtualPath); err != nil {
		log.Error().Str("op", "create").Str("path", virtualPath).Str("storage_id", target.ID).Err(err).Msg("failed to materialize parent dirs")
		return nil, nil, 0, fs.ToErrno(err)
	}
	if callerOK {
		parentPhysical := filepath.Dir(physicalPath)
		pst := syscall.Stat_t{}
		if err := syscall.Lstat(parentPhysical, &pst); err != nil {
			log.Error().Str("op", "create").Str("path", virtualPath).Str("storage_id", target.ID).Err(err).Msg("failed to lstat parent")
			return nil, nil, 0, fs.ToErrno(err)
		}
		if uint32(pst.Mode)&syscall.S_IFMT != syscall.S_IFDIR {
			return nil, nil, 0, syscall.ENOTDIR
		}
		if errno := dirWriteExecPermErrno(caller, uint32(pst.Mode), pst.Uid, pst.Gid); errno != 0 {
			return nil, nil, 0, errno
		}
	}

	openFlags := int(flags) &^ syscall.O_APPEND
	fd, err := syscall.Open(physicalPath, openFlags|syscall.O_CREAT, mode)
	if err != nil {
		log.Error().Str("op", "create").Str("path", virtualPath).Str("storage_id", target.ID).Err(err).Msg("failed to open file for create")
		return nil, nil, 0, fs.ToErrno(err)
	}
	// If the daemon runs as root, preserve the calling uid/gid (and force gid when parent has setgid).
	if err := preserveOwnerForCreate(ctx, filepath.Dir(physicalPath), fd, ""); err != nil {
		_ = syscall.Close(fd)
		log.Error().Str("op", "create").Str("path", virtualPath).Str("storage_id", target.ID).Err(err).Msg("failed to preserve owner")
		return nil, nil, 0, fs.ToErrno(err)
	}

	st := syscall.Stat_t{}
	if err := syscall.Fstat(fd, &st); err != nil {
		_ = syscall.Close(fd)
		return nil, nil, 0, fs.ToErrno(err)
	}
	out.FromStat(&st)

	ch := newChildInode(ctx, n.EmbeddedInode(), n.RootData, n.mountName, n.state, n.reload, n.db, n.disk, n.open, target.ID, virtualPath, uint32(st.Mode))

	fh := &FileHandle{virtualPath: virtualPath, physicalPath: physicalPath, storageID: target.ID, indexed: target.Indexed, fd: fd, flags: flags}
	write := flags&gofuse.O_ANYWRITE != 0
	attachOpenTrackingFromStat(n, virtualPath, fh, write, &st)
	log.Debug().Str("op", "create").Str("path", virtualPath).Str("storage_id", target.ID).Msg("create")
	return ch, fh, 0, 0
}

// Mkdir creates a new directory on a selected write target.
func (n *Node) Mkdir(ctx context.Context, name string, mode uint32, out *gofuse.EntryOut) (*fs.Inode, syscall.Errno) {
	if n == nil {
		return nil, fs.ToErrno(&errkind.NilError{What: "node"})
	}
	rt, log := n.runtime()
	if rt == nil {
		return nil, fs.ToErrno(&errkind.NilError{What: "router"})
	}

	caller, callerOK := gofuse.FromContext(ctx)
	if !callerOK {
		return nil, syscall.EPERM
	}

	parentVirtualPath := n.Path(n.Root())
	if parentVirtualPath == "." {
		parentVirtualPath = ""
	}
	virtualPath, errno := joinVirtualPath(parentVirtualPath, name)
	if errno != 0 {
		return nil, errno
	}

	// Route: choose a single write target for this mkdir.
	target, err := rt.SelectWriteTarget(virtualPath)
	if err != nil {
		log.Error().Str("op", "mkdir").Str("path", virtualPath).Err(err).Msg("failed to select write target")
		return nil, toErrno(err)
	}
	// Indexed targets are not writable yet.
	if target.Indexed {
		log.Debug().Str("op", "mkdir").Str("path", virtualPath).Str("storage_id", target.ID).Msg("mkdir blocked: indexed target is read-only")
		return nil, syscall.EROFS
	}

	physicalPath := filepath.Join(target.Root, virtualPath)
	// Ensure the parent directory exists on the chosen target.
	if err := materializeParentDirs(ctx, target.Root, virtualPath); err != nil {
		log.Error().Str("op", "mkdir").Str("path", virtualPath).Str("storage_id", target.ID).Err(err).Msg("failed to materialize parent dirs")
		return nil, fs.ToErrno(err)
	}

	parentPhysical := filepath.Dir(physicalPath)
	pst := syscall.Stat_t{}
	if err := syscall.Lstat(parentPhysical, &pst); err != nil {
		log.Error().Str("op", "mkdir").Str("path", virtualPath).Str("storage_id", target.ID).Err(err).Msg("failed to lstat parent")
		return nil, fs.ToErrno(err)
	}
	if uint32(pst.Mode)&syscall.S_IFMT != syscall.S_IFDIR {
		return nil, syscall.ENOTDIR
	}
	if callerOK {
		if errno := dirWriteExecPermErrno(caller, uint32(pst.Mode), pst.Uid, pst.Gid); errno != 0 {
			return nil, errno
		}
	}
	if uint32(pst.Mode)&syscall.S_ISGID != 0 {
		// setgid must propagate from the physical parent directory.
		mode |= syscall.S_ISGID
	}
	if err := os.Mkdir(physicalPath, os.FileMode(mode)); err != nil {
		log.Error().Str("op", "mkdir").Str("path", virtualPath).Str("storage_id", target.ID).Err(err).Msg("failed to mkdir")
		return nil, fs.ToErrno(err)
	}
	// If the daemon runs as root, preserve the calling uid/gid (and force gid when parent has setgid).
	if err := preserveOwnerForCreate(ctx, parentPhysical, -1, physicalPath); err != nil {
		_ = syscall.Rmdir(physicalPath)
		log.Error().Str("op", "mkdir").Str("path", virtualPath).Str("storage_id", target.ID).Err(err).Msg("failed to preserve owner")
		return nil, fs.ToErrno(err)
	}
	// On Linux, chown may clear setgid bits; apply chmod after chown to ensure it sticks.
	if err := syscall.Chmod(physicalPath, mode); err != nil {
		_ = syscall.Rmdir(physicalPath)
		log.Error().Str("op", "mkdir").Str("path", virtualPath).Str("storage_id", target.ID).Err(err).Msg("failed to chmod")
		return nil, fs.ToErrno(err)
	}

	st := syscall.Stat_t{}
	if err := syscall.Lstat(physicalPath, &st); err != nil {
		_ = syscall.Rmdir(physicalPath)
		return nil, fs.ToErrno(err)
	}
	out.FromStat(&st)

	ch := newChildInode(ctx, n.EmbeddedInode(), n.RootData, n.mountName, n.state, n.reload, n.db, n.disk, n.open, target.ID, virtualPath, uint32(st.Mode))
	log.Debug().Str("op", "mkdir").Str("path", virtualPath).Str("storage_id", target.ID).Msg("mkdir")
	return ch, 0
}
