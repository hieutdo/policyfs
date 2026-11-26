package fuse

import (
	"context"
	"errors"
	"path/filepath"
	"syscall"

	"github.com/hanwen/go-fuse/v2/fs"
	gofuse "github.com/hanwen/go-fuse/v2/fuse"
	"github.com/hieutdo/policyfs/internal/errkind"
)

// Symlink creates a symbolic link on a selected write target.
func (n *Node) Symlink(ctx context.Context, target string, name string, out *gofuse.EntryOut) (*fs.Inode, syscall.Errno) {
	if n == nil {
		return nil, toErrno(&errkind.NilError{What: "node"})
	}
	rt, log := n.runtime()
	if rt == nil {
		return nil, toErrno(&errkind.NilError{What: "router"})
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

	writeTarget, err := rt.SelectWriteTarget(virtualPath)
	if err != nil {
		log.Error().Str("op", "symlink").Str("path", virtualPath).Str("target", target).Err(err).Msg("failed to select write target")
		return nil, toErrno(err)
	}
	log.Debug().Str("op", "symlink").Str("path", virtualPath).Str("target", target).Str("storage_id", writeTarget.ID).Bool("indexed", writeTarget.Indexed).Msg("symlink selected write target")
	if writeTarget.Indexed {
		log.Debug().Str("op", "symlink").Str("path", virtualPath).Str("target", target).Str("storage_id", writeTarget.ID).Msg("symlink blocked: indexed target is read-only")
		return nil, syscall.EROFS
	}

	physicalPath := filepath.Join(writeTarget.Root, virtualPath)
	if err := materializeParentDirs(ctx, writeTarget.Root, virtualPath); err != nil {
		log.Error().Str("op", "symlink").Str("path", virtualPath).Str("target", target).Str("storage_id", writeTarget.ID).Err(err).Msg("failed to materialize parent dirs")
		return nil, toErrno(err)
	}
	log.Debug().Str("op", "symlink").Str("path", virtualPath).Str("target", target).Str("storage_id", writeTarget.ID).Str("real_path", physicalPath).Msg("symlink materialized parent dirs")

	parentPhysical := filepath.Dir(physicalPath)
	pst := syscall.Stat_t{}
	if err := syscall.Lstat(parentPhysical, &pst); err != nil {
		log.Error().Str("op", "symlink").Str("path", virtualPath).Str("target", target).Str("storage_id", writeTarget.ID).Err(err).Msg("failed to lstat parent")
		return nil, toErrno(err)
	}
	if uint32(pst.Mode)&syscall.S_IFMT != syscall.S_IFDIR {
		return nil, syscall.ENOTDIR
	}
	if errno := dirWriteExecPermErrno(caller, uint32(pst.Mode), pst.Uid, pst.Gid); errno != 0 {
		return nil, errno
	}

	if err := syscall.Symlink(target, physicalPath); err != nil {
		log.Error().Str("op", "symlink").Str("path", virtualPath).Str("target", target).Str("storage_id", writeTarget.ID).Err(err).Msg("failed to symlink")
		return nil, toErrno(err)
	}
	log.Debug().Str("op", "symlink").Str("path", virtualPath).Str("target", target).Str("storage_id", writeTarget.ID).Str("real_path", physicalPath).Msg("symlink created on disk")

	if err := preserveOwnerForCreate(ctx, parentPhysical, -1, physicalPath); err != nil {
		_ = syscall.Unlink(physicalPath)
		log.Error().Str("op", "symlink").Str("path", virtualPath).Str("target", target).Str("storage_id", writeTarget.ID).Err(err).Msg("failed to preserve owner")
		return nil, toErrno(err)
	}

	st := syscall.Stat_t{}
	if err := syscall.Lstat(physicalPath, &st); err != nil {
		_ = syscall.Unlink(physicalPath)
		return nil, toErrno(err)
	}
	out.FromStat(&st)

	ch := newChildInode(ctx, n.EmbeddedInode(), n.mountName, n.state, n.reload, n.db, n.disk, n.open, writeTarget.ID, virtualPath, uint32(st.Mode))
	log.Debug().Str("op", "symlink").Str("path", virtualPath).Str("target", target).Str("storage_id", writeTarget.ID).Str("real_path", physicalPath).Bool("indexed", false).Msg("symlink")
	return ch, 0
}

// Readlink reads the target of a symbolic link from the first matching read target.
func (n *Node) Readlink(ctx context.Context) ([]byte, syscall.Errno) {
	if n == nil {
		return nil, toErrno(&errkind.NilError{What: "node"})
	}
	rt, log := n.runtime()
	if rt == nil {
		return nil, toErrno(&errkind.NilError{What: "router"})
	}

	virtualPath := n.Path(n.Root())
	if virtualPath == "." {
		virtualPath = ""
	}
	if errno := validateVirtualPath(virtualPath); errno != 0 {
		return nil, errno
	}

	targets, err := rt.ResolveReadTargets(virtualPath)
	if err != nil {
		return nil, toErrno(err)
	}
	log.Debug().Str("op", "readlink").Str("path", virtualPath).Msg("readlink resolved read targets")

	for _, t := range targets {
		if t.Indexed {
			continue
		}
		physicalPath := filepath.Join(t.Root, virtualPath)
		for l := 256; ; l *= 2 {
			buf := make([]byte, l)
			sz, err := syscall.Readlink(physicalPath, buf)
			if err != nil {
				if errors.Is(err, syscall.ENOENT) {
					log.Debug().Str("op", "readlink").Str("path", virtualPath).Str("storage_id", t.ID).Str("real_path", physicalPath).Msg("readlink missed on target")
					break
				}
				log.Error().Str("op", "readlink").Str("path", virtualPath).Str("storage_id", t.ID).Str("real_path", physicalPath).Err(err).Msg("failed to readlink")
				return nil, toErrno(err)
			}
			if sz < len(buf) {
				log.Debug().Str("op", "readlink").Str("path", virtualPath).Str("storage_id", t.ID).Str("real_path", physicalPath).Msg("readlink")
				_ = ctx
				return buf[:sz], 0
			}
		}
	}

	log.Debug().Str("op", "readlink").Str("path", virtualPath).Msg("readlink missed on all targets")
	return nil, syscall.ENOENT
}
