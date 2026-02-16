package fuse

import (
	"context"
	"errors"
	"os"
	"path/filepath"
	"syscall"

	"github.com/hanwen/go-fuse/v2/fs"
	gofuse "github.com/hanwen/go-fuse/v2/fuse"
	"github.com/hieutdo/policyfs/internal/errkind"
	"github.com/hieutdo/policyfs/internal/indexdb"
	"github.com/hieutdo/policyfs/internal/router"
	"github.com/rs/zerolog"
)

// openFirst opens a file by searching targets in the router-defined order.
func openFirst(ctx context.Context, rt *router.Router, db *indexdb.DB, virtualPath string, flags int, write bool) (fs.FileHandle, uint32, syscall.Errno) {
	if rt == nil {
		return nil, 0, fs.ToErrno(&errkind.NilError{What: "router"})
	}

	var targets []router.Target
	var err error
	if write {
		targets, err = rt.ResolveWriteTargets(virtualPath)
	} else {
		targets, err = rt.ResolveReadTargets(virtualPath)
	}
	if err != nil {
		return nil, 0, toErrno(err)
	}

	sawIndexed := false
	for _, t := range targets {
		if write && t.Indexed {
			sawIndexed = true
			continue
		}
		physicalPath := filepath.Join(t.Root, virtualPath)
		if t.Indexed {
			if db == nil {
				return nil, 0, syscall.EIO
			}
			f, ok, err := db.GetEffectiveFile(ctx, t.ID, virtualPath)
			if err != nil {
				return nil, 0, toErrno(err)
			}
			if !ok || f.IsDir {
				continue
			}
			physicalPath = filepath.Join(t.Root, f.RealPath)
		}
		fd, oerr := syscall.Open(physicalPath, flags, 0)
		if oerr != nil {
			// For indexed files, stale pending real_path can cause ENOENT after prune.
			// Fallback to opening by the requested virtualPath when real_path no longer exists.
			if t.Indexed && errors.Is(oerr, syscall.ENOENT) {
				fallbackPath := filepath.Join(t.Root, virtualPath)
				if fallbackPath != physicalPath {
					if fd2, oerr2 := syscall.Open(fallbackPath, flags, 0); oerr2 == nil {
						fh := &FileHandle{virtualPath: virtualPath, physicalPath: fallbackPath, storageID: t.ID, indexed: t.Indexed, fd: fd2, flags: uint32(flags)}
						_ = ctx
						return fh, 0, 0
					}
				}
			}
			if errors.Is(oerr, syscall.ENOENT) {
				continue
			}
			return nil, 0, fs.ToErrno(oerr)
		}
		fh := &FileHandle{virtualPath: virtualPath, physicalPath: physicalPath, storageID: t.ID, indexed: t.Indexed, fd: fd, flags: uint32(flags)}
		_ = ctx
		return fh, 0, 0
	}
	if sawIndexed {
		return nil, 0, syscall.EROFS
	}
	return nil, 0, syscall.ENOENT
}

// newChildInode creates a child inode with Node ops and stable mode derived from a stat mode.
func newChildInode(ctx context.Context, parent *fs.Inode, rootData *fs.LoopbackRoot, mountName string, rt *router.Router, db *indexdb.DB, log zerolog.Logger, disk *diskAccessLogger, stMode uint32) *fs.Inode {
	child := &Node{LoopbackNode: &fs.LoopbackNode{RootData: rootData}, mountName: mountName, rt: rt, db: db, log: log, disk: disk}
	typeMode := uint32(stMode & syscall.S_IFMT)
	ch := parent.NewInode(ctx, child, fs.StableAttr{Mode: typeMode, Gen: 1})
	return ch
}

// statfsWriteTarget resolves write targets for a virtual path and populates out
// with the filesystem stats of the first write target. Returns true if stats
// were successfully populated, false if the caller should fall back.
func statfsWriteTarget(rt *router.Router, virtualPath string, out *gofuse.StatfsOut) bool {
	if rt == nil {
		return false
	}
	// Prefer selecting the actual write target so Statfs matches create/mkdir routing
	// (write_policy, path_preserving, min_free_gb filtering).
	if t, err := rt.SelectWriteTarget(virtualPath); err == nil {
		var st syscall.Statfs_t
		if err := syscall.Statfs(t.Root, &st); err == nil {
			out.FromStatfsT(&st)
			return true
		}
	}

	// Fallback: if write target selection fails for any reason, report the first
	// resolved write target in routing order.
	targets, err := rt.ResolveWriteTargets(virtualPath)
	if err != nil || len(targets) == 0 {
		return false
	}
	var st syscall.Statfs_t
	if err := syscall.Statfs(targets[0].Root, &st); err != nil {
		return false
	}
	out.FromStatfsT(&st)
	return true
}

// firstExistingPhysical resolves the first existing target and its physical path.
func firstExistingPhysical(rt *router.Router, virtualPath string) (router.Target, string, syscall.Errno) {
	targets, err := rt.ResolveReadTargets(virtualPath)
	if err != nil {
		return router.Target{}, "", toErrno(err)
	}

	for _, t := range targets {
		physicalPath := filepath.Join(t.Root, virtualPath)
		if _, err := os.Lstat(physicalPath); err != nil {
			if errors.Is(err, os.ErrNotExist) {
				continue
			}
			return router.Target{}, "", fs.ToErrno(err)
		}
		return t, physicalPath, 0
	}
	return router.Target{}, "", syscall.ENOENT
}
