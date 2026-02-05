package fuse

import (
	"context"
	"errors"
	"os"
	"path/filepath"
	"syscall"

	"github.com/hanwen/go-fuse/v2/fs"
	"github.com/hieutdo/policyfs/internal/router"
)

// openFirst opens a file by searching targets in the router-defined order.
func openFirst(ctx context.Context, rt *router.Router, virtualPath string, flags int, write bool) (fs.FileHandle, uint32, syscall.Errno) {
	if rt == nil {
		return nil, 0, fs.ToErrno(errors.New("router is nil"))
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
		fd, oerr := syscall.Open(physicalPath, flags, 0)
		if oerr != nil {
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
func newChildInode(ctx context.Context, parent *fs.Inode, rootData *fs.LoopbackRoot, rt *router.Router, stMode uint32) *fs.Inode {
	child := &Node{LoopbackNode: &fs.LoopbackNode{RootData: rootData}, rt: rt}
	typeMode := uint32(stMode & syscall.S_IFMT)
	ch := parent.NewInode(ctx, child, fs.StableAttr{Mode: typeMode, Gen: 1})
	return ch
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
