package fuse

import (
	"context"
	"path/filepath"
	"syscall"

	"github.com/hanwen/go-fuse/v2/fs"
	"github.com/hieutdo/policyfs/internal/errkind"
)

// Unlink removes a child file on the first existing read target.
func (n *Node) Unlink(ctx context.Context, name string) syscall.Errno {
	if n == nil {
		return fs.ToErrno(&errkind.NilError{What: "node"})
	}
	if n.rt == nil {
		return fs.ToErrno(&errkind.NilError{What: "router"})
	}

	parentVirtualPath := n.Path(n.Root())
	virtualPath := filepath.Join(parentVirtualPath, name)

	// We remove the first existing physical entry based on the read-target resolution order.
	// This matches how lookups/read resolve across multiple targets.
	target, physicalPath, errno := firstExistingPhysical(n.rt, virtualPath)
	if errno != 0 {
		return errno
	}
	// Indexed targets are not writable yet.
	if target.Indexed {
		return syscall.EROFS
	}
	_ = ctx
	return fs.ToErrno(syscall.Unlink(physicalPath))
}

// Rmdir removes a child directory on the first existing read target.
func (n *Node) Rmdir(ctx context.Context, name string) syscall.Errno {
	if n == nil {
		return fs.ToErrno(&errkind.NilError{What: "node"})
	}
	if n.rt == nil {
		return fs.ToErrno(&errkind.NilError{What: "router"})
	}

	parentVirtualPath := n.Path(n.Root())
	virtualPath := filepath.Join(parentVirtualPath, name)

	// We remove the first existing physical entry based on the read-target resolution order.
	target, physicalPath, errno := firstExistingPhysical(n.rt, virtualPath)
	if errno != 0 {
		return errno
	}
	// Indexed targets are not writable yet.
	if target.Indexed {
		return syscall.EROFS
	}
	_ = ctx
	return fs.ToErrno(syscall.Rmdir(physicalPath))
}
