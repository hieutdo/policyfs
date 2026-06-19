package fuse

import (
	"context"
	"syscall"

	"github.com/hanwen/go-fuse/v2/fs"
	gofuse "github.com/hanwen/go-fuse/v2/fuse"
	"github.com/hieutdo/policyfs/internal/errkind"
)

// Symlink creates a symbolic link using the current loopback behavior while emitting
// structured debug logs for support and debugging.
func (n *Node) Symlink(ctx context.Context, target string, name string, out *gofuse.EntryOut) (*fs.Inode, syscall.Errno) {
	if n == nil {
		return nil, toErrno(&errkind.NilError{What: "node"})
	}
	_, log := n.runtime()

	parentVirtualPath := n.Path(n.Root())
	if parentVirtualPath == "." {
		parentVirtualPath = ""
	}
	virtualPath, errno := joinVirtualPath(parentVirtualPath, name)
	if errno != 0 {
		return nil, errno
	}
	log.Debug().Str("op", "symlink").Str("path", virtualPath).Str("target", target).Msg("symlink using loopback behavior")

	ch, errno := n.LoopbackNode.Symlink(ctx, target, name, out)
	if errno != 0 {
		log.Error().Str("op", "symlink").Str("path", virtualPath).Str("target", target).Err(errno).Msg("failed to symlink")
		return nil, errno
	}

	log.Debug().Str("op", "symlink").Str("path", virtualPath).Str("target", target).Msg("symlink")
	return ch, 0
}
