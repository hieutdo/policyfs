package fuse

import (
	"context"
	"fmt"
	"path/filepath"
	"syscall"

	"github.com/hanwen/go-fuse/v2/fs"
)

// Root is the PolicyFS root node. Today it delegates to go-fuse's loopback implementation,
// but it provides a stable place to implement PolicyFS-specific behavior.
type Root struct {
	*fs.LoopbackNode
}

// Node is a PolicyFS inode implementation. For now it wraps go-fuse's LoopbackNode.
type Node struct {
	*fs.LoopbackNode
}

// NewRoot creates the PolicyFS root node for mounting.
//
// Currently this is a thin wrapper around go-fuse's loopback root to keep behavior
// identical while we incrementally add PolicyFS operations.
func NewRoot(rootPath string) (fs.InodeEmbedder, error) {
	op, err := fs.NewLoopbackRoot(rootPath)
	if err != nil {
		return nil, fmt.Errorf("failed to create loopback root: %w", err)
	}

	lb, ok := op.(*fs.LoopbackNode)
	if !ok {
		// Fallback: preserve behavior if go-fuse changes the concrete type.
		return op, nil
	}

	return &Root{LoopbackNode: lb}, nil
}

// WrapChild wraps child inodes so we can override ops without rewriting the loopback FS.
func (r *Root) WrapChild(ctx context.Context, ops fs.InodeEmbedder) fs.InodeEmbedder {
	lb, ok := ops.(*fs.LoopbackNode)
	if !ok {
		return ops
	}
	return &Node{LoopbackNode: lb}
}

// WrapChild wraps descendant nodes.
func (n *Node) WrapChild(ctx context.Context, ops fs.InodeEmbedder) fs.InodeEmbedder {
	lb, ok := ops.(*fs.LoopbackNode)
	if !ok {
		return ops
	}
	return &Node{LoopbackNode: lb}
}

// Open opens a file and returns a cached FileHandle.
func (n *Node) Open(ctx context.Context, flags uint32) (fs.FileHandle, uint32, syscall.Errno) {
	virtualPath := n.Path(n.Root())
	physicalPath := filepath.Join(n.RootData.Path, virtualPath)

	fd, err := syscall.Open(physicalPath, int(flags), 0)
	if err != nil {
		return nil, 0, fs.ToErrno(err)
	}

	fh := &FileHandle{
		virtualPath:  virtualPath,
		physicalPath: physicalPath,
		fd:           fd,
		flags:        flags,
	}

	return fh, 0, 0
}

// Release closes any file handles we created.
func (n *Node) Release(ctx context.Context, f fs.FileHandle) syscall.Errno {
	if r, ok := f.(interface {
		Release(ctx context.Context) syscall.Errno
	}); ok {
		return r.Release(ctx)
	}
	return 0
}
