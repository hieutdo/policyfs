package fuse

import (
	"context"
	"errors"
	"fmt"
	"os"
	"path/filepath"
	"syscall"

	"github.com/hanwen/go-fuse/v2/fs"
	gofuse "github.com/hanwen/go-fuse/v2/fuse"
	"github.com/hieutdo/policyfs/internal/config"
	"github.com/hieutdo/policyfs/internal/router"
)

// Node is a PolicyFS inode implementation (including the root inode).
type Node struct {
	*fs.LoopbackNode
	rt *router.Router
}

// NewRoot creates the PolicyFS root node for mounting.
//
// Currently this is a thin wrapper around go-fuse's loopback root to keep behavior
// identical while we incrementally add PolicyFS operations.
func NewRoot(m *config.MountConfig, primaryRootPath string) (fs.InodeEmbedder, error) {
	rt, err := router.New(m)
	if err != nil {
		return nil, fmt.Errorf("failed to create router: %w", err)
	}

	op, err := fs.NewLoopbackRoot(primaryRootPath)
	if err != nil {
		return nil, fmt.Errorf("failed to create loopback root: %w", err)
	}

	lb, ok := op.(*fs.LoopbackNode)
	if !ok {
		// Fallback: preserve behavior if go-fuse changes the concrete type.
		return op, nil
	}

	return &Node{LoopbackNode: lb, rt: rt}, nil
}

// WrapChild wraps descendant nodes.
func (n *Node) WrapChild(ctx context.Context, ops fs.InodeEmbedder) fs.InodeEmbedder {
	lb, ok := ops.(*fs.LoopbackNode)
	if !ok {
		return ops
	}
	return &Node{LoopbackNode: lb, rt: n.rt}
}

// Lookup resolves a child entry using the router's read target order.
func (n *Node) Lookup(ctx context.Context, name string, out *gofuse.EntryOut) (*fs.Inode, syscall.Errno) {
	return lookupChild(ctx, n.EmbeddedInode(), n.RootData, n.rt, name, out)
}

// Getattr reads attributes using the router's read target order.
func (n *Node) Getattr(ctx context.Context, f fs.FileHandle, out *gofuse.AttrOut) syscall.Errno {
	return getattrPath(ctx, n.EmbeddedInode(), n.rt, out)
}

// Readdir returns a union of directory entries across read targets, deduped by name.
func (n *Node) Readdir(ctx context.Context) (fs.DirStream, syscall.Errno) {
	return readdirPath(ctx, n.EmbeddedInode(), n.rt)
}

// OpendirHandle returns a directory handle that merges entries across read targets.
func (n *Node) OpendirHandle(ctx context.Context, flags uint32) (fs.FileHandle, uint32, syscall.Errno) {
	entries, errno := listDirEntries(ctx, n.EmbeddedInode(), n.rt)
	if errno != 0 {
		return nil, 0, errno
	}
	return &DirHandle{entries: entries}, 0, 0
}

// Open opens a file and returns a cached FileHandle.
func (n *Node) Open(ctx context.Context, flags uint32) (fs.FileHandle, uint32, syscall.Errno) {
	virtualPath := n.Path(n.Root())

	if flags&gofuse.O_ANYWRITE != 0 {
		return openFirst(ctx, n.rt, virtualPath, int(flags), true)
	}
	return openFirst(ctx, n.rt, virtualPath, int(flags), false)
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
		return nil, 0, fs.ToErrno(err)
	}

	for _, t := range targets {
		physicalPath := filepath.Join(t.Root, virtualPath)
		fd, oerr := syscall.Open(physicalPath, flags, 0)
		if oerr != nil {
			if errors.Is(oerr, syscall.ENOENT) {
				continue
			}
			return nil, 0, fs.ToErrno(oerr)
		}
		fh := &FileHandle{virtualPath: virtualPath, physicalPath: physicalPath, fd: fd, flags: uint32(flags)}
		return fh, 0, 0
	}
	return nil, 0, syscall.ENOENT
}

// lookupChild looks up a child by name using router read targets.
func lookupChild(ctx context.Context, parent *fs.Inode, rootData *fs.LoopbackRoot, rt *router.Router, name string, out *gofuse.EntryOut) (*fs.Inode, syscall.Errno) {
	if parent == nil {
		return nil, fs.ToErrno(errors.New("parent inode is nil"))
	}
	if rt == nil {
		return nil, fs.ToErrno(errors.New("router is nil"))
	}

	parentPath := parent.Path(parent.Root())
	childPath := filepath.Join(parentPath, name)

	targets, err := rt.ResolveReadTargets(childPath)
	if err != nil {
		return nil, fs.ToErrno(err)
	}

	st := syscall.Stat_t{}
	for _, t := range targets {
		p := filepath.Join(t.Root, childPath)
		err := syscall.Lstat(p, &st)
		if err != nil {
			if errors.Is(err, syscall.ENOENT) {
				continue
			}
			return nil, fs.ToErrno(err)
		}
		out.FromStat(&st)

		typeMode := uint32(st.Mode & syscall.S_IFMT)
		child := &Node{LoopbackNode: &fs.LoopbackNode{RootData: rootData}, rt: rt}
		ch := parent.NewInode(ctx, child, fs.StableAttr{Mode: typeMode, Gen: 1})
		return ch, 0
	}

	return nil, syscall.ENOENT
}

// getattrPath gets attributes for a virtual path by searching read targets.
func getattrPath(ctx context.Context, ino *fs.Inode, rt *router.Router, out *gofuse.AttrOut) syscall.Errno {
	if ino == nil {
		return fs.ToErrno(errors.New("inode is nil"))
	}
	if rt == nil {
		return fs.ToErrno(errors.New("router is nil"))
	}

	virtualPath := ino.Path(ino.Root())
	targets, err := rt.ResolveReadTargets(virtualPath)
	if err != nil {
		return fs.ToErrno(err)
	}

	st := syscall.Stat_t{}
	for _, t := range targets {
		p := filepath.Join(t.Root, virtualPath)
		err := syscall.Lstat(p, &st)
		if err != nil {
			if errors.Is(err, syscall.ENOENT) {
				continue
			}
			return fs.ToErrno(err)
		}
		out.FromStat(&st)
		return 0
	}
	return syscall.ENOENT
}

// readdirPath lists directory entries across read targets and dedupes by name.
func readdirPath(ctx context.Context, ino *fs.Inode, rt *router.Router) (fs.DirStream, syscall.Errno) {
	entries, errno := listDirEntries(ctx, ino, rt)
	if errno != 0 {
		return nil, errno
	}
	return fs.NewListDirStream(entries), 0
}

// listDirEntries returns merged directory entries across read targets (union + dedupe).
func listDirEntries(ctx context.Context, ino *fs.Inode, rt *router.Router) ([]gofuse.DirEntry, syscall.Errno) {
	if ino == nil {
		return nil, fs.ToErrno(errors.New("inode is nil"))
	}
	if rt == nil {
		return nil, fs.ToErrno(errors.New("router is nil"))
	}

	virtualPath := ino.Path(ino.Root())
	targets, err := rt.ResolveReadTargets(virtualPath)
	if err != nil {
		return nil, fs.ToErrno(err)
	}

	seen := map[string]struct{}{}
	entries := []gofuse.DirEntry{}
	foundAnyDir := false

	for _, t := range targets {
		p := filepath.Join(t.Root, virtualPath)
		list, err := os.ReadDir(p)
		if err != nil {
			if errors.Is(err, os.ErrNotExist) {
				continue
			}
			return nil, fs.ToErrno(err)
		}
		foundAnyDir = true
		for _, e := range list {
			name := e.Name()
			if _, ok := seen[name]; ok {
				continue
			}
			seen[name] = struct{}{}
			mode := uint32(gofuse.S_IFREG)
			if e.IsDir() {
				mode = uint32(gofuse.S_IFDIR)
			}
			entries = append(entries, gofuse.DirEntry{Name: name, Mode: mode})
		}
	}
	if !foundAnyDir {
		return nil, syscall.ENOENT
	}
	return entries, 0
}
