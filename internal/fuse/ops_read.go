package fuse

import (
	"context"
	"errors"
	"os"
	"path/filepath"
	"syscall"

	"github.com/hanwen/go-fuse/v2/fs"
	gofuse "github.com/hanwen/go-fuse/v2/fuse"
	"github.com/hieutdo/policyfs/internal/router"
)

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
		return nil, toErrno(err)
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
		return toErrno(err)
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
	return listDirEntriesForVirtualPath(ctx, virtualPath, rt)
}

// listDirEntriesForVirtualPath returns merged directory entries across read targets (union + dedupe).
func listDirEntriesForVirtualPath(ctx context.Context, virtualPath string, rt *router.Router) ([]gofuse.DirEntry, syscall.Errno) {
	if rt == nil {
		return nil, fs.ToErrno(errors.New("router is nil"))
	}

	targets, err := rt.ResolveListTargets(virtualPath)
	if err != nil {
		return nil, toErrno(err)
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
	_ = ctx
	return entries, 0
}
