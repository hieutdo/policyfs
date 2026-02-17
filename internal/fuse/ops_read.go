package fuse

import (
	"context"
	"errors"
	"fmt"
	"os"
	"path"
	"path/filepath"
	"strings"
	"syscall"

	"github.com/hanwen/go-fuse/v2/fs"
	gofuse "github.com/hanwen/go-fuse/v2/fuse"
	"github.com/hieutdo/policyfs/internal/errkind"
	"github.com/hieutdo/policyfs/internal/indexdb"
	"github.com/hieutdo/policyfs/internal/router"
	"github.com/rs/zerolog"
)

// lookupChild looks up a child by name using router read targets.
func lookupChild(ctx context.Context, parent *fs.Inode, rootData *fs.LoopbackRoot, mountName string, rt *router.Router, db *indexdb.DB, log zerolog.Logger, disk *diskAccessLogger, name string, out *gofuse.EntryOut) (*fs.Inode, syscall.Errno) {
	if parent == nil {
		return nil, fs.ToErrno(&errkind.NilError{What: "parent inode"})
	}
	if rt == nil {
		return nil, fs.ToErrno(&errkind.NilError{What: "router"})
	}

	parentPath := parent.Path(parent.Root())
	childPath := path.Clean(path.Join(parentPath, name))
	if childPath == "." {
		childPath = ""
	}
	if childPath == ".." || strings.HasPrefix(childPath, "../") {
		childPath = ""
	}

	targets, err := rt.ResolveReadTargets(childPath)
	if err != nil {
		return nil, toErrno(err)
	}

	for _, t := range targets {
		if !t.Indexed {
			p := filepath.Join(t.Root, childPath)
			st := syscall.Stat_t{}
			err := syscall.Lstat(p, &st)
			if err != nil {
				if errors.Is(err, syscall.ENOENT) {
					continue
				}
				return nil, fs.ToErrno(err)
			}
			out.FromStat(&st)

			typeMode := uint32(st.Mode & syscall.S_IFMT)
			child := &Node{LoopbackNode: &fs.LoopbackNode{RootData: rootData}, mountName: mountName, rt: rt, db: db, log: log, disk: disk}
			ch := parent.NewInode(ctx, child, fs.StableAttr{Mode: typeMode, Ino: stableIno(t.ID, childPath), Gen: 1})
			return ch, 0
		}

		if db == nil {
			log.Error().Str("op", "lookup").Str("path", childPath).Str("storage_id", t.ID).Msg("failed to lookup: db is nil for indexed target")
			return nil, syscall.EIO
		}

		f, ok, err := db.GetEffectiveFile(ctx, t.ID, childPath)
		if err != nil {
			return nil, toErrno(fmt.Errorf("failed to lookup indexed file: %w", err))
		}
		if ok {
			out.Size = uint64(f.Size)
			out.Mtime = uint64(f.MTimeSec)
			out.Mtimensec = 0
			out.Mode = f.Mode
			out.Nlink = 1
			out.Uid = f.UID
			out.Gid = f.GID

			typeMode := uint32(f.Mode & uint32(syscall.S_IFMT))
			child := &Node{LoopbackNode: &fs.LoopbackNode{RootData: rootData}, mountName: mountName, rt: rt, db: db, log: log, disk: disk}
			ch := parent.NewInode(ctx, child, fs.StableAttr{Mode: typeMode, Ino: stableIno(t.ID, childPath), Gen: 1})
			return ch, 0
		}

		dirOK, err := db.DirExists(ctx, t.ID, childPath)
		if err != nil {
			return nil, toErrno(fmt.Errorf("failed to lookup indexed dir: %w", err))
		}
		if dirOK {
			out.Size = 0
			out.Mtime = 0
			out.Mtimensec = 0
			out.Mode = uint32(syscall.S_IFDIR | 0o755)
			out.Nlink = 1
			out.Uid = 0
			out.Gid = 0

			child := &Node{LoopbackNode: &fs.LoopbackNode{RootData: rootData}, mountName: mountName, rt: rt, db: db, log: log, disk: disk}
			ch := parent.NewInode(ctx, child, fs.StableAttr{Mode: uint32(syscall.S_IFDIR), Ino: stableIno(t.ID, childPath), Gen: 1})
			return ch, 0
		}
	}

	return nil, syscall.ENOENT
}

// getattrPath gets attributes for a virtual path by searching read targets.
func getattrPath(ctx context.Context, ino *fs.Inode, rt *router.Router, db *indexdb.DB, out *gofuse.AttrOut) syscall.Errno {
	if ino == nil {
		return fs.ToErrno(&errkind.NilError{What: "inode"})
	}
	if rt == nil {
		return fs.ToErrno(&errkind.NilError{What: "router"})
	}

	virtualPath := ino.Path(ino.Root())
	targets, err := rt.ResolveReadTargets(virtualPath)
	if err != nil {
		return toErrno(err)
	}

	for _, t := range targets {
		if !t.Indexed {
			p := filepath.Join(t.Root, virtualPath)
			st := syscall.Stat_t{}
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

		if db == nil {
			return syscall.EIO
		}

		f, ok, err := db.GetEffectiveFile(ctx, t.ID, virtualPath)
		if err != nil {
			return toErrno(fmt.Errorf("failed to getattr indexed file: %w", err))
		}
		if ok {
			out.Size = uint64(f.Size)
			out.Mtime = uint64(f.MTimeSec)
			out.Mtimensec = 0
			out.Mode = f.Mode
			out.Nlink = 1
			out.Uid = f.UID
			out.Gid = f.GID
			return 0
		}

		dirOK, err := db.DirExists(ctx, t.ID, virtualPath)
		if err != nil {
			return toErrno(fmt.Errorf("failed to getattr indexed dir: %w", err))
		}
		if dirOK {
			out.Size = 0
			out.Mtime = 0
			out.Mtimensec = 0
			out.Mode = uint32(syscall.S_IFDIR | 0o755)
			out.Nlink = 1
			out.Uid = 0
			out.Gid = 0
			return 0
		}
	}
	return syscall.ENOENT
}

// readdirPath lists directory entries across read targets and dedupes by name.
func readdirPath(ctx context.Context, ino *fs.Inode, rt *router.Router, db *indexdb.DB) (fs.DirStream, syscall.Errno) {
	entries, errno := listDirEntries(ctx, ino, rt, db)
	if errno != 0 {
		return nil, errno
	}
	return fs.NewListDirStream(entries), 0
}

// listDirEntries returns merged directory entries across read targets (union + dedupe).
func listDirEntries(ctx context.Context, ino *fs.Inode, rt *router.Router, db *indexdb.DB) ([]gofuse.DirEntry, syscall.Errno) {
	if ino == nil {
		return nil, fs.ToErrno(&errkind.NilError{What: "inode"})
	}
	if rt == nil {
		return nil, fs.ToErrno(&errkind.NilError{What: "router"})
	}

	virtualPath := ino.Path(ino.Root())
	return listDirEntriesForVirtualPath(ctx, virtualPath, rt, db)
}

// listDirEntriesForVirtualPath returns merged directory entries across read targets (union + dedupe).
func listDirEntriesForVirtualPath(ctx context.Context, virtualPath string, rt *router.Router, db *indexdb.DB) ([]gofuse.DirEntry, syscall.Errno) {
	if rt == nil {
		return nil, fs.ToErrno(&errkind.NilError{What: "router"})
	}

	targets, err := rt.ResolveListTargets(virtualPath)
	if err != nil {
		return nil, toErrno(err)
	}

	readTargetCache := map[string][]router.Target{}
	listTargetCache := map[string][]router.Target{}

	seen := map[string]struct{}{}
	entries := []gofuse.DirEntry{}
	foundAnyDir := false

	for _, t := range targets {
		if !t.Indexed {
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
				mode := uint32(gofuse.S_IFREG)
				if e.IsDir() {
					mode = uint32(gofuse.S_IFDIR)
				}

				childPath := path.Clean(path.Join(virtualPath, name))
				if childPath == "." {
					childPath = ""
				}

				allowed := false
				if mode == uint32(gofuse.S_IFDIR) {
					lt, ok := listTargetCache[childPath]
					if !ok {
						lt, err = rt.ResolveListTargets(childPath)
						if err != nil {
							return nil, toErrno(err)
						}
						listTargetCache[childPath] = lt
					}
					for _, cand := range lt {
						if cand.ID == t.ID {
							allowed = true
							break
						}
					}
				} else {
					rtgt, ok := readTargetCache[childPath]
					if !ok {
						rtgt, err = rt.ResolveReadTargets(childPath)
						if err != nil {
							return nil, toErrno(err)
						}
						readTargetCache[childPath] = rtgt
					}
					for _, cand := range rtgt {
						if cand.ID == t.ID {
							allowed = true
							break
						}
					}
				}
				if !allowed {
					continue
				}

				if _, ok := seen[name]; ok {
					continue
				}
				seen[name] = struct{}{}
				entries = append(entries, gofuse.DirEntry{Name: name, Mode: mode, Ino: stableIno(t.ID, childPath)})
			}
			continue
		}

		if db == nil {
			return nil, syscall.EIO
		}
		list, ok, err := db.ListDirEntries(ctx, t.ID, virtualPath)
		if err != nil {
			return nil, toErrno(fmt.Errorf("failed to readdir indexed dir: %w", err))
		}
		if !ok {
			continue
		}
		foundAnyDir = true
		for _, e := range list {
			name := e.Name

			childPath := path.Clean(path.Join(virtualPath, name))
			if childPath == "." {
				childPath = ""
			}

			allowed := false
			if e.Mode == uint32(gofuse.S_IFDIR) {
				lt, ok := listTargetCache[childPath]
				if !ok {
					lt, err = rt.ResolveListTargets(childPath)
					if err != nil {
						return nil, toErrno(err)
					}
					listTargetCache[childPath] = lt
				}
				for _, cand := range lt {
					if cand.ID == t.ID {
						allowed = true
						break
					}
				}
			} else {
				rtgt, ok := readTargetCache[childPath]
				if !ok {
					rtgt, err = rt.ResolveReadTargets(childPath)
					if err != nil {
						return nil, toErrno(err)
					}
					readTargetCache[childPath] = rtgt
				}
				for _, cand := range rtgt {
					if cand.ID == t.ID {
						allowed = true
						break
					}
				}
			}
			if !allowed {
				continue
			}

			if _, ok := seen[name]; ok {
				continue
			}
			seen[name] = struct{}{}
			entries = append(entries, gofuse.DirEntry{Name: name, Mode: e.Mode, Ino: stableIno(t.ID, childPath)})
		}
	}
	if !foundAnyDir {
		return nil, syscall.ENOENT
	}

	// Add dot entries so `ls -al` shows `.` and `..`.
	// Many tools expect these to exist and will display odd output when they are missing.
	out := make([]gofuse.DirEntry, 0, len(entries)+2)
	if _, ok := seen["."]; !ok {
		out = append(out, gofuse.DirEntry{Name: ".", Mode: uint32(gofuse.S_IFDIR)})
	}
	if _, ok := seen[".."]; !ok {
		out = append(out, gofuse.DirEntry{Name: "..", Mode: uint32(gofuse.S_IFDIR)})
	}
	out = append(out, entries...)
	_ = ctx
	return out, 0
}
