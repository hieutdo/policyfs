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
	"github.com/hieutdo/policyfs/internal/errkind"
	"github.com/hieutdo/policyfs/internal/indexdb"
	"github.com/hieutdo/policyfs/internal/router"
	"github.com/rs/zerolog"
	"golang.org/x/sys/unix"
)

// resolvedLookupPath describes a resolved virtual path and the target that supplied it.
type resolvedLookupPath struct {
	targetID string
	statx    *unix.Statx_t // populated by physical statx requests
	attr     gofuse.Attr
}

// typeMode returns the file type bits needed for the child inode stable attributes.
func (r resolvedLookupPath) typeMode() uint32 {
	return r.attr.Mode & uint32(syscall.S_IFMT)
}

// metadataRequest selects the physical metadata syscall for this routing pass.
type metadataRequest struct {
	op           string
	readPhysical func(path string) (*resolvedLookupPath, syscall.Errno)
}

// lstatMetadata reads physical lookup/getattr fields without following symlinks.
func lstatMetadata(path string) (*resolvedLookupPath, syscall.Errno) {
	var st syscall.Stat_t
	if err := syscall.Lstat(path, &st); err != nil {
		return nil, toErrno(err)
	}
	result := &resolvedLookupPath{}
	result.attr.FromStat(&st)
	result.attr.Ino = 0 // go-fuse supplies the virtual inode number
	return result, 0
}

// resolveLookupPath resolves a virtual path using first-match read targets and a directory-only list fallback.
func resolveLookupPath(ctx context.Context, rt *router.Router, db *indexdb.DB, log zerolog.Logger, virtualPath string, req metadataRequest) (*resolvedLookupPath, syscall.Errno) {
	targets, err := rt.ResolveReadTargets(virtualPath)
	if err != nil {
		return nil, toErrno(err)
	}
	log.Debug().Str("op", req.op).Str("path", virtualPath).Msg("lookup resolved read targets")

	resolved, seenTargets, errno := resolveLookupPathInTargets(ctx, targets, db, log, virtualPath, req, false, nil)
	if errno != 0 {
		return nil, errno
	}
	if resolved != nil {
		log.Debug().Str("op", req.op).Str("path", virtualPath).Str("storage_id", resolved.targetID).Msg("lookup resolved on read target")
		return resolved, 0
	}

	listTargets, err := rt.ResolveListTargets(virtualPath)
	if err != nil {
		return nil, toErrno(err)
	}
	log.Debug().Str("op", req.op).Str("path", virtualPath).Msg("lookup read targets exhausted; trying list fallback")

	resolved, _, errno = resolveLookupPathInTargets(ctx, listTargets, db, log, virtualPath, req, true, seenTargets)
	if errno != 0 {
		return nil, errno
	}
	if resolved != nil {
		log.Debug().Str("op", req.op).Str("path", virtualPath).Str("storage_id", resolved.targetID).Msg("lookup resolved on list fallback")
		return resolved, 0
	}

	log.Debug().Str("op", req.op).Str("path", virtualPath).Msg("lookup missed on all targets")
	return nil, syscall.ENOENT
}

// resolveLookupPathInTargets scans targets in order and optionally restricts matches to directories.
func resolveLookupPathInTargets(ctx context.Context, targets []router.Target, db *indexdb.DB, log zerolog.Logger, virtualPath string, req metadataRequest, directoriesOnly bool, seenTargets map[string]struct{}) (*resolvedLookupPath, map[string]struct{}, syscall.Errno) {
	if seenTargets == nil {
		seenTargets = make(map[string]struct{}, len(targets))
	}

	for _, t := range targets {
		if _, seen := seenTargets[t.ID]; seen {
			log.Debug().Str("op", req.op).Str("path", virtualPath).Str("storage_id", t.ID).Bool("indexed", t.Indexed).Msg("lookup skipped previously scanned target")
			continue
		}
		seenTargets[t.ID] = struct{}{}
		log.Debug().Str("op", req.op).Str("path", virtualPath).Str("storage_id", t.ID).Bool("indexed", t.Indexed).Msg("lookup scanning target")

		resolved, errno := resolveLookupPathOnTarget(ctx, t, db, log, virtualPath, req, directoriesOnly)
		if errno != 0 {
			return nil, seenTargets, errno
		}
		if resolved != nil {
			return resolved, seenTargets, 0
		}
	}

	return nil, seenTargets, 0
}

// resolveLookupPathOnTarget resolves a virtual path against a single target.
func resolveLookupPathOnTarget(ctx context.Context, t router.Target, db *indexdb.DB, log zerolog.Logger, virtualPath string, req metadataRequest, directoriesOnly bool) (*resolvedLookupPath, syscall.Errno) {
	if !t.Indexed {
		p := filepath.Join(t.Root, virtualPath)
		resolved, errno := req.readPhysical(p)
		if errno != 0 {
			if errno == syscall.ENOENT {
				log.Debug().Str("op", req.op).Str("path", virtualPath).Str("storage_id", t.ID).Str("real_path", p).Msg("lookup missed on non-indexed target")
				return nil, 0
			}
			log.Debug().Str("op", req.op).Str("path", virtualPath).Str("storage_id", t.ID).Str("real_path", p).Err(errno).Msg("failed to lookup on non-indexed target")
			return nil, errno
		}
		if directoriesOnly && resolved.typeMode() != syscall.S_IFDIR {
			log.Debug().Str("op", req.op).Str("path", virtualPath).Str("storage_id", t.ID).Str("real_path", p).Msg("lookup skipped non-directory on non-indexed target")
			return nil, 0
		}
		log.Debug().Str("op", req.op).Str("path", virtualPath).Str("storage_id", t.ID).Str("real_path", p).Msg("lookup hit on non-indexed target")
		resolved.targetID = t.ID
		return resolved, 0
	}

	if db == nil {
		if req.op == "lookup" {
			log.Error().Str("op", req.op).Str("path", virtualPath).Str("storage_id", t.ID).Msg("failed to lookup: db is nil for indexed target")
		}
		return nil, syscall.EIO
	}

	f, ok, err := db.GetEffectiveFile(ctx, t.ID, virtualPath)
	if err != nil {
		log.Debug().Str("op", req.op).Str("path", virtualPath).Str("storage_id", t.ID).Err(err).Msg("failed to lookup indexed file")
		return nil, toErrno(fmt.Errorf("failed to %s indexed file: %w", req.op, err))
	}
	if ok {
		realPath := filepath.Join(t.Root, f.RealPath)
		if directoriesOnly && !f.IsDir {
			log.Debug().Str("op", req.op).Str("path", virtualPath).Str("storage_id", t.ID).Str("real_path", realPath).Msg("lookup skipped non-directory on indexed target")
			return nil, 0
		}
		log.Debug().Str("op", req.op).Str("path", virtualPath).Str("storage_id", t.ID).Str("real_path", realPath).Msg("lookup hit on indexed target")
		return &resolvedLookupPath{
			targetID: t.ID,
			attr: gofuse.Attr{
				Size:  uint64(f.Size),
				Mtime: uint64(f.MTimeSec),
				Mode:  f.Mode,
				Nlink: 1,
				Owner: gofuse.Owner{Uid: f.UID, Gid: f.GID},
			},
		}, 0
	}
	log.Debug().Str("op", req.op).Str("path", virtualPath).Str("storage_id", t.ID).Msg("lookup indexed file missed; probing directory")

	dirOK, err := db.DirExists(ctx, t.ID, virtualPath)
	if err != nil {
		log.Debug().Str("op", req.op).Str("path", virtualPath).Str("storage_id", t.ID).Err(err).Msg("failed to lookup indexed dir")
		return nil, toErrno(fmt.Errorf("failed to %s indexed dir: %w", req.op, err))
	}
	if !dirOK {
		log.Debug().Str("op", req.op).Str("path", virtualPath).Str("storage_id", t.ID).Msg("lookup missed on indexed target")
		return nil, 0
	}

	log.Debug().Str("op", req.op).Str("path", virtualPath).Str("storage_id", t.ID).Msg("lookup synthesized indexed directory")
	return &resolvedLookupPath{targetID: t.ID, attr: gofuse.Attr{Mode: syscall.S_IFDIR | 0o755, Nlink: 1}}, 0
}

// newResolvedChildInode creates a child inode from resolved lookup metadata.
func newResolvedChildInode(ctx context.Context, parent *fs.Inode, mountName string, state *runtimeState, reload *reloadState, db *indexdb.DB, disk *diskAccessLogger, open *OpenTracker, virtualPath string, resolved *resolvedLookupPath) *fs.Inode {
	child := &Node{mountName: mountName, state: state, reload: reload, db: db, disk: disk, open: open}
	return parent.NewInode(ctx, child, fs.StableAttr{Mode: resolved.typeMode(), Ino: stableIno(resolved.targetID, virtualPath), Gen: 1})
}

// lookupChild looks up a child by name using router read targets.
func lookupChild(ctx context.Context, parent *fs.Inode, mountName string, state *runtimeState, reload *reloadState, rt *router.Router, db *indexdb.DB, log zerolog.Logger, disk *diskAccessLogger, open *OpenTracker, name string, out *gofuse.EntryOut) (*fs.Inode, syscall.Errno) {
	if parent == nil {
		return nil, toErrno(&errkind.NilError{What: "parent inode"})
	}
	if rt == nil {
		return nil, toErrno(&errkind.NilError{What: "router"})
	}

	parentPath := parent.Path(parent.Root())
	childPath, errno := joinVirtualPath(parentPath, name)
	if errno != 0 {
		return nil, errno
	}

	resolved, errno := resolveLookupPath(ctx, rt, db, log, childPath, metadataRequest{op: "lookup", readPhysical: lstatMetadata})
	if errno != 0 {
		return nil, errno
	}

	out.Attr = resolved.attr
	return newResolvedChildInode(ctx, parent, mountName, state, reload, db, disk, open, childPath, resolved), 0
}

// resolveInodeMetadata finds metadata for an inode using the same routing as Lookup.
func resolveInodeMetadata(ctx context.Context, ino *fs.Inode, rt *router.Router, db *indexdb.DB, log zerolog.Logger, req metadataRequest) (*resolvedLookupPath, syscall.Errno) {
	if ino == nil {
		return nil, toErrno(&errkind.NilError{What: "inode"})
	}
	if rt == nil {
		return nil, toErrno(&errkind.NilError{What: "router"})
	}

	virtualPath := ino.Path(ino.Root())
	if virtualPath == "." {
		virtualPath = ""
	}
	if errno := validateVirtualPath(virtualPath); errno != 0 {
		return nil, errno
	}

	return resolveLookupPath(ctx, rt, db, log, virtualPath, req)
}

// readdirPath lists directory entries across read targets and dedupes by name.
func readdirPath(ctx context.Context, ino *fs.Inode, rt *router.Router, db *indexdb.DB, log zerolog.Logger) (fs.DirStream, syscall.Errno) {
	entries, errno := listDirEntries(ctx, ino, rt, db, log)
	if errno != 0 {
		return nil, errno
	}
	return fs.NewListDirStream(entries), 0
}

// listDirEntries returns merged directory entries across read targets (union + dedupe).
func listDirEntries(ctx context.Context, ino *fs.Inode, rt *router.Router, db *indexdb.DB, log zerolog.Logger) ([]gofuse.DirEntry, syscall.Errno) {
	if ino == nil {
		return nil, toErrno(&errkind.NilError{What: "inode"})
	}
	if rt == nil {
		return nil, toErrno(&errkind.NilError{What: "router"})
	}

	virtualPath := ino.Path(ino.Root())
	return listDirEntriesForVirtualPath(ctx, virtualPath, rt, db, log)
}

// listDirEntriesForVirtualPath returns merged directory entries across read targets (union + dedupe).
func listDirEntriesForVirtualPath(ctx context.Context, virtualPath string, rt *router.Router, db *indexdb.DB, log zerolog.Logger) ([]gofuse.DirEntry, syscall.Errno) {
	if rt == nil {
		return nil, toErrno(&errkind.NilError{What: "router"})
	}
	if virtualPath == "." {
		virtualPath = ""
	}
	if errno := validateVirtualPath(virtualPath); errno != 0 {
		return nil, errno
	}

	targets, err := rt.ResolveListTargets(virtualPath)
	if err != nil {
		return nil, toErrno(err)
	}
	log.Debug().Str("op", "readdir").Str("path", virtualPath).Msg("readdir resolved list targets")

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
					log.Debug().Str("op", "readdir").Str("path", virtualPath).Str("storage_id", t.ID).Str("real_path", p).Msg("readdir missed on non-indexed target")
					continue
				}
				log.Debug().Str("op", "readdir").Str("path", virtualPath).Str("storage_id", t.ID).Str("real_path", p).Err(err).Msg("failed to readdir non-indexed target")
				return nil, toErrno(err)
			}
			foundAnyDir = true
			log.Debug().Str("op", "readdir").Str("path", virtualPath).Str("storage_id", t.ID).Str("real_path", p).Msg("readdir scanning non-indexed target")
			for _, e := range list {
				name := e.Name()
				mode := dirEntryTypeMode(e)

				childPath, errno := joinVirtualPath(virtualPath, name)
				if errno != 0 {
					continue
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
					log.Debug().Str("op", "readdir").Str("path", childPath).Str("storage_id", t.ID).Msg("readdir skipped entry by routing")
					continue
				}

				if _, ok := seen[name]; ok {
					log.Debug().Str("op", "readdir").Str("path", childPath).Str("storage_id", t.ID).Msg("readdir skipped duplicate entry")
					continue
				}
				seen[name] = struct{}{}
				log.Debug().Str("op", "readdir").Str("path", childPath).Str("storage_id", t.ID).Msg("readdir added entry")
				entries = append(entries, gofuse.DirEntry{Name: name, Mode: mode, Ino: stableIno(t.ID, childPath)})
			}
			continue
		}

		if db == nil {
			log.Error().Str("op", "readdir").Str("path", virtualPath).Str("storage_id", t.ID).Msg("failed to readdir: db is nil for indexed target")
			return nil, syscall.EIO
		}
		list, ok, err := db.ListDirEntries(ctx, t.ID, virtualPath)
		if err != nil {
			log.Debug().Str("op", "readdir").Str("path", virtualPath).Str("storage_id", t.ID).Err(err).Msg("failed to readdir indexed target")
			return nil, toErrno(fmt.Errorf("failed to readdir indexed dir: %w", err))
		}
		if !ok {
			log.Debug().Str("op", "readdir").Str("path", virtualPath).Str("storage_id", t.ID).Msg("readdir missed on indexed target")
			continue
		}
		foundAnyDir = true
		log.Debug().Str("op", "readdir").Str("path", virtualPath).Str("storage_id", t.ID).Msg("readdir scanning indexed target")
		for _, e := range list {
			name := e.Name

			childPath, errno := joinVirtualPath(virtualPath, name)
			if errno != 0 {
				continue
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
				log.Debug().Str("op", "readdir").Str("path", childPath).Str("storage_id", t.ID).Msg("readdir skipped entry by routing")
				continue
			}

			if _, ok := seen[name]; ok {
				log.Debug().Str("op", "readdir").Str("path", childPath).Str("storage_id", t.ID).Msg("readdir skipped duplicate entry")
				continue
			}
			seen[name] = struct{}{}
			log.Debug().Str("op", "readdir").Str("path", childPath).Str("storage_id", t.ID).Msg("readdir added entry")
			entries = append(entries, gofuse.DirEntry{Name: name, Mode: e.Mode, Ino: stableIno(t.ID, childPath)})
		}
	}
	if !foundAnyDir {
		log.Debug().Str("op", "readdir").Str("path", virtualPath).Msg("readdir missed on all targets")
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
	log.Debug().Str("op", "readdir").Str("path", virtualPath).Msg("readdir merged entries")
	_ = ctx
	return out, 0
}

// dirEntryTypeMode maps an os.DirEntry type bitmask to the corresponding FUSE S_IFMT file type.
func dirEntryTypeMode(e os.DirEntry) uint32 {
	if e.IsDir() {
		return uint32(gofuse.S_IFDIR)
	}
	typ := e.Type()
	switch {
	case typ&os.ModeSymlink != 0:
		return uint32(gofuse.S_IFLNK)
	case typ&os.ModeNamedPipe != 0:
		return uint32(syscall.S_IFIFO)
	case typ&os.ModeSocket != 0:
		return uint32(syscall.S_IFSOCK)
	case typ&os.ModeCharDevice != 0:
		return uint32(syscall.S_IFCHR)
	case typ&os.ModeDevice != 0:
		return uint32(syscall.S_IFBLK)
	default:
		return uint32(gofuse.S_IFREG)
	}
}
