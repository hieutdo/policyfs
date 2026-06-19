package fuse

import (
	"context"
	"errors"
	"os"
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

// stableIno returns a deterministic inode number for a (storageID, virtualPath) pair.
//
// This avoids flakiness and copy safety issues in tools like mv/cp when the kernel
// decides not to cache directory entries and we end up recreating inodes frequently.
func stableIno(storageID string, virtualPath string) uint64 {
	const (
		fnv64Offset = 14695981039346656037
		fnv64Prime  = 1099511628211
	)

	h := uint64(fnv64Offset)
	for i := 0; i < len(storageID); i++ {
		h ^= uint64(storageID[i])
		h *= fnv64Prime
	}
	// Delimiter.
	h ^= uint64(0xff)
	h *= fnv64Prime
	for i := 0; i < len(virtualPath); i++ {
		h ^= uint64(virtualPath[i])
		h *= fnv64Prime
	}

	ino := h | (1 << 63)
	if ino == 0 || ino == ^uint64(0) {
		ino = (1 << 63) + 1
	}
	return ino
}

// openFirst opens a file by searching targets in the router-defined order.
func openFirst(ctx context.Context, rt *router.Router, db *indexdb.DB, log zerolog.Logger, virtualPath string, flags int, write bool) (fs.FileHandle, uint32, syscall.Errno) {
	if rt == nil {
		return nil, 0, toErrno(&errkind.NilError{What: "router"})
	}
	if errno := validateVirtualPath(virtualPath); errno != 0 {
		return nil, 0, errno
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
	log.Debug().Str("op", "open").Str("path", virtualPath).Bool("write", write).Msg("open resolved targets")

	sawIndexed := false
	for _, t := range targets {
		log.Debug().Str("op", "open").Str("path", virtualPath).Str("storage_id", t.ID).Bool("indexed", t.Indexed).Bool("write", write).Msg("open scanning target")
		if write && t.Indexed {
			sawIndexed = true
			log.Debug().Str("op", "open").Str("path", virtualPath).Str("storage_id", t.ID).Msg("open skipped indexed write target")
			continue
		}
		physicalPath := filepath.Join(t.Root, virtualPath)
		if t.Indexed {
			if db == nil {
				log.Error().Str("op", "open").Str("path", virtualPath).Str("storage_id", t.ID).Msg("failed to open: db is nil for indexed target")
				return nil, 0, syscall.EIO
			}
			f, ok, err := db.GetEffectiveFile(ctx, t.ID, virtualPath)
			if err != nil {
				log.Debug().Str("op", "open").Str("path", virtualPath).Str("storage_id", t.ID).Err(err).Msg("failed to resolve indexed file for open")
				return nil, 0, toErrno(err)
			}
			if !ok {
				log.Debug().Str("op", "open").Str("path", virtualPath).Str("storage_id", t.ID).Msg("open missed on indexed target")
				continue
			}
			if f.IsDir {
				log.Debug().Str("op", "open").Str("path", virtualPath).Str("storage_id", t.ID).Msg("open skipped directory on indexed target")
				continue
			}
			log.Debug().Str("op", "open").Str("path", virtualPath).Str("storage_id", t.ID).Str("real_path", f.RealPath).Msg("open resolved indexed real path")
			if errno := validateVirtualPath(f.RealPath); errno != 0 {
				return nil, 0, errno
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
					log.Debug().Str("op", "open").Str("path", virtualPath).Str("storage_id", t.ID).Str("real_path", physicalPath).Msg("open trying stale real_path fallback")
					if fd2, oerr2 := syscall.Open(fallbackPath, flags, 0); oerr2 == nil {
						log.Debug().Str("op", "open").Str("path", virtualPath).Str("storage_id", t.ID).Str("real_path", fallbackPath).Msg("open resolved on stale real_path fallback")
						fh := &FileHandle{virtualPath: virtualPath, physicalPath: fallbackPath, storageID: t.ID, indexed: t.Indexed, fallback: true, fd: fd2, flags: uint32(flags)}
						_ = ctx
						return fh, 0, 0
					} else {
						log.Debug().Str("op", "open").Str("path", virtualPath).Str("storage_id", t.ID).Str("real_path", physicalPath).Err(oerr2).Msg("failed to open on stale real_path fallback")
					}
				}
			}
			if errors.Is(oerr, syscall.ENOENT) {
				log.Debug().Str("op", "open").Str("path", virtualPath).Str("storage_id", t.ID).Str("real_path", physicalPath).Msg("open missed on target")
				continue
			}
			log.Debug().Str("op", "open").Str("path", virtualPath).Str("storage_id", t.ID).Str("real_path", physicalPath).Err(oerr).Msg("failed to open on target")
			return nil, 0, toErrno(oerr)
		}
		fh := &FileHandle{virtualPath: virtualPath, physicalPath: physicalPath, storageID: t.ID, indexed: t.Indexed, fd: fd, flags: uint32(flags)}
		_ = ctx
		return fh, 0, 0
	}
	if sawIndexed {
		log.Debug().Str("op", "open").Str("path", virtualPath).Bool("write", write).Msg("open blocked by indexed-only write targets")
		return nil, 0, syscall.EROFS
	}
	log.Debug().Str("op", "open").Str("path", virtualPath).Bool("write", write).Msg("open missed on all targets")
	return nil, 0, syscall.ENOENT
}

// newChildInode creates a child inode with Node ops and a deterministic inode number.
func newChildInode(ctx context.Context, parent *fs.Inode, rootData *fs.LoopbackRoot, mountName string, state *runtimeState, reload *reloadState, db *indexdb.DB, disk *diskAccessLogger, open *OpenTracker, storageID string, virtualPath string, stMode uint32) *fs.Inode {
	child := &Node{LoopbackNode: &fs.LoopbackNode{RootData: rootData}, mountName: mountName, state: state, reload: reload, db: db, disk: disk, open: open}
	typeMode := uint32(stMode & syscall.S_IFMT)
	ch := parent.NewInode(ctx, child, fs.StableAttr{Mode: typeMode, Ino: stableIno(storageID, virtualPath), Gen: 1})
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

// statfsPooledRoots pools statfs across multiple filesystem roots and populates out.
//
// Returns (ok, hadError) where ok indicates at least one root was successfully stat'ed.
func statfsPooledRoots(roots []string, out *gofuse.StatfsOut) (bool, bool) {
	if len(roots) == 0 {
		return false, false
	}

	seen := map[string]struct{}{}
	var base syscall.Statfs_t
	baseSet := false
	unit := uint64(0)

	totalBytes := uint64(0)
	freeBytes := uint64(0)
	availBytes := uint64(0)
	files := uint64(0)
	ffree := uint64(0)

	hadError := false
	for _, root := range roots {
		root = strings.TrimSpace(root)
		if root == "" {
			continue
		}
		if _, ok := seen[root]; ok {
			continue
		}
		seen[root] = struct{}{}

		var st syscall.Statfs_t
		if err := syscall.Statfs(root, &st); err != nil {
			hadError = true
			continue
		}

		u := uint64(0)
		if st.Bsize > 0 {
			u = uint64(st.Bsize)
		}
		if u == 0 {
			u = 4096
		}
		if !baseSet {
			base = st
			unit = u
			baseSet = true
		}

		totalBytes += st.Blocks * u
		freeBytes += st.Bfree * u
		availBytes += st.Bavail * u
		files += st.Files
		ffree += st.Ffree
	}
	if !baseSet || unit == 0 {
		return false, hadError
	}

	base.Blocks = totalBytes / unit
	base.Bfree = freeBytes / unit
	base.Bavail = availBytes / unit
	base.Files = files
	base.Ffree = ffree
	out.FromStatfsT(&base)
	return true, hadError
}

// firstExistingPhysical resolves the first existing target and its physical path.
func firstExistingPhysical(rt *router.Router, virtualPath string) (router.Target, string, syscall.Errno) {
	if errno := validateVirtualPath(virtualPath); errno != 0 {
		return router.Target{}, "", errno
	}
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
			return router.Target{}, "", toErrno(err)
		}
		return t, physicalPath, 0
	}
	return router.Target{}, "", syscall.ENOENT
}
