package fuse

import (
	"context"
	"errors"
	"fmt"
	"path/filepath"
	"syscall"
	"time"

	"github.com/hanwen/go-fuse/v2/fs"
	"github.com/hieutdo/policyfs/internal/errkind"
	"github.com/hieutdo/policyfs/internal/eventlog"
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

	targets, err := n.rt.ResolveReadTargets(virtualPath)
	if err != nil {
		return toErrno(err)
	}

	for _, t := range targets {
		if !t.Indexed {
			physicalPath := filepath.Join(t.Root, virtualPath)
			st := syscall.Stat_t{}
			if err := syscall.Lstat(physicalPath, &st); err != nil {
				if errors.Is(err, syscall.ENOENT) {
					continue
				}
				return fs.ToErrno(err)
			}
			if uint32(st.Mode)&syscall.S_IFMT == syscall.S_IFDIR {
				return syscall.EISDIR
			}
			errno := fs.ToErrno(syscall.Unlink(physicalPath))
			if errno != 0 {
				n.log.Error().Str("op", "unlink").Str("path", virtualPath).Str("storage_id", t.ID).Err(errno).Msg("failed to unlink")
			} else {
				n.log.Debug().Str("op", "unlink").Str("path", virtualPath).Str("storage_id", t.ID).Bool("indexed", false).Msg("unlink")
			}
			return errno
		}

		if n.db == nil {
			n.log.Error().Str("op", "unlink").Str("path", virtualPath).Str("storage_id", t.ID).Msg("failed to unlink: db is nil for indexed target")
			return syscall.EIO
		}
		f, ok, err := n.db.GetEffectiveFile(ctx, t.ID, virtualPath)
		if err != nil {
			return toErrno(fmt.Errorf("failed to lookup indexed file: %w", err))
		}
		if !ok {
			continue
		}
		if f.IsDir {
			return syscall.EISDIR
		}
		updated, err := n.db.MarkDeleted(ctx, t.ID, virtualPath, false)
		if err != nil {
			return fs.ToErrno(err)
		}
		if !updated {
			continue
		}
		if err := eventlog.Append(ctx, n.mountName, eventlog.DeleteEvent{Type: eventlog.TypeDelete, StorageID: t.ID, Path: virtualPath, IsDir: false, TS: time.Now().Unix()}); err != nil {
			n.log.Error().Str("op", "unlink").Str("path", virtualPath).Str("storage_id", t.ID).Err(err).Msg("failed to append eventlog")
			return syscall.EIO
		}
		n.log.Debug().Str("op", "unlink").Str("path", virtualPath).Str("storage_id", t.ID).Bool("indexed", true).Msg("unlink")
		return 0
	}

	return syscall.ENOENT
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

	targets, err := n.rt.ResolveReadTargets(virtualPath)
	if err != nil {
		return toErrno(err)
	}

	for _, t := range targets {
		if !t.Indexed {
			physicalPath := filepath.Join(t.Root, virtualPath)
			st := syscall.Stat_t{}
			if err := syscall.Lstat(physicalPath, &st); err != nil {
				if errors.Is(err, syscall.ENOENT) {
					continue
				}
				return fs.ToErrno(err)
			}
			if uint32(st.Mode)&syscall.S_IFMT != syscall.S_IFDIR {
				return syscall.ENOTDIR
			}
			errno := fs.ToErrno(syscall.Rmdir(physicalPath))
			if errno != 0 {
				n.log.Error().Str("op", "rmdir").Str("path", virtualPath).Str("storage_id", t.ID).Err(errno).Msg("failed to rmdir")
			} else {
				n.log.Debug().Str("op", "rmdir").Str("path", virtualPath).Str("storage_id", t.ID).Bool("indexed", false).Msg("rmdir")
			}
			return errno
		}

		if n.db == nil {
			n.log.Error().Str("op", "rmdir").Str("path", virtualPath).Str("storage_id", t.ID).Msg("failed to rmdir: db is nil for indexed target")
			return syscall.EIO
		}
		f, ok, err := n.db.GetEffectiveFile(ctx, t.ID, virtualPath)
		if err != nil {
			return toErrno(fmt.Errorf("failed to lookup indexed dir: %w", err))
		}
		if !ok {
			continue
		}
		if !f.IsDir {
			return syscall.ENOTDIR
		}
		updated, err := n.db.MarkDeleted(ctx, t.ID, virtualPath, true)
		if err != nil {
			return fs.ToErrno(err)
		}
		if !updated {
			continue
		}
		if err := eventlog.Append(ctx, n.mountName, eventlog.DeleteEvent{Type: eventlog.TypeDelete, StorageID: t.ID, Path: virtualPath, IsDir: true, TS: time.Now().Unix()}); err != nil {
			n.log.Error().Str("op", "rmdir").Str("path", virtualPath).Str("storage_id", t.ID).Err(err).Msg("failed to append eventlog")
			return syscall.EIO
		}
		n.log.Debug().Str("op", "rmdir").Str("path", virtualPath).Str("storage_id", t.ID).Bool("indexed", true).Msg("rmdir")
		return 0
	}

	return syscall.ENOENT
}
