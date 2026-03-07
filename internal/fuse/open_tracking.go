package fuse

import (
	"context"
	"syscall"

	"github.com/hieutdo/policyfs/internal/daemonctl"
)

// attachOpenTracking attaches open-tracking metadata to a FileHandle and increments open counts.
func attachOpenTracking(ctx context.Context, n *Node, virtualPath string, h *FileHandle, write bool) {
	if n == nil || h == nil {
		return
	}
	if n.open == nil {
		return
	}
	_, log := n.runtime()

	st := syscall.Stat_t{}
	if err := syscall.Fstat(h.fd, &st); err != nil {
		log.Error().Str("op", "open").Str("path", virtualPath).Str("storage_id", h.storageID).Err(err).Msg("failed to fstat open handle")
		return
	}
	attachOpenTrackingFromStat(n, virtualPath, h, write, &st)
	_ = ctx
}

// attachOpenTrackingFromStat is like attachOpenTracking but reuses an already-populated Stat_t.
func attachOpenTrackingFromStat(n *Node, virtualPath string, h *FileHandle, write bool, st *syscall.Stat_t) {
	if n == nil || h == nil || st == nil {
		return
	}
	if n.open == nil {
		return
	}
	if h.openTracked {
		return
	}

	id := daemonctl.OpenFileID{StorageID: h.storageID, Dev: uint64(st.Dev), Ino: st.Ino}
	n.open.Inc(id, write)

	h.openTracker = n.open
	h.openID = id
	h.openWrite = write
	h.openTracked = true

	_ = virtualPath
}
