package fuse

import (
	"context"
	"sync"

	"github.com/hieutdo/policyfs/internal/daemonctl"
)

// OpenTracker tracks open file handles held by FUSE clients.
//
// It is used by maintenance jobs (e.g. mover) to avoid moving files that are currently open.
// The key is a stable file identity (storage_id + dev + ino).
//
// This is best-effort and intentionally simple: counts are in-memory only.
type OpenTracker struct {
	mu sync.Mutex
	m  map[daemonctl.OpenFileID]openCounts
}

// openCounts holds the open counters for one file ID.
type openCounts struct {
	openCount      int64
	openWriteCount int64
}

// NewOpenTracker constructs a ready-to-use OpenTracker.
func NewOpenTracker() *OpenTracker {
	return &OpenTracker{m: make(map[daemonctl.OpenFileID]openCounts)}
}

// Inc increments the open counters for a file ID.
func (t *OpenTracker) Inc(id daemonctl.OpenFileID, write bool) {
	if t == nil {
		return
	}
	t.mu.Lock()
	defer t.mu.Unlock()

	c := t.m[id]
	c.openCount++
	if write {
		c.openWriteCount++
	}
	t.m[id] = c
}

// Dec decrements the open counters for a file ID.
func (t *OpenTracker) Dec(id daemonctl.OpenFileID, write bool) {
	if t == nil {
		return
	}
	t.mu.Lock()
	defer t.mu.Unlock()

	c, ok := t.m[id]
	if !ok {
		return
	}
	if c.openCount > 0 {
		c.openCount--
	}
	if write && c.openWriteCount > 0 {
		c.openWriteCount--
	}
	if c.openCount <= 0 {
		delete(t.m, id)
		return
	}
	t.m[id] = c
}

// OpenCounts returns open-count snapshots for the given file IDs.
func (t *OpenTracker) OpenCounts(ctx context.Context, files []daemonctl.OpenFileID) ([]daemonctl.OpenStat, error) {
	if t == nil {
		return nil, nil
	}

	t.mu.Lock()
	defer t.mu.Unlock()

	out := make([]daemonctl.OpenStat, 0, len(files))
	for _, id := range files {
		c := t.m[id]
		out = append(out, daemonctl.OpenStat{
			OpenFileID:     id,
			OpenCount:      c.openCount,
			OpenWriteCount: c.openWriteCount,
		})
	}
	_ = ctx
	return out, nil
}
