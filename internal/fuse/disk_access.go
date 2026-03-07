package fuse

import (
	"context"
	"os"
	"path/filepath"
	"runtime"
	"strconv"
	"strings"
	"sync"
	"sync/atomic"
	"time"

	gofuse "github.com/hanwen/go-fuse/v2/fuse"
	"github.com/rs/zerolog"
)

const (
	// diskAccessDedupMaxEntries is a soft upper bound for the in-memory dedup map.
	// We prune expired entries when it grows beyond this threshold.
	diskAccessDedupMaxEntries = 2048
	// procNameCacheMaxEntries is a soft upper bound for cached /proc/<pid>/comm entries.
	// We prune expired entries (and then random entries) when it grows beyond this threshold.
	procNameCacheMaxEntries = 4096
)

// DiskAccessConfig controls the optional disk access logging mode for `pfs mount`.
//
// The intent is debugging: identify which process wakes up indexed storage by opening files.
// All durations are interpreted as seconds-based flags in the CLI.
type DiskAccessConfig struct {
	Enabled bool
	// DedupTTL is a best-effort deduplication window for identical events.
	// When <= 0, deduplication is disabled.
	DedupTTL time.Duration
	// SummaryInterval controls periodic summary emission.
	// When <= 0, summary emission is disabled.
	SummaryInterval time.Duration
}

// diskAccessLogger emits structured `disk_access` logs with anti-spam controls.
//
// It is safe for concurrent use.
type diskAccessLogger struct {
	log atomic.Value // zerolog.Logger
	cfg DiskAccessConfig

	mu      sync.Mutex
	last    map[diskAccessKey]time.Time
	counts  map[diskAccessSummaryKey]uint64
	lastSum time.Time
	total   uint64
	dropped uint64

	limiter rateLimiter
	procs   *procNameCache
}

// diskAccessKey dedupes disk access logs by (storage, path, op, pid).
type diskAccessKey struct {
	storageID string
	path      string
	op        string
	pid       uint32
}

// diskAccessSummaryKey aggregates disk access counts by (storage, pid, name).
type diskAccessSummaryKey struct {
	storageID string
	pid       uint32
	name      string
}

// newDiskAccessLogger constructs a disk access logger or returns nil when disabled.
func newDiskAccessLogger(log zerolog.Logger, cfg DiskAccessConfig) *diskAccessLogger {
	if !cfg.Enabled {
		return nil
	}
	l := &diskAccessLogger{
		cfg:     cfg,
		last:    map[diskAccessKey]time.Time{},
		counts:  map[diskAccessSummaryKey]uint64{},
		limiter: newRateLimiter(60, time.Minute),
		procs:   newProcNameCache(5 * time.Minute),
	}
	l.log.Store(log)

	return l
}

// SetLog replaces the logger used by the disk access logger.
func (l *diskAccessLogger) SetLog(log zerolog.Logger) {
	if l == nil {
		return
	}
	l.log.Store(log)
}

// RecordOpen records a disk access event for an open(2) on an indexed storage target.
func (l *diskAccessLogger) RecordOpen(ctx context.Context, storageID string, virtualPath string, indexed bool) {
	if l == nil {
		return
	}
	if !indexed {
		return
	}

	op := "open"
	pid, name := l.callerInfo(ctx)
	key := diskAccessKey{storageID: storageID, path: virtualPath, op: op, pid: pid}

	now := time.Now()

	l.mu.Lock()
	defer l.mu.Unlock()

	if l.cfg.SummaryInterval > 0 {
		l.total++
		sumKey := diskAccessSummaryKey{storageID: storageID, pid: pid, name: name}
		l.counts[sumKey]++
		if l.lastSum.IsZero() {
			l.lastSum = now
		}
	}

	shouldLog := l.limiter.Allow(now)
	if !shouldLog {
		l.dropped++
	}

	dedupHit := false
	if l.cfg.DedupTTL > 0 {
		if last, ok := l.last[key]; ok {
			if now.Sub(last) < l.cfg.DedupTTL {
				dedupHit = true
			}
		}
		if !dedupHit {
			l.last[key] = now
			// Best-effort pruning to avoid unbounded growth in long-running daemons.
			if len(l.last) > diskAccessDedupMaxEntries {
				cutoff := now.Add(-l.cfg.DedupTTL)
				for k, t := range l.last {
					if t.Before(cutoff) {
						delete(l.last, k)
					}
				}
			}
		}
	}

	if shouldLog && !dedupHit {
		log := l.log.Load().(zerolog.Logger)
		log.Info().
			Str("op", op).
			Str("storage_id", storageID).
			Str("path", virtualPath).
			Uint32("caller_pid", pid).
			Str("caller_name", name).
			Msg("disk_access")
	}

	if l.cfg.SummaryInterval > 0 {
		if now.Sub(l.lastSum) >= l.cfg.SummaryInterval {
			interval := l.cfg.SummaryInterval
			l.lastSum = now
			l.emitSummaryLocked(interval)
		}
	}
}

// emitSummaryLocked emits a single summary line and resets counters.
//
// Callers must hold l.mu.
func (l *diskAccessLogger) emitSummaryLocked(interval time.Duration) {
	total := l.total
	dropped := l.dropped
	counts := l.counts
	l.total = 0
	l.dropped = 0
	l.counts = map[diskAccessSummaryKey]uint64{}

	if total == 0 && dropped == 0 {
		return
	}

	var top diskAccessSummaryKey
	var topCount uint64
	unique := uint64(len(counts))
	for k, c := range counts {
		if c > topCount {
			top = k
			topCount = c
		}
	}

	log := l.log.Load().(zerolog.Logger)
	log.Info().
		Str("op", "open").
		Int64("interval_sec", int64(interval/time.Second)).
		Uint64("total", total).
		Uint64("unique", unique).
		Uint64("dropped", dropped).
		Str("storage_id", top.storageID).
		Uint32("caller_pid", top.pid).
		Str("caller_name", top.name).
		Uint64("count", topCount).
		Msg("disk_access_summary")
}

// callerInfo extracts PID and best-effort process name from a FUSE request context.
func (l *diskAccessLogger) callerInfo(ctx context.Context) (uint32, string) {
	caller, ok := gofuse.FromContext(ctx)
	if !ok {
		return 0, ""
	}

	pid := uint32(caller.Pid)
	if pid == 0 {
		return 0, ""
	}

	name := ""
	if l.procs != nil {
		name = l.procs.Get(pid)
	}
	return pid, name
}

// procNameCache caches /proc/<pid>/comm lookups.
type procNameCache struct {
	ttl time.Duration

	mu    sync.Mutex
	cache map[uint32]procNameEntry
}

// procNameEntry is a single cached process name lookup.
type procNameEntry struct {
	name      string
	expiresAt time.Time
}

// newProcNameCache creates a process name cache.
func newProcNameCache(ttl time.Duration) *procNameCache {
	return &procNameCache{ttl: ttl, cache: map[uint32]procNameEntry{}}
}

// Get returns the cached process name for pid or performs a best-effort lookup.
func (c *procNameCache) Get(pid uint32) string {
	if c == nil {
		return ""
	}
	if pid == 0 {
		return ""
	}
	if runtime.GOOS != "linux" {
		return ""
	}

	now := time.Now()

	c.mu.Lock()
	if e, ok := c.cache[pid]; ok {
		if now.Before(e.expiresAt) {
			name := e.name
			c.mu.Unlock()
			return name
		}
	}
	c.mu.Unlock()

	name := readProcComm(pid)

	c.mu.Lock()
	c.cache[pid] = procNameEntry{name: name, expiresAt: now.Add(c.ttl)}
	if len(c.cache) > procNameCacheMaxEntries {
		for k, e := range c.cache {
			if now.After(e.expiresAt) {
				delete(c.cache, k)
			}
		}
		for len(c.cache) > procNameCacheMaxEntries {
			for k := range c.cache {
				delete(c.cache, k)
				break
			}
		}
	}
	c.mu.Unlock()
	return name
}

// readProcComm reads /proc/<pid>/comm and returns a trimmed name.
func readProcComm(pid uint32) string {
	p := filepath.Join("/proc", strconv.FormatUint(uint64(pid), 10), "comm")
	b, err := os.ReadFile(p)
	if err != nil {
		return ""
	}
	return strings.TrimSpace(string(b))
}

// rateLimiter is a small token bucket limiter.
type rateLimiter struct {
	capacity float64
	rate     float64
	tokens   float64
	last     time.Time
}

// newRateLimiter creates a limiter with the given capacity over a window.
func newRateLimiter(capacity int, window time.Duration) rateLimiter {
	c := float64(capacity)
	r := c / window.Seconds()
	return rateLimiter{capacity: c, rate: r, tokens: c, last: time.Now()}
}

// Allow returns true if an event should be allowed now.
func (r *rateLimiter) Allow(now time.Time) bool {
	if r.last.IsZero() {
		r.last = now
		r.tokens = r.capacity
	}

	delta := now.Sub(r.last).Seconds()
	if delta > 0 {
		r.tokens += delta * r.rate
		if r.tokens > r.capacity {
			r.tokens = r.capacity
		}
		r.last = now
	}

	if r.tokens < 1 {
		return false
	}
	r.tokens -= 1
	return true
}
