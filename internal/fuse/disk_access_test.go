package fuse

import (
	"bytes"
	"context"
	"encoding/json"
	"testing"
	"time"

	gofuse "github.com/hanwen/go-fuse/v2/fuse"
	"github.com/rs/zerolog"
	"github.com/stretchr/testify/require"
)

// --- rateLimiter tests ---

// TestRateLimiter_shouldAllowUpToCapacity verifies the limiter permits events up to its configured capacity.
func TestRateLimiter_shouldAllowUpToCapacity(t *testing.T) {
	r := newRateLimiter(3, time.Minute)
	now := time.Now()

	require.True(t, r.Allow(now))
	require.True(t, r.Allow(now))
	require.True(t, r.Allow(now))
	require.False(t, r.Allow(now), "should reject after exhausting capacity")
}

// TestRateLimiter_shouldRefillOverTime verifies tokens refill based on elapsed time.
func TestRateLimiter_shouldRefillOverTime(t *testing.T) {
	r := newRateLimiter(1, time.Second)
	now := time.Now()

	require.True(t, r.Allow(now))
	require.False(t, r.Allow(now), "should reject immediately after")

	// After 1 second, 1 token should be refilled.
	later := now.Add(time.Second)
	require.True(t, r.Allow(later), "should allow after refill interval")
}

// --- procNameCache tests ---

// TestProcNameCache_nilReceiver_shouldReturnEmpty verifies the cache is nil-safe.
func TestProcNameCache_nilReceiver_shouldReturnEmpty(t *testing.T) {
	var c *procNameCache
	require.Equal(t, "", c.Get(1234))
}

// TestProcNameCache_zeroPID_shouldReturnEmpty verifies pid=0 is treated as unknown.
func TestProcNameCache_zeroPID_shouldReturnEmpty(t *testing.T) {
	c := newProcNameCache(time.Minute)
	require.Equal(t, "", c.Get(0))
}

// --- diskAccessLogger tests ---

// testContext returns a context with the given PID as the FUSE caller.
func testContext(pid uint32) context.Context {
	caller := &gofuse.Caller{
		Owner: gofuse.Owner{Uid: 1000, Gid: 1000},
		Pid:   pid,
	}
	return gofuse.NewContext(context.Background(), caller)
}

// TestDiskAccessLogger_nilReceiver_shouldNotPanic verifies RecordOpen is safe to call when logging is disabled.
func TestDiskAccessLogger_nilReceiver_shouldNotPanic(t *testing.T) {
	var l *diskAccessLogger
	l.RecordOpen(context.Background(), "hdd1", "test.txt", true)
}

// TestDiskAccessLogger_disabled_shouldReturnNil verifies the feature is completely disabled when cfg.Enabled is false.
func TestDiskAccessLogger_disabled_shouldReturnNil(t *testing.T) {
	log := zerolog.New(nil)
	cfg := DiskAccessConfig{Enabled: false}
	l := newDiskAccessLogger(log, cfg)
	require.Nil(t, l)
}

// TestDiskAccessLogger_RecordOpen_shouldSkipNonIndexed verifies disk access logging only applies to indexed storage.
func TestDiskAccessLogger_RecordOpen_shouldSkipNonIndexed(t *testing.T) {
	var buf bytes.Buffer
	log := zerolog.New(&buf)
	cfg := DiskAccessConfig{Enabled: true}
	l := newDiskAccessLogger(log, cfg)

	l.RecordOpen(testContext(42), "hdd1", "test.txt", false)

	require.Empty(t, buf.String(), "should not log for non-indexed storage")
}

// TestDiskAccessLogger_RecordOpen_shouldLogIndexedAccess verifies a single indexed open emits one disk_access entry.
func TestDiskAccessLogger_RecordOpen_shouldLogIndexedAccess(t *testing.T) {
	var buf bytes.Buffer
	log := zerolog.New(&buf)
	cfg := DiskAccessConfig{Enabled: true}
	l := newDiskAccessLogger(log, cfg)

	l.RecordOpen(testContext(42), "hdd1", "library/movie.mkv", true)

	require.NotEmpty(t, buf.String())

	var entry map[string]interface{}
	require.NoError(t, json.Unmarshal(buf.Bytes(), &entry))
	require.Equal(t, "disk_access", entry[zerolog.MessageFieldName])
	require.Equal(t, "open", entry["op"])
	require.Equal(t, "hdd1", entry["storage_id"])
	require.Equal(t, "library/movie.mkv", entry["path"])
	require.Equal(t, float64(42), entry["caller_pid"])
}

// TestDiskAccessLogger_RecordOpen_dedupShouldSuppressDuplicateWithinTTL verifies identical opens are deduped.
func TestDiskAccessLogger_RecordOpen_dedupShouldSuppressDuplicateWithinTTL(t *testing.T) {
	var buf bytes.Buffer
	log := zerolog.New(&buf)
	cfg := DiskAccessConfig{Enabled: true, DedupTTL: time.Minute}
	l := newDiskAccessLogger(log, cfg)

	ctx := testContext(42)
	l.RecordOpen(ctx, "hdd1", "same.txt", true)
	first := buf.Len()
	require.Greater(t, first, 0)

	l.RecordOpen(ctx, "hdd1", "same.txt", true)
	require.Equal(t, first, buf.Len(), "second call within TTL should be suppressed")
}

// TestDiskAccessLogger_RecordOpen_dedupShouldAllowAfterTTLExpires verifies the same key logs again after DedupTTL.
func TestDiskAccessLogger_RecordOpen_dedupShouldAllowAfterTTLExpires(t *testing.T) {
	var buf bytes.Buffer
	log := zerolog.New(&buf)
	cfg := DiskAccessConfig{Enabled: true, DedupTTL: 10 * time.Millisecond}
	l := newDiskAccessLogger(log, cfg)

	ctx := testContext(42)
	l.RecordOpen(ctx, "hdd1", "same.txt", true)
	first := buf.Len()
	require.Greater(t, first, 0)

	// Wait for TTL to expire.
	time.Sleep(15 * time.Millisecond)

	l.RecordOpen(ctx, "hdd1", "same.txt", true)
	require.Greater(t, buf.Len(), first, "should log again after TTL expires")
}

// TestDiskAccessLogger_RecordOpen_dedupShouldAllowDifferentPaths verifies dedup is per-path, not per-caller only.
func TestDiskAccessLogger_RecordOpen_dedupShouldAllowDifferentPaths(t *testing.T) {
	var buf bytes.Buffer
	log := zerolog.New(&buf)
	cfg := DiskAccessConfig{Enabled: true, DedupTTL: time.Minute}
	l := newDiskAccessLogger(log, cfg)

	ctx := testContext(42)
	l.RecordOpen(ctx, "hdd1", "a.txt", true)
	first := buf.Len()
	require.Greater(t, first, 0)

	l.RecordOpen(ctx, "hdd1", "b.txt", true)
	require.Greater(t, buf.Len(), first, "different path should not be deduped")
}

// TestDiskAccessLogger_summary_shouldEmitAfterInterval verifies summary lines are emitted periodically when enabled.
func TestDiskAccessLogger_summary_shouldEmitAfterInterval(t *testing.T) {
	var buf bytes.Buffer
	log := zerolog.New(&buf)
	cfg := DiskAccessConfig{Enabled: true, SummaryInterval: 10 * time.Millisecond}
	l := newDiskAccessLogger(log, cfg)

	ctx := testContext(42)
	l.RecordOpen(ctx, "hdd1", "a.txt", true)

	// Wait for the summary interval to elapse.
	time.Sleep(15 * time.Millisecond)

	l.RecordOpen(ctx, "hdd1", "b.txt", true)

	// Parse all JSON lines from the log buffer.
	lines := bytes.Split(bytes.TrimSpace(buf.Bytes()), []byte("\n"))
	foundSummary := false
	for _, line := range lines {
		var entry map[string]interface{}
		if err := json.Unmarshal(line, &entry); err != nil {
			continue
		}
		if entry[zerolog.MessageFieldName] == "disk_access_summary" {
			foundSummary = true
			require.Equal(t, "open", entry["op"])
			require.NotZero(t, entry["total"])
			break
		}
	}
	require.True(t, foundSummary, "should have emitted a disk_access_summary line")
}

// TestDiskAccessLogger_summary_shouldCountDedupedEvents verifies summary totals include deduped events.
func TestDiskAccessLogger_summary_shouldCountDedupedEvents(t *testing.T) {
	var buf bytes.Buffer
	log := zerolog.New(&buf)
	cfg := DiskAccessConfig{Enabled: true, DedupTTL: time.Minute, SummaryInterval: 10 * time.Millisecond}
	l := newDiskAccessLogger(log, cfg)

	ctx := testContext(42)
	for i := 0; i < 5; i++ {
		l.RecordOpen(ctx, "hdd1", "same.txt", true)
	}

	time.Sleep(15 * time.Millisecond)
	// This call triggers summary emission; it is expected to be deduped, but still counted.
	l.RecordOpen(ctx, "hdd1", "same.txt", true)

	lines := bytes.Split(bytes.TrimSpace(buf.Bytes()), []byte("\n"))
	var gotTotal uint64
	found := false
	for _, line := range lines {
		var entry map[string]interface{}
		if err := json.Unmarshal(line, &entry); err != nil {
			continue
		}
		if entry[zerolog.MessageFieldName] != "disk_access_summary" {
			continue
		}
		v, ok := entry["total"].(float64)
		require.True(t, ok)
		gotTotal = uint64(v)
		found = true
		break
	}
	require.True(t, found, "should have emitted a disk_access_summary line")
	require.Equal(t, uint64(6), gotTotal, "summary total should include deduped events")
}

// TestDiskAccessLogger_summary_shouldIncludeDroppedFromRateLimiter verifies summary includes dropped counts.
func TestDiskAccessLogger_summary_shouldIncludeDroppedFromRateLimiter(t *testing.T) {
	var buf bytes.Buffer
	log := zerolog.New(&buf)
	cfg := DiskAccessConfig{Enabled: true, SummaryInterval: 10 * time.Millisecond}
	l := newDiskAccessLogger(log, cfg)
	// Force drops quickly.
	l.limiter = newRateLimiter(1, time.Minute)

	ctx := testContext(42)
	l.RecordOpen(ctx, "hdd1", "a.txt", true)
	l.RecordOpen(ctx, "hdd1", "b.txt", true)
	l.RecordOpen(ctx, "hdd1", "c.txt", true)

	time.Sleep(15 * time.Millisecond)
	// Trigger summary emission; still expected to be dropped (no refill yet).
	l.RecordOpen(ctx, "hdd1", "d.txt", true)

	lines := bytes.Split(bytes.TrimSpace(buf.Bytes()), []byte("\n"))
	var gotTotal uint64
	var gotDropped uint64
	found := false
	for _, line := range lines {
		var entry map[string]interface{}
		if err := json.Unmarshal(line, &entry); err != nil {
			continue
		}
		if entry[zerolog.MessageFieldName] != "disk_access_summary" {
			continue
		}
		totalV, ok := entry["total"].(float64)
		require.True(t, ok)
		droppedV, ok := entry["dropped"].(float64)
		require.True(t, ok)
		gotTotal = uint64(totalV)
		gotDropped = uint64(droppedV)
		found = true
		break
	}
	require.True(t, found, "should have emitted a disk_access_summary line")
	require.Equal(t, uint64(4), gotTotal, "summary total should include dropped events")
	require.Equal(t, uint64(3), gotDropped, "summary dropped should include rate-limited events")
}

// TestDiskAccessLogger_rateLimiter_shouldDropWhenExhausted verifies per-event logs stop when rate limit is exceeded.
func TestDiskAccessLogger_rateLimiter_shouldDropWhenExhausted(t *testing.T) {
	var buf bytes.Buffer
	log := zerolog.New(&buf)
	cfg := DiskAccessConfig{Enabled: true}
	l := newDiskAccessLogger(log, cfg)
	// Exhaust the rate limiter (60 tokens).
	l.limiter = newRateLimiter(2, time.Minute)

	ctx := testContext(42)
	l.RecordOpen(ctx, "hdd1", "a.txt", true)
	l.RecordOpen(ctx, "hdd1", "b.txt", true)
	afterTwo := buf.Len()

	l.RecordOpen(ctx, "hdd1", "c.txt", true)
	require.Equal(t, afterTwo, buf.Len(), "third event should be dropped by rate limiter")

	// Verify the dropped counter was incremented.
	l.mu.Lock()
	dropped := l.dropped
	l.mu.Unlock()
	require.Equal(t, uint64(1), dropped)
}
