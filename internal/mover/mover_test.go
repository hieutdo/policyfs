package mover

import (
	"bytes"
	"context"
	"errors"
	"fmt"
	"os"
	"path/filepath"
	"syscall"
	"testing"
	"time"

	"github.com/hieutdo/policyfs/internal/config"
	"github.com/hieutdo/policyfs/internal/pathmatch"
	"github.com/stretchr/testify/require"
)

// durPtr returns a pointer to v.
func durPtr(v time.Duration) *time.Duration {
	return &v
}

// TestParseConditions_shouldParseMinAgeMinSizeMaxSize verifies parseConditions accepts valid humanfmt fields.
func TestParseConditions_shouldParseMinAgeMinSizeMaxSize(t *testing.T) {
	c, err := parseConditions(config.MoverConditionsConfig{MinAge: "7d", MinSize: "100MB", MaxSize: "1GiB"})
	require.NoError(t, err)
	require.NotNil(t, c.MinAge)
	require.NotNil(t, c.MinSize)
	require.NotNil(t, c.MaxSize)
	require.Equal(t, 7*24*time.Hour, *c.MinAge)
	require.Equal(t, int64(100000000), *c.MinSize)
	require.Equal(t, int64(1024*1024*1024), *c.MaxSize)
}

// TestParseConditions_invalidMinAge_shouldReturnError verifies invalid durations are rejected.
func TestParseConditions_invalidMinAge_shouldReturnError(t *testing.T) {
	_, err := parseConditions(config.MoverConditionsConfig{MinAge: "nope"})
	require.Error(t, err)
}

// TestPlanner_expandRefs_shouldExpandGroupsAndDedup verifies group expansion preserves order and de-duplicates ids.
func TestPlanner_expandRefs_shouldExpandGroupsAndDedup(t *testing.T) {
	mc := &config.MountConfig{
		StoragePaths: []config.StoragePath{{ID: "ssd1"}, {ID: "ssd2"}},
		StorageGroups: map[string][]string{
			"ssds": {"ssd1", "ssd2"},
		},
	}
	p := newPlanner("media", mc, Opts{})

	ids, err := p.expandRefs([]string{"ssd1"}, []string{"ssds"})
	require.NoError(t, err)
	require.Equal(t, []string{"ssd1", "ssd2"}, ids)
}

// TestPlanner_expandRefs_unknownGroupMember_shouldReturnInvalid verifies referencing an unknown storage id in a group is invalid.
func TestPlanner_expandRefs_unknownGroupMember_shouldReturnInvalid(t *testing.T) {
	mc := &config.MountConfig{
		StoragePaths: []config.StoragePath{{ID: "ssd1"}},
		StorageGroups: map[string][]string{
			"ssds": {"ssd1", "nope"},
		},
	}
	p := newPlanner("media", mc, Opts{})

	_, err := p.expandRefs(nil, []string{"ssds"})
	require.Error(t, err)
}

// TestPlanner_activeSourcesForJob_manual_shouldReturnAllInOrder verifies manual jobs process all sources in order.
func TestPlanner_activeSourcesForJob_manual_shouldReturnAllInOrder(t *testing.T) {
	mc := &config.MountConfig{StoragePaths: []config.StoragePath{{ID: "ssd1"}, {ID: "ssd2"}}}
	p := newPlanner("media", mc, Opts{})

	ids, err := p.activeSourcesForJob("manual", config.MoverJobConfig{}, []string{"ssd1", "ssd2"})
	require.NoError(t, err)
	require.Equal(t, []string{"ssd1", "ssd2"}, ids)
}

// TestPlanner_activeSourcesForJob_usage_shouldFilterAndSort verifies usage trigger filters below threshold_start and sorts by usage descending.
func TestPlanner_activeSourcesForJob_usage_shouldFilterAndSort(t *testing.T) {
	mc := &config.MountConfig{StoragePaths: []config.StoragePath{{ID: "ssd1", Path: "/mnt/ssd1"}, {ID: "ssd2", Path: "/mnt/ssd2"}, {ID: "ssd3", Path: "/mnt/ssd3"}}}
	p := newPlanner("media", mc, Opts{})
	p.usagePct = func(path string) (float64, error) {
		switch filepath.Base(path) {
		case "ssd1":
			return 90, nil
		case "ssd2":
			return 85, nil
		case "ssd3":
			return 50, nil
		default:
			return 0, nil
		}
	}

	j := config.MoverJobConfig{Trigger: config.MoverTriggerConfig{ThresholdStart: 80, ThresholdStop: 70}}
	ids, err := p.activeSourcesForJob("usage", j, []string{"ssd1", "ssd2", "ssd3"})
	require.NoError(t, err)
	require.Equal(t, []string{"ssd1", "ssd2"}, ids)
}

// TestPlanner_selectDestinations_pathPreserving_shouldPreferExistingParent verifies path_preserving narrows destinations to those with an existing parent dir.
func TestPlanner_selectDestinations_pathPreserving_shouldPreferExistingParent(t *testing.T) {
	hdd1 := t.TempDir()
	hdd2 := t.TempDir()
	rel := "library/tv/Show/S01E01.mkv"
	require.NoError(t, os.MkdirAll(filepath.Join(hdd1, filepath.FromSlash("library/tv/Show")), 0o755))

	mc := &config.MountConfig{StoragePaths: []config.StoragePath{{ID: "hdd1", Path: hdd1}, {ID: "hdd2", Path: hdd2}}}
	p := newPlanner("media", mc, Opts{})
	p.freeSpaceGB = func(_ string) (float64, error) { return 100, nil }

	j := config.MoverJobConfig{Destination: config.MoverDestinationConfig{PathPreserving: true, Policy: "first_found"}}
	dr, err := p.selectDestinations(j, []string{"hdd1", "hdd2"}, candidate{RelPath: rel})
	require.NoError(t, err)
	require.Len(t, dr.choices, 1)
	require.Equal(t, "hdd1", dr.choices[0].id)
	require.Equal(t, []string{"hdd1"}, dr.pathPreservingKept)
}

// TestPlanner_selectDestinations_policyMostFree_shouldSort verifies most_free sorts by free space descending.
func TestPlanner_selectDestinations_policyMostFree_shouldSort(t *testing.T) {
	mc := &config.MountConfig{StoragePaths: []config.StoragePath{{ID: "hdd1", Path: "/mnt/hdd1"}, {ID: "hdd2", Path: "/mnt/hdd2"}}}
	p := newPlanner("media", mc, Opts{})
	p.freeSpaceGB = func(path string) (float64, error) {
		switch filepath.Base(path) {
		case "hdd1":
			return 10, nil
		case "hdd2":
			return 20, nil
		default:
			return 0, nil
		}
	}

	j := config.MoverJobConfig{Destination: config.MoverDestinationConfig{Policy: "most_free"}}
	dr, err := p.selectDestinations(j, []string{"hdd1", "hdd2"}, candidate{RelPath: "a.txt"})
	require.NoError(t, err)
	require.Equal(t, "hdd2", dr.choices[0].id)
}

// TestDiscoverCandidatesOneSource_shouldFilterByMinAgeAndSort verifies min_age filtering uses planner.now and candidates are sorted by size desc then mtime asc.
func TestDiscoverCandidatesOneSource_shouldFilterByMinAgeAndSort(t *testing.T) {
	root := t.TempDir()
	require.NoError(t, os.MkdirAll(filepath.Join(root, "library"), 0o755))

	pSmall := filepath.Join(root, "library", "small.txt")
	require.NoError(t, os.WriteFile(pSmall, []byte("a"), 0o644))
	pBigOld := filepath.Join(root, "library", "big-old.txt")
	require.NoError(t, os.WriteFile(pBigOld, []byte("hello"), 0o644))
	pBigNew := filepath.Join(root, "library", "big-new.txt")
	require.NoError(t, os.WriteFile(pBigNew, []byte("world"), 0o644))

	now := time.Date(2026, 1, 1, 0, 0, 0, 0, time.UTC)
	require.NoError(t, os.Chtimes(pSmall, now.Add(-2*time.Hour), now.Add(-2*time.Hour)))
	require.NoError(t, os.Chtimes(pBigOld, now.Add(-48*time.Hour), now.Add(-48*time.Hour)))
	require.NoError(t, os.Chtimes(pBigNew, now.Add(-2*time.Hour), now.Add(-2*time.Hour)))

	mc := &config.MountConfig{StoragePaths: []config.StoragePath{{ID: "ssd1", Path: root}}}
	pl := newPlanner("media", mc, Opts{})
	pl.now = func() time.Time { return now }

	m, err := pathmatch.NewMatcher([]string{"library/**"})
	require.NoError(t, err)

	conds := conditions{MinAge: durPtr(24 * time.Hour)}
	cands, err := pl.discoverCandidatesOneSource(context.Background(), "job", "ssd1", m, nil, conds, nil)
	require.NoError(t, err)

	require.Len(t, cands, 1)
	require.Equal(t, "library/big-old.txt", cands[0].RelPath)
}

// TestCopyWithContext_canceled_shouldReturnContextError verifies cancellation is wrapped but still matches errors.Is(ctx.Err()).
func TestCopyWithContext_canceled_shouldReturnContextError(t *testing.T) {
	ctx, cancel := context.WithCancel(context.Background())
	cancel()

	var dst bytes.Buffer
	err := copyWithContext(ctx, &dst, bytes.NewReader([]byte("hello")), nil)
	require.Error(t, err)
	require.True(t, errors.Is(err, context.Canceled))
}

// TestCopyFileWithVerify_destinationExists_shouldReturnSkip verifies existing destination yields a skipError.
func TestCopyFileWithVerify_destinationExists_shouldReturnSkip(t *testing.T) {
	dir := t.TempDir()
	src := filepath.Join(dir, "src.txt")
	dst := filepath.Join(dir, "dst.txt")
	require.NoError(t, os.WriteFile(src, []byte("hello"), 0o644))
	require.NoError(t, os.WriteFile(dst, []byte("exists"), 0o644))

	st, err := os.Stat(src)
	require.NoError(t, err)

	c := candidate{Mode: uint32(st.Mode()), MTimeSec: st.ModTime().Unix()}
	err = copyFileWithVerify(context.Background(), src, dst, c, false, nil)
	require.Error(t, err)
	var skip *skipError
	require.True(t, errors.As(err, &skip))
}

// TestCopyFileWithVerify_verifyMismatch_shouldReturnSkipAndNotCreateDest verifies verify failures
// return a skipError (per spec: skip file) and do not leave a destination file behind.
func TestCopyFileWithVerify_verifyMismatch_shouldReturnSkipAndNotCreateDest(t *testing.T) {
	dir := t.TempDir()
	src := filepath.Join(dir, "src.txt")
	dst := filepath.Join(dir, "dst.txt")
	require.NoError(t, os.WriteFile(src, []byte("hello"), 0o644))

	old := hashXX64Func
	// hashXX64Func is only called for the dest temp file (source hash is streamed during copy).
	hashXX64Func = func(_ context.Context, _ string, _ copyProgressFunc) (uint64, error) {
		return 999, nil // deliberately wrong hash to trigger mismatch
	}
	t.Cleanup(func() { hashXX64Func = old })

	st, err := os.Stat(src)
	require.NoError(t, err)

	c := candidate{Mode: uint32(st.Mode()), MTimeSec: st.ModTime().Unix()}
	err = copyFileWithVerify(context.Background(), src, dst, c, true, nil)
	require.Error(t, err)

	var skip *skipError
	require.True(t, errors.As(err, &skip), "verify failure should be a skipError, got: %v", err)
	require.NoFileExists(t, dst)
}

// TestCopyFileWithVerify_success_shouldCopyContent verifies a basic copy succeeds and preserves content.
func TestCopyFileWithVerify_success_shouldCopyContent(t *testing.T) {
	dir := t.TempDir()
	src := filepath.Join(dir, "src.txt")
	dst := filepath.Join(dir, "dst.txt")
	content := []byte("hello")
	require.NoError(t, os.WriteFile(src, content, 0o644))

	st, err := os.Stat(src)
	require.NoError(t, err)

	c := candidate{Mode: uint32(st.Mode()), MTimeSec: st.ModTime().Unix()}
	err = copyFileWithVerify(context.Background(), src, dst, c, false, nil)
	require.NoError(t, err)

	got, err := os.ReadFile(dst)
	require.NoError(t, err)
	require.Equal(t, content, got)
}

// --- inAllowedWindow tests ---

// TestInAllowedWindow_sameDay_insideWindow verifies a same-day window (09:00-17:00) reports inside at 12:00.
func TestInAllowedWindow_sameDay_insideWindow(t *testing.T) {
	now := time.Date(2026, 2, 15, 12, 0, 0, 0, time.UTC)
	inside, winEnd, err := inAllowedWindow(now, "09:00", "17:00")
	require.NoError(t, err)
	require.True(t, inside, "12:00 should be inside 09:00-17:00")
	require.Equal(t, 17, winEnd.Hour())
}

// TestInAllowedWindow_sameDay_outsideWindow verifies a same-day window reports outside at 20:00.
func TestInAllowedWindow_sameDay_outsideWindow(t *testing.T) {
	now := time.Date(2026, 2, 15, 20, 0, 0, 0, time.UTC)
	inside, _, err := inAllowedWindow(now, "09:00", "17:00")
	require.NoError(t, err)
	require.False(t, inside, "20:00 should be outside 09:00-17:00")
}

// TestInAllowedWindow_crossMidnight_insideAfterStart verifies a cross-midnight window (23:00-06:00) reports inside at 01:00.
func TestInAllowedWindow_crossMidnight_insideAfterStart(t *testing.T) {
	now := time.Date(2026, 2, 16, 1, 0, 0, 0, time.UTC)
	inside, winEnd, err := inAllowedWindow(now, "23:00", "06:00")
	require.NoError(t, err)
	require.True(t, inside, "01:00 should be inside cross-midnight 23:00-06:00")
	require.Equal(t, 6, winEnd.Hour())
}

// TestInAllowedWindow_crossMidnight_insideBeforeMidnight verifies 23:30 is inside 23:00-06:00.
func TestInAllowedWindow_crossMidnight_insideBeforeMidnight(t *testing.T) {
	now := time.Date(2026, 2, 15, 23, 30, 0, 0, time.UTC)
	inside, _, err := inAllowedWindow(now, "23:00", "06:00")
	require.NoError(t, err)
	require.True(t, inside, "23:30 should be inside cross-midnight 23:00-06:00")
}

// TestInAllowedWindow_crossMidnight_outside verifies 12:00 is outside 23:00-06:00.
func TestInAllowedWindow_crossMidnight_outside(t *testing.T) {
	now := time.Date(2026, 2, 15, 12, 0, 0, 0, time.UTC)
	inside, _, err := inAllowedWindow(now, "23:00", "06:00")
	require.NoError(t, err)
	require.False(t, inside, "12:00 should be outside cross-midnight 23:00-06:00")
}

// TestInAllowedWindow_exactStart_shouldBeInside verifies the start boundary is inclusive.
func TestInAllowedWindow_exactStart_shouldBeInside(t *testing.T) {
	now := time.Date(2026, 2, 15, 9, 0, 0, 0, time.UTC)
	inside, _, err := inAllowedWindow(now, "09:00", "17:00")
	require.NoError(t, err)
	require.True(t, inside, "exact start should be inside")
}

// TestInAllowedWindow_exactEnd_shouldBeOutside verifies the end boundary is exclusive.
func TestInAllowedWindow_exactEnd_shouldBeOutside(t *testing.T) {
	now := time.Date(2026, 2, 15, 17, 0, 0, 0, time.UTC)
	inside, _, err := inAllowedWindow(now, "09:00", "17:00")
	require.NoError(t, err)
	require.False(t, inside, "exact end should be outside (exclusive)")
}

// --- discover min_size / max_size tests ---

// int64Ptr returns a pointer to v.
func int64Ptr(v int64) *int64 {
	return &v
}

// TestDiscoverCandidatesOneSource_shouldFilterByMinSizeAndMaxSize verifies min_size and max_size filtering.
func TestDiscoverCandidatesOneSource_shouldFilterByMinSizeAndMaxSize(t *testing.T) {
	root := t.TempDir()
	require.NoError(t, os.MkdirAll(filepath.Join(root, "lib"), 0o755))

	// 2 bytes — too small
	require.NoError(t, os.WriteFile(filepath.Join(root, "lib", "tiny.txt"), []byte("ab"), 0o644))
	// 10 bytes — in range
	require.NoError(t, os.WriteFile(filepath.Join(root, "lib", "mid.txt"), make([]byte, 10), 0o644))
	// 100 bytes — too big
	require.NoError(t, os.WriteFile(filepath.Join(root, "lib", "huge.txt"), make([]byte, 100), 0o644))

	mc := &config.MountConfig{StoragePaths: []config.StoragePath{{ID: "s1", Path: root}}}
	pl := newPlanner("media", mc, Opts{})
	pl.now = func() time.Time { return time.Now() }

	m, err := pathmatch.NewMatcher([]string{"lib/**"})
	require.NoError(t, err)

	conds := conditions{MinSize: int64Ptr(5), MaxSize: int64Ptr(50)}
	cands, err := pl.discoverCandidatesOneSource(context.Background(), "job", "s1", m, nil, conds, nil)
	require.NoError(t, err)

	require.Len(t, cands, 1, "only mid.txt (10 bytes) should match min_size=5, max_size=50")
	require.Equal(t, "lib/mid.txt", cands[0].RelPath)
}

// --- copyFileWithVerifyRetry tests ---

// TestCopyFileWithVerifyRetry_shouldRetryOnTransientError verifies transient copy errors are retried up to N times.
func TestCopyFileWithVerifyRetry_shouldRetryOnTransientError(t *testing.T) {
	dir := t.TempDir()
	src := filepath.Join(dir, "src.txt")
	dst := filepath.Join(dir, "dst.txt")
	require.NoError(t, os.WriteFile(src, []byte("hello"), 0o644))

	st, err := os.Stat(src)
	require.NoError(t, err)
	c := candidate{Mode: uint32(st.Mode()), MTimeSec: st.ModTime().Unix()}

	callCount := 0
	oldHash := hashXX64Func
	// hashXX64Func is called once per attempt for the dest hash (source hash is streamed).
	// First 2 attempts fail; 3rd succeeds.
	hashXX64Func = func(ctx context.Context, p string, _ copyProgressFunc) (uint64, error) {
		callCount++
		if callCount <= 2 {
			return 0, errors.New("transient IO error")
		}
		return oldHash(ctx, p, nil)
	}
	t.Cleanup(func() { hashXX64Func = oldHash })

	err = copyFileWithVerifyRetry(context.Background(), src, dst, c, true, 3, nil)
	require.NoError(t, err, "should succeed on 3rd attempt")
	require.FileExists(t, dst)
}

// TestCopyFileWithVerifyRetry_shouldNotRetryOnENOSPC verifies disk-full errors are not retried.
func TestCopyFileWithVerifyRetry_shouldNotRetryOnENOSPC(t *testing.T) {
	dir := t.TempDir()
	src := filepath.Join(dir, "src.txt")
	require.NoError(t, os.WriteFile(src, []byte("hello"), 0o644))

	// Make dst parent dir read-only so MkdirAll doesn't fail, but create a scenario
	// that would trigger ENOSPC. We'll simulate this with a hash seam instead.
	dstDir := filepath.Join(dir, "dst-dir")
	require.NoError(t, os.MkdirAll(dstDir, 0o755))
	dst := filepath.Join(dstDir, "dst.txt")

	st, err := os.Stat(src)
	require.NoError(t, err)
	c := candidate{Mode: uint32(st.Mode()), MTimeSec: st.ModTime().Unix()}

	callCount := 0
	oldHash := hashXX64Func
	// Inject ENOSPC on the dest hash call (simulating disk full during verify).
	hashXX64Func = func(_ context.Context, _ string, _ copyProgressFunc) (uint64, error) {
		callCount++
		return 0, syscall.ENOSPC
	}
	t.Cleanup(func() { hashXX64Func = oldHash })

	err = copyFileWithVerifyRetry(context.Background(), src, dst, c, true, 3, nil)
	require.Error(t, err)
	require.True(t, errors.Is(err, syscall.ENOSPC))
	// Hash func called only once for dest hash — ENOSPC should not retry.
	require.Equal(t, 1, callCount, "ENOSPC should not be retried")
}

// TestCopyFileWithVerifyRetry_verifyMismatch_shouldNotRetry verifies that verify failure (skipError) is not retried.
func TestCopyFileWithVerifyRetry_verifyMismatch_shouldNotRetry(t *testing.T) {
	dir := t.TempDir()
	src := filepath.Join(dir, "src.txt")
	dst := filepath.Join(dir, "dst.txt")
	require.NoError(t, os.WriteFile(src, []byte("hello"), 0o644))

	st, err := os.Stat(src)
	require.NoError(t, err)
	c := candidate{Mode: uint32(st.Mode()), MTimeSec: st.ModTime().Unix()}

	callCount := 0
	oldHash := hashXX64Func
	// hashXX64Func is only called for dest hash (source hash streamed during copy).
	hashXX64Func = func(_ context.Context, _ string, _ copyProgressFunc) (uint64, error) {
		callCount++
		return 999, nil // deliberately wrong to trigger mismatch
	}
	t.Cleanup(func() { hashXX64Func = oldHash })

	err = copyFileWithVerifyRetry(context.Background(), src, dst, c, true, 3, nil)
	require.Error(t, err)
	var skip *skipError
	require.True(t, errors.As(err, &skip), "verify mismatch should be skipError")
	// 1 hash call (dest only), only 1 attempt since skipError is not retried.
	require.Equal(t, 1, callCount, "verify mismatch should not be retried")
}

// --- Error handling tests (spec table) ---

// TestCopyFileWithVerify_sourceDisappeared_shouldReturnNotExist verifies vanished source returns os.ErrNotExist.
func TestCopyFileWithVerify_sourceDisappeared_shouldReturnNotExist(t *testing.T) {
	dir := t.TempDir()
	src := filepath.Join(dir, "gone.txt")
	dst := filepath.Join(dir, "dst.txt")

	c := candidate{Mode: 0o644, MTimeSec: time.Now().Unix()}
	err := copyFileWithVerify(context.Background(), src, dst, c, false, nil)
	require.Error(t, err)
	require.True(t, errors.Is(err, os.ErrNotExist), "source disappeared should wrap os.ErrNotExist, got: %v", err)
}

// TestCopyFileWithVerify_destDirPermissionDenied_shouldReturnError verifies permission denied on dest parent.
func TestCopyFileWithVerify_destDirPermissionDenied_shouldReturnError(t *testing.T) {
	if os.Getuid() == 0 {
		t.Skip("skip permission test when running as root")
	}

	dir := t.TempDir()
	src := filepath.Join(dir, "src.txt")
	require.NoError(t, os.WriteFile(src, []byte("hello"), 0o644))

	// Create a read-only parent for the destination.
	restrictedDir := filepath.Join(dir, "restricted")
	require.NoError(t, os.MkdirAll(restrictedDir, 0o555))
	t.Cleanup(func() { _ = os.Chmod(restrictedDir, 0o755) })

	dst := filepath.Join(restrictedDir, "subdir", "dst.txt")

	st, err := os.Stat(src)
	require.NoError(t, err)
	c := candidate{Mode: uint32(st.Mode()), MTimeSec: st.ModTime().Unix()}

	err = copyFileWithVerify(context.Background(), src, dst, c, false, nil)
	require.Error(t, err)
	require.True(t, errors.Is(err, syscall.EACCES), "permission denied on dest should wrap EACCES, got: %v", err)
}

// TestSelectDestinations_allOffline_shouldReturnError verifies that when all destinations fail statfs, no destinations are available.
func TestSelectDestinations_allOffline_shouldReturnError(t *testing.T) {
	mc := &config.MountConfig{StoragePaths: []config.StoragePath{{ID: "hdd1", Path: "/mnt/hdd1"}, {ID: "hdd2", Path: "/mnt/hdd2"}}}
	p := newPlanner("media", mc, Opts{})
	p.freeSpaceGB = func(_ string) (float64, error) {
		return 0, errors.New("statfs failed: device offline")
	}

	j := config.MoverJobConfig{Destination: config.MoverDestinationConfig{Policy: "first_found"}}
	_, err := p.selectDestinations(j, []string{"hdd1", "hdd2"}, candidate{RelPath: "a.txt"})
	require.Error(t, err)
	require.Contains(t, err.Error(), "no destination available")
}

// TestSelectDestinations_destFull_shouldFilterByMinFreeGB verifies destinations below min_free_gb are excluded.
func TestSelectDestinations_destFull_shouldFilterByMinFreeGB(t *testing.T) {
	mc := &config.MountConfig{StoragePaths: []config.StoragePath{
		{ID: "hdd1", Path: "/mnt/hdd1", MinFreeGB: 10},
		{ID: "hdd2", Path: "/mnt/hdd2", MinFreeGB: 10},
	}}
	p := newPlanner("media", mc, Opts{})
	p.freeSpaceGB = func(path string) (float64, error) {
		switch filepath.Base(path) {
		case "hdd1":
			return 5, nil // below min_free_gb=10
		case "hdd2":
			return 20, nil
		default:
			return 0, nil
		}
	}

	j := config.MoverJobConfig{Destination: config.MoverDestinationConfig{Policy: "first_found"}}
	dr, err := p.selectDestinations(j, []string{"hdd1", "hdd2"}, candidate{RelPath: "a.txt"})
	require.NoError(t, err)
	require.Len(t, dr.choices, 1)
	require.Equal(t, "hdd2", dr.choices[0].id, "hdd1 should be excluded (below min_free_gb)")
}

// TestSelectDestinations_allFull_shouldReturnError verifies all destinations below min_free_gb returns error.
func TestSelectDestinations_allFull_shouldReturnError(t *testing.T) {
	mc := &config.MountConfig{StoragePaths: []config.StoragePath{
		{ID: "hdd1", Path: "/mnt/hdd1", MinFreeGB: 10},
		{ID: "hdd2", Path: "/mnt/hdd2", MinFreeGB: 10},
	}}
	p := newPlanner("media", mc, Opts{})
	p.freeSpaceGB = func(_ string) (float64, error) { return 1, nil }

	j := config.MoverJobConfig{Destination: config.MoverDestinationConfig{Policy: "first_found"}}
	_, err := p.selectDestinations(j, []string{"hdd1", "hdd2"}, candidate{RelPath: "a.txt"})
	require.Error(t, err)
	require.Contains(t, err.Error(), "no destination available")
}

// TestCopyFileWithVerifyRetry_copyFailed_shouldRetryAndFail verifies copy failures are retried 3x then return error.
func TestCopyFileWithVerifyRetry_copyFailed_shouldRetryAndFail(t *testing.T) {
	dir := t.TempDir()
	src := filepath.Join(dir, "src.txt")
	dst := filepath.Join(dir, "dst.txt")
	require.NoError(t, os.WriteFile(src, []byte("hello"), 0o644))

	st, err := os.Stat(src)
	require.NoError(t, err)
	c := candidate{Mode: uint32(st.Mode()), MTimeSec: st.ModTime().Unix()}

	callCount := 0
	oldHash := hashXX64Func
	// Every attempt fails with a transient error on dest hash.
	hashXX64Func = func(_ context.Context, _ string, _ copyProgressFunc) (uint64, error) {
		callCount++
		return 0, errors.New("transient IO")
	}
	t.Cleanup(func() { hashXX64Func = oldHash })

	err = copyFileWithVerifyRetry(context.Background(), src, dst, c, true, 3, nil)
	require.Error(t, err)
	// Each attempt calls hashXX64Func once for dest hash before failing.
	require.Equal(t, 3, callCount, "should retry exactly 3 times")
}

// --- Missing coverage tests ---

// boolPtr returns a pointer to v.
func boolPtr(v bool) *bool {
	return &v
}

// TestSelectDestinations_policyLeastFree_shouldSort verifies least_free sorts by free space ascending.
func TestSelectDestinations_policyLeastFree_shouldSort(t *testing.T) {
	mc := &config.MountConfig{StoragePaths: []config.StoragePath{{ID: "hdd1", Path: "/mnt/hdd1"}, {ID: "hdd2", Path: "/mnt/hdd2"}}}
	p := newPlanner("media", mc, Opts{})
	p.freeSpaceGB = func(path string) (float64, error) {
		switch filepath.Base(path) {
		case "hdd1":
			return 20, nil
		case "hdd2":
			return 10, nil
		default:
			return 0, nil
		}
	}

	j := config.MoverJobConfig{Destination: config.MoverDestinationConfig{Policy: "least_free"}}
	dr, err := p.selectDestinations(j, []string{"hdd1", "hdd2"}, candidate{RelPath: "a.txt"})
	require.NoError(t, err)
	require.Len(t, dr.choices, 2)
	require.Equal(t, "hdd2", dr.choices[0].id, "least_free should prefer hdd2 (10 GB) over hdd1 (20 GB)")
	require.Equal(t, "hdd1", dr.choices[1].id)
}

// TestCount_shouldReturnCandidateCount verifies Count counts discovered candidates.
func TestCount_shouldReturnCandidateCount(t *testing.T) {
	root := t.TempDir()
	require.NoError(t, os.MkdirAll(filepath.Join(root, "media"), 0o755))
	require.NoError(t, os.WriteFile(filepath.Join(root, "media", "a.txt"), []byte("a"), 0o644))
	require.NoError(t, os.WriteFile(filepath.Join(root, "media", "b.txt"), []byte("b"), 0o644))

	mc := &config.MountConfig{
		StoragePaths: []config.StoragePath{{ID: "ssd1", Path: root}},
		Mover: config.MoverConfig{
			Enabled: boolPtr(true),
			Jobs: []config.MoverJobConfig{{
				Name:        "test",
				Trigger:     config.MoverTriggerConfig{Type: "manual"},
				Source:      config.MoverSourceConfig{Paths: []string{"ssd1"}, Patterns: []string{"media/**"}},
				Destination: config.MoverDestinationConfig{Paths: []string{"ssd1"}, Policy: "first_found"},
			}},
		},
	}

	cr, err := Count(context.Background(), "test-mount", mc, Opts{})
	require.NoError(t, err)
	require.Equal(t, int64(2), cr.TotalCandidates)
}

// TestCount_withLimit_shouldCapCount verifies Count respects the limit option.
func TestCount_withLimit_shouldCapCount(t *testing.T) {
	root := t.TempDir()
	require.NoError(t, os.MkdirAll(filepath.Join(root, "media"), 0o755))
	for i := 0; i < 5; i++ {
		require.NoError(t, os.WriteFile(filepath.Join(root, "media", fmt.Sprintf("f%d.txt", i)), []byte("x"), 0o644))
	}

	mc := &config.MountConfig{
		StoragePaths: []config.StoragePath{{ID: "ssd1", Path: root}},
		Mover: config.MoverConfig{
			Enabled: boolPtr(true),
			Jobs: []config.MoverJobConfig{{
				Name:        "test",
				Trigger:     config.MoverTriggerConfig{Type: "manual"},
				Source:      config.MoverSourceConfig{Paths: []string{"ssd1"}, Patterns: []string{"media/**"}},
				Destination: config.MoverDestinationConfig{Paths: []string{"ssd1"}, Policy: "first_found"},
			}},
		},
	}

	cr, err := Count(context.Background(), "test-mount", mc, Opts{Limit: 2})
	require.NoError(t, err)
	require.Equal(t, int64(2), cr.TotalCandidates)
}

// TestRunJob_usageTrigger_shouldStopAtThresholdStop verifies hysteresis: job stops when source usage drops to threshold_stop.
func TestRunJob_usageTrigger_shouldStopAtThresholdStop(t *testing.T) {
	srcDir := t.TempDir()
	dstDir := t.TempDir()
	require.NoError(t, os.MkdirAll(filepath.Join(srcDir, "lib"), 0o755))
	for i := 0; i < 5; i++ {
		require.NoError(t, os.WriteFile(filepath.Join(srcDir, "lib", fmt.Sprintf("f%d.txt", i)), make([]byte, 100), 0o644))
	}

	mc := &config.MountConfig{StoragePaths: []config.StoragePath{
		{ID: "ssd1", Path: srcDir},
		{ID: "hdd1", Path: dstDir},
	}}
	p := newPlanner("media", mc, Opts{})
	p.freeSpaceGB = func(_ string) (float64, error) { return 100, nil }
	p.now = func() time.Time { return time.Now() }

	usageCalls := 0
	p.usagePct = func(_ string) (float64, error) {
		usageCalls++
		// Call 1: activeSourcesForJob threshold check → 90% (above start=80).
		// Calls 2+: hysteresis check after each successful move.
		switch usageCalls {
		case 1:
			return 90, nil
		case 2:
			return 85, nil
		case 3:
			return 80, nil
		default:
			return 75, nil // <= threshold_stop=75 → break
		}
	}

	j := config.MoverJobConfig{
		Name:         "test",
		Trigger:      config.MoverTriggerConfig{Type: "usage", ThresholdStart: 80, ThresholdStop: 75},
		Source:       config.MoverSourceConfig{Paths: []string{"ssd1"}, Patterns: []string{"lib/**"}},
		Destination:  config.MoverDestinationConfig{Paths: []string{"hdd1"}, Policy: "first_found"},
		DeleteSource: boolPtr(false),
		Verify:       boolPtr(false),
	}

	jr, err := p.runJob(context.Background(), j, Hooks{}, nil, 0, 0)
	require.NoError(t, err)
	require.Equal(t, int64(3), jr.FilesMoved, "should stop after 3 moves when usage reaches threshold_stop")
	require.Equal(t, int64(5), jr.TotalCandidates, "should have discovered all 5 candidates")
}
