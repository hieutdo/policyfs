package mover

import (
	"context"
	"os"
	"path/filepath"
	"testing"

	"github.com/hieutdo/policyfs/internal/daemonctl"
	"github.com/rs/zerolog"
	"github.com/stretchr/testify/require"
)

// testSock returns a short Unix socket path (macOS limits sun_path to 104 bytes).
func testSock(t *testing.T) string {
	t.Helper()
	dir, err := os.MkdirTemp("/tmp", "mchk")
	require.NoError(t, err)
	t.Cleanup(func() { _ = os.RemoveAll(dir) })
	return filepath.Join(dir, "d.sock")
}

// --- nil / empty planner ---

func TestQueryOpenFileSet_nilPlanner_shouldReturnNil(t *testing.T) {
	var p *planner
	set, err := p.queryOpenFileSet(context.Background(), []candidate{{SrcStorageID: "s", Dev: 1, Ino: 1}})
	require.NoError(t, err)
	require.Nil(t, set)
}

func TestQueryOpenFileSet_emptyDaemonSockPath_shouldReturnNil(t *testing.T) {
	p := &planner{daemonSockPath: ""}
	set, err := p.queryOpenFileSet(context.Background(), []candidate{{SrcStorageID: "s", Dev: 1, Ino: 1}})
	require.NoError(t, err)
	require.Nil(t, set)
}

// --- daemon unreachable ---

func TestQueryOpenFileSet_noSocket_shouldReturnNilGracefully(t *testing.T) {
	p := &planner{daemonSockPath: filepath.Join(t.TempDir(), "nonexistent.sock")}
	set, err := p.queryOpenFileSet(context.Background(), []candidate{
		{SrcStorageID: "ssd1", Dev: 1, Ino: 100},
	})
	require.NoError(t, err)
	require.Nil(t, set)
}

func TestQueryOpenFileSet_canceledContext_shouldReturnNilGracefully(t *testing.T) {
	ctx, cancel := context.WithCancel(context.Background())
	cancel()

	p := &planner{daemonSockPath: filepath.Join(t.TempDir(), "d.sock")}
	set, err := p.queryOpenFileSet(ctx, []candidate{
		{SrcStorageID: "ssd1", Dev: 1, Ino: 100},
	})
	require.NoError(t, err)
	require.Nil(t, set)
}

// --- round-trip with real server ---

func TestQueryOpenFileSet_withServer_shouldReturnOpenFiles(t *testing.T) {
	sock := testSock(t)

	id1 := daemonctl.OpenFileID{StorageID: "ssd1", Dev: 1, Ino: 100}
	id2 := daemonctl.OpenFileID{StorageID: "ssd1", Dev: 1, Ino: 200}
	id3 := daemonctl.OpenFileID{StorageID: "ssd1", Dev: 1, Ino: 300}

	provider := &stubOpenCountsProvider{
		stats: []daemonctl.OpenStat{
			{OpenFileID: id1, OpenCount: 2},
			{OpenFileID: id2, OpenCount: 0},
			{OpenFileID: id3, OpenCount: 1},
		},
	}

	ctx, cancel := context.WithCancel(context.Background())
	defer cancel()

	srv, err := daemonctl.StartServer(ctx, sock, provider, zerolog.Nop())
	require.NoError(t, err)
	defer func() { _ = srv.Close() }()

	p := &planner{daemonSockPath: sock}
	cands := []candidate{
		{SrcStorageID: "ssd1", Dev: 1, Ino: 100},
		{SrcStorageID: "ssd1", Dev: 1, Ino: 200},
		{SrcStorageID: "ssd1", Dev: 1, Ino: 300},
	}

	set, err := p.queryOpenFileSet(ctx, cands)
	require.NoError(t, err)
	require.NotNil(t, set)

	_, ok := set[id1]
	require.True(t, ok, "id1 should be in open set (OpenCount=2)")

	_, ok = set[id2]
	require.False(t, ok, "id2 should NOT be in open set (OpenCount=0)")

	_, ok = set[id3]
	require.True(t, ok, "id3 should be in open set (OpenCount=1)")
}

func TestQueryOpenFileSet_emptyCandidates_shouldReturnEmptySet(t *testing.T) {
	sock := testSock(t)

	provider := &stubOpenCountsProvider{}

	ctx, cancel := context.WithCancel(context.Background())
	defer cancel()

	srv, err := daemonctl.StartServer(ctx, sock, provider, zerolog.Nop())
	require.NoError(t, err)
	defer func() { _ = srv.Close() }()

	p := &planner{daemonSockPath: sock}
	set, err := p.queryOpenFileSet(ctx, []candidate{})
	require.NoError(t, err)
	require.NotNil(t, set)
	require.Empty(t, set)
}

// stubOpenCountsProvider implements daemonctl.OpenCountsProvider for testing.
type stubOpenCountsProvider struct {
	stats []daemonctl.OpenStat
}

func (s *stubOpenCountsProvider) OpenCounts(_ context.Context, files []daemonctl.OpenFileID) ([]daemonctl.OpenStat, error) {
	if s.stats != nil {
		return s.stats, nil
	}
	out := make([]daemonctl.OpenStat, len(files))
	for i, f := range files {
		out[i] = daemonctl.OpenStat{OpenFileID: f}
	}
	return out, nil
}
