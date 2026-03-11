package daemonctl

import (
	"context"
	"encoding/json"
	"errors"
	"net"
	"os"
	"path/filepath"
	"strings"
	"testing"
	"time"

	"github.com/hieutdo/policyfs/internal/errkind"
	"github.com/rs/zerolog"
	"github.com/stretchr/testify/require"
)

// testSock returns a short Unix socket path (macOS limits sun_path to 104 bytes).
func testSock(t *testing.T) string {
	t.Helper()
	dir, err := os.MkdirTemp("/tmp", "dctl")
	require.NoError(t, err)
	t.Cleanup(func() { _ = os.RemoveAll(dir) })
	return filepath.Join(dir, "d.sock")
}

// stubProvider implements OpenCountsProvider for testing.
type stubProvider struct {
	stats []OpenStat
	err   error
}

func (s *stubProvider) OpenCounts(_ context.Context, files []OpenFileID) ([]OpenStat, error) {
	if s.err != nil {
		return nil, s.err
	}
	if s.stats != nil {
		return s.stats, nil
	}
	out := make([]OpenStat, len(files))
	for i, f := range files {
		out[i] = OpenStat{OpenFileID: f}
	}
	return out, nil
}

// stubReloadProvider implements both OpenCountsProvider and ReloadProvider for testing.
type stubReloadProvider struct {
	stubProvider

	changed bool
	fields  []string
	err     error
}

// Reload returns the configured test result for a reload request.
func (s *stubReloadProvider) Reload(_ context.Context, configPath string) (bool, []string, error) {
	if strings.TrimSpace(configPath) == "" {
		return false, nil, &errkind.RequiredError{What: "config path"}
	}
	if s.err != nil {
		return false, nil, s.err
	}
	return s.changed, s.fields, nil
}

func testLogger() zerolog.Logger {
	return zerolog.Nop()
}

// writeUnsupportedOpResponse connects to srv and writes a request with an unsupported op,
// then returns the decoded response.
func writeUnsupportedOpResponse(t *testing.T, sock string) OpenCountsResponse {
	t.Helper()

	c, err := net.Dial("unix", sock)
	require.NoError(t, err)
	defer func() { _ = c.Close() }()

	enc := json.NewEncoder(c)
	require.NoError(t, enc.Encode(OpenCountsRequest{Op: "nope", Files: nil}))

	dec := json.NewDecoder(c)
	var resp OpenCountsResponse
	require.NoError(t, dec.Decode(&resp))
	return resp
}

// --- StartServer ---

func TestStartServer_emptyPath_shouldReturnRequiredError(t *testing.T) {
	_, err := StartServer(context.Background(), "", &stubProvider{}, testLogger())
	require.Error(t, err)
	var re *errkind.RequiredError
	require.True(t, errors.As(err, &re))
}

func TestStartServer_nilProvider_shouldReturnNilError(t *testing.T) {
	sock := filepath.Join(t.TempDir(), "d.sock")
	_, err := StartServer(context.Background(), sock, nil, testLogger())
	require.Error(t, err)
	var ne *errkind.NilError
	require.True(t, errors.As(err, &ne))
}

func TestStartServer_valid_shouldListenAndClose(t *testing.T) {
	sock := testSock(t)
	ctx := t.Context()

	srv, err := StartServer(ctx, sock, &stubProvider{}, testLogger())
	require.NoError(t, err)

	st, err := os.Stat(sock)
	require.NoError(t, err)
	require.Equal(t, os.FileMode(0o600), st.Mode()&os.ModePerm)

	require.NoError(t, srv.Close())
	_, err = os.Stat(sock)
	require.True(t, os.IsNotExist(err))
}

// TestStartServer_sockPathIsDirectory_shouldCleanupAndListen verifies StartServer
// can recover from a stale directory artifact at the socket path.
func TestStartServer_sockPathIsDirectory_shouldCleanupAndListen(t *testing.T) {
	sock := testSock(t)

	require.NoError(t, os.MkdirAll(sock, 0o755))

	ctx := t.Context()

	srv, err := StartServer(ctx, sock, &stubProvider{}, testLogger())
	require.NoError(t, err)
	defer func() { _ = srv.Close() }()

	st, err := os.Stat(sock)
	require.NoError(t, err)
	require.False(t, st.IsDir())
}

func TestStartServer_contextCancel_shouldCloseServer(t *testing.T) {
	sock := testSock(t)
	ctx, cancel := context.WithCancel(context.Background())

	_, err := StartServer(ctx, sock, &stubProvider{}, testLogger())
	require.NoError(t, err)

	cancel()
	require.Eventually(t, func() bool {
		_, err := os.Stat(sock)
		return os.IsNotExist(err)
	}, 500*time.Millisecond, 10*time.Millisecond)
}

// --- Close ---

func TestClose_nil_shouldNotPanic(t *testing.T) {
	var s *Server
	require.NoError(t, s.Close())
}

func TestClose_idempotent_shouldNotError(t *testing.T) {
	sock := testSock(t)
	srv, err := StartServer(context.Background(), sock, &stubProvider{}, testLogger())
	require.NoError(t, err)

	require.NoError(t, srv.Close())
	require.NoError(t, srv.Close())
}

// --- QueryOpenCounts ---

func TestQueryOpenCounts_emptyPath_shouldReturnRequiredError(t *testing.T) {
	_, err := QueryOpenCounts(context.Background(), "", nil)
	require.Error(t, err)
	var re *errkind.RequiredError
	require.True(t, errors.As(err, &re))
}

func TestQueryOpenCounts_noServer_shouldReturnDialError(t *testing.T) {
	sock := filepath.Join(t.TempDir(), "x.sock")
	_, err := QueryOpenCounts(context.Background(), sock, nil)
	require.Error(t, err)
	require.ErrorIs(t, err, ErrDialDaemonSocket)
}

// --- Reload ---

// TestReload_emptySockPath_shouldReturnRequiredError verifies sock path is required.
func TestReload_emptySockPath_shouldReturnRequiredError(t *testing.T) {
	_, _, err := Reload(context.Background(), "", "/etc/pfs/pfs.yaml")
	require.Error(t, err)
	var re *errkind.RequiredError
	require.True(t, errors.As(err, &re))
}

// TestReload_emptyConfigPath_shouldReturnRequiredError verifies config path is required.
func TestReload_emptyConfigPath_shouldReturnRequiredError(t *testing.T) {
	_, _, err := Reload(context.Background(), "/tmp/daemon.sock", "")
	require.Error(t, err)
	var re *errkind.RequiredError
	require.True(t, errors.As(err, &re))
}

// TestReload_noServer_shouldReturnDialError verifies dial errors are classified.
func TestReload_noServer_shouldReturnDialError(t *testing.T) {
	sock := filepath.Join(t.TempDir(), "x.sock")
	_, _, err := Reload(context.Background(), sock, "/etc/pfs/pfs.yaml")
	require.Error(t, err)
	require.ErrorIs(t, err, ErrDialDaemonSocket)
}

// TestReload_serverWithoutReloadProvider_shouldReturnRemoteError verifies unsupported op mapping.
func TestReload_serverWithoutReloadProvider_shouldReturnRemoteError(t *testing.T) {
	sock := testSock(t)
	provider := &stubProvider{}

	ctx := t.Context()

	srv, err := StartServer(ctx, sock, provider, testLogger())
	require.NoError(t, err)
	defer func() { _ = srv.Close() }()

	_, _, err = Reload(ctx, sock, "/etc/pfs/pfs.yaml")
	require.Error(t, err)
	require.ErrorIs(t, err, ErrRemote)
}

// TestReload_roundTrip_shouldReturnChanged verifies a successful reload round-trip.
func TestReload_roundTrip_shouldReturnChanged(t *testing.T) {
	sock := testSock(t)
	provider := &stubReloadProvider{changed: true, fields: []string{"mounts.media.routing_rules"}}

	ctx := t.Context()

	srv, err := StartServer(ctx, sock, provider, testLogger())
	require.NoError(t, err)
	defer func() { _ = srv.Close() }()

	changed, fields, err := Reload(ctx, sock, "/etc/pfs/pfs.yaml")
	require.NoError(t, err)
	require.True(t, changed)
	require.Equal(t, []string{"mounts.media.routing_rules"}, fields)
}

// TestReload_providerError_shouldReturnRemoteError verifies provider errors are surfaced as remote errors.
func TestReload_providerError_shouldReturnRemoteError(t *testing.T) {
	sock := testSock(t)
	provider := &stubReloadProvider{err: errors.New("boom")}

	ctx := t.Context()

	srv, err := StartServer(ctx, sock, provider, testLogger())
	require.NoError(t, err)
	defer func() { _ = srv.Close() }()

	_, _, err = Reload(ctx, sock, "/etc/pfs/pfs.yaml")
	require.Error(t, err)
	require.ErrorIs(t, err, ErrRemote)
}

// --- Round-trip integration ---

func TestQueryOpenCounts_roundTrip_shouldReturnCounts(t *testing.T) {
	sock := testSock(t)

	id1 := OpenFileID{StorageID: "ssd1", Dev: 1, Ino: 100}
	id2 := OpenFileID{StorageID: "ssd1", Dev: 1, Ino: 200}

	provider := &stubProvider{
		stats: []OpenStat{
			{OpenFileID: id1, OpenCount: 2, OpenWriteCount: 1},
			{OpenFileID: id2, OpenCount: 0, OpenWriteCount: 0},
		},
	}

	ctx := t.Context()

	srv, err := StartServer(ctx, sock, provider, testLogger())
	require.NoError(t, err)
	defer func() { _ = srv.Close() }()

	stats, err := QueryOpenCounts(ctx, sock, []OpenFileID{id1, id2})
	require.NoError(t, err)
	require.Len(t, stats, 2)
	require.Equal(t, int64(2), stats[0].OpenCount)
	require.Equal(t, int64(1), stats[0].OpenWriteCount)
	require.Equal(t, int64(0), stats[1].OpenCount)
}

func TestQueryOpenCounts_emptyFileList_shouldReturnEmpty(t *testing.T) {
	sock := testSock(t)
	provider := &stubProvider{}

	ctx := t.Context()

	srv, err := StartServer(ctx, sock, provider, testLogger())
	require.NoError(t, err)
	defer func() { _ = srv.Close() }()

	stats, err := QueryOpenCounts(ctx, sock, []OpenFileID{})
	require.NoError(t, err)
	require.Empty(t, stats)
}

func TestQueryOpenCounts_providerError_shouldReturnError(t *testing.T) {
	sock := testSock(t)
	provider := &stubProvider{err: errors.New("boom")}

	ctx := t.Context()

	srv, err := StartServer(ctx, sock, provider, testLogger())
	require.NoError(t, err)
	defer func() { _ = srv.Close() }()

	_, err = QueryOpenCounts(ctx, sock, []OpenFileID{{StorageID: "s", Dev: 1, Ino: 1}})
	require.Error(t, err)
	require.ErrorIs(t, err, ErrRemote)
}

// TestServer_unsupportedOp_shouldReturnError verifies server-side handling for unknown operations.
func TestServer_unsupportedOp_shouldReturnError(t *testing.T) {
	sock := testSock(t)
	provider := &stubProvider{}

	ctx := t.Context()

	srv, err := StartServer(ctx, sock, provider, testLogger())
	require.NoError(t, err)
	defer func() { _ = srv.Close() }()

	resp := writeUnsupportedOpResponse(t, sock)
	require.NotEmpty(t, resp.Error)

	// QueryOpenCounts expects OpOpenCounts, so to validate client-side remote error mapping
	// we rely on provider error test above. This guards the server-side behavior.
}

func TestQueryOpenCounts_multipleSequential_shouldWork(t *testing.T) {
	sock := testSock(t)
	id := OpenFileID{StorageID: "ssd1", Dev: 1, Ino: 42}
	provider := &stubProvider{
		stats: []OpenStat{{OpenFileID: id, OpenCount: 5}},
	}

	ctx := t.Context()

	srv, err := StartServer(ctx, sock, provider, testLogger())
	require.NoError(t, err)
	defer func() { _ = srv.Close() }()

	// First query.
	stats, err := QueryOpenCounts(ctx, sock, []OpenFileID{id})
	require.NoError(t, err)
	require.Len(t, stats, 1)
	require.Equal(t, int64(5), stats[0].OpenCount)

	// Second query on same server (each query opens a new connection).
	stats, err = QueryOpenCounts(ctx, sock, []OpenFileID{id})
	require.NoError(t, err)
	require.Len(t, stats, 1)
	require.Equal(t, int64(5), stats[0].OpenCount)
}
