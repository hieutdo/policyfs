package daemonctl

import (
	"context"
	"encoding/json"
	"errors"
	"fmt"
	"net"
	"os"
	"path/filepath"
	"strings"
	"sync"
	"time"

	"github.com/hieutdo/policyfs/internal/errkind"
	"github.com/rs/zerolog"
)

const (
	// OpOpenCounts is the request op for querying open counts.
	OpOpenCounts = "open_counts"

	// OpReload is the request op for hot-reloading mount-scoped config.
	OpReload = "reload"
)

var (
	// ErrDialDaemonSocket indicates the client failed to connect to daemon.sock.
	ErrDialDaemonSocket = errkind.SentinelError("daemon socket dial failed")

	// ErrRemote indicates the daemon responded with a non-empty error string.
	ErrRemote = errkind.SentinelError("daemonctl remote error")
)

// RemoteError wraps an error returned by the daemon over the control socket.
//
// Callers can match it via errors.Is(err, ErrRemote).
type RemoteError struct {
	Msg string
}

func (e *RemoteError) Error() string {
	if e == nil {
		return ""
	}
	return e.Msg
}

func (e *RemoteError) Is(target error) bool {
	return target == ErrRemote
}

// OpenFileID uniquely identifies a file on a storage backend.
//
// Dev+Ino come from syscall.Stat_t and are used to identify hardlinks correctly.
// StorageID scopes the ID to the configured storage root.
type OpenFileID struct {
	StorageID string `json:"storage_id"`
	Dev       uint64 `json:"dev"`
	Ino       uint64 `json:"ino"`
}

// OpenStat is an open-count snapshot for a file.
type OpenStat struct {
	OpenFileID
	OpenCount      int64 `json:"open_count"`
	OpenWriteCount int64 `json:"open_write_count"`
}

// OpenCountsRequest is a daemon.sock request.
type OpenCountsRequest struct {
	Op    string       `json:"op"`
	Files []OpenFileID `json:"files"`
}

// OpenCountsResponse is a daemon.sock response.
type OpenCountsResponse struct {
	Files []OpenStat `json:"files"`
	Error string     `json:"error,omitempty"`
}

// ReloadRequest is a daemon.sock request.
type ReloadRequest struct {
	Op         string `json:"op"`
	ConfigPath string `json:"config_path"`
}

// ReloadResponse is a daemon.sock response.
type ReloadResponse struct {
	Changed       bool     `json:"changed"`
	ChangedFields []string `json:"changed_fields,omitempty"`
	Error         string   `json:"error,omitempty"`
}

// OpenCountsProvider exposes open-count information to the control server.
type OpenCountsProvider interface {
	OpenCounts(ctx context.Context, files []OpenFileID) ([]OpenStat, error)
}

// ReloadProvider applies a config reload request.
type ReloadProvider interface {
	Reload(ctx context.Context, configPath string) (bool, []string, error)
}

// Server is a unix domain socket server for daemon control/status queries.
type Server struct {
	sockPath string
	ln       net.Listener
	provider OpenCountsProvider
	log      zerolog.Logger
	ctx      context.Context

	closeOnce sync.Once
}

// StartServer starts a unix domain socket server at sockPath.
func StartServer(ctx context.Context, sockPath string, provider OpenCountsProvider, log zerolog.Logger) (*Server, error) {
	if strings.TrimSpace(sockPath) == "" {
		return nil, &errkind.RequiredError{What: "sock path"}
	}
	if provider == nil {
		return nil, &errkind.NilError{What: "open counts provider"}
	}

	if err := os.MkdirAll(filepath.Dir(sockPath), 0o755); err != nil {
		return nil, fmt.Errorf("failed to ensure daemon socket dir: %w", err)
	}
	if err := cleanupSockPath(sockPath); err != nil {
		return nil, fmt.Errorf("failed to cleanup daemon socket path: %w", err)
	}

	ln, err := net.Listen("unix", sockPath)
	if err != nil {
		return nil, fmt.Errorf("failed to listen on daemon socket: %w", err)
	}
	if err := os.Chmod(sockPath, 0o600); err != nil {
		_ = ln.Close()
		_ = os.Remove(sockPath)
		return nil, fmt.Errorf("failed to chmod daemon socket: %w", err)
	}

	s := &Server{sockPath: sockPath, ln: ln, provider: provider, log: log, ctx: ctx}

	go func() {
		<-ctx.Done()
		_ = s.Close()
	}()
	go s.acceptLoop()

	return s, nil
}

// cleanupSockPath removes sockPath if it exists.
//
// This is defensive against stale artifacts (e.g. a directory at sockPath) that would
// otherwise make net.Listen("unix") fail with opaque errors.
func cleanupSockPath(sockPath string) error {
	if strings.TrimSpace(sockPath) == "" {
		return nil
	}
	st, err := os.Lstat(sockPath)
	if err != nil {
		if errors.Is(err, os.ErrNotExist) {
			return nil
		}
		return fmt.Errorf("failed to lstat socket path: %w", err)
	}
	if st.IsDir() {
		if err := os.RemoveAll(sockPath); err != nil {
			return fmt.Errorf("failed to remove socket path dir: %w", err)
		}
		return nil
	}
	if err := os.Remove(sockPath); err != nil {
		return fmt.Errorf("failed to remove socket path: %w", err)
	}
	return nil
}

// Close shuts down the server and removes the socket file.
func (s *Server) Close() error {
	if s == nil {
		return nil
	}
	var err error
	s.closeOnce.Do(func() {
		if s.ln != nil {
			if cerr := s.ln.Close(); cerr != nil {
				err = fmt.Errorf("failed to close daemon socket listener: %w", cerr)
			}
		}
		if s.sockPath != "" {
			_ = os.Remove(s.sockPath)
		}
	})
	return err
}

// acceptLoop accepts connections until the listener is closed.
func (s *Server) acceptLoop() {
	for {
		c, err := s.ln.Accept()
		if err != nil {
			if errors.Is(err, net.ErrClosed) {
				return
			}
			// Transient errors (e.g. EMFILE): log and keep accepting.
			s.log.Error().Str("op", "accept").Err(err).Msg("failed to accept daemonctl connection")
			time.Sleep(50 * time.Millisecond)
			continue
		}
		go s.handleConn(c)
	}
}

// handleConn handles one request per connection.
func (s *Server) handleConn(c net.Conn) {
	defer func() { _ = c.Close() }()

	_ = c.SetDeadline(time.Now().Add(2 * time.Second))

	dec := json.NewDecoder(c)
	var raw json.RawMessage
	if err := dec.Decode(&raw); err != nil {
		s.log.Error().Str("op", "decode").Err(err).Msg("failed to decode daemonctl request")
		return
	}

	type baseRequest struct {
		Op string `json:"op"`
	}
	type errorResponse struct {
		Error string `json:"error,omitempty"`
	}

	var base baseRequest
	if err := json.Unmarshal(raw, &base); err != nil {
		s.log.Error().Str("op", "decode").Err(err).Msg("failed to decode daemonctl request")
		return
	}

	switch strings.TrimSpace(base.Op) {
	case OpOpenCounts:
		var req OpenCountsRequest
		if err := json.Unmarshal(raw, &req); err != nil {
			s.log.Error().Str("op", "decode").Err(err).Msg("failed to decode daemonctl request")
			return
		}

		stats, err := s.provider.OpenCounts(s.ctx, req.Files)
		if err != nil {
			s.log.Error().Str("op", "open_counts").Err(err).Msg("failed to fetch open counts")
			_ = writeResponse(c, OpenCountsResponse{Error: "failed to fetch open counts"})
			return
		}
		_ = writeResponse(c, OpenCountsResponse{Files: stats})
		return
	case OpReload:
		var req ReloadRequest
		if err := json.Unmarshal(raw, &req); err != nil {
			s.log.Error().Str("op", "decode").Err(err).Msg("failed to decode daemonctl request")
			return
		}
		if strings.TrimSpace(req.ConfigPath) == "" {
			_ = writeResponse(c, errorResponse{Error: "config_path is required"})
			return
		}
		rp, ok := s.provider.(ReloadProvider)
		if !ok {
			_ = writeResponse(c, errorResponse{Error: "unsupported op"})
			return
		}

		changed, changedFields, err := rp.Reload(s.ctx, req.ConfigPath)
		if err != nil {
			s.log.Error().Str("op", "reload").Str("path", req.ConfigPath).Err(err).Msg("failed to reload config")
			_ = writeResponse(c, ReloadResponse{Error: err.Error()})
			return
		}
		_ = writeResponse(c, ReloadResponse{Changed: changed, ChangedFields: changedFields})
		return
	default:
		_ = writeResponse(c, errorResponse{Error: "unsupported op"})
		return
	}
}

// QueryOpenCounts queries daemon.sock for open counts.
func QueryOpenCounts(ctx context.Context, sockPath string, files []OpenFileID) ([]OpenStat, error) {
	if strings.TrimSpace(sockPath) == "" {
		return nil, &errkind.RequiredError{What: "sock path"}
	}

	d := net.Dialer{Timeout: 200 * time.Millisecond}
	c, err := d.DialContext(ctx, "unix", sockPath)
	if err != nil {
		return nil, &errkind.KindError{Kind: ErrDialDaemonSocket, Msg: "failed to dial daemon socket", Cause: err}
	}
	defer func() { _ = c.Close() }()

	deadline := time.Now().Add(2 * time.Second)
	if dl, ok := ctx.Deadline(); ok && dl.Before(deadline) {
		deadline = dl
	}
	_ = c.SetDeadline(deadline)

	enc := json.NewEncoder(c)
	if err := enc.Encode(OpenCountsRequest{Op: OpOpenCounts, Files: files}); err != nil {
		return nil, fmt.Errorf("failed to encode daemonctl request: %w", err)
	}

	dec := json.NewDecoder(c)
	var resp OpenCountsResponse
	if err := dec.Decode(&resp); err != nil {
		return nil, fmt.Errorf("failed to decode daemonctl response: %w", err)
	}
	if strings.TrimSpace(resp.Error) != "" {
		return nil, &RemoteError{Msg: resp.Error}
	}
	return resp.Files, nil
}

// Reload requests that the daemon reloads its mount-scoped configuration.
func Reload(ctx context.Context, sockPath string, configPath string) (bool, []string, error) {
	if strings.TrimSpace(sockPath) == "" {
		return false, nil, &errkind.RequiredError{What: "sock path"}
	}
	if strings.TrimSpace(configPath) == "" {
		return false, nil, &errkind.RequiredError{What: "config path"}
	}

	d := net.Dialer{Timeout: 200 * time.Millisecond}
	c, err := d.DialContext(ctx, "unix", sockPath)
	if err != nil {
		return false, nil, &errkind.KindError{Kind: ErrDialDaemonSocket, Msg: "failed to dial daemon socket", Cause: err}
	}
	defer func() { _ = c.Close() }()

	deadline := time.Now().Add(2 * time.Second)
	if dl, ok := ctx.Deadline(); ok && dl.Before(deadline) {
		deadline = dl
	}
	_ = c.SetDeadline(deadline)

	enc := json.NewEncoder(c)
	if err := enc.Encode(ReloadRequest{Op: OpReload, ConfigPath: configPath}); err != nil {
		return false, nil, fmt.Errorf("failed to encode daemonctl request: %w", err)
	}

	dec := json.NewDecoder(c)
	var resp ReloadResponse
	if err := dec.Decode(&resp); err != nil {
		return false, nil, fmt.Errorf("failed to decode daemonctl response: %w", err)
	}
	if strings.TrimSpace(resp.Error) != "" {
		return false, nil, &RemoteError{Msg: resp.Error}
	}
	return resp.Changed, resp.ChangedFields, nil
}

// writeResponse writes a single JSON response.
func writeResponse(w net.Conn, resp any) error {
	enc := json.NewEncoder(w)
	if err := enc.Encode(resp); err != nil {
		return fmt.Errorf("failed to encode daemonctl response: %w", err)
	}
	return nil
}
