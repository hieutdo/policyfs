package eventlog

import (
	"context"
	"encoding/json"
	"errors"
	"fmt"
	"io"
	"os"
	"path/filepath"
	"strconv"
	"strings"
	"syscall"

	"github.com/hieutdo/policyfs/internal/config"
	"github.com/hieutdo/policyfs/internal/errkind"
)

const (
	// FileMode restricts the event log and offset files to owner-only access.
	FileMode = 0o600
)

// Type defines the supported deferred mutation event types.
type Type string

const (
	// TypeDelete marks a file/dir for later physical deletion.
	TypeDelete Type = "DELETE"
	// TypeRename marks a file/dir for later physical rename.
	TypeRename Type = "RENAME"
	// TypeSetattr marks a file for later physical chmod/chown/utimens.
	TypeSetattr Type = "SETATTR"
)

// DeleteEvent describes one deferred DELETE operation.
type DeleteEvent struct {
	Type      Type   `json:"type"`
	StorageID string `json:"storage_id"`
	Path      string `json:"path"`
	IsDir     bool   `json:"is_dir"`
	TS        int64  `json:"ts"`
}

// RenameEvent describes one deferred RENAME operation.
type RenameEvent struct {
	Type      Type   `json:"type"`
	StorageID string `json:"storage_id"`
	OldPath   string `json:"old_path"`
	NewPath   string `json:"new_path"`
	TS        int64  `json:"ts"`
}

// SetattrEvent describes one deferred SETATTR operation.
// Fields are pointers so absence can be distinguished from setting a zero value.
type SetattrEvent struct {
	Type      Type    `json:"type"`
	StorageID string  `json:"storage_id"`
	Path      string  `json:"path"`
	Mode      *uint32 `json:"mode,omitempty"`
	UID       *uint32 `json:"uid,omitempty"`
	GID       *uint32 `json:"gid,omitempty"`
	MTime     *int64  `json:"mtime,omitempty"`
	ATime     *int64  `json:"atime,omitempty"`
	TS        int64   `json:"ts"`
}

// Event is the union of supported event payloads.
// It is returned from Parse().
type Event interface {
	// EventType returns the stable type discriminator.
	EventType() Type
}

// EventType returns the event type discriminator.
func (e DeleteEvent) EventType() Type { return e.Type }

// EventType returns the event type discriminator.
func (e RenameEvent) EventType() Type { return e.Type }

// EventType returns the event type discriminator.
func (e SetattrEvent) EventType() Type { return e.Type }

// LogPath returns the absolute path to the events.ndjson file for a mount.
func LogPath(mountName string) (string, error) {
	mountName = strings.TrimSpace(mountName)
	if mountName == "" {
		return "", &errkind.RequiredError{What: "mount name"}
	}
	return filepath.Join(config.MountStateDir(mountName), "events.ndjson"), nil
}

// OffsetPath returns the absolute path to the events.offset file for a mount.
func OffsetPath(mountName string) (string, error) {
	mountName = strings.TrimSpace(mountName)
	if mountName == "" {
		return "", &errkind.RequiredError{What: "mount name"}
	}
	return filepath.Join(config.MountStateDir(mountName), "events.offset"), nil
}

// Append appends one event to the mount's NDJSON log.
// The caller is responsible for populating required fields like type/storage_id/ts.
func Append(ctx context.Context, mountName string, v any) error {
	_ = ctx
	p, err := LogPath(mountName)
	if err != nil {
		return err
	}
	if err := os.MkdirAll(filepath.Dir(p), 0o755); err != nil {
		return fmt.Errorf("failed to ensure mount state dir: %w", err)
	}

	b, err := json.Marshal(v)
	if err != nil {
		return fmt.Errorf("failed to marshal event: %w", err)
	}
	b = append(b, '\n')

	f, err := os.OpenFile(p, os.O_CREATE|os.O_WRONLY|os.O_APPEND, FileMode)
	if err != nil {
		return fmt.Errorf("failed to open event log: %w", err)
	}
	defer func() { _ = f.Close() }()

	// Shared lock coordinates with maybeTruncateEvents (exclusive lock) to prevent
	// the TOCTOU race where truncation drops freshly-appended events.
	if err := syscall.Flock(int(f.Fd()), syscall.LOCK_SH); err != nil {
		return fmt.Errorf("failed to lock event log: %w", err)
	}

	// Write the full NDJSON line in a single write syscall so concurrent appends
	// cannot interleave bytes.
	n, err := f.Write(b)
	if err != nil {
		return fmt.Errorf("failed to append event: %w", err)
	}
	if n != len(b) {
		return fmt.Errorf("failed to append event: %w", io.ErrShortWrite)
	}
	return nil
}

// ReadOffset reads the last processed byte offset for prune.
// Missing offset files are treated as 0.
func ReadOffset(mountName string) (int64, error) {
	p, err := OffsetPath(mountName)
	if err != nil {
		return 0, err
	}

	b, err := os.ReadFile(p)
	if err != nil {
		if errors.Is(err, os.ErrNotExist) {
			return 0, nil
		}
		return 0, fmt.Errorf("failed to read event offset: %w", err)
	}
	raw := strings.TrimSpace(string(b))
	if raw == "" {
		return 0, nil
	}
	off, err := strconv.ParseInt(raw, 10, 64)
	if err != nil {
		return 0, fmt.Errorf("failed to parse event offset: %w", err)
	}
	if off < 0 {
		return 0, &errkind.InvalidError{What: "event offset"}
	}
	return off, nil
}

// WriteOffset persists the last processed byte offset for prune.
// It uses a temp file + rename for atomic replacement.
func WriteOffset(mountName string, off int64) error {
	p, err := OffsetPath(mountName)
	if err != nil {
		return err
	}
	if err := os.MkdirAll(filepath.Dir(p), 0o755); err != nil {
		return fmt.Errorf("failed to ensure mount state dir: %w", err)
	}
	if off < 0 {
		return &errkind.InvalidError{What: "event offset"}
	}

	tmp := p + ".tmp"
	f, err := os.OpenFile(tmp, os.O_CREATE|os.O_TRUNC|os.O_WRONLY, FileMode)
	if err != nil {
		return fmt.Errorf("failed to create temp offset file: %w", err)
	}
	defer func() {
		_ = f.Close()
		_ = os.Remove(tmp)
	}()

	if _, err := fmt.Fprintf(f, "%d\n", off); err != nil {
		return fmt.Errorf("failed to write temp offset: %w", err)
	}
	if err := f.Sync(); err != nil {
		return fmt.Errorf("failed to sync temp offset: %w", err)
	}
	if err := f.Close(); err != nil {
		return fmt.Errorf("failed to close temp offset: %w", err)
	}
	if err := os.Rename(tmp, p); err != nil {
		return fmt.Errorf("failed to replace offset file: %w", err)
	}
	return nil
}

// Parse decodes one NDJSON event line into a typed event payload.
func Parse(line []byte) (Event, error) {
	var base struct {
		Type Type `json:"type"`
	}
	if err := json.Unmarshal(line, &base); err != nil {
		return nil, fmt.Errorf("failed to parse event: %w", err)
	}

	switch base.Type {
	case TypeDelete:
		var e DeleteEvent
		if err := json.Unmarshal(line, &e); err != nil {
			return nil, fmt.Errorf("failed to parse delete event: %w", err)
		}
		return e, nil
	case TypeRename:
		var e RenameEvent
		if err := json.Unmarshal(line, &e); err != nil {
			return nil, fmt.Errorf("failed to parse rename event: %w", err)
		}
		return e, nil
	case TypeSetattr:
		var e SetattrEvent
		if err := json.Unmarshal(line, &e); err != nil {
			return nil, fmt.Errorf("failed to parse setattr event: %w", err)
		}
		return e, nil
	default:
		return nil, &errkind.InvalidError{What: "event type"}
	}
}
