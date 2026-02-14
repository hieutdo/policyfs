package eventlog

import (
	"bufio"
	"errors"
	"fmt"
	"io"
	"os"
	"strings"

	"github.com/hieutdo/policyfs/internal/errkind"
)

// Reader reads NDJSON events sequentially starting from a byte offset.
// It never returns a partial last line; callers should retry later.
type Reader struct {
	f      *os.File
	r      *bufio.Reader
	offset int64
	empty  bool
}

// OpenReader opens events.ndjson for a mount and seeks to an offset.
// Missing log files are treated as empty.
func OpenReader(mountName string, offset int64) (*Reader, error) {
	if strings.TrimSpace(mountName) == "" {
		return nil, &errkind.RequiredError{What: "mount name"}
	}
	if offset < 0 {
		return nil, &errkind.InvalidError{What: "event offset"}
	}

	p, err := LogPath(mountName)
	if err != nil {
		return nil, err
	}

	f, err := os.Open(p)
	if err != nil {
		if errors.Is(err, os.ErrNotExist) {
			return &Reader{empty: true, offset: offset}, nil
		}
		return nil, fmt.Errorf("failed to open event log: %w", err)
	}

	st, err := f.Stat()
	if err != nil {
		_ = f.Close()
		return nil, fmt.Errorf("failed to stat event log: %w", err)
	}
	if offset > st.Size() {
		offset = st.Size()
	}
	if _, err := f.Seek(offset, io.SeekStart); err != nil {
		_ = f.Close()
		return nil, fmt.Errorf("failed to seek event log: %w", err)
	}

	return &Reader{f: f, r: bufio.NewReader(f), offset: offset}, nil
}

// Close releases the underlying file.
func (r *Reader) Close() error {
	if r == nil || r.f == nil {
		return nil
	}
	err := r.f.Close()
	r.f = nil
	r.r = nil
	if err != nil {
		return fmt.Errorf("failed to close event log: %w", err)
	}
	return nil
}

// Offset returns the current byte offset.
func (r *Reader) Offset() int64 {
	if r == nil {
		return 0
	}
	return r.offset
}

// Next returns the next complete NDJSON line without the trailing newline.
// On EOF (including partial last line), it returns io.EOF and does not advance the offset.
func (r *Reader) Next() (line []byte, nextOffset int64, err error) {
	if r == nil {
		return nil, 0, &errkind.NilError{What: "event reader"}
	}
	if r.empty {
		return nil, r.offset, io.EOF
	}
	if r.r == nil {
		return nil, r.offset, io.EOF
	}

	b, err := r.r.ReadBytes('\n')
	if err != nil {
		if errors.Is(err, io.EOF) {
			// Partial last line: do not advance offset.
			return nil, r.offset, io.EOF
		}
		return nil, r.offset, fmt.Errorf("failed to read event log: %w", err)
	}

	next := r.offset + int64(len(b))
	if len(b) == 0 {
		return nil, r.offset, io.EOF
	}
	// Trim trailing \n.
	b = b[:len(b)-1]

	r.offset = next
	return b, r.offset, nil
}
