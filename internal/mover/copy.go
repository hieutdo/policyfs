package mover

import (
	"context"
	"errors"
	"fmt"
	"io"
	"os"
	"path/filepath"
	"syscall"
	"time"

	"github.com/cespare/xxhash/v2"
)

// defaultCopyRetries is the number of copy attempts before giving up on transient errors.
const defaultCopyRetries = 3

// hashXX64Func is a test seam for hashXX64.
var hashXX64Func = hashXX64

// copyProgressFunc is an optional callback for byte-level progress.
//
// doneBytes is monotonically increasing for a given copy/hash operation.
type copyProgressFunc func(doneBytes int64)

// copyWithContext copies from src to dst while honoring ctx cancellation.
func copyWithContext(ctx context.Context, dst io.Writer, src io.Reader, progress copyProgressFunc) error {
	buf := make([]byte, 256*1024)
	done := int64(0)
	lastProgress := time.Now()
	for {
		if err := ctx.Err(); err != nil {
			return fmt.Errorf("copy canceled: %w", err)
		}
		n, rerr := src.Read(buf)
		if n > 0 {
			if _, werr := dst.Write(buf[:n]); werr != nil {
				return fmt.Errorf("failed to write: %w", werr)
			}
			done += int64(n)
			if progress != nil {
				now := time.Now()
				if now.Sub(lastProgress) >= 200*time.Millisecond {
					lastProgress = now
					progress(done)
				}
			}
		}
		if rerr != nil {
			if errors.Is(rerr, io.EOF) {
				if progress != nil {
					progress(done)
				}
				return nil
			}
			return fmt.Errorf("failed to read: %w", rerr)
		}
	}
}

// skipError is a typed marker error to indicate a candidate should be counted as skipped.
type skipError struct {
	Cause error
}

// Error formats the skip error.
func (e *skipError) Error() string {
	if e == nil || e.Cause == nil {
		return "skip"
	}
	return e.Cause.Error()
}

// Unwrap returns the underlying cause.
func (e *skipError) Unwrap() error {
	if e == nil {
		return nil
	}
	return e.Cause
}

// copyFileWithVerify copies a file to destination with optional checksum verification.
//
// When verify is true, the source hash is computed in a single streaming pass during
// the copy (via io.MultiWriter) to avoid re-reading the source. Only the destination
// temp file is read a second time for the verification hash.
func copyFileWithVerify(ctx context.Context, srcPhys string, dstPhys string, c candidate, verify bool, progress func(phase string, doneBytes int64, totalBytes int64)) error {
	if err := ctx.Err(); err != nil {
		return fmt.Errorf("copy canceled: %w", err)
	}

	if err := os.MkdirAll(filepath.Dir(dstPhys), 0o755); err != nil {
		return fmt.Errorf("failed to create destination dir: %w", err)
	}
	if _, err := os.Stat(dstPhys); err == nil {
		return &skipError{Cause: errors.New("destination already exists")}
	}

	tmp, err := os.CreateTemp(filepath.Dir(dstPhys), ".pfs-move-*")
	if err != nil {
		return fmt.Errorf("failed to create temp file: %w", err)
	}
	tmpPath := tmp.Name()
	closed := false
	defer func() {
		if !closed {
			_ = tmp.Close()
		}
		_ = os.Remove(tmpPath) // no-op if already renamed
	}()

	src, err := os.Open(srcPhys)
	if err != nil {
		return fmt.Errorf("failed to open source: %w", err)
	}
	defer func() { _ = src.Close() }()

	// When verify is enabled, compute source hash in a single pass during copy.
	var srcHash uint64
	var dst io.Writer = tmp
	var srcHasher *xxhash.Digest
	if verify {
		srcHasher = xxhash.New()
		dst = io.MultiWriter(tmp, srcHasher)
	}

	var copyProgress copyProgressFunc
	if progress != nil {
		total := c.SizeBytes
		copyProgress = func(done int64) {
			progress("copy", done, total)
		}
	}
	if err := copyWithContext(ctx, dst, src, copyProgress); err != nil {
		return fmt.Errorf("failed to copy: %w", err)
	}
	if verify {
		srcHash = srcHasher.Sum64()
	}

	if err := tmp.Sync(); err != nil {
		return fmt.Errorf("failed to sync temp file: %w", err)
	}
	if err := tmp.Close(); err != nil {
		return fmt.Errorf("failed to close temp file: %w", err)
	}
	closed = true

	// Best-effort permission preservation.
	perm := os.FileMode(c.Mode & 0o777)
	_ = os.Chmod(tmpPath, perm)

	if verify {
		var verifyProgress copyProgressFunc
		if progress != nil {
			total := c.SizeBytes
			verifyProgress = func(done int64) {
				progress("verify", done, total)
			}
		}
		dstHash, err := hashXX64Func(ctx, tmpPath, verifyProgress)
		if err != nil {
			return err
		}
		if srcHash != dstHash {
			return &skipError{Cause: errors.New("verify failed: checksum mismatch")}
		}
	}

	if err := os.Rename(tmpPath, dstPhys); err != nil {
		return fmt.Errorf("failed to rename temp file: %w", err)
	}

	// Best-effort metadata preservation.
	_ = syscall.Chown(dstPhys, int(c.UID), int(c.GID))
	mt := time.Unix(c.MTimeSec, 0)
	_ = os.Chtimes(dstPhys, mt, mt)

	return nil
}

// copyFileWithVerifyRetry retries copy/verify a few times for transient errors.
func copyFileWithVerifyRetry(ctx context.Context, srcPhys string, dstPhys string, c candidate, verify bool, attempts int, progress func(phase string, doneBytes int64, totalBytes int64)) error {
	if attempts < 1 {
		attempts = 1
	}
	var last error
	for i := 0; i < attempts; i++ {
		err := copyFileWithVerify(ctx, srcPhys, dstPhys, c, verify, progress)
		if err == nil {
			return nil
		}
		// Do not retry skips.
		var skip *skipError
		if errors.As(err, &skip) {
			return err
		}
		// Do not retry disk-full.
		if errors.Is(err, syscall.ENOSPC) {
			return err
		}
		last = err
		select {
		case <-ctx.Done():
			return fmt.Errorf("copy canceled: %w", ctx.Err())
		case <-time.After(50 * time.Millisecond):
		}
	}
	if last == nil {
		last = errors.New("copy failed")
	}
	return last
}

// hashXX64 computes xxhash64 for a file path.
func hashXX64(ctx context.Context, p string, progress copyProgressFunc) (uint64, error) {
	f, err := os.Open(p)
	if err != nil {
		return 0, fmt.Errorf("failed to open file for hash: %w", err)
	}
	defer func() { _ = f.Close() }()

	h := xxhash.New()
	if err := copyWithContext(ctx, h, f, progress); err != nil {
		return 0, fmt.Errorf("failed to hash file: %w", err)
	}
	return h.Sum64(), nil
}
