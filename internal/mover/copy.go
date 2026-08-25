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
	"github.com/hieutdo/policyfs/internal/errkind"
)

// defaultCopyRetries is the number of copy attempts before giving up on transient errors.
const defaultCopyRetries = 3

// errDestinationExists marks a candidate skipped because its destination already exists.
var errDestinationExists = errkind.SentinelError("destination already exists")

// errVerifyMismatch marks a candidate skipped after checksum verification fails.
var errVerifyMismatch = errkind.SentinelError("verify failed: checksum mismatch")

// errCopyFailed marks a retry loop that completed without a concrete cause.
var errCopyFailed = errkind.SentinelError("copy failed")

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

// copyLocation identifies one physical file and its configured storage root.
type copyLocation struct {
	root         string
	physicalPath string
}

// copyFileWithVerify copies a file to destination with optional checksum verification and metadata-aware parents.
//
// When verify is true, the source hash is computed in a single streaming pass during
// the copy (via io.MultiWriter) to avoid re-reading the source. Only the destination
// temp file is read a second time for the verification hash.
func copyFileWithVerify(ctx context.Context, srcLocation copyLocation, dstLocation copyLocation, c candidate, verify bool, progress func(phase string, doneBytes int64, totalBytes int64)) (retErr error) {
	if err := ctx.Err(); err != nil {
		return fmt.Errorf("copy canceled: %w", err)
	}
	if _, err := os.Stat(dstLocation.physicalPath); err == nil {
		return &skipError{Cause: errDestinationExists}
	} else if !errors.Is(err, os.ErrNotExist) {
		return fmt.Errorf("failed to stat destination: %w", err)
	}

	createdDirs, err := ensureDestinationParent(srcLocation, dstLocation)
	if err != nil {
		return err
	}
	committed := false
	defer func() {
		if committed {
			createdDirs.close()
			return
		}
		retErr = errors.Join(retErr, createdDirs.cleanup())
	}()

	tmp, err := os.CreateTemp(filepath.Dir(dstLocation.physicalPath), ".pfs-move-*")
	if err != nil {
		return fmt.Errorf("failed to create temp file: %w", err)
	}
	tmpPath := tmp.Name()
	closed := false
	defer func() {
		if !closed {
			_ = tmp.Close()
		}
		_ = os.Remove(tmpPath)
	}()

	src, err := os.Open(srcLocation.physicalPath)
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
	_ = os.Chmod(tmpPath, os.FileMode(c.Mode&0o777))

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
			return &skipError{Cause: errVerifyMismatch}
		}
	}
	if err := os.Rename(tmpPath, dstLocation.physicalPath); err != nil {
		return fmt.Errorf("failed to rename temp file: %w", err)
	}
	committed = true

	// Best-effort metadata preservation.
	_ = syscall.Chown(dstLocation.physicalPath, int(c.UID), int(c.GID))
	mtime := time.Unix(c.MTimeSec, 0)
	_ = os.Chtimes(dstLocation.physicalPath, mtime, mtime)
	return nil
}

// copyFileWithVerifyRetry retries copy/verify a few times for transient errors.
func copyFileWithVerifyRetry(ctx context.Context, srcLocation copyLocation, dstLocation copyLocation, c candidate, verify bool, attempts int, progress func(phase string, doneBytes int64, totalBytes int64)) error {
	if attempts < 1 {
		attempts = 1
	}
	var last error
	for i := 0; i < attempts; i++ {
		err := copyFileWithVerify(ctx, srcLocation, dstLocation, c, verify, progress)
		if err == nil {
			return nil
		}
		// Do not retry skips.
		if _, ok := errors.AsType[*skipError](err); ok {
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
		last = errCopyFailed
	}
	return last
}

// hashXX64 computes xxhash64 for a file path.
func hashXX64(ctx context.Context, path string, progress copyProgressFunc) (uint64, error) {
	file, err := os.Open(path)
	if err != nil {
		return 0, fmt.Errorf("failed to open file for hash: %w", err)
	}
	defer func() { _ = file.Close() }()

	h := xxhash.New()
	if err := copyWithContext(ctx, h, file, progress); err != nil {
		return 0, fmt.Errorf("failed to hash file: %w", err)
	}
	return h.Sum64(), nil
}
