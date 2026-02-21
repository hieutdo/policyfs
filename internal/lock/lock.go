package lock

import (
	"errors"
	"fmt"
	"os"
	"path/filepath"
	"syscall"

	"github.com/hieutdo/policyfs/internal/config"
	"github.com/hieutdo/policyfs/internal/errkind"
)

// FileLock is a POSIX advisory file lock held via flock(2).
type FileLock struct {
	path string
	f    *os.File
}

// mountLockDir computes the mount-scoped runtime lock directory.
func mountLockDir(mountName string) string {
	return config.MountLockDir(mountName)
}

// AcquireMountLock acquires one of the v1 locks for a mount (daemon.lock or job.lock).
func AcquireMountLock(mountName string, lockFile string) (*FileLock, error) {
	if mountName == "" {
		return nil, &errkind.RequiredError{What: "mount name"}
	}
	if lockFile == "" {
		return nil, &errkind.RequiredError{What: "lock file"}
	}

	lockDir := mountLockDir(mountName)
	if err := os.MkdirAll(lockDir, 0o755); err != nil {
		return nil, fmt.Errorf("failed to ensure lock dir: %w", err)
	}
	p := filepath.Join(lockDir, lockFile)

	f, err := os.OpenFile(p, os.O_CREATE|os.O_RDWR, 0o644)
	if err != nil {
		return nil, fmt.Errorf("failed to open lock file: %w", err)
	}

	if err := syscall.Flock(int(f.Fd()), syscall.LOCK_EX|syscall.LOCK_NB); err != nil {
		_ = f.Close()
		if errors.Is(err, syscall.EWOULDBLOCK) || errors.Is(err, syscall.EAGAIN) {
			return nil, &errkind.BusyError{What: "lock"}
		}
		return nil, fmt.Errorf("failed to acquire lock: %w", err)
	}

	// Write PID so other tools (e.g. pfs doctor) can identify the holder.
	_ = f.Truncate(0)
	_, _ = f.Seek(0, 0)
	_, _ = fmt.Fprintf(f, "%d\n", os.Getpid())

	return &FileLock{path: p, f: f}, nil
}

// ProbeMountLock tests whether a mount lock is currently held without acquiring it.
// Returns busy=true when another process holds the lock.
// The returned pid is best-effort (read from the lock file content) and may be 0.
func ProbeMountLock(mountName string, lockFile string) (busy bool, pid int, err error) {
	if mountName == "" {
		return false, 0, &errkind.RequiredError{What: "mount name"}
	}
	if lockFile == "" {
		return false, 0, &errkind.RequiredError{What: "lock file"}
	}

	p := filepath.Join(mountLockDir(mountName), lockFile)

	f, err := os.OpenFile(p, os.O_RDONLY, 0)
	if err != nil {
		if errors.Is(err, os.ErrNotExist) {
			return false, 0, nil
		}
		return false, 0, fmt.Errorf("failed to open lock file: %w", err)
	}
	defer func() { _ = f.Close() }()

	if err := syscall.Flock(int(f.Fd()), syscall.LOCK_EX|syscall.LOCK_NB); err != nil {
		if errors.Is(err, syscall.EWOULDBLOCK) || errors.Is(err, syscall.EAGAIN) {
			return true, readLockPID(f), nil
		}
		return false, 0, fmt.Errorf("failed to probe lock: %w", err)
	}

	// Lock succeeded — release immediately.
	_ = syscall.Flock(int(f.Fd()), syscall.LOCK_UN)
	return false, 0, nil
}

// readLockPID attempts to read a PID integer from the lock file content. Returns 0 on any failure.
func readLockPID(f *os.File) int {
	b := make([]byte, 32)
	n, err := f.ReadAt(b, 0)
	if err != nil && n == 0 {
		return 0
	}
	var pid int
	if _, err := fmt.Sscanf(string(b[:n]), "%d", &pid); err != nil {
		return 0
	}
	return pid
}

// Close releases the lock and closes the underlying file descriptor.
func (l *FileLock) Close() error {
	if l == nil || l.f == nil {
		return nil
	}
	_ = syscall.Flock(int(l.f.Fd()), syscall.LOCK_UN)
	err := l.f.Close()
	if err != nil {
		return fmt.Errorf("failed to close lock file: %w", err)
	}
	return nil
}
