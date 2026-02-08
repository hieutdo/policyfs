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

	return &FileLock{path: p, f: f}, nil
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
