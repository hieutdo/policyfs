package fuse

import (
	"fmt"
	"os"
	"path/filepath"
	"syscall"
)

// syncBackingFD replaces sync_syscall.go in the test-owned daemon to inject ENOSYS.
func syncBackingFD(fd int, dataOnly bool) syscall.Errno {
	if dataOnly {
		return syscall.ENOSYS
	}
	path, err := os.Readlink(fmt.Sprintf("/proc/self/fd/%d", fd))
	if err != nil {
		return toErrno(err)
	}
	if filepath.Base(path) == "unsupported" {
		return syscall.ENOSYS
	}
	return toErrno(syscall.Fsync(fd))
}
