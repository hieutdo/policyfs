package fuse

import "syscall"

// syncBackingFD issues the selected Linux sync syscall for an open backing file.
func syncBackingFD(fd int, dataOnly bool) syscall.Errno {
	if dataOnly {
		return toErrno(syscall.Fdatasync(fd))
	}
	return toErrno(syscall.Fsync(fd))
}
