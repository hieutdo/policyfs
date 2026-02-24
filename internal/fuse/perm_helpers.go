package fuse

import (
	"syscall"

	gofuse "github.com/hanwen/go-fuse/v2/fuse"
)

// permBitsForCaller returns the rwx bits (0-7) applicable to the caller for a given inode.
func permBitsForCaller(mode uint32, callerUID uint32, callerGID uint32, uid uint32, gid uint32) uint32 {
	mode &= 0o777
	if callerUID == uid {
		return (mode >> 6) & 0o7
	}
	if callerGID == gid {
		return (mode >> 3) & 0o7
	}
	return mode & 0o7
}

// dirWriteExecPermErrno enforces write+exec permission on a directory for the given caller.
func dirWriteExecPermErrno(caller *gofuse.Caller, dirMode uint32, dirUID uint32, dirGID uint32) syscall.Errno {
	if caller == nil {
		return syscall.EPERM
	}
	// Root bypasses permission checks.
	if caller.Uid == 0 {
		return 0
	}

	bits := permBitsForCaller(dirMode, caller.Uid, caller.Gid, dirUID, dirGID)
	// Creating/removing/renaming entries requires both write and exec on the directory.
	const want = 0o3 // w + x
	if bits&want != want {
		return syscall.EACCES
	}
	return 0
}

// stickyDirMayRemoveErrno enforces the sticky-bit ownership rules for unlink/rename within a directory.
func stickyDirMayRemoveErrno(caller *gofuse.Caller, dirMode uint32, dirUID uint32, entryUID uint32) syscall.Errno {
	if caller == nil {
		return syscall.EPERM
	}
	if caller.Uid == 0 {
		return 0
	}
	if dirMode&syscall.S_ISVTX == 0 {
		return 0
	}
	// POSIX/Linux sticky semantics: allow only root, dir owner, or entry owner.
	if caller.Uid != dirUID && caller.Uid != entryUID {
		return syscall.EPERM
	}
	return 0
}
