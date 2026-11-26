package fuse

import (
	"os"
	"slices"
	"strconv"
	"strings"
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

// callerInGroup reads the requesting Linux thread's filesystem credentials.
// The FUSE request header carries its fsuid and fsgid but omits supplementary groups.
func callerInGroup(caller *gofuse.Caller, gid uint32) (member, known bool) {
	status, err := os.ReadFile("/proc/" + strconv.FormatUint(uint64(caller.Pid), 10) + "/status")
	if err != nil {
		return false, false
	}
	var uidMatches, gidMatches, hasGroups bool
	for line := range strings.SplitSeq(string(status), "\n") {
		key, value, _ := strings.Cut(line, ":")
		switch key {
		case "Uid", "Gid":
			fields := strings.Fields(value)
			if len(fields) != 4 {
				return false, false
			}
			if key == "Uid" {
				uidMatches = fields[3] == strconv.FormatUint(uint64(caller.Uid), 10)
			} else {
				gidMatches = fields[3] == strconv.FormatUint(uint64(caller.Gid), 10)
			}
		case "Groups":
			hasGroups = true
			member = slices.Contains(strings.Fields(value), strconv.FormatUint(uint64(gid), 10))
		}
	}
	return member, uidMatches && gidMatches && hasGroups
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
	groupBits := (dirMode >> 3) & want
	if caller.Uid != dirUID && caller.Gid != dirGID && (groupBits == want) != (bits&want == want) {
		member, known := callerInGroup(caller, dirGID)
		if !known {
			return syscall.EACCES
		}
		if member {
			bits = groupBits
		}
	}
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
