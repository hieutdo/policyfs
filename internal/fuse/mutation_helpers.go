package fuse

import (
	"context"
	"errors"
	"fmt"
	"os"
	"path/filepath"
	"strings"
	"syscall"

	gofuse "github.com/hanwen/go-fuse/v2/fuse"
)

// materializeParentDirs ensures all parent directories of virtualPath exist on a specific target.
//
// This is like mkdir -p, but done one segment at a time so we can:
// - Inherit permissions (mode bits) from the physical parent directory.
// - Propagate setgid from the physical parent directory.
// - Preserve caller ownership (uid/gid) when the daemon runs as root.
//
// Key sharp edge:
// - On Linux, chown may clear setgid bits; we apply chmod after chown to ensure setgid sticks.
func materializeParentDirs(ctx context.Context, targetRoot string, virtualPath string) error {
	if errno := validateVirtualPath(virtualPath); errno != 0 {
		return errno
	}
	parentVirtual := filepath.Dir(virtualPath)
	if parentVirtual == "." {
		return nil
	}

	caller, callerOK := gofuse.FromContext(ctx)
	canChown := os.Getuid() == 0 && callerOK

	parts := strings.Split(parentVirtual, string(filepath.Separator))
	curVirtual := ""
	for _, part := range parts {
		if part == "" || part == "." {
			continue
		}
		if curVirtual == "" {
			curVirtual = part
		} else {
			curVirtual = filepath.Join(curVirtual, part)
		}

		curPhysical := filepath.Join(targetRoot, curVirtual)
		st := syscall.Stat_t{}
		err := syscall.Lstat(curPhysical, &st)
		if err == nil {
			if uint32(st.Mode)&syscall.S_IFMT != syscall.S_IFDIR {
				return syscall.ENOTDIR
			}
			continue
		}
		if !errors.Is(err, syscall.ENOENT) {
			return fmt.Errorf("failed to lstat path %s: %w", curPhysical, err)
		}

		parentPhysical := filepath.Join(targetRoot, filepath.Dir(curVirtual))
		pst := syscall.Stat_t{}
		if err := syscall.Lstat(parentPhysical, &pst); err != nil {
			return fmt.Errorf("failed to lstat path %s: %w", parentPhysical, err)
		}
		if uint32(pst.Mode)&syscall.S_IFMT != syscall.S_IFDIR {
			return syscall.ENOTDIR
		}

		// Inherit permissions from the physical parent directory (but let setgid propagate explicitly).
		newMode := uint32(pst.Mode) & 0o777
		parentSetgid := uint32(pst.Mode)&syscall.S_ISGID != 0
		if parentSetgid {
			newMode |= syscall.S_ISGID
		}

		if err := os.Mkdir(curPhysical, os.FileMode(newMode)); err != nil {
			return fmt.Errorf("failed to create directory %s: %w", curPhysical, err)
		}

		// NOTE: On Linux, chown may clear setgid bits; apply chmod after chown to ensure it sticks.
		if canChown {
			uid := int(caller.Uid)
			gid := int(caller.Gid)
			if parentSetgid {
				gid = int(pst.Gid)
			}
			if err := syscall.Lchown(curPhysical, uid, gid); err != nil {
				_ = syscall.Rmdir(curPhysical)
				return fmt.Errorf("failed to chown directory %s: %w", curPhysical, err)
			}
		}

		if err := syscall.Chmod(curPhysical, newMode); err != nil {
			_ = syscall.Rmdir(curPhysical)
			return fmt.Errorf("failed to chmod directory %s: %w", curPhysical, err)
		}
	}
	return nil
}

// preserveOwnerForCreate sets owner/group on a newly created file/dir based on the caller.
//
// This is an integration-friendly approximation of kernel permission behavior:
//   - If the daemon runs as root and the caller identity is available in context, we chown the newly
//     created entry to that caller.
//   - If the physical parent directory has setgid, the created entry's gid is forced to the parent's gid.
//
// For files created via open(2), prefer Fchown on the fd. For mkdir(2), fall back to Lchown on path.
func preserveOwnerForCreate(ctx context.Context, parentPhysicalPath string, fd int, path string) error {
	if os.Getuid() != 0 {
		return nil
	}
	caller, ok := gofuse.FromContext(ctx)
	if !ok {
		return nil
	}

	uid := int(caller.Uid)
	gid := int(caller.Gid)
	if parentPhysicalPath != "" {
		pst := syscall.Stat_t{}
		if err := syscall.Lstat(parentPhysicalPath, &pst); err != nil {
			return fmt.Errorf("failed to lstat path %s: %w", parentPhysicalPath, err)
		}
		if uint32(pst.Mode)&syscall.S_IFMT != syscall.S_IFDIR {
			return syscall.ENOTDIR
		}
		if uint32(pst.Mode)&syscall.S_ISGID != 0 {
			gid = int(pst.Gid)
		}
	}

	if fd >= 0 {
		if err := syscall.Fchown(fd, uid, gid); err != nil {
			return fmt.Errorf("failed to chown fd: %w", err)
		}
		return nil
	}
	if err := syscall.Lchown(path, uid, gid); err != nil {
		return fmt.Errorf("failed to chown path %s: %w", path, err)
	}
	return nil
}
