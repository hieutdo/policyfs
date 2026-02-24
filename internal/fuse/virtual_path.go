package fuse

import (
	"strings"
	"syscall"
)

// validateVirtualPath rejects any virtual path that could escape the mount root.
//
// This is fail-hard: we do not try to normalize or repair; callers should return EPERM.
func validateVirtualPath(virtualPath string) syscall.Errno {
	if virtualPath == "" {
		return 0
	}
	if virtualPath == "." {
		return 0
	}
	if strings.ContainsRune(virtualPath, 0) {
		return syscall.EPERM
	}
	if strings.HasPrefix(virtualPath, "/") {
		return syscall.EPERM
	}
	if strings.HasSuffix(virtualPath, "/") {
		return syscall.EPERM
	}
	if strings.Contains(virtualPath, "//") {
		return syscall.EPERM
	}

	parts := strings.SplitSeq(virtualPath, "/")
	for part := range parts {
		if part == "" || part == "." || part == ".." {
			return syscall.EPERM
		}
	}
	return 0
}

// joinVirtualPath joins a parent virtual path and a single FUSE-provided name component.
//
// It returns EPERM if the join would produce an invalid/escaping path.
func joinVirtualPath(parentVirtualPath string, name string) (string, syscall.Errno) {
	if name == "" {
		return "", syscall.EPERM
	}
	if strings.ContainsRune(name, 0) {
		return "", syscall.EPERM
	}
	if name == "." || name == ".." {
		return "", syscall.EPERM
	}
	if strings.Contains(name, "/") {
		return "", syscall.EPERM
	}
	if strings.HasPrefix(name, "/") {
		return "", syscall.EPERM
	}

	if parentVirtualPath == "." {
		parentVirtualPath = ""
	}
	if errno := validateVirtualPath(parentVirtualPath); errno != 0 {
		return "", errno
	}

	p := name
	if parentVirtualPath != "" {
		p = parentVirtualPath + "/" + name
	}
	if errno := validateVirtualPath(p); errno != 0 {
		return "", errno
	}
	return p, 0
}
