package fuse

import (
	"errors"
	"syscall"

	"github.com/hanwen/go-fuse/v2/fs"
	"github.com/hieutdo/policyfs/internal/router"
)

const (
	utimeOmitNsec = int64((1 << 30) - 2)
)

// toErrno maps domain errors to stable errno values per spec.
func toErrno(err error) syscall.Errno {
	if err == nil {
		return 0
	}
	if errors.Is(err, router.ErrNoRuleMatched) {
		return syscall.EROFS
	}
	if errors.Is(err, router.ErrNoTargetsResolved) {
		return syscall.EROFS
	}
	if errors.Is(err, router.ErrNoWriteSpace) {
		return syscall.ENOSPC
	}
	return fs.ToErrno(err)
}
