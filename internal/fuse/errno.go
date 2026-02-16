package fuse

import (
	"context"
	"errors"
	"syscall"

	"github.com/hanwen/go-fuse/v2/fs"
	"github.com/hieutdo/policyfs/internal/router"
)

const (
	utimeOmitNsec = int64((1 << 30) - 2)
)

// toErrno maps domain errors to stable errno values per spec.
//
// This function handles context cancellation errors that fs.ToErrno cannot
// convert (it only recognizes unwrapped syscall.Errno values). Without this,
// wrapped context errors like "failed to lookup: ... context canceled" are
// converted to ENOSYS ("function not implemented"), which is confusing and
// causes flaky FUSE operations under DB connection contention.
func toErrno(err error) syscall.Errno {
	if err == nil {
		return 0
	}
	if errors.Is(err, context.Canceled) {
		return syscall.EINTR
	}
	if errors.Is(err, context.DeadlineExceeded) {
		return syscall.ETIMEDOUT
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
