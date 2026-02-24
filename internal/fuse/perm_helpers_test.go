package fuse

import (
	"syscall"
	"testing"

	gofuse "github.com/hanwen/go-fuse/v2/fuse"
	"github.com/stretchr/testify/require"
)

// Test_permBitsForCaller verifies we pick the correct permission class for the caller.
func Test_permBitsForCaller(t *testing.T) {
	t.Run("should use owner bits when uid matches", func(t *testing.T) {
		bits := permBitsForCaller(0o754, 1000, 2000, 1000, 3000)
		require.Equal(t, uint32(0o7), bits)
	})

	t.Run("should use group bits when gid matches", func(t *testing.T) {
		bits := permBitsForCaller(0o754, 1000, 2000, 3000, 2000)
		require.Equal(t, uint32(0o5), bits)
	})

	t.Run("should use other bits when neither uid nor gid matches", func(t *testing.T) {
		bits := permBitsForCaller(0o754, 1000, 2000, 3000, 4000)
		require.Equal(t, uint32(0o4), bits)
	})
}

// Test_dirWriteExecPermErrno verifies we require write+exec on directories for mutations.
func Test_dirWriteExecPermErrno(t *testing.T) {
	caller := &gofuse.Caller{Owner: gofuse.Owner{Uid: 1000, Gid: 2000}}

	t.Run("should allow root", func(t *testing.T) {
		require.Equal(t, syscall.Errno(0), dirWriteExecPermErrno(&gofuse.Caller{Owner: gofuse.Owner{Uid: 0, Gid: 0}}, 0o000, 0, 0))
	})

	t.Run("should allow when dir has write+exec for owner", func(t *testing.T) {
		errno := dirWriteExecPermErrno(caller, 0o700, 1000, 3000)
		require.Equal(t, syscall.Errno(0), errno)
	})

	t.Run("should deny when missing write", func(t *testing.T) {
		errno := dirWriteExecPermErrno(caller, 0o500, 1000, 3000)
		require.Equal(t, syscall.EACCES, errno)
	})

	t.Run("should deny when missing exec", func(t *testing.T) {
		errno := dirWriteExecPermErrno(caller, 0o600, 1000, 3000)
		require.Equal(t, syscall.EACCES, errno)
	})
}

// Test_stickyDirMayRemoveErrno verifies sticky-bit ownership semantics.
func Test_stickyDirMayRemoveErrno(t *testing.T) {
	caller := &gofuse.Caller{Owner: gofuse.Owner{Uid: 1000, Gid: 2000}}

	t.Run("should allow when sticky bit not set", func(t *testing.T) {
		require.Equal(t, syscall.Errno(0), stickyDirMayRemoveErrno(caller, 0o777, 0, 0))
	})

	t.Run("should allow dir owner", func(t *testing.T) {
		require.Equal(t, syscall.Errno(0), stickyDirMayRemoveErrno(caller, syscall.S_ISVTX|0o777, 1000, 3000))
	})

	t.Run("should allow entry owner", func(t *testing.T) {
		require.Equal(t, syscall.Errno(0), stickyDirMayRemoveErrno(caller, syscall.S_ISVTX|0o777, 3000, 1000))
	})

	t.Run("should deny unrelated user", func(t *testing.T) {
		require.Equal(t, syscall.EPERM, stickyDirMayRemoveErrno(caller, syscall.S_ISVTX|0o777, 3000, 4000))
	})
}
