package fuse

import (
	"errors"
	"fmt"
	"os"
	"syscall"
	"testing"

	"github.com/hieutdo/policyfs/internal/errkind"
	"github.com/hieutdo/policyfs/internal/router"
	"github.com/stretchr/testify/require"
)

// Test_toErrno verifies domain errors are mapped to stable syscall.Errno values.
func Test_toErrno(t *testing.T) {
	// This test pins the contract that router domain errors map to specific errno values.
	t.Run("should return 0 for nil", func(t *testing.T) {
		require.Equal(t, syscall.Errno(0), toErrno(nil))
	})

	// This pins the fix for mkdir/touch: wrapped errno values must survive fmt.Errorf("...: %w", err).
	t.Run("should unwrap wrapped syscall.Errno", func(t *testing.T) {
		err := fmt.Errorf("wrapped: %w", syscall.EACCES)
		require.Equal(t, syscall.EACCES, toErrno(err))
	})

	// This pins behavior when the underlying error is a PathError (common for os.Mkdir/os.OpenFile).
	t.Run("should unwrap wrapped os.PathError", func(t *testing.T) {
		perr := &os.PathError{Op: "mkdir", Path: "/tmp/x", Err: syscall.EPERM}
		err := fmt.Errorf("wrapped: %w", perr)
		require.Equal(t, syscall.EPERM, toErrno(err))
	})

	t.Run("should map ErrNoRuleMatched to EROFS", func(t *testing.T) {
		err := &errkind.KindError{Kind: router.ErrNoRuleMatched, Msg: "no rule"}
		require.Equal(t, syscall.EROFS, toErrno(err))
		require.True(t, errors.Is(err, router.ErrNoRuleMatched))
	})

	t.Run("should map ErrNoTargetsResolved to EROFS", func(t *testing.T) {
		err := &errkind.KindError{Kind: router.ErrNoTargetsResolved, Msg: "no targets"}
		require.Equal(t, syscall.EROFS, toErrno(err))
		require.True(t, errors.Is(err, router.ErrNoTargetsResolved))
	})

	t.Run("should map ErrNoWriteSpace to ENOSPC", func(t *testing.T) {
		err := &errkind.KindError{Kind: router.ErrNoWriteSpace, Msg: "no space"}
		require.Equal(t, syscall.ENOSPC, toErrno(err))
		require.True(t, errors.Is(err, router.ErrNoWriteSpace))
	})
}
