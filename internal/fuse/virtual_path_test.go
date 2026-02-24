package fuse

import (
	"syscall"
	"testing"

	"github.com/stretchr/testify/require"
)

// Test_validateVirtualPath verifies fail-hard validation rejects traversal/escape sequences.
func Test_validateVirtualPath(t *testing.T) {
	t.Run("should allow empty and dot root", func(t *testing.T) {
		require.Equal(t, syscall.Errno(0), validateVirtualPath(""))
		require.Equal(t, syscall.Errno(0), validateVirtualPath("."))
	})

	t.Run("should reject absolute path", func(t *testing.T) {
		require.Equal(t, syscall.EPERM, validateVirtualPath("/etc/passwd"))
	})

	t.Run("should reject dot segments", func(t *testing.T) {
		require.Equal(t, syscall.EPERM, validateVirtualPath(".."))
		require.Equal(t, syscall.EPERM, validateVirtualPath("../a"))
		require.Equal(t, syscall.EPERM, validateVirtualPath("a/.."))
		require.Equal(t, syscall.EPERM, validateVirtualPath("a/./b"))
	})

	t.Run("should reject trailing slash", func(t *testing.T) {
		require.Equal(t, syscall.EPERM, validateVirtualPath("a/"))
	})

	t.Run("should reject double slash", func(t *testing.T) {
		require.Equal(t, syscall.EPERM, validateVirtualPath("a//b"))
	})
}

// Test_joinVirtualPath verifies join rejects invalid names and rejects/accepts parents correctly.
func Test_joinVirtualPath(t *testing.T) {
	t.Run("should join root and child", func(t *testing.T) {
		p, errno := joinVirtualPath("", "a")
		require.Equal(t, syscall.Errno(0), errno)
		require.Equal(t, "a", p)
	})

	t.Run("should join dot root and child", func(t *testing.T) {
		p, errno := joinVirtualPath(".", "a")
		require.Equal(t, syscall.Errno(0), errno)
		require.Equal(t, "a", p)
	})

	t.Run("should reject name containing slash", func(t *testing.T) {
		_, errno := joinVirtualPath("", "a/b")
		require.Equal(t, syscall.EPERM, errno)
	})

	t.Run("should reject traversal name", func(t *testing.T) {
		_, errno := joinVirtualPath("", "..")
		require.Equal(t, syscall.EPERM, errno)
	})

	t.Run("should reject invalid parent", func(t *testing.T) {
		_, errno := joinVirtualPath("a/..", "b")
		require.Equal(t, syscall.EPERM, errno)
	})
}
