//go:build !linux

package fuse

// openDirect represents a fallback for O_DIRECT on non-Linux platforms (e.g. macOS host IDE analysis)
// where syscall.O_DIRECT is undefined.
const openDirect = 0x4000
