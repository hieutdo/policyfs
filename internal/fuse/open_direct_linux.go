//go:build linux

package fuse

import "syscall"

// openDirect represents the O_DIRECT open flag on Linux.
const openDirect = syscall.O_DIRECT
