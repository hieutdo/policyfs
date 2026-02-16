package mover

import (
	"fmt"
	"syscall"
)

// usagePercent returns filesystem usage percentage (0..100) for a path using statfs.
func usagePercent(p string) (float64, error) {
	var st syscall.Statfs_t
	if err := syscall.Statfs(p, &st); err != nil {
		return 0, fmt.Errorf("failed to statfs %q: %w", p, err)
	}
	if st.Blocks == 0 {
		return 0, nil
	}
	used := float64(st.Blocks-st.Bavail) / float64(st.Blocks)
	return used * 100.0, nil
}

// freeSpaceGB returns free disk space for a filesystem path in GiB.
func freeSpaceGB(path string) (float64, error) {
	var st syscall.Statfs_t
	if err := syscall.Statfs(path, &st); err != nil {
		return 0, fmt.Errorf("failed to statfs %q: %w", path, err)
	}
	free := float64(st.Bavail) * float64(st.Bsize)
	return free / (1024.0 * 1024.0 * 1024.0), nil
}
