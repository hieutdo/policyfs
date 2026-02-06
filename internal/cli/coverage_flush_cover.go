//go:build cover

package cli

import (
	"fmt"
	"os"
	"runtime/coverage"
)

// flushCoverageIfEnabled writes out coverage data when the binary is built with coverage enabled.
func flushCoverageIfEnabled(mountName string, mountPoint string) {
	dir := os.Getenv("GOCOVERDIR")
	if dir == "" {
		return
	}

	if err := coverage.WriteMetaDir(dir); err != nil {
		fmt.Fprintln(os.Stderr, fmt.Sprintf("failed to write coverage meta for mount '%s' at %s: %v", mountName, mountPoint, err))
	}
	if err := coverage.WriteCountersDir(dir); err != nil {
		fmt.Fprintln(os.Stderr, fmt.Sprintf("failed to write coverage counters for mount '%s' at %s: %v", mountName, mountPoint, err))
	}
}
