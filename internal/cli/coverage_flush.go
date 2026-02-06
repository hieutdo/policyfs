//go:build !cover

package cli

// flushCoverageIfEnabled writes out coverage data when the binary is built with coverage enabled.
func flushCoverageIfEnabled(mountName string, mountPoint string) {
	_ = mountName
	_ = mountPoint
}
