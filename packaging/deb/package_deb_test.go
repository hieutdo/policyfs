package deb_test

import (
	"os"
	"os/exec"
	"path/filepath"
	"runtime"
	"testing"

	"github.com/stretchr/testify/require"
)

// TestPackageDebShouldInstallSystemdUnitsUnderVendorDir verifies the built Debian package ships vendor units under lib/systemd/system instead of /etc/systemd/system.
func TestPackageDebShouldInstallSystemdUnitsUnderVendorDir(t *testing.T) {
	t.Helper()

	if _, err := exec.LookPath("dpkg-deb"); err != nil {
		t.Skip("skip Debian package layout test when dpkg-deb is unavailable")
	}

	_, file, _, ok := runtime.Caller(0)
	require.True(t, ok, "expected runtime.Caller to resolve the current test file")
	repoRoot := filepath.Clean(filepath.Join(filepath.Dir(file), "..", ".."))

	outDir := t.TempDir()
	buildDir := filepath.Join(t.TempDir(), "build")
	binaryPath := filepath.Join(t.TempDir(), "pfs")
	pkgName := filepath.Join(outDir, "policyfs_9.9.9-test_amd64.deb")
	require.NoError(t, os.WriteFile(binaryPath, []byte("#!/bin/sh\nexit 0\n"), 0o755))

	cmd := exec.Command("bash", filepath.Join(repoRoot, "scripts", "package_deb.sh"))
	cmd.Dir = repoRoot
	cmd.Env = append(os.Environ(),
		"VERSION=9.9.9-test",
		"OUT_DIR="+outDir,
		"BUILD_DIR="+buildDir,
		"BINARY_PATH="+binaryPath,
	)
	output, err := cmd.CombinedOutput()
	require.NoError(t, err, "expected package_deb.sh to succeed, got output: %s", string(output))

	listCmd := exec.Command("dpkg-deb", "-c", pkgName)
	listCmd.Dir = repoRoot
	listing, err := listCmd.CombinedOutput()
	require.NoError(t, err, "expected dpkg-deb -c to succeed, got output: %s", string(listing))

	text := string(listing)
	require.Contains(t, text, "./lib/systemd/system/pfs@.service")
	require.Contains(t, text, "./lib/systemd/system/pfs-maint@.timer")
	require.NotContains(t, text, "./etc/systemd/system/pfs@.service")
	require.NotContains(t, text, "./etc/systemd/system/pfs-maint@.timer")
}
