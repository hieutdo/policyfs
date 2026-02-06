package lock

import (
	"os"
	"path/filepath"
	"testing"

	"github.com/stretchr/testify/require"
)

// Test_mountLockDir_shouldUseDefaultWhenEnvUnset verifies mountLockDir uses /run/pfs when PFS_RUNTIME_DIR is unset.
func Test_mountLockDir_shouldUseDefaultWhenEnvUnset(t *testing.T) {
	t.Setenv("PFS_RUNTIME_DIR", "")
	require.Equal(t, filepath.Join("/run/pfs", "media", "locks"), mountLockDir("media"))
}

// Test_mountLockDir_shouldUseEnvOverride verifies mountLockDir uses PFS_RUNTIME_DIR when provided.
func Test_mountLockDir_shouldUseEnvOverride(t *testing.T) {
	t.Setenv("PFS_RUNTIME_DIR", "/tmp/pfs-runtime")
	require.Equal(t, filepath.Join("/tmp/pfs-runtime", "media", "locks"), mountLockDir("media"))
}

// TestAcquireMountLock_shouldCreateLockUnderRuntimeDir verifies lock files are created under the runtime dir override.
func TestAcquireMountLock_shouldCreateLockUnderRuntimeDir(t *testing.T) {
	base := t.TempDir()
	t.Setenv("PFS_RUNTIME_DIR", base)

	lk, err := AcquireMountLock("media", "job.lock")
	require.NoError(t, err)
	t.Cleanup(func() { _ = lk.Close() })

	_, err = os.Stat(filepath.Join(base, "media", "locks", "job.lock"))
	require.NoError(t, err)
}
