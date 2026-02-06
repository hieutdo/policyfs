package indexdb

import (
	"os"
	"path/filepath"
	"testing"

	"github.com/stretchr/testify/require"
)

// Test_mountStateDir_shouldUseDefaultWhenEnvUnset verifies mountStateDir uses /var/lib/pfs when PFS_STATE_DIR is unset.
func Test_mountStateDir_shouldUseDefaultWhenEnvUnset(t *testing.T) {
	t.Setenv("PFS_STATE_DIR", "")
	require.Equal(t, filepath.Join("/var/lib/pfs", "media"), mountStateDir("media"))
}

// Test_mountStateDir_shouldUseEnvOverride verifies mountStateDir uses PFS_STATE_DIR when provided.
func Test_mountStateDir_shouldUseEnvOverride(t *testing.T) {
	t.Setenv("PFS_STATE_DIR", "/tmp/pfs-state")
	require.Equal(t, filepath.Join("/tmp/pfs-state", "media"), mountStateDir("media"))
}

// TestOpen_shouldCreateDBUnderStateDir verifies Open creates the DB under the PFS_STATE_DIR override.
func TestOpen_shouldCreateDBUnderStateDir(t *testing.T) {
	base := t.TempDir()
	t.Setenv("PFS_STATE_DIR", base)

	db, err := Open("media")
	require.NoError(t, err)
	t.Cleanup(func() { _ = db.Close() })

	want := filepath.Join(base, "media", "index.db")
	require.Equal(t, want, db.Path)
	_, err = os.Stat(want)
	require.NoError(t, err)
}
