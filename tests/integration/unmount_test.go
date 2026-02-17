//go:build integration

package integration

import (
	"os"
	"os/exec"
	"testing"
	"time"

	"github.com/stretchr/testify/require"
)

// TestUnmount_shouldDetachBusyMount verifies `pfs unmount` can detach the mount even when a file handle is open.
func TestUnmount_shouldDetachBusyMount(t *testing.T) {
	withMountedFS(t, IntegrationConfig{}, func(env *MountedFS) {
		rel := "unmount/busy.txt"
		env.MustWriteFileInMountPoint(t, rel, []byte("hello"))

		f, err := os.Open(env.MountPath(rel))
		require.NoError(t, err)
		defer func() { _ = f.Close() }()

		cmd := exec.Command(pfsBin, "--config", env.ConfigPath, "unmount", env.MountName)
		cmd.Stdout = os.Stdout
		cmd.Stderr = os.Stderr
		require.NoError(t, cmd.Run())

		deadline := time.Now().Add(5 * time.Second)
		for time.Now().Before(deadline) {
			mounted, err := isMountpointMounted(env.MountPoint)
			require.NoError(t, err)
			if !mounted {
				return
			}
			time.Sleep(100 * time.Millisecond)
		}
		t.Fatalf("expected mount to be unmounted (mountpoint=%s)", env.MountPoint)
	})
}
