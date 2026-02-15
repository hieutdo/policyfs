package lock

import (
	"bufio"
	"errors"
	"fmt"
	"io"
	"os"
	"os/exec"
	"path/filepath"
	"testing"
	"time"

	"github.com/hieutdo/policyfs/internal/config"
	"github.com/hieutdo/policyfs/internal/errkind"
	"github.com/stretchr/testify/require"
)

// Test_mountLockDir_shouldUseDefaultWhenEnvUnset verifies mountLockDir uses /run/pfs when PFS_RUNTIME_DIR is unset.
func Test_mountLockDir_shouldUseDefaultWhenEnvUnset(t *testing.T) {
	t.Setenv(config.EnvRuntimeDir, "")
	require.Equal(t, filepath.Join("/run/pfs", "media", "locks"), mountLockDir("media"))
}

// Test_mountLockDir_shouldUseEnvOverride verifies mountLockDir uses PFS_RUNTIME_DIR when provided.
func Test_mountLockDir_shouldUseEnvOverride(t *testing.T) {
	t.Setenv(config.EnvRuntimeDir, "/tmp/pfs-runtime")
	require.Equal(t, filepath.Join("/tmp/pfs-runtime", "media", "locks"), mountLockDir("media"))
}

// TestAcquireMountLock_shouldCreateLockUnderRuntimeDir verifies lock files are created under the runtime dir override.
func TestAcquireMountLock_shouldCreateLockUnderRuntimeDir(t *testing.T) {
	base := t.TempDir()
	t.Setenv(config.EnvRuntimeDir, base)

	lk, err := AcquireMountLock("media", "job.lock")
	require.NoError(t, err)
	t.Cleanup(func() { _ = lk.Close() })

	_, err = os.Stat(filepath.Join(base, "media", "locks", "job.lock"))
	require.NoError(t, err)
}

// TestAcquireMountLock_shouldMatchErrBusyWhenContended verifies AcquireMountLock returns an error
// that matches errors.Is(err, errkind.ErrBusy) when another process holds the lock.
func TestAcquireMountLock_shouldMatchErrBusyWhenContended(t *testing.T) {
	base := t.TempDir()
	t.Setenv(config.EnvRuntimeDir, base)

	cmd := exec.Command(os.Args[0], "-test.run=TestHelperProcessAcquireMountLockHold")
	cmd.Env = append(
		os.Environ(),
		config.EnvTestHelper+"=1",
		config.EnvRuntimeDir+"="+base,
		config.EnvTestMount+"=media",
		config.EnvTestLockFile+"=job.lock",
	)
	cmd.Stderr = os.Stderr

	stdout, err := cmd.StdoutPipe()
	require.NoError(t, err)
	stdin, err := cmd.StdinPipe()
	require.NoError(t, err)

	require.NoError(t, cmd.Start())
	waited := false
	t.Cleanup(func() {
		_ = stdin.Close()
		if !waited {
			_ = cmd.Process.Kill()
			_ = cmd.Wait()
		}
	})

	r := bufio.NewReader(stdout)
	line, err := r.ReadString('\n')
	require.NoError(t, err)
	require.Equal(t, "ready\n", line)

	lk, err := AcquireMountLock("media", "job.lock")
	require.Error(t, err)
	require.Nil(t, lk)
	require.True(t, errors.Is(err, errkind.ErrBusy))

	require.NoError(t, stdin.Close())
	require.NoError(t, cmd.Wait())
	waited = true

	deadline := time.Now().Add(2 * time.Second)
	for {
		lk2, err := AcquireMountLock("media", "job.lock")
		if err == nil {
			require.NoError(t, lk2.Close())
			break
		}
		if time.Now().After(deadline) {
			t.Fatalf("expected lock to be released, got err=%v", err)
		}
		time.Sleep(10 * time.Millisecond)
	}
}

// TestHelperProcessAcquireMountLockHold is a helper test entrypoint used to create real
// cross-process lock contention for TestAcquireMountLock_shouldMatchErrBusyWhenContended.
func TestHelperProcessAcquireMountLockHold(t *testing.T) {
	if os.Getenv(config.EnvTestHelper) != "1" {
		return
	}

	mountName := os.Getenv(config.EnvTestMount)
	lockFile := os.Getenv(config.EnvTestLockFile)

	lk, err := AcquireMountLock(mountName, lockFile)
	if err != nil {
		_, _ = fmt.Fprintf(os.Stderr, "failed to acquire helper lock: %v\n", err)
		os.Exit(1)
	}

	_, _ = fmt.Fprintln(os.Stdout, "ready")
	_, _ = io.ReadAll(os.Stdin)
	_ = lk.Close()
	os.Exit(0)
}
