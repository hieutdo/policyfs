//go:build integration

package integration

import (
	"bytes"
	"os"
	"os/exec"
	"strings"
	"testing"

	"github.com/hieutdo/policyfs/internal/config"
	"github.com/stretchr/testify/require"
)

// TestDoctorSmoke verifies `pfs doctor` runs successfully while the daemon is running.
func TestDoctorSmoke(t *testing.T) {
	withMountedFS(t, IntegrationConfig{}, func(fsEnv *MountedFS) {
		cfg := fsEnv.ConfigPath
		if strings.TrimSpace(cfg) == "" {
			cfg = strings.TrimSpace(os.Getenv(config.EnvIntegrationConfig))
		}
		if strings.TrimSpace(cfg) == "" {
			t.Skip("missing config path; set " + config.EnvIntegrationConfig + " when using an existing mount")
		}

		cmd := exec.Command(pfsBin, "--config", cfg, "doctor", fsEnv.MountName)
		var stderr bytes.Buffer
		cmd.Stderr = &stderr
		out, err := cmd.Output()
		require.NoError(t, err)
		require.Empty(t, stderr.String())
		require.NotEmpty(t, strings.TrimSpace(string(out)))
		require.Contains(t, string(out), "Mount:")
	})
}
