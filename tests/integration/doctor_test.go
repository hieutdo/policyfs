//go:build integration

package integration

import (
	"bytes"
	"encoding/json"
	"os"
	"os/exec"
	"strings"
	"testing"

	"github.com/stretchr/testify/require"
)

type doctorEnvelope struct {
	Command  string `json:"command"`
	OK       bool   `json:"ok"`
	Warnings any    `json:"warnings"`
	Errors   any    `json:"errors"`
}

// TestDoctorJSONSmoke verifies `pfs doctor --json` emits valid JSON while the daemon is running.
func TestDoctorJSONSmoke(t *testing.T) {
	withMountedFS(t, IntegrationConfig{}, func(fsEnv *MountedFS) {
		cfg := fsEnv.ConfigPath
		if strings.TrimSpace(cfg) == "" {
			cfg = strings.TrimSpace(os.Getenv("PFS_INTEGRATION_CONFIG"))
		}
		if strings.TrimSpace(cfg) == "" {
			t.Skip("missing config path; set PFS_INTEGRATION_CONFIG when using an existing mount")
		}

		cmd := exec.Command(pfsBin, "--config", cfg, "doctor", fsEnv.MountName, "--json")
		var stderr bytes.Buffer
		cmd.Stderr = &stderr
		out, err := cmd.Output()
		require.NoError(t, err)
		require.Empty(t, stderr.String())

		var env doctorEnvelope
		require.NoError(t, json.Unmarshal(out, &env))
		require.Equal(t, "doctor", env.Command)
		require.True(t, env.OK)
	})
}
