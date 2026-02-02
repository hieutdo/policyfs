//go:build integration

package integration

import (
	"bytes"
	"encoding/json"
	"os/exec"
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
	cmd := exec.Command(pfsBin, "--config", pfsCfg, "doctor", "media", "--json")
	var stderr bytes.Buffer
	cmd.Stderr = &stderr
	out, err := cmd.Output()
	require.NoError(t, err)
	require.Empty(t, stderr.String())

	var env doctorEnvelope
	require.NoError(t, json.Unmarshal(out, &env))
	require.Equal(t, "doctor", env.Command)
	require.True(t, env.OK)
}
