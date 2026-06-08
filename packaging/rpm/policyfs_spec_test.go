package rpm_test

import (
	"os"
	"os/exec"
	"path/filepath"
	"strings"
	"testing"

	"github.com/stretchr/testify/require"
)

// scriptletEnv holds the fake command shims used to exercise RPM scriptlets safely.
type scriptletEnv struct {
	binDir  string
	etcDir  string
	logPath string
}

// newScriptletEnv creates an isolated PATH and temp config dir for RPM scriptlet execution tests.
func newScriptletEnv(t *testing.T) *scriptletEnv {
	t.Helper()

	rootDir := t.TempDir()
	binDir := filepath.Join(rootDir, "bin")
	etcDir := filepath.Join(rootDir, "etc", "pfs")
	logPath := filepath.Join(rootDir, "commands.log")
	require.NoError(t, os.MkdirAll(binDir, 0o755))
	require.NoError(t, os.MkdirAll(etcDir, 0o755))

	env := &scriptletEnv{binDir: binDir, etcDir: etcDir, logPath: logPath}
	env.writeExecutable(t, "systemctl", "#!/bin/sh\nprintf 'systemctl %s\\n' \"$*\" >> \"$LOG_FILE\"\n")

	return env
}

// writeExecutable writes a fake executable into the temporary PATH for a test case.
func (e *scriptletEnv) writeExecutable(t *testing.T, name string, content string) {
	t.Helper()

	path := filepath.Join(e.binDir, name)
	require.NoError(t, os.WriteFile(path, []byte(content), 0o755))
}

// commandEnv returns the environment used when executing extracted RPM scriptlets.
func (e *scriptletEnv) commandEnv() []string {
	return append(os.Environ(),
		"LOG_FILE="+e.logPath,
		"PATH="+e.binDir+string(os.PathListSeparator)+os.Getenv("PATH"),
	)
}

// logLines returns the recorded fake systemctl invocations in execution order.
func (e *scriptletEnv) logLines(t *testing.T) []string {
	t.Helper()

	data, err := os.ReadFile(e.logPath)
	if os.IsNotExist(err) {
		return nil
	}
	require.NoError(t, err)

	trimmed := strings.TrimSpace(string(data))
	if trimmed == "" {
		return nil
	}

	return strings.Split(trimmed, "\n")
}

// extractScriptlet returns the shell body for a specific RPM scriptlet section.
func extractScriptlet(t *testing.T, section string) string {
	t.Helper()

	data, err := os.ReadFile(filepath.Join(".", "policyfs.spec"))
	require.NoError(t, err)

	lines := strings.Split(string(data), "\n")
	marker := "%" + section
	start := -1
	for i, line := range lines {
		if strings.TrimSpace(line) == marker {
			start = i + 1
			break
		}
	}
	require.NotEqual(t, -1, start, "expected spec to contain section %s", marker)

	var body []string
	for _, line := range lines[start:] {
		trimmed := strings.TrimSpace(line)
		if strings.HasPrefix(trimmed, "%") {
			break
		}
		body = append(body, line)
	}

	return strings.Join(body, "\n")
}

// runScriptlet executes an extracted RPM scriptlet body under /bin/sh with a chosen $1 value.
func runScriptlet(t *testing.T, env *scriptletEnv, section string, arg string) {
	t.Helper()

	script := extractScriptlet(t, section)
	if section == "post" {
		script = strings.ReplaceAll(script, "/etc/pfs", env.etcDir)
	}

	cmd := exec.Command("/bin/sh", "-c", script, "rpm-scriptlet", arg)
	cmd.Env = env.commandEnv()
	output, err := cmd.CombinedOutput()
	require.NoError(t, err, "expected scriptlet to succeed, got output: %s", string(output))
}

// TestPostInstallShouldOnlyReloadSystemd verifies initial installs reload unit files without restarting mount daemons.
func TestPostInstallShouldOnlyReloadSystemd(t *testing.T) {
	env := newScriptletEnv(t)

	runScriptlet(t, env, "post", "1")

	require.Equal(t, []string{"systemctl daemon-reload"}, env.logLines(t))
}

// TestPostUpgradeShouldRestartActiveMountDaemons verifies RPM upgrades reload systemd and restart only active mount daemons.
func TestPostUpgradeShouldRestartActiveMountDaemons(t *testing.T) {
	env := newScriptletEnv(t)

	runScriptlet(t, env, "post", "2")

	require.Equal(t, []string{
		"systemctl daemon-reload",
		"systemctl try-restart pfs@*.service",
	}, env.logLines(t))
}

// TestPreunUpgradeShouldNotStopUnits verifies RPM upgrades do not stop running PolicyFS units in %preun.
func TestPreunUpgradeShouldNotStopUnits(t *testing.T) {
	env := newScriptletEnv(t)

	runScriptlet(t, env, "preun", "1")

	require.Empty(t, env.logLines(t))
}

// TestPreunEraseShouldStopUnits verifies RPM erase still stops PolicyFS daemon and timer units.
func TestPreunEraseShouldStopUnits(t *testing.T) {
	env := newScriptletEnv(t)

	runScriptlet(t, env, "preun", "0")

	require.Equal(t, []string{
		"systemctl stop pfs@*.service",
		"systemctl stop pfs-index@*.service",
		"systemctl stop pfs-index@*.timer",
		"systemctl stop pfs-move@*.service",
		"systemctl stop pfs-move@*.timer",
		"systemctl stop pfs-prune@*.service",
		"systemctl stop pfs-prune@*.timer",
		"systemctl stop pfs-maint@*.service",
		"systemctl stop pfs-maint@*.timer",
	}, env.logLines(t))
}
