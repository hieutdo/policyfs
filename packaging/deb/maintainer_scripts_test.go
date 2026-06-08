package deb_test

import (
	"os"
	"os/exec"
	"path/filepath"
	"strings"
	"testing"

	"github.com/stretchr/testify/require"
)

// scriptEnv holds the fake filesystem and command shims used to exercise maintainer scripts safely.
type scriptEnv struct {
	binDir           string
	etcDir           string
	logPath          string
	stateDir         string
	systemdDir       string
	vendorSystemdDir string
}

// newScriptEnv creates an isolated environment so maintainer scripts can run without touching the host system.
func newScriptEnv(t *testing.T) *scriptEnv {
	t.Helper()

	rootDir := t.TempDir()
	binDir := filepath.Join(rootDir, "bin")
	etcDir := filepath.Join(rootDir, "etc", "pfs")
	stateDir := filepath.Join(rootDir, "var", "lib", "pfs")
	systemdDir := filepath.Join(rootDir, "systemd")
	vendorSystemdDir := filepath.Join(rootDir, "vendor-systemd")
	logPath := filepath.Join(rootDir, "commands.log")

	require.NoError(t, os.MkdirAll(binDir, 0o755))
	require.NoError(t, os.MkdirAll(etcDir, 0o755))
	require.NoError(t, os.MkdirAll(stateDir, 0o755))
	require.NoError(t, os.MkdirAll(systemdDir, 0o755))
	require.NoError(t, os.MkdirAll(vendorSystemdDir, 0o755))

	env := &scriptEnv{
		binDir:           binDir,
		etcDir:           etcDir,
		logPath:          logPath,
		stateDir:         stateDir,
		systemdDir:       systemdDir,
		vendorSystemdDir: vendorSystemdDir,
	}
	env.installDefaultCommands(t)

	return env
}

// installDefaultCommands installs fake system binaries so tests can assert script behavior deterministically.
func (e *scriptEnv) installDefaultCommands(t *testing.T) {
	t.Helper()

	e.writeExecutable(t, "systemctl", "#!/bin/sh\nprintf 'systemctl %s\\n' \"$*\" >> \"$LOG_FILE\"\nif [ \"${1:-}\" = \"enable\" ] && [ -n \"${2:-}\" ]; then\n  unit=\"${2}\"\n  case \"${unit}\" in\n    pfs@*.service)\n      wants_dir=\"${PFS_SYSTEMD_DIR}/multi-user.target.wants\"\n      template=\"pfs@.service\"\n      ;;\n    pfs-index@*.timer)\n      wants_dir=\"${PFS_SYSTEMD_DIR}/timers.target.wants\"\n      template=\"pfs-index@.timer\"\n      ;;\n    pfs-move@*.timer)\n      wants_dir=\"${PFS_SYSTEMD_DIR}/timers.target.wants\"\n      template=\"pfs-move@.timer\"\n      ;;\n    pfs-prune@*.timer)\n      wants_dir=\"${PFS_SYSTEMD_DIR}/timers.target.wants\"\n      template=\"pfs-prune@.timer\"\n      ;;\n    pfs-maint@*.timer)\n      wants_dir=\"${PFS_SYSTEMD_DIR}/timers.target.wants\"\n      template=\"pfs-maint@.timer\"\n      ;;\n    *)\n      exit 0\n      ;;\n  esac\n  mkdir -p \"${wants_dir}\"\n  rm -f \"${wants_dir}/${unit}\"\n  ln -s \"${PFS_VENDOR_SYSTEMD_DIR}/${template}\" \"${wants_dir}/${unit}\"\nfi\n")
	e.writeExecutable(t, "fusermount3", "#!/bin/sh\nexit 0\n")
	e.writeExecutable(t, "id", "#!/bin/sh\nif [ \"${1:-}\" = \"-u\" ]; then\n  echo 0\n  exit 0\nfi\nexec /usr/bin/id \"$@\"\n")
}

// writeExecutable writes a fake executable into the temporary PATH for a test case.
func (e *scriptEnv) writeExecutable(t *testing.T, name string, content string) {
	t.Helper()

	path := filepath.Join(e.binDir, name)
	require.NoError(t, os.WriteFile(path, []byte(content), 0o755))
}

// commandEnv returns the environment variables required to run a maintainer script against the temp dirs.
func (e *scriptEnv) commandEnv() []string {
	return append(os.Environ(),
		"LOG_FILE="+e.logPath,
		"PATH="+e.binDir+string(os.PathListSeparator)+os.Getenv("PATH"),
		"PFS_ETC_DIR="+e.etcDir,
		"PFS_STATE_DIR="+e.stateDir,
		"PFS_SYSTEMD_DIR="+e.systemdDir,
		"PFS_VENDOR_SYSTEMD_DIR="+e.vendorSystemdDir,
	)
}

// logLines returns the recorded fake command invocations in execution order.
func (e *scriptEnv) logLines(t *testing.T) []string {
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

// upgradeUnitsPath returns the transient snapshot path used during package upgrades.
func (e *scriptEnv) upgradeUnitsPath() string {
	return filepath.Join(e.stateDir, ".upgrade-enabled-units")
}

// legacyUnitPath returns the legacy /etc systemd unit location used by older Debian packages.
func (e *scriptEnv) legacyUnitPath(template string) string {
	return filepath.Join(e.systemdDir, template)
}

// vendorUnitPath returns the vendor systemd unit location used by the current Debian package.
func (e *scriptEnv) vendorUnitPath(template string) string {
	return filepath.Join(e.vendorSystemdDir, template)
}

// readUpgradeUnits returns the recorded enabled units from the transient upgrade snapshot.
func (e *scriptEnv) readUpgradeUnits(t *testing.T) []string {
	t.Helper()

	data, err := os.ReadFile(e.upgradeUnitsPath())
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

// writeUpgradeUnits seeds the transient upgrade snapshot so postinst tests can restore prior enablement.
func (e *scriptEnv) writeUpgradeUnits(t *testing.T, units []string) {
	t.Helper()

	content := strings.Join(units, "\n")
	if content != "" {
		content += "\n"
	}
	require.NoError(t, os.WriteFile(e.upgradeUnitsPath(), []byte(content), 0o644))
}

// policyfsTemplates returns the known PolicyFS systemd unit templates managed by the Debian package.
func policyfsTemplates() []string {
	return []string{
		"pfs@.service",
		"pfs-index@.service",
		"pfs-index@.timer",
		"pfs-move@.service",
		"pfs-move@.timer",
		"pfs-prune@.service",
		"pfs-prune@.timer",
		"pfs-maint@.service",
		"pfs-maint@.timer",
	}
}

// templateContent returns deterministic test content for a packaged PolicyFS unit template.
func templateContent(template string) string {
	return template + "\n"
}

// seedVendorPolicyfsTemplates writes vendor unit templates into the distro-managed directory used by the new package.
func (e *scriptEnv) seedVendorPolicyfsTemplates(t *testing.T) {
	t.Helper()

	for _, template := range policyfsTemplates() {
		require.NoError(t, os.WriteFile(e.vendorUnitPath(template), []byte(templateContent(template)), 0o644))
	}
}

// writeLegacyTemplate writes a legacy unit template under /etc/systemd/system for upgrade cleanup tests.
func (e *scriptEnv) writeLegacyTemplate(t *testing.T, template string, content string) {
	t.Helper()

	require.NoError(t, os.WriteFile(e.legacyUnitPath(template), []byte(content), 0o644))
}

// policyfsUnitTemplateForInstance maps an instance unit name back to its unit template filename.
func policyfsUnitTemplateForInstance(t *testing.T, unit string) string {
	t.Helper()

	switch {
	case strings.HasPrefix(unit, "pfs@") && strings.HasSuffix(unit, ".service"):
		return "pfs@.service"
	case strings.HasPrefix(unit, "pfs-index@") && strings.HasSuffix(unit, ".service"):
		return "pfs-index@.service"
	case strings.HasPrefix(unit, "pfs-index@") && strings.HasSuffix(unit, ".timer"):
		return "pfs-index@.timer"
	case strings.HasPrefix(unit, "pfs-move@") && strings.HasSuffix(unit, ".service"):
		return "pfs-move@.service"
	case strings.HasPrefix(unit, "pfs-move@") && strings.HasSuffix(unit, ".timer"):
		return "pfs-move@.timer"
	case strings.HasPrefix(unit, "pfs-prune@") && strings.HasSuffix(unit, ".service"):
		return "pfs-prune@.service"
	case strings.HasPrefix(unit, "pfs-prune@") && strings.HasSuffix(unit, ".timer"):
		return "pfs-prune@.timer"
	case strings.HasPrefix(unit, "pfs-maint@") && strings.HasSuffix(unit, ".service"):
		return "pfs-maint@.service"
	case strings.HasPrefix(unit, "pfs-maint@") && strings.HasSuffix(unit, ".timer"):
		return "pfs-maint@.timer"
	default:
		t.Fatalf("unexpected PolicyFS unit in test: %s", unit)
		return ""
	}
}

// enablementLinkPath returns the wants symlink path for a PolicyFS enabled unit.
func (e *scriptEnv) enablementLinkPath(t *testing.T, unit string) string {
	t.Helper()

	switch {
	case strings.HasSuffix(unit, ".service"):
		return filepath.Join(e.systemdDir, "multi-user.target.wants", unit)
	case strings.HasSuffix(unit, ".timer"):
		return filepath.Join(e.systemdDir, "timers.target.wants", unit)
	default:
		t.Fatalf("unexpected PolicyFS unit suffix in test: %s", unit)
		return ""
	}
}

// seedLegacyEnablementLink creates a wants symlink that points at the old /etc systemd template path.
func (e *scriptEnv) seedLegacyEnablementLink(t *testing.T, unit string) string {
	t.Helper()

	linkPath := e.enablementLinkPath(t, unit)
	template := policyfsUnitTemplateForInstance(t, unit)
	legacyTemplatePath := e.legacyUnitPath(template)
	if _, err := os.Stat(legacyTemplatePath); os.IsNotExist(err) {
		e.writeLegacyTemplate(t, template, templateContent(template))
	} else {
		require.NoError(t, err)
	}
	if _, err := os.Lstat(linkPath); err == nil {
		require.NoError(t, os.Remove(linkPath))
	} else {
		require.True(t, os.IsNotExist(err), "expected missing wants link before seeding: %s", linkPath)
	}
	require.NoError(t, os.MkdirAll(filepath.Dir(linkPath), 0o755))
	require.NoError(t, os.Symlink(legacyTemplatePath, linkPath))
	return linkPath
}

// seedEnabledPolicyfsUnits creates representative wants links so maintainer-script tests can observe preserved enablement.
func (e *scriptEnv) seedEnabledPolicyfsUnits(t *testing.T) []string {
	t.Helper()

	units := []string{
		"pfs@media.service",
		"pfs-index@media.timer",
		"pfs-move@media.timer",
		"pfs-prune@media.timer",
		"pfs-maint@media.timer",
	}

	paths := make([]string, 0, len(units))
	for _, unit := range units {
		paths = append(paths, e.seedLegacyEnablementLink(t, unit))
	}

	return paths
}

// requirePathsExist verifies seeded enablement links survive maintainer-script paths that must preserve enablement.
func requirePathsExist(t *testing.T, paths []string) {
	t.Helper()

	for _, path := range paths {
		_, err := os.Lstat(path)
		require.NoError(t, err, "expected path to exist: %s", path)
	}
}

// requirePathsMissing verifies cleanup paths removed the seeded enablement links.
func requirePathsMissing(t *testing.T, paths []string) {
	t.Helper()

	for _, path := range paths {
		_, err := os.Lstat(path)
		require.True(t, os.IsNotExist(err), "expected path to be removed: %s, got err=%v", path, err)
	}
}

// requireFileMissing verifies transient upgrade state or legacy packaged copies were cleaned up.
func requireFileMissing(t *testing.T, path string) {
	t.Helper()

	_, err := os.Stat(path)
	require.True(t, os.IsNotExist(err), "expected file to be removed: %s, got err=%v", path, err)
}

// requireSymlinkTarget verifies a symlink now points at the expected target after cleanup rewrites.
func requireSymlinkTarget(t *testing.T, path string, want string) {
	t.Helper()

	got, err := os.Readlink(path)
	require.NoError(t, err, "expected symlink target to be readable: %s", path)
	require.Equal(t, want, got)
}

// runScript executes a maintainer script with the fake command environment and returns its stderr output.
func runScript(t *testing.T, env *scriptEnv, scriptPath string, args ...string) string {
	t.Helper()

	cmdArgs := append([]string{scriptPath}, args...)
	cmd := exec.Command("/bin/sh", cmdArgs...)
	cmd.Env = env.commandEnv()
	output, err := cmd.CombinedOutput()
	require.NoError(t, err, "expected script to succeed, got output: %s", string(output))

	return string(output)
}

// TestPreinstUpgradeShouldSnapshotEnabledUnits verifies upgrade preinst captures enabled mount services and timers before old cleanup hooks run.
func TestPreinstUpgradeShouldSnapshotEnabledUnits(t *testing.T) {
	env := newScriptEnv(t)
	paths := env.seedEnabledPolicyfsUnits(t)

	runScript(t, env, filepath.Join(".", "preinst"), "upgrade", "1.0.6")

	requirePathsExist(t, paths)
	require.Equal(t, []string{
		"pfs-index@media.timer",
		"pfs-maint@media.timer",
		"pfs-move@media.timer",
		"pfs-prune@media.timer",
		"pfs@media.service",
	}, env.readUpgradeUnits(t))
}

// TestPreinstInstallWithOldVersionShouldSnapshotEnabledUnits verifies install-with-old-version paths preserve enablement the same way as upgrades.
func TestPreinstInstallWithOldVersionShouldSnapshotEnabledUnits(t *testing.T) {
	env := newScriptEnv(t)
	paths := env.seedEnabledPolicyfsUnits(t)

	runScript(t, env, filepath.Join(".", "preinst"), "install", "1.0.6")

	requirePathsExist(t, paths)
	require.Equal(t, []string{
		"pfs-index@media.timer",
		"pfs-maint@media.timer",
		"pfs-move@media.timer",
		"pfs-prune@media.timer",
		"pfs@media.service",
	}, env.readUpgradeUnits(t))
}

// TestPreinstShouldIgnoreSnapshotSetupFailures verifies best-effort snapshot bookkeeping never aborts the package flow.
func TestPreinstShouldIgnoreSnapshotSetupFailures(t *testing.T) {
	env := newScriptEnv(t)
	paths := env.seedEnabledPolicyfsUnits(t)
	badStatePath := filepath.Join(filepath.Dir(env.stateDir), "state-file")
	require.NoError(t, os.WriteFile(badStatePath, []byte("occupied\n"), 0o644))
	env.stateDir = badStatePath

	runScript(t, env, filepath.Join(".", "preinst"), "upgrade", "1.0.6")

	requirePathsExist(t, paths)
}

// TestPreinstAbortUpgradeShouldCleanupUpgradeState verifies abort-upgrade removes transient snapshot state instead of re-snapshotting enabled units.
func TestPreinstAbortUpgradeShouldCleanupUpgradeState(t *testing.T) {
	env := newScriptEnv(t)
	_ = env.seedEnabledPolicyfsUnits(t)
	env.writeUpgradeUnits(t, []string{"stale-unit.service"})

	runScript(t, env, filepath.Join(".", "preinst"), "abort-upgrade", "1.0.7")

	requireFileMissing(t, env.upgradeUnitsPath())
}

// TestPreinstAbortInstallShouldCleanupUpgradeState verifies abort-install also removes transient snapshot state.
func TestPreinstAbortInstallShouldCleanupUpgradeState(t *testing.T) {
	env := newScriptEnv(t)
	_ = env.seedEnabledPolicyfsUnits(t)
	env.writeUpgradeUnits(t, []string{"stale-unit.service"})

	runScript(t, env, filepath.Join(".", "preinst"), "abort-install", "1.0.7")

	requireFileMissing(t, env.upgradeUnitsPath())
}

// TestPostinstFreshInstallShouldCreateConfigAndNotRestoreUnits verifies fresh installs only reload systemd and seed the example config.
func TestPostinstFreshInstallShouldCreateConfigAndNotRestoreUnits(t *testing.T) {
	env := newScriptEnv(t)
	examplePath := filepath.Join(env.etcDir, "pfs.yaml.example")
	require.NoError(t, os.WriteFile(examplePath, []byte("mounts: []\n"), 0o644))

	stderr := runScript(t, env, filepath.Join(".", "postinst"), "configure")

	require.FileExists(t, filepath.Join(env.etcDir, "pfs.yaml"))
	require.Contains(t, stderr, "created "+env.etcDir+"/pfs.yaml from example")
	require.Equal(t, []string{"systemctl daemon-reload"}, env.logLines(t))
}

// TestPostinstConfigureShouldCleanupLegacyPackagedUnits verifies postinst removes packaged legacy /etc unit copies and rewrites stale wants links to the vendor directory.
func TestPostinstConfigureShouldCleanupLegacyPackagedUnits(t *testing.T) {
	env := newScriptEnv(t)
	env.seedVendorPolicyfsTemplates(t)
	env.writeLegacyTemplate(t, "pfs@.service", templateContent("pfs@.service"))
	env.writeLegacyTemplate(t, "pfs-maint@.timer", templateContent("pfs-maint@.timer"))
	serviceLink := env.seedLegacyEnablementLink(t, "pfs@media.service")
	timerLink := env.seedLegacyEnablementLink(t, "pfs-maint@media.timer")

	runScript(t, env, filepath.Join(".", "postinst"), "configure")

	requireFileMissing(t, env.legacyUnitPath("pfs@.service"))
	requireFileMissing(t, env.legacyUnitPath("pfs-maint@.timer"))
	requireSymlinkTarget(t, serviceLink, env.vendorUnitPath("pfs@.service"))
	requireSymlinkTarget(t, timerLink, env.vendorUnitPath("pfs-maint@.timer"))
	require.Equal(t, []string{
		"systemctl daemon-reload",
		"systemctl enable pfs@media.service",
		"systemctl enable pfs-maint@media.timer",
	}, env.logLines(t))
}

// TestPostinstConfigureShouldKeepCustomLegacyUnits verifies postinst leaves custom /etc unit overrides alone when they differ from the packaged vendor copy.
func TestPostinstConfigureShouldKeepCustomLegacyUnits(t *testing.T) {
	env := newScriptEnv(t)
	env.seedVendorPolicyfsTemplates(t)
	env.writeLegacyTemplate(t, "pfs@.service", "[Unit]\nDescription=custom override\n")
	serviceLink := env.seedLegacyEnablementLink(t, "pfs@media.service")

	runScript(t, env, filepath.Join(".", "postinst"), "configure")

	require.FileExists(t, env.legacyUnitPath("pfs@.service"))
	requireSymlinkTarget(t, serviceLink, env.legacyUnitPath("pfs@.service"))
	require.Equal(t, []string{"systemctl daemon-reload"}, env.logLines(t))
}

// TestPostinstUpgradeShouldRestoreEnabledUnits verifies upgrades re-enable and restart the specific units captured by preinst.
func TestPostinstUpgradeShouldRestoreEnabledUnits(t *testing.T) {
	env := newScriptEnv(t)
	env.writeUpgradeUnits(t, []string{
		"pfs-index@media.timer",
		"pfs-maint@media.timer",
		"pfs-move@media.timer",
		"pfs-prune@media.timer",
		"pfs@media.service",
	})

	runScript(t, env, filepath.Join(".", "postinst"), "configure", "1.0.5")

	require.Equal(t, []string{
		"systemctl daemon-reload",
		"systemctl enable pfs-index@media.timer",
		"systemctl start pfs-index@media.timer",
		"systemctl enable pfs-maint@media.timer",
		"systemctl start pfs-maint@media.timer",
		"systemctl enable pfs-move@media.timer",
		"systemctl start pfs-move@media.timer",
		"systemctl enable pfs-prune@media.timer",
		"systemctl start pfs-prune@media.timer",
		"systemctl enable pfs@media.service",
		"systemctl restart pfs@media.service",
	}, env.logLines(t))
	requireFileMissing(t, env.upgradeUnitsPath())
}

// TestPostinstAbortUpgradeShouldCleanupStaleUpgradeState verifies aborted upgrade paths discard stale snapshots instead of replaying them later.
func TestPostinstAbortUpgradeShouldCleanupStaleUpgradeState(t *testing.T) {
	env := newScriptEnv(t)
	env.writeUpgradeUnits(t, []string{"pfs@media.service"})

	runScript(t, env, filepath.Join(".", "postinst"), "abort-upgrade", "1.0.5")

	require.Equal(t, []string{"systemctl daemon-reload"}, env.logLines(t))
	requireFileMissing(t, env.upgradeUnitsPath())
}

// TestPrermUpgradeShouldNotStopUnits verifies upgrades leave currently configured PolicyFS units alone until postinst handles restore.
func TestPrermUpgradeShouldNotStopUnits(t *testing.T) {
	env := newScriptEnv(t)

	runScript(t, env, filepath.Join(".", "prerm"), "upgrade", "1.0.6")

	require.Empty(t, env.logLines(t))
}

// TestPrermFailedUpgradeShouldNotStopUnits verifies failed-upgrade paths do not tear down running units.
func TestPrermFailedUpgradeShouldNotStopUnits(t *testing.T) {
	env := newScriptEnv(t)

	runScript(t, env, filepath.Join(".", "prerm"), "failed-upgrade", "1.0.6")

	require.Empty(t, env.logLines(t))
}

// TestPrermRemoveShouldStopUnits verifies actual package removal still stops PolicyFS daemon and timer units.
func TestPrermRemoveShouldStopUnits(t *testing.T) {
	env := newScriptEnv(t)

	runScript(t, env, filepath.Join(".", "prerm"), "remove")

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

// TestPostrmUpgradeShouldNotRemoveEnablement verifies package upgrades preserve existing wants links and do not disable units.
func TestPostrmUpgradeShouldNotRemoveEnablement(t *testing.T) {
	env := newScriptEnv(t)
	paths := env.seedEnabledPolicyfsUnits(t)

	runScript(t, env, filepath.Join(".", "postrm"), "upgrade", "1.0.6")

	requirePathsExist(t, paths)
	require.Empty(t, env.logLines(t))
}

// TestPostrmAbortUpgradeShouldNotCleanupEnablement verifies aborted upgrades preserve existing wants links.
func TestPostrmAbortUpgradeShouldNotCleanupEnablement(t *testing.T) {
	env := newScriptEnv(t)
	paths := env.seedEnabledPolicyfsUnits(t)

	runScript(t, env, filepath.Join(".", "postrm"), "abort-upgrade", "1.0.6")

	requirePathsExist(t, paths)
	require.Empty(t, env.logLines(t))
}

// TestPostrmRemoveShouldCleanupEnablementLinks verifies remove deletes wants links so uninstall does not silently preserve enablement.
func TestPostrmRemoveShouldCleanupEnablementLinks(t *testing.T) {
	env := newScriptEnv(t)
	paths := env.seedEnabledPolicyfsUnits(t)
	env.writeUpgradeUnits(t, []string{"pfs@media.service"})

	runScript(t, env, filepath.Join(".", "postrm"), "remove")

	requirePathsMissing(t, paths)
	require.Equal(t, []string{"systemctl daemon-reload"}, env.logLines(t))
	requireFileMissing(t, env.upgradeUnitsPath())
}

// TestPostrmPurgeShouldCleanupUpgradeState verifies purge clears the transient upgrade snapshot and any remaining enablement links.
func TestPostrmPurgeShouldCleanupUpgradeState(t *testing.T) {
	env := newScriptEnv(t)
	paths := env.seedEnabledPolicyfsUnits(t)
	env.writeUpgradeUnits(t, []string{"pfs@media.service"})

	runScript(t, env, filepath.Join(".", "postrm"), "purge")

	requirePathsMissing(t, paths)
	require.Equal(t, []string{"systemctl daemon-reload"}, env.logLines(t))
	requireFileMissing(t, env.upgradeUnitsPath())
}

// TestPostrmRemoveShouldCleanupDirectJobServiceEnablementLinks verifies remove also deletes wants links for directly enabled maintenance services.
func TestPostrmRemoveShouldCleanupDirectJobServiceEnablementLinks(t *testing.T) {
	env := newScriptEnv(t)
	units := []string{
		"pfs-index@media.service",
		"pfs-move@media.service",
		"pfs-prune@media.service",
		"pfs-maint@media.service",
	}
	paths := make([]string, 0, len(units))
	for _, unit := range units {
		paths = append(paths, env.seedLegacyEnablementLink(t, unit))
	}

	runScript(t, env, filepath.Join(".", "postrm"), "remove")

	requirePathsMissing(t, paths)
	require.Equal(t, []string{"systemctl daemon-reload"}, env.logLines(t))
}
