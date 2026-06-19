//go:build integration

package integration

import (
	"os"
	"testing"
	"time"

	"github.com/hieutdo/policyfs/internal/config"
	"github.com/stretchr/testify/require"
)

// TestMutationLogs_shouldShowIndexedUnlinkLifecycle verifies debug logs cover the full indexed
// unlink lifecycle from deferred metadata delete to prune finalization.
func TestMutationLogs_shouldShowIndexedUnlinkLifecycle(t *testing.T) {
	cfg := createIndexedCfg()
	cfg.LogLevel = "debug"

	withMountedFS(t, cfg, func(env *MountedFS) {
		rel := "mutation-logs/unlink/a.txt"
		content := []byte("unlink-log-content")
		env.MustCreateFileInStoragePath(t, content, "hdd1", rel)

		mustRunPFS(t, env, "index", env.MountName)
		env.MustRemoveFileInMountPoint(t, rel)
		waitForEventsFile(t, env, 2*time.Second)

		mustRunPFS(t, env, "prune", env.MountName)

		logData := env.MustWaitForDaemonLogSequence(t, 10*time.Second, []DaemonLogExpectation{
			{Msg: "unlink updated indexed metadata", Fields: map[string]string{"op": "unlink", "path": rel, "storage_id": "hdd1"}},
			{Msg: "unlink appended deferred event", Fields: map[string]string{"op": "unlink", "path": rel, "storage_id": "hdd1"}},
			{Msg: "prune resolved delete path", Fields: map[string]string{"op": "prune", "mount": env.MountName, "storage_id": "hdd1", "path": rel, "real_path": env.StoragePath("hdd1", rel)}},
			{Msg: "prune applying unlink event", Fields: map[string]string{"op": "prune", "mount": env.MountName, "storage_id": "hdd1", "path": rel, "real_path": env.StoragePath("hdd1", rel)}},
			{Msg: "prune applied unlink on disk", Fields: map[string]string{"op": "prune", "mount": env.MountName, "storage_id": "hdd1", "path": rel, "real_path": env.StoragePath("hdd1", rel)}},
			{Msg: "prune finalized delete metadata", Fields: map[string]string{"op": "prune", "mount": env.MountName, "storage_id": "hdd1", "path": rel}},
		})
		require.NotEmpty(t, logData)
	})
}

// TestMutationLogs_shouldShowCreateLifecycle verifies debug logs cover write target selection and
// physical file creation for non-indexed CREATE.
func TestMutationLogs_shouldShowCreateLifecycle(t *testing.T) {
	withMountedFS(t, IntegrationConfig{
		LogLevel: "debug",
		RoutingRules: []config.RoutingRule{
			{Match: "mutation-logs/create/**", WriteTargets: []string{"ssd1"}, ReadTargets: []string{"ssd1"}},
			{Match: "**", WriteTargets: []string{"ssd2"}, ReadTargets: []string{"ssd2"}},
		},
	}, func(env *MountedFS) {
		rel := "mutation-logs/create/hello.txt"
		content := []byte("create-log-content")

		env.MustMkdirInMountPoint(t, "mutation-logs/create")
		env.MustWriteFileInMountPoint(t, rel, content)

		require.FileExists(t, env.StoragePath("ssd1", rel))
		require.NoFileExists(t, env.StoragePath("ssd2", rel))

		logData := env.MustWaitForDaemonLogSequence(t, 10*time.Second, []DaemonLogExpectation{
			{Msg: "create selected write target", Fields: map[string]string{"op": "create", "path": rel, "storage_id": "ssd1"}},
			{Msg: "create materialized parent dirs", Fields: map[string]string{"op": "create", "path": rel, "storage_id": "ssd1", "real_path": env.StoragePath("ssd1", rel)}},
			{Msg: "create opened physical file", Fields: map[string]string{"op": "create", "path": rel, "storage_id": "ssd1", "real_path": env.StoragePath("ssd1", rel)}},
			{Msg: "create", Fields: map[string]string{"op": "create", "path": rel, "storage_id": "ssd1", "real_path": env.StoragePath("ssd1", rel), "indexed": "false"}},
		})
		require.NotEmpty(t, logData)
	})
}

// TestMutationLogs_shouldShowMkdirLifecycle verifies debug logs cover write target selection and
// physical directory creation for non-indexed MKDIR.
func TestMutationLogs_shouldShowMkdirLifecycle(t *testing.T) {
	withMountedFS(t, IntegrationConfig{
		LogLevel: "debug",
		RoutingRules: []config.RoutingRule{
			{Match: "mutation-log-mkdir", WriteTargets: []string{"ssd2"}, ReadTargets: []string{"ssd2"}},
			{Match: "**", WriteTargets: []string{"ssd1"}, ReadTargets: []string{"ssd1"}},
		},
	}, func(env *MountedFS) {
		rel := "mutation-log-mkdir"

		require.NoError(t, os.Mkdir(env.MountPath(rel), 0o755))

		require.DirExists(t, env.StoragePath("ssd2", rel))
		require.NoDirExists(t, env.StoragePath("ssd1", rel))

		logData := env.MustWaitForDaemonLogSequence(t, 10*time.Second, []DaemonLogExpectation{
			{Msg: "mkdir selected write target", Fields: map[string]string{"op": "mkdir", "path": rel, "storage_id": "ssd2"}},
			{Msg: "mkdir materialized parent dirs", Fields: map[string]string{"op": "mkdir", "path": rel, "storage_id": "ssd2", "real_path": env.StoragePath("ssd2", rel)}},
			{Msg: "mkdir created directory on disk", Fields: map[string]string{"op": "mkdir", "path": rel, "storage_id": "ssd2", "real_path": env.StoragePath("ssd2", rel)}},
			{Msg: "mkdir", Fields: map[string]string{"op": "mkdir", "path": rel, "storage_id": "ssd2", "real_path": env.StoragePath("ssd2", rel), "indexed": "false"}},
		})
		require.NotEmpty(t, logData)
	})
}

// TestMutationLogs_shouldShowIndexedSetattrLifecycle verifies debug logs cover indexed setattr
// resolution, deferred metadata update, event append, and effective attribute refresh.
func TestMutationLogs_shouldShowIndexedSetattrLifecycle(t *testing.T) {
	cfg := createIndexedCfg()
	cfg.LogLevel = "debug"

	withMountedFS(t, cfg, func(env *MountedFS) {
		rel := "mutation-logs/setattr/file.txt"
		content := []byte("setattr-log-content")
		env.MustCreateFileInStoragePath(t, content, "hdd1", rel)

		mustRunPFS(t, env, "index", env.MountName)
		require.NoError(t, os.Chmod(env.MountPath(rel), 0o600))

		logData := env.MustWaitForDaemonLogSequence(t, 10*time.Second, []DaemonLogExpectation{
			{Msg: "setattr resolved read targets", Fields: map[string]string{"op": "setattr", "path": rel}},
			{Msg: "setattr missed on non-indexed target", Fields: map[string]string{"op": "setattr", "path": rel, "storage_id": "ssd1", "real_path": env.StoragePath("ssd1", rel)}},
			{Msg: "setattr resolved indexed file", Fields: map[string]string{"op": "setattr", "path": rel, "storage_id": "hdd1"}},
			{Msg: "setattr updated indexed metadata", Fields: map[string]string{"op": "setattr", "path": rel, "storage_id": "hdd1"}},
			{Msg: "setattr appended deferred event", Fields: map[string]string{"op": "setattr", "path": rel, "storage_id": "hdd1"}},
			{Msg: "setattr refreshed indexed attributes", Fields: map[string]string{"op": "setattr", "path": rel, "storage_id": "hdd1"}},
			{Msg: "setattr", Fields: map[string]string{"op": "setattr", "path": rel, "storage_id": "hdd1", "indexed": "true", "chmod": "true"}},
		})
		require.NotEmpty(t, logData)
	})
}

// TestMutationLogs_shouldShowLinkLifecycle verifies debug logs cover hardlink source
// resolution, destination preparation, and physical link creation.
func TestMutationLogs_shouldShowLinkLifecycle(t *testing.T) {
	withMountedFS(t, IntegrationConfig{
		LogLevel: "debug",
		RoutingRules: []config.RoutingRule{
			{Match: "mutation-logs/link/**", WriteTargets: []string{"ssd1"}, ReadTargets: []string{"ssd1"}},
			{Match: "**", WriteTargets: []string{"ssd2"}, ReadTargets: []string{"ssd2"}},
		},
	}, func(env *MountedFS) {
		srcRel := "mutation-logs/link/src.txt"
		dstRel := "mutation-logs/link/dst.txt"
		env.MustWriteFileInMountPoint(t, srcRel, []byte("link-log-content"))

		require.NoError(t, os.Link(env.MountPath(srcRel), env.MountPath(dstRel)))

		logData := env.MustWaitForDaemonLogSequence(t, 10*time.Second, []DaemonLogExpectation{
			{Msg: "link resolved source target", Fields: map[string]string{"op": "link", "old_path": srcRel, "new_path": dstRel, "storage_id": "ssd1", "real_path": env.StoragePath("ssd1", srcRel)}},
			{Msg: "link materialized destination parent", Fields: map[string]string{"op": "link", "old_path": srcRel, "new_path": dstRel, "storage_id": "ssd1", "real_path": env.StoragePath("ssd1", dstRel)}},
			{Msg: "link created hardlink on disk", Fields: map[string]string{"op": "link", "old_path": srcRel, "new_path": dstRel, "storage_id": "ssd1", "real_path": env.StoragePath("ssd1", dstRel)}},
			{Msg: "link", Fields: map[string]string{"op": "link", "old_path": srcRel, "new_path": dstRel, "storage_id": "ssd1", "real_path": env.StoragePath("ssd1", dstRel), "indexed": "false"}},
		})
		require.NotEmpty(t, logData)
	})
}

// TestMutationLogs_shouldShowSymlinkLifecycle verifies debug logs cover symbolic link
// creation while preserving the current loopback behavior.
func TestMutationLogs_shouldShowSymlinkLifecycle(t *testing.T) {
	withMountedFS(t, IntegrationConfig{LogLevel: "debug"}, func(env *MountedFS) {
		linkRel := "mutation-logs/symlink/link.txt"
		target := "target.txt"

		env.MustSymlinkInMountPoint(t, target, linkRel)
		require.Equal(t, target, env.MustReadlinkInMountPoint(t, linkRel))

		logData := env.MustWaitForDaemonLogSequence(t, 10*time.Second, []DaemonLogExpectation{
			{Msg: "symlink using loopback behavior", Fields: map[string]string{"op": "symlink", "path": linkRel, "target": target}},
			{Msg: "symlink", Fields: map[string]string{"op": "symlink", "path": linkRel, "target": target}},
		})
		require.NotEmpty(t, logData)
	})
}

// TestMutationLogs_shouldShowWritableReleaseLifecycle verifies debug logs cover releasing
// a writable handle without logging every write call.
func TestMutationLogs_shouldShowWritableReleaseLifecycle(t *testing.T) {
	withMountedFS(t, IntegrationConfig{
		LogLevel: "debug",
		RoutingRules: []config.RoutingRule{
			{Match: "mutation-logs/release/**", WriteTargets: []string{"ssd1"}, ReadTargets: []string{"ssd1"}},
			{Match: "**", WriteTargets: []string{"ssd2"}, ReadTargets: []string{"ssd2"}},
		},
	}, func(env *MountedFS) {
		rel := "mutation-logs/release/file.txt"
		env.MustWriteFileInMountPoint(t, rel, []byte("release-log-content"))

		logData := env.MustWaitForDaemonLogSequence(t, 10*time.Second, []DaemonLogExpectation{
			{Msg: "release", Fields: map[string]string{"op": "release", "path": rel, "storage_id": "ssd1", "indexed": "false", "write": "true"}},
		})
		require.NotEmpty(t, logData)
	})
}

// TestMutationLogs_shouldShowIndexedRenameFinalizeLifecycle verifies debug logs cover deferred
// indexed rename plus the prune idempotent finalize path when the physical rename already happened.
func TestMutationLogs_shouldShowIndexedRenameFinalizeLifecycle(t *testing.T) {
	cfg := createIndexedCfg()
	cfg.LogLevel = "debug"

	withMountedFS(t, cfg, func(env *MountedFS) {
		oldRel := "mutation-logs/rename/src.txt"
		newRel := "mutation-logs/rename/dst.txt"
		content := []byte("rename-log-content")
		env.MustCreateFileInStoragePath(t, content, "hdd1", oldRel)

		mustRunPFS(t, env, "index", env.MountName)
		env.MustRenameFileInMountPoint(t, oldRel, newRel)
		waitForEventsFile(t, env, 2*time.Second)

		require.NoError(t, os.Rename(env.StoragePath("hdd1", oldRel), env.StoragePath("hdd1", newRel)))
		mustRunPFS(t, env, "prune", env.MountName)

		logData := env.MustWaitForDaemonLogSequence(t, 10*time.Second, []DaemonLogExpectation{
			{Msg: "rename resolved indexed source file", Fields: map[string]string{"op": "rename", "old_path": oldRel, "storage_id": "hdd1"}},
			{Msg: "rename updated indexed metadata", Fields: map[string]string{"op": "rename", "old_path": oldRel, "new_path": newRel, "storage_id": "hdd1"}},
			{Msg: "rename appended deferred event", Fields: map[string]string{"op": "rename", "old_path": oldRel, "new_path": newRel, "storage_id": "hdd1"}},
			{Msg: "prune resolved rename source path", Fields: map[string]string{"op": "prune", "mount": env.MountName, "storage_id": "hdd1", "old_path": oldRel, "new_path": newRel, "real_path": env.StoragePath("hdd1", oldRel)}},
			{Msg: "prune rename missing on disk; finalizing metadata", Fields: map[string]string{"op": "prune", "mount": env.MountName, "storage_id": "hdd1", "old_path": oldRel, "new_path": newRel}},
			{Msg: "prune finalized rename metadata", Fields: map[string]string{"op": "prune", "mount": env.MountName, "storage_id": "hdd1", "old_path": oldRel, "new_path": newRel}},
		})
		require.NotEmpty(t, logData)
	})
}
