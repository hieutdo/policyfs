//go:build integration

package integration

import (
	"os"
	"testing"
	"time"

	"github.com/hieutdo/policyfs/internal/config"
	"github.com/stretchr/testify/require"
)

// TestLookupLogs_shouldShowListFallbackForPartitionBoundaryDirectory verifies that the daemon log
// captures the exact readdir -> lookup fallback sequence for the issue-5 partition-boundary case.
func TestLookupLogs_shouldShowListFallbackForPartitionBoundaryDirectory(t *testing.T) {
	cfg := IntegrationConfig{
		LogLevel: "debug",
		Storages: []IntegrationStorage{
			{ID: "ssd2", Indexed: false, BasePath: "/mnt/ssd2/pfs-integration"},
			{ID: "hdd1", Indexed: true, BasePath: "/mnt/hdd1/pfs-integration"},
		},
		RoutingRules: []config.RoutingRule{
			{Match: "library/movies/**", ReadTargets: []string{"hdd1"}, WriteTargets: []string{"hdd1"}},
			{Match: "**", ReadTargets: []string{"ssd2"}, WriteTargets: []string{"ssd2"}},
		},
	}

	withMountedFS(t, cfg, func(env *MountedFS) {
		const rel = "library/movies/existing/file.txt"
		content := []byte("partition-boundary-log")
		env.MustCreateFileInStoragePath(t, content, "hdd1", rel)

		mustRunPFS(t, env, "index", env.MountName)

		rootEntries := env.MustReadDirInMountPoint(t, "")
		require.Contains(t, dirEntryNames(rootEntries), "library")

		libraryInfo := env.MustLstatInMountPoint(t, "library")
		require.True(t, libraryInfo.IsDir())

		logData := env.MustWaitForDaemonLogSequence(t, 10*time.Second, []DaemonLogExpectation{
			{Msg: "readdir added entry", Fields: map[string]string{"op": "readdir", "path": "library"}},
			{Msg: "lookup missed on non-indexed target", Fields: map[string]string{"op": "lookup", "path": "library", "storage_id": "ssd2"}},
			{Msg: "lookup read targets exhausted; trying list fallback", Fields: map[string]string{"op": "lookup", "path": "library"}},
			{Msg: "lookup resolved on list fallback", Fields: map[string]string{"op": "lookup", "path": "library", "storage_id": "hdd1"}},
		})
		require.NotEmpty(t, logData)
	})
}

// TestOpenLogs_shouldShowStaleRealPathFallback verifies that the daemon log captures indexed open
// fallback when the DB still points at an old real_path but the file has already moved physically.
func TestOpenLogs_shouldShowStaleRealPathFallback(t *testing.T) {
	cfg := createIndexedCfg()
	cfg.LogLevel = "debug"

	withMountedFS(t, cfg, func(env *MountedFS) {
		oldRel := "open-logs/stale-real-path/src.txt"
		newRel := "open-logs/stale-real-path/dst.txt"
		content := []byte("stale-real-path-fallback")
		env.MustCreateFileInStoragePath(t, content, "hdd1", oldRel)

		mustRunPFS(t, env, "index", env.MountName)
		env.MustRenameFileInMountPoint(t, oldRel, newRel)

		require.NoError(t, os.Rename(env.StoragePath("hdd1", oldRel), env.StoragePath("hdd1", newRel)))

		got := env.MustReadFileInMountPoint(t, newRel)
		require.Equal(t, content, got)

		logData := env.MustWaitForDaemonLogSequence(t, 10*time.Second, []DaemonLogExpectation{
			{Msg: "open resolved indexed real path", Fields: map[string]string{"op": "open", "path": newRel, "storage_id": "hdd1", "real_path": oldRel}},
			{Msg: "open trying stale real_path fallback", Fields: map[string]string{"op": "open", "path": newRel, "storage_id": "hdd1", "real_path": env.StoragePath("hdd1", oldRel)}},
			{Msg: "open resolved on stale real_path fallback", Fields: map[string]string{"op": "open", "path": newRel, "storage_id": "hdd1", "real_path": env.StoragePath("hdd1", newRel)}},
		})
		require.NotEmpty(t, logData)
	})
}
