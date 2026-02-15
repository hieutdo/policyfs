//go:build integration

package integration

import (
	"context"
	"database/sql"
	"os"
	"os/exec"
	"path/filepath"
	"testing"
	"time"

	"github.com/hieutdo/policyfs/internal/config"
	_ "github.com/mattn/go-sqlite3"
	"github.com/stretchr/testify/require"
)

// TestIndex_shouldPopulateDB_andMountShouldExposeIndexedEntries verifies the oneshot index job populates the DB
// and the running mount can serve directory listings/stat for indexed targets.
func TestIndex_shouldPopulateDB_andMountShouldExposeIndexedEntries(t *testing.T) {
	if os.Getenv(config.EnvIntegrationUseExistingMount) != "" {
		t.Skip("skip index flow test when using an existing mount")
	}

	cfg := IntegrationConfig{
		Storages: []IntegrationStorage{
			{ID: "ssd1", Indexed: false, BasePath: "/mnt/ssd1/pfs-integration"},
			{ID: "hdd1", Indexed: true, BasePath: "/mnt/hdd1/pfs-integration"},
		},
		Targets:     []string{"hdd1", "ssd1"},
		ReadTargets: []string{"ssd1", "hdd1"},
	}

	withMountedFS(t, cfg, func(fsEnv *MountedFS) {
		require.NotEmpty(t, fsEnv.RuntimeDir)
		require.NotEmpty(t, fsEnv.StateDir)

		content := []byte("hello-index")
		fsEnv.MustCreateFileInStoragePath(t, content, "hdd1", "library/a.txt")

		cmd := exec.Command(pfsBin, "--config", fsEnv.ConfigPath, "index", fsEnv.MountName)
		cmd.Env = append(
			os.Environ(),
			config.EnvRuntimeDir+"="+fsEnv.RuntimeDir,
			config.EnvStateDir+"="+fsEnv.StateDir,
		)
		cmd.Stdout = os.Stdout
		cmd.Stderr = os.Stderr
		require.NoError(t, cmd.Run())

		dbPath := filepath.Join(fsEnv.StateDir, fsEnv.MountName, "index.db")
		db, err := sql.Open("sqlite3", "file:"+dbPath+"?_foreign_keys=on")
		require.NoError(t, err)
		t.Cleanup(func() { _ = db.Close() })

		ctx, cancel := context.WithTimeout(context.Background(), 2*time.Second)
		defer cancel()

		var one int
		err = db.QueryRowContext(ctx, `SELECT 1 FROM files WHERE storage_id = ? AND path = ? AND deleted = 0 LIMIT 1;`, "hdd1", "library/a.txt").Scan(&one)
		require.NoError(t, err)

		deadline := time.Now().Add(2 * time.Second)
		for time.Now().Before(deadline) {
			entries, err := fsEnv.ReadDirInMountPoint("library")
			if err == nil {
				found := false
				for _, e := range entries {
					if e.Name() == "a.txt" {
						found = true
						break
					}
				}
				if found {
					break
				}
			}
			time.Sleep(50 * time.Millisecond)
		}

		entries := fsEnv.MustReadDirInMountPoint(t, "library")
		found := false
		for _, e := range entries {
			if e.Name() == "a.txt" {
				found = true
				break
			}
		}
		require.True(t, found)

		fi, err := os.Stat(fsEnv.MountPath("library/a.txt"))
		require.NoError(t, err)
		require.Equal(t, int64(len(content)), fi.Size())
	})
}
