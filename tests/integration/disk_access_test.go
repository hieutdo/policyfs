//go:build integration

package integration

import (
	"encoding/json"
	"os"
	"strings"
	"testing"
	"time"

	_ "github.com/mattn/go-sqlite3"
	"github.com/stretchr/testify/require"
)

// TestDiskAccess_shouldNotSpamDuringBurst verifies that a burst of opens on indexed storage
// produces helpful logs (deduped per-event logs + periodic summaries), without spamming.
func TestDiskAccess_shouldNotSpamDuringBurst(t *testing.T) {
	cfg := createIndexedCfg()
	cfg.MountArgs = []string{"--log-disk-access", "--dedup-ttl=60", "--disk-access-summary=1"}

	logPath := tmpDir + "/" + sanitizeName(t.Name()) + ".log"
	_ = os.Remove(logPath)

	withMountedFS(t, cfg, func(env *MountedFS) {
		rel := "disk-access/hello.txt"
		env.MustCreateFileInStoragePath(t, []byte("data"), "hdd1", rel)
		mustRunPFS(t, env, "index", env.MountName)

		// Simulate a burst of opens (e.g., Sonarr/Bazarr scanning), where logs should remain helpful
		// and not spammy due to dedup + periodic summary.
		for i := 0; i < 50; i++ {
			got := env.MustReadFileInMountPoint(t, rel)
			require.Equal(t, []byte("data"), got)
		}

		// Poll until:
		// - we have at least one disk_access entry for the file
		// - we have at least one disk_access_summary entry
		// - the sum of summary totals covers the access burst
		//
		// Note: summary emission can happen mid-burst under slow/covered builds, so
		// we cannot rely on a single summary line having total >= burst size.
		deadline := time.Now().Add(10 * time.Second)
		var logData []byte
		var found bool
		var foundSummary bool
		var diskAccessLines int
		var sumSummaryTotals uint64
		for time.Now().Before(deadline) {
			_ = env.MustReadFileInMountPoint(t, rel)

			b, err := os.ReadFile(logPath)
			if err != nil {
				time.Sleep(100 * time.Millisecond)
				continue
			}
			logData = b

			lines := strings.Split(strings.TrimSpace(string(logData)), "\n")
			found = false
			foundSummary = false
			diskAccessLines = 0
			sumSummaryTotals = 0
			for _, line := range lines {
				var entry map[string]interface{}
				if err := json.Unmarshal([]byte(line), &entry); err != nil {
					continue
				}
				msg, _ := entry["msg"].(string)
				switch msg {
				case "disk_access":
					storageID, _ := entry["storage_id"].(string)
					path, _ := entry["path"].(string)
					if storageID == "hdd1" && strings.Contains(path, "hello.txt") {
						diskAccessLines++
						found = true
					}
				case "disk_access_summary":
					foundSummary = true
					v, ok := entry["total"].(float64)
					if ok {
						sumSummaryTotals += uint64(v)
					}
				}
			}

			if found && foundSummary && sumSummaryTotals >= uint64(50) {
				break
			}
			time.Sleep(100 * time.Millisecond)
		}
		require.NotEmpty(t, logData, "expected daemon log file to be readable at %s", logPath)
		require.True(t, found, "expected a disk_access log entry for hdd1/hello.txt in %s", logPath)
		require.True(t, foundSummary, "expected a disk_access_summary log entry in %s", logPath)
		require.GreaterOrEqual(t, sumSummaryTotals, uint64(50), "expected summary totals to cover the access burst")
		require.LessOrEqual(t, diskAccessLines, 5, "expected dedup to prevent log spam")
	})
}
