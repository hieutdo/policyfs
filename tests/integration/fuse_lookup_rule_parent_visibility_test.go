//go:build integration

package integration

import (
	"os/exec"
	"strings"
	"testing"

	"github.com/hieutdo/policyfs/internal/config"
	"github.com/stretchr/testify/require"
)

// createRuleParentVisibilityCfg returns a config where a parent directory is visible only because
// descendant rules can match below it, while the catch-all read target does not contain it.
func createRuleParentVisibilityCfg() IntegrationConfig {
	return IntegrationConfig{
		Storages: []IntegrationStorage{
			{ID: "ssd2", Indexed: false, BasePath: "/mnt/ssd2/pfs-integration"},
			{ID: "hdd1", Indexed: true, BasePath: "/mnt/hdd1/pfs-integration"},
		},
		RoutingRules: []config.RoutingRule{
			{Match: "library/**", ReadTargets: []string{"ssd2", "hdd1"}, WriteTargets: []string{"ssd2"}},
			{Match: "**", ReadTargets: []string{"ssd2"}, WriteTargets: []string{"ssd2"}},
		},
	}
}

// TestMixed_lookup_shouldResolveRuleParentDirectoryReturnedByReaddir verifies that a parent
// directory exposed at root by descendant routing rules can also be looked up and traversed.
func TestMixed_lookup_shouldResolveRuleParentDirectoryReturnedByReaddir(t *testing.T) {
	withMountedFS(t, createRuleParentVisibilityCfg(), func(env *MountedFS) {
		rel := "library/movies/existing/file.txt"
		content := []byte("rule-parent")
		env.MustCreateFileInStoragePath(t, content, "hdd1", rel)

		mustRunPFS(t, env, "index", env.MountName)

		rootEntries := env.MustReadDirInMountPoint(t, "")
		require.Contains(t, dirEntryNames(rootEntries), "library")

		libraryInfo := env.MustLstatInMountPoint(t, "library")
		require.True(t, libraryInfo.IsDir())

		libraryEntries := env.MustReadDirInMountPoint(t, "library")
		require.Contains(t, dirEntryNames(libraryEntries), "movies")

		moviesInfo := env.MustLstatInMountPoint(t, "library/movies")
		require.True(t, moviesInfo.IsDir())

		got := env.MustReadFileInMountPoint(t, rel)
		require.Equal(t, content, got)
	})
}

// TestMixed_ls_shouldNotShowQuestionMarksForRuleParentDirectoryReturnedByReaddir verifies the
// exact shell symptom for a parent directory surfaced by readdir via descendant rules.
func TestMixed_ls_shouldNotShowQuestionMarksForRuleParentDirectoryReturnedByReaddir(t *testing.T) {
	withMountedFS(t, createRuleParentVisibilityCfg(), func(env *MountedFS) {
		rel := "library/movies/existing/file.txt"
		env.MustCreateFileInStoragePath(t, []byte("rule-parent-ls"), "hdd1", rel)

		mustRunPFS(t, env, "index", env.MountName)

		rootEntries := env.MustReadDirInMountPoint(t, "")
		require.Contains(t, dirEntryNames(rootEntries), "library")

		cmd := exec.Command("ls", "-ld", env.MountPath("library"))
		out, err := cmd.CombinedOutput()
		require.NoError(t, err, "expected ls -ld to succeed: out=%s", string(out))
		require.NotContains(t, string(out), "?????????")
		require.NotContains(t, strings.ToLower(string(out)), "cannot access")
	})
}
