//go:build integration

package integration

import (
	"testing"

	"github.com/stretchr/testify/require"
)

// TestMountSmoke verifies basic read/write through the mounted filesystem and that data lands in the backing storage root.
func TestMountSmoke(t *testing.T) {
	withMountedFS(t, IntegrationConfig{}, func(env *MountedFS) {
		rel := "smoke/hello.txt"
		want := []byte("hello from integration test")
		env.MustWriteFileInMountPoint(t, rel, want)

		got := env.MustReadFileInMountPoint(t, rel)
		require.Equal(t, want, got)

		require.FileExists(t, env.StoragePath("ssd1", rel))

		env.MustRemoveFileInMountPoint(t, rel)
	})
}
