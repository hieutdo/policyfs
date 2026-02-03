//go:build integration

package integration

import (
	"os"
	"path/filepath"
	"testing"
)

// TestMountSmoke verifies basic read/write through the mounted filesystem and that data lands in the backing storage root.
func TestMountSmoke(t *testing.T) {
	withMountedFS(t, IntegrationConfig{}, func(env *MountedFS) {
		p := filepath.Join(env.MountPoint, "smoke", "hello.txt")
		if err := os.MkdirAll(filepath.Dir(p), 0o755); err != nil {
			t.Fatalf("mkdir: %v", err)
		}

		want := []byte("hello from integration test")
		if err := os.WriteFile(p, want, 0o644); err != nil {
			t.Fatalf("write via mount: %v", err)
		}

		got, err := os.ReadFile(p)
		if err != nil {
			t.Fatalf("read via mount: %v", err)
		}
		if string(got) != string(want) {
			t.Fatalf("unexpected content: got %q want %q", got, want)
		}

		backing := filepath.Join(env.StorageRoot1, "smoke", "hello.txt")
		if _, err := os.Stat(backing); err != nil {
			t.Fatalf("expected backing file in storage root to exist: %v", err)
		}

		if err := os.Remove(p); err != nil {
			t.Fatalf("cleanup remove: %v", err)
		}
	})
}
