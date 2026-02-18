package config

import (
	"os"
	"path/filepath"
	"testing"
)

func TestFirstStoragePath(t *testing.T) {
	cfg := &MountConfig{StoragePaths: []StoragePath{{Path: "/mnt/ssd1/media"}}}
	got, err := cfg.FirstStoragePath()
	if err != nil {
		t.Fatalf("expected nil err, got %v", err)
	}
	if got != "/mnt/ssd1/media" {
		t.Fatalf("unexpected path: %q", got)
	}
}

func TestLoad_shouldDefaultMoverVerifyToFalse(t *testing.T) {
	dir := t.TempDir()
	p := filepath.Join(dir, "pfs.yaml")

	content := `
mounts:
  media:
    mountpoint: "/mnt/pfs/media"
    storage_paths:
      - id: "ssd1"
        path: "/mnt/ssd1/media"
        indexed: false
      - id: "hdd1"
        path: "/mnt/hdd1/media"
        indexed: false
    routing_rules:
      - match: "**"
        targets: ["ssd1"]
    mover:
      enabled: true
      jobs:
        - name: "archive"
          trigger:
            type: manual
          source:
            paths: ["ssd1"]
            patterns: ["library/**"]
          destination:
            paths: ["hdd1"]
            policy: first_found
`

	if err := os.WriteFile(p, []byte(content), 0o644); err != nil {
		t.Fatalf("expected nil err, got %v", err)
	}

	cfg, err := Load(p)
	if err != nil {
		t.Fatalf("expected nil err, got %v", err)
	}
	m, err := cfg.Mount("media")
	if err != nil {
		t.Fatalf("expected nil err, got %v", err)
	}
	if len(m.Mover.Jobs) != 1 {
		t.Fatalf("expected 1 job, got %d", len(m.Mover.Jobs))
	}
	if m.Mover.Jobs[0].Verify == nil {
		t.Fatalf("expected verify to be defaulted, got nil")
	}
	if *m.Mover.Jobs[0].Verify != false {
		t.Fatalf("expected verify=false, got %v", *m.Mover.Jobs[0].Verify)
	}
}
