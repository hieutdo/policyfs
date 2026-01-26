package config

import "testing"

func TestFirstStoragePath(t *testing.T) {
	cfg := &Config{StoragePaths: []StoragePath{{Path: "/mnt/ssd1/media"}}}
	got, err := cfg.FirstStoragePath()
	if err != nil {
		t.Fatalf("expected nil err, got %v", err)
	}
	if got != "/mnt/ssd1/media" {
		t.Fatalf("unexpected path: %q", got)
	}
}
