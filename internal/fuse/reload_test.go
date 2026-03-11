package fuse

import (
	"bytes"
	"context"
	"errors"
	"os"
	"path/filepath"
	"sync"
	"testing"

	"github.com/hieutdo/policyfs/internal/config"
	"github.com/hieutdo/policyfs/internal/errkind"
	"github.com/rs/zerolog"
	"github.com/stretchr/testify/require"
)

// TestNodeReload_noop_shouldReturnChangedFalse verifies reload is a no-op when config is unchanged.
func TestNodeReload_noop_shouldReturnChangedFalse(t *testing.T) {
	cfgDir := t.TempDir()
	cfgPath := filepath.Join(cfgDir, "pfs.yaml")

	storage1 := t.TempDir()
	storage2 := t.TempDir()

	yaml := `fuse:
  allow_other: false
log:
  level: info
  format: json
mounts:
  m1:
    mountpoint: /mnt/unused
    storage_paths:
      - id: ssd1
        path: "` + storage1 + `"
      - id: ssd2
        path: "` + storage2 + `"
    routing_rules:
      - match: "**"
        read_targets: [ssd1, ssd2]
        write_targets: [ssd1, ssd2]
        write_policy: first_found
`
	require.NoError(t, os.WriteFile(cfgPath, []byte(yaml), 0o644))

	rootCfg, err := config.Load(cfgPath)
	require.NoError(t, err)
	mountCfg, err := rootCfg.Mount("m1")
	require.NoError(t, err)
	source, err := mountCfg.FirstStoragePath()
	require.NoError(t, err)

	effLogCfg := mountCfg.EffectiveLogConfig(rootCfg.Log)
	lvl, err := parseLogLevel(effLogCfg.Level)
	require.NoError(t, err)
	baseLog := zerolog.New(ioDiscard{}).Level(lvl).With().Timestamp().Logger()

	root, err := NewRootWithReload("m1", mountCfg, source, nil, baseLog, DiskAccessConfig{}, rootCfg.Fuse.AllowOther, rootCfg.Log)
	require.NoError(t, err)
	n, ok := root.(*Node)
	require.True(t, ok)

	changed, fields, err := n.Reload(context.Background(), cfgPath)
	require.NoError(t, err)
	require.False(t, changed)
	require.Nil(t, fields)
}

// TestNodeReload_routerChange_shouldSwapRouter verifies routing rule changes swap router atomically.
func TestNodeReload_routerChange_shouldSwapRouter(t *testing.T) {
	cfgDir := t.TempDir()
	cfgPath1 := filepath.Join(cfgDir, "pfs1.yaml")
	cfgPath2 := filepath.Join(cfgDir, "pfs2.yaml")

	storage1 := t.TempDir()
	storage2 := t.TempDir()

	yaml1 := `fuse:
  allow_other: false
log:
  level: info
  format: json
mounts:
  m1:
    mountpoint: /mnt/unused
    storage_paths:
      - id: ssd1
        path: "` + storage1 + `"
      - id: ssd2
        path: "` + storage2 + `"
    routing_rules:
      - match: "**"
        read_targets: [ssd1, ssd2]
        write_targets: [ssd1, ssd2]
        write_policy: first_found
`
	yaml2 := `fuse:
  allow_other: false
log:
  level: info
  format: json
mounts:
  m1:
    mountpoint: /mnt/unused
    storage_paths:
      - id: ssd1
        path: "` + storage1 + `"
      - id: ssd2
        path: "` + storage2 + `"
    routing_rules:
      - match: "**"
        read_targets: [ssd2, ssd1]
        write_targets: [ssd2, ssd1]
        write_policy: first_found
`
	require.NoError(t, os.WriteFile(cfgPath1, []byte(yaml1), 0o644))
	require.NoError(t, os.WriteFile(cfgPath2, []byte(yaml2), 0o644))

	rootCfg, err := config.Load(cfgPath1)
	require.NoError(t, err)
	mountCfg, err := rootCfg.Mount("m1")
	require.NoError(t, err)
	source, err := mountCfg.FirstStoragePath()
	require.NoError(t, err)

	effLogCfg := mountCfg.EffectiveLogConfig(rootCfg.Log)
	lvl, err := parseLogLevel(effLogCfg.Level)
	require.NoError(t, err)
	baseLog := zerolog.New(ioDiscard{}).Level(lvl).With().Timestamp().Logger()

	root, err := NewRootWithReload("m1", mountCfg, source, nil, baseLog, DiskAccessConfig{}, rootCfg.Fuse.AllowOther, rootCfg.Log)
	require.NoError(t, err)
	n := root.(*Node)

	rtBefore, _ := n.runtime()
	targetsBefore, err := rtBefore.ResolveReadTargets("dir/file.txt")
	require.NoError(t, err)
	require.Len(t, targetsBefore, 2)
	require.Equal(t, "ssd1", targetsBefore[0].ID)

	changed, fields, err := n.Reload(context.Background(), cfgPath2)
	require.NoError(t, err)
	require.True(t, changed)
	require.Equal(t, []string{"mounts.m1.routing_rules"}, fields)

	rtAfter, _ := n.runtime()
	targetsAfter, err := rtAfter.ResolveReadTargets("dir/file.txt")
	require.NoError(t, err)
	require.Len(t, targetsAfter, 2)
	require.Equal(t, "ssd2", targetsAfter[0].ID)
}

// TestNodeReload_logLevelChange_shouldUpdateLogger verifies log level changes are applied.
func TestNodeReload_logLevelChange_shouldUpdateLogger(t *testing.T) {
	cfgDir := t.TempDir()
	cfgPath1 := filepath.Join(cfgDir, "pfs1.yaml")
	cfgPath2 := filepath.Join(cfgDir, "pfs2.yaml")

	storage1 := t.TempDir()
	storage2 := t.TempDir()

	yaml1 := `fuse:
  allow_other: false
log:
  level: debug
  format: json
mounts:
  m1:
    mountpoint: /mnt/unused
    storage_paths:
      - id: ssd1
        path: "` + storage1 + `"
      - id: ssd2
        path: "` + storage2 + `"
    routing_rules:
      - match: "**"
        targets: [ssd1]
        write_policy: first_found
`
	yaml2 := `fuse:
  allow_other: false
log:
  level: error
  format: json
mounts:
  m1:
    mountpoint: /mnt/unused
    storage_paths:
      - id: ssd1
        path: "` + storage1 + `"
      - id: ssd2
        path: "` + storage2 + `"
    routing_rules:
      - match: "**"
        targets: [ssd1]
        write_policy: first_found
`
	require.NoError(t, os.WriteFile(cfgPath1, []byte(yaml1), 0o644))
	require.NoError(t, os.WriteFile(cfgPath2, []byte(yaml2), 0o644))

	rootCfg, err := config.Load(cfgPath1)
	require.NoError(t, err)
	mountCfg, err := rootCfg.Mount("m1")
	require.NoError(t, err)
	source, err := mountCfg.FirstStoragePath()
	require.NoError(t, err)

	effLogCfg := mountCfg.EffectiveLogConfig(rootCfg.Log)
	lvl, err := parseLogLevel(effLogCfg.Level)
	require.NoError(t, err)

	var buf bytes.Buffer
	baseLog := zerolog.New(&buf).Level(lvl).With().Timestamp().Logger()

	root, err := NewRootWithReload("m1", mountCfg, source, nil, baseLog, DiskAccessConfig{}, rootCfg.Fuse.AllowOther, rootCfg.Log)
	require.NoError(t, err)
	n := root.(*Node)

	_, l1 := n.runtime()
	l1.Debug().Str("op", "test").Msg("before")
	beforeLen := buf.Len()
	require.Greater(t, beforeLen, 0)

	changed, fields, err := n.Reload(context.Background(), cfgPath2)
	require.NoError(t, err)
	require.True(t, changed)
	require.Equal(t, []string{"mounts.m1.log.level"}, fields)
	afterReloadLen := buf.Len()
	require.Greater(t, afterReloadLen, beforeLen)

	_, l2 := n.runtime()
	l2.Debug().Str("op", "test").Msg("after")
	afterLen := buf.Len()
	require.Equal(t, afterReloadLen, afterLen)
}

// TestNodeReload_nonReloadableChange_shouldFail verifies non-reloadable changes are rejected.
func TestNodeReload_nonReloadableChange_shouldFail(t *testing.T) {
	cfgDir := t.TempDir()
	cfgPath1 := filepath.Join(cfgDir, "pfs1.yaml")
	cfgPath2 := filepath.Join(cfgDir, "pfs2.yaml")

	storage1 := t.TempDir()
	storage2 := t.TempDir()

	yaml1 := `fuse:
  allow_other: false
log:
  level: info
  format: json
mounts:
  m1:
    mountpoint: /mnt/old
    storage_paths:
      - id: ssd1
        path: "` + storage1 + `"
      - id: ssd2
        path: "` + storage2 + `"
    routing_rules:
      - match: "**"
        targets: [ssd1]
        write_policy: first_found
`
	yaml2 := `fuse:
  allow_other: false
log:
  level: info
  format: json
mounts:
  m1:
    mountpoint: /mnt/new
    storage_paths:
      - id: ssd1
        path: "` + storage1 + `"
      - id: ssd2
        path: "` + storage2 + `"
    routing_rules:
      - match: "**"
        targets: [ssd1]
        write_policy: first_found
`
	require.NoError(t, os.WriteFile(cfgPath1, []byte(yaml1), 0o644))
	require.NoError(t, os.WriteFile(cfgPath2, []byte(yaml2), 0o644))

	rootCfg, err := config.Load(cfgPath1)
	require.NoError(t, err)
	mountCfg, err := rootCfg.Mount("m1")
	require.NoError(t, err)
	source, err := mountCfg.FirstStoragePath()
	require.NoError(t, err)

	effLogCfg := mountCfg.EffectiveLogConfig(rootCfg.Log)
	lvl, err := parseLogLevel(effLogCfg.Level)
	require.NoError(t, err)
	baseLog := zerolog.New(ioDiscard{}).Level(lvl).With().Timestamp().Logger()

	root, err := NewRootWithReload("m1", mountCfg, source, nil, baseLog, DiskAccessConfig{}, rootCfg.Fuse.AllowOther, rootCfg.Log)
	require.NoError(t, err)
	n := root.(*Node)

	_, _, err = n.Reload(context.Background(), cfgPath2)
	require.Error(t, err)
	require.True(t, errors.Is(err, ErrReloadRequiresRestart))
}

// TestNodeReload_concurrent_shouldSerializeSafely verifies concurrent reload calls
// serialize correctly and do not race. Run with -race to catch data races.
func TestNodeReload_concurrent_shouldSerializeSafely(t *testing.T) {
	cfgDir := t.TempDir()
	cfgPathA := filepath.Join(cfgDir, "a.yaml")
	cfgPathB := filepath.Join(cfgDir, "b.yaml")

	storage1 := t.TempDir()
	storage2 := t.TempDir()

	makeYAML := func(level string) string {
		return `fuse:
  allow_other: false
log:
  level: ` + level + `
  format: json
mounts:
  m1:
    mountpoint: /mnt/unused
    storage_paths:
      - id: ssd1
        path: "` + storage1 + `"
      - id: ssd2
        path: "` + storage2 + `"
    routing_rules:
      - match: "**"
        targets: [ssd1]
        write_policy: first_found
`
	}

	require.NoError(t, os.WriteFile(cfgPathA, []byte(makeYAML("debug")), 0o644))
	require.NoError(t, os.WriteFile(cfgPathB, []byte(makeYAML("error")), 0o644))

	rootCfg, err := config.Load(cfgPathA)
	require.NoError(t, err)
	mountCfg, err := rootCfg.Mount("m1")
	require.NoError(t, err)
	source, err := mountCfg.FirstStoragePath()
	require.NoError(t, err)

	baseLog := zerolog.New(ioDiscard{}).Level(zerolog.DebugLevel).With().Timestamp().Logger()
	root, err := NewRootWithReload("m1", mountCfg, source, nil, baseLog, DiskAccessConfig{}, rootCfg.Fuse.AllowOther, rootCfg.Log)
	require.NoError(t, err)
	n := root.(*Node)

	const goroutines = 8
	const iterations = 20

	var wg sync.WaitGroup
	wg.Add(goroutines)
	for g := range goroutines {
		g := g
		go func() {
			defer wg.Done()
			for i := range iterations {
				p := cfgPathA
				if (g+i)%2 == 1 {
					p = cfgPathB
				}
				_, _, err := n.Reload(context.Background(), p)
				if err != nil {
					t.Errorf("g=%d i=%d: %v", g, i, err)
					return
				}
			}
		}()
	}
	wg.Wait()

	// After all reloads, state must still be valid.
	rt, log := n.runtime()
	require.NotNil(t, rt)
	log.Info().Msg("post-concurrent-reload")
}

// TestNodeReload_nonReloadableFields_shouldRejectAll verifies all non-reloadable field
// changes are rejected with ErrReloadRequiresRestart.
func TestNodeReload_nonReloadableFields_shouldRejectAll(t *testing.T) {
	storage1 := t.TempDir()
	storage2 := t.TempDir()
	storage3 := t.TempDir()

	makeYAML := func(mp, sp0, allowOther, extraSP, format, file string) string {
		y := "fuse:\n  allow_other: " + allowOther + "\n"
		y += "log:\n  level: info\n  format: " + format + "\n"
		if file != "" {
			y += "  file: " + file + "\n"
		}
		y += "mounts:\n  m1:\n    mountpoint: " + mp + "\n"
		y += "    storage_paths:\n"
		y += "      - id: ssd1\n        path: \"" + sp0 + "\"\n"
		y += "      - id: ssd2\n        path: \"" + storage2 + "\"\n"
		y += extraSP
		y += "    routing_rules:\n"
		y += "      - match: \"**\"\n        targets: [ssd1]\n        write_policy: first_found\n"
		return y
	}

	baseMP := "/mnt/unchanged"
	baseSP0 := storage1
	baseAllowOther := "false"
	baseFormat := "json"
	baseFile := ""

	baseConfig := makeYAML(baseMP, baseSP0, baseAllowOther, "", baseFormat, baseFile)

	tests := []struct {
		name string
		yaml string
	}{
		{
			name: "mountpoint changed",
			yaml: makeYAML("/mnt/different", baseSP0, baseAllowOther, "", baseFormat, baseFile),
		},
		{
			name: "source root changed",
			yaml: makeYAML(baseMP, storage3, baseAllowOther, "", baseFormat, baseFile),
		},
		{
			name: "fuse.allow_other changed",
			yaml: makeYAML(baseMP, baseSP0, "true", "", baseFormat, baseFile),
		},
		{
			name: "storage_paths changed",
			yaml: makeYAML(baseMP, baseSP0, baseAllowOther,
				"      - id: ssd3\n        path: \""+storage3+"\"\n",
				baseFormat, baseFile),
		},
		{
			name: "log.format changed",
			yaml: makeYAML(baseMP, baseSP0, baseAllowOther, "", "text", baseFile),
		},
		{
			name: "log.file changed",
			yaml: makeYAML(baseMP, baseSP0, baseAllowOther, "", baseFormat, "/tmp/pfs-test.log"),
		},
	}

	for _, tc := range tests {
		t.Run(tc.name, func(t *testing.T) {
			cfgDir := t.TempDir()
			cfgPath1 := filepath.Join(cfgDir, "base.yaml")
			cfgPath2 := filepath.Join(cfgDir, "changed.yaml")

			require.NoError(t, os.WriteFile(cfgPath1, []byte(baseConfig), 0o644))
			require.NoError(t, os.WriteFile(cfgPath2, []byte(tc.yaml), 0o644))

			rootCfg, err := config.Load(cfgPath1)
			require.NoError(t, err)
			mountCfg, err := rootCfg.Mount("m1")
			require.NoError(t, err)
			source, err := mountCfg.FirstStoragePath()
			require.NoError(t, err)

			baseLog := zerolog.New(ioDiscard{}).Level(zerolog.InfoLevel).With().Timestamp().Logger()
			root, err := NewRootWithReload("m1", mountCfg, source, nil, baseLog, DiskAccessConfig{}, rootCfg.Fuse.AllowOther, rootCfg.Log)
			require.NoError(t, err)
			n := root.(*Node)

			_, _, err = n.Reload(context.Background(), cfgPath2)
			require.Error(t, err)
			require.True(t, errors.Is(err, ErrReloadRequiresRestart), "expected ErrReloadRequiresRestart, got: %v", err)
		})
	}
}

// TestNodeReload_routerFailure_shouldPreserveOldState verifies that a failed router
// construction during reload does not corrupt the running state.
func TestNodeReload_routerFailure_shouldPreserveOldState(t *testing.T) {
	cfgDir := t.TempDir()
	cfgPath1 := filepath.Join(cfgDir, "valid.yaml")
	cfgPath2 := filepath.Join(cfgDir, "bad-router.yaml")

	storage1 := t.TempDir()
	storage2 := t.TempDir()

	validYAML := `fuse:
  allow_other: false
log:
  level: info
  format: json
mounts:
  m1:
    mountpoint: /mnt/unused
    storage_paths:
      - id: ssd1
        path: "` + storage1 + `"
      - id: ssd2
        path: "` + storage2 + `"
    routing_rules:
      - match: "**"
        targets: [ssd1]
        write_policy: first_found
`
	// Routing rules without a catch-all: may pass config.Load but fails router.New.
	badRouterYAML := `fuse:
  allow_other: false
log:
  level: info
  format: json
mounts:
  m1:
    mountpoint: /mnt/unused
    storage_paths:
      - id: ssd1
        path: "` + storage1 + `"
      - id: ssd2
        path: "` + storage2 + `"
    routing_rules:
      - match: "*.txt"
        targets: [ssd1]
        write_policy: first_found
`
	require.NoError(t, os.WriteFile(cfgPath1, []byte(validYAML), 0o644))
	require.NoError(t, os.WriteFile(cfgPath2, []byte(badRouterYAML), 0o644))

	rootCfg, err := config.Load(cfgPath1)
	require.NoError(t, err)
	mountCfg, err := rootCfg.Mount("m1")
	require.NoError(t, err)
	source, err := mountCfg.FirstStoragePath()
	require.NoError(t, err)

	baseLog := zerolog.New(ioDiscard{}).Level(zerolog.InfoLevel).With().Timestamp().Logger()
	root, err := NewRootWithReload("m1", mountCfg, source, nil, baseLog, DiskAccessConfig{}, rootCfg.Fuse.AllowOther, rootCfg.Log)
	require.NoError(t, err)
	n := root.(*Node)

	// Capture state before failed reload.
	rtBefore, _ := n.runtime()
	require.NotNil(t, rtBefore)

	// Attempt reload with bad routing config.
	_, _, err = n.Reload(context.Background(), cfgPath2)
	require.Error(t, err)
	require.True(t, errors.Is(err, errkind.ErrRequired), "expected required error kind, got: %v", err)

	// State must be unchanged after failed reload.
	rtAfter, _ := n.runtime()
	require.Same(t, rtBefore, rtAfter, "router pointer should be unchanged after failed reload")

	// A subsequent valid reload must still work.
	changed, _, err := n.Reload(context.Background(), cfgPath1)
	require.NoError(t, err)
	require.False(t, changed, "reload with same config should be a no-op")
}

// ioDiscard is a small io.Writer that drops writes (avoids importing io in every test).
//
// It exists so we can create zerolog loggers without depending on stdout/stderr.
type ioDiscard struct{}

// Write discards the provided bytes.
func (ioDiscard) Write(p []byte) (int, error) { return len(p), nil }
