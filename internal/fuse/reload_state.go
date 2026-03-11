package fuse

import (
	"fmt"
	"reflect"
	"strings"
	"sync"

	"github.com/hieutdo/policyfs/internal/config"
	"github.com/hieutdo/policyfs/internal/errkind"
)

var (
	// ErrReloadRequiresRestart indicates the requested config change cannot be applied without restarting the mount.
	ErrReloadRequiresRestart = errkind.SentinelError("reload requires restart")
)

// reloadSnapshot represents the reloadable subset we track for no-op detection.
//
// It is intended for in-memory comparisons only.
type reloadSnapshot struct {
	StorageGroups     map[string][]string
	RoutingRules      []config.RoutingRule
	EffectiveLogLevel string
}

// reloadState tracks baseline non-reloadable fields and last-applied reloadable state.
//
// It is safe for concurrent use.
type reloadState struct {
	mu sync.Mutex

	mountName string

	mountPoint      string
	primaryRootPath string
	fuseAllowOther  bool
	storagePaths    []config.StoragePath
	rootLogCfg      config.LogConfig

	last reloadSnapshot
}

// lock serializes reload decisions and state updates.
func (s *reloadState) lock() {
	if s == nil {
		return
	}
	s.mu.Lock()
}

// unlock releases the reload lock.
func (s *reloadState) unlock() {
	if s == nil {
		return
	}
	s.mu.Unlock()
}

// newReloadState constructs reloadState from the initial mount configuration.
func newReloadState(mountName string, m *config.MountConfig, primaryRootPath string, fuseAllowOther bool, rootLogCfg config.LogConfig) *reloadState {
	rootLogCfg = normalizeRootLogCfg(rootLogCfg)
	rs := &reloadState{
		mountName:       strings.TrimSpace(mountName),
		mountPoint:      "",
		primaryRootPath: strings.TrimSpace(primaryRootPath),
		fuseAllowOther:  fuseAllowOther,
		rootLogCfg:      rootLogCfg,
	}
	if m != nil {
		rs.mountPoint = strings.TrimSpace(m.MountPoint)
		rs.storagePaths = copyStoragePaths(m.StoragePaths)
		rs.last = snapshotReloadable(rootLogCfg, m)
	}
	return rs
}

// normalizeRootLogCfg returns a normalized root log config for stable comparisons.
func normalizeRootLogCfg(cfg config.LogConfig) config.LogConfig {
	cfg.Level = strings.TrimSpace(cfg.Level)
	cfg.Format = strings.TrimSpace(cfg.Format)
	cfg.File = strings.TrimSpace(cfg.File)
	if cfg.Format == "" {
		cfg.Format = "json"
	}
	return cfg
}

// copyStoragePaths makes a deep copy of a storage path slice.
func copyStoragePaths(in []config.StoragePath) []config.StoragePath {
	if in == nil {
		return nil
	}
	out := make([]config.StoragePath, len(in))
	copy(out, in)
	return out
}

// snapshotReloadable extracts the reloadable portion of config for no-op detection.
func snapshotReloadable(rootLogCfg config.LogConfig, m *config.MountConfig) reloadSnapshot {
	s := reloadSnapshot{}
	if m == nil {
		return s
	}
	if m.StorageGroups != nil {
		s.StorageGroups = make(map[string][]string, len(m.StorageGroups))
		for k, v := range m.StorageGroups {
			vv := append([]string{}, v...)
			s.StorageGroups[k] = vv
		}
	}
	s.RoutingRules = append([]config.RoutingRule{}, m.RoutingRules...)
	eff := m.EffectiveLogConfig(rootLogCfg)
	lvl := strings.TrimSpace(eff.Level)
	if lvl == "" {
		lvl = "info"
	}
	s.EffectiveLogLevel = lvl
	return s
}

// nonReloadableMismatch returns an error if new config changes require restart.
func (s *reloadState) nonReloadableMismatch(rootCfg *config.RootConfig, mountCfg *config.MountConfig, primaryRootPath string) error {
	if s == nil {
		return &errkind.NilError{What: "reload state"}
	}
	if rootCfg == nil {
		return &errkind.NilError{What: "root config"}
	}
	if mountCfg == nil {
		return &errkind.NilError{What: "mount config"}
	}

	if strings.TrimSpace(mountCfg.MountPoint) != s.mountPoint {
		return &errkind.KindError{Kind: ErrReloadRequiresRestart, Msg: "mountpoint changed"}
	}
	if strings.TrimSpace(primaryRootPath) != s.primaryRootPath {
		return &errkind.KindError{Kind: ErrReloadRequiresRestart, Msg: "source root changed"}
	}
	if rootCfg.Fuse.AllowOther != s.fuseAllowOther {
		return &errkind.KindError{Kind: ErrReloadRequiresRestart, Msg: "fuse.allow_other changed"}
	}
	if !reflect.DeepEqual(mountCfg.StoragePaths, s.storagePaths) {
		return &errkind.KindError{Kind: ErrReloadRequiresRestart, Msg: "storage_paths changed"}
	}

	nextRootLog := normalizeRootLogCfg(rootCfg.Log)
	if nextRootLog.Format != s.rootLogCfg.Format {
		return &errkind.KindError{Kind: ErrReloadRequiresRestart, Msg: "log.format changed"}
	}
	if nextRootLog.File != s.rootLogCfg.File {
		return &errkind.KindError{Kind: ErrReloadRequiresRestart, Msg: "log.file changed"}
	}
	return nil
}

// isNoop reports whether applying the snapshot would change anything.
func (s *reloadState) isNoop(next reloadSnapshot) bool {
	if s == nil {
		return true
	}
	return reflect.DeepEqual(s.last, next)
}

func (s *reloadState) changedFields(next reloadSnapshot) []string {
	if s == nil {
		return nil
	}

	prefix := fmt.Sprintf("mounts.%s.", strings.TrimSpace(s.mountName))
	fields := make([]string, 0, 3)
	if !reflect.DeepEqual(s.last.StorageGroups, next.StorageGroups) {
		fields = append(fields, prefix+"storage_groups")
	}
	if !reflect.DeepEqual(s.last.RoutingRules, next.RoutingRules) {
		fields = append(fields, prefix+"routing_rules")
	}
	if s.last.EffectiveLogLevel != next.EffectiveLogLevel {
		fields = append(fields, prefix+"log.level")
	}
	if len(fields) == 0 {
		return nil
	}
	return fields
}

// applySnapshot updates the last-applied snapshot.
func (s *reloadState) applySnapshot(next reloadSnapshot) {
	if s == nil {
		return
	}
	s.last = next
}
