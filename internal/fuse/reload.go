package fuse

import (
	"context"
	"fmt"
	"strings"

	"github.com/hieutdo/policyfs/internal/config"
	"github.com/hieutdo/policyfs/internal/errkind"
	"github.com/hieutdo/policyfs/internal/router"
	"github.com/rs/zerolog"
)

// parseLogLevel parses a config log level into a zerolog level.
func parseLogLevel(level string) (zerolog.Level, error) {
	lvl := strings.TrimSpace(level)
	switch lvl {
	case "debug":
		return zerolog.DebugLevel, nil
	case "info", "":
		return zerolog.InfoLevel, nil
	case "warn":
		return zerolog.WarnLevel, nil
	case "error":
		return zerolog.ErrorLevel, nil
	case "off":
		return zerolog.Disabled, nil
	default:
		return zerolog.InfoLevel, &errkind.InvalidError{Msg: fmt.Sprintf("unsupported log level: %q", lvl)}
	}
}

// Reload hot-reloads mount-scoped config in the running daemon.
func (n *Node) Reload(ctx context.Context, configPath string) (bool, error) {
	if n == nil {
		return false, &errkind.NilError{What: "node"}
	}
	if strings.TrimSpace(configPath) == "" {
		return false, &errkind.RequiredError{What: "config path"}
	}
	if n.state == nil {
		return false, &errkind.NilError{What: "runtime state"}
	}
	if n.reload == nil {
		return false, &errkind.NilError{What: "reload state"}
	}

	rootCfg, err := config.Load(configPath)
	if err != nil {
		return false, fmt.Errorf("failed to load config: %w", err)
	}
	mountCfg, err := rootCfg.Mount(n.mountName)
	if err != nil {
		return false, fmt.Errorf("failed to resolve mount config: %w", err)
	}
	primaryRootPath, err := mountCfg.FirstStoragePath()
	if err != nil {
		return false, fmt.Errorf("failed to resolve source root: %w", err)
	}

	if lvl := strings.TrimSpace(mountCfg.Log.Level); lvl != "" {
		if _, err := parseLogLevel(lvl); err != nil {
			return false, fmt.Errorf("invalid config: %w", err)
		}
	}

	// Serialize reload decisions and snapshot comparisons.
	n.reload.lock()
	defer n.reload.unlock()

	if err := n.reload.nonReloadableMismatch(rootCfg, mountCfg, primaryRootPath); err != nil {
		return false, err
	}

	next := snapshotReloadable(rootCfg.Log, mountCfg)
	if n.reload.isNoop(next) {
		return false, nil
	}

	rt, err := router.New(mountCfg)
	if err != nil {
		return false, fmt.Errorf("failed to create router: %w", err)
	}

	eff := mountCfg.EffectiveLogConfig(rootCfg.Log)
	level, err := parseLogLevel(eff.Level)
	if err != nil {
		return false, fmt.Errorf("invalid config: %w", err)
	}

	_, curLog := n.runtime()
	newLog := curLog.Level(level)

	if err := n.state.Swap(rt, newLog); err != nil {
		return false, fmt.Errorf("failed to swap runtime state: %w", err)
	}
	if n.disk != nil {
		n.disk.SetLog(newLog)
	}

	n.reload.applySnapshot(next)
	_ = ctx
	return true, nil
}
