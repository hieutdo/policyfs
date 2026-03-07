package cli

import (
	"errors"
	"fmt"
	"regexp"
	"strings"

	"github.com/hieutdo/policyfs/internal/config"
)

var mountNameRE = regexp.MustCompile(`^[a-zA-Z0-9][a-zA-Z0-9_-]{0,63}$`)

// usageError marks an error as a CLI usage/argument error.
type usageError struct {
	err error
}

// Error formats a usage error.
func (e *usageError) Error() string {
	return e.err.Error()
}

// Unwrap returns the underlying error.
func (e *usageError) Unwrap() error {
	return e.err
}

// isUsageError checks whether an error represents a CLI usage error.
func isUsageError(err error) bool {
	_, ok := errors.AsType[*usageError](err)
	return ok
}

// validateMountName validates a CLI mount name argument.
func validateMountName(name string) error {
	if !mountNameRE.MatchString(name) {
		return &usageError{err: errors.New("invalid mount name")}
	}
	return nil
}

// loadRootConfig loads a RootConfig from a path.
func loadRootConfig(configPath string) (*config.RootConfig, error) {
	rootCfg, err := config.Load(configPath)
	if err != nil {
		return nil, fmt.Errorf("failed to load config: %w", err)
	}
	return rootCfg, nil
}

// resolveMount selects a mount from a loaded RootConfig and returns its config plus the resolved source path.
func resolveMount(rootCfg *config.RootConfig, mountName string) (*config.MountConfig, string, error) {
	if err := validateMountName(mountName); err != nil {
		return nil, "", err
	}

	mountCfg, err := rootCfg.Mount(mountName)
	if err != nil {
		return nil, "", &usageError{err: err}
	}
	if lvl := strings.TrimSpace(mountCfg.Log.Level); lvl != "" {
		switch lvl {
		case "debug", "info", "warn", "error", "off":
			// ok
		default:
			return nil, "", fmt.Errorf("config: mount %q: log.level is invalid", mountName)
		}
	}
	if mountCfg.MountPoint == "" {
		return nil, "", fmt.Errorf("config: mount %q: mountpoint is required", mountName)
	}
	source, err := mountCfg.FirstStoragePath()
	if err != nil {
		return nil, "", fmt.Errorf("failed to resolve source path: %w", err)
	}
	return mountCfg, source, nil
}

// loadAndResolveMount loads a RootConfig from disk and resolves the given mount.
func loadAndResolveMount(configPath string, mountName string) (*config.RootConfig, *config.MountConfig, string, error) {
	if err := validateMountName(mountName); err != nil {
		return nil, nil, "", err
	}

	rootCfg, err := loadRootConfig(configPath)
	if err != nil {
		return nil, nil, "", err
	}
	mountCfg, source, err := resolveMount(rootCfg, mountName)
	if err != nil {
		return nil, nil, "", err
	}
	return rootCfg, mountCfg, source, nil
}
