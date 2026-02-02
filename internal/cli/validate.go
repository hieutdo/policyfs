package cli

import (
	"errors"
	"fmt"
	"regexp"
	"sort"

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
	var ue *usageError
	return errors.As(err, &ue)
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

// validateConfigAll runs doctor-style config validation and returns all discovered errors.
func validateConfigAll(c *config.RootConfig) []error {
	errList := []error{}
	if c == nil {
		return []error{errors.New("config is nil")}
	}
	if len(c.Mounts) == 0 {
		return []error{errors.New("config: mounts is required")}
	}

	mountNames := make([]string, 0, len(c.Mounts))
	for name := range c.Mounts {
		mountNames = append(mountNames, name)
	}
	sort.Strings(mountNames)

	for _, mountName := range mountNames {
		m := c.Mounts[mountName]
		if mountName == "" {
			errList = append(errList, errors.New("config: mount name must not be empty"))
		}
		if m.MountPoint == "" {
			errList = append(errList, fmt.Errorf("config: mount %q: mountpoint is required", mountName))
		}
		if len(m.StoragePaths) == 0 {
			errList = append(errList, fmt.Errorf("config: mount %q: storage_paths must not be empty", mountName))
		}
		if len(m.RoutingRules) == 0 {
			errList = append(errList, fmt.Errorf("config: mount %q: routing_rules must not be empty", mountName))
		}
		if len(m.RoutingRules) > 0 {
			foundCatchAll := false
			for i, r := range m.RoutingRules {
				if r.Match == "" {
					errList = append(errList, fmt.Errorf("config: mount %q: routing_rules[%d].match is required", mountName, i))
				}
				if r.Match == "**" {
					foundCatchAll = true
					if i != len(m.RoutingRules)-1 {
						errList = append(errList, errors.New("catch-all rule must be last"))
					}
				}
			}
			if !foundCatchAll {
				errList = append(errList, errors.New("missing catch-all rule '**'"))
			}
		}

		storageIDs := map[string]struct{}{}
		for i, sp := range m.StoragePaths {
			if sp.ID == "" {
				errList = append(errList, fmt.Errorf("config: mount %q: storage_paths[%d].id is required", mountName, i))
				continue
			}
			if _, ok := storageIDs[sp.ID]; ok {
				errList = append(errList, fmt.Errorf("config: mount %q: duplicate storage_paths id %q", mountName, sp.ID))
				continue
			}
			storageIDs[sp.ID] = struct{}{}
			if sp.Path == "" {
				errList = append(errList, fmt.Errorf("config: mount %q: storage_paths[%d].path is required", mountName, i))
			}
		}

		groupIDs := map[string]struct{}{}
		groupNames := make([]string, 0, len(m.StorageGroups))
		for g := range m.StorageGroups {
			groupNames = append(groupNames, g)
		}
		sort.Strings(groupNames)
		for _, g := range groupNames {
			members := m.StorageGroups[g]
			if g == "" {
				errList = append(errList, fmt.Errorf("config: mount %q: storage_groups name must not be empty", mountName))
				continue
			}
			groupIDs[g] = struct{}{}
			for _, sid := range members {
				if _, ok := storageIDs[sid]; !ok {
					errList = append(errList, fmt.Errorf("config: mount %q: storage_groups %q references unknown storage id %q", mountName, g, sid))
				}
			}
		}

		isKnownTarget := func(t string) bool {
			if t == "" {
				return false
			}
			if _, ok := storageIDs[t]; ok {
				return true
			}
			if _, ok := groupIDs[t]; ok {
				return true
			}
			return false
		}

		for i, r := range m.RoutingRules {
			for _, t := range r.Targets {
				if !isKnownTarget(t) {
					errList = append(errList, fmt.Errorf("config: mount %q: routing_rules[%d].targets references unknown id %q", mountName, i, t))
				}
			}
			for _, t := range r.ReadTargets {
				if !isKnownTarget(t) {
					errList = append(errList, fmt.Errorf("config: mount %q: routing_rules[%d].read_targets references unknown id %q", mountName, i, t))
				}
			}
			for _, t := range r.WriteTargets {
				if !isKnownTarget(t) {
					errList = append(errList, fmt.Errorf("config: mount %q: routing_rules[%d].write_targets references unknown id %q", mountName, i, t))
				}
			}
			if r.WritePolicy != "" {
				switch r.WritePolicy {
				case "first_found", "most_free", "least_free":
				default:
					errList = append(errList, fmt.Errorf("config: mount %q: routing_rules[%d].write_policy is invalid", mountName, i))
				}
			}
		}
	}

	return errList
}
