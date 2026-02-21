package cli

import (
	"errors"
	"fmt"
	"regexp"
	"sort"
	"strings"
	"time"

	"github.com/hieutdo/policyfs/internal/config"
	"github.com/hieutdo/policyfs/internal/humanfmt"
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
			normalizeMatch := func(p string) string {
				p = strings.TrimSpace(p)
				p = strings.TrimPrefix(p, "/")
				p = strings.TrimSuffix(p, "/")
				for strings.Contains(p, "//") {
					p = strings.ReplaceAll(p, "//", "/")
				}
				return p
			}
			isCatchAll := func(match string) bool {
				return normalizeMatch(match) == "**"
			}

			catchAllIdx := []int{}
			for i, r := range m.RoutingRules {
				if r.Match == "" {
					errList = append(errList, fmt.Errorf("config: mount %q: routing_rules[%d].match is required", mountName, i))
				}
				if isCatchAll(r.Match) {
					catchAllIdx = append(catchAllIdx, i)
				}
			}
			if len(catchAllIdx) == 0 {
				errList = append(errList, fmt.Errorf("config: mount %q: missing catch-all rule '**'", mountName))
			} else {
				if len(catchAllIdx) > 1 {
					errList = append(errList, fmt.Errorf("config: mount %q: multiple catch-all rules '**'", mountName))
				}
				if catchAllIdx[len(catchAllIdx)-1] != len(m.RoutingRules)-1 {
					errList = append(errList, fmt.Errorf("config: mount %q: catch-all rule '**' must be last", mountName))
				}
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

		isStorageID := func(id string) bool {
			_, ok := storageIDs[id]
			return ok
		}
		isGroupID := func(id string) bool {
			_, ok := groupIDs[id]
			return ok
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

		enabled := true
		if m.Mover.Enabled != nil {
			enabled = *m.Mover.Enabled
		}
		if enabled {
			seenJobs := map[string]struct{}{}
			for ji, j := range m.Mover.Jobs {
				if strings.TrimSpace(j.Name) == "" {
					errList = append(errList, fmt.Errorf("config: mount %q: mover.jobs[%d].name is required", mountName, ji))
				} else {
					if _, dup := seenJobs[j.Name]; dup {
						errList = append(errList, fmt.Errorf("config: mount %q: duplicate mover job name %q", mountName, j.Name))
					}
					seenJobs[j.Name] = struct{}{}
				}

				tt := strings.TrimSpace(j.Trigger.Type)
				switch tt {
				case "usage", "manual":
				default:
					errList = append(errList, fmt.Errorf("config: mount %q: mover.jobs[%d].trigger.type is required and must be 'usage' or 'manual'", mountName, ji))
				}
				if tt == "usage" {
					if j.Trigger.ThresholdStart < 1 || j.Trigger.ThresholdStart > 100 {
						errList = append(errList, fmt.Errorf("config: mount %q: mover.jobs[%d].trigger.threshold_start must be 1..100", mountName, ji))
					}
					if j.Trigger.ThresholdStop < 1 || j.Trigger.ThresholdStop > 100 {
						errList = append(errList, fmt.Errorf("config: mount %q: mover.jobs[%d].trigger.threshold_stop must be 1..100", mountName, ji))
					}
					if j.Trigger.ThresholdStop >= j.Trigger.ThresholdStart {
						errList = append(errList, fmt.Errorf("config: mount %q: mover.jobs[%d].trigger.threshold_stop must be < threshold_start", mountName, ji))
					}
				}

				aw := j.Trigger.AllowedWindow
				if aw != nil {
					if tt != "usage" {
						errList = append(errList, fmt.Errorf("config: mount %q: mover.jobs[%d].trigger.allowed_window is only valid for trigger.type=usage", mountName, ji))
					}
					if strings.TrimSpace(aw.Start) == "" {
						errList = append(errList, fmt.Errorf("config: mount %q: mover.jobs[%d].trigger.allowed_window.start is required", mountName, ji))
					} else if _, err := time.Parse("15:04", aw.Start); err != nil {
						errList = append(errList, fmt.Errorf("config: mount %q: mover.jobs[%d].trigger.allowed_window.start must be HH:MM", mountName, ji))
					}
					if strings.TrimSpace(aw.End) == "" {
						errList = append(errList, fmt.Errorf("config: mount %q: mover.jobs[%d].trigger.allowed_window.end is required", mountName, ji))
					} else if _, err := time.Parse("15:04", aw.End); err != nil {
						errList = append(errList, fmt.Errorf("config: mount %q: mover.jobs[%d].trigger.allowed_window.end must be HH:MM", mountName, ji))
					}
				}

				if len(j.Source.Paths) == 0 && len(j.Source.Groups) == 0 {
					errList = append(errList, fmt.Errorf("config: mount %q: mover.jobs[%d].source.paths or source.groups is required", mountName, ji))
				}
				for _, sid := range j.Source.Paths {
					if !isStorageID(sid) {
						errList = append(errList, fmt.Errorf("config: mount %q: mover.jobs[%d].source.paths references unknown id %q", mountName, ji, sid))
					}
				}
				for _, gid := range j.Source.Groups {
					if !isGroupID(gid) {
						errList = append(errList, fmt.Errorf("config: mount %q: mover.jobs[%d].source.groups references unknown id %q", mountName, ji, gid))
					}
				}
				if len(j.Source.Patterns) == 0 {
					errList = append(errList, fmt.Errorf("config: mount %q: mover.jobs[%d].source.patterns must not be empty", mountName, ji))
				}
				for pi, p := range j.Source.Patterns {
					if strings.TrimSpace(p) == "" {
						errList = append(errList, fmt.Errorf("config: mount %q: mover.jobs[%d].source.patterns[%d] must not be empty", mountName, ji, pi))
					}
				}

				if len(j.Destination.Paths) == 0 && len(j.Destination.Groups) == 0 {
					errList = append(errList, fmt.Errorf("config: mount %q: mover.jobs[%d].destination.paths or destination.groups is required", mountName, ji))
				}
				for _, sid := range j.Destination.Paths {
					if !isStorageID(sid) {
						errList = append(errList, fmt.Errorf("config: mount %q: mover.jobs[%d].destination.paths references unknown id %q", mountName, ji, sid))
					}
				}
				for _, gid := range j.Destination.Groups {
					if !isGroupID(gid) {
						errList = append(errList, fmt.Errorf("config: mount %q: mover.jobs[%d].destination.groups references unknown id %q", mountName, ji, gid))
					}
				}
				switch strings.TrimSpace(j.Destination.Policy) {
				case "", "first_found", "most_free", "least_free":
				default:
					errList = append(errList, fmt.Errorf("config: mount %q: mover.jobs[%d].destination.policy is invalid", mountName, ji))
				}

				var minSizeBytes *int64
				var maxSizeBytes *int64
				if strings.TrimSpace(j.Conditions.MinAge) != "" {
					if _, err := humanfmt.ParseDuration(j.Conditions.MinAge); err != nil {
						errList = append(errList, fmt.Errorf("config: mount %q: mover.jobs[%d].conditions.min_age is invalid", mountName, ji))
					}
				}
				if strings.TrimSpace(j.Conditions.MinSize) != "" {
					v, err := humanfmt.ParseBytes(j.Conditions.MinSize)
					if err != nil {
						errList = append(errList, fmt.Errorf("config: mount %q: mover.jobs[%d].conditions.min_size is invalid", mountName, ji))
					} else {
						minSizeBytes = &v
					}
				}
				if strings.TrimSpace(j.Conditions.MaxSize) != "" {
					v, err := humanfmt.ParseBytes(j.Conditions.MaxSize)
					if err != nil {
						errList = append(errList, fmt.Errorf("config: mount %q: mover.jobs[%d].conditions.max_size is invalid", mountName, ji))
					} else {
						maxSizeBytes = &v
					}
				}
				if minSizeBytes != nil && maxSizeBytes != nil && *minSizeBytes > *maxSizeBytes {
					errList = append(errList, fmt.Errorf("config: mount %q: mover.jobs[%d].conditions.min_size must be <= max_size", mountName, ji))
				}
			}
		}
	}

	return errList
}
