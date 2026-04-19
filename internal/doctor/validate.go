package doctor

import (
	"errors"
	"fmt"
	"sort"
	"strings"
	"time"

	"github.com/hieutdo/policyfs/internal/config"
	"github.com/hieutdo/policyfs/internal/humanfmt"
)

// MountConfigError is a mount-scoped configuration validation error.
//
// It exists so callers can group errors by mount without parsing error strings.
type MountConfigError struct {
	Mount string
	Msg   string
}

// Error formats the validation error in the same stable shape used elsewhere.
func (e *MountConfigError) Error() string {
	if e == nil {
		return ""
	}
	return fmt.Sprintf("config: mount %q: %s", e.Mount, e.Msg)
}

// ValidateConfigAll runs doctor-style config validation and returns all discovered errors.
func ValidateConfigAll(c *config.RootConfig) []error {
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
		if strings.TrimSpace(m.Log.Level) != "" {
			switch strings.TrimSpace(m.Log.Level) {
			case "debug", "info", "warn", "error", "off":
				// ok
			default:
				errList = append(errList, &MountConfigError{Mount: mountName, Msg: "log.level is invalid"})
			}
		}
		if strings.TrimSpace(m.Statfs.Reporting) != "" {
			switch strings.TrimSpace(m.Statfs.Reporting) {
			case "mount_pooled_targets", "path_pooled_targets":
				// ok
			default:
				errList = append(errList, &MountConfigError{Mount: mountName, Msg: "statfs.reporting is invalid"})
			}
		}
		if strings.TrimSpace(m.Statfs.OnError) != "" {
			switch strings.TrimSpace(m.Statfs.OnError) {
			case "ignore_failed", "fail_eio", "fallback_effective_target", "fallback_loopback":
				// ok
			default:
				errList = append(errList, &MountConfigError{Mount: mountName, Msg: "statfs.on_error is invalid"})
			}
		}
		if m.MountPoint == "" {
			errList = append(errList, &MountConfigError{Mount: mountName, Msg: "mountpoint is required"})
		}
		if len(m.StoragePaths) == 0 {
			errList = append(errList, &MountConfigError{Mount: mountName, Msg: "storage_paths must not be empty"})
		}
		if len(m.RoutingRules) == 0 {
			errList = append(errList, &MountConfigError{Mount: mountName, Msg: "routing_rules must not be empty"})
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
					errList = append(errList, &MountConfigError{Mount: mountName, Msg: fmt.Sprintf("routing_rules[%d].match is required", i)})
				}
				if isCatchAll(r.Match) {
					catchAllIdx = append(catchAllIdx, i)
				}
			}
			if len(catchAllIdx) == 0 {
				errList = append(errList, &MountConfigError{Mount: mountName, Msg: "missing catch-all rule '**'"})
			} else {
				if len(catchAllIdx) > 1 {
					errList = append(errList, &MountConfigError{Mount: mountName, Msg: "multiple catch-all rules '**'"})
				}
				if catchAllIdx[len(catchAllIdx)-1] != len(m.RoutingRules)-1 {
					errList = append(errList, &MountConfigError{Mount: mountName, Msg: "catch-all rule '**' must be last"})
				}
			}
		}

		storageIDs := map[string]struct{}{}
		for i, sp := range m.StoragePaths {
			if sp.ID == "" {
				errList = append(errList, &MountConfigError{Mount: mountName, Msg: fmt.Sprintf("storage_paths[%d].id is required", i)})
				continue
			}
			if _, ok := storageIDs[sp.ID]; ok {
				errList = append(errList, &MountConfigError{Mount: mountName, Msg: fmt.Sprintf("duplicate storage_paths id %q", sp.ID)})
				continue
			}
			storageIDs[sp.ID] = struct{}{}
			if sp.Path == "" {
				errList = append(errList, &MountConfigError{Mount: mountName, Msg: fmt.Sprintf("storage_paths[%d].path is required", i)})
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
				errList = append(errList, &MountConfigError{Mount: mountName, Msg: "storage_groups name must not be empty"})
				continue
			}
			groupIDs[g] = struct{}{}
			for _, sid := range members {
				if _, ok := storageIDs[sid]; !ok {
					errList = append(errList, &MountConfigError{Mount: mountName, Msg: fmt.Sprintf("storage_groups %q references unknown storage id %q", g, sid)})
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
					errList = append(errList, &MountConfigError{Mount: mountName, Msg: fmt.Sprintf("routing_rules[%d].targets references unknown id %q", i, t)})
				}
			}
			for _, t := range r.ReadTargets {
				if !isKnownTarget(t) {
					errList = append(errList, &MountConfigError{Mount: mountName, Msg: fmt.Sprintf("routing_rules[%d].read_targets references unknown id %q", i, t)})
				}
			}
			for _, t := range r.WriteTargets {
				if !isKnownTarget(t) {
					errList = append(errList, &MountConfigError{Mount: mountName, Msg: fmt.Sprintf("routing_rules[%d].write_targets references unknown id %q", i, t)})
				}
			}
			if r.WritePolicy != "" {
				switch r.WritePolicy {
				case "first_found", "most_free", "least_free":
				default:
					errList = append(errList, &MountConfigError{Mount: mountName, Msg: fmt.Sprintf("routing_rules[%d].write_policy is invalid", i)})
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
					errList = append(errList, &MountConfigError{Mount: mountName, Msg: fmt.Sprintf("mover.jobs[%d].name is required", ji)})
				} else {
					if _, dup := seenJobs[j.Name]; dup {
						errList = append(errList, &MountConfigError{Mount: mountName, Msg: fmt.Sprintf("duplicate mover job name %q", j.Name)})
					}
					seenJobs[j.Name] = struct{}{}
				}

				tt := strings.TrimSpace(j.Trigger.Type)
				switch tt {
				case "usage", "manual":
				default:
					errList = append(errList, &MountConfigError{Mount: mountName, Msg: fmt.Sprintf("mover.jobs[%d].trigger.type is required and must be 'usage' or 'manual'", ji)})
				}
				if tt == "usage" {
					if j.Trigger.ThresholdStart < 1 || j.Trigger.ThresholdStart > 100 {
						errList = append(errList, &MountConfigError{Mount: mountName, Msg: fmt.Sprintf("mover.jobs[%d].trigger.threshold_start must be 1..100", ji)})
					}
					if j.Trigger.ThresholdStop < 1 || j.Trigger.ThresholdStop > 100 {
						errList = append(errList, &MountConfigError{Mount: mountName, Msg: fmt.Sprintf("mover.jobs[%d].trigger.threshold_stop must be 1..100", ji)})
					}
					if j.Trigger.ThresholdStop >= j.Trigger.ThresholdStart {
						errList = append(errList, &MountConfigError{Mount: mountName, Msg: fmt.Sprintf("mover.jobs[%d].trigger.threshold_stop must be < threshold_start", ji)})
					}
				}

				aw := j.Trigger.AllowedWindow
				if aw != nil {
					if tt != "usage" {
						errList = append(errList, &MountConfigError{Mount: mountName, Msg: fmt.Sprintf("mover.jobs[%d].trigger.allowed_window is only valid for trigger.type=usage", ji)})
					}
					if strings.TrimSpace(aw.Start) == "" {
						errList = append(errList, &MountConfigError{Mount: mountName, Msg: fmt.Sprintf("mover.jobs[%d].trigger.allowed_window.start is required", ji)})
					} else if _, err := time.Parse("15:04", aw.Start); err != nil {
						errList = append(errList, &MountConfigError{Mount: mountName, Msg: fmt.Sprintf("mover.jobs[%d].trigger.allowed_window.start must be HH:MM", ji)})
					}
					if strings.TrimSpace(aw.End) == "" {
						errList = append(errList, &MountConfigError{Mount: mountName, Msg: fmt.Sprintf("mover.jobs[%d].trigger.allowed_window.end is required", ji)})
					} else if _, err := time.Parse("15:04", aw.End); err != nil {
						errList = append(errList, &MountConfigError{Mount: mountName, Msg: fmt.Sprintf("mover.jobs[%d].trigger.allowed_window.end must be HH:MM", ji)})
					}
				}

				if len(j.Source.Paths) == 0 && len(j.Source.Groups) == 0 {
					errList = append(errList, &MountConfigError{Mount: mountName, Msg: fmt.Sprintf("mover.jobs[%d].source.paths or source.groups is required", ji)})
				}
				for _, sid := range j.Source.Paths {
					if !isStorageID(sid) {
						errList = append(errList, &MountConfigError{Mount: mountName, Msg: fmt.Sprintf("mover.jobs[%d].source.paths references unknown id %q", ji, sid)})
					}
				}
				for _, gid := range j.Source.Groups {
					if !isGroupID(gid) {
						errList = append(errList, &MountConfigError{Mount: mountName, Msg: fmt.Sprintf("mover.jobs[%d].source.groups references unknown id %q", ji, gid)})
					}
				}

				includeFile := strings.TrimSpace(j.Source.IncludeFile)
				ignoreFile := strings.TrimSpace(j.Source.IgnoreFile)
				if j.Source.IncludeFile != "" && includeFile == "" {
					errList = append(errList, &MountConfigError{Mount: mountName, Msg: fmt.Sprintf("mover.jobs[%d].source.include_file must not be empty", ji)})
				}
				if j.Source.IgnoreFile != "" && ignoreFile == "" {
					errList = append(errList, &MountConfigError{Mount: mountName, Msg: fmt.Sprintf("mover.jobs[%d].source.ignore_file must not be empty", ji)})
				}
				if len(j.Source.Patterns) == 0 && j.Source.IncludeFile == "" {
					errList = append(errList, &MountConfigError{Mount: mountName, Msg: fmt.Sprintf("mover.jobs[%d].source.patterns or source.include_file is required", ji)})
				}
				for pi, p := range j.Source.Patterns {
					if strings.TrimSpace(p) == "" {
						errList = append(errList, &MountConfigError{Mount: mountName, Msg: fmt.Sprintf("mover.jobs[%d].source.patterns[%d] must not be empty", ji, pi)})
					}
				}

				if len(j.Destination.Paths) == 0 && len(j.Destination.Groups) == 0 {
					errList = append(errList, &MountConfigError{Mount: mountName, Msg: fmt.Sprintf("mover.jobs[%d].destination.paths or destination.groups is required", ji)})
				}
				for _, sid := range j.Destination.Paths {
					if !isStorageID(sid) {
						errList = append(errList, &MountConfigError{Mount: mountName, Msg: fmt.Sprintf("mover.jobs[%d].destination.paths references unknown id %q", ji, sid)})
					}
				}
				for _, gid := range j.Destination.Groups {
					if !isGroupID(gid) {
						errList = append(errList, &MountConfigError{Mount: mountName, Msg: fmt.Sprintf("mover.jobs[%d].destination.groups references unknown id %q", ji, gid)})
					}
				}
				switch strings.TrimSpace(j.Destination.Policy) {
				case "", "first_found", "most_free", "least_free":
				default:
					errList = append(errList, &MountConfigError{Mount: mountName, Msg: fmt.Sprintf("mover.jobs[%d].destination.policy is invalid", ji)})
				}

				var minSizeBytes *int64
				var maxSizeBytes *int64
				if strings.TrimSpace(j.Conditions.MinAge) != "" {
					if _, err := humanfmt.ParseDuration(j.Conditions.MinAge); err != nil {
						errList = append(errList, &MountConfigError{Mount: mountName, Msg: fmt.Sprintf("mover.jobs[%d].conditions.min_age is invalid", ji)})
					}
				}
				if strings.TrimSpace(j.Conditions.MinSize) != "" {
					v, err := humanfmt.ParseBytes(j.Conditions.MinSize)
					if err != nil {
						errList = append(errList, &MountConfigError{Mount: mountName, Msg: fmt.Sprintf("mover.jobs[%d].conditions.min_size is invalid", ji)})
					} else {
						minSizeBytes = &v
					}
				}
				if strings.TrimSpace(j.Conditions.MaxSize) != "" {
					v, err := humanfmt.ParseBytes(j.Conditions.MaxSize)
					if err != nil {
						errList = append(errList, &MountConfigError{Mount: mountName, Msg: fmt.Sprintf("mover.jobs[%d].conditions.max_size is invalid", ji)})
					} else {
						maxSizeBytes = &v
					}
				}
				if minSizeBytes != nil && maxSizeBytes != nil && *minSizeBytes > *maxSizeBytes {
					errList = append(errList, &MountConfigError{Mount: mountName, Msg: fmt.Sprintf("mover.jobs[%d].conditions.min_size must be <= max_size", ji)})
				}
			}
		}
	}

	return errList
}
