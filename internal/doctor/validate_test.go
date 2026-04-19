package doctor

import (
	"errors"
	"testing"

	"github.com/hieutdo/policyfs/internal/config"
	"github.com/stretchr/testify/require"
)

// TestValidateConfigAll_shouldRejectNilAndEmptyMounts verifies ValidateConfigAll rejects nil configs
// and configs without mounts.
func TestValidateConfigAll_shouldRejectNilAndEmptyMounts(t *testing.T) {
	t.Run("should reject nil config", func(t *testing.T) {
		errList := ValidateConfigAll(nil)
		require.Len(t, errList, 1)
		require.EqualError(t, errList[0], "config is nil")
	})

	t.Run("should reject missing mounts", func(t *testing.T) {
		errList := ValidateConfigAll(&config.RootConfig{})
		require.Len(t, errList, 1)
		require.EqualError(t, errList[0], "config: mounts is required")
	})
}

// TestValidateConfigAll_shouldValidateStatfsConfig verifies statfs mount options are validated.
func TestValidateConfigAll_shouldValidateStatfsConfig(t *testing.T) {
	base := func() *config.RootConfig {
		return &config.RootConfig{Mounts: map[string]config.MountConfig{
			"m": {
				MountPoint:   "/mnt/pfs/m",
				StoragePaths: []config.StoragePath{{ID: "ssd1", Path: "/mnt/ssd1"}},
				RoutingRules: []config.RoutingRule{{Match: "**"}},
				Mover:        config.MoverConfig{Enabled: boolPtr(false)},
			},
		}}
	}

	t.Run("should reject invalid statfs.reporting", func(t *testing.T) {
		cfg := base()
		m := cfg.Mounts["m"]
		m.Statfs.Reporting = "nope"
		cfg.Mounts["m"] = m

		msgs := mountMsgs(t, ValidateConfigAll(cfg), "m")
		require.Contains(t, msgs, "statfs.reporting is invalid")
	})

	t.Run("should reject invalid statfs.on_error", func(t *testing.T) {
		cfg := base()
		m := cfg.Mounts["m"]
		m.Statfs.OnError = "nope"
		cfg.Mounts["m"] = m

		msgs := mountMsgs(t, ValidateConfigAll(cfg), "m")
		require.Contains(t, msgs, "statfs.on_error is invalid")
	})
}

// TestValidateConfigAll_shouldValidateRoutingRulesCatchAll verifies routing rule validation around
// match requirements and the catch-all "**" rule.
func TestValidateConfigAll_shouldValidateRoutingRulesCatchAll(t *testing.T) {
	base := func() *config.RootConfig {
		return &config.RootConfig{Mounts: map[string]config.MountConfig{
			"m": {
				MountPoint:   "/mnt/pfs/m",
				StoragePaths: []config.StoragePath{{ID: "ssd1", Path: "/mnt/ssd1"}},
				RoutingRules: []config.RoutingRule{{Match: "**"}},
				Mover:        config.MoverConfig{Enabled: boolPtr(false)},
			},
		}}
	}

	t.Run("should require match", func(t *testing.T) {
		cfg := base()
		cfg.Mounts["m"] = config.MountConfig{
			MountPoint:   "/mnt/pfs/m",
			StoragePaths: []config.StoragePath{{ID: "ssd1", Path: "/mnt/ssd1"}},
			RoutingRules: []config.RoutingRule{{Match: ""}, {Match: "**"}},
			Mover:        config.MoverConfig{Enabled: boolPtr(false)},
		}

		errList := ValidateConfigAll(cfg)
		msgs := mountMsgs(t, errList, "m")
		require.Contains(t, msgs, "routing_rules[0].match is required")
	})

	t.Run("should require a catch-all", func(t *testing.T) {
		cfg := base()
		m := cfg.Mounts["m"]
		m.RoutingRules = []config.RoutingRule{{Match: "library/**"}}
		cfg.Mounts["m"] = m

		errList := ValidateConfigAll(cfg)
		msgs := mountMsgs(t, errList, "m")
		require.Contains(t, msgs, "missing catch-all rule '**'")
	})

	t.Run("should reject multiple catch-all", func(t *testing.T) {
		cfg := base()
		m := cfg.Mounts["m"]
		m.RoutingRules = []config.RoutingRule{{Match: "**"}, {Match: "**"}}
		cfg.Mounts["m"] = m

		errList := ValidateConfigAll(cfg)
		msgs := mountMsgs(t, errList, "m")
		require.Contains(t, msgs, "multiple catch-all rules '**'")
	})

	t.Run("should require catch-all to be last", func(t *testing.T) {
		cfg := base()
		m := cfg.Mounts["m"]
		m.RoutingRules = []config.RoutingRule{{Match: "**"}, {Match: "library/**"}}
		cfg.Mounts["m"] = m

		errList := ValidateConfigAll(cfg)
		msgs := mountMsgs(t, errList, "m")
		require.Contains(t, msgs, "catch-all rule '**' must be last")
	})
}

// TestValidateConfigAll_shouldValidateStoragePathsGroupsAndTargets verifies storage/group/target validation
// catches missing IDs, duplicates, and unknown references.
func TestValidateConfigAll_shouldValidateStoragePathsGroupsAndTargets(t *testing.T) {
	base := func() *config.RootConfig {
		return &config.RootConfig{Mounts: map[string]config.MountConfig{
			"m": {
				MountPoint:    "/mnt/pfs/m",
				StoragePaths:  []config.StoragePath{{ID: "ssd1", Path: "/mnt/ssd1"}},
				StorageGroups: map[string][]string{},
				RoutingRules:  []config.RoutingRule{{Match: "**"}},
				Mover:         config.MoverConfig{Enabled: boolPtr(false)},
			},
		}}
	}

	t.Run("should require storage_paths.id", func(t *testing.T) {
		cfg := base()
		m := cfg.Mounts["m"]
		m.StoragePaths = []config.StoragePath{{ID: "", Path: "/mnt/ssd1"}}
		cfg.Mounts["m"] = m

		errList := ValidateConfigAll(cfg)
		msgs := mountMsgs(t, errList, "m")
		require.Contains(t, msgs, "storage_paths[0].id is required")
	})

	t.Run("should reject duplicate storage_paths ids", func(t *testing.T) {
		cfg := base()
		m := cfg.Mounts["m"]
		m.StoragePaths = []config.StoragePath{{ID: "ssd1", Path: "/mnt/a"}, {ID: "ssd1", Path: "/mnt/b"}}
		cfg.Mounts["m"] = m

		errList := ValidateConfigAll(cfg)
		msgs := mountMsgs(t, errList, "m")
		require.Contains(t, msgs, "duplicate storage_paths id \"ssd1\"")
	})

	t.Run("should require storage_paths.path", func(t *testing.T) {
		cfg := base()
		m := cfg.Mounts["m"]
		m.StoragePaths = []config.StoragePath{{ID: "ssd1", Path: ""}}
		cfg.Mounts["m"] = m

		errList := ValidateConfigAll(cfg)
		msgs := mountMsgs(t, errList, "m")
		require.Contains(t, msgs, "storage_paths[0].path is required")
	})

	t.Run("should validate storage_groups name and member references", func(t *testing.T) {
		cfg := base()
		m := cfg.Mounts["m"]
		m.StorageGroups = map[string][]string{"": {}, "g": {"unknown"}}
		cfg.Mounts["m"] = m

		errList := ValidateConfigAll(cfg)
		msgs := mountMsgs(t, errList, "m")
		require.Contains(t, msgs, "storage_groups name must not be empty")
		require.Contains(t, msgs, "storage_groups \"g\" references unknown storage id \"unknown\"")
	})

	t.Run("should validate routing rule targets and write_policy", func(t *testing.T) {
		cfg := base()
		m := cfg.Mounts["m"]
		m.RoutingRules = []config.RoutingRule{{
			Match:        "**",
			Targets:      []string{"unknown-target"},
			ReadTargets:  []string{"unknown-read"},
			WriteTargets: []string{"unknown-write"},
			WritePolicy:  "not-a-policy",
		}}
		cfg.Mounts["m"] = m

		errList := ValidateConfigAll(cfg)
		msgs := mountMsgs(t, errList, "m")
		require.Contains(t, msgs, "routing_rules[0].targets references unknown id \"unknown-target\"")
		require.Contains(t, msgs, "routing_rules[0].read_targets references unknown id \"unknown-read\"")
		require.Contains(t, msgs, "routing_rules[0].write_targets references unknown id \"unknown-write\"")
		require.Contains(t, msgs, "routing_rules[0].write_policy is invalid")
	})
}

// TestValidateConfigAll_shouldValidateMoverJobs verifies mover job validation catches common config errors.
func TestValidateConfigAll_shouldValidateMoverJobs(t *testing.T) {
	cfg := &config.RootConfig{Mounts: map[string]config.MountConfig{
		"m": {
			MountPoint:   "/mnt/pfs/m",
			StoragePaths: []config.StoragePath{{ID: "ssd1", Path: "/mnt/ssd1"}},
			StorageGroups: map[string][]string{
				"g": {"ssd1"},
			},
			RoutingRules: []config.RoutingRule{{Match: "**"}},
			Mover: config.MoverConfig{
				Enabled: boolPtr(true),
				Jobs: []config.MoverJobConfig{
					{
						Name: "",
						Trigger: config.MoverTriggerConfig{
							Type:          "",
							AllowedWindow: &config.MoverAllowedWindow{Start: "", End: ""},
						},
						Source: config.MoverSourceConfig{
							Paths:       []string{"unknown-src"},
							Groups:      []string{"unknown-group"},
							Patterns:    []string{""},
							IncludeFile: "/etc/pfs/include.txt",
							IgnoreFile:  "/etc/pfs/ignore.txt",
						},
						Destination: config.MoverDestinationConfig{
							Paths:  []string{"unknown-dst"},
							Groups: []string{"unknown-dst-group"},
							Policy: "not-a-policy",
						},
						Conditions: config.MoverConditionsConfig{
							MinAge:  "nope",
							MinSize: "nope",
							MaxSize: "nope",
						},
					},
					{
						Name: "dup",
						Trigger: config.MoverTriggerConfig{
							Type:           "usage",
							ThresholdStart: 101,
							ThresholdStop:  101,
							AllowedWindow:  &config.MoverAllowedWindow{Start: "99:99", End: "aa"},
						},
						Source:      config.MoverSourceConfig{Paths: []string{"ssd1"}, Patterns: []string{"**"}},
						Destination: config.MoverDestinationConfig{Paths: []string{"ssd1"}, Policy: "first_found"},
					},
					{
						Name:        "dup",
						Trigger:     config.MoverTriggerConfig{Type: "manual"},
						Source:      config.MoverSourceConfig{Paths: []string{"ssd1"}, Patterns: []string{"**"}},
						Destination: config.MoverDestinationConfig{Paths: []string{"ssd1"}, Policy: "first_found"},
					},
					{
						Name:        "minmax",
						Trigger:     config.MoverTriggerConfig{Type: "manual"},
						Source:      config.MoverSourceConfig{Paths: []string{"ssd1"}, Patterns: []string{"**"}},
						Destination: config.MoverDestinationConfig{Paths: []string{"ssd1"}, Policy: "first_found"},
						Conditions:  config.MoverConditionsConfig{MinSize: "2GB", MaxSize: "1GB"},
					},
				},
			},
		},
	}}

	errList := ValidateConfigAll(cfg)
	msgs := mountMsgs(t, errList, "m")

	require.Contains(t, msgs, "mover.jobs[0].name is required")
	require.Contains(t, msgs, "mover.jobs[0].trigger.type is required and must be 'usage' or 'manual'")
	require.Contains(t, msgs, "mover.jobs[0].trigger.allowed_window is only valid for trigger.type=usage")
	require.Contains(t, msgs, "mover.jobs[0].trigger.allowed_window.start is required")
	require.Contains(t, msgs, "mover.jobs[0].trigger.allowed_window.end is required")
	require.Contains(t, msgs, "mover.jobs[0].source.paths references unknown id \"unknown-src\"")
	require.Contains(t, msgs, "mover.jobs[0].source.groups references unknown id \"unknown-group\"")
	require.Contains(t, msgs, "mover.jobs[0].source.patterns[0] must not be empty")
	require.Contains(t, msgs, "mover.jobs[0].destination.paths references unknown id \"unknown-dst\"")
	require.Contains(t, msgs, "mover.jobs[0].destination.groups references unknown id \"unknown-dst-group\"")
	require.Contains(t, msgs, "mover.jobs[0].destination.policy is invalid")
	require.Contains(t, msgs, "mover.jobs[0].conditions.min_age is invalid")
	require.Contains(t, msgs, "mover.jobs[0].conditions.min_size is invalid")
	require.Contains(t, msgs, "mover.jobs[0].conditions.max_size is invalid")

	require.Contains(t, msgs, "duplicate mover job name \"dup\"")
	require.Contains(t, msgs, "mover.jobs[1].trigger.threshold_start must be 1..100")
	require.Contains(t, msgs, "mover.jobs[1].trigger.threshold_stop must be 1..100")
	require.Contains(t, msgs, "mover.jobs[1].trigger.threshold_stop must be < threshold_start")
	require.Contains(t, msgs, "mover.jobs[1].trigger.allowed_window.start must be HH:MM")
	require.Contains(t, msgs, "mover.jobs[1].trigger.allowed_window.end must be HH:MM")

	require.Contains(t, msgs, "mover.jobs[3].conditions.min_size must be <= max_size")
}

// TestValidateConfigAll_shouldAllowIncludeFileWithoutPatterns verifies a job can omit source.patterns
// when source.include_file is set.
func TestValidateConfigAll_shouldAllowIncludeFileWithoutPatterns(t *testing.T) {
	cfg := &config.RootConfig{Mounts: map[string]config.MountConfig{
		"m": {
			MountPoint:   "/mnt/pfs/m",
			StoragePaths: []config.StoragePath{{ID: "ssd1", Path: "/mnt/ssd1"}},
			RoutingRules: []config.RoutingRule{{Match: "**"}},
			Mover: config.MoverConfig{Enabled: boolPtr(true), Jobs: []config.MoverJobConfig{{
				Name:        "promote",
				Trigger:     config.MoverTriggerConfig{Type: "manual"},
				Source:      config.MoverSourceConfig{Paths: []string{"ssd1"}, IncludeFile: "/etc/pfs/include.txt"},
				Destination: config.MoverDestinationConfig{Paths: []string{"ssd1"}, Policy: "first_found"},
			}}},
		},
	}}

	errList := ValidateConfigAll(cfg)
	require.Len(t, errList, 0)
	msgs := mountMsgs(t, errList, "m")
	require.NotContains(t, msgs, "mover.jobs[0].source.patterns or source.include_file is required")
}

// TestValidateConfigAll_shouldRejectEmptyPatternsAndIncludeFile verifies validation fails when
// both source.patterns and source.include_file are absent.
func TestValidateConfigAll_shouldRejectEmptyPatternsAndIncludeFile(t *testing.T) {
	cfg := &config.RootConfig{Mounts: map[string]config.MountConfig{
		"m": {
			MountPoint:   "/mnt/pfs/m",
			StoragePaths: []config.StoragePath{{ID: "ssd1", Path: "/mnt/ssd1"}},
			RoutingRules: []config.RoutingRule{{Match: "**"}},
			Mover: config.MoverConfig{Enabled: boolPtr(true), Jobs: []config.MoverJobConfig{{
				Name:        "empty",
				Trigger:     config.MoverTriggerConfig{Type: "manual"},
				Source:      config.MoverSourceConfig{Paths: []string{"ssd1"}},
				Destination: config.MoverDestinationConfig{Paths: []string{"ssd1"}, Policy: "first_found"},
			}}},
		},
	}}

	msgs := mountMsgs(t, ValidateConfigAll(cfg), "m")
	require.Contains(t, msgs, "mover.jobs[0].source.patterns or source.include_file is required")
}

// TestValidateConfigAll_shouldRejectWhitespaceOnlyIncludeFile verifies whitespace-only
// include_file / ignore_file values are rejected.
func TestValidateConfigAll_shouldRejectWhitespaceOnlyIncludeFile(t *testing.T) {
	cfg := &config.RootConfig{Mounts: map[string]config.MountConfig{
		"m": {
			MountPoint:   "/mnt/pfs/m",
			StoragePaths: []config.StoragePath{{ID: "ssd1", Path: "/mnt/ssd1"}},
			RoutingRules: []config.RoutingRule{{Match: "**"}},
			Mover: config.MoverConfig{Enabled: boolPtr(true), Jobs: []config.MoverJobConfig{{
				Name:        "ws",
				Trigger:     config.MoverTriggerConfig{Type: "manual"},
				Source:      config.MoverSourceConfig{Paths: []string{"ssd1"}, Patterns: []string{"**"}, IncludeFile: "  ", IgnoreFile: "  "},
				Destination: config.MoverDestinationConfig{Paths: []string{"ssd1"}, Policy: "first_found"},
			}}},
		},
	}}

	msgs := mountMsgs(t, ValidateConfigAll(cfg), "m")
	require.Contains(t, msgs, "mover.jobs[0].source.include_file must not be empty")
	require.Contains(t, msgs, "mover.jobs[0].source.ignore_file must not be empty")
	// Whitespace-only include_file should NOT also trigger the "required" error.
	require.NotContains(t, msgs, "mover.jobs[0].source.patterns or source.include_file is required")
}

// boolPtr is a helper for constructing *bool values in test configs.
func boolPtr(v bool) *bool { return &v }

// mountMsgs extracts mount-scoped doctor validation message strings for a given mount.
func mountMsgs(t *testing.T, errList []error, mount string) []string {
	t.Helper()

	var out []string
	for _, e := range errList {
		var me *MountConfigError
		if errors.As(e, &me) && me != nil && me.Mount == mount {
			out = append(out, me.Msg)
		}
	}
	return out
}
