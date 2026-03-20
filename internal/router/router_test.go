package router

import (
	"os"
	"path/filepath"
	"testing"

	"github.com/hieutdo/policyfs/internal/config"
	"github.com/hieutdo/policyfs/internal/errkind"
	"github.com/stretchr/testify/require"
)

// TestRouter_MatchRule_GlobSyntax verifies the router glob matcher covers the routing spec syntax.
func TestRouter_MatchRule_GlobSyntax(t *testing.T) {
	tests := []struct {
		name    string
		pattern string
		path    string
		match   bool
	}{
		{
			name:    "should match catch-all",
			pattern: "**",
			path:    "library/movies/a.mkv",
			match:   true,
		},
		{
			name:    "should match single segment star",
			pattern: "*.mkv",
			path:    "a.mkv",
			match:   true,
		},
		{
			name:    "should not match star across slash",
			pattern: "*.mkv",
			path:    "dir/a.mkv",
			match:   false,
		},
		{
			name:    "should match doublestar at start",
			pattern: "**/*.mkv",
			path:    "dir/sub1/sub2/a.mkv",
			match:   true,
		},
		{
			name:    "should match doublestar in middle",
			pattern: "dir/**/*.mkv",
			path:    "dir/sub1/sub2/a.mkv",
			match:   true,
		},
		{
			name:    "should match doublestar at end",
			pattern: "dir/**",
			path:    "dir/sub/a.mkv",
			match:   true,
		},
		{
			name:    "should support brace expansion",
			pattern: "library/{movies,tv}/**",
			path:    "library/movies/a.mkv",
			match:   true,
		},
		{
			name:    "should support brace expansion alternative",
			pattern: "library/{movies,tv}/**",
			path:    "library/tv/a.mkv",
			match:   true,
		},
		{
			name:    "should not match brace expansion when segment differs",
			pattern: "library/{movies,tv}/**",
			path:    "library/music/a.mkv",
			match:   false,
		},
		{
			name:    "should support char class",
			pattern: "file[ab].txt",
			path:    "filea.txt",
			match:   true,
		},
		{
			name:    "should support negated char class",
			pattern: "file[!a].txt",
			path:    "fileb.txt",
			match:   true,
		},
		{
			name:    "should not match negated char class",
			pattern: "file[!a].txt",
			path:    "filea.txt",
			match:   false,
		},
		{
			name:    "should normalize leading and trailing slashes",
			pattern: "/downloads/**/",
			path:    "/downloads/x/y",
			match:   true,
		},
		{
			name:    "should collapse double slashes",
			pattern: "downloads/**",
			path:    "downloads//x",
			match:   true,
		},
	}

	for _, tc := range tests {
		t.Run(tc.name, func(t *testing.T) {
			// Build a router that always has a valid catch-all rule.
			rules := []config.RoutingRule{{Match: tc.pattern, Targets: []string{"ssd1"}}}
			if tc.pattern != "**" {
				rules = append(rules, config.RoutingRule{Match: "**", Targets: []string{"ssd1"}})
			}
			r, err := New(&config.MountConfig{
				StoragePaths: []config.StoragePath{{ID: "ssd1", Path: "/mnt/ssd1"}},
				RoutingRules: rules,
			})
			require.NoError(t, err)

			got, ok := r.matchRule(tc.path)
			require.True(t, ok)
			if tc.match {
				require.Equal(t, tc.pattern, got.rule.Match)
			} else {
				require.Equal(t, "**", got.rule.Match)
			}
		})
	}
}

// TestRouter_ResolveListTargets_Union verifies directory listing targets are the union of all applicable rules.
func TestRouter_ResolveListTargets_Union(t *testing.T) {
	// This config intentionally makes catch-all read only ssd2.
	// If listing used first-match (catch-all for "library"), entries from ssd1 would be missed.
	r, err := New(&config.MountConfig{
		StoragePaths: []config.StoragePath{
			{ID: "ssd1", Path: "/mnt/ssd1"},
			{ID: "ssd2", Path: "/mnt/ssd2"},
		},
		RoutingRules: []config.RoutingRule{
			{Match: "library/movies/**", ReadTargets: []string{"ssd1"}},
			{Match: "library/music/**", ReadTargets: []string{"ssd2"}},
			{Match: "**", ReadTargets: []string{"ssd2"}},
		},
	})
	require.NoError(t, err)

	got, err := r.ResolveListTargets("library")
	require.NoError(t, err)

	ids := make([]string, 0, len(got))
	for _, t := range got {
		ids = append(ids, t.ID)
	}

	require.Equal(t, []string{"ssd1", "ssd2"}, ids)
}

// TestRouter_New_shouldValidateConfig verifies New rejects invalid mount configs and
// returns typed errors that callers can reliably match via errors.Is.
func TestRouter_New_shouldValidateConfig(t *testing.T) {
	t.Run("should reject nil mount config", func(t *testing.T) {
		_, err := New(nil)
		require.Error(t, err)
		require.ErrorIs(t, err, errkind.ErrNil)
		var ne *errkind.NilError
		require.ErrorAs(t, err, &ne)
	})

	t.Run("should require storage_paths", func(t *testing.T) {
		_, err := New(&config.MountConfig{RoutingRules: []config.RoutingRule{{Match: "**", Targets: []string{"ssd1"}}}})
		require.Error(t, err)
		require.ErrorIs(t, err, errkind.ErrRequired)
	})

	t.Run("should require routing_rules", func(t *testing.T) {
		_, err := New(&config.MountConfig{StoragePaths: []config.StoragePath{{ID: "ssd1", Path: "/mnt/ssd1"}}})
		require.Error(t, err)
		require.ErrorIs(t, err, errkind.ErrRequired)
	})

	t.Run("should require a catch-all", func(t *testing.T) {
		_, err := New(&config.MountConfig{
			StoragePaths: []config.StoragePath{{ID: "ssd1", Path: "/mnt/ssd1"}},
			RoutingRules: []config.RoutingRule{{Match: "library/**", Targets: []string{"ssd1"}}},
		})
		require.Error(t, err)
		require.ErrorIs(t, err, errkind.ErrRequired)
	})

	t.Run("should reject multiple catch-all", func(t *testing.T) {
		_, err := New(&config.MountConfig{
			StoragePaths: []config.StoragePath{{ID: "ssd1", Path: "/mnt/ssd1"}},
			RoutingRules: []config.RoutingRule{{Match: "**", Targets: []string{"ssd1"}}, {Match: "**", Targets: []string{"ssd1"}}},
		})
		require.Error(t, err)
		require.ErrorIs(t, err, errkind.ErrInvalid)
	})

	t.Run("should require catch-all to be last", func(t *testing.T) {
		_, err := New(&config.MountConfig{
			StoragePaths: []config.StoragePath{{ID: "ssd1", Path: "/mnt/ssd1"}},
			RoutingRules: []config.RoutingRule{{Match: "**", Targets: []string{"ssd1"}}, {Match: "library/**", Targets: []string{"ssd1"}}},
		})
		require.Error(t, err)
		require.ErrorIs(t, err, errkind.ErrInvalid)
	})

	t.Run("should require storage_paths.id", func(t *testing.T) {
		_, err := New(&config.MountConfig{
			StoragePaths: []config.StoragePath{{ID: "", Path: "/mnt/ssd1"}},
			RoutingRules: []config.RoutingRule{{Match: "**", Targets: []string{"ssd1"}}},
		})
		require.Error(t, err)
		require.ErrorIs(t, err, errkind.ErrRequired)
	})

	t.Run("should require storage_paths.path", func(t *testing.T) {
		_, err := New(&config.MountConfig{
			StoragePaths: []config.StoragePath{{ID: "ssd1", Path: ""}},
			RoutingRules: []config.RoutingRule{{Match: "**", Targets: []string{"ssd1"}}},
		})
		require.Error(t, err)
		require.ErrorIs(t, err, errkind.ErrRequired)
	})

	t.Run("should require routing_rules[].match", func(t *testing.T) {
		_, err := New(&config.MountConfig{
			StoragePaths: []config.StoragePath{{ID: "ssd1", Path: "/mnt/ssd1"}},
			RoutingRules: []config.RoutingRule{{Match: "", Targets: []string{"ssd1"}}, {Match: "**", Targets: []string{"ssd1"}}},
		})
		require.Error(t, err)
		require.ErrorIs(t, err, errkind.ErrRequired)
	})

	t.Run("should wrap pattern compile errors", func(t *testing.T) {
		_, err := New(&config.MountConfig{
			StoragePaths: []config.StoragePath{{ID: "ssd1", Path: "/mnt/ssd1"}},
			RoutingRules: []config.RoutingRule{{Match: "{}", Targets: []string{"ssd1"}}, {Match: "**", Targets: []string{"ssd1"}}},
		})
		require.Error(t, err)
		require.ErrorIs(t, err, errkind.ErrInvalid)
		var ie *errkind.InvalidError
		require.ErrorAs(t, err, &ie)
	})
}

// TestRouter_expandTargets_shouldExpandGroupsAndDedupe verifies expandTargets expands storage groups,
// trims whitespace, and dedupes while preserving order.
func TestRouter_expandTargets_shouldExpandGroupsAndDedupe(t *testing.T) {
	r := &Router{
		storageByID: map[string]config.StoragePath{
			"ssd1": {ID: "ssd1", Path: "/mnt/ssd1"},
			"ssd2": {ID: "ssd2", Path: "/mnt/ssd2"},
		},
		storageGroups: map[string][]string{
			"g": {" ssd1 ", "ssd2", "ssd2"},
		},
	}

	ids, err := r.expandTargets([]string{"ssd1", "g", "ssd1", ""})
	require.NoError(t, err)
	require.Equal(t, []string{"ssd1", "ssd2"}, ids)
}

// TestRouter_expandTargets_shouldRejectUnknownTargets verifies unknown target IDs and unknown group members
// are reported as typed invalid errors.
func TestRouter_expandTargets_shouldRejectUnknownTargets(t *testing.T) {
	r := &Router{
		storageByID: map[string]config.StoragePath{
			"ssd1": {ID: "ssd1", Path: "/mnt/ssd1"},
		},
		storageGroups: map[string][]string{
			"g": {"unknown"},
		},
	}

	t.Run("should reject unknown direct id", func(t *testing.T) {
		_, err := r.expandTargets([]string{"unknown"})
		require.Error(t, err)
		require.ErrorIs(t, err, errkind.ErrInvalid)
		var ie *errkind.InvalidError
		require.ErrorAs(t, err, &ie)
	})

	t.Run("should reject unknown group member", func(t *testing.T) {
		_, err := r.expandTargets([]string{"g"})
		require.Error(t, err)
		require.ErrorIs(t, err, errkind.ErrInvalid)
		var ie *errkind.InvalidError
		require.ErrorAs(t, err, &ie)
	})
}

// TestRouter_expandTargets_shouldReturnErrNoTargetsResolved verifies empty/blank inputs produce
// a stable sentinel error for callers to match.
func TestRouter_expandTargets_shouldReturnErrNoTargetsResolved(t *testing.T) {
	r := &Router{storageByID: map[string]config.StoragePath{"ssd1": {ID: "ssd1", Path: "/mnt/ssd1"}}}

	_, err := r.expandTargets([]string{" ", "\t"})
	require.Error(t, err)
	require.ErrorIs(t, err, ErrNoTargetsResolved)
}

// TestRouter_ResolveReadTargets_shouldExpandTargets verifies ResolveReadTargets uses Targets as the default
// when ReadTargets is empty and supports group expansion.
func TestRouter_ResolveReadTargets_shouldExpandTargets(t *testing.T) {
	r, err := New(&config.MountConfig{
		StoragePaths: []config.StoragePath{
			{ID: "ssd1", Path: "/mnt/ssd1", Indexed: true},
			{ID: "ssd2", Path: "/mnt/ssd2", Indexed: false},
		},
		StorageGroups: map[string][]string{"g": {"ssd1", "ssd2"}},
		RoutingRules: []config.RoutingRule{
			{Match: "library/**", Targets: []string{"g"}},
			{Match: "**", Targets: []string{"ssd2"}},
		},
	})
	require.NoError(t, err)

	got, err := r.ResolveReadTargets("library/movies/a.mkv")
	require.NoError(t, err)
	require.Len(t, got, 2)
	require.Equal(t, "ssd1", got[0].ID)
	require.True(t, got[0].Indexed)
	require.Equal(t, "ssd2", got[1].ID)
}

// TestRouter_ResolveWriteTargets_shouldReturnErrNoTargetsResolved verifies routing rules with empty write
// target lists surface ErrNoTargetsResolved.
func TestRouter_ResolveWriteTargets_shouldReturnErrNoTargetsResolved(t *testing.T) {
	r, err := New(&config.MountConfig{
		StoragePaths: []config.StoragePath{{ID: "ssd1", Path: "/mnt/ssd1"}},
		RoutingRules: []config.RoutingRule{{Match: "**"}},
	})
	require.NoError(t, err)

	_, err = r.ResolveWriteTargets("x")
	require.Error(t, err)
	require.ErrorIs(t, err, ErrNoTargetsResolved)
}

// TestRouter_SelectWriteTarget_shouldRequireWritePolicy verifies SelectWriteTarget rejects missing write_policy.
func TestRouter_SelectWriteTarget_shouldRequireWritePolicy(t *testing.T) {
	dir := t.TempDir()
	r, err := New(&config.MountConfig{
		StoragePaths: []config.StoragePath{{ID: "ssd1", Path: dir}},
		RoutingRules: []config.RoutingRule{{Match: "**", Targets: []string{"ssd1"}}},
	})
	require.NoError(t, err)

	_, err = r.SelectWriteTarget("library/movies/a.mkv")
	require.Error(t, err)
	require.ErrorIs(t, err, errkind.ErrRequired)
	var re *errkind.RequiredError
	require.ErrorAs(t, err, &re)
	require.Equal(t, "write_policy", re.What)
}

// TestRouter_SelectWriteTarget_shouldRejectInvalidWritePolicy verifies SelectWriteTarget rejects unknown
// write policy values.
func TestRouter_SelectWriteTarget_shouldRejectInvalidWritePolicy(t *testing.T) {
	dir := t.TempDir()
	r, err := New(&config.MountConfig{
		StoragePaths: []config.StoragePath{{ID: "ssd1", Path: dir}},
		RoutingRules: []config.RoutingRule{{Match: "**", Targets: []string{"ssd1"}, WritePolicy: "nope"}},
	})
	require.NoError(t, err)

	_, err = r.SelectWriteTarget("x")
	require.Error(t, err)
	require.ErrorIs(t, err, errkind.ErrInvalid)
}

// TestRouter_SelectWriteTarget_shouldPreferPathPreserving verifies path_preserving prefers targets that
// already contain the parent directory.
func TestRouter_SelectWriteTarget_shouldPreferPathPreserving(t *testing.T) {
	rootA := t.TempDir()
	rootB := t.TempDir()
	require.NoError(t, os.MkdirAll(filepath.Join(rootB, "library", "movies"), 0o755))

	r, err := New(&config.MountConfig{
		StoragePaths: []config.StoragePath{
			{ID: "a", Path: rootA},
			{ID: "b", Path: rootB},
		},
		RoutingRules: []config.RoutingRule{{
			Match:          "**",
			Targets:        []string{"a", "b"},
			WritePolicy:    "first_found",
			PathPreserving: true,
		}},
	})
	require.NoError(t, err)

	tgt, err := r.SelectWriteTarget("library/movies/a.mkv")
	require.NoError(t, err)
	require.Equal(t, "b", tgt.ID)
}

// TestRouter_SelectWriteTarget_shouldReturnErrNoWriteSpace verifies min_free_gb constraints can result
// in a stable ErrNoWriteSpace sentinel.
func TestRouter_SelectWriteTarget_shouldReturnErrNoWriteSpace(t *testing.T) {
	rootA := t.TempDir()
	rootB := t.TempDir()

	r, err := New(&config.MountConfig{
		StoragePaths: []config.StoragePath{
			{ID: "a", Path: rootA, MinFreeGB: 1e9},
			{ID: "b", Path: rootB, MinFreeGB: 1e9},
		},
		RoutingRules: []config.RoutingRule{{
			Match:       "**",
			Targets:     []string{"a", "b"},
			WritePolicy: "first_found",
		}},
	})
	require.NoError(t, err)

	_, err = r.SelectWriteTarget("x")
	require.Error(t, err)
	require.ErrorIs(t, err, ErrNoWriteSpace)
}

// TestParentVirtualDir_shouldHandleRoot verifies parentVirtualDir returns an empty parent for root-level
// virtual paths.
func TestParentVirtualDir_shouldHandleRoot(t *testing.T) {
	require.Equal(t, "", parentVirtualDir("a.mkv"))
}
