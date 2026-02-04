package router

import (
	"testing"

	"github.com/hieutdo/policyfs/internal/config"
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
		tc := tc
		t.Run(tc.name, func(t *testing.T) {
			// Build a 1-rule router to exercise matchRule directly.
			r, err := New(&config.MountConfig{
				StoragePaths: []config.StoragePath{{ID: "ssd1", Path: "/mnt/ssd1"}},
				RoutingRules: []config.RoutingRule{{Match: tc.pattern, Targets: []string{"ssd1"}}},
			})
			require.NoError(t, err)

			_, ok := r.matchRule(tc.path)
			require.Equal(t, tc.match, ok)
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
