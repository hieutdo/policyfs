package pathmatch

import (
	"testing"

	"github.com/stretchr/testify/require"
)

// TestPatternMatch_GlobSyntax verifies glob syntax matches routing semantics.
func TestPatternMatch_GlobSyntax(t *testing.T) {
	cases := []struct {
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

	for _, tc := range cases {
		t.Run(tc.name, func(t *testing.T) {
			pat, err := Compile(tc.pattern)
			require.NoError(t, err)
			require.Equal(t, tc.match, pat.Match(tc.path))
		})
	}
}

// TestPatternCanMatchDescendant_shouldMatchPrefix verifies CanMatchDescendant works for directory listing union.
func TestPatternCanMatchDescendant_shouldMatchPrefix(t *testing.T) {
	cases := []struct {
		name    string
		pattern string
		dir     string
		want    bool
	}{
		{
			name:    "should match ancestor dir",
			pattern: "library/movies/**",
			dir:     "library",
			want:    true,
		},
		{
			name:    "should match dir itself",
			pattern: "library/movies/**",
			dir:     "library/movies",
			want:    true,
		},
		{
			name:    "should not match sibling dir",
			pattern: "library/movies/**",
			dir:     "library/music",
			want:    false,
		},
		{
			name:    "should match exact dir rule for listing",
			pattern: "library",
			dir:     "library",
			want:    true,
		},
		{
			name:    "should match root ancestor",
			pattern: "library",
			dir:     "",
			want:    true,
		},
		{
			name:    "should not match deeper dir when pattern is exact",
			pattern: "library",
			dir:     "library/movies",
			want:    false,
		},
		{
			name:    "should match when pattern can match entries directly under dir",
			pattern: "library/*.mkv",
			dir:     "library",
			want:    true,
		},
	}

	for _, tc := range cases {
		t.Run(tc.name, func(t *testing.T) {
			pat, err := Compile(tc.pattern)
			require.NoError(t, err)
			require.Equal(t, tc.want, pat.CanMatchDescendant(tc.dir))
		})
	}
}

// TestMatcherMatch_shouldMatchAnyPattern verifies Match returns true when any compiled pattern matches.
func TestMatcherMatch_shouldMatchAnyPattern(t *testing.T) {
	m, err := NewMatcher([]string{"cache/**", "**/.DS_Store"})
	require.NoError(t, err)

	require.True(t, m.Match("cache/a.txt"))
	require.True(t, m.Match("library/movies/.DS_Store"))
	require.False(t, m.Match("library/movies/a.mkv"))
}
