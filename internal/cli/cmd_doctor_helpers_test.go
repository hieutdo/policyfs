package cli

import (
	"testing"

	"github.com/stretchr/testify/require"
)

// TestSplitVirtualDirAndPrefix_shouldHandleEmptyAndRoot verifies splitVirtualDirAndPrefix handles
// empty input and root-like paths.
func TestSplitVirtualDirAndPrefix_shouldHandleEmptyAndRoot(t *testing.T) {
	t.Run("should return empty for empty", func(t *testing.T) {
		dir, prefix := splitVirtualDirAndPrefix("")
		require.Equal(t, "", dir)
		require.Equal(t, "", prefix)
	})

	t.Run("should return empty for root", func(t *testing.T) {
		dir, prefix := splitVirtualDirAndPrefix("/")
		require.Equal(t, "", dir)
		require.Equal(t, "", prefix)
	})
}

// TestSplitVirtualDirAndPrefix_shouldSplitDirAndNamePrefix verifies splitVirtualDirAndPrefix produces
// (dirPath, namePrefix) suitable for indexdb prefix listing.
func TestSplitVirtualDirAndPrefix_shouldSplitDirAndNamePrefix(t *testing.T) {
	cases := []struct {
		name       string
		in         string
		wantDir    string
		wantPrefix string
	}{
		{name: "should treat trailing slash as dir", in: "library/movies/", wantDir: "library/movies", wantPrefix: ""},
		{name: "should normalize leading slash and double slashes", in: "/library//movies/", wantDir: "library/movies", wantPrefix: ""},
		{name: "should split parent and prefix", in: "library/mov", wantDir: "library", wantPrefix: "mov"},
		{name: "should handle root-level name", in: "abc", wantDir: "", wantPrefix: "abc"},
		{name: "should clean dot segments", in: "library/../movies/a", wantDir: "movies", wantPrefix: "a"},
	}

	for _, tc := range cases {
		t.Run(tc.name, func(t *testing.T) {
			dir, prefix := splitVirtualDirAndPrefix(tc.in)
			require.Equal(t, tc.wantDir, dir)
			require.Equal(t, tc.wantPrefix, prefix)
		})
	}
}
