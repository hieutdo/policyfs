package cli

import (
	"strconv"
	"testing"

	"github.com/stretchr/testify/require"
)

// TestUnescapeMountinfoPath_shouldDecodeEscapes verifies unescapeMountinfoPath decodes Linux mountinfo
// escape sequences and returns errors for invalid escapes.
func TestUnescapeMountinfoPath_shouldDecodeEscapes(t *testing.T) {
	t.Run("should return input when no escapes", func(t *testing.T) {
		got, err := unescapeMountinfoPath("/mnt/data")
		require.NoError(t, err)
		require.Equal(t, "/mnt/data", got)
	})

	t.Run("should decode common escapes", func(t *testing.T) {
		got, err := unescapeMountinfoPath("/mnt/My\\040Disk\\011tab")
		require.NoError(t, err)
		require.Equal(t, "/mnt/My Disk\ttab", got)
	})

	t.Run("should decode multiple escapes", func(t *testing.T) {
		got, err := unescapeMountinfoPath("a\\040b\\040c")
		require.NoError(t, err)
		require.Equal(t, "a b c", got)
	})

	t.Run("should return error for incomplete escape", func(t *testing.T) {
		got, err := unescapeMountinfoPath("a\\0")
		require.Error(t, err)
		require.Equal(t, "", got)
	})

	t.Run("should wrap parse errors", func(t *testing.T) {
		got, err := unescapeMountinfoPath("a\\xxxb")
		require.Error(t, err)
		require.Equal(t, "", got)
		var ne *strconv.NumError
		require.ErrorAs(t, err, &ne)
	})
}
