package humanfmt

import (
	"testing"

	"github.com/stretchr/testify/require"
)

// TestFormatBytesIEC_shouldFormat verifies IEC formatting (KiB/MiB/GiB/TiB) with fixed decimals.
func TestFormatBytesIEC_shouldFormat(t *testing.T) {
	t.Run("should format bytes", func(t *testing.T) {
		require.Equal(t, "0 B", FormatBytesIEC(0, 1))
		require.Equal(t, "1 B", FormatBytesIEC(1, 1))
	})

	t.Run("should format kib", func(t *testing.T) {
		require.Equal(t, "1.0 KiB", FormatBytesIEC(1024, 1))
		require.Equal(t, "1.5 KiB", FormatBytesIEC(1536, 1))
	})

	t.Run("should respect decimals", func(t *testing.T) {
		require.Equal(t, "1.50 KiB", FormatBytesIEC(1536, 2))
	})

	t.Run("should preserve fixed width for round numbers", func(t *testing.T) {
		require.Equal(t, "9.0 GiB", FormatBytesIEC(9*1024*1024*1024, 1))
		require.Equal(t, "10.0 GiB", FormatBytesIEC(10*1024*1024*1024, 1))
	})
}

// TestFormatBytesSI_shouldFormat verifies SI formatting (KB/MB/GB/TB) with fixed decimals.
func TestFormatBytesSI_shouldFormat(t *testing.T) {
	require.Equal(t, "1.0 KB", FormatBytesSI(1000, 1))
	require.Equal(t, "1.5 KB", FormatBytesSI(1500, 1))
}

// TestFormatBytes_shouldKeepFixedDecimals verifies we keep fixed decimal places.
func TestFormatBytes_shouldKeepFixedDecimals(t *testing.T) {
	require.Equal(t, "1.00 KiB", FormatBytesIEC(1024, 2))
	require.Equal(t, "1.25 KiB", FormatBytesIEC(1280, 2))
}
