package humanfmt

import (
	"testing"
	"time"

	"github.com/stretchr/testify/require"
)

// TestParseBytes_shouldParseSIAndIEC verifies ParseBytes supports SI and IEC units.
func TestParseBytes_shouldParseSIAndIEC(t *testing.T) {
	t.Run("should parse bytes with no unit", func(t *testing.T) {
		b, err := ParseBytes("1024")
		require.NoError(t, err)
		require.Equal(t, int64(1024), b)
	})

	t.Run("should parse SI units", func(t *testing.T) {
		b, err := ParseBytes("1.5GB")
		require.NoError(t, err)
		require.Equal(t, int64(1500000000), b)
	})

	t.Run("should parse IEC units", func(t *testing.T) {
		b, err := ParseBytes("1.5GiB")
		require.NoError(t, err)
		require.Equal(t, int64(1610612736), b)
	})
}

// TestParseBytes_shouldRejectInvalid verifies ParseBytes rejects invalid strings.
func TestParseBytes_shouldRejectInvalid(t *testing.T) {
	_, err := ParseBytes("nope")
	require.Error(t, err)
}

// TestParseDuration_shouldParseUnitsAndCombined verifies ParseDuration supports v1 duration formats.
func TestParseDuration_shouldParseUnitsAndCombined(t *testing.T) {
	t.Run("should parse days and hours", func(t *testing.T) {
		d, err := ParseDuration("1d12h")
		require.NoError(t, err)
		require.Equal(t, 36*time.Hour, d)
	})

	t.Run("should parse weeks and days", func(t *testing.T) {
		d, err := ParseDuration("2w3d")
		require.NoError(t, err)
		require.Equal(t, (17*24)*time.Hour, d)
	})

	t.Run("should parse months", func(t *testing.T) {
		d, err := ParseDuration("1mo")
		require.NoError(t, err)
		require.Equal(t, 30*24*time.Hour, d)
	})
}

// TestParseDuration_shouldRejectInvalid verifies ParseDuration rejects invalid strings.
func TestParseDuration_shouldRejectInvalid(t *testing.T) {
	_, err := ParseDuration("1x")
	require.Error(t, err)
}
