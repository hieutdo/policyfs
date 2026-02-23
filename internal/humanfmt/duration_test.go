package humanfmt

import (
	"testing"
	"time"

	"github.com/stretchr/testify/require"
)

// TestHumanizeDuration_shouldFormat verifies HumanizeDuration produces stable, human-friendly durations.
func TestHumanizeDuration_shouldFormat(t *testing.T) {
	tests := []struct {
		d    time.Duration
		want string
	}{
		{30 * time.Second, "just now"},
		{42 * time.Minute, "42m"},
		{2 * time.Hour, "2h"},
		{2*time.Hour + 30*time.Minute, "2h 30m"},
		{14*time.Hour + 22*time.Minute, "14h 22m"},
		{24 * time.Hour, "1d"},
		{3*24*time.Hour + 14*time.Hour, "3d 14h"},
		{7 * 24 * time.Hour, "1w"},
		{14*24*time.Hour + 3*24*time.Hour, "2w 3d"},
		{30 * 24 * time.Hour, "1mo"},
		{65 * 24 * time.Hour, "2mo 5d"},
		{365 * 24 * time.Hour, "1y"},
		{395 * 24 * time.Hour, "1y 1mo"},
	}

	for _, tt := range tests {
		t.Run(tt.want, func(t *testing.T) {
			got := HumanizeDuration(tt.d)
			require.Equal(t, tt.want, got)
		})
	}
}
