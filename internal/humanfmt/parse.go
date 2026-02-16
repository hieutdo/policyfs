package humanfmt

import (
	"fmt"
	"strconv"
	"strings"
	"time"
)

// ParseBytes parses a human-readable byte string into bytes.
//
// Supported examples:
//   - "100MB", "50 GB", "1.5GiB", "1024" (bytes)
//   - Units: B, KB, MB, GB, TB (SI, base 1000) and KiB, MiB, GiB, TiB (IEC, base 1024)
func ParseBytes(s string) (int64, error) {
	s = strings.TrimSpace(s)
	if s == "" {
		return 0, fmt.Errorf("invalid bytes: empty")
	}

	s = strings.ReplaceAll(s, " ", "")
	if s == "" {
		return 0, fmt.Errorf("invalid bytes: empty")
	}

	// Split numeric prefix and unit suffix.
	i := 0
	for i < len(s) {
		c := s[i]
		if (c >= '0' && c <= '9') || c == '.' {
			i++
			continue
		}
		break
	}
	if i == 0 {
		return 0, fmt.Errorf("invalid bytes: %q", s)
	}

	numStr := s[:i]
	unitStr := ""
	if i < len(s) {
		unitStr = s[i:]
	}

	v, err := strconv.ParseFloat(numStr, 64)
	if err != nil {
		return 0, fmt.Errorf("invalid bytes: %q", s)
	}
	if v < 0 {
		return 0, fmt.Errorf("invalid bytes: %q", s)
	}

	mult, ok := bytesMultiplier(unitStr)
	if !ok {
		return 0, fmt.Errorf("invalid bytes: %q", s)
	}

	out := v * float64(mult)
	if out > float64(int64(^uint64(0)>>1)) {
		return 0, fmt.Errorf("invalid bytes: %q", s)
	}
	return int64(out + 0.5), nil
}

// bytesMultiplier returns the unit multiplier for ParseBytes.
func bytesMultiplier(unit string) (int64, bool) {
	unit = strings.TrimSpace(unit)
	unit = strings.ToLower(unit)

	switch unit {
	case "", "b":
		return 1, true
	case "k", "kb":
		return 1000, true
	case "m", "mb":
		return 1000 * 1000, true
	case "g", "gb":
		return 1000 * 1000 * 1000, true
	case "t", "tb":
		return 1000 * 1000 * 1000 * 1000, true
	case "kib":
		return 1024, true
	case "mib":
		return 1024 * 1024, true
	case "gib":
		return 1024 * 1024 * 1024, true
	case "tib":
		return 1024 * 1024 * 1024 * 1024, true
	default:
		return 0, false
	}
}

// ParseDuration parses a human-readable duration string into a time.Duration.
//
// Supported examples:
//   - "30s", "5m", "2h", "7d", "1w", "1mo"
//   - Combined: "1d12h", "2w3d"
//
// Units:
//   - s (seconds), m (minutes), h (hours), d (days=24h), w (weeks=7d), mo (months=30d)
func ParseDuration(s string) (time.Duration, error) {
	s = strings.TrimSpace(s)
	if s == "" {
		return 0, fmt.Errorf("invalid duration: empty")
	}
	// Allow whitespace in user config: "7 d".
	s = strings.ReplaceAll(s, " ", "")

	var total time.Duration
	for i := 0; i < len(s); {
		start := i
		for i < len(s) {
			c := s[i]
			if c >= '0' && c <= '9' {
				i++
				continue
			}
			break
		}
		if start == i {
			return 0, fmt.Errorf("invalid duration: %q", s)
		}

		numStr := s[start:i]
		n, err := strconv.ParseInt(numStr, 10, 64)
		if err != nil || n < 0 {
			return 0, fmt.Errorf("invalid duration: %q", s)
		}
		if i >= len(s) {
			return 0, fmt.Errorf("invalid duration: %q", s)
		}

		unit, next, ok := parseDurationUnit(s, i)
		if !ok {
			return 0, fmt.Errorf("invalid duration: %q", s)
		}
		i = next

		part := unit * time.Duration(n)
		if part < 0 {
			return 0, fmt.Errorf("invalid duration: %q", s)
		}
		total += part
	}

	return total, nil
}

// parseDurationUnit parses the unit starting at s[i:] for ParseDuration.
func parseDurationUnit(s string, i int) (time.Duration, int, bool) {
	if i >= len(s) {
		return 0, i, false
	}
	// Prefer "mo" over "m".
	if i+1 < len(s) && s[i] == 'm' && s[i+1] == 'o' {
		return 30 * 24 * time.Hour, i + 2, true
	}

	switch s[i] {
	case 's':
		return time.Second, i + 1, true
	case 'm':
		return time.Minute, i + 1, true
	case 'h':
		return time.Hour, i + 1, true
	case 'd':
		return 24 * time.Hour, i + 1, true
	case 'w':
		return 7 * 24 * time.Hour, i + 1, true
	default:
		return 0, i, false
	}
}
