package humanfmt

import (
	"fmt"
	"strings"
)

// BytesUnitSystem selects whether to use IEC (binary, 1024) or SI (decimal, 1000) units.
type BytesUnitSystem int

const (
	BytesIEC BytesUnitSystem = iota
	BytesSI
)

// BytesFormatOptions configures FormatBytes.
type BytesFormatOptions struct {
	System   BytesUnitSystem
	Decimals int
}

// FormatBytes formats a byte count into a human-readable string.
func FormatBytes(totalBytes int64, opts BytesFormatOptions) string {
	if opts.Decimals < 0 {
		opts.Decimals = 0
	}

	switch opts.System {
	case BytesSI:
		return formatBytes(totalBytes, 1000, []string{"B", "KB", "MB", "GB", "TB"}, opts.Decimals)
	default:
		return formatBytes(totalBytes, 1024, []string{"B", "KiB", "MiB", "GiB", "TiB"}, opts.Decimals)
	}
}

// FormatBytesIEC formats a byte count using IEC units (B, KiB, MiB, GiB, TiB).
func FormatBytesIEC(totalBytes int64, decimals int) string {
	return FormatBytes(totalBytes, BytesFormatOptions{System: BytesIEC, Decimals: decimals})
}

// FormatBytesSI formats a byte count using SI units (B, KB, MB, GB, TB).
func FormatBytesSI(totalBytes int64, decimals int) string {
	return FormatBytes(totalBytes, BytesFormatOptions{System: BytesSI, Decimals: decimals})
}

// formatBytes is the shared formatter for IEC/SI.
func formatBytes(totalBytes int64, base int64, units []string, decimals int) string {
	if totalBytes <= 0 {
		return "0 B"
	}
	if base <= 0 {
		base = 1024
	}
	if len(units) == 0 {
		units = []string{"B"}
	}

	if totalBytes < base || len(units) == 1 {
		return fmt.Sprintf("%d B", totalBytes)
	}

	scaled := float64(totalBytes)
	idx := 0
	for idx+1 < len(units) && scaled >= float64(base) {
		scaled /= float64(base)
		idx++
	}

	if idx == 0 {
		return fmt.Sprintf("%d B", totalBytes)
	}

	out := fmt.Sprintf("%.*f %s", decimals, scaled, units[idx])
	return trimTrailingZeros(out)
}

// trimTrailingZeros removes redundant trailing zeros and a trailing decimal point.
func trimTrailingZeros(s string) string {
	parts := strings.SplitN(s, " ", 2)
	if len(parts) != 2 {
		return s
	}

	num := parts[0]
	unit := parts[1]
	if !strings.Contains(num, ".") {
		return s
	}

	num = strings.TrimRight(num, "0")
	num = strings.TrimRight(num, ".")
	return num + " " + unit
}
