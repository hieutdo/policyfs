package humanfmt

import (
	"fmt"
	"time"
)

// HumanizeDuration formats a duration in a human-friendly way.
//
//	< 1m   → "just now"
//	< 1h   → "42m"
//	< 24h  → "14h 22m"
//	< 7d   → "3d 14h"
//	< 30d  → "2w 3d"
//	< 365d → "2mo 5d"
//	≥ 365d → "1y 2mo"
func HumanizeDuration(d time.Duration) string {
	if d < time.Minute {
		return "just now"
	}

	totalMinutes := int(d.Minutes())
	totalHours := totalMinutes / 60
	totalDays := totalHours / 24

	if totalHours < 1 {
		return fmt.Sprintf("%dm", totalMinutes)
	}
	if totalDays < 1 {
		m := totalMinutes % 60
		if m == 0 {
			return fmt.Sprintf("%dh", totalHours)
		}
		return fmt.Sprintf("%dh %dm", totalHours, m)
	}
	if totalDays < 7 {
		h := totalHours % 24
		if h == 0 {
			return fmt.Sprintf("%dd", totalDays)
		}
		return fmt.Sprintf("%dd %dh", totalDays, h)
	}
	if totalDays < 30 {
		weeks := totalDays / 7
		days := totalDays % 7
		if days == 0 {
			return fmt.Sprintf("%dw", weeks)
		}
		return fmt.Sprintf("%dw %dd", weeks, days)
	}
	if totalDays < 365 {
		months := totalDays / 30
		days := totalDays % 30
		if days == 0 {
			return fmt.Sprintf("%dmo", months)
		}
		return fmt.Sprintf("%dmo %dd", months, days)
	}

	years := totalDays / 365
	remDays := totalDays % 365
	months := remDays / 30
	days := remDays % 30
	if months > 0 {
		return fmt.Sprintf("%dy %dmo", years, months)
	}
	if days > 0 {
		return fmt.Sprintf("%dy %dd", years, days)
	}
	return fmt.Sprintf("%dy", years)
}
