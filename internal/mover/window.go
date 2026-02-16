package mover

import (
	"fmt"
	"time"
)

// inAllowedWindow reports whether now is inside the configured window and returns the window end time.
func inAllowedWindow(now time.Time, start string, end string) (bool, time.Time, error) {
	st, err := time.Parse("15:04", start)
	if err != nil {
		return false, time.Time{}, fmt.Errorf("invalid allowed_window.start: %w", err)
	}
	et, err := time.Parse("15:04", end)
	if err != nil {
		return false, time.Time{}, fmt.Errorf("invalid allowed_window.end: %w", err)
	}

	// Construct candidate window bounds relative to now, including cross-midnight windows.
	startToday := time.Date(now.Year(), now.Month(), now.Day(), st.Hour(), st.Minute(), 0, 0, now.Location())
	endToday := time.Date(now.Year(), now.Month(), now.Day(), et.Hour(), et.Minute(), 0, 0, now.Location())

	var winStart time.Time
	var winEnd time.Time
	if !startToday.After(endToday) {
		winStart = startToday
		winEnd = endToday
	} else {
		// Cross-midnight window.
		if now.Equal(startToday) || now.After(startToday) {
			winStart = startToday
			winEnd = endToday.Add(24 * time.Hour)
		} else {
			winStart = startToday.Add(-24 * time.Hour)
			winEnd = endToday
		}
	}

	inside := (now.Equal(winStart) || now.After(winStart)) && now.Before(winEnd)
	return inside, winEnd, nil
}
