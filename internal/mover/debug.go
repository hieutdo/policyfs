package mover

// PlanDebugEntry is one debug record produced during planning (discovery).
// It is intended for CLI diagnostics (pfs move --debug).
//
// Fields are kept simple and stable so tests can assert against them.
type PlanDebugEntry struct {
	JobName   string
	StorageID string
	Path      string
	Reason    string
	Detail    string
}

// PlanDebug summarizes debug output produced during planning.
type PlanDebug struct {
	Max          int
	Entries      []PlanDebugEntry
	Dropped      int
	Destinations []JobDestinationDebug
}

// DestinationDebugEntry is one destination stats record used for debugging policy selection.
type DestinationDebugEntry struct {
	StorageID string
	Eligible  bool
	Reason    string
	UsePct    float64
	FreeGB    float64
	MinFreeGB float64
}

// JobDestinationDebug summarizes destination policy inputs for one mover job.
type JobDestinationDebug struct {
	JobName         string
	Note            string
	Policy          string
	PathPreserving  bool
	OrderedEligible []string
	PrimaryChoice   string
	Destinations    []DestinationDebugEntry
}

// debugCollector collects debug entries with a hard cap.
// It is intentionally best-effort and should never affect move behavior.
type debugCollector struct {
	max          int
	entries      []PlanDebugEntry
	dropped      int
	destinations []JobDestinationDebug
}

// newDebugCollector constructs a collector with the given max.
func newDebugCollector(max int) *debugCollector {
	if max < 0 {
		max = 0
	}
	return &debugCollector{max: max}
}

// add appends a debug entry if the cap isn't reached.
func (c *debugCollector) add(e PlanDebugEntry) {
	if c == nil {
		return
	}
	if c.max <= 0 {
		c.dropped++
		return
	}
	if len(c.entries) >= c.max {
		c.dropped++
		return
	}
	c.entries = append(c.entries, e)
}

// result returns the final debug result.
func (c *debugCollector) result() *PlanDebug {
	if c == nil {
		return nil
	}
	out := &PlanDebug{
		Max:          c.max,
		Entries:      append([]PlanDebugEntry{}, c.entries...),
		Dropped:      c.dropped,
		Destinations: append([]JobDestinationDebug{}, c.destinations...),
	}
	return out
}

// addDestinationDebug appends one job-level destination debug record.
func (c *debugCollector) addDestinationDebug(d JobDestinationDebug) {
	if c == nil {
		return
	}
	c.destinations = append(c.destinations, d)
}
