package mover

import (
	"fmt"
	"sort"
	"strings"
	"time"

	"github.com/hieutdo/policyfs/internal/config"
	"github.com/hieutdo/policyfs/internal/errkind"
)

// planner contains shared state for counting and running jobs.
type planner struct {
	mountName string
	mountCfg  *config.MountConfig

	opts Opts

	storageByID map[string]config.StoragePath
	groups      map[string][]string
	now         func() time.Time
	usagePct    func(path string) (float64, error)
	freeSpaceGB func(path string) (float64, error)
}

// newPlanner constructs a planner for one run.
func newPlanner(mountName string, mountCfg *config.MountConfig, opts Opts) *planner {
	storageByID := map[string]config.StoragePath{}
	for _, sp := range mountCfg.StoragePaths {
		storageByID[sp.ID] = sp
	}
	groups := map[string][]string{}
	for k, v := range mountCfg.StorageGroups {
		groups[k] = append([]string{}, v...)
	}
	return &planner{
		mountName:   mountName,
		mountCfg:    mountCfg,
		opts:        opts,
		storageByID: storageByID,
		groups:      groups,
		now:         time.Now,
		usagePct:    usagePercent,
		freeSpaceGB: freeSpaceGB,
	}
}

// selectJobs filters jobs by enabled + --job.
func (p *planner) selectJobs() ([]config.MoverJobConfig, error) {
	if p == nil {
		return nil, &errkind.NilError{What: "planner"}
	}
	jobs := p.mountCfg.Mover.Jobs
	if strings.TrimSpace(p.opts.Job) == "" {
		return append([]config.MoverJobConfig{}, jobs...), nil
	}

	name := strings.TrimSpace(p.opts.Job)
	for _, j := range jobs {
		if j.Name == name {
			return []config.MoverJobConfig{j}, nil
		}
	}
	return nil, &errkind.NotFoundError{Msg: fmt.Sprintf("config: mover job %q not found", name)}
}

// expandRefs expands storage IDs and group names into storage IDs.
func (p *planner) expandRefs(paths []string, groups []string) ([]string, error) {
	if p == nil {
		return nil, &errkind.NilError{What: "planner"}
	}

	in := []string{}
	in = append(in, paths...)
	in = append(in, groups...)

	out := []string{}
	seen := map[string]struct{}{}
	for _, id := range in {
		id = strings.TrimSpace(id)
		if id == "" {
			continue
		}
		if _, ok := p.storageByID[id]; ok {
			if _, dup := seen[id]; !dup {
				seen[id] = struct{}{}
				out = append(out, id)
			}
			continue
		}
		if members, ok := p.groups[id]; ok {
			for _, m := range members {
				m = strings.TrimSpace(m)
				if m == "" {
					continue
				}
				if _, ok := p.storageByID[m]; !ok {
					return nil, &errkind.InvalidError{Msg: fmt.Sprintf("config: storage_groups %q references unknown storage id %q", id, m)}
				}
				if _, dup := seen[m]; dup {
					continue
				}
				seen[m] = struct{}{}
				out = append(out, m)
			}
			continue
		}
		return nil, &errkind.NotFoundError{Msg: fmt.Sprintf("unknown storage id or group: %s", id)}
	}
	return out, nil
}

// debugDestinationsForJob collects job-level destination policy inputs.
//
// Note: path_preserving depends on each candidate path (parent existence), so this debug output does not
// attempt to simulate that step. It only reports statfs-based eligibility and the policy ordering.
func (p *planner) debugDestinationsForJob(j config.MoverJobConfig) *JobDestinationDebug {
	if p == nil {
		return nil
	}

	policy := strings.TrimSpace(j.Destination.Policy)
	if policy == "" {
		policy = config.DefaultMovePolicy
	}

	dstIDs, err := p.expandRefs(j.Destination.Paths, j.Destination.Groups)
	if err != nil {
		return &JobDestinationDebug{
			JobName:        j.Name,
			Note:           err.Error(),
			Policy:         policy,
			PathPreserving: j.Destination.PathPreserving,
		}
	}

	entries := make([]DestinationDebugEntry, 0, len(dstIDs))
	eligible := make([]DestinationDebugEntry, 0, len(dstIDs))
	for _, id := range dstIDs {
		sp := p.storageByID[id]
		de := DestinationDebugEntry{StorageID: id, MinFreeGB: sp.MinFreeGB}

		usePct, uerr := p.usagePct(sp.Path)
		freeGB, ferr := p.freeSpaceGB(sp.Path)
		if uerr == nil {
			de.UsePct = usePct
		}
		if ferr == nil {
			de.FreeGB = freeGB
		}

		if uerr != nil || ferr != nil {
			de.Eligible = false
			de.Reason = "statfs_failed"
			entries = append(entries, de)
			continue
		}
		if sp.MinFreeGB > 0 && freeGB < sp.MinFreeGB {
			de.Eligible = false
			de.Reason = "below_min_free_gb"
			entries = append(entries, de)
			continue
		}

		de.Eligible = true
		entries = append(entries, de)
		eligible = append(eligible, de)
	}

	ordered := append([]DestinationDebugEntry{}, eligible...)
	switch policy {
	case "first_found":
		// Keep config order.
	case "most_free":
		sort.SliceStable(ordered, func(i, j int) bool { return ordered[i].FreeGB > ordered[j].FreeGB })
	case "least_free":
		sort.SliceStable(ordered, func(i, j int) bool { return ordered[i].FreeGB < ordered[j].FreeGB })
	default:
		return &JobDestinationDebug{
			JobName:        j.Name,
			Note:           fmt.Sprintf("invalid destination policy: %s", policy),
			Policy:         policy,
			PathPreserving: j.Destination.PathPreserving,
			Destinations:   entries,
		}
	}

	orderedIDs := make([]string, 0, len(ordered))
	for _, e := range ordered {
		orderedIDs = append(orderedIDs, e.StorageID)
	}
	primary := ""
	if len(orderedIDs) > 0 {
		primary = orderedIDs[0]
	}

	note := ""
	if j.Destination.PathPreserving {
		note = "primary ignores path_preserving; actual dest chosen per file"
	}

	return &JobDestinationDebug{
		JobName:         j.Name,
		Note:            note,
		Policy:          policy,
		PathPreserving:  j.Destination.PathPreserving,
		OrderedEligible: orderedIDs,
		PrimaryChoice:   primary,
		Destinations:    entries,
	}
}
