package mover

import (
	"fmt"
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
