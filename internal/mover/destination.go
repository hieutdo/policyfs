package mover

import (
	"errors"
	"fmt"
	"os"
	"path"
	"path/filepath"
	"sort"
	"strings"

	"github.com/hieutdo/policyfs/internal/config"
	"github.com/hieutdo/policyfs/internal/errkind"
)

// destChoice is a resolved destination root.
type destChoice struct {
	id   string
	root string
	free float64
}

// selectDestinations returns an ordered list of destination choices for a candidate.
func (p *planner) selectDestinations(j config.MoverJobConfig, dstIDs []string, c candidate) ([]destChoice, error) {
	if p == nil {
		return nil, &errkind.NilError{What: "planner"}
	}

	cands := append([]string{}, dstIDs...)
	parentDir := path.Dir(c.RelPath)
	if parentDir == "." {
		parentDir = ""
	}

	if j.Destination.PathPreserving && parentDir != "" {
		kept := []string{}
		for _, id := range cands {
			root := p.storageByID[id].Path
			physDir := filepath.Join(root, parentDir)
			if _, err := os.Stat(physDir); err == nil {
				kept = append(kept, id)
			}
		}
		if len(kept) > 0 {
			cands = kept
		}
	}

	// Filter min_free_gb.
	filtered := []destChoice{}
	for _, id := range cands {
		sp := p.storageByID[id]
		freeGB, err := p.freeSpaceGB(sp.Path)
		if err != nil {
			// Skip offline/unstatfs-able destinations.
			continue
		}
		if sp.MinFreeGB > 0 && freeGB < sp.MinFreeGB {
			continue
		}
		filtered = append(filtered, destChoice{id: id, root: sp.Path, free: freeGB})
	}
	if len(filtered) == 0 {
		return nil, errors.New("no destination available")
	}

	policy := strings.TrimSpace(j.Destination.Policy)
	if policy == "" {
		policy = config.DefaultMovePolicy
	}
	switch policy {
	case "first_found":
		// Keep order.
	case "most_free":
		sort.Slice(filtered, func(i, j int) bool { return filtered[i].free > filtered[j].free })
	case "least_free":
		sort.Slice(filtered, func(i, j int) bool { return filtered[i].free < filtered[j].free })
	default:
		return nil, fmt.Errorf("invalid destination policy: %s", policy)
	}

	return filtered, nil
}
