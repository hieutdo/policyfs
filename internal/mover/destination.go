package mover

import (
	"fmt"
	"os"
	"path"
	"path/filepath"
	"sort"
	"strings"

	"github.com/hieutdo/policyfs/internal/config"
	"github.com/hieutdo/policyfs/internal/errkind"
)

// errNoDestinationAvailable indicates that no destination survives availability checks.
var errNoDestinationAvailable = errkind.SentinelError("no destination available")

// destChoice is a resolved destination root.
type destChoice struct {
	id   string
	root string
	free float64
}

// destResult is the result of selectDestinations.
type destResult struct {
	choices            []destChoice
	pathPreservingKept []string // destinations that passed path_preserving filter (nil if not applicable)
}

// filterAvailableDestinations keeps destinations that pass statfs and min_free_gb checks.
func (p *planner) filterAvailableDestinations(dstIDs []string) []destChoice {
	filtered := []destChoice{}
	for _, id := range dstIDs {
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
	return filtered
}

// selectDestinations returns policy-ordered destinations, preferring existing parent directories when possible.
func (p *planner) selectDestinations(j config.MoverJobConfig, dstIDs []string, c candidate) (destResult, error) {
	if p == nil {
		return destResult{}, &errkind.NilError{What: "planner"}
	}

	cands := append([]string{}, dstIDs...)
	parentDir := path.Dir(c.RelPath)
	if parentDir == "." {
		parentDir = ""
	}

	var ppKept []string
	if j.Destination.PathPreserving && parentDir != "" {
		ppKept = []string{} // non-nil: path_preserving was checked
		for _, id := range cands {
			root := p.storageByID[id].Path
			physDir := filepath.Join(root, parentDir)
			if _, err := os.Stat(physDir); err == nil {
				ppKept = append(ppKept, id)
			}
		}
		if len(ppKept) > 0 {
			cands = ppKept
		}
	}

	filtered := p.filterAvailableDestinations(cands)
	if len(filtered) == 0 && len(ppKept) > 0 {
		filtered = p.filterAvailableDestinations(dstIDs)
	}
	if len(filtered) == 0 {
		return destResult{}, errNoDestinationAvailable
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
		return destResult{}, fmt.Errorf("invalid destination policy: %s", policy)
	}

	return destResult{choices: filtered, pathPreservingKept: ppKept}, nil
}
