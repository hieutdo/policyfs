package router

import (
	"fmt"
	"os"
	"path/filepath"
	"strings"
	"syscall"

	"github.com/hieutdo/policyfs/internal/config"
	"github.com/hieutdo/policyfs/internal/errkind"
	"github.com/hieutdo/policyfs/internal/pathmatch"
)

var (
	// ErrNoRuleMatched is returned when no routing rule matches a virtual path.
	ErrNoRuleMatched = errkind.SentinelError("no routing rule matched")
	// ErrNoTargetsResolved is returned when no targets can be resolved after expansion/deduping.
	ErrNoTargetsResolved = errkind.SentinelError("no targets resolved")
	// ErrNoWriteSpace is returned when no write target satisfies min_free_gb constraints.
	ErrNoWriteSpace = errkind.SentinelError("no write target has enough free space")
)

// Target is a resolved storage target for a virtual path.
type Target struct {
	ID      string
	Root    string
	Indexed bool
}

// Router resolves virtual paths to storage targets based on mount routing rules.
type Router struct {
	storageByID   map[string]config.StoragePath
	storageGroups map[string][]string
	rules         []compiledRule
}

// compiledRule is an internal compiled routing rule.
type compiledRule struct {
	rule  config.RoutingRule
	read  []string
	write []string
	pat   *pathmatch.Pattern
}

// normalizeRuleMatch normalizes a routing rule match string for comparisons.
func normalizeRuleMatch(p string) string {
	p = strings.TrimSpace(p)
	p = strings.TrimPrefix(p, "/")
	p = strings.TrimSuffix(p, "/")
	for strings.Contains(p, "//") {
		p = strings.ReplaceAll(p, "//", "/")
	}
	return p
}

// isCatchAllMatch reports whether a routing match string is equivalent to the catch-all pattern "**".
func isCatchAllMatch(match string) bool {
	return normalizeRuleMatch(match) == "**"
}

// New builds a Router from a mount config.
func New(m *config.MountConfig) (*Router, error) {
	if m == nil {
		return nil, &errkind.NilError{What: "mount config"}
	}
	if len(m.StoragePaths) == 0 {
		return nil, &errkind.RequiredError{Msg: "config: storage_paths must not be empty"}
	}
	if len(m.RoutingRules) == 0 {
		return nil, &errkind.RequiredError{Msg: "config: routing_rules must not be empty"}
	}

	catchAllIdx := []int{}
	for i, rr := range m.RoutingRules {
		if isCatchAllMatch(rr.Match) {
			catchAllIdx = append(catchAllIdx, i)
		}
	}
	if len(catchAllIdx) == 0 {
		return nil, &errkind.RequiredError{Msg: "config: missing catch-all rule '**'"}
	}
	if len(catchAllIdx) > 1 {
		return nil, &errkind.InvalidError{Msg: "config: multiple catch-all rules '**'"}
	}
	if catchAllIdx[0] != len(m.RoutingRules)-1 {
		return nil, &errkind.InvalidError{Msg: "config: catch-all rule '**' must be last"}
	}

	storageByID := make(map[string]config.StoragePath, len(m.StoragePaths))
	for _, sp := range m.StoragePaths {
		if strings.TrimSpace(sp.ID) == "" {
			return nil, &errkind.RequiredError{Msg: "config: storage_paths.id is required"}
		}
		if strings.TrimSpace(sp.Path) == "" {
			return nil, &errkind.RequiredError{Msg: fmt.Sprintf("config: storage_paths %q: path is required", sp.ID)}
		}
		storageByID[sp.ID] = sp
	}

	storageGroups := map[string][]string{}
	for k, v := range m.StorageGroups {
		storageGroups[k] = append([]string{}, v...)
	}

	r := &Router{storageByID: storageByID, storageGroups: storageGroups}
	for i, rr := range m.RoutingRules {
		if strings.TrimSpace(rr.Match) == "" {
			return nil, &errkind.RequiredError{Msg: fmt.Sprintf("config: routing_rules[%d].match is required", i)}
		}

		pat, err := pathmatch.Compile(rr.Match)
		if err != nil {
			return nil, fmt.Errorf("failed to compile routing rule pattern: %w", err)
		}

		read := rr.ReadTargets
		if len(read) == 0 {
			read = rr.Targets
		}
		write := rr.WriteTargets
		if len(write) == 0 {
			write = rr.Targets
		}

		r.rules = append(r.rules, compiledRule{rule: rr, read: read, write: write, pat: pat})
	}
	return r, nil
}

// ResolveReadTargets returns storage targets for reads for the given virtual path.
func (r *Router) ResolveReadTargets(virtualPath string) ([]Target, error) {
	if r == nil {
		return nil, &errkind.NilError{What: "router"}
	}

	cr, ok := r.matchRule(virtualPath)
	if !ok {
		return nil, &errkind.KindError{Kind: ErrNoRuleMatched, Msg: fmt.Sprintf("no routing rule matched for path: %s", virtualPath)}
	}
	ids, err := r.expandTargets(cr.read)
	if err != nil {
		return nil, err
	}
	return r.targetsFromIDs(ids)
}

// ResolveWriteTargets returns storage targets for writes for the given virtual path.
func (r *Router) ResolveWriteTargets(virtualPath string) ([]Target, error) {
	if r == nil {
		return nil, &errkind.NilError{What: "router"}
	}

	cr, ok := r.matchRule(virtualPath)
	if !ok {
		return nil, &errkind.KindError{Kind: ErrNoRuleMatched, Msg: fmt.Sprintf("no routing rule matched for path: %s", virtualPath)}
	}
	ids, err := r.expandTargets(cr.write)
	if err != nil {
		return nil, err
	}
	return r.targetsFromIDs(ids)
}

// SelectWriteTarget selects a single write target for a virtual path.
//
// This applies mount routing rule attributes: path_preserving, min_free_gb, and write_policy.
func (r *Router) SelectWriteTarget(virtualPath string) (Target, error) {
	if r == nil {
		return Target{}, &errkind.NilError{What: "router"}
	}

	cr, ok := r.matchRule(virtualPath)
	if !ok {
		return Target{}, &errkind.KindError{Kind: ErrNoRuleMatched, Msg: fmt.Sprintf("no routing rule matched for path: %s", virtualPath)}
	}

	ids, err := r.expandTargets(cr.write)
	if err != nil {
		return Target{}, err
	}
	candidates, err := r.targetsFromIDs(ids)
	if err != nil {
		return Target{}, err
	}

	parent := parentVirtualDir(virtualPath)
	if cr.rule.PathPreserving {
		preferred := []Target{}
		for _, t := range candidates {
			if strings.TrimSpace(parent) == "" {
				preferred = append(preferred, t)
				continue
			}
			pp := filepath.Join(t.Root, parent)
			fi, err := os.Stat(pp)
			if err != nil {
				continue
			}
			if fi.IsDir() {
				preferred = append(preferred, t)
			}
		}
		if len(preferred) > 0 {
			candidates = preferred
		}
	}

	policy := strings.TrimSpace(cr.rule.WritePolicy)
	if policy == "" {
		return Target{}, &errkind.RequiredError{What: "write_policy", Msg: "write_policy is required"}
	}

	filtered := []Target{}
	filteredScores := []float64{}

	for _, t := range candidates {
		freeGB, err := freeSpaceGB(t.Root)
		if err != nil {
			return Target{}, fmt.Errorf("failed to statfs write target: %w", err)
		}

		sp, ok := r.storageByID[t.ID]
		if !ok {
			return Target{}, &errkind.InvalidError{Msg: fmt.Sprintf("unknown storage id %q", t.ID)}
		}
		if sp.MinFreeGB > 0 && freeGB < sp.MinFreeGB {
			continue
		}
		filtered = append(filtered, t)
		filteredScores = append(filteredScores, freeGB)
	}
	if len(filtered) == 0 {
		return Target{}, &errkind.KindError{Kind: ErrNoWriteSpace, Msg: fmt.Sprintf("no write target has enough free space for path: %s", virtualPath)}
	}

	switch policy {
	case "first_found":
		return filtered[0], nil
	case "most_free":
		best := 0
		for i := 1; i < len(filtered); i++ {
			if filteredScores[i] > filteredScores[best] {
				best = i
			}
		}
		return filtered[best], nil
	case "least_free":
		best := 0
		for i := 1; i < len(filtered); i++ {
			if filteredScores[i] < filteredScores[best] {
				best = i
			}
		}
		return filtered[best], nil
	default:
		return Target{}, &errkind.InvalidError{Msg: fmt.Sprintf("invalid write_policy %q", policy)}
	}
}

// matchRule returns the first routing rule that matches a path.
func (r *Router) matchRule(virtualPath string) (compiledRule, bool) {
	for _, cr := range r.rules {
		if cr.pat.Match(virtualPath) {
			return cr, true
		}
	}
	return compiledRule{}, false
}

// ResolveMountWriteTargets returns the union of all configured write targets for this mount.
//
// Unlike ResolveWriteTargets (first match wins), mount-wide statfs reporting must consider
// all routing rules so that the result is stable for the mountpoint.
func (r *Router) ResolveMountWriteTargets() ([]Target, error) {
	if r == nil {
		return nil, &errkind.NilError{What: "router"}
	}

	union := []string{}
	for _, cr := range r.rules {
		union = append(union, cr.write...)
	}

	ids, err := r.expandTargets(union)
	if err != nil {
		return nil, err
	}
	return r.targetsFromIDs(ids)
}

// ResolveListTargets returns the union of storage targets for directory listings.
//
// Unlike read/write routing (first match wins), directory listings must consider
// all rules that could match descendants under the given directory.
func (r *Router) ResolveListTargets(virtualDirPath string) ([]Target, error) {
	if r == nil {
		return nil, &errkind.NilError{What: "router"}
	}

	union := []string{}
	matchedAny := false
	for _, cr := range r.rules {
		if !cr.pat.CanMatchDescendant(virtualDirPath) {
			continue
		}
		matchedAny = true
		union = append(union, cr.read...)
	}
	if !matchedAny {
		return nil, &errkind.KindError{Kind: ErrNoRuleMatched, Msg: fmt.Sprintf("no routing rule matched for path: %s", virtualDirPath)}
	}

	ids, err := r.expandTargets(union)
	if err != nil {
		return nil, err
	}
	return r.targetsFromIDs(ids)
}

// expandTargets expands storage groups into storage IDs and dedupes while preserving order.
func (r *Router) expandTargets(ids []string) ([]string, error) {
	out := []string{}
	seen := map[string]struct{}{}
	for _, id := range ids {
		id = strings.TrimSpace(id)
		if id == "" {
			continue
		}
		if sp, ok := r.storageByID[id]; ok {
			if _, dup := seen[sp.ID]; dup {
				continue
			}
			seen[sp.ID] = struct{}{}
			out = append(out, sp.ID)
			continue
		}
		if members, ok := r.storageGroups[id]; ok {
			for _, m := range members {
				m = strings.TrimSpace(m)
				if m == "" {
					continue
				}
				if _, ok := r.storageByID[m]; !ok {
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
		return nil, &errkind.InvalidError{Msg: fmt.Sprintf("unknown target id %q", id)}
	}
	if len(out) == 0 {
		return nil, &errkind.KindError{Kind: ErrNoTargetsResolved, Msg: "no targets resolved"}
	}
	return out, nil
}

// targetsFromIDs converts storage IDs into resolved targets.
func (r *Router) targetsFromIDs(ids []string) ([]Target, error) {
	out := make([]Target, 0, len(ids))
	for _, id := range ids {
		sp, ok := r.storageByID[id]
		if !ok {
			return nil, &errkind.InvalidError{Msg: fmt.Sprintf("unknown storage id %q", id)}
		}
		out = append(out, Target{ID: sp.ID, Root: sp.Path, Indexed: sp.Indexed})
	}
	return out, nil
}

// parentVirtualDir returns the parent directory of a virtual path.
func parentVirtualDir(virtualPath string) string {
	p := pathmatch.NormalizePath(virtualPath)
	parent := filepath.Dir(p)
	if parent == "." {
		return ""
	}
	return parent
}

// freeSpaceGB returns free disk space for a filesystem path in GiB.
func freeSpaceGB(path string) (float64, error) {
	var st syscall.Statfs_t
	if err := syscall.Statfs(path, &st); err != nil {
		return 0, fmt.Errorf("failed to statfs %q: %w", path, err)
	}
	free := float64(st.Bavail) * float64(st.Bsize)
	return free / (1024.0 * 1024.0 * 1024.0), nil
}
