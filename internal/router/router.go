package router

import (
	"fmt"
	"os"
	"path/filepath"
	"strings"
	"syscall"

	"github.com/hieutdo/policyfs/internal/config"
	"github.com/hieutdo/policyfs/internal/errkind"
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
	segs  [][]string
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

		expanded, err := expandBraces(rr.Match)
		if err != nil {
			return nil, fmt.Errorf("failed to expand routing rule pattern: %w", err)
		}
		segs, err := parseGlobExpanded(expanded)
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

		r.rules = append(r.rules, compiledRule{rule: rr, read: read, write: write, segs: segs})
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
	p := normalizePath(virtualPath)
	pathSegs := splitSegments(p)
	for _, cr := range r.rules {
		if globMatchAny(cr.segs, pathSegs) {
			return cr, true
		}
	}
	return compiledRule{}, false
}

// ResolveListTargets returns the union of storage targets for directory listings.
//
// Unlike read/write routing (first match wins), directory listings must consider
// all rules that could match descendants under the given directory.
func (r *Router) ResolveListTargets(virtualDirPath string) ([]Target, error) {
	if r == nil {
		return nil, &errkind.NilError{What: "router"}
	}

	dir := normalizePath(virtualDirPath)
	dirSegs := splitSegments(dir)

	union := []string{}
	matchedAny := false
	for _, cr := range r.rules {
		if !ruleCanMatchDescendant(cr.segs, dirSegs) {
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

// parseGlobExpanded parses brace-expanded patterns into segment lists.
func parseGlobExpanded(expanded []string) ([][]string, error) {
	if len(expanded) == 0 {
		return nil, &errkind.InvalidError{Msg: "empty expanded patterns"}
	}

	segs := make([][]string, 0, len(expanded))
	for _, p := range expanded {
		p = normalizeGlobPattern(p)
		segs = append(segs, splitSegments(p))
	}
	return segs, nil
}

// expandBraces expands simple {a,b,c} brace syntax.
func expandBraces(pattern string) ([]string, error) {
	const maxExpansions = 64
	pattern = strings.TrimSpace(pattern)
	if pattern == "" {
		return nil, &errkind.InvalidError{Msg: "empty pattern"}
	}

	open := strings.IndexByte(pattern, '{')
	if open == -1 {
		return []string{pattern}, nil
	}
	close := strings.IndexByte(pattern[open+1:], '}')
	if close == -1 {
		return []string{pattern}, nil
	}
	close = open + 1 + close

	inner := pattern[open+1 : close]
	parts := strings.Split(inner, ",")
	if len(parts) == 0 {
		return []string{pattern}, nil
	}

	out := []string{}
	for _, part := range parts {
		part = strings.TrimSpace(part)
		candidate := pattern[:open] + part + pattern[close+1:]
		exp, err := expandBraces(candidate)
		if err != nil {
			return nil, err
		}
		out = append(out, exp...)
		if len(out) > maxExpansions {
			return nil, &errkind.InvalidError{Msg: "pattern expansion too large"}
		}
	}
	return out, nil
}

// normalizeGlobPattern normalizes patterns per routing spec.
func normalizeGlobPattern(p string) string {
	p = strings.TrimSpace(p)
	p = strings.TrimPrefix(p, "/")
	p = strings.TrimSuffix(p, "/")
	return collapseSlashes(p)
}

// normalizePath normalizes virtual paths per routing spec.
func normalizePath(p string) string {
	p = strings.TrimSpace(p)
	p = strings.TrimPrefix(p, "/")
	p = strings.TrimSuffix(p, "/")
	return collapseSlashes(p)
}

// collapseSlashes collapses repeated slashes into a single slash.
func collapseSlashes(s string) string {
	for strings.Contains(s, "//") {
		s = strings.ReplaceAll(s, "//", "/")
	}
	return s
}

// splitSegments splits a normalized path into segments.
func splitSegments(p string) []string {
	p = strings.TrimPrefix(p, "/")
	p = strings.TrimSuffix(p, "/")
	if strings.TrimSpace(p) == "" {
		return nil
	}
	return strings.Split(p, "/")
}

// parentVirtualDir returns the parent directory of a virtual path.
func parentVirtualDir(virtualPath string) string {
	p := normalizePath(virtualPath)
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

// ruleCanMatchDescendant reports whether any expanded pattern can match a path under dirSegs.
func ruleCanMatchDescendant(expandedSegs [][]string, dirSegs []string) bool {
	for _, segs := range expandedSegs {
		if globCanMatchDescendant(segs, dirSegs) {
			return true
		}
	}
	return false
}

// globCanMatchDescendant checks whether a pattern can match at least one descendant path.
func globCanMatchDescendant(patternSegs []string, dirSegs []string) bool {
	states := map[int]struct{}{0: {}}
	states = globEpsilonClose(patternSegs, states)
	for _, ds := range dirSegs {
		next := map[int]struct{}{}
		for i := range states {
			if i >= len(patternSegs) {
				continue
			}
			ps := patternSegs[i]
			if ps == "**" {
				next[i] = struct{}{}
				continue
			}
			if matchSegment(ps, ds) {
				next[i+1] = struct{}{}
			}
		}
		states = globEpsilonClose(patternSegs, next)
		if len(states) == 0 {
			return false
		}
	}
	return len(states) > 0
}

// globEpsilonClose expands states that can advance without consuming a segment (only `**`).
func globEpsilonClose(patternSegs []string, in map[int]struct{}) map[int]struct{} {
	out := map[int]struct{}{}
	for k := range in {
		out[k] = struct{}{}
	}
	changed := true
	for changed {
		changed = false
		for i := range out {
			if i < len(patternSegs) && patternSegs[i] == "**" {
				if _, ok := out[i+1]; !ok {
					out[i+1] = struct{}{}
					changed = true
				}
			}
		}
	}
	return out
}

// matchSegment matches a single path segment (no '/') against a glob segment.
func matchSegment(pattern string, s string) bool {
	pi := 0
	si := 0
	star := -1
	starMatch := 0

	for si < len(s) {
		if pi < len(pattern) {
			switch pattern[pi] {
			case '*':
				star = pi
				pi++
				starMatch = si
				continue
			case '?':
				pi++
				si++
				continue
			case '[':
				ok, n := matchCharClass(pattern[pi:], s[si])
				if ok {
					pi += n
					si++
					continue
				}
			}

			if pattern[pi] == s[si] {
				pi++
				si++
				continue
			}
		}

		if star != -1 {
			pi = star + 1
			starMatch++
			si = starMatch
			continue
		}
		return false
	}

	for pi < len(pattern) && pattern[pi] == '*' {
		pi++
	}
	return pi == len(pattern)
}

// matchCharClass matches a simple character class at the start of a pattern.
func matchCharClass(pattern string, b byte) (bool, int) {
	if len(pattern) == 0 || pattern[0] != '[' {
		return false, 0
	}
	end := strings.IndexByte(pattern, ']')
	if end == -1 {
		return false, 0
	}

	inner := pattern[1:end]
	neg := false
	if strings.HasPrefix(inner, "!") {
		neg = true
		inner = inner[1:]
	}

	match := false
	for i := 0; i < len(inner); i++ {
		if inner[i] == b {
			match = true
			break
		}
	}
	if neg {
		match = !match
	}
	return match, end + 1
}

// globMatchAny matches a path against any brace-expanded segment list.
func globMatchAny(expandedSegs [][]string, pathSegs []string) bool {
	for _, segs := range expandedSegs {
		if globMatchSegments(segs, pathSegs) {
			return true
		}
	}
	return false
}

// globMatchSegments matches a full path against a glob segment list.
func globMatchSegments(patternSegs []string, pathSegs []string) bool {
	states := map[int]struct{}{0: {}}
	states = globEpsilonClose(patternSegs, states)

	for _, s := range pathSegs {
		next := map[int]struct{}{}
		for i := range states {
			if i >= len(patternSegs) {
				continue
			}
			ps := patternSegs[i]
			if ps == "**" {
				next[i] = struct{}{}
				continue
			}
			if matchSegment(ps, s) {
				next[i+1] = struct{}{}
			}
		}
		states = globEpsilonClose(patternSegs, next)
		if len(states) == 0 {
			return false
		}
	}
	states = globEpsilonClose(patternSegs, states)
	_, ok := states[len(patternSegs)]
	return ok
}
