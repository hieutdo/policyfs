package router

import (
	"errors"
	"fmt"
	"regexp"
	"strings"

	"github.com/hieutdo/policyfs/internal/config"
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
	re    *regexp.Regexp
	read  []string
	write []string
}

// New builds a Router from a mount config.
func New(m *config.MountConfig) (*Router, error) {
	if m == nil {
		return nil, errors.New("mount config is nil")
	}
	if len(m.StoragePaths) == 0 {
		return nil, errors.New("config: storage_paths must not be empty")
	}
	if len(m.RoutingRules) == 0 {
		return nil, errors.New("config: routing_rules must not be empty")
	}

	storageByID := make(map[string]config.StoragePath, len(m.StoragePaths))
	for _, sp := range m.StoragePaths {
		if strings.TrimSpace(sp.ID) == "" {
			return nil, errors.New("config: storage_paths.id is required")
		}
		if strings.TrimSpace(sp.Path) == "" {
			return nil, fmt.Errorf("config: storage_paths %q: path is required", sp.ID)
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
			return nil, fmt.Errorf("config: routing_rules[%d].match is required", i)
		}
		re, err := compileGlob(rr.Match)
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

		r.rules = append(r.rules, compiledRule{rule: rr, re: re, read: read, write: write})
	}
	return r, nil
}

// ResolveReadTargets returns storage targets for reads for the given virtual path.
func (r *Router) ResolveReadTargets(virtualPath string) ([]Target, error) {
	cr, ok := r.matchRule(virtualPath)
	if !ok {
		return nil, errors.New("no routing rule matched")
	}
	ids, err := r.expandTargets(cr.read)
	if err != nil {
		return nil, err
	}
	return r.targetsFromIDs(ids)
}

// ResolveWriteTargets returns storage targets for writes for the given virtual path.
func (r *Router) ResolveWriteTargets(virtualPath string) ([]Target, error) {
	cr, ok := r.matchRule(virtualPath)
	if !ok {
		return nil, errors.New("no routing rule matched")
	}
	ids, err := r.expandTargets(cr.write)
	if err != nil {
		return nil, err
	}
	return r.targetsFromIDs(ids)
}

// matchRule returns the first routing rule that matches a path.
func (r *Router) matchRule(virtualPath string) (compiledRule, bool) {
	p := strings.TrimPrefix(virtualPath, "/")
	for _, cr := range r.rules {
		if cr.re.MatchString(p) {
			return cr, true
		}
	}
	return compiledRule{}, false
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
					return nil, fmt.Errorf("config: storage_groups %q references unknown storage id %q", id, m)
				}
				if _, dup := seen[m]; dup {
					continue
				}
				seen[m] = struct{}{}
				out = append(out, m)
			}
			continue
		}
		return nil, fmt.Errorf("unknown target id %q", id)
	}
	if len(out) == 0 {
		return nil, errors.New("no targets resolved")
	}
	return out, nil
}

// targetsFromIDs converts storage IDs into resolved targets.
func (r *Router) targetsFromIDs(ids []string) ([]Target, error) {
	out := make([]Target, 0, len(ids))
	for _, id := range ids {
		sp, ok := r.storageByID[id]
		if !ok {
			return nil, fmt.Errorf("unknown storage id %q", id)
		}
		out = append(out, Target{ID: sp.ID, Root: sp.Path, Indexed: sp.Indexed})
	}
	return out, nil
}

// compileGlob compiles PolicyFS glob patterns.
//
// Supported tokens:
// - `**` matches any chars, including '/'
// - `*` matches any chars except '/'
// - `?` matches exactly one char except '/'
func compileGlob(pattern string) (*regexp.Regexp, error) {
	p := strings.TrimPrefix(pattern, "/")
	var b strings.Builder
	b.WriteString("^")

	for i := 0; i < len(p); i++ {
		c := p[i]
		switch c {
		case '*':
			if i+1 < len(p) && p[i+1] == '*' {
				b.WriteString(".*")
				i++
				continue
			}
			b.WriteString("[^/]*")
		case '?':
			b.WriteString("[^/]")
		case '.', '+', '(', ')', '|', '^', '$', '{', '}', '[', ']', '\\':
			b.WriteByte('\\')
			b.WriteByte(c)
		default:
			b.WriteByte(c)
		}
	}

	b.WriteString("$")
	c := b.String()
	re, err := regexp.Compile(c)
	if err != nil {
		return nil, fmt.Errorf("failed to compile glob: %w", err)
	}
	return re, nil
}
