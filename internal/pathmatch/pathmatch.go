package pathmatch

import (
	"strings"

	"github.com/hieutdo/policyfs/internal/errkind"
)

// Pattern is a compiled glob pattern that can match full paths.
type Pattern struct {
	segs [][]string
}

// Matcher matches paths against a list of compiled glob patterns.
type Matcher struct {
	patterns []*Pattern
}

// Compile parses a glob pattern into a reusable compiled Pattern.
func Compile(pattern string) (*Pattern, error) {
	expanded, err := expandBraces(pattern)
	if err != nil {
		return nil, err
	}
	segs, err := parseGlobExpanded(expanded)
	if err != nil {
		return nil, err
	}
	return &Pattern{segs: segs}, nil
}

// NewMatcher compiles a list of glob patterns into a Matcher.
func NewMatcher(patterns []string) (*Matcher, error) {
	compiled := []*Pattern{}
	for _, p := range patterns {
		p = strings.TrimSpace(p)
		if p == "" {
			continue
		}
		pat, err := Compile(p)
		if err != nil {
			return nil, err
		}
		compiled = append(compiled, pat)
	}
	return &Matcher{patterns: compiled}, nil
}

// Match reports whether the pattern matches the full path.
func (p *Pattern) Match(path string) bool {
	if p == nil {
		return false
	}
	path = NormalizePath(path)
	pathSegs := splitSegments(path)
	return globMatchAny(p.segs, pathSegs)
}

// CanMatchDescendant reports whether the pattern can match any descendant of dir.
func (p *Pattern) CanMatchDescendant(dir string) bool {
	if p == nil {
		return false
	}
	dir = NormalizePath(dir)
	dirSegs := splitSegments(dir)
	return ruleCanMatchDescendant(p.segs, dirSegs)
}

// Match reports whether any pattern in the matcher matches the full path.
func (m *Matcher) Match(path string) bool {
	if m == nil {
		return false
	}
	for _, p := range m.patterns {
		if p.Match(path) {
			return true
		}
	}
	return false
}

// CanMatchDescendant reports whether any pattern can match under dir.
func (m *Matcher) CanMatchDescendant(dir string) bool {
	if m == nil {
		return false
	}
	for _, p := range m.patterns {
		if p.CanMatchDescendant(dir) {
			return true
		}
	}
	return false
}

// NormalizePath normalizes a slash-separated path for glob matching.
func NormalizePath(p string) string {
	p = strings.TrimSpace(p)
	p = strings.TrimPrefix(p, "/")
	p = strings.TrimSuffix(p, "/")
	return collapseSlashes(p)
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

// normalizeGlobPattern normalizes patterns for path matching.
func normalizeGlobPattern(p string) string {
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

// splitSegments splits a normalized path into slash-delimited segments.
func splitSegments(p string) []string {
	p = strings.TrimPrefix(p, "/")
	p = strings.TrimSuffix(p, "/")
	if strings.TrimSpace(p) == "" {
		return nil
	}
	return strings.Split(p, "/")
}

// ruleCanMatchDescendant reports whether any expanded pattern can match under dirSegs.
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

// globEpsilonClose expands states that can advance without consuming a segment.
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

// matchSegment matches a single path segment against a glob segment.
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
