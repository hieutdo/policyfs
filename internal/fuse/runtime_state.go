package fuse

import (
	"sync/atomic"

	"github.com/hieutdo/policyfs/internal/errkind"
	"github.com/hieutdo/policyfs/internal/router"
	"github.com/rs/zerolog"
)

// runtimeState holds the hot-swappable runtime components used by all inodes.
//
// It is safe for concurrent use. Readers are lock-free.
type runtimeState struct {
	v atomic.Value // runtimeSnapshot
}

// runtimeSnapshot is an immutable bundle of runtime components.
type runtimeSnapshot struct {
	rt  *router.Router
	log zerolog.Logger
}

// newRuntimeState constructs a runtimeState initialized with rt and log.
func newRuntimeState(rt *router.Router, log zerolog.Logger) *runtimeState {
	s := &runtimeState{}
	s.v.Store(runtimeSnapshot{rt: rt, log: log})
	return s
}

// Snapshot returns a consistent (router, logger) pair.
func (s *runtimeState) Snapshot() (*router.Router, zerolog.Logger) {
	if s == nil {
		return nil, zerolog.Logger{}
	}
	v := s.v.Load()
	if v == nil {
		return nil, zerolog.Logger{}
	}
	snap, _ := v.(runtimeSnapshot)
	return snap.rt, snap.log
}

// Swap replaces the router and logger atomically.
func (s *runtimeState) Swap(rt *router.Router, log zerolog.Logger) error {
	if s == nil {
		return &errkind.NilError{What: "runtime state"}
	}
	if rt == nil {
		return &errkind.NilError{What: "router"}
	}
	s.v.Store(runtimeSnapshot{rt: rt, log: log})
	return nil
}
