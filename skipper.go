package metronome

import (
	"sync"
)

// Skipper is a Recorder that excludes some Results from the recorder it wraps
// and counts what it excluded.
//
// It exists so that an exclusion is auditable: Snapshot().Count plus Skipped()
// is the whole population the Skipper was offered. A report that says
// "measured 4,500 of 5,000 — 500 excluded as warmup" is honest; one that says
// "4,500 requests" invites the reader to wonder where the rest went.
//
// Skipped is not a Snapshot field: Stats, RollingStats and LabeledStats never
// skip anything, so it would be zero in every existing use and non-zero only
// when something upstream — which a Snapshot cannot see — happened to be a
// Skipper. Snapshot describes what was recorded; this describes what was not.
//
// Safe for concurrent Record.
type Skipper struct {
	rec Recorder

	mu      sync.Mutex
	rule    skipRule
	skipped int64
}

// skipRule decides whether a Result is admitted. It is called under the
// Skipper's mutex, so a rule may hold state.
type skipRule interface {
	admit(r Result) bool
}

func newSkipper(rec Recorder, rule skipRule) *Skipper {
	if rec == nil {
		panic("metronome: a Skipper requires a Recorder")
	}
	return &Skipper{rec: rec, rule: rule}
}

// Record admits r to the wrapped recorder, or skips and counts it.
func (s *Skipper) Record(r Result) {
	s.mu.Lock()
	admit := s.rule.admit(r)
	if !admit {
		s.skipped++
	}
	s.mu.Unlock()

	// Outside the lock, for the same reason LabeledStats releases before
	// calling a child: the wrapped recorder has its own synchronisation and
	// serialising it here would make this wrapper the bottleneck.
	if admit {
		s.rec.Record(r)
	}
}

// Snapshot returns the wrapped recorder's aggregate: the admitted Results only.
func (s *Skipper) Snapshot() Snapshot { return s.rec.Snapshot() }

// Skipped reports how many Results were excluded so far.
func (s *Skipper) Skipped() int64 {
	s.mu.Lock()
	defer s.mu.Unlock()
	return s.skipped
}

// countRule admits everything after the first n Results it sees.
type countRule struct {
	n    int64
	seen int64
}

func (c *countRule) admit(Result) bool {
	c.seen++
	return c.seen > c.n
}

// AfterN returns a Skipper that excludes the first n Results it sees, then
// admits the rest. It panics if n is negative or rec is nil.
//
// The count is in arrival order, not schedule order — use After when the
// boundary is a point on the run's timeline rather than a number of units.
// AfterN(0) admits everything.
func AfterN(n int64, rec Recorder) *Skipper {
	if n < 0 {
		panic("metronome: AfterN n must be >= 0")
	}
	return newSkipper(rec, &countRule{n: n})
}
