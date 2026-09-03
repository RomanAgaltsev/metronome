package metronome

import (
	"sync"
	"time"
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

// resultStamp is the point on the run's timeline r belongs to: the schedule it
// was due on, or when it actually started if it carries no schedule. A zero
// time means the Result cannot be placed at all.
func resultStamp(r Result) time.Time {
	if !r.Scheduled.IsZero() {
		return r.Scheduled
	}
	return r.Start
}

// timeRule admits Results stamped at or after from. When anchored is false,
// from is computed from the first placeable Result's stamp plus d.
type timeRule struct {
	d        time.Duration
	from     time.Time
	anchored bool
}

func (t *timeRule) admit(r Result) bool {
	stamp := resultStamp(r)
	if stamp.IsZero() {
		// Unplaceable: it cannot be said to be inside or outside the excluded
		// region, and admitting it would be a guess.
		return false
	}
	if !t.anchored {
		t.from = stamp.Add(t.d)
		t.anchored = true
	}
	return !stamp.Before(t.from)
}

// After returns a Skipper that excludes Results scheduled within d of the
// run's start, then admits the rest. It panics if d is negative or rec is nil.
//
// Use it to keep a warmup out of a measurement: cold connection pools, TLS
// handshakes that will be reused, an empty page cache and a target that has not
// warmed up are not the system under test, and a percentile threshold asserted
// over them is asserted over the handshake. Phased can drive a gentler opening
// phase, but it changes the load rather than the measurement — those Results
// still land in the same Stats.
//
// The boundary is anchored on the first Result's Scheduled stamp, falling back
// to Start, so it is fixed by the stream rather than by when this Skipper was
// constructed, and it is measured against the schedule rather than against when
// the generator got there. A Result carrying neither stamp is skipped and
// counted, and does not set the anchor; under a Driver that never happens,
// because the Driver stamps Start even when a Runner forgets.
//
// Admission is decided per Result, not as a one-way switch, so a unit that was
// due inside the warmup is excluded even if it arrives after one that was due
// outside it — which happens routinely, because Results arrive off a channel
// fed by many workers.
//
// The anchor can be up to one reordering window late, since the first Result
// seen need not be the earliest scheduled. That is negligible against a warmup
// measured in seconds; use AfterTime when the boundary must be exact.
//
// After(0) admits every placeable Result.
func After(d time.Duration, rec Recorder) *Skipper {
	if d < 0 {
		panic("metronome: After d must be >= 0")
	}
	return newSkipper(rec, &timeRule{d: d})
}

// AfterTime returns a Skipper that excludes Results scheduled before t, then
// admits the rest. It panics if rec is nil.
//
// It is the exact form of After, for a caller who knows the run's origin:
// record clock.Now() immediately before Driver.Run and pass
// origin.Add(warmup). Unlike After it does not infer the boundary from the
// stream, so it is unaffected by which Result arrives first.
//
// Results carrying neither a Scheduled nor a Start stamp are skipped and
// counted, as with After.
func AfterTime(t time.Time, rec Recorder) *Skipper {
	return newSkipper(rec, &timeRule{from: t, anchored: true})
}
