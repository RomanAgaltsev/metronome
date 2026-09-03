package metronome

import "slices"

// Multi returns a Recorder that records each Result into every rec, in
// argument order. It panics if given no recorders.
//
// Use it to keep several views of one run without plumbing a second channel:
// a flat Stats beside a LabeledStats beside a RollingStats, all fed from one
// drain loop.
//
// Snapshot delegates to the first recorder. There is no non-arbitrary
// aggregate answer — the recorders may be different types measuring different
// populations — so Snapshot exists here to satisfy Recorder, which is what
// lets a Multi nest inside After. Hold each recorder by name and read it
// directly.
//
// It is synchronous, on the calling goroutine, and that has a sharp edge:
// time spent in Record is time not spent receiving from the result channel, so
// a slow recorder fills the channel, then the Driver's delivery backlog, and
// in open loop surfaces as ErrSaturated — which reads as "the generator ran
// out of workers" and is counted inside Errors and ErrorRate. A slow writer in
// a Multi can therefore present itself as a target problem. Everything in a
// Multi should be an in-memory aggregate; anything doing I/O belongs behind
// the caller's own buffered goroutine, where the caller owns the policy for
// falling behind.
//
// Every recorder receives the same Result, and Result holds Labels by
// reference, so a recorder that mutates the map corrupts what the others see.
// Treat Results as read-only.
func Multi(recs ...Recorder) Recorder {
	if len(recs) == 0 {
		panic("metronome: Multi requires at least one Recorder")
	}
	// The caller may retain and mutate the slice they passed — a variadic call
	// site like Multi(recs...) hands us their slice, not a copy.
	return &multi{recs: slices.Clone(recs)}
}

type multi struct{ recs []Recorder }

// Record -
func (m *multi) Record(r Result) {
	for _, rec := range m.recs {
		rec.Record(r)
	}
}

// Snapshot -
func (m *multi) Snapshot() Snapshot { return m.recs[0].Snapshot() }

// Filter returns a Recorder that records only the Results for which keep
// reports true. It panics if keep or rec is nil.
//
// keep is called once per Result, on the calling goroutine, and may keep state
// — the warmup skippers are exactly this with a stateful predicate. It must not
// mutate the Result.
//
// Snapshot delegates to rec, so the filtered aggregate reads normally.
func Filter(keep func(Result) bool, rec Recorder) Recorder {
	if keep == nil {
		panic("metronome: Filter keep must not be nil")
	}
	if rec == nil {
		panic("metronome: Filter rec must not be nil")
	}
	return &filter{keep: keep, rec: rec}
}

type filter struct {
	keep func(Result) bool
	rec  Recorder
}

// Record -
func (f *filter) Record(r Result) {
	if f.keep(r) {
		f.rec.Record(r)
	}
}

// Snapshot -
func (f *filter) Snapshot() Snapshot { return f.rec.Snapshot() }

// Drain records every Result from ch into rec until ch is closed. It is the
// canonical consumption loop, named so that consumers stop rewriting the one
// place where getting it wrong leaks goroutines.
//
// It deliberately takes no context. The Driver's contract on its result channel
// is blunt — abandoning a live one leaks the units still in flight for the
// lifetime of the process — so a Drain that returned early on cancellation
// would be a leak generator in the shape of good practice. Cancel the Driver's
// context instead: it stops, closes the channel, and this returns because the
// range ended.
func Drain(ch <-chan Result, rec Recorder) {
	for r := range ch {
		rec.Record(r)
	}
}
