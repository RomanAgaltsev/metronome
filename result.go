package metronome

import (
	"errors"
	"fmt"
	"time"
)

// ErrSaturated marks an open-loop unit of work that found no free worker at its
// scheduled time. It is recorded rather than silently dropped, so saturation
// appears in Snapshot.ErrorRate instead of as invisible rate sag. Test for it
// with errors.Is.
var ErrSaturated = errors.New("metronome: no worker free at scheduled time")

// Result is the outcome of one unit of work.
type Result struct {
	// Scheduled is the time this unit of work was due per the pacing schedule —
	// the run's origin plus one interval for each unit dispatched before it,
	// advanced independently of when the generator actually got to it.
	//
	// Start minus Scheduled is therefore the generator's own queueing delay: the
	// wait a client that never fell behind would have suffered before its
	// request even started. It is what Stats uses to correct for coordinated
	// omission, and it is aggregated as Snapshot.MaxScheduleLag.
	//
	// It may be in the past relative to Start (the generator was late — the
	// whole point) or in the future when Burst > 1 lets several units go at once
	// and the schedule says the later ones were not due yet; Stats floors the
	// correction at zero for that case. Zero if the Result did not come from a
	// Driver.
	Scheduled time.Time

	Start   time.Time
	Latency time.Duration
	Err     error
	Code    string // status/gRPC code, caller-supplied
	Bytes   int64

	// Labels is caller-side metadata carried alongside the Result, e.g.
	// {"endpoint": "get_user"}. Stats does not aggregate it: a per-label
	// breakdown helper is roadmapped, and until it exists a caller who needs
	// one keys their own map off these Results. Leave it nil if unused — it is
	// an allocation per request.
	Labels map[string]string
}

// Success reports whether the unit of work completed without error.
func (r Result) Success() bool { return r.Err == nil }

// Snapshot is an aggregated view of many Results over a window.
type Snapshot struct {
	Count         int64
	Errors        int64
	RPS           float64
	ErrorRate     float64
	P50, P95, P99 time.Duration
	Max           time.Duration

	// Clamped counts Results whose Latency fell outside the histogram's range
	// and was clamped to a bound — at most one per Result. Non-zero means P50,
	// P95 and P99 understate reality at one end; widen the range with
	// NewStatsRange. Max always reports the true maximum regardless.
	Clamped int64

	// CorrectedClamped is the same count for the corrected histogram, which
	// records Latency plus queueing delay and therefore overflows a bound the
	// raw latency never reaches. It is counted separately because it says
	// something about CorrectedP50/P95/P99 only — a run can have
	// CorrectedClamped > 0 with Clamped == 0 and percentiles that are fine.
	CorrectedClamped int64

	// MaxScheduleLag is the largest gap between when a unit of work was due and
	// when it actually started. It is the generator's own lateness: non-zero
	// means metronome did not keep the schedule it offered, whatever the target
	// did. Read it against Saturated — a large lag with Saturated == 0 says the
	// generator, not the target, is the bottleneck.
	MaxScheduleLag time.Duration

	// Saturated counts Results carrying ErrSaturated: open-loop units that found
	// no free worker at their scheduled time. They are included in Errors and
	// ErrorRate, and this field is what separates "my generator ran out of
	// workers" from "the target failed". Always zero in ClosedLoop.
	Saturated int64

	// Bytes is the sum of Result.Bytes.
	Bytes int64

	// Throughput is bytes/sec: RPS multiplied by the mean Bytes per Result, so
	// it is the same kind of estimate as RPS rather than a raw total over the
	// observed span. (Bytes/span would spend all N samples' bytes over the N-1
	// intervals N timestamps actually bound — the bias RPS avoids.)
	Throughput float64

	// Codes counts Results by Result.Code. Results with an empty Code are
	// counted in Count but not here. The map is a copy owned by the caller.
	Codes map[string]int64

	// CorrectedP* are the coordinated-omission-corrected percentiles: each
	// Result's latency plus the time it spent waiting past its Scheduled send
	// time. They answer "what would a client that kept to the schedule have
	// seen?", which is the honest number when the target stalls. They are zero
	// when no Result carried a Scheduled stamp.
	//
	// Read them against the raw P* above: a large gap means the generator
	// queued, so the raw numbers understate what a real client would suffer.
	CorrectedP50, CorrectedP95, CorrectedP99 time.Duration

	// CorrectedCount is how many Results carried a Scheduled stamp and are
	// therefore represented in CorrectedP*.
	CorrectedCount int64
}

// String renders the numbers a run is usually judged on, in the order they
// should be read: what was achieved, what it cost, what a schedule-faithful
// client would have seen, and whether the generator itself kept up.
//
// It is a summary, not a serialisation — Codes, Bytes, Throughput and the
// clamping counters are omitted. Reach for the fields directly when you need
// them.
func (s Snapshot) String() string {
	return fmt.Sprintf(
		"%d req, %.1f rps, %.2f%% err (%d saturated), p50/p95/p99 %v/%v/%v, "+
			"corrected p95/p99 %v/%v, behind schedule %v",
		s.Count, s.RPS, s.ErrorRate*100, s.Saturated,
		s.P50, s.P95, s.P99,
		s.CorrectedP95, s.CorrectedP99,
		s.MaxScheduleLag,
	)
}

// PanicError reports a panic raised inside a Runner. The Driver recovers it so
// that one bad unit of work cannot abort a whole load run; the panic is
// delivered as a failed Result instead, and Stack holds the stack captured at
// the point of recovery.
type PanicError struct {
	Value any
	Stack []byte
}

func (e *PanicError) Error() string {
	return fmt.Sprintf("metronome: runner panicked: %v", e.Value)
}
