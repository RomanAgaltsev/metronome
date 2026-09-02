package metronome

import (
	"errors"
	"maps"
	"sync"
	"sync/atomic"
	"time"

	hdr "github.com/HdrHistogram/hdrhistogram-go"
)

const (
	bytesPerCount      = 8 // one int64 per histogram bucket count
	histogramsPerStats = 2 // raw and corrected
)

// statsID orders lock acquisition in Merge. Construction order is a total
// order over every Stats in the process, which is all Merge needs and is
// cheaper to reason about than comparing pointer addresses through unsafe.
var statsID atomic.Uint64

// Stats aggregates Results into percentile Snapshots. Safe for concurrent Record.
type Stats struct {
	mu               sync.Mutex
	hist             *hdr.Histogram
	count            int64
	errors           int64
	saturated        int64
	clamped          int64
	first            time.Time
	last             time.Time
	maxLat           time.Duration // largest raw Latency
	maxLag           time.Duration // largest Start-Scheduled, floored at zero
	bytes            int64
	codes            map[string]int64
	corrected        *hdr.Histogram
	correctedCount   int64
	correctedClamped int64

	// countsLen is the length of each histogram's counts array, derived at
	// construction. Immutable, so Bytes needs no lock.
	countsLen int64

	id uint64 // construction order; see statsID
}

// NewStats returns Stats recording latencies from 1µs to 60s with 3 significant
// digits — a sensible default for HTTP and gRPC. Use NewStatsRange when your
// work is slower or faster than that.
func NewStats() *Stats { return NewStatsRange(time.Microsecond, time.Minute, 3) }

// NewStatsRange returns Stats recording latencies in [lo, hi] with sigfigs
// significant digits. Latencies outside the range are clamped to the nearest
// bound and counted in Snapshot.Clamped rather than dropped; Snapshot.Max always
// reports the true maximum.
//
// It panics if lo <= 0, hi <= lo, or sigfigs is outside [1, 5] — programmer
// errors, and there is no error path on a constructor two consumers call at
// startup.
func NewStatsRange(lo, hi time.Duration, sigfigs int) *Stats {
	if lo <= 0 {
		panic("metronome: NewStatsRange lo must be > 0")
	}
	if hi <= lo {
		panic("metronome: NewStatsRange hi must be > lo")
	}
	if sigfigs < 1 || sigfigs > 5 {
		panic("metronome: NewStatsRange sigfigs must be in [1, 5]")
	}
	return &Stats{
		hist:      hdr.New(int64(lo/time.Microsecond), int64(hi/time.Microsecond), sigfigs),
		corrected: hdr.New(int64(lo/time.Microsecond), int64(hi/time.Microsecond), sigfigs),
		codes:     make(map[string]int64),
		countsLen: histogramCounts(lo, hi, sigfigs),
		id:        statsID.Add(1),
	}
}

// clampMicros converts d to whole microseconds inside the histogram's range,
// reporting whether it had to clamp. Caller holds s.mu.
func (s *Stats) clampMicros(d time.Duration) (int64, bool) {
	v := int64(d / time.Microsecond)
	switch {
	case v < s.hist.LowestTrackableValue():
		return s.hist.LowestTrackableValue(), true
	case v > s.hist.HighestTrackableValue():
		return s.hist.HighestTrackableValue(), true
	default:
		return v, false
	}
}

// Record adds one Result to the aggregate. It is safe to call concurrently.
func (s *Stats) Record(r Result) {
	s.mu.Lock()
	defer s.mu.Unlock()
	s.count++
	if !r.Success() {
		s.errors++
		if errors.Is(r.Err, ErrSaturated) {
			s.saturated++
		}
	}
	s.bytes += r.Bytes
	if r.Code != "" {
		s.codes[r.Code]++
	}

	// Failed Results' latencies are recorded too (the k6/vegeta convention):
	// timeouts and slow errors are part of the latency story.
	v, clamped := s.clampMicros(r.Latency)
	if clamped {
		s.clamped++
	}
	//nolint:gosec // clampMicros guarantees the value is within the histogram's bounds
	_ = s.hist.RecordValue(v)

	// Coordinated-omission correction: a unit that started late was, from the
	// schedule's point of view, already in flight while it waited. Results
	// without a Scheduled stamp (not produced by a Driver) are simply not
	// represented in the corrected percentiles.
	if !r.Scheduled.IsZero() {
		queued := max(r.Start.Sub(r.Scheduled),
			// ran early: never correct downward
			0)
		if queued > s.maxLag {
			s.maxLag = queued
		}
		// Counted separately from s.clamped: this says the *corrected*
		// percentiles hit a bound, which a raw latency well inside the range can
		// still cause once the queueing delay is added.
		cv, clamped := s.clampMicros(r.Latency + queued)
		if clamped {
			s.correctedClamped++
		}
		//nolint:gosec // clampMicros guarantees the value is within the histogram's bounds
		_ = s.corrected.RecordValue(cv)
		s.correctedCount++
	}

	if r.Latency > s.maxLat {
		s.maxLat = r.Latency
	}
	if s.first.IsZero() || r.Start.Before(s.first) {
		s.first = r.Start
	}
	if r.Start.After(s.last) {
		s.last = r.Start
	}
}

// Snapshot returns the cumulative aggregate so far. Safe to call concurrently
// with Record.
func (s *Stats) Snapshot() Snapshot { return s.snapshot(0) }

// snapshot builds a Snapshot over a known or an inferred duration.
//
// A positive window means the caller knows how much wall time these Results
// cover — the rolling-window case — so RPS is count/window. That estimator can
// report zero, which is what makes a stall visible.
//
// A zero window means the duration is unknown and must be inferred from the
// Result timestamps themselves. N Result.Start timestamps bound N-1 intervals,
// so the unbiased estimate is (N-1)/span; using N/span reports r*N/(N-1) — 10%
// high at N=11 and 100% high at N=2. A single Result bounds no interval and
// reports 0. This estimator cannot represent a stall: no new Results means no
// new span, and the ratio holds at the last healthy rate.
//
// Throughput is RPS x the mean bytes per Result either way, so it is always the
// same kind of estimate as RPS. Bytes/span would spend all N samples' bytes over
// N-1 intervals and report the N/(N-1) bias RPS exists to avoid — 2x at N=2.
func (s *Stats) snapshot(window time.Duration) Snapshot {
	s.mu.Lock()
	defer s.mu.Unlock()

	rps := 0.0
	if window > 0 {
		rps = float64(s.count) / window.Seconds()
	} else if span := s.last.Sub(s.first).Seconds(); s.count > 1 && span > 0 {
		rps = float64(s.count-1) / span
	}

	throughput := 0.0
	if s.count > 0 {
		throughput = rps * float64(s.bytes) / float64(s.count)
	}

	errRate := 0.0
	if s.count > 0 {
		errRate = float64(s.errors) / float64(s.count)
	}

	us := func(p float64) time.Duration {
		return time.Duration(s.hist.ValueAtQuantile(p)) * time.Microsecond
	}
	corrected := func(p float64) time.Duration {
		if s.correctedCount == 0 {
			return 0
		}
		return time.Duration(s.corrected.ValueAtQuantile(p)) * time.Microsecond
	}

	return Snapshot{
		Window:           window,
		Count:            s.count,
		Errors:           s.errors,
		RPS:              rps,
		ErrorRate:        errRate,
		P50:              us(50),
		P95:              us(95),
		P99:              us(99),
		Max:              s.maxLat,
		MaxScheduleLag:   s.maxLag,
		Clamped:          s.clamped,
		CorrectedClamped: s.correctedClamped,
		Saturated:        s.saturated,
		Bytes:            s.bytes,
		Throughput:       throughput,
		Codes:            maps.Clone(s.codes),
		CorrectedP50:     corrected(50),
		CorrectedP95:     corrected(95),
		CorrectedP99:     corrected(99),
		CorrectedCount:   s.correctedCount,
	}
}

// reset returns s to its just-constructed state so a rolling bucket can be
// recycled. The histograms keep their allocated counts arrays — only the
// recorded values go — which is the whole reason a ring is affordable.
func (s *Stats) reset() {
	s.mu.Lock()
	defer s.mu.Unlock()

	s.hist.Reset()
	s.corrected.Reset()
	s.count, s.errors, s.saturated, s.clamped = 0, 0, 0, 0
	s.correctedCount, s.correctedClamped = 0, 0
	s.bytes = 0
	s.maxLat, s.maxLag = 0, 0
	s.first, s.last = time.Time{}, time.Time{}
	clear(s.codes)
}

// merge folds src into s.
//
// The caller must hold exclusive use of s; src is locked for the read. That
// asymmetry is deliberate and sufficient for RollingStats, which only ever
// merges buckets into its own private scratch Stats. Merge is the exported
// form and takes both locks.
func (s *Stats) merge(src *Stats) {
	src.mu.Lock()
	defer src.mu.Unlock()
	s.mergeLocked(src)
}

// mergeLocked folds src into s. The caller holds exclusive use of s and holds
// src.mu.
func (s *Stats) mergeLocked(src *Stats) {
	// Merge reports values it had to drop for falling outside the destination's
	// range. Both callers guarantee identical ranges — RollingStats by
	// construction, Merge by an explicit check — so this is always zero.
	_ = s.hist.Merge(src.hist)
	_ = s.corrected.Merge(src.corrected)

	s.count += src.count
	s.errors += src.errors
	s.saturated += src.saturated
	s.clamped += src.clamped
	s.correctedCount += src.correctedCount
	s.correctedClamped += src.correctedClamped
	s.bytes += src.bytes

	s.maxLat = max(s.maxLat, src.maxLat)
	s.maxLag = max(s.maxLag, src.maxLag)

	for code, n := range src.codes {
		s.codes[code] += n
	}
	if !src.first.IsZero() && (s.first.IsZero() || src.first.Before(s.first)) {
		s.first = src.first
	}
	if src.last.After(s.last) {
		s.last = src.last
	}
}

// Bytes reports the memory this Stats holds for its histograms: two count
// arrays of int64. The surrounding struct, the codes map and the recorded
// Results are not included — the count arrays dominate and are the term that
// scales with the range and significant figures.
//
// It needs no lock: the length is fixed at construction.
func (s *Stats) Bytes() int64 {
	return s.countsLen * bytesPerCount * histogramsPerStats
}

// sameRange reports whether s and src record over the same range with the same
// significant figures. It reads only values fixed at construction, so it needs
// no lock.
func (s *Stats) sameRange(src *Stats) bool {
	return s.hist.LowestTrackableValue() == src.hist.LowestTrackableValue() &&
		s.hist.HighestTrackableValue() == src.hist.HighestTrackableValue() &&
		s.hist.SignificantFigures() == src.hist.SignificantFigures()
}

// Merge folds src into s, so that s reports what it would have reported had it
// recorded every Result src did. Percentiles cannot be combined after the fact,
// so this merges the underlying histograms rather than the Snapshots.
//
// Use it to roll a LabeledStats breakdown up into an ad-hoc subtotal, to
// combine per-worker aggregates recorded in parallel, or to combine separate
// runs.
//
// Both Stats must have been built with the same range and significant figures;
// Merge panics otherwise, and panics if src is s. Range equality is a property
// of construction, so a mismatch fires on the first call rather than later and
// only for some values — it is a programmer error, and it is reported as one.
//
// It is safe to call concurrently with Record on either Stats, and safe to call
// in both directions from different goroutines.
func (s *Stats) Merge(src *Stats) {
	if s == src {
		panic("metronome: Stats.Merge cannot merge a Stats into itself")
	}
	if !s.sameRange(src) {
		panic("metronome: Stats.Merge requires both Stats to have the same range and sigfigs")
	}

	// Lock in construction order so that a.Merge(b) racing b.Merge(a) cannot
	// deadlock on taking the two mutexes in opposite orders.
	first, second := s, src
	if src.id < s.id {
		first, second = src, s
	}
	first.mu.Lock()
	defer first.mu.Unlock()
	second.mu.Lock()
	defer second.mu.Unlock()

	s.mergeLocked(src)
}
