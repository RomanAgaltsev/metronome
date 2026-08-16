package metronome

import (
	"errors"
	"maps"
	"sync"
	"time"

	hdr "github.com/HdrHistogram/hdrhistogram-go"
)

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
		queued := r.Start.Sub(r.Scheduled)
		if queued < 0 {
			queued = 0 // ran early: never correct downward
		}
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

// Snapshot returns the aggregate so far. Safe to call concurrently with Record.
func (s *Stats) Snapshot() Snapshot {
	s.mu.Lock()
	defer s.mu.Unlock()

	// N Result.Start timestamps bound N-1 intervals, so the unbiased rate
	// estimate is (N-1)/span. Using N/span reports r*N/(N-1) — 10% high at N=11
	// and 100% high at N=2. A single Result bounds no interval and reports 0.
	// Throughput is RPS x the mean bytes per Result, so it is the same estimator
	// as RPS. Bytes/span would spend all N samples' bytes over N-1 intervals and
	// report the N/(N-1) bias RPS exists to avoid — 2x at N=2.
	rps, throughput := 0.0, 0.0
	if span := s.last.Sub(s.first).Seconds(); s.count > 1 && span > 0 {
		rps = float64(s.count-1) / span
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
		Count:     s.count,
		Errors:    s.errors,
		RPS:       rps,
		ErrorRate: errRate,
		P50:       us(50), P95: us(95), P99: us(99),
		Max:              s.maxLat,
		MaxScheduleLag:   s.maxLag,
		Clamped:          s.clamped,
		CorrectedClamped: s.correctedClamped,
		Saturated:        s.saturated,
		Bytes:            s.bytes,
		Throughput:       throughput,
		Codes:            maps.Clone(s.codes),
		CorrectedP50:     corrected(50), CorrectedP95: corrected(95), CorrectedP99: corrected(99),
		CorrectedCount: s.correctedCount,
	}
}
