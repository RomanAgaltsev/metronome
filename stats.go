package metronome

import (
	"sync"
	"time"

	hdr "github.com/HdrHistogram/hdrhistogram-go"
)

// Stats aggregates Results into percentile Snapshots. Safe for concurrent Record.
type Stats struct {
	mu      sync.Mutex
	hist    *hdr.Histogram
	count   int64
	errors  int64
	clamped int64
	first   time.Time
	last    time.Time
	maxLat  time.Duration
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
	return &Stats{hist: hdr.New(int64(lo/time.Microsecond), int64(hi/time.Microsecond), sigfigs)}
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
	}

	// Failed Results' latencies are recorded too (the k6/vegeta convention):
	// timeouts and slow errors are part of the latency story.
	v, clamped := s.clampMicros(r.Latency)
	if clamped {
		s.clamped++
	}
	//nolint:gosec // clampMicros guarantees the value is within the histogram's bounds
	_ = s.hist.RecordValue(v)

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
	rps := 0.0
	if span := s.last.Sub(s.first).Seconds(); s.count > 1 && span > 0 {
		rps = float64(s.count-1) / span
	}

	errRate := 0.0
	if s.count > 0 {
		errRate = float64(s.errors) / float64(s.count)
	}

	us := func(p float64) time.Duration {
		return time.Duration(s.hist.ValueAtQuantile(p)) * time.Microsecond
	}
	return Snapshot{
		Count: s.count, Errors: s.errors, RPS: rps, ErrorRate: errRate,
		P50: us(50), P95: us(95), P99: us(99), Max: s.maxLat,
		Clamped: s.clamped,
	}
}
