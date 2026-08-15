package metronome

import (
	"sync"
	"time"

	hdr "github.com/HdrHistogram/hdrhistogram-go"
)

// Stats aggregates Result into percentile Snapshots. Safe for concurrent Record.
type Stats struct {
	mu     sync.Mutex
	hist   *hdr.Histogram
	count  int64
	errors int64
	first  time.Time
	last   time.Time
	maxLat time.Duration
}

func NewStats() *Stats {
	return &Stats{hist: hdr.New(1, int64(60*time.Second/time.Microsecond), 3)}
}

func (s *Stats) Record(r Result) {
	s.mu.Lock()
	defer s.mu.Unlock()
	s.count++
	if !r.Success() {
		s.errors++
	}
	// Failed Results' latencies are recorded too (k6/vegeta convention):
	// timeouts and slow errors are part of the latency story. Values outside
	// the histogram range are clamped, not dropped (Max keeps the true value).
	v := int64(r.Latency / time.Microsecond)
	if v < s.hist.LowestTrackableValue() {
		v = s.hist.LowestTrackableValue()
	}
	if v > s.hist.HighestTrackableValue() {
		v = s.hist.HighestTrackableValue()
	}
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

func (s *Stats) Snapshot() Snapshot {
	s.mu.Lock()
	defer s.mu.Unlock()
	span := s.last.Sub(s.first).Seconds()
	rps := 0.0
	if span > 0 {
		rps = float64(s.count) / span
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
	}
}
