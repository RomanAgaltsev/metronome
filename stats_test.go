package metronome

import (
	"errors"
	"testing"
	"time"
)

func TestStatsSnapshot(t *testing.T) {
	s := NewStats()
	base := time.Unix(0, 0)
	for i := 0; i < 100; i++ {
		s.Record(Result{
			Start:   base.Add(time.Duration(i) * time.Millisecond),
			Latency: 10 * time.Millisecond,
		})
	}
	s.Record(Result{
		Start:   base,
		Latency: time.Second,
		Err:     errors.New("x"),
	})

	snap := s.Snapshot()
	if snap.Count != 101 {
		t.Fatalf("Count=%d want 101", snap.Count)
	}
	if snap.Errors != 1 {
		t.Fatalf("Errors=%d want 1", snap.Errors)
	}
	// p50 of the 100 successful 10ms results should be ~10ms (HDR tolerance).
	if snap.P50 < 9*time.Millisecond || snap.P50 > 11*time.Millisecond {
		t.Fatalf("P50=%v want ~10ms", snap.P50)
	}
}

func TestStatsClampsOutOfRangeLatency(t *testing.T) {
	s := NewStats()
	s.Record(Result{ // above the 60s histogram bound
		Start:   time.Unix(0, 0),
		Latency: 5 * time.Minute,
	})
	snap := s.Snapshot()
	if snap.P99 < 50*time.Second {
		t.Fatalf("P99=%v; out-of-range latency should clamp to ~60s, not be dropped", snap.P99)
	}
	if snap.Max != 5*time.Minute {
		t.Fatalf("Max=%v want the true 5m maximum", snap.Max)
	}
}
