package metronome

import (
	"errors"
	"maps"
	"math"
	"testing"
	"time"
)

func TestStatsSnapshot(t *testing.T) {
	s := NewStats()
	base := time.Unix(0, 0)
	for i := range 100 {
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

func TestSnapshotRPSIsUnbiased(t *testing.T) {
	s := NewStats()
	base := time.Unix(0, 0)
	// 11 results spaced exactly 100ms bound 10 intervals over exactly 1s = 10 rps.
	for i := range 11 {
		s.Record(Result{Start: base.Add(time.Duration(i) * 100 * time.Millisecond), Latency: time.Millisecond})
	}
	if got := s.Snapshot().RPS; math.Abs(got-10) > 1e-9 {
		t.Fatalf("RPS=%v want exactly 10 (count/span would report 11)", got)
	}
}

func TestSnapshotRPSZeroForSingleResult(t *testing.T) {
	s := NewStats()
	s.Record(Result{Start: time.Unix(0, 0), Latency: time.Millisecond})
	if got := s.Snapshot().RPS; got != 0 {
		t.Fatalf("RPS=%v want 0 — one timestamp bounds no interval", got)
	}
}

func TestSnapshotReportsClampedCount(t *testing.T) {
	s := NewStats()
	s.Record(Result{Start: time.Unix(0, 0), Latency: 10 * time.Millisecond})
	s.Record(Result{Start: time.Unix(0, 1), Latency: 5 * time.Minute})       // above 60s
	s.Record(Result{Start: time.Unix(0, 2), Latency: 100 * time.Nanosecond}) // below 1µs

	snap := s.Snapshot()
	if snap.Clamped != 2 {
		t.Fatalf("Clamped=%d want 2 — a clamped percentile must be visible, not just documented", snap.Clamped)
	}
	if snap.Max != 5*time.Minute {
		t.Fatalf("Max=%v want the true 5m maximum", snap.Max)
	}
}

func TestNewStatsRangeWidensTheHistogram(t *testing.T) {
	s := NewStatsRange(time.Millisecond, 10*time.Minute, 3)
	s.Record(Result{Start: time.Unix(0, 0), Latency: 5 * time.Minute})

	snap := s.Snapshot()
	if snap.Clamped != 0 {
		t.Fatalf("Clamped=%d want 0 — 5m is inside [1ms, 10m]", snap.Clamped)
	}
	if snap.P99 < 4*time.Minute || snap.P99 > 6*time.Minute {
		t.Fatalf("P99=%v want ~5m", snap.P99)
	}
}

func TestNewStatsRangeRejectsBadBounds(t *testing.T) {
	cases := []struct {
		name string
		fn   func()
	}{
		{"lo <= 0", func() { NewStatsRange(0, time.Minute, 3) }},
		{"hi <= lo", func() { NewStatsRange(time.Minute, time.Second, 3) }},
		{"sigfigs out of range", func() { NewStatsRange(time.Microsecond, time.Minute, 9) }},
	}
	for _, tc := range cases {
		t.Run(tc.name, func(t *testing.T) {
			defer func() {
				if recover() == nil {
					t.Fatal("expected panic")
				}
			}()
			tc.fn()
		})
	}
}

func TestSnapshotAggregatesBytesAndCodes(t *testing.T) {
	s := NewStats()
	base := time.Unix(0, 0)
	// 5 results spaced 250ms → 4 intervals over 1s.
	for i, code := range []string{"200", "200", "500", "200", "429"} {
		s.Record(Result{
			Start:   base.Add(time.Duration(i) * 250 * time.Millisecond),
			Latency: 10 * time.Millisecond,
			Code:    code,
			Bytes:   1000,
		})
	}
	s.Record(Result{Start: base, Latency: time.Millisecond}) // no Code: must not create an "" key

	snap := s.Snapshot()
	if snap.Bytes != 5000 {
		t.Fatalf("Bytes=%d want 5000", snap.Bytes)
	}
	if math.Abs(snap.Throughput-5000/1.0) > 1e-6 {
		t.Fatalf("Throughput=%v want 5000 bytes/sec", snap.Throughput)
	}
	want := map[string]int64{"200": 3, "500": 1, "429": 1}
	if !maps.Equal(snap.Codes, want) {
		t.Fatalf("Codes=%v want %v", snap.Codes, want)
	}
}

func TestSnapshotCodesIsACopy(t *testing.T) {
	s := NewStats()
	s.Record(Result{Start: time.Unix(0, 0), Latency: time.Millisecond, Code: "200"})
	snap := s.Snapshot()
	snap.Codes["200"] = 99 // caller mutates their copy

	if got := s.Snapshot().Codes["200"]; got != 1 {
		t.Fatalf("Codes[200]=%d after a caller mutated a Snapshot; Snapshot must return a copy", got)
	}
}
