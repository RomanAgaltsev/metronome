package metronome

import (
	"testing"
	"time"
)

func TestNewRollingStatsZeroValueIsUsable(t *testing.T) {
	rs := NewRollingStats(Rolling{})

	if got := len(rs.ring); got != DefaultBuckets {
		t.Fatalf("len(ring)=%d want %d", got, DefaultBuckets)
	}
	if got, want := rs.interval, DefaultWindow/DefaultBuckets; got != want {
		t.Fatalf("interval=%v want %v", got, want)
	}
	if rs.clock == nil {
		t.Fatal("clock is nil; want the wall clock")
	}
	if rs.live != 1 {
		t.Fatalf("live=%d want 1 at construction", rs.live)
	}
	if !rs.origin.Equal(rs.curStart) {
		t.Fatalf("origin=%v curStart=%v want them equal at construction", rs.origin, rs.curStart)
	}
	if rs.life == nil || rs.scratch == nil {
		t.Fatal("life and scratch must be allocated")
	}
	for i, b := range rs.ring {
		if b == nil {
			t.Fatalf("ring[%d] is nil", i)
		}
	}
}

func TestNewRollingStatsTruncatesWindowToWholeBuckets(t *testing.T) {
	// 10s over 3 buckets is 3.333s each, so the ring spans 9.999s. The
	// truncation is documented rather than rounded away.
	rs := NewRollingStats(Rolling{Window: 10 * time.Second, Buckets: 3})
	if got, want := rs.interval, 10*time.Second/3; got != want {
		t.Fatalf("interval=%v want %v", got, want)
	}
}

func TestNewRollingStatsPanicsOnImpossibleConfig(t *testing.T) {
	cases := []struct {
		name string
		cfg  Rolling
	}{
		{"negative window", Rolling{Window: -time.Second}},
		{"negative buckets", Rolling{Buckets: -1}},
		{"window narrower than one nanosecond per bucket", Rolling{Window: 5, Buckets: 10}},
		{"negative sigfigs", Rolling{Sigfigs: -1}},
		{"sigfigs above the histogram maximum", Rolling{Sigfigs: 6}},
		{"negative lo", Rolling{Lo: -time.Second}},
		{"hi below lo", Rolling{Lo: time.Minute, Hi: time.Microsecond}},
	}

	for _, tc := range cases {
		t.Run(tc.name, func(t *testing.T) {
			defer func() {
				if recover() == nil {
					t.Fatalf("NewRollingStats(%+v) returned; want panic", tc.cfg)
				}
			}()
			NewRollingStats(tc.cfg)
		})
	}
}

func TestNewRollingStatsAcceptsTheMinimumLegalWindow(t *testing.T) {
	// One nanosecond per bucket is legal and must not panic; anything less
	// floors interval to zero, which is the case above.
	rs := NewRollingStats(Rolling{Window: 10, Buckets: 10})
	if rs.interval != time.Nanosecond {
		t.Fatalf("interval=%v want 1ns", rs.interval)
	}
}

func TestRollingBytes(t *testing.T) {
	// Exact values, not bounds. Bytes duplicates hdr.New's sizing formula, so a
	// dependency bump that changed that formula would make it silently wrong.
	// These assertions are the tripwire: if they fail after a go.mod update,
	// re-derive histogramCounts against the new hdr.New before touching them.
	cases := []struct {
		name string
		cfg  Rolling
		want int64
	}{
		// countsLen 17,408 -> 136 KiB per histogram, x2 per Stats, x12 Stats.
		{"default", Rolling{}, 3342336},
		// The knob that reads like resolution and is really a multiplier: same
		// range, 100x the buckets, ~100x the memory.
		{"a hundred buckets at the default range", Rolling{Buckets: 100}, 28409856},
		// countsLen 128 -> 1 KiB per histogram. Narrowing the range is what makes
		// a large ring affordable.
		{"a thousand narrow buckets", Rolling{
			Window: 10 * time.Second, Buckets: 1000,
			Lo: time.Millisecond, Hi: time.Second, Sigfigs: 1,
		}, 2052096},
	}

	for _, tc := range cases {
		t.Run(tc.name, func(t *testing.T) {
			if got := tc.cfg.Bytes(); got != tc.want {
				t.Fatalf("Bytes()=%d want %d", got, tc.want)
			}
		})
	}
}

func TestRollingBytesPanicsWhereNewRollingStatsDoes(t *testing.T) {
	// Pricing a configuration that cannot be built must not quietly return a
	// number for it.
	defer func() {
		if recover() == nil {
			t.Fatal("Bytes() returned on an unbuildable config; want panic")
		}
	}()
	_ = Rolling{Window: 5, Buckets: 10}.Bytes()
}

func TestRollingRecordFeedsBothViews(t *testing.T) {
	clk := NewManualClock(time.Unix(0, 0))
	rs := NewRollingStats(Rolling{Window: 4 * time.Second, Buckets: 4, Clock: clk})

	for range 3 {
		rs.Record(Result{Start: clk.Now(), Latency: time.Millisecond})
	}
	clk.Advance(time.Second)
	for range 5 {
		rs.Record(Result{Start: clk.Now(), Latency: time.Millisecond})
	}

	life := rs.Snapshot()
	if life.Count != 8 {
		t.Fatalf("lifetime Count=%d want 8", life.Count)
	}
	if life.Window != 0 {
		t.Fatalf("lifetime Window=%v want 0", life.Window)
	}
	if got := rs.ring[0].Snapshot().Count; got != 3 {
		t.Fatalf("ring[0] Count=%d want 3", got)
	}
	if got := rs.ring[1].Snapshot().Count; got != 5 {
		t.Fatalf("ring[1] Count=%d want 5", got)
	}
}

func TestRollingRotationStaysAnchoredToOrigin(t *testing.T) {
	clk := NewManualClock(time.Unix(0, 0))
	rs := NewRollingStats(Rolling{Window: 10 * time.Second, Buckets: 10, Clock: clk})

	// Ragged steps that never land on a bucket boundary. If rotation reset
	// curStart to now instead of advancing it by whole intervals, the grid would
	// drift by the remainder every time.
	for range 7 {
		clk.Advance(1400 * time.Millisecond)
		rs.Record(Result{Start: clk.Now(), Latency: time.Millisecond})
	}

	// 9.8s elapsed, so nine whole intervals have passed.
	if got, want := rs.curStart.Sub(rs.origin), 9*time.Second; got != want {
		t.Fatalf("curStart drifted to %v after 9.8s; want exactly %v", got, want)
	}
	if rs.live != 10 {
		t.Fatalf("live=%d want 10", rs.live)
	}
}

func TestRollingGapLongerThanTheWindowClearsTheRing(t *testing.T) {
	clk := NewManualClock(time.Unix(0, 0))
	rs := NewRollingStats(Rolling{Window: 10 * time.Second, Buckets: 10, Clock: clk})

	for range 100 {
		rs.Record(Result{Start: clk.Now(), Latency: time.Millisecond})
	}
	clk.Advance(time.Hour) // 3,600 intervals; rotation must not loop 3,600 times
	rs.Record(Result{Start: clk.Now(), Latency: time.Millisecond})

	if rs.live != 1 {
		t.Fatalf("live=%d want 1 after a gap longer than the window", rs.live)
	}
	if got := rs.ring[rs.idx].Snapshot().Count; got != 1 {
		t.Fatalf("current bucket Count=%d want 1", got)
	}
	total := int64(0)
	for _, b := range rs.ring {
		total += b.Snapshot().Count
	}
	if total != 1 {
		t.Fatalf("ring holds %d Results want 1; stale buckets were not reset", total)
	}
	if got := rs.Snapshot().Count; got != 101 {
		t.Fatalf("lifetime Count=%d want 101; the gap must not touch it", got)
	}
}

func TestRollingIgnoresAClockThatGoesBackwards(t *testing.T) {
	clk := NewManualClock(time.Unix(100, 0))
	rs := NewRollingStats(Rolling{Window: 10 * time.Second, Buckets: 10, Clock: clk})

	clk.Advance(-50 * time.Second)
	rs.Record(Result{Start: clk.Now(), Latency: time.Millisecond})

	if rs.live != 1 {
		t.Fatalf("live=%d want 1; a backwards clock must not rotate", rs.live)
	}
	if got := rs.ring[rs.idx].Snapshot().Count; got != 1 {
		t.Fatalf("current bucket Count=%d want 1", got)
	}
}
