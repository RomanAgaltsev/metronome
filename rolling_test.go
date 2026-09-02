package metronome

import (
	"maps"
	"math"
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

// impossibleRollings are the configurations no defaulting can rescue. One table
// drives both NewRollingStats and Bytes, because the two must agree: a config
// that cannot be built must not be priceable either, and a table copied into one
// test is a table that drifts out of the other.
func impossibleRollings() []struct {
	name string
	cfg  Rolling
} {
	return []struct {
		name string
		cfg  Rolling
	}{
		{"negative window", Rolling{Window: -time.Second}},
		{"negative buckets", Rolling{Buckets: -1}},
		{"window narrower than one nanosecond per bucket", Rolling{Window: 5, Buckets: 10}},
		{"negative sigfigs", Rolling{Sigfigs: -1}},
		{"sigfigs above the histogram maximum", Rolling{Sigfigs: 6}},
		{"negative lo", Rolling{Lo: -time.Second}},
		{"negative hi", Rolling{Hi: -time.Second}},
		{"hi below lo", Rolling{Lo: time.Minute, Hi: time.Microsecond}},
		{"hi equal to lo", Rolling{Lo: time.Second, Hi: time.Second}},
	}
}

func TestNewRollingStatsPanicsOnImpossibleConfig(t *testing.T) {
	for _, tc := range impossibleRollings() {
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
		// 266 MiB from a one-field struct literal. The README's memory table
		// quotes this row and the three around it, so they are all pinned here.
		{"a thousand buckets at the default range", Rolling{Buckets: 1000}, 279085056},
		// countsLen 128 -> 1 KiB per histogram. Narrowing the range is what makes
		// a large ring affordable.
		{"a thousand narrow buckets", Rolling{
			Window: 10 * time.Second, Buckets: 1000,
			Lo: time.Millisecond, Hi: time.Second, Sigfigs: 1,
		}, 2052096},
		// Sub-microsecond Lo is legal and is the same size as the default: the
		// histogram counts in whole microseconds, so anything under 1µs is one
		// microsecond's resolution. Pinned because histogramCounts has to floor
		// it the way hdr.New does, and nothing else exercises that path.
		{"lo below the histogram's unit", Rolling{Lo: 500 * time.Nanosecond}, 3342336},
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
	// number for it. Bytes never calls NewStatsRange, so the histogram-range
	// rules have to be enforced on the way in: without them Rolling{Sigfigs: 6}
	// reports a 1.3 GiB budget for a config whose constructor panics.
	for _, tc := range impossibleRollings() {
		t.Run(tc.name, func(t *testing.T) {
			defer func() {
				if recover() == nil {
					t.Fatalf("Rolling%+v.Bytes() returned; want panic", tc.cfg)
				}
			}()
			_ = tc.cfg.Bytes()
		})
	}
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

func TestRollingWindowDuringWarmupDividesByWhatItCovers(t *testing.T) {
	clk := NewManualClock(time.Unix(0, 0))
	rs := NewRollingStats(Rolling{Window: 10 * time.Second, Buckets: 10, Clock: clk})

	// Three seconds of a ten-second window, at a steady 100 rps.
	for range 3 {
		for range 100 {
			rs.Record(Result{Start: clk.Now(), Latency: time.Millisecond})
		}
		clk.Advance(time.Second)
	}

	snap := rs.Window()
	if snap.Window != 3*time.Second {
		t.Fatalf("Window=%v want 3s", snap.Window)
	}
	if snap.Count != 300 {
		t.Fatalf("Count=%d want 300", snap.Count)
	}
	if snap.RPS != 100 {
		t.Fatalf("RPS=%v want exactly 100; dividing by the nominal 10s would give 30", snap.RPS)
	}
}

func TestRollingWindowRetiresOldBuckets(t *testing.T) {
	clk := NewManualClock(time.Unix(0, 0))
	rs := NewRollingStats(Rolling{Window: 5 * time.Second, Buckets: 5, Clock: clk})

	// Twenty seconds at 10 rps.
	for range 20 {
		for range 10 {
			rs.Record(Result{Start: clk.Now(), Latency: time.Millisecond})
		}
		clk.Advance(time.Second)
	}

	// The clock sits exactly on a bucket boundary, so the newest bucket is empty
	// and the window covers the four completed ones behind it. That is the
	// [window-interval, window] sawtooth every bucketed window has, reported
	// rather than rounded up.
	snap := rs.Window()
	if snap.Window != 4*time.Second {
		t.Fatalf("Window=%v want 4s on a bucket boundary", snap.Window)
	}
	if snap.Count != 40 {
		t.Fatalf("Count=%d want 40", snap.Count)
	}
	if snap.RPS != 10 {
		t.Fatalf("RPS=%v want 10", snap.RPS)
	}
	if got := rs.Snapshot().Count; got != 200 {
		t.Fatalf("lifetime Count=%d want 200", got)
	}

	// Half a bucket later the current bucket is partial and counted.
	clk.Advance(500 * time.Millisecond)
	rs.Record(Result{Start: clk.Now(), Latency: time.Millisecond})

	snap = rs.Window()
	if snap.Window != 4500*time.Millisecond {
		t.Fatalf("Window=%v want 4.5s mid-bucket", snap.Window)
	}
	if snap.Count != 41 {
		t.Fatalf("Count=%d want 41; the partial current bucket must be included", snap.Count)
	}
}

func TestRollingWindowWithOneBucketIsATumblingWindow(t *testing.T) {
	clk := NewManualClock(time.Unix(0, 0))
	rs := NewRollingStats(Rolling{Window: time.Second, Buckets: 1, Clock: clk})

	for range 10 {
		rs.Record(Result{Start: clk.Now(), Latency: time.Millisecond})
	}
	if got := rs.Window().Count; got != 10 {
		t.Fatalf("Count=%d want 10", got)
	}

	clk.Advance(time.Second)
	snap := rs.Window()
	if snap.Count != 0 {
		t.Fatalf("Count=%d want 0 once the single bucket rotated", snap.Count)
	}
	if snap.Window <= 0 {
		t.Fatalf("Window=%v want a positive duration; RPS divides by it", snap.Window)
	}
	if snap.RPS != 0 {
		t.Fatalf("RPS=%v want 0", snap.RPS)
	}
}

func TestRollingWindowImmediatelyAfterConstructionIsFinite(t *testing.T) {
	// No time has elapsed, so coverage would be zero and RPS a division by it.
	clk := NewManualClock(time.Unix(0, 0))
	rs := NewRollingStats(Rolling{Clock: clk})
	rs.Record(Result{Start: clk.Now(), Latency: time.Millisecond})

	snap := rs.Window()
	if math.IsInf(snap.RPS, 0) || math.IsNaN(snap.RPS) {
		t.Fatalf("RPS=%v want a finite number", snap.RPS)
	}
	if snap.Window <= 0 {
		t.Fatalf("Window=%v want positive", snap.Window)
	}
}

func TestRollingWindowMatchesAStatsFedTheSameLiveResults(t *testing.T) {
	// The merge must not lose or double-count anything: a window wide enough to
	// hold every Result must equal one Stats fed all of them.
	//
	// The geometry matters. 300 Results one second apart span 300s, so the ring
	// has to retire nothing and rotate several times: 10 buckets of 60s over a
	// 10-minute window puts the run in six of them. A window whose buckets are
	// wider than the whole run would leave live == 1, and this test would then
	// pass against a Window() that merged only the current bucket.
	clk := NewManualClock(time.Unix(0, 0))
	rs := NewRollingStats(Rolling{Window: 10 * time.Minute, Buckets: 10, Clock: clk})
	whole := NewStats()

	for _, r := range buildMergeResults() {
		rs.Record(r)
		whole.Record(r)
		clk.Advance(time.Second)
	}

	win, want := rs.Window(), whole.Snapshot()
	if rs.live < 2 {
		t.Fatalf("live=%d; the merge path under test needs more than one bucket", rs.live)
	}
	if win.Count != want.Count || win.Errors != want.Errors || win.Bytes != want.Bytes {
		t.Fatalf("counts differ: window %+v want %+v", win, want)
	}
	if win.P50 != want.P50 || win.P95 != want.P95 || win.P99 != want.P99 {
		t.Fatalf("percentiles differ: window %v/%v/%v want %v/%v/%v",
			win.P50, win.P95, win.P99, want.P50, want.P95, want.P99)
	}
	if win.MaxScheduleLag != want.MaxScheduleLag {
		t.Fatalf("MaxScheduleLag=%v want %v", win.MaxScheduleLag, want.MaxScheduleLag)
	}
	if !maps.Equal(win.Codes, want.Codes) {
		t.Fatalf("Codes=%v want %v", win.Codes, want.Codes)
	}
}
