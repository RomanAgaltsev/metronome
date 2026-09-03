package metronome

import (
	"context"
	"testing"
	"time"
)

// TestAfterExcludesAColdStartTheLifetimeAggregateCannot is the motivating
// defect, encoded: the first two seconds of a run are an order of magnitude
// slower, and a threshold asserted over the whole run is asserted over the
// handshake.
func TestAfterExcludesAColdStartTheLifetimeAggregateCannot(t *testing.T) {
	const (
		cold   = 400 * time.Millisecond
		warm   = 10 * time.Millisecond
		warmup = 2 * time.Second
	)
	base := time.Unix(0, 0)

	// 2s of cold traffic at 100rps, then 8s of warm traffic at the same rate.
	stream := make([]Result, 0, 1000)
	for i := range 1000 {
		at := base.Add(time.Duration(i) * 10 * time.Millisecond)
		lat := warm
		if at.Sub(base) < warmup {
			lat = cold
		}
		stream = append(stream, Result{Scheduled: at, Start: at, Latency: lat})
	}

	whole := NewStats()
	measured := After(warmup, NewStats())
	for _, r := range stream {
		whole.Record(r)
		measured.Record(r)
	}

	// The unwrapped aggregate carries the cold start into its percentiles.
	if got := whole.Snapshot().P99; got < cold {
		t.Fatalf("lifetime P99=%v want >= %v — the cold start should dominate it", got, cold)
	}

	// The warmed one does not.
	warmSnap := measured.Snapshot()
	if warmSnap.P99 > warm+time.Millisecond {
		t.Fatalf("warmed P99=%v want ~%v", warmSnap.P99, warm)
	}

	// And the exclusion is auditable.
	if got := measured.Skipped(); got != 200 {
		t.Fatalf("Skipped()=%d want 200", got)
	}
	if got, want := warmSnap.Count+measured.Skipped(), int64(len(stream)); got != want {
		t.Fatalf("Count+Skipped=%d want %d", got, want)
	}
}

// TestWarmupAndFanOutComposeThroughARealDriver runs the shape a consumer
// actually writes: drain once, exclude a warmup, keep a flat report beside a
// per-endpoint breakdown.
func TestWarmupAndFanOutComposeThroughARealDriver(t *testing.T) {
	endpoint := func(name string, lat time.Duration) Runner {
		return RunnerFunc(func(context.Context) Result {
			return Result{Latency: lat, Labels: map[string]string{"endpoint": name}}
		})
	}

	d := Driver{
		Runner: Mix(
			Weighted{Runner: endpoint("list", 5*time.Millisecond), Weight: 1},
			Weighted{Runner: endpoint("search", 50*time.Millisecond), Weight: 1},
		),
		Rate:        Constant(10000),
		Workers:     8,
		MaxRequests: 500,
	}

	report := NewStats()
	byRoute := NewLabeledStats(Labeled[*Stats]{Key: "endpoint", New: NewStats})
	measured := AfterN(100, Multi(report, byRoute))

	Drain(d.Run(context.Background()), measured)

	if got, want := report.Snapshot().Count, int64(400); got != want {
		t.Fatalf("report Count=%d want %d", got, want)
	}
	if got, want := byRoute.Snapshot().Count, int64(400); got != want {
		t.Fatalf("breakdown total Count=%d want %d", got, want)
	}
	if got := measured.Skipped(); got != 100 {
		t.Fatalf("Skipped()=%d want 100", got)
	}
	if got := len(byRoute.Series()); got != 2 {
		t.Fatalf("len(Series())=%d want 2", got)
	}
}

// TestCompositionOrderIsVisibleInTheExpression asserts the difference the spec
// claims between wrapping the fan-out and fanning out to wrapped recorders.
func TestCompositionOrderIsVisibleInTheExpression(t *testing.T) {
	base := time.Unix(0, 0)
	stream := make([]Result, 0, 30)
	for i := range 30 {
		at := base.Add(time.Duration(i) * time.Millisecond)
		stream = append(stream, Result{Scheduled: at, Start: at, Latency: time.Millisecond})
	}

	// Warmup wraps the fan-out: both recorders see the same measured population.
	a1, b1 := NewStats(), NewStats()
	outer := After(10*time.Millisecond, Multi(a1, b1))
	for _, r := range stream {
		outer.Record(r)
	}
	if a1.Snapshot().Count != 20 || b1.Snapshot().Count != 20 {
		t.Fatalf("After(Multi): a=%d b=%d want 20 and 20", a1.Snapshot().Count, b1.Snapshot().Count)
	}

	// Fan out to one warmed and one unwarmed recorder: different populations,
	// and the difference is in the expression rather than in a flag.
	a2, b2 := NewStats(), NewStats()
	inner := Multi(After(10*time.Millisecond, a2), b2)
	for _, r := range stream {
		inner.Record(r)
	}
	if a2.Snapshot().Count != 20 {
		t.Fatalf("Multi(After(a), b): a=%d want 20", a2.Snapshot().Count)
	}
	if b2.Snapshot().Count != 30 {
		t.Fatalf("Multi(After(a), b): b=%d want 30 (unwarmed)", b2.Snapshot().Count)
	}
}

func TestFilterAndSkipperNestInBothDirections(t *testing.T) {
	base := time.Unix(0, 0)
	only500 := func(r Result) bool { return r.Code == "500" }

	mk := func(i int) Result {
		at := base.Add(time.Duration(i) * time.Millisecond)
		code := "200"
		if i%2 == 0 {
			code = "500"
		}
		return Result{Scheduled: at, Start: at, Latency: time.Millisecond, Code: code}
	}

	// Filter inside a Skipper: skip the first 10, then keep the 500s.
	inner := NewStats()
	outer := AfterN(10, Filter(only500, inner))
	for i := range 30 {
		outer.Record(mk(i))
	}
	if got := inner.Snapshot().Count; got != 10 { // i=10..29, even -> 10
		t.Fatalf("AfterN(Filter): Count=%d want 10", got)
	}
	if got := outer.Skipped(); got != 10 {
		t.Fatalf("AfterN(Filter): Skipped()=%d want 10", got)
	}

	// Skipper inside a Filter: only the 500s reach the Skipper, so its own
	// count of ten is ten *500s*, not ten Results.
	inner2 := NewStats()
	skip := AfterN(10, inner2)
	f := Filter(only500, skip)
	for i := range 30 {
		f.Record(mk(i))
	}
	if got := inner2.Snapshot().Count; got != 5 { // 15 500s, first 10 skipped
		t.Fatalf("Filter(AfterN): Count=%d want 5", got)
	}
}

func TestCombinatorKnobEnds(t *testing.T) {
	base := time.Unix(0, 0)
	stream := func(n int) []Result {
		out := make([]Result, 0, n)
		for i := range n {
			at := base.Add(time.Duration(i) * time.Millisecond)
			out = append(out, Result{Scheduled: at, Start: at, Latency: time.Millisecond})
		}
		return out
	}

	cases := map[string]struct {
		build       func(Recorder) *Skipper
		wantCount   int64
		wantSkipped int64
	}{
		"After 0":                  {func(r Recorder) *Skipper { return After(0, r) }, 20, 0},
		"After the whole stream":   {func(r Recorder) *Skipper { return After(20*time.Millisecond, r) }, 0, 20},
		"After one interval":       {func(r Recorder) *Skipper { return After(time.Millisecond, r) }, 19, 1},
		"AfterN 0":                 {func(r Recorder) *Skipper { return AfterN(0, r) }, 20, 0},
		"AfterN 1":                 {func(r Recorder) *Skipper { return AfterN(1, r) }, 19, 1},
		"AfterN the whole stream":  {func(r Recorder) *Skipper { return AfterN(20, r) }, 0, 20},
		"AfterN past the stream":   {func(r Recorder) *Skipper { return AfterN(999, r) }, 0, 20},
		"AfterTime before the run": {func(r Recorder) *Skipper { return AfterTime(base.Add(-time.Hour), r) }, 20, 0},
		"AfterTime after the run":  {func(r Recorder) *Skipper { return AfterTime(base.Add(time.Hour), r) }, 0, 20},
	}

	for name, tc := range cases {
		t.Run(name, func(t *testing.T) {
			stats := NewStats()
			s := tc.build(stats)
			for _, r := range stream(20) {
				s.Record(r)
			}

			if got := stats.Snapshot().Count; got != tc.wantCount {
				t.Fatalf("Count=%d want %d", got, tc.wantCount)
			}
			if got := s.Skipped(); got != tc.wantSkipped {
				t.Fatalf("Skipped()=%d want %d", got, tc.wantSkipped)
			}
			if got, want := stats.Snapshot().Count+s.Skipped(), int64(20); got != want {
				t.Fatalf("Count+Skipped=%d want %d", got, want)
			}
		})
	}
}

func TestMultiWithOneRecorderIsAPassThrough(t *testing.T) {
	stats := NewStats()
	m := Multi(stats)
	m.Record(Result{Start: time.Unix(0, 0), Latency: time.Millisecond})

	if got := m.Snapshot().Count; got != 1 {
		t.Fatalf("Count=%d want 1", got)
	}
}

func TestAfterPairedWithPhaseEndKeepsOneSourceOfTruth(t *testing.T) {
	rate := Phased{Phases: []Phase{
		{Duration: 5 * time.Millisecond, TargetRPS: 10},
		{Duration: 25 * time.Millisecond, TargetRPS: 100},
	}}

	base := time.Unix(0, 0)
	stats := NewStats()
	s := After(rate.PhaseEnd(0), stats)
	for i := range 30 {
		at := base.Add(time.Duration(i) * time.Millisecond)
		s.Record(Result{Scheduled: at, Start: at, Latency: time.Millisecond})
	}

	if got := s.Skipped(); got != 5 {
		t.Fatalf("Skipped()=%d want 5 — the first phase's duration", got)
	}
	if got := stats.Snapshot().Count; got != 25 {
		t.Fatalf("Count=%d want 25", got)
	}
}
