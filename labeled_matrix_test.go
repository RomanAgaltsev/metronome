package metronome

import (
	"context"
	"testing"
	"time"
)

// TestLabeledStatsSeparatesASlowEndpointTheTotalHides is the motivating defect,
// encoded. A Mix of three endpoints where one is an order of magnitude slower
// reports an aggregate P99 that no endpoint exhibits.
func TestLabeledStatsSeparatesASlowEndpointTheTotalHides(t *testing.T) {
	const (
		fast = 5 * time.Millisecond
		slow = 200 * time.Millisecond
	)

	ls := newLabeled(t, Labeled[*Stats]{})
	base := time.Unix(0, 0)

	// Two fast endpoints and one slow one, evenly mixed.
	for i := range 300 {
		at := base.Add(time.Duration(i) * time.Millisecond)
		switch i % 3 {
		case 0:
			ls.Record(labelled("endpoint", "slow", slow, at))
		case 1:
			ls.Record(labelled("endpoint", "fast_a", fast, at))
		default:
			ls.Record(labelled("endpoint", "fast_b", fast, at))
		}
	}

	series := ls.Series()
	slowP99 := series["slow"].Snapshot().P99
	fastP99 := series["fast_a"].Snapshot().P99
	totalP99 := ls.Snapshot().P99

	// The per-series numbers are the real ones.
	if slowP99 < slow || slowP99 > slow+time.Millisecond {
		t.Fatalf("slow series P99=%v want ~%v", slowP99, slow)
	}
	if fastP99 < fast || fastP99 > fast+time.Millisecond {
		t.Fatalf("fast series P99=%v want ~%v", fastP99, fast)
	}

	// And the total is a number neither endpoint exhibits. Asserted so the
	// defect this feature exists to fix is a documented property, not an
	// anecdote.
	if totalP99 <= fastP99 {
		t.Fatalf("total P99=%v should exceed the fast endpoints' %v", totalP99, fastP99)
	}
	if got, want := sumSeriesCount(ls), ls.Snapshot().Count; got != want {
		t.Fatalf("series=%d total=%d", got, want)
	}
}

// TestLabeledStatsReadsLabelsStampedThroughARealDriver proves the labels
// survive the Mix and Driver pipeline a consumer actually uses, not just
// hand-built Results.
func TestLabeledStatsReadsLabelsStampedThroughARealDriver(t *testing.T) {
	runner := func(name string, lat time.Duration) Runner {
		return RunnerFunc(func(context.Context) Result {
			return Result{Latency: lat, Labels: map[string]string{"endpoint": name}}
		})
	}

	d := Driver{
		Runner: Mix(
			Weighted{Runner: runner("slow", 200*time.Millisecond), Weight: 1},
			Weighted{Runner: runner("fast", 5*time.Millisecond), Weight: 1},
		),
		Rate:        Constant(10000),
		Workers:     4,
		MaxRequests: 400,
	}

	ls := newLabeled(t, Labeled[*Stats]{})
	for r := range d.Run(context.Background()) {
		ls.Record(r)
	}

	series := ls.Series()
	if len(series) != 2 {
		t.Fatalf("len(Series())=%d want 2, keys=%v", len(series), series)
	}
	if got := series["slow"].Snapshot().P50; got < 200*time.Millisecond {
		t.Fatalf("slow P50=%v want >= 200ms", got)
	}
	if got := series["fast"].Snapshot().P50; got > 6*time.Millisecond {
		t.Fatalf("fast P50=%v want <= 6ms", got)
	}
	if got, want := ls.Snapshot().Count, int64(400); got != want {
		t.Fatalf("total Count=%d want %d", got, want)
	}
	if got := sumSeriesCount(ls); got != 400 {
		t.Fatalf("series total=%d want 400", got)
	}
}

// TestLabeledStatsAcceptsTheEndsOfEveryKnobRange drives a real stream through
// every end of every Labeled field, in both child types. The v0.4 matrix test
// exists because a hang reachable from Ramp{Start: 0} survived two reviews;
// this is the same treatment for four new knobs.
func TestLabeledStatsAcceptsTheEndsOfEveryKnobRange(t *testing.T) {
	base := time.Unix(0, 0)

	feed := func(rec Recorder, distinct, n int) {
		for i := range n {
			value := string(rune('a' + i%distinct))
			if i%11 == 0 {
				value = "" // some unlabeled traffic in every case
			}
			rec.Record(labelled("endpoint", value, time.Duration(i%20+1)*time.Millisecond,
				base.Add(time.Duration(i)*time.Millisecond)))
		}
	}

	cases := map[string]struct {
		cfg      Labeled[*Stats]
		distinct int
		wantLen  int
	}{
		"MaxSeries 1": {
			cfg: Labeled[*Stats]{Key: "endpoint", New: NewStats, MaxSeries: 1}, distinct: 9, wantLen: 2,
		},
		"MaxSeries larger than the distinct values": {
			cfg: Labeled[*Stats]{Key: "endpoint", New: NewStats, MaxSeries: 50}, distinct: 3, wantLen: 4,
		},
		"exactly MaxSeries distinct values, plus unlabeled": {
			// Three real values and the unlabeled sentinel: the sentinel is the
			// fourth named series, so one value overflows. The off-by-one bait.
			cfg: Labeled[*Stats]{Key: "endpoint", New: NewStats, MaxSeries: 3}, distinct: 3, wantLen: 4,
		},
		"default MaxSeries": {
			cfg: Labeled[*Stats]{Key: "endpoint", New: NewStats}, distinct: 5, wantLen: 6,
		},
		"custom sentinels": {
			cfg: Labeled[*Stats]{
				Key: "endpoint", New: NewStats, MaxSeries: 2,
				Overflow: "OTHER", Unlabeled: "NONE",
			}, distinct: 9, wantLen: 3,
		},
		"narrow-range children": {
			cfg: Labeled[*Stats]{
				Key: "endpoint",
				New: func() *Stats { return NewStatsRange(time.Millisecond, time.Second, 1) },
			}, distinct: 4, wantLen: 5,
		},
	}

	for name, tc := range cases {
		t.Run(name, func(t *testing.T) {
			ls := NewLabeledStats(tc.cfg)
			feed(ls, tc.distinct, 200)

			if got := ls.Len(); got != tc.wantLen {
				t.Fatalf("Len()=%d want %d, series=%v", got, tc.wantLen, ls.Series())
			}
			if got, want := sumSeriesCount(ls), ls.Snapshot().Count; got != want {
				t.Fatalf("series=%d total=%d", got, want)
			}
			if got, want := ls.Snapshot().Count, int64(200); got != want {
				t.Fatalf("total Count=%d want %d", got, want)
			}
			if got := ls.Bytes(); got <= 0 {
				t.Fatalf("Bytes()=%d want > 0", got)
			}
		})
	}
}

// TestLabeledStatsKnobEndsHoldOverRollingChildren runs the same knob ends over
// R = *RollingStats.
//
// The generic parameter is the one thing the *Stats matrix above cannot vary,
// and the routing, capping and reconciliation must not depend on it: every
// series here is 3.2 MiB rather than 272 KiB, so the cases are the shapes where
// the cap and the sentinels interact, not the whole grid. The narrowed
// histogram range keeps the whole test around 2 MiB.
func TestLabeledStatsKnobEndsHoldOverRollingChildren(t *testing.T) {
	base := time.Unix(0, 0)

	// A narrow range and a small ring: the point here is series accounting, not
	// histogram resolution, and the default would allocate ~3.2 MiB per series.
	newRolling := func(clock Clock) func() *RollingStats {
		return func() *RollingStats {
			return NewRollingStats(Rolling{
				Window: time.Second, Buckets: 2, Clock: clock,
				Lo: time.Millisecond, Hi: time.Second, Sigfigs: 1,
			})
		}
	}

	cases := map[string]struct {
		maxSeries int
		distinct  int
		wantLen   int
	}{
		"MaxSeries 1": {maxSeries: 1, distinct: 9, wantLen: 2},
		"MaxSeries larger than the distinct values": {maxSeries: 50, distinct: 3, wantLen: 4},
		"exactly MaxSeries, plus unlabeled":         {maxSeries: 3, distinct: 3, wantLen: 4},
	}

	for name, tc := range cases {
		t.Run(name, func(t *testing.T) {
			clock := NewManualClock(base)
			ls := NewLabeledStats(Labeled[*RollingStats]{
				Key: "endpoint", New: newRolling(clock), MaxSeries: tc.maxSeries,
			})

			for i := range 100 {
				value := string(rune('a' + i%tc.distinct))
				if i%11 == 0 {
					value = "" // some unlabeled traffic in every case
				}
				ls.Record(labelled("endpoint", value, time.Duration(i%20+1)*time.Millisecond,
					base.Add(time.Duration(i)*time.Millisecond)))
			}

			if got := ls.Len(); got != tc.wantLen {
				t.Fatalf("Len()=%d want %d, series=%v", got, tc.wantLen, ls.Series())
			}
			if got, want := ls.Snapshot().Count, int64(100); got != want {
				t.Fatalf("total Count=%d want %d", got, want)
			}

			var sum int64
			for _, child := range ls.Series() {
				sum += child.Snapshot().Count
				// The typed access the generic parameter exists for: no assertion.
				if w := child.Window(); w.Window <= 0 {
					t.Fatalf("Window() reported Window=%v, want a positive covered duration", w.Window)
				}
			}
			if sum != ls.Snapshot().Count {
				t.Fatalf("series=%d total=%d", sum, ls.Snapshot().Count)
			}

			// Bytes reaches through the optional interface to rolling children
			// exactly as it does to flat ones.
			if got, want := ls.Bytes(), int64(tc.wantLen+1)*ls.Total().Bytes(); got != want {
				t.Fatalf("Bytes()=%d want %d (%d series + the total)", got, want, tc.wantLen)
			}
		})
	}
}

// countingRecorder is a Recorder that reports no size, so LabeledStats.Bytes
// must say so rather than guess.
type countingRecorder struct{ n int64 }

func (c *countingRecorder) Record(Result)      { c.n++ }
func (c *countingRecorder) Snapshot() Snapshot { return Snapshot{Count: c.n} }

func TestLabeledStatsBytesIsUnknownForAnUnmeasurableChild(t *testing.T) {
	ls := NewLabeledStats(Labeled[*countingRecorder]{
		Key: "endpoint",
		New: func() *countingRecorder { return &countingRecorder{} },
	})
	ls.Record(labelled("endpoint", "a", time.Millisecond, time.Unix(0, 0)))

	if got := ls.Bytes(); got != -1 {
		t.Fatalf("Bytes()=%d want -1 for a child that does not report its size", got)
	}
	if got := ls.Snapshot().Count; got != 1 {
		t.Fatalf("a custom Recorder still totals: Count=%d want 1", got)
	}
}

func TestLabeledStatsBytesGrowsWithEachNewSeries(t *testing.T) {
	ls := newLabeled(t, Labeled[*Stats]{})
	one := NewStats().Bytes()
	base := time.Unix(0, 0)

	if got := ls.Bytes(); got != one {
		t.Fatalf("Bytes()=%d want %d (total only)", got, one)
	}
	ls.Record(labelled("endpoint", "a", time.Millisecond, base))
	if got := ls.Bytes(); got != 2*one {
		t.Fatalf("Bytes()=%d want %d (total + one series)", got, 2*one)
	}
	ls.Record(labelled("endpoint", "b", time.Millisecond, base.Add(time.Millisecond)))
	if got := ls.Bytes(); got != 3*one {
		t.Fatalf("Bytes()=%d want %d (total + two series)", got, 3*one)
	}
}
