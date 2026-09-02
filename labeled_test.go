package metronome

import (
	"reflect"
	"testing"
	"time"
)

// newLabeled is the common construction used across this file.
func newLabeled(t *testing.T, cfg Labeled[*Stats]) *LabeledStats[*Stats] {
	t.Helper()
	if cfg.Key == "" {
		cfg.Key = "endpoint"
	}
	if cfg.New == nil {
		cfg.New = NewStats
	}
	return NewLabeledStats(cfg)
}

func TestNewLabeledStatsAcceptsTheZeroValuedOptionalFields(t *testing.T) {
	ls := newLabeled(t, Labeled[*Stats]{})
	if ls == nil {
		t.Fatal("NewLabeledStats returned nil")
	}
	if got := ls.Len(); got != 0 {
		t.Fatalf("Len()=%d want 0 before any Record", got)
	}
}

func TestNewLabeledStatsPanicsOnProgrammerError(t *testing.T) {
	cases := map[string]Labeled[*Stats]{
		"empty Key":                     {New: NewStats},
		"nil New":                       {Key: "endpoint"},
		"negative MaxSeries":            {Key: "endpoint", New: NewStats, MaxSeries: -1},
		"colliding sentinels":           {Key: "endpoint", New: NewStats, Overflow: "x", Unlabeled: "x"},
		"Overflow == default Unlabeled": {Key: "endpoint", New: NewStats, Overflow: DefaultUnlabeledLabel},
	}
	for name, cfg := range cases {
		t.Run(name, func(t *testing.T) {
			defer func() {
				if recover() == nil {
					t.Fatal("NewLabeledStats did not panic")
				}
			}()
			NewLabeledStats(cfg)
		})
	}
}

func TestNewLabeledStatsAllocatesOnlyTheTotalUpFront(t *testing.T) {
	// Series are lazy: a fresh LabeledStats holds exactly one child.
	ls := newLabeled(t, Labeled[*Stats]{})
	if got, want := ls.Bytes(), NewStats().Bytes(); got != want {
		t.Fatalf("Bytes()=%d want %d (the total alone)", got, want)
	}
}

func TestLabeledStatsSatisfiesRecorder(t *testing.T) {
	var _ Recorder = newLabeled(t, Labeled[*Stats]{})
	var _ Recorder = NewLabeledStats(Labeled[*RollingStats]{
		Key: "endpoint",
		New: func() *RollingStats { return NewRollingStats(Rolling{}) },
	})
}

// labelled returns a Result carrying key=value, with a fixed non-zero latency
// so that percentiles are meaningful.
func labelled(key, value string, lat time.Duration, at time.Time) Result {
	r := Result{Start: at, Latency: lat}
	if value != "" {
		r.Labels = map[string]string{key: value}
	}
	return r
}

// sumSeriesCount is the invariant's left-hand side.
func sumSeriesCount[R Recorder](ls *LabeledStats[R]) int64 {
	var n int64
	for _, c := range ls.Series() {
		n += c.Snapshot().Count
	}
	return n
}

func TestLabeledStatsRoutesEachResultToItsOwnSeries(t *testing.T) {
	ls := newLabeled(t, Labeled[*Stats]{})
	base := time.Unix(0, 0)

	for i := range 30 {
		name := []string{"a", "b", "c"}[i%3]
		ls.Record(labelled("endpoint", name, time.Duration(i%3+1)*time.Millisecond, base.Add(time.Duration(i)*time.Millisecond)))
	}

	series := ls.Series()
	if got := len(series); got != 3 {
		t.Fatalf("len(Series())=%d want 3", got)
	}
	for _, name := range []string{"a", "b", "c"} {
		if got := series[name].Snapshot().Count; got != 10 {
			t.Fatalf("series %q Count=%d want 10", name, got)
		}
	}
}

func TestLabeledStatsTotalEqualsAPlainStatsFedTheSameStream(t *testing.T) {
	ls := newLabeled(t, Labeled[*Stats]{})
	plain := NewStats()
	base := time.Unix(0, 0)

	for i := range 90 {
		r := labelled("endpoint", []string{"a", "b", "c"}[i%3],
			time.Duration(i%17+1)*time.Millisecond, base.Add(time.Duration(i)*time.Millisecond))
		ls.Record(r)
		plain.Record(r)
	}

	if got, want := ls.Snapshot(), plain.Snapshot(); !reflect.DeepEqual(got, want) {
		t.Fatalf("total differs from a plain Stats\nwant %+v\n got %+v", want, got)
	}
}

func TestLabeledStatsSeriesAlwaysReconcileWithTheTotal(t *testing.T) {
	// The invariant, across every routing path at once: named series,
	// unlabeled Results, and overflow.
	ls := newLabeled(t, Labeled[*Stats]{MaxSeries: 3})
	base := time.Unix(0, 0)

	for i := range 100 {
		var value string
		switch {
		case i%5 == 0:
			value = "" // unlabeled
		default:
			value = string(rune('a' + i%9)) // nine distinct values, cap of three
		}
		ls.Record(labelled("endpoint", value, time.Millisecond, base.Add(time.Duration(i)*time.Millisecond)))
	}

	if got, want := sumSeriesCount(ls), ls.Snapshot().Count; got != want {
		t.Fatalf("series total=%d want total Count=%d", got, want)
	}
	if got, want := ls.Snapshot().Count, int64(100); got != want {
		t.Fatalf("total Count=%d want %d", got, want)
	}
}

func TestLabeledStatsSendsUnusableLabelsToTheUnlabeledSeries(t *testing.T) {
	ls := newLabeled(t, Labeled[*Stats]{})
	base := time.Unix(0, 0)

	ls.Record(Result{Start: base, Latency: time.Millisecond})                                          // nil Labels
	ls.Record(Result{Start: base, Latency: time.Millisecond, Labels: map[string]string{"other": "x"}}) // wrong key
	ls.Record(Result{Start: base, Latency: time.Millisecond, Labels: map[string]string{"endpoint": ""}})

	if got := ls.Series()[DefaultUnlabeledLabel].Snapshot().Count; got != 3 {
		t.Fatalf("unlabeled series Count=%d want 3", got)
	}
}

func TestLabeledStatsCapsSeriesAtMaxSeries(t *testing.T) {
	ls := newLabeled(t, Labeled[*Stats]{MaxSeries: 2})
	base := time.Unix(0, 0)

	for i := range 10 {
		ls.Record(labelled("endpoint", string(rune('a'+i)), time.Millisecond, base.Add(time.Duration(i)*time.Millisecond)))
	}

	series := ls.Series()
	if got := len(series); got != 3 { // two named plus overflow
		t.Fatalf("len(Series())=%d want 3 (2 named + overflow)", got)
	}
	if got := series[DefaultOverflowLabel].Snapshot().Count; got != 8 {
		t.Fatalf("overflow Count=%d want 8", got)
	}
}

func TestLabeledStatsMergesARealValueCollidingWithTheOverflowName(t *testing.T) {
	ls := newLabeled(t, Labeled[*Stats]{MaxSeries: 1})
	base := time.Unix(0, 0)

	ls.Record(labelled("endpoint", DefaultOverflowLabel, time.Millisecond, base))
	ls.Record(labelled("endpoint", "b", time.Millisecond, base.Add(time.Millisecond)))
	ls.Record(labelled("endpoint", "c", time.Millisecond, base.Add(2*time.Millisecond)))

	series := ls.Series()
	if got := len(series); got != 1 {
		t.Fatalf("len(Series())=%d want 1 — the real value and overflow share a series", got)
	}
	if got := series[DefaultOverflowLabel].Snapshot().Count; got != 3 {
		t.Fatalf("overflow Count=%d want 3", got)
	}
	if got, want := sumSeriesCount(ls), ls.Snapshot().Count; got != want {
		t.Fatalf("collision broke the invariant: series=%d total=%d", got, want)
	}
}

func TestLabeledStatsRecordIsSafeUnderConcurrentSeriesCreation(t *testing.T) {
	ls := newLabeled(t, Labeled[*Stats]{MaxSeries: 4})
	base := time.Unix(0, 0)

	const goroutines, each = 8, 250
	done := make(chan struct{})
	for g := range goroutines {
		go func() {
			defer func() { done <- struct{}{} }()
			for i := range each {
				// Every goroutine races to create the same handful of series,
				// and to cross the overflow boundary.
				ls.Record(labelled("endpoint", string(rune('a'+(i+g)%12)), time.Millisecond,
					base.Add(time.Duration(i)*time.Millisecond)))
			}
		}()
	}
	for range goroutines {
		<-done
	}

	if got, want := ls.Snapshot().Count, int64(goroutines*each); got != want {
		t.Fatalf("total Count=%d want %d", got, want)
	}
	if got := sumSeriesCount(ls); got != int64(goroutines*each) {
		t.Fatalf("series total=%d want %d", got, goroutines*each)
	}
	if got := ls.Len(); got != 5 { // four named plus overflow
		t.Fatalf("Len()=%d want 5", got)
	}
}

func TestLabeledStatsSeriesReturnsACopyOfTheMap(t *testing.T) {
	ls := newLabeled(t, Labeled[*Stats]{})
	base := time.Unix(0, 0)
	ls.Record(labelled("endpoint", "a", time.Millisecond, base))

	m := ls.Series()
	delete(m, "a")
	m["injected"] = NewStats()

	if got := ls.Len(); got != 1 {
		t.Fatalf("Len()=%d want 1 — mutating the returned map changed the LabeledStats", got)
	}
	if _, ok := ls.Series()["a"]; !ok {
		t.Fatal(`series "a" disappeared after the caller mutated their copy`)
	}
}

func TestLabeledStatsTotalIsTheSameRecorderSnapshotReports(t *testing.T) {
	ls := newLabeled(t, Labeled[*Stats]{})
	base := time.Unix(0, 0)
	for i := range 20 {
		ls.Record(labelled("endpoint", "a", time.Millisecond, base.Add(time.Duration(i)*time.Millisecond)))
	}

	if got, want := ls.Total().Snapshot(), ls.Snapshot(); !reflect.DeepEqual(got, want) {
		t.Fatalf("Total().Snapshot()=%+v want Snapshot()=%+v", got, want)
	}
}

func TestLabeledStatsOverRollingChildrenGivesTypedPerSeriesWindows(t *testing.T) {
	// The whole reason the type is generic: no assertion here.
	clk := NewManualClock(time.Unix(0, 0))
	ls := NewLabeledStats(Labeled[*RollingStats]{
		Key: "endpoint",
		New: func() *RollingStats {
			return NewRollingStats(Rolling{Window: 10 * time.Second, Buckets: 10, Clock: clk})
		},
	})

	base := clk.Now()
	for i := range 10 {
		ls.Record(labelled("endpoint", "a", 5*time.Millisecond, base.Add(time.Duration(i)*time.Millisecond)))
	}

	if got := ls.Series()["a"].Window().Count; got != 10 {
		t.Fatalf("per-series Window().Count=%d want 10", got)
	}

	// Past the whole window with no traffic, the series drains but the lifetime
	// view does not.
	clk.Advance(30 * time.Second)
	if got := ls.Series()["a"].Window().Count; got != 0 {
		t.Fatalf("per-series Window().Count=%d want 0 after a full stall", got)
	}
	if got := ls.Series()["a"].Snapshot().Count; got != 10 {
		t.Fatalf("per-series lifetime Count=%d want 10", got)
	}
	if got := ls.Total().Window().Count; got != 0 {
		t.Fatalf("total Window().Count=%d want 0 after a full stall", got)
	}
}
