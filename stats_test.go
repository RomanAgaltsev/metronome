package metronome

import (
	"errors"
	"maps"
	"math"
	"reflect"
	"strings"
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

// Clamped counts Results, so it can never exceed Count. It used to be incremented
// once for the raw histogram and again for the corrected one, so a single
// Driver-produced Result with an out-of-range latency counted twice.
func TestSnapshotClampedNeverExceedsCount(t *testing.T) {
	s := NewStats()
	base := time.Unix(0, 0)
	s.Record(Result{Scheduled: base, Start: base, Latency: 5 * time.Minute}) // above the 60s bound

	snap := s.Snapshot()
	if snap.Clamped != 1 {
		t.Fatalf("Clamped=%d want 1 — one Result clamped once", snap.Clamped)
	}
	if snap.Clamped > snap.Count {
		t.Fatalf("Clamped=%d exceeds Count=%d", snap.Clamped, snap.Count)
	}
}

// A raw latency well inside the histogram can still overflow the corrected
// histogram once the queueing delay is added. That is a different fact about a
// different number, and conflating the two sends the reader to widen the range
// for percentiles that were never clamped.
func TestSnapshotSeparatesCorrectedClamping(t *testing.T) {
	s := NewStats()
	base := time.Unix(0, 0)
	s.Record(Result{Scheduled: base, Start: base.Add(2 * time.Minute), Latency: 10 * time.Millisecond})

	snap := s.Snapshot()
	if snap.Clamped != 0 {
		t.Fatalf("Clamped=%d want 0 — the raw 10ms latency is inside [1µs, 60s]", snap.Clamped)
	}
	if snap.CorrectedClamped != 1 {
		t.Fatalf("CorrectedClamped=%d want 1 — 2m10ms is above the 60s bound", snap.CorrectedClamped)
	}
}

func TestSnapshotTracksMaxScheduleLag(t *testing.T) {
	s := NewStats()
	base := time.Unix(0, 0)
	s.Record(Result{Scheduled: base, Start: base.Add(10 * time.Millisecond), Latency: time.Millisecond})
	s.Record(Result{Scheduled: base, Start: base.Add(250 * time.Millisecond), Latency: time.Millisecond})
	s.Record(Result{Scheduled: base.Add(time.Second), Start: base, Latency: time.Millisecond}) // early

	if got := s.Snapshot().MaxScheduleLag; got != 250*time.Millisecond {
		t.Fatalf("MaxScheduleLag=%v want 250ms (an early unit must not count as negative lag)", got)
	}
}

func TestSnapshotStringLeadsWithTheNumbersThatMatter(t *testing.T) {
	s := NewStats()
	base := time.Unix(0, 0)
	s.Record(Result{Scheduled: base, Start: base, Latency: 10 * time.Millisecond})
	s.Record(Result{
		Scheduled: base,
		Start:     base.Add(time.Second),
		Latency:   20 * time.Millisecond,
		Err:       ErrSaturated,
	})

	got := s.Snapshot().String()
	for _, want := range []string{"2 req", "saturated", "corrected", "behind schedule"} {
		if !strings.Contains(got, want) {
			t.Errorf("Snapshot.String() = %q, missing %q", got, want)
		}
	}
}

func TestSnapshotCountsSaturation(t *testing.T) {
	s := NewStats()
	base := time.Unix(0, 0)
	s.Record(Result{Start: base, Latency: 10 * time.Millisecond})
	s.Record(Result{Start: base.Add(time.Second), Latency: time.Millisecond, Err: ErrSaturated})
	s.Record(Result{Start: base.Add(2 * time.Second), Latency: time.Millisecond, Err: errors.New("target said no")})

	snap := s.Snapshot()
	if snap.Errors != 2 {
		t.Fatalf("Errors=%d want 2", snap.Errors)
	}
	if snap.Saturated != 1 {
		t.Fatalf("Saturated=%d want 1 — generator saturation must be separable from target errors",
			snap.Saturated)
	}
}

// Throughput and RPS must be the same kind of estimate: N Result.Start stamps
// bound N-1 intervals, so bytes/span (which spends all N samples over N-1
// intervals) carries exactly the bias Snapshot.RPS was fixed to avoid.
func TestSnapshotThroughputAgreesWithRPS(t *testing.T) {
	s := NewStats()
	base := time.Unix(0, 0)
	s.Record(Result{Start: base, Latency: time.Millisecond, Bytes: 1000})
	s.Record(Result{Start: base.Add(time.Second), Latency: time.Millisecond, Bytes: 1000})

	snap := s.Snapshot()
	want := snap.RPS * float64(snap.Bytes) / float64(snap.Count) // rps x mean bytes/request
	if math.Abs(snap.Throughput-want) > 1e-9 {
		t.Fatalf("Throughput=%v want %v (RPS=%v, %d bytes over %d results)",
			snap.Throughput, want, snap.RPS, snap.Bytes, snap.Count)
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
	// 6 Results spanning 1s bound 5 intervals, so RPS is 5 and the mean payload
	// is 5000/6 bytes; Throughput is their product. (Bytes/span would say 5000,
	// which is the N/(N-1) bias Snapshot.RPS exists to avoid — see
	// TestSnapshotThroughputAgreesWithRPS.)
	if want := 5.0 * 5000 / 6; math.Abs(snap.Throughput-want) > 1e-6 {
		t.Fatalf("Throughput=%v want %v bytes/sec", snap.Throughput, want)
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

func TestSnapshotCorrectsForCoordinatedOmission(t *testing.T) {
	s := NewStats()
	base := time.Unix(0, 0)

	// A schedule of one unit every 10ms that the target stalled on: each unit
	// starts 100ms after it was due, and then takes 10ms.
	for i := range 100 {
		sched := base.Add(time.Duration(i) * 10 * time.Millisecond)
		s.Record(Result{
			Scheduled: sched,
			Start:     sched.Add(100 * time.Millisecond),
			Latency:   10 * time.Millisecond,
		})
	}

	snap := s.Snapshot()
	if snap.CorrectedCount != 100 {
		t.Fatalf("CorrectedCount=%d want 100", snap.CorrectedCount)
	}
	if snap.P50 < 9*time.Millisecond || snap.P50 > 11*time.Millisecond {
		t.Fatalf("raw P50=%v want ~10ms (raw percentiles must be unchanged)", snap.P50)
	}
	// 10ms of service + 100ms of queueing the naive number hides.
	if snap.CorrectedP50 < 105*time.Millisecond || snap.CorrectedP50 > 115*time.Millisecond {
		t.Fatalf("CorrectedP50=%v want ~110ms", snap.CorrectedP50)
	}
}

func TestSnapshotCorrectedIsZeroWithoutScheduledStamps(t *testing.T) {
	s := NewStats()
	s.Record(Result{Start: time.Unix(0, 0), Latency: 10 * time.Millisecond}) // no Scheduled
	snap := s.Snapshot()
	if snap.CorrectedCount != 0 || snap.CorrectedP95 != 0 {
		t.Fatalf("CorrectedCount=%d CorrectedP95=%v; both must be zero without stamps",
			snap.CorrectedCount, snap.CorrectedP95)
	}
}

func TestSnapshotCorrectedNeverBelowRaw(t *testing.T) {
	s := NewStats()
	base := time.Unix(0, 0)
	// Start *before* Scheduled (the generator ran early): the correction must
	// floor at zero, not subtract from the measured latency.
	s.Record(Result{Scheduled: base.Add(time.Second), Start: base, Latency: 10 * time.Millisecond})
	snap := s.Snapshot()
	if snap.CorrectedP50 < snap.P50 {
		t.Fatalf("CorrectedP50=%v < P50=%v; correction must never reduce latency", snap.CorrectedP50, snap.P50)
	}
}

func TestStatsSnapshotWindowUsesCountOverWindow(t *testing.T) {
	s := NewStats()
	base := time.Unix(0, 0)
	for i := range 50 {
		s.Record(Result{
			Start:   base.Add(time.Duration(i) * time.Millisecond),
			Latency: time.Millisecond,
			Bytes:   100,
		})
	}

	// Lifetime: 50 samples bound 49ms, so (50-1)/0.049 == 1000 rps, and a
	// lifetime Snapshot carries no window.
	life := s.Snapshot()
	if life.Window != 0 {
		t.Fatalf("lifetime Window=%v want 0", life.Window)
	}
	if life.RPS < 999 || life.RPS > 1001 {
		t.Fatalf("lifetime RPS=%v want ~1000", life.RPS)
	}

	// Windowed: the caller knows these 50 Results cover 5s, so the rate is
	// exactly 10 rps — no inference from timestamps, no N-1 correction.
	win := s.snapshot(5 * time.Second)
	if win.Window != 5*time.Second {
		t.Fatalf("Window=%v want 5s", win.Window)
	}
	if win.RPS != 10 {
		t.Fatalf("windowed RPS=%v want exactly 10", win.RPS)
	}
	if win.Throughput != 1000 {
		t.Fatalf("windowed Throughput=%v want 1000 (10 rps x 100 bytes)", win.Throughput)
	}
}

func TestStatsSnapshotWindowReportsZeroRateWhenEmpty(t *testing.T) {
	// The property the lifetime estimator structurally cannot have: no data over
	// a known duration is a rate of zero, not an undefined one.
	got := NewStats().snapshot(5 * time.Second)
	if got.RPS != 0 {
		t.Fatalf("RPS=%v want 0", got.RPS)
	}
	if got.Window != 5*time.Second {
		t.Fatalf("Window=%v want 5s", got.Window)
	}
}

// buildMergeResults returns a fixed, varied Result set: several latencies, some
// errors, two codes, byte counts and schedule stamps, so a merge that drops any
// one field shows up.
func buildMergeResults() []Result {
	base := time.Unix(0, 0)
	out := make([]Result, 0, 300)
	for i := range 300 {
		r := Result{
			Start:     base.Add(time.Duration(i) * time.Millisecond),
			Scheduled: base.Add(time.Duration(i)*time.Millisecond - time.Duration(i%9)*time.Millisecond),
			Latency:   time.Duration(i%50+1) * time.Millisecond,
			Bytes:     int64(i),
			Code:      [2]string{"200", "500"}[i%2],
		}
		if i%7 == 0 {
			r.Err = errors.New("x")
		}
		out = append(out, r)
	}
	return out
}

func TestStatsMergeEqualsOneStatsFedTheSameResults(t *testing.T) {
	results := buildMergeResults()

	whole := NewStats()
	for _, r := range results {
		whole.Record(r)
	}

	// The same Results dealt round-robin into three Stats, then merged back.
	parts := []*Stats{NewStats(), NewStats(), NewStats()}
	for i, r := range results {
		parts[i%3].Record(r)
	}
	merged := NewStats()
	for _, p := range parts {
		merged.merge(p)
	}

	want, got := whole.Snapshot(), merged.Snapshot()
	if !reflect.DeepEqual(want, got) {
		t.Fatalf("merged snapshot differs\nwant %+v\n got %+v", want, got)
	}
}

func TestStatsResetReturnsToTheConstructedState(t *testing.T) {
	s := NewStats()
	for _, r := range buildMergeResults() {
		s.Record(r)
	}
	s.reset()

	if got, want := s.Snapshot(), NewStats().Snapshot(); !reflect.DeepEqual(got, want) {
		t.Fatalf("after reset snapshot=%+v want %+v", got, want)
	}
}

func TestStatsResetThenRecordIsIndependentOfHistory(t *testing.T) {
	// A recycled bucket must not let an old maximum survive: this is the exact
	// defect the rolling window exists to fix, at bucket scope.
	s := NewStats()
	s.Record(Result{Start: time.Unix(0, 0), Scheduled: time.Unix(0, 0).Add(-5 * time.Second), Latency: time.Second})
	s.reset()
	s.Record(Result{Start: time.Unix(1, 0), Scheduled: time.Unix(1, 0), Latency: time.Millisecond})

	snap := s.Snapshot()
	if snap.MaxScheduleLag != 0 {
		t.Fatalf("MaxScheduleLag=%v want 0 after reset", snap.MaxScheduleLag)
	}
	if snap.Max != time.Millisecond {
		t.Fatalf("Max=%v want 1ms after reset", snap.Max)
	}
}

func TestStatsExportedMergeEqualsOneStatsFedTheSameResults(t *testing.T) {
	results := buildMergeResults()

	whole := NewStats()
	for _, r := range results {
		whole.Record(r)
	}

	parts := []*Stats{NewStats(), NewStats(), NewStats()}
	for i, r := range results {
		parts[i%3].Record(r)
	}
	merged := NewStats()
	for _, p := range parts {
		merged.Merge(p)
	}

	want, got := whole.Snapshot(), merged.Snapshot()
	if !reflect.DeepEqual(want, got) {
		t.Fatalf("merged snapshot differs\nwant %+v\n got %+v", want, got)
	}
}

func TestStatsMergePanicsOnAMismatchedRange(t *testing.T) {
	for name, src := range map[string]*Stats{
		"lo":      NewStatsRange(2*time.Microsecond, time.Minute, 3),
		"hi":      NewStatsRange(time.Microsecond, 2*time.Minute, 3),
		"sigfigs": NewStatsRange(time.Microsecond, time.Minute, 2),
	} {
		t.Run(name, func(t *testing.T) {
			defer func() {
				if recover() == nil {
					t.Fatal("Merge did not panic on a mismatched range")
				}
			}()
			NewStats().Merge(src)
		})
	}
}

func TestStatsMergePanicsOnSelfMerge(t *testing.T) {
	defer func() {
		if recover() == nil {
			t.Fatal("Merge did not panic merging a Stats into itself")
		}
	}()
	s := NewStats()
	s.Merge(s)
}

func TestStatsMergeInBothDirectionsDoesNotDeadlock(t *testing.T) {
	// Address-ordered locking is what makes this terminate. Without it the two
	// goroutines take a.mu/b.mu in opposite orders and the test hangs.
	a, b := NewStats(), NewStats()
	for i, r := range buildMergeResults() {
		if i%2 == 0 {
			a.Record(r)
		} else {
			b.Record(r)
		}
	}

	done := make(chan struct{})
	go func() {
		defer close(done)
		for range 200 {
			a.Merge(b)
		}
	}()
	for range 200 {
		b.Merge(a)
	}
	<-done
}

func TestStatsMergeOfAnEmptyStatsIsANoOp(t *testing.T) {
	s := NewStats()
	for _, r := range buildMergeResults() {
		s.Record(r)
	}
	want := s.Snapshot()

	s.Merge(NewStats())

	if got := s.Snapshot(); !reflect.DeepEqual(got, want) {
		t.Fatalf("merging an empty Stats changed the aggregate\nwant %+v\n got %+v", want, got)
	}
}

func TestStatsMergeIntoAnEmptyStatsIsACopy(t *testing.T) {
	src := NewStats()
	for _, r := range buildMergeResults() {
		src.Record(r)
	}

	dst := NewStats()
	dst.Merge(src)

	if got, want := dst.Snapshot(), src.Snapshot(); !reflect.DeepEqual(got, want) {
		t.Fatalf("merge into empty is not a copy\nwant %+v\n got %+v", want, got)
	}
}
