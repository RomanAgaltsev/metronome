package metronome

import (
	"maps"
	"testing"
	"time"

	"pgregory.net/rapid"
)

// denseRolling builds a RollingStats whose buckets are dense, so it can be
// compared against the sparse-bucket production path on identical input. It is
// the v0.8 ring, reconstructed here rather than kept behind a flag: the only
// thing that needs it is the proof that the two agree.
func denseRolling(cfg Rolling) *RollingStats {
	rs := NewRollingStats(cfg)
	c := cfg.config()
	for i := range rs.ring {
		rs.ring[i] = NewStatsRange(c.lo, c.hi, c.sigfigs)
	}
	return rs
}

// reporter is the part of *testing.T that sameSnapshot needs, which *rapid.T
// also satisfies — so the property and the table-driven tests share one
// comparison rather than keeping two that could drift apart.
type reporter interface {
	Helper()
	Errorf(format string, args ...any)
}

// sameSnapshot compares every field. Snapshot carries a Codes map, so it is not
// comparable with != and a struct equality would not compile; spelling the
// fields out also means a mismatch names the field rather than printing two
// structs and leaving the reader to diff them.
func sameSnapshot(t reporter, got, want Snapshot, ctx string) {
	t.Helper()
	type field struct {
		name      string
		got, want any
	}
	for _, f := range []field{
		{"Window", got.Window, want.Window},
		{"Count", got.Count, want.Count},
		{"Errors", got.Errors, want.Errors},
		{"RPS", got.RPS, want.RPS},
		{"ErrorRate", got.ErrorRate, want.ErrorRate},
		{"P50", got.P50, want.P50},
		{"P95", got.P95, want.P95},
		{"P99", got.P99, want.P99},
		{"Max", got.Max, want.Max},
		{"MaxScheduleLag", got.MaxScheduleLag, want.MaxScheduleLag},
		{"Clamped", got.Clamped, want.Clamped},
		{"CorrectedClamped", got.CorrectedClamped, want.CorrectedClamped},
		{"Saturated", got.Saturated, want.Saturated},
		{"Bytes", got.Bytes, want.Bytes},
		{"Throughput", got.Throughput, want.Throughput},
		{"CorrectedP50", got.CorrectedP50, want.CorrectedP50},
		{"CorrectedP95", got.CorrectedP95, want.CorrectedP95},
		{"CorrectedP99", got.CorrectedP99, want.CorrectedP99},
		{"CorrectedCount", got.CorrectedCount, want.CorrectedCount},
	} {
		if f.got != f.want {
			t.Errorf("%s: %s = %v, want %v (sparse must be invisible)", ctx, f.name, f.got, f.want)
		}
	}
	if !maps.Equal(got.Codes, want.Codes) {
		t.Errorf("%s: Codes = %v, want %v", ctx, got.Codes, want.Codes)
	}
}

func TestSparseAndDenseRingsAgree(t *testing.T) {
	rapid.Check(t, func(rt *rapid.T) {
		n := rapid.IntRange(1, 500).Draw(rt, "results")
		clk := NewManualClock(time.Now())

		cfg := Rolling{Clock: clk}
		sparse := NewRollingStats(cfg)
		dense := denseRolling(cfg)

		for range n {
			lat := time.Duration(rapid.Int64Range(1, 5_000_000_000).Draw(rt, "latency"))
			adv := time.Duration(rapid.Int64Range(0, 500_000_000).Draw(rt, "advance"))
			at := clk.Now()
			r := Result{Start: at, Scheduled: at, Latency: lat}

			sparse.Record(r)
			dense.Record(r)
			clk.Advance(adv)
		}

		sameSnapshot(rt, sparse.Window(), dense.Window(), "Window")
		sameSnapshot(rt, sparse.Snapshot(), dense.Snapshot(), "Snapshot")
	})
}

// TestSparseAndDenseRingsAgreeOnClamping drives latencies deliberately outside
// the range, in both directions, because Clamped is the field a consumer reads
// to know its percentiles understate reality. Getting it wrong on the sparse
// path would turn a failed measurement into a passing one.
func TestSparseAndDenseRingsAgreeOnClamping(t *testing.T) {
	clk := NewManualClock(time.Now())
	cfg := Rolling{Clock: clk, Lo: time.Millisecond, Hi: time.Second, Sigfigs: 3}
	sparse := NewRollingStats(cfg)
	dense := denseRolling(cfg)

	lats := []time.Duration{
		time.Microsecond, // under the floor
		10 * time.Millisecond,
		time.Hour, // over the ceiling
		time.Millisecond,
		time.Second,
		999 * time.Microsecond, // one microsecond under the floor
	}
	for i, lat := range lats {
		at := clk.Now()
		// Every third Result carries no Scheduled stamp, so the corrected
		// distribution sees fewer values than the raw one -- the case a single
		// paired record call cannot express.
		r := Result{Start: at, Latency: lat}
		if i%3 != 0 {
			r.Scheduled = at.Add(-time.Duration(i) * time.Millisecond)
		}
		sparse.Record(r)
		dense.Record(r)
		clk.Advance(100 * time.Millisecond)
	}

	sameSnapshot(t, sparse.Window(), dense.Window(), "Window")
	sameSnapshot(t, sparse.Snapshot(), dense.Snapshot(), "Snapshot")
}

func TestWindowIdenticalAcrossPromotion(t *testing.T) {
	clk := NewManualClock(time.Now())
	cfg := Rolling{Clock: clk}
	sparse := NewRollingStats(cfg)
	dense := denseRolling(cfg)

	// The store holds whole microseconds, so distinct values are counted in
	// microseconds and the spread has to be wide enough to cross promoteAt --
	// about 2,785 for this range. Latencies spanning a millisecond in
	// nanoseconds yield roughly a thousand distinct values and would never
	// promote, leaving this test silently passing on the sparse path alone.
	threshold := sparse.ring[sparse.idx].lat.promoteAt
	spread := threshold * 2

	promoted := false
	for i := range 200_000 {
		at := clk.Now()
		r := Result{Start: at, Scheduled: at, Latency: time.Duration(i%spread+1) * time.Microsecond}
		sparse.Record(r)
		dense.Record(r)

		if !promoted && !sparse.ring[sparse.idx].lat.isSparse() {
			promoted = true
			sameSnapshot(t, sparse.Window(), dense.Window(), "Window at the moment of promotion")
			if t.Failed() {
				t.FailNow()
			}
		}
	}
	if !promoted {
		t.Fatalf("no bucket promoted after 200k Results over %d distinct microsecond values, "+
			"with a threshold of %d; the promotion path is untested", spread, threshold)
	}
	sameSnapshot(t, sparse.Window(), dense.Window(), "Window after promotion")
}
