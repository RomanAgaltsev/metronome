package metronome

import (
	"context"
	"fmt"
	"math"
	"runtime"
	"testing"
	"time"
)

// BenchmarkDriverOverhead measures the kernel's ceiling: a no-op Runner at an
// unlimited rate, so the number is Driver plumbing (reservation, channel send,
// goroutine handoff) and nothing else. ns/op is the per-request cost; its
// reciprocal is the maximum rate this machine can drive.
//
// Run this at the DEFAULT -benchtime. Each iteration of b.N shares one Driver
// start-up (worker pool, updater, closer), so at a small forced -benchtime the
// number is start-up cost amortised over a handful of requests: 3x reports
// ~46,000 ns/op where the real per-request cost is ~425 ns/op, a 100x error.
// Only BenchmarkDriverPaced below wants a small -benchtime, because each of its
// iterations is a full one-second load run.
func BenchmarkDriverOverhead(b *testing.B) {
	for _, mode := range []struct {
		name string
		p    PacingMode
	}{{"closed", ClosedLoop}, {"open", OpenLoop}} {
		b.Run(mode.name, func(b *testing.B) {
			d := Driver{
				Runner:       RunnerFunc(func(context.Context) Result { return Result{} }),
				Rate:         Constant(math.Inf(1)), // sanitizeRate maps +Inf to rate.Inf
				Workers:      runtime.GOMAXPROCS(0),
				MaxRequests:  b.N,
				Pacing:       mode.p,
				ResultBuffer: 8192,
			}
			b.ResetTimer()
			for range d.Run(context.Background()) { // draining is the work
			}
		})
	}
}

// BenchmarkDriverPaced reports how closely the achieved rate tracks the offered
// rate. The "adherence" metric is achieved/offered: 1.00 is perfect, below 1.00
// means the generator could not keep its own schedule.
//
// Every iteration is a full one-second load run, so use a small explicit count:
//
//	go test -run '^$' -bench BenchmarkDriverPaced -benchtime 3x ./...
//
// The reported adherence is the mean over the iterations, not the last one — a
// single sample hides the run-to-run variance that matters most at the rates
// where the generator starts to struggle.
func BenchmarkDriverPaced(b *testing.B) {
	for _, mode := range []struct {
		name string
		p    PacingMode
	}{{"closed", ClosedLoop}, {"open", OpenLoop}} {
		for _, rps := range []float64{10, 100, 1000, 5000} {
			b.Run(fmt.Sprintf("%s/%.0frps", mode.name, rps), func(b *testing.B) {
				n := int(rps) // one second of load per iteration
				var total float64
				var runs int
				for b.Loop() {
					d := Driver{
						Runner:       RunnerFunc(func(context.Context) Result { return Result{} }),
						Rate:         Constant(rps),
						Workers:      runtime.GOMAXPROCS(0),
						MaxRequests:  n,
						Pacing:       mode.p,
						ResultBuffer: 8192,
					}
					s := NewStats()
					for r := range d.Run(context.Background()) {
						s.Record(r)
					}
					total += s.Snapshot().RPS / rps
					runs++
				}
				if runs > 0 {
					b.ReportMetric(total/float64(runs), "adherence")
				}
			})
		}
	}
}

// BenchmarkStatsRecord measures the aggregator under the contention a real run
// produces: every worker records into one mutex-guarded pair of histograms.
func BenchmarkStatsRecord(b *testing.B) {
	s := NewStats()
	base := time.Unix(0, 0)
	b.RunParallel(func(pb *testing.PB) {
		i := 0
		for pb.Next() {
			i++
			s.Record(Result{
				Scheduled: base,
				Start:     base.Add(time.Duration(i) * time.Microsecond),
				Latency:   time.Duration(i%1000) * time.Microsecond,
				Code:      "200",
				Bytes:     512,
			})
		}
	})
}

// BenchmarkRollingStatsRecord measures what a caller pays for the trailing
// window: one extra mutex around the pair Stats already takes, plus a rotation
// check on every Record. Compare it against BenchmarkStatsRecord.
func BenchmarkRollingStatsRecord(b *testing.B) {
	rs := NewRollingStats(Rolling{})
	base := time.Unix(0, 0)
	b.RunParallel(func(pb *testing.PB) {
		i := 0
		for pb.Next() {
			i++
			rs.Record(Result{
				Scheduled: base,
				Start:     base.Add(time.Duration(i) * time.Microsecond),
				Latency:   time.Duration(i%1000) * time.Microsecond,
				Code:      "200",
				Bytes:     512,
			})
		}
	})
}

// BenchmarkRollingStatsWindow measures the merge a control loop pays per poll.
// The cost is proportional to the live bucket count and to the histogram's
// counts array, not to the number of Results in them — so this fills the whole
// default ring, which is the steady-state worst case.
//
// It runs on a ManualClock rather than the wall clock deliberately: with real
// time and one-second buckets, the live count would climb from 1 to 10 partway
// through the run and then the ring would drain, so the reported figure would
// depend on -benchtime rather than on the code.
func BenchmarkRollingStatsWindow(b *testing.B) {
	clk := NewManualClock(time.Unix(0, 0))
	rs := NewRollingStats(Rolling{Clock: clk})
	base := time.Unix(0, 0)

	// 10,000 Results into each of the ten buckets, so every one is live.
	for bucket := range DefaultBuckets {
		for i := range 10000 {
			rs.Record(Result{
				Scheduled: base,
				Start:     base.Add(time.Duration(i) * time.Microsecond),
				Latency:   time.Duration(i%1000) * time.Microsecond,
				Code:      "200",
				Bytes:     512,
			})
		}
		if bucket < DefaultBuckets-1 {
			clk.Advance(DefaultWindow / DefaultBuckets)
		}
	}
	if rs.live != DefaultBuckets {
		b.Fatalf("live=%d want %d; the benchmark is not measuring a full ring", rs.live, DefaultBuckets)
	}

	for b.Loop() {
		_ = rs.Window()
	}
}

// BenchmarkRollingStatsWindowStalled measures the same poll during a stall: the
// ring is still fully live, but every bucket has been rotated through and reset
// without being written to again. That is the state a control loop polls in when
// the target has stopped answering, which is the case this type exists for, and
// it is the one Stats.merge's empty-source check is there to make cheap.
func BenchmarkRollingStatsWindowStalled(b *testing.B) {
	clk := NewManualClock(time.Unix(0, 0))
	rs := NewRollingStats(Rolling{Clock: clk})
	base := time.Unix(0, 0)

	for i := range 10000 {
		rs.Record(Result{
			Scheduled: base,
			Start:     base.Add(time.Duration(i) * time.Microsecond),
			Latency:   time.Duration(i%1000) * time.Microsecond,
			Code:      "200",
			Bytes:     512,
		})
	}
	// One bucket at a time, so live climbs to the whole ring instead of
	// collapsing to 1 the way a single gap wider than the window would. Ten
	// rotations also retires the bucket holding the traffic.
	for range DefaultBuckets {
		clk.Advance(DefaultWindow / DefaultBuckets)
		_ = rs.Window()
	}
	if snap := rs.Window(); rs.live != DefaultBuckets || snap.Count != 0 {
		b.Fatalf("live=%d count=%d; want a fully live, wholly empty ring", rs.live, snap.Count)
	}

	for b.Loop() {
		_ = rs.Window()
	}
}
