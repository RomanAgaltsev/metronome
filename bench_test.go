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
func BenchmarkDriverPaced(b *testing.B) {
	for _, mode := range []struct {
		name string
		p    PacingMode
	}{{"closed", ClosedLoop}, {"open", OpenLoop}} {
		for _, rps := range []float64{10, 100, 1000, 5000} {
			b.Run(fmt.Sprintf("%s/%.0frps", mode.name, rps), func(b *testing.B) {
				n := int(rps) // one second of load per iteration
				var adherence float64
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
					adherence = s.Snapshot().RPS / rps
				}
				b.ReportMetric(adherence, "adherence")
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
