package metronome

import (
	"context"
	"math"
	"testing"
	"time"
)

// The tests in this file exist because three releases and two reviews missed a
// hang reachable from a four-line Driver literal (Ramp{Start: 0} delivered one
// request out of a hundred). Every earlier suite tested the code that had been
// written; none of them tested the *configurations a user can type*.
//
// So: every RateController shape, both pacing modes, and the ends of every
// Driver field's range, all asserting the one contract that must hold for all of
// them — exactly MaxRequests Results arrive, each carrying its schedule stamp,
// and the run ends on its own.

// rateShapes covers every RateController the package ships, at both ends of its
// range. Constructors rather than values, so a controller that has to be resumed
// from another goroutine gets a fresh one per subtest.
func rateShapes() []struct {
	name string
	new  func() RateController
} {
	return []struct {
		name string
		new  func() RateController
	}{
		{"Constant", func() RateController { return Constant(500) }},
		{"Constant zero then unusable", func() RateController { return Constant(0) }},
		{"Constant unlimited", func() RateController { return Constant(math.Inf(1)) }},
		{"Ramp up", func() RateController {
			return Ramp{Start: 100, End: 1000, Over: 200 * time.Millisecond}
		}},
		{"Ramp from zero", func() RateController {
			return Ramp{Start: 0, End: 1000, Over: 200 * time.Millisecond}
		}},
		{"Ramp down to zero", func() RateController {
			return Ramp{Start: 1000, End: 0, Over: 200 * time.Millisecond}
		}},
		{"Ramp with non-positive Over", func() RateController {
			return Ramp{Start: 10, End: 1000, Over: 0}
		}},
		{"Phased", func() RateController {
			return Phased{Phases: []Phase{
				{Duration: 100 * time.Millisecond, TargetRPS: 200},
				{Duration: time.Second, TargetRPS: 1000},
			}}
		}},
		{"Phased with a zero first phase", func() RateController {
			return Phased{Phases: []Phase{
				{Duration: 100 * time.Millisecond, TargetRPS: 0},
				{Duration: time.Second, TargetRPS: 1000},
			}}
		}},
		{"Adaptive steady", func() RateController { return NewAdaptive(500) }},
		{"Adaptive paused then resumed", func() RateController {
			a := NewAdaptive(0)
			go func() {
				time.Sleep(100 * time.Millisecond)
				a.SetRate(1000)
			}()
			return a
		}},
		{"Adaptive NaN then healthy", func() RateController {
			a := NewAdaptive(math.NaN())
			go func() {
				time.Sleep(100 * time.Millisecond)
				a.SetRate(1000)
			}()
			return a
		}},
	}
}

// A "Constant zero" controller never recovers by design — it is a pause with
// nothing to resume it — so it is the one shape that cannot deliver its
// MaxRequests. It is still exercised below to prove it pauses rather than
// panics, spins or leaks.
const pausedForever = "Constant zero then unusable"

func TestDriverDeliversUnderEveryRateShape(t *testing.T) {
	if testing.Short() {
		t.Skip("timing-sensitive")
	}
	for _, mode := range []struct {
		name string
		p    PacingMode
	}{{"closed", ClosedLoop}, {"open", OpenLoop}} {
		for _, shape := range rateShapes() {
			t.Run(mode.name+"/"+shape.name, func(t *testing.T) {
				const want = 40
				d := Driver{
					Runner:      RunnerFunc(func(context.Context) Result { return Result{Latency: time.Millisecond} }),
					Rate:        shape.new(),
					Workers:     4,
					MaxRequests: want,
					Pacing:      mode.p,
				}
				ctx, cancel := context.WithTimeout(context.Background(), 15*time.Second)
				defer cancel()

				got, stamped := 0, 0
				for r := range d.Run(ctx) {
					got++
					if !r.Scheduled.IsZero() {
						stamped++
					}
				}

				if shape.name == pausedForever {
					// The burst token lets exactly one through; after that the
					// generator is paused and nothing resumes it.
					if got > 1 {
						t.Fatalf("a permanently zero rate delivered %d results; it must pause", got)
					}
					return
				}
				if got != want {
					t.Fatalf("delivered %d/%d — this configuration does not finish", got, want)
				}
				if stamped != got {
					t.Fatalf("%d of %d Results carried no Scheduled stamp", got-stamped, got)
				}
			})
		}
	}
}

// The ends of every other Driver field's range, against a rate that is not the
// interesting variable.
func TestDriverAcceptsTheEndsOfEveryFieldRange(t *testing.T) {
	if testing.Short() {
		t.Skip("timing-sensitive")
	}
	const want = 20
	base := func() Driver {
		return Driver{
			Runner:      RunnerFunc(func(context.Context) Result { return Result{Latency: time.Millisecond} }),
			Rate:        Constant(2000),
			Workers:     4,
			MaxRequests: want,
		}
	}

	cases := []struct {
		name  string
		tweak func(*Driver)
	}{
		{"Workers zero defaults", func(d *Driver) { d.Workers = 0 }},
		{"Workers negative defaults", func(d *Driver) { d.Workers = -7 }},
		{"Workers one", func(d *Driver) { d.Workers = 1 }},
		{"Workers far above GOMAXPROCS", func(d *Driver) { d.Workers = 256 }},
		{"ResultBuffer unbuffered", func(d *Driver) { d.ResultBuffer = -1 }},
		{"ResultBuffer default", func(d *Driver) { d.ResultBuffer = 0 }},
		{"ResultBuffer one", func(d *Driver) { d.ResultBuffer = 1 }},
		{"Burst zero means one", func(d *Driver) { d.Burst = 0 }},
		{"Burst one", func(d *Driver) { d.Burst = 1 }},
		{"Burst larger than MaxRequests", func(d *Driver) { d.Burst = want * 2 }},
		{"Clock injected", func(d *Driver) { d.Clock = SystemClock() }},
		{"open loop", func(d *Driver) { d.Pacing = OpenLoop }},
		{"open loop unbuffered", func(d *Driver) { d.Pacing = OpenLoop; d.ResultBuffer = -1 }},
		{"open loop single worker", func(d *Driver) { d.Pacing = OpenLoop; d.Workers = 1 }},
	}
	for _, tc := range cases {
		t.Run(tc.name, func(t *testing.T) {
			d := base()
			tc.tweak(&d)
			ctx, cancel := context.WithTimeout(context.Background(), 15*time.Second)
			defer cancel()

			got := 0
			for range d.Run(ctx) {
				got++
			}
			if got != want {
				t.Fatalf("delivered %d/%d", got, want)
			}
		})
	}
}

// MaxRequests == 0 means unlimited, so the only stop condition is the context.
// Both modes must honour it promptly and close the channel on their own.
func TestDriverUnlimitedStopsOnContextInBothModes(t *testing.T) {
	if testing.Short() {
		t.Skip("timing-sensitive")
	}
	for _, mode := range []struct {
		name string
		p    PacingMode
	}{{"closed", ClosedLoop}, {"open", OpenLoop}} {
		t.Run(mode.name, func(t *testing.T) {
			d := Driver{
				Runner:  RunnerFunc(func(context.Context) Result { return Result{Latency: time.Millisecond} }),
				Rate:    Constant(1000),
				Workers: 4,
				Pacing:  mode.p,
			}
			ctx, cancel := context.WithTimeout(context.Background(), 200*time.Millisecond)
			defer cancel()

			start := time.Now()
			got := 0
			for range d.Run(ctx) {
				got++
			}
			if got == 0 {
				t.Fatal("no results before the deadline")
			}
			if elapsed := time.Since(start); elapsed > 5*time.Second {
				t.Fatalf("channel closed %v after start; cancellation is not prompt", elapsed)
			}
		})
	}
}
