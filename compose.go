package metronome

import (
	"slices"
	"strconv"
	"time"
)

// Sum returns a RateController reporting the sum of every controller's rate at
// a given elapsed. It panics if cs is empty or if any controller is nil.
//
// This is now a spike on a baseline is expressed, which is why the package
// ships no Burst type:
//
// metronome.Sum(metronome.Constant(100),
//
//	metronome.Repeat(spike, 10*time.Minute))
//
// It also puts a floor under an Adaptive controller, so a control signal that
// collapses cannot drive the run down to nothing.
//
// Nothing clamps the total. A sum large enough to exceed what the target or
// the generator can serve shows up as ErrSaturated and shortfall in the
// Snapshot, the same as any other over-ambitious rate.
func Sum(cs ...RateController) RateController {
	if len(cs) == 0 {
		panic("metronome: Sum requires at least one RateController")
	}
	// Nil is caught here rather than at the first Rate call, so it reports
	// itself while the caller is still looking at the argument list. A Rate
	// call happens on the Driver's rate-updater goroutine, mid-run, after load
	// is already being generated.
	for i, c := range cs {
		if c == nil {
			panic("metronome: Sum RateController " + strconv.Itoa(i) + " is nil")
		}
	}
	// The caller may retain and mutate the slice they passed — a variadic call
	// site like Sum(cs...) hands us their slice, not a copy.
	return &sum{cs: slices.Clone(cs)}
}

type sum struct{ cs []RateController }

// Rate -
func (s *sum) Rate(elapsed time.Duration) float64 {
	var total float64
	for _, c := range s.cs {
		total += c.Rate(elapsed)
	}
	return total
}

// Scale returns a RateController reporting factor times c's rate. It panics if
// c is nil.
//
// It exists for reuse rather than arithmetic: a Sine can be rescaled by hand
// through its Min and Max, but a Phased table cannot without rewriting every
// phase. Scale runs an agreed profile at a fraction of its intensity — a
// canary, or a reduced-blast-radius rerun of the same shape.
//
// A negative factor is not rejected here; the Driver floors the result, as it
// does for any controller.
func Scale(factor float64, c RateController) RateController {
	if c == nil {
		panic("metronome: Scale c must not be nil")
	}
	return &scaled{factor: factor, c: c}
}

type scaled struct {
	factor float64
	c      RateController
}

// Rate -
func (s *scaled) Rate(elapsed time.Duration) float64 { return s.factor * s.c.Rate(elapsed) }
