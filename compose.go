package metronome

import (
	"slices"
	"strconv"
	"time"
)

// Sum returns a RateController reporting the sum of every controller's rate at
// a given elapsed. It panics if cs is empty or if any controller is nil.
//
// This is how a spike on a baseline is expressed, which is why the package
// ships no Burst type:
//
//	metronome.Sum(metronome.Constant(100),
//		metronome.Repeat(spike, 10*time.Minute))
//
// It also puts a floor under an Adaptive controller, so a control signal that
// collapses cannot drive the run down to nothing.
//
// Nothing clamps the total, in either direction. A sum large enough to exceed
// what the target or the generator can serve shows up as ErrSaturated and
// shortfall in the Snapshot, the same as any other over-ambitious rate; a
// total that a negative member drags to zero or below is floored by the
// Driver, as any controller's result is.
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

// Repeat returns a RateController that cycles c with the given period,
// reporting c.Rate(elapsed mod period). It panics if c is nil.
//
// This is what makes a finite shape endless, and cyclic is what diurnal
// traffic, periodic spikes and sawtooth all are. Pair it with a phase table's
// own length so the two cannot drift:
//
//	metronome.Repeat(p, p.Duration())
//
// Sine is already periodic and is not wrapped in Repeat.
//
// A non-positive period delegates straight to c and does not cycle: there is
// no cycle length to place elapsed on, and unlike a nil controller a zero
// period plausibly means "do not repeat". Ramp sets the same precedent, where
// a non-positive Over reports End immediately.
//
// The Driver samples the controller ten times a second, so a period near 200ms
// aliases into a shape unrelated to the one asked for, and anything shorter
// does not survive sampling at all. Keep the period, and any feature inside
// it, at a second or more.
func Repeat(c RateController, period time.Duration) RateController {
	if c == nil {
		panic("metronome: Repeat c must not be nil")
	}
	return &repeat{c: c, period: period}
}

type repeat struct {
	c      RateController
	period time.Duration
}

// Rate -
func (r *repeat) Rate(elapsed time.Duration) float64 {
	if r.period <= 0 {
		return r.c.Rate(elapsed)
	}
	return r.c.Rate(elapsed % r.period)
}
