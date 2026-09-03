package metronome

import (
	"math"
	"sync/atomic"
	"time"
)

// RateController decides the target rate (requests/sec) over elapsed time.
type RateController interface {
	Rate(elapsed time.Duration) float64
}

// Constant holds a fixed rate.
type Constant float64

// Rate reports the fixed rate, ignoring elapsed time.
func (c Constant) Rate(time.Duration) float64 { return float64(c) }

// Ramp linearly interpolates from Start to End over the Over duration, then holds End.
type Ramp struct {
	Start, End float64
	Over       time.Duration
}

// Rate reports the interpolated rate at elapsed: Start at 0, End at Over and
// after. A non-positive Over reports End immediately.
func (r Ramp) Rate(elapsed time.Duration) float64 {
	if elapsed >= r.Over || r.Over <= 0 {
		return r.End
	}
	frac := float64(elapsed) / float64(r.Over)
	return r.Start + (r.End-r.Start)*frac
}

// Phase is one flat-rate segment of a Phased controller.
type Phase struct {
	Duration  time.Duration
	TargetRPS float64
}

// Phased steps through phases by elapsed time, holding the last phase's rate past the end.
type Phased struct {
	Phases []Phase
}

// Rate reports the TargetRPS of the phase covering elapsed, holding the last
// phase's rate past the end. It reports 0 when there are no phases.
func (p Phased) Rate(elapsed time.Duration) float64 {
	var acc time.Duration
	for _, ph := range p.Phases {
		acc += ph.Duration
		if elapsed < acc {
			return ph.TargetRPS
		}
	}
	if len(p.Phases) == 0 {
		return 0
	}
	return p.Phases[len(p.Phases)-1].TargetRPS
}

// PhaseEnd reports the elapsed time at which phase i ends: the sum of the
// durations of phases 0 through i. It panics if i is out of range.
//
// Pair it with After to keep one source of truth for a warmup boundary:
//
//	measured := metronome.After(rate.PhaseEnd(0), stats)
//
// rather than repeating the first phase's duration, where the two can drift
// and only one of them fails loudly.
func (p Phased) PhaseEnd(i int) time.Duration {
	if i < 0 || i >= len(p.Phases) {
		panic("metronome: Phased.PhaseEnd index out of range")
	}
	var acc time.Duration
	for _, ph := range p.Phases[:i+1] {
		acc += ph.Duration
	}
	return acc
}

// Duration reports the elapsed time at which the last phase ends, or zero when
// there are no phases.
//
// Rate holds the last phase's rate past this point, so this is a measurement
// boundary rather than a stop condition — a Driver runs until its context ends
// or MaxRequests is reached, whatever the controller says.
func (p Phased) Duration() time.Duration {
	var acc time.Duration
	for _, ph := range p.Phases {
		acc += ph.Duration
	}
	return acc
}

// Adaptive is a RateController whose rate is set externally.
// Safe for concurrent SetRate/Rate.
type Adaptive struct {
	rps atomic.Uint64 // stores math.Float64bits
}

// NewAdaptive returns an Adaptive controller starting at initial rps.
func NewAdaptive(initial float64) *Adaptive {
	a := &Adaptive{}
	a.SetRate(initial)
	return a
}

// SetRate sets the rate reported from the next Rate call onward. It is safe to
// call concurrently with Rate and from any goroutine.
//
// A rate of zero or less is floored by the Driver to a token every ~10,000
// seconds: effectively paused, and resumable — a later SetRate takes effect
// within one rate-update interval (see maxReservationWait), so pausing is not a
// one-way door. NaN is floored the same way rather than treated as "unlimited",
// because a control loop dividing by an empty PromQL vector produces NaN and
// flooding the target is the wrong direction to fail; see sanitizeRate.
func (a *Adaptive) SetRate(rps float64) {
	a.rps.Store(math.Float64bits(rps))
}

// Rate reports the rate most recently passed to SetRate, ignoring elapsed time.
func (a *Adaptive) Rate(time.Duration) float64 {
	return math.Float64frombits(a.rps.Load())
}
