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

func (c Constant) Rate(time.Duration) float64 { return float64(c) }

// Ramp linearly interpolates from Start to End over the Over dureation, then holds End.
type Ramp struct {
	Start, End float64
	Over       time.Duration
}

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

// Adaptive is a RateController whose rate is set externally.
// Safe for concurrent SetRate/Rate.
type Adaptive struct {
	rps atomic.Uint64 // stores math.Float64bits
}

func NewAdaptive(initial float64) *Adaptive {
	a := &Adaptive{}
	a.SetRate(initial)
	return a
}

func (a *Adaptive) SetRate(rps float64) {
	a.rps.Store(math.Float64bits(rps))
}

func (a *Adaptive) Rate(time.Duration) float64 {
	return math.Float64frombits(a.rps.Load())
}
