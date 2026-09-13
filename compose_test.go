package metronome

import (
	"math"
	"strings"
	"testing"
	"time"
)

func TestSumAddsEveryController(t *testing.T) {
	c := Sum(Constant(100), Constant(25), Constant(0.5))
	for _, at := range []time.Duration{0, time.Second, time.Hour} {
		if got := c.Rate(at); math.Abs(got-125.5) > 1e-9 {
			t.Errorf("Rate(%v) = %v, want 125.5", at, got)
		}
	}
}

func TestSumSingleControllerIsPassThrough(t *testing.T) {
	inner := Ramp{Start: 10, End: 20, Over: 10 * time.Second}
	c := Sum(inner)
	for _, at := range []time.Duration{0, 5 * time.Second, 20 * time.Second} {
		if got, want := c.Rate(at), inner.Rate(at); math.Abs(got-want) > 1e-9 {
			t.Errorf("Rate(%v) = %v, want %v", at, got, want)
		}
	}
}

func TestSumEmptyPanics(t *testing.T) {
	defer func() {
		r := recover()
		if r == nil {
			t.Fatal("Sum() did not panic")
		}
		msg, ok := r.(string)
		if !ok || !strings.Contains(msg, "metronome: Sum requires at least one") {
			t.Errorf("panic = %v, want a metronome-prefixed empty-argument message", r)
		}
	}()
	Sum()
}

func TestSumNilPanicsNamingTheIndex(t *testing.T) {
	defer func() {
		r := recover()
		if r == nil {
			t.Fatal("Sum with a nil controller did not panic")
		}
		msg, ok := r.(string)
		if !ok || !strings.Contains(msg, "metronome: Sum RateController 1 is nil") {
			t.Errorf("panic = %v, want the message to name index 1", r)
		}
	}()
	Sum(Constant(1), nil, Constant(2))
}

func TestSumCopiesItsArguments(t *testing.T) {
	args := []RateController{Constant(10), Constant(20)}
	c := Sum(args...)
	args[0] = Constant(9999)
	if got := c.Rate(0); math.Abs(got-30) > 1e-9 {
		t.Errorf("Rate(0) = %v, want 30 — Sum did not copy the caller's slice", got)
	}
}

func TestScaleMultiplies(t *testing.T) {
	c := Scale(0.5, Constant(200))
	if got := c.Rate(time.Second); math.Abs(got-100) > 1e-9 {
		t.Errorf("Rate = %v, want 100", got)
	}
}

func TestScaleZeroAndNegative(t *testing.T) {
	if got := Scale(0, Constant(200)).Rate(0); got != 0 {
		t.Errorf("Scale(0) = %v, want 0", got)
	}
	if got := Scale(-1, Constant(200)).Rate(0); math.Abs(got-(-200)) > 1e-9 {
		t.Errorf("Scale(-1) = %v, want -200 (the Driver floors it, Scale does not)", got)
	}
}

func TestScaleNilPanics(t *testing.T) {
	defer func() {
		r := recover()
		if r == nil {
			t.Fatal("Scale with a nil controller did not panic")
		}
		msg, ok := r.(string)
		if !ok || !strings.Contains(msg, "metronome: Scale c must not be nil") {
			t.Errorf("panic = %v, want the named nil message", r)
		}
	}()
	Scale(2, nil)
}

func TestRepeatCyclesTheInnerController(t *testing.T) {
	inner := Ramp{Start: 0, End: 100, Over: 10 * time.Second}
	c := Repeat(inner, 10*time.Second)
	tests := []struct {
		name    string
		elapsed time.Duration
		want    float64
	}{
		{"first cycle start", 0, 0},
		{"first cycle middle", 5 * time.Second, 50},
		{"second cycle start", 10 * time.Second, 0},
		{"second cycle middle", 15 * time.Second, 50},
		{"fifth cycle middle", 45 * time.Second, 50},
	}
	for _, tt := range tests {
		t.Run(tt.name, func(t *testing.T) {
			if got := c.Rate(tt.elapsed); math.Abs(got-tt.want) > 1e-9 {
				t.Errorf("Rate(%v) = %v, want %v", tt.elapsed, got, tt.want)
			}
		})
	}
}

func TestRepeatWithPhasedOwnDuration(t *testing.T) {
	p := Phased{Phases: []Phase{
		{Duration: 2 * time.Second, TargetRPS: 10},
		{Duration: 3 * time.Second, TargetRPS: 40},
	}}
	c := Repeat(p, p.Duration())

	// One table length is 5s. The pattern must reproduce on later passes.
	for cycle := range 3 {
		base := time.Duration(cycle) * p.Duration()
		if got := c.Rate(base); math.Abs(got-10) > 1e-9 {
			t.Errorf("cycle %d start: %v, want 10", cycle, got)
		}
		if got := c.Rate(base + 2*time.Second); math.Abs(got-40) > 1e-9 {
			t.Errorf("cycle %d at 2s: %v, want 40", cycle, got)
		}
		if got := c.Rate(base + 4999*time.Millisecond); math.Abs(got-40) > 1e-9 {
			t.Errorf("cycle %d at 4.999s: %v, want 40", cycle, got)
		}
	}
}

func TestRepeatNonPositivePeriodDelegates(t *testing.T) {
	inner := Ramp{Start: 0, End: 100, Over: 10 * time.Second}
	for _, p := range []time.Duration{0, -time.Second} {
		c := Repeat(inner, p)
		for _, at := range []time.Duration{0, 5 * time.Second, 30 * time.Second} {
			if got, want := c.Rate(at), inner.Rate(at); math.Abs(got-want) > 1e-9 {
				t.Errorf("period=%v Rate(%v) = %v, want %v (delegated)", p, at, got, want)
			}
		}
	}
}

func TestRepeatNilPanics(t *testing.T) {
	defer func() {
		r := recover()
		if r == nil {
			t.Fatal("Repeat with a nil controller did not panic")
		}
		msg, ok := r.(string)
		if !ok || !strings.Contains(msg, "metronome: Repeat c must not be nil") {
			t.Errorf("panic = %v, want the named nil message", r)
		}
	}()
	Repeat(nil, time.Second)
}
