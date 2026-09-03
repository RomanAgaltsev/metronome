package metronome

import (
	"testing"
	"time"
)

func TestConstantRate(t *testing.T) {
	c := Constant(100)
	if c.Rate(0) != 100 || c.Rate(time.Hour) != 100 {
		t.Fatal("Constant must ignore elapsed")
	}
}

func TestRampRate(t *testing.T) {
	r := Ramp{Start: 0, End: 100, Over: 10 * time.Second}
	cases := []struct {
		elapsed time.Duration
		want    float64
	}{
		{0, 0},
		{5 * time.Second, 50},
		{10 * time.Second, 100},
		{20 * time.Second, 100},
	}
	for _, tc := range cases {
		if got := r.Rate(tc.elapsed); got != tc.want {
			t.Fatalf("Ramp.Rate(%v)=%v want %v", tc.elapsed, got, tc.want)
		}
	}
}

func TestPhasedRate(t *testing.T) {
	p := Phased{Phases: []Phase{
		{Duration: 2 * time.Second, TargetRPS: 10},
		{Duration: 3 * time.Second, TargetRPS: 50},
	}}
	if p.Rate(1*time.Second) != 10 {
		t.Fatal("t=1s should be in phase 1 (10 rps)")
	}
	if p.Rate(3*time.Second) != 50 {
		t.Fatal("t=3s should be in phase 2 (50 rps)")
	}
	if p.Rate(10*time.Second) != 50 {
		t.Fatal("past the end should hold the last phase rate")
	}
}

func TestAdaptiveSetRate(t *testing.T) {
	a := NewAdaptive(10)
	if a.Rate(0) != 10 {
		t.Fatal("initial rate should be 10")
	}
	a.SetRate(42.5)
	if a.Rate(time.Second) != 42.5 {
		t.Fatal("SetRate should update the reported rate")
	}
}

func TestAdaptiveConcurrent(t *testing.T) {
	a := NewAdaptive(1)
	done := make(chan struct{})
	go func() {
		for i := range 1000 {
			a.SetRate(float64(i))
		}
		close(done)
	}()
	for range 1000 {
		_ = a.Rate(0)
	}
	<-done
}

func TestPhasedPhaseEndAccumulatesDurations(t *testing.T) {
	p := Phased{Phases: []Phase{
		{Duration: 10 * time.Second, TargetRPS: 50},
		{Duration: 30 * time.Second, TargetRPS: 200},
		{Duration: 20 * time.Second, TargetRPS: 100},
	}}

	for i, want := range []time.Duration{10 * time.Second, 40 * time.Second, 60 * time.Second} {
		if got := p.PhaseEnd(i); got != want {
			t.Fatalf("PhaseEnd(%d)=%v want %v", i, got, want)
		}
	}
}

func TestPhasedDurationIsTheLastPhaseEnd(t *testing.T) {
	p := Phased{Phases: []Phase{
		{Duration: 10 * time.Second, TargetRPS: 50},
		{Duration: 30 * time.Second, TargetRPS: 200},
	}}

	if got, want := p.Duration(), 40*time.Second; got != want {
		t.Fatalf("Duration()=%v want %v", got, want)
	}
	if got, want := p.Duration(), p.PhaseEnd(len(p.Phases)-1); got != want {
		t.Fatalf("Duration()=%v want PhaseEnd(last)=%v", got, want)
	}
}

func TestPhasedDurationOfNoPhasesIsZero(t *testing.T) {
	if got := (Phased{}).Duration(); got != 0 {
		t.Fatalf("Duration()=%v want 0", got)
	}
}

func TestPhasedPhaseEndPanicsOutOfRange(t *testing.T) {
	p := Phased{Phases: []Phase{{Duration: time.Second, TargetRPS: 1}}}
	for name, i := range map[string]int{"negative": -1, "past the end": 1, "far past": 99} {
		t.Run(name, func(t *testing.T) {
			defer func() {
				if recover() == nil {
					t.Fatalf("PhaseEnd(%d) did not panic", i)
				}
			}()
			p.PhaseEnd(i)
		})
	}
}

func TestPhasedPhaseEndPanicsOnNoPhases(t *testing.T) {
	defer func() {
		if recover() == nil {
			t.Fatal("PhaseEnd(0) on an empty Phased did not panic")
		}
	}()
	(Phased{}).PhaseEnd(0)
}
