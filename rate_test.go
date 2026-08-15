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
