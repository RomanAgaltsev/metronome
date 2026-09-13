package metronome_test

import (
	"fmt"
	"time"

	"github.com/RomanAgaltsev/metronome"
)

// A Sine gives a smooth periodic load rather than the steps a phase table
// produces. It starts at Min, peaks at Period/2, and returns to Min.
func ExampleSine() {
	s := metronome.Sine{Min: 100, Max: 300, Period: 10 * time.Second}

	for _, at := range []time.Duration{0, 2500 * time.Millisecond, 5 * time.Second} {
		fmt.Printf("%v: %.0f rps\n", at, s.Rate(at))
	}
	// Output:
	// 0s: 100 rps
	// 2.5s: 200 rps
	// 5s: 300 rps
}

// Repeat makes a finite shape endless. Pairing a phase table with its own
// Duration keeps the cycle length in one place.
func ExampleRepeat() {
	p := metronome.Phased{Phases: []metronome.Phase{
		{Duration: 2 * time.Second, TargetRPS: 10},
		{Duration: 3 * time.Second, TargetRPS: 40},
	}}
	c := metronome.Repeat(p, p.Duration())

	for _, at := range []time.Duration{0, 2 * time.Second, 5 * time.Second, 7 * time.Second} {
		fmt.Printf("%v: %.0f rps\n", at, c.Rate(at))
	}
	// Output:
	// 0s: 10 rps
	// 2s: 40 rps
	// 5s: 10 rps
	// 7s: 40 rps
}

// A spike on a baseline needs no Burst type: it is a constant plus a repeating
// phase table. Sum adds the two, so the composed shape is 100 rps rising to
// 500 for one minute in every ten.
func ExampleSum() {
	c := metronome.Sum(metronome.Constant(100),
		metronome.Repeat(metronome.Phased{Phases: []metronome.Phase{
			{Duration: 9 * time.Minute, TargetRPS: 0},
			{Duration: 1 * time.Minute, TargetRPS: 400},
		}}, 10*time.Minute))

	for _, at := range []time.Duration{0, 9 * time.Minute, 10 * time.Minute} {
		fmt.Printf("%v: %.0f rps\n", at, c.Rate(at))
	}
	// Output:
	// 0s: 100 rps
	// 9m0s: 500 rps
	// 10m0s: 100 rps
}
