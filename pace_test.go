package metronome

import (
	"context"
	"math"
	"testing"
	"time"

	"golang.org/x/time/rate"
)

func TestSanitizeRate(t *testing.T) {
	cases := []struct {
		name string
		in   float64
		want rate.Limit
	}{
		{"normal", 250, rate.Limit(250)},
		{"tiny but valid", 0.5, rate.Limit(0.5)},
		{"zero floors", 0, rate.Limit(minRPS)},
		{"negative floors", -17, rate.Limit(minRPS)},
		{"below floor floors", minRPS / 2, rate.Limit(minRPS)},
		{"NaN floors, does NOT become unlimited", math.NaN(), rate.Limit(minRPS)},
		{"+Inf is honoured as unlimited", math.Inf(1), rate.Inf},
		{"-Inf floors", math.Inf(-1), rate.Limit(minRPS)},
	}
	for _, tc := range cases {
		t.Run(tc.name, func(t *testing.T) {
			if got := sanitizeRate(tc.in); got != tc.want {
				t.Fatalf("sanitizeRate(%v)=%v want %v", tc.in, got, tc.want)
			}
		})
	}
}

// A NaN rate used to make every lim.Wait return instantly, so the Driver
// hammered the target as fast as the CPU allowed. It must pace (i.e. stall)
// instead.
func TestDriverNaNRateDoesNotFlood(t *testing.T) {
	d := Driver{
		Runner:      RunnerFunc(func(context.Context) Result { return Result{Start: time.Now()} }),
		Rate:        NewAdaptive(math.NaN()),
		Workers:     2,
		MaxRequests: 5,
	}
	ctx, cancel := context.WithTimeout(context.Background(), 300*time.Millisecond)
	defer cancel()

	got := 0
	for range d.Run(ctx) {
		got++
	}
	// At the floor rate, burst 1 permits exactly one immediate token; the rest
	// are ~10,000s away, so the context expires first.
	if got > 1 {
		t.Fatalf("NaN rate delivered %d results in 300ms; pacing was disabled", got)
	}
}
