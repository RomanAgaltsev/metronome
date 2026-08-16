package metronome

import (
	"context"
	"errors"
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

func TestPacerSchedulesOnTheClock(t *testing.T) {
	m := NewManualClock(time.Unix(0, 0))
	p := newPacer(m, 10, 1) // 10 rps, burst 1 → one token per 100ms

	// Burst 1 makes the first token available immediately.
	first, err := p.next(context.Background())
	if err != nil {
		t.Fatalf("next: %v", err)
	}
	if got := first.Sub(time.Unix(0, 0)); got != 0 {
		t.Fatalf("first scheduled at %v want 0", got)
	}

	got := make(chan time.Time, 1)
	go func() {
		s, err := p.next(context.Background())
		if err == nil {
			got <- s
		}
	}()
	m.BlockUntilSleepers(1) // the pacer must be sleeping on the injected Clock

	m.Advance(100 * time.Millisecond)
	select {
	case s := <-got:
		if d := s.Sub(time.Unix(0, 0)); d != 100*time.Millisecond {
			t.Fatalf("second scheduled at %v want 100ms", d)
		}
	case <-time.After(time.Second):
		t.Fatal("pacer did not wake on Advance; it is not Clock-driven")
	}
}

func TestPacerReturnsContextError(t *testing.T) {
	m := NewManualClock(time.Unix(0, 0))
	p := newPacer(m, 1, 1)
	if _, err := p.next(context.Background()); err != nil { // consume the burst token
		t.Fatalf("next: %v", err)
	}

	ctx, cancel := context.WithCancel(context.Background())
	done := make(chan error, 1)
	go func() { _, err := p.next(ctx); done <- err }()
	m.BlockUntilSleepers(1)

	cancel()
	select {
	case err := <-done:
		if !errors.Is(err, context.Canceled) {
			t.Fatalf("next returned %v want Canceled", err)
		}
	case <-time.After(time.Second):
		t.Fatal("next ignored cancellation")
	}
}
