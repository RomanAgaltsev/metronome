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

// The schedule is anchored: nominal send times advance by exactly one interval per
// unit, whatever time the pacer is actually called at. A pacer that arrives late
// must return a Scheduled time in the PAST, not "now".
func TestPacerScheduleIsAnchoredNotDerivedFromArrival(t *testing.T) {
	m := NewManualClock(time.Unix(0, 0))
	p := newPacer(m, 10, 1) // one unit per 100ms
	base := time.Unix(0, 0)

	first, err := p.next(context.Background())
	if err != nil {
		t.Fatalf("next: %v", err)
	}
	if got := first.Sub(base); got != 0 {
		t.Fatalf("first scheduled at %v want 0", got)
	}

	// Jump the clock far past the next unit's due time: the token is waiting, so
	// next returns immediately — and must report the time the unit was DUE.
	m.Advance(time.Second)
	second, err := p.next(context.Background())
	if err != nil {
		t.Fatalf("next: %v", err)
	}
	if got := second.Sub(base); got != 100*time.Millisecond {
		t.Fatalf("second scheduled at %v, want 100ms — the schedule was redefined by a "+
			"late arrival instead of anchored", got)
	}
	if !second.Before(m.Now()) {
		t.Fatal("a late unit's Scheduled time must be in the past")
	}
}

func TestPacerUnlimitedRateHasNoSchedule(t *testing.T) {
	m := NewManualClock(time.Unix(0, 0))
	p := newPacer(m, math.Inf(1), 1)

	if _, err := p.next(context.Background()); err != nil {
		t.Fatalf("next: %v", err)
	}
	m.Advance(time.Second)
	got, err := p.next(context.Background())
	if err != nil {
		t.Fatalf("next: %v", err)
	}
	// Nothing is ever late against a schedule that demands everything immediately.
	if !got.Equal(m.Now()) {
		t.Fatalf("scheduled %v want %v — an unlimited rate has no schedule to fall behind",
			got, m.Now())
	}
}

// This is a characterisation test of golang.org/x/time/rate, and it is the whole
// justification for pacer.mu. It is deterministic — no goroutines, no wall clock
// — because the effect it pins is a property of the limiter, not of scheduling.
//
// reserveN sets lim.last = t unconditionally and advance() clamps its anchor
// backwards when t precedes it. So if a caller reads the clock and then loses
// the race for the limiter's own mutex, its stale timestamp rewinds the anchor,
// and the NEXT caller is credited with tokens for an interval that has already
// been spent. The bias is one-directional: the limiter hands out more than the
// rate allows, i.e. metronome drives faster than it was asked to.
//
// If this test ever fails, x/time/rate changed its anchoring rules — re-derive
// whether pacer.mu is still needed rather than deleting the test.
func TestStaleTimestampRewindsTheLimiterAnchor(t *testing.T) {
	const perToken = time.Millisecond // 1000 rps
	base := time.Unix(0, 0)
	at := base.Add

	// Interleaved: a caller that read the clock at 90ms reserves after one that
	// read it at 100ms.
	stale := rate.NewLimiter(rate.Limit(1000), 100)
	stale.ReserveN(at(100*time.Millisecond), 100) // drain the bucket, anchor at 100ms
	stale.ReserveN(at(90*time.Millisecond), 1)    // stale: rewinds the anchor to 90ms
	staleDelay := stale.ReserveN(at(101*time.Millisecond), 1).DelayFrom(at(101 * time.Millisecond))

	// The same three reservations, ordered — what pacer.mu guarantees.
	ordered := rate.NewLimiter(rate.Limit(1000), 100)
	ordered.ReserveN(at(100*time.Millisecond), 100)
	ordered.ReserveN(at(100*time.Millisecond), 1)
	orderedDelay := ordered.ReserveN(at(101*time.Millisecond), 1).DelayFrom(at(101 * time.Millisecond))

	if orderedDelay != perToken {
		t.Fatalf("ordered delay %v, want %v — an empty bucket owes one token's wait", orderedDelay, perToken)
	}
	if staleDelay != 0 {
		t.Fatalf("stale delay %v, want 0 — this test no longer demonstrates the rewind", staleDelay)
	}
	if staleDelay >= orderedDelay {
		t.Fatalf("stale delay %v >= ordered delay %v; the rewind no longer over-grants",
			staleDelay, orderedDelay)
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
