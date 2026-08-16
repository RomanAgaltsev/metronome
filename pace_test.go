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

// The schedule must never claim a unit was due LATER than it actually went out.
// Without that bound the anchor accumulates a permanent lead, and every later
// Start-Scheduled goes negative — which Stats floors to zero, silently switching
// the correction back off.
//
// The floored-rate route into this is already closed by maxReservationWait: a
// reserve beyond the cap returns before touching the schedule, so nominal never
// jumps 2h46m ahead. This pins that, so the two fixes cannot drift apart.
func TestPacerScheduleNeverLeadsActualDispatch(t *testing.T) {
	m := NewManualClock(time.Unix(0, 0))
	p := newPacer(m, 100, 1) // 10ms interval

	if _, err := p.next(context.Background()); err != nil {
		t.Fatalf("next: %v", err)
	}

	// Floor the rate, let one unit through at the floor, then restore it. Before
	// the clamp this left nominal 2h46m40s ahead of the clock for good.
	p.setRate(0)
	if _, err := p.reserve(); err == nil {
		t.Log("floored reserve committed")
	}
	p.setRate(100)

	s, err := p.reserve()
	if err != nil {
		t.Fatalf("reserve after resume: %v", err)
	}
	if lead := s.scheduled.Sub(m.Now()); lead > 10*time.Millisecond {
		t.Fatalf("scheduled %v ahead of the clock after a floored rate; the anchor kept a "+
			"permanent lead and the correction is dead for the rest of the run", lead)
	}
}

// Burst legitimately dispatches B units at once. The schedule must not then sit
// (B-1) intervals ahead of reality for the whole run, hiding that much lag.
func TestPacerBurstDoesNotLeaveAPermanentLead(t *testing.T) {
	m := NewManualClock(time.Unix(0, 0))
	p := newPacer(m, 10, 5) // 100ms interval, burst 5

	for range 5 { // drain the burst; all five go out at once
		if _, err := p.reserve(); err != nil {
			t.Fatalf("burst reserve: %v", err)
		}
	}
	s, err := p.reserve() // the first paced unit
	if err != nil {
		t.Fatalf("reserve: %v", err)
	}
	// It actually goes out at now+delay; the schedule must not say it was due
	// four intervals after that.
	if actual := m.Now().Add(s.delay); s.scheduled.After(actual) {
		t.Fatalf("scheduled %v but the unit goes out at %v — the schedule leads dispatch by "+
			"%v, so that much lag is invisible for the rest of the run",
			s.scheduled.Sub(time.Unix(0, 0)), actual.Sub(time.Unix(0, 0)), s.scheduled.Sub(actual))
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
