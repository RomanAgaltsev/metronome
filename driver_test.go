package metronome

import (
	"context"
	"errors"
	"fmt"
	"math"
	"os"
	"runtime"
	"slices"
	"sync/atomic"
	"testing"
	"time"

	"go.uber.org/goleak"
)

func TestMain(m *testing.M) {
	goleak.VerifyTestMain(m)
}

func TestDriverMaxRequest(t *testing.T) {
	var calls atomic.Int64
	d := Driver{
		Runner:      RunnerFunc(func(context.Context) Result { calls.Add(1); return Result{Start: time.Now()} }),
		Rate:        Constant(1000),
		Workers:     4,
		MaxRequests: 50,
	}
	var got int
	for range d.Run(context.Background()) {
		got++
	}
	if got != 50 {
		t.Fatalf("received %d results, want 50", got)
	}
	if calls.Load() != 50 {
		t.Fatalf("runner called %d times, want 50", calls.Load())
	}
}

func TestDriverStopsOnCancel(t *testing.T) {
	d := Driver{
		Runner:  RunnerFunc(func(context.Context) Result { return Result{Start: time.Now()} }),
		Rate:    Constant(1000),
		Workers: 2,
	}
	ctx, cancel := context.WithTimeout(context.Background(), 100*time.Millisecond)
	defer cancel()
	got := 0
	for range d.Run(ctx) {
		got++
	}
	if got == 0 {
		t.Fatal("expected some results before cancel")
	}
}

func TestDriverPacesApproximately(t *testing.T) {
	if testing.Short() {
		t.Skip("timing-sensitive pacing test")
	}
	d := Driver{
		Runner:      RunnerFunc(func(context.Context) Result { return Result{Start: time.Now()} }),
		Rate:        Constant(200),
		Workers:     4,
		MaxRequests: 100,
	}
	start := time.Now()
	n := 0
	for range d.Run(context.Background()) {
		n++
	}
	elapsed := time.Since(start)
	if n != 100 {
		t.Fatalf("received %d results, want 100", n)
	}
	// 100 requests at 200 rps ≈ 500ms ideal (burst 1). The upper bound is very
	// wide because CI runs this on three OSes under -race, where coarse timers
	// and instrumentation both inflate it — but pacing that is off by 10x still
	// fails. Precise pacing assertions live in TestDriverPacesDeterministically.
	if elapsed < 250*time.Millisecond || elapsed > 5*time.Second {
		t.Fatalf("100 req at 200 rps took %v, want ~500ms (accept 250ms..5s)", elapsed)
	}
}

func TestDriverAppliesLiveRateUpdates(t *testing.T) {
	if testing.Short() {
		t.Skip("timing-sensitive pacing test")
	}
	a := NewAdaptive(10) // at 10 rps, 60 requests would take ~6s unchanged
	d := Driver{
		Runner:      RunnerFunc(func(context.Context) Result { return Result{Start: time.Now()} }),
		Rate:        a,
		Workers:     4,
		MaxRequests: 60,
	}
	go func() {
		time.Sleep(300 * time.Millisecond)
		a.SetRate(5000) // the 100ms limiter updater must pick this up live
	}()
	start := time.Now()
	for range d.Run(context.Background()) {
	}
	if elapsed := time.Since(start); elapsed > 3*time.Second {
		t.Fatalf("SetRate mid-run not applied: 60 req took %v, want well under 3s", elapsed)
	}
}

func TestDriverContainsRunnerPanic(t *testing.T) {
	var n atomic.Int64
	d := Driver{
		Runner: RunnerFunc(func(context.Context) Result {
			if n.Add(1) == 2 {
				panic("runner exploded")
			}
			return Result{Start: time.Now(), Latency: time.Millisecond}
		}),
		Rate:        Constant(1000),
		Workers:     1,
		MaxRequests: 3,
	}

	var results []Result
	for r := range d.Run(context.Background()) {
		results = append(results, r)
	}
	if len(results) != 3 {
		t.Fatalf("got %d results, want 3 — a panicking Runner must not abort the run", len(results))
	}

	var panics int
	for _, r := range results {
		var pe *PanicError
		if errors.As(r.Err, &pe) {
			panics++
			if pe.Value != "runner exploded" {
				t.Errorf("PanicError.Value=%v want %q", pe.Value, "runner exploded")
			}
			if len(pe.Stack) == 0 {
				t.Error("PanicError.Stack is empty")
			}
			// The rendered message is what a user actually reads in a log.
			if got, want := pe.Error(), "metronome: runner panicked: runner exploded"; got != want {
				t.Errorf("PanicError.Error()=%q want %q", got, want)
			}
			if r.Start.IsZero() {
				t.Error("a panicked Result must still carry Start")
			}
		}
	}
	if panics != 1 {
		t.Fatalf("got %d PanicError results, want 1", panics)
	}
}

func TestDriverStampsStartWhenRunnerForgets(t *testing.T) {
	d := Driver{
		Runner:      RunnerFunc(func(context.Context) Result { return Result{} }), // no Start
		Rate:        Constant(1000),
		Workers:     1,
		MaxRequests: 1,
	}
	for r := range d.Run(context.Background()) {
		if r.Start.IsZero() {
			t.Fatal("Driver must stamp Start when the Runner leaves it zero; Stats' RPS depends on it")
		}
	}
}

func TestDriverBuffersResults(t *testing.T) {
	d := Driver{
		Runner:      RunnerFunc(func(context.Context) Result { return Result{Start: time.Now()} }),
		Rate:        Constant(math.Inf(1)),
		Workers:     1,
		MaxRequests: 64,
	}
	ch := d.Run(context.Background())

	// With the default buffer the producers run ahead of a consumer that has not
	// read anything yet. Poll rather than sleep-and-hope.
	deadline := time.Now().Add(2 * time.Second)
	for len(ch) == 0 && time.Now().Before(deadline) {
		runtime.Gosched()
	}
	if len(ch) == 0 {
		t.Fatal("result channel never buffered anything; Run is still synchronous")
	}

	got := 0
	for range ch {
		got++
	}
	if got != 64 {
		t.Fatalf("got %d results, want 64", got)
	}
}

func TestDriverUnbufferedWhenRequested(t *testing.T) {
	d := Driver{
		Runner:       RunnerFunc(func(context.Context) Result { return Result{Start: time.Now()} }),
		Rate:         Constant(1000),
		Workers:      1,
		MaxRequests:  4,
		ResultBuffer: -1,
	}
	ch := d.Run(context.Background())
	if cap(ch) != 0 {
		t.Fatalf("cap=%d want 0 for ResultBuffer=-1", cap(ch))
	}
	got := 0
	for range ch {
		got++
	}
	if got != 4 {
		t.Fatalf("got %d results, want 4", got)
	}
}

func TestDriverPanicsOnMissingFields(t *testing.T) {
	cases := []struct {
		name string
		d    Driver
	}{
		{"nil Runner", Driver{Rate: Constant(1)}},
		{"nil Rate", Driver{Runner: RunnerFunc(func(context.Context) Result { return Result{} })}},
	}
	for _, tc := range cases {
		t.Run(tc.name, func(t *testing.T) {
			defer func() {
				if recover() == nil {
					t.Fatal("expected panic")
				}
			}()
			tc.d.Run(context.Background())
		})
	}
}

func TestDriverDefaultsWorkers(t *testing.T) {
	for _, w := range []int{0, -3} {
		d := Driver{
			Runner:      RunnerFunc(func(context.Context) Result { return Result{Start: time.Now()} }),
			Rate:        Constant(math.Inf(1)),
			Workers:     w,
			MaxRequests: 20,
		}
		if got := d.config().workers; got != DefaultWorkers {
			t.Fatalf("Workers=%d resolved to %d, want %d", w, got, DefaultWorkers)
		}
		n := 0
		for range d.Run(context.Background()) {
			n++
		}
		if n != 20 {
			t.Fatalf("Workers=%d delivered %d results, want 20", w, n)
		}
	}
}

func TestDriverClosesPromptlyOnCancel(t *testing.T) {
	d := Driver{
		Runner:  RunnerFunc(func(context.Context) Result { return Result{Start: time.Now()} }),
		Rate:    Constant(1000),
		Workers: 2,
	}
	ctx, cancel := context.WithCancel(context.Background())
	defer cancel() // the loop below may exit without reaching its own cancel
	ch := d.Run(ctx)

	got := 0
	for range ch {
		got++
		if got == 5 {
			cancel()
			break
		}
	}
	// The channel must close on its own now that ctx is cancelled — that is the
	// documented escape hatch for a caller who stops draining.
	closed := make(chan struct{})
	go func() {
		for range ch { // draining to close is the point of the test
		}
		close(closed)
	}()
	select {
	case <-closed:
	case <-time.After(2 * time.Second):
		t.Fatal("Run's channel did not close within 2s of cancel")
	}
}

func TestDriverStampsScheduled(t *testing.T) {
	d := Driver{
		Runner:      RunnerFunc(func(context.Context) Result { return Result{Start: time.Now(), Latency: time.Millisecond} }),
		Rate:        Constant(500),
		Workers:     2,
		MaxRequests: 20,
	}
	for r := range d.Run(context.Background()) {
		if r.Scheduled.IsZero() {
			t.Fatal("every Result must carry its scheduled send time")
		}
		if r.Start.Before(r.Scheduled.Add(-50 * time.Millisecond)) {
			t.Fatalf("Start %v is implausibly far before Scheduled %v", r.Start, r.Scheduled)
		}
	}
}

// The v0.2 replacement for TestDriverManualClockPinsRateAtZeroElapsed (H1):
// pacing is now exact under a ManualClock, with no wall-clock tolerance.
func TestDriverPacesDeterministically(t *testing.T) {
	m := NewManualClock(time.Unix(0, 0))
	d := Driver{
		Runner:  RunnerFunc(func(context.Context) Result { return Result{Latency: time.Millisecond} }),
		Rate:    Constant(10), // one unit per 100ms
		Workers: 1,
		Clock:   m,
	}
	ctx, cancel := context.WithCancel(context.Background())
	ch := d.Run(ctx)

	var stamps []time.Time
	for range 3 {
		// The worker (after the first, immediate token) and the rate updater are
		// both asleep on this clock at 100ms boundaries.
		if len(stamps) > 0 {
			m.BlockUntilSleepers(2)
			m.Advance(100 * time.Millisecond)
		}
		select {
		case r, ok := <-ch:
			if !ok {
				t.Fatal("channel closed early")
			}
			stamps = append(stamps, r.Scheduled)
		case <-time.After(2 * time.Second):
			t.Fatalf("no result after %d stamps", len(stamps))
		}
	}

	for i := 1; i < len(stamps); i++ {
		if gap := stamps[i].Sub(stamps[i-1]); gap != 100*time.Millisecond {
			t.Fatalf("scheduled gap %d = %v, want exactly 100ms (stamps: %v)", i, gap, stamps)
		}
	}

	cancel()
	for range ch { // drain to close
	}
}

func TestOpenLoopRecordsSaturation(t *testing.T) {
	release := make(chan struct{})
	d := Driver{
		Runner: RunnerFunc(func(ctx context.Context) Result {
			select {
			case <-release:
			case <-ctx.Done():
			}
			return Result{Latency: time.Millisecond}
		}),
		Rate:        Constant(1000),
		Workers:     1, // exactly one in-flight unit allowed
		MaxRequests: 20,
		Pacing:      OpenLoop,
	}

	ctx, cancel := context.WithTimeout(context.Background(), 5*time.Second)
	defer cancel()

	var saturated, completed int
	var released bool
	ch := d.Run(ctx)
	for r := range ch {
		if errors.Is(r.Err, ErrSaturated) {
			saturated++
			if r.Scheduled.IsZero() {
				t.Error("a saturated Result must still carry its Scheduled time")
			}
		} else {
			completed++
		}
		// Close once, and never reassign release: the Runner closure reads the
		// variable, so nilling it would park every later worker on a nil channel
		// until ctx expires.
		if saturated >= 5 && !released {
			close(release) // let the stuck worker finish so the run can drain
			released = true
		}
	}
	if saturated == 0 {
		t.Fatal("open loop with 1 worker and a blocked Runner recorded no saturation — " +
			"the schedule blocked on the target instead")
	}
	if saturated+completed != 20 {
		t.Fatalf("dispatched %d units (%d saturated + %d completed), want 20 — "+
			"MaxRequests counts saturated attempts", saturated+completed, saturated, completed)
	}
}

// A consumer that stops reading must not be reported as target saturation. The
// worker used to hold its in-flight slot across the result send, so a paused
// consumer consumed open-loop capacity and the dispatcher blamed the target.
func TestOpenLoopDoesNotBlameTheConsumer(t *testing.T) {
	d := Driver{
		Runner:       RunnerFunc(func(context.Context) Result { return Result{Latency: time.Millisecond} }),
		Rate:         Constant(200),
		Workers:      2,
		MaxRequests:  6,
		Pacing:       OpenLoop,
		ResultBuffer: -1, // unbuffered: every send waits for this test's reads
	}
	ctx, cancel := context.WithTimeout(context.Background(), 10*time.Second)
	defer cancel()

	ch := d.Run(ctx)
	// Pause before reading anything, so the first units finish with nowhere to
	// put their Results while the schedule keeps ticking.
	time.Sleep(150 * time.Millisecond)

	var saturated, completed int
	for r := range ch {
		if errors.Is(r.Err, ErrSaturated) {
			saturated++
		} else {
			completed++
		}
	}
	if saturated+completed != 6 {
		t.Fatalf("got %d results, want 6", saturated+completed)
	}
	if saturated != 0 {
		t.Fatalf("%d of 6 units reported ErrSaturated with a fast Runner and a slow consumer; "+
			"the in-flight slot is being held across the result send", saturated)
	}
}

func TestClosedLoopIsTheDefault(t *testing.T) {
	var concurrent, maxConcurrent atomic.Int64
	d := Driver{
		Runner: RunnerFunc(func(context.Context) Result {
			c := concurrent.Add(1)
			for {
				m := maxConcurrent.Load()
				if c <= m || maxConcurrent.CompareAndSwap(m, c) {
					break
				}
			}
			time.Sleep(5 * time.Millisecond)
			concurrent.Add(-1)
			return Result{Latency: 5 * time.Millisecond}
		}),
		Rate:        Constant(1000),
		Workers:     3,
		MaxRequests: 30,
	}
	for r := range d.Run(context.Background()) {
		if errors.Is(r.Err, ErrSaturated) {
			t.Fatal("the default mode must be closed-loop: it blocks, it never saturates")
		}
	}
	if got := maxConcurrent.Load(); got > 3 {
		t.Fatalf("max concurrency %d exceeded Workers=3", got)
	}
}

func TestDriverBurstIsHonoured(t *testing.T) {
	m := NewManualClock(time.Unix(0, 0))
	d := Driver{
		Runner:      RunnerFunc(func(context.Context) Result { return Result{Latency: time.Millisecond} }),
		Rate:        Constant(1), // one per second...
		Workers:     4,
		MaxRequests: 5,
		Burst:       5, // ...but five available at once
		Clock:       m,
	}
	ctx, cancel := context.WithTimeout(context.Background(), 5*time.Second)
	defer cancel()

	n := 0
	for range d.Run(ctx) {
		n++
	}
	// All five come from the initial burst without the clock advancing at all.
	if n != 5 {
		t.Fatalf("got %d results from a burst of 5 on a frozen clock, want 5", n)
	}
}

// The three v0.2 pieces composed: a schedule kept under a frozen clock, a
// target that stalls, saturation recorded rather than absorbed, and corrected
// percentiles that reveal the queueing the raw ones hide.
func TestOpenLoopSaturationIsMeasuredNotHidden(t *testing.T) {
	m := NewManualClock(time.Unix(0, 0))
	release := make(chan struct{})

	d := Driver{
		Runner: RunnerFunc(func(ctx context.Context) Result {
			start := m.Now()
			select {
			case <-release:
			case <-ctx.Done():
			}
			return Result{Start: start, Latency: 10 * time.Millisecond}
		}),
		Rate:        Constant(10), // one unit per 100ms
		Workers:     1,
		MaxRequests: 4,
		Pacing:      OpenLoop,
		Clock:       m,
	}

	ctx, cancel := context.WithTimeout(context.Background(), 5*time.Second)
	defer cancel()

	stats := NewStats()
	ch := d.Run(ctx)

	// Unit 1 takes the only slot (burst token, no sleep) and blocks in Do.
	// Units 2, 3 and 4 are dispatched at 100ms, 200ms and 300ms with no slot
	// free, so each is emitted as ErrSaturated at its scheduled time.
	//
	// Each Result is read here, before the next tick and before release is
	// closed: receiving it is the only barrier proving the dispatcher already
	// found the slot busy. Advance only wakes the dispatcher — it does not run
	// it — so closing release first would race the freed slot against unit 4's
	// dispatch and let unit 4 succeed. Unit 1 cannot emit until release closes,
	// so every Result read in this loop is a saturated one.
	for i := range 3 {
		m.BlockUntilSleepers(2) // the dispatcher and the rate updater
		m.Advance(100 * time.Millisecond)

		r, ok := <-ch
		if !ok {
			t.Fatalf("channel closed after %d saturated Results", i)
		}
		if !errors.Is(r.Err, ErrSaturated) {
			t.Fatalf("unit %d: Err=%v, want ErrSaturated", i+2, r.Err)
		}
		stats.Record(r)
	}
	close(release)

	collected := make(chan Snapshot, 1)
	go func() {
		for r := range ch {
			stats.Record(r)
		}
		collected <- stats.Snapshot()
	}()

	select {
	case snap := <-collected:
		if snap.Count != 4 {
			t.Fatalf("Count=%d want 4", snap.Count)
		}
		if snap.Errors != 3 {
			t.Fatalf("Errors=%d want 3 saturated attempts", snap.Errors)
		}
		if snap.ErrorRate != 0.75 {
			t.Fatalf("ErrorRate=%v want 0.75 — saturation must be visible in the summary", snap.ErrorRate)
		}
		if snap.CorrectedCount != 4 {
			t.Fatalf("CorrectedCount=%d want 4 — every Result carries Scheduled", snap.CorrectedCount)
		}
	case <-time.After(5 * time.Second):
		t.Fatal("the run never finished")
	}
}

// The v0.3 acceptance test. Offered 100 rps against a target that takes 50ms with
// one worker: the generator can only manage ~20 rps, so a client that kept to the
// 10ms schedule would queue without bound. That queueing is exactly what
// coordinated-omission correction exists to surface, and in v0.2 it reported zero.
func TestCorrectedPercentilesRevealClosedLoopSag(t *testing.T) {
	if testing.Short() {
		t.Skip("timing-sensitive")
	}
	const offered = 100.0
	d := Driver{
		Runner: RunnerFunc(func(context.Context) Result {
			time.Sleep(50 * time.Millisecond)
			return Result{Latency: 50 * time.Millisecond}
		}),
		Rate:        Constant(offered),
		Workers:     1,
		MaxRequests: 20,
	}
	s := NewStats()
	for r := range d.Run(context.Background()) {
		s.Record(r)
	}
	snap := s.Snapshot()

	if snap.RPS > offered/2 {
		t.Fatalf("achieved %.1f rps of %.0f offered; this test needs a sagging run", snap.RPS, offered)
	}
	if snap.CorrectedP99 <= snap.P99 {
		t.Fatalf("CorrectedP99=%v <= P99=%v; the correction is inert — Scheduled is still "+
			"being derived from arrival time", snap.CorrectedP99, snap.P99)
	}
	// By the last of 20 units the schedule is ~1s behind, so the corrected tail must
	// be far above the 50ms of service time, not marginally above it.
	if snap.CorrectedP99 < 300*time.Millisecond {
		t.Fatalf("CorrectedP99=%v; want well above the 50ms service time", snap.CorrectedP99)
	}
	if snap.MaxScheduleLag < 300*time.Millisecond {
		t.Fatalf("MaxScheduleLag=%v; the generator fell ~1s behind its own schedule",
			snap.MaxScheduleLag)
	}
}

// H2, deterministically: a generator that cannot hold its own interval is late,
// and that lateness must be reported. Advancing the manual clock in 500ms steps
// against a 100ms schedule IS a pacer that runs 400ms late, with no wall-clock
// timing involved.
func TestDriverReportsGeneratorLagUnderAManualClock(t *testing.T) {
	m := NewManualClock(time.Unix(0, 0))
	d := Driver{
		Runner:      RunnerFunc(func(context.Context) Result { return Result{Latency: time.Millisecond} }),
		Rate:        Constant(10), // one unit per 100ms
		Workers:     1,
		MaxRequests: 3,
		Clock:       m,
	}
	ctx, cancel := context.WithTimeout(context.Background(), 5*time.Second)
	defer cancel()

	base := time.Unix(0, 0)
	s := NewStats()
	ch := d.Run(ctx)

	first, ok := <-ch // due at 0, dispatched at 0
	if !ok {
		t.Fatal("channel closed before the first result")
	}
	s.Record(first)

	// The worker is now asleep waiting for the unit due at 100ms, and so is the
	// rate updater. Jumping 500ms makes the pacer late by construction: the
	// limiter's bucket has refilled, so the units due at 100ms and 200ms are
	// both dispatched at 500ms.
	m.BlockUntilSleepers(2)
	m.Advance(500 * time.Millisecond)

	var scheduled []time.Duration
	for r := range ch {
		scheduled = append(scheduled, r.Scheduled.Sub(base))
		s.Record(r)
	}

	want := []time.Duration{100 * time.Millisecond, 200 * time.Millisecond}
	if !slices.Equal(scheduled, want) {
		t.Fatalf("Scheduled times %v, want %v — the schedule must advance one interval per "+
			"unit, not jump to the time the pacer happened to arrive", scheduled, want)
	}

	snap := s.Snapshot()
	// The unit due at 100ms started at 500ms: 400ms late, and nothing about the
	// target caused it.
	if snap.MaxScheduleLag != 400*time.Millisecond {
		t.Fatalf("MaxScheduleLag=%v want exactly 400ms", snap.MaxScheduleLag)
	}
	if snap.Saturated != 0 {
		t.Fatalf("Saturated=%d want 0 — the target was never the problem", snap.Saturated)
	}
}

// TestPacingAdherence asserts the achieved rate is within ±5% of the offered
// rate. It is env-gated because it needs an idle machine: on a busy CI runner it
// measures the runner, not metronome. Run it deliberately:
//
//	METRONOME_PACING_TEST=1 go test -run TestPacingAdherence -v ./...
func TestPacingAdherence(t *testing.T) {
	if os.Getenv("METRONOME_PACING_TEST") != "1" {
		t.Skip("set METRONOME_PACING_TEST=1 to run (needs an idle machine)")
	}
	for _, rps := range []float64{10, 100, 1000} {
		t.Run(fmt.Sprintf("%.0frps", rps), func(t *testing.T) {
			d := Driver{
				Runner:      RunnerFunc(func(context.Context) Result { return Result{} }),
				Rate:        Constant(rps),
				Workers:     runtime.GOMAXPROCS(0),
				MaxRequests: int(rps * 2), // two seconds of load
			}
			s := NewStats()
			for r := range d.Run(context.Background()) {
				s.Record(r)
			}
			got := s.Snapshot().RPS
			if math.Abs(got-rps)/rps > 0.05 {
				t.Fatalf("offered %.0f rps, achieved %.1f rps (%.1f%% off, want within 5%%)",
					rps, got, math.Abs(got-rps)/rps*100)
			}
		})
	}
}
