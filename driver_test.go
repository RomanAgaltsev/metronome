package metronome

import (
	"context"
	"errors"
	"math"
	"runtime"
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
	// 100 requests at 200 rps ≈ 500ms ideal (burst 1). Bounds are wide because
	// coarse Windows timers overshoot — but pacing that is off by 10x must fail.
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
		for range ch { //nolint:revive // draining to close is the point of the test
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
	for range ch { //nolint:revive // drain to close
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
