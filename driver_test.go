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
	if elapsed < 250*time.Millisecond || elapsed > 2*time.Second {
		t.Fatalf("100 req at 200 rps took %v, want ~500ms (accept 250ms..2s)", elapsed)
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
