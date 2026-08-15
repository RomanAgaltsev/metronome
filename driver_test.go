package metronome

import (
	"context"
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
