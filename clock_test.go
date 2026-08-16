package metronome

import (
	"context"
	"errors"
	"testing"
	"time"
)

func TestManualClockAdvance(t *testing.T) {
	c := NewManualClock(time.Unix(0, 0))
	start := c.Now()
	c.Advance(2 * time.Second)
	if got := c.Now().Sub(start); got != 2*time.Second {
		t.Fatalf("want 2s elapsed, got %v", got)
	}
}

func TestSystemClockTracksWallClock(t *testing.T) {
	before := time.Now()
	got := SystemClock().Now()
	after := time.Now()
	if got.Before(before) || got.After(after) {
		t.Fatalf("SystemClock().Now()=%v outside [%v, %v]", got, before, after)
	}
}

func TestRealClockSleep(t *testing.T) {
	start := time.Now()
	if err := SystemClock().Sleep(context.Background(), 50*time.Millisecond); err != nil {
		t.Fatalf("Sleep returned %v", err)
	}
	if elapsed := time.Since(start); elapsed < 40*time.Millisecond {
		t.Fatalf("Sleep(50ms) returned after %v", elapsed)
	}
}

func TestRealClockSleepHonoursContext(t *testing.T) {
	ctx, cancel := context.WithTimeout(context.Background(), 20*time.Millisecond)
	defer cancel()
	start := time.Now()
	if err := SystemClock().Sleep(ctx, time.Hour); !errors.Is(err, context.DeadlineExceeded) {
		t.Fatalf("Sleep returned %v, want DeadlineExceeded", err)
	}
	if elapsed := time.Since(start); elapsed > time.Second {
		t.Fatalf("Sleep ignored the deadline for %v", elapsed)
	}
}

func TestClockSleepNonPositiveReturnsImmediately(t *testing.T) {
	for _, c := range []Clock{SystemClock(), NewManualClock(time.Unix(0, 0))} {
		if err := c.Sleep(context.Background(), 0); err != nil {
			t.Fatalf("%T.Sleep(0) returned %v", c, err)
		}
		if err := c.Sleep(context.Background(), -time.Second); err != nil {
			t.Fatalf("%T.Sleep(-1s) returned %v", c, err)
		}
	}
}

func TestManualClockSleepWakesOnAdvance(t *testing.T) {
	m := NewManualClock(time.Unix(0, 0))
	woke := make(chan time.Time, 2)

	go func() {
		_ = m.Sleep(context.Background(), 100*time.Millisecond)
		woke <- m.Now()
	}()
	go func() {
		_ = m.Sleep(context.Background(), 300*time.Millisecond)
		woke <- m.Now()
	}()
	m.BlockUntilSleepers(2)

	m.Advance(100 * time.Millisecond)
	select {
	case at := <-woke:
		if got := at.Sub(time.Unix(0, 0)); got != 100*time.Millisecond {
			t.Fatalf("first sleeper woke at %v want 100ms", got)
		}
	case <-time.After(time.Second):
		t.Fatal("the 100ms sleeper did not wake on Advance(100ms)")
	}

	select {
	case <-woke:
		t.Fatal("the 300ms sleeper woke early")
	case <-time.After(50 * time.Millisecond):
	}

	m.Advance(200 * time.Millisecond)
	select {
	case at := <-woke:
		if got := at.Sub(time.Unix(0, 0)); got != 300*time.Millisecond {
			t.Fatalf("second sleeper woke at %v want 300ms", got)
		}
	case <-time.After(time.Second):
		t.Fatal("the 300ms sleeper did not wake on the second Advance")
	}
}

func TestManualClockSleepHonoursContext(t *testing.T) {
	m := NewManualClock(time.Unix(0, 0))
	ctx, cancel := context.WithCancel(context.Background())
	done := make(chan error, 1)
	go func() { done <- m.Sleep(ctx, time.Hour) }()
	m.BlockUntilSleepers(1)

	cancel()
	select {
	case err := <-done:
		if !errors.Is(err, context.Canceled) {
			t.Fatalf("Sleep returned %v want Canceled", err)
		}
	case <-time.After(time.Second):
		t.Fatal("Sleep ignored cancellation")
	}

	// The abandoned sleeper must be deregistered, or every later Advance drags a
	// dead waiter along and BlockUntilSleepers over-counts. Assert on the waiter
	// list directly: BlockUntilSleepers(0) loops `for len(waiters) < 0`, so it
	// returns immediately however many stale waiters are left and proves nothing.
	m.Advance(time.Hour) // must not panic on a closed/stale waiter
	m.mu.Lock()
	left := len(m.waiters)
	m.mu.Unlock()
	if left != 0 {
		t.Fatalf("%d waiters left after a cancelled Sleep; the abandoned sleeper was not deregistered", left)
	}
}
