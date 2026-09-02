package metronome

import (
	"math"
	"testing"
	"time"
)

// The tests in this file exist for the reason matrix_test.go does. Three
// releases and two reviews missed a hang reachable from a four-line Driver
// literal, because every suite tested the code that had been written rather
// than the configurations a user can type. Rolling adds five knobs.

// TestRollingWindowSeesAStallThatLifetimeStatsCannot is this feature's
// acceptance test: the v0.3 review's M5, encoded. It does not compile against
// v0.4.0, where RollingStats does not exist.
func TestRollingWindowSeesAStallThatLifetimeStatsCannot(t *testing.T) {
	clk := NewManualClock(time.Unix(0, 0))
	rs := NewRollingStats(Rolling{Window: 5 * time.Second, Buckets: 5, Clock: clk})

	// Five seconds of healthy traffic on schedule.
	for range 5 {
		for range 100 {
			rs.Record(Result{Start: clk.Now(), Scheduled: clk.Now(), Latency: time.Millisecond})
		}
		clk.Advance(time.Second)
	}

	// One unit goes out two seconds behind schedule.
	rs.Record(Result{
		Start:     clk.Now(),
		Scheduled: clk.Now().Add(-2 * time.Second),
		Latency:   time.Millisecond,
	})
	if got := rs.Window().MaxScheduleLag; got != 2*time.Second {
		t.Fatalf("window MaxScheduleLag=%v want 2s immediately after the late Result", got)
	}

	// The target then stops answering entirely for longer than a whole window.
	clk.Advance(6 * time.Second)

	life, win := rs.Snapshot(), rs.Window()

	// The lifetime view cannot see the stall. Asserted rather than lamented:
	// this is Stats' documented behaviour and the reason Window exists.
	if life.MaxScheduleLag != 2*time.Second {
		t.Fatalf("lifetime MaxScheduleLag=%v want it still pinned at 2s", life.MaxScheduleLag)
	}
	if life.RPS < 90 {
		t.Fatalf("lifetime RPS=%v want it still reporting the pre-stall rate", life.RPS)
	}
	if life.Window != 0 {
		t.Fatalf("lifetime Window=%v want 0", life.Window)
	}

	// The window does see it.
	if win.MaxScheduleLag != 0 {
		t.Fatalf("window MaxScheduleLag=%v want 0 once the late Result aged out", win.MaxScheduleLag)
	}
	if win.RPS != 0 {
		t.Fatalf("window RPS=%v want 0 during a full stall", win.RPS)
	}
	if win.Count != 0 {
		t.Fatalf("window Count=%d want 0 during a full stall", win.Count)
	}

	// And it recovers: the window is a view, not a latch.
	clk.Advance(100 * time.Millisecond)
	rs.Record(Result{Start: clk.Now(), Scheduled: clk.Now(), Latency: time.Millisecond})
	if got := rs.Window().Count; got != 1 {
		t.Fatalf("window Count=%d want 1 after traffic resumed", got)
	}
}

// rollingShapes covers the ends of every Rolling field's range.
//
// The histogram-shape cases pin Buckets low on purpose: memory is
// Buckets+2 histogram pairs, so a wide range or high Sigfigs across a large
// ring is hundreds of megabytes. The many-buckets case narrows the range for
// the same reason. That interaction is the one worth knowing about, so it is
// exercised at both ends rather than avoided.
func rollingShapes() []struct {
	name string
	cfg  Rolling
} {
	return []struct {
		name string
		cfg  Rolling
	}{
		{"zero value", Rolling{}},
		{"one bucket", Rolling{Window: time.Second, Buckets: 1}},
		{"two buckets", Rolling{Window: time.Second, Buckets: 2}},
		{"window not divisible by buckets", Rolling{Window: 10 * time.Second, Buckets: 3}},
		{"minimum legal window", Rolling{Window: 10, Buckets: 10}},
		{"long window", Rolling{Window: time.Hour, Buckets: 60}},
		{"many narrow buckets", Rolling{
			Window: 10 * time.Second, Buckets: 1000,
			Lo: time.Millisecond, Hi: time.Second, Sigfigs: 1,
		}},
		{"lowest sigfigs", Rolling{Window: time.Second, Buckets: 2, Sigfigs: 1}},
		{"highest sigfigs, narrow range", Rolling{
			Window: time.Second, Buckets: 2,
			Lo: time.Millisecond, Hi: 10 * time.Second, Sigfigs: 5,
		}},
		{"narrow histogram", Rolling{
			Window: time.Second, Buckets: 2,
			Lo: time.Millisecond, Hi: 2 * time.Millisecond, Sigfigs: 1,
		}},
	}
}

func TestRollingMatrix(t *testing.T) {
	for _, sh := range rollingShapes() {
		t.Run(sh.name, func(t *testing.T) {
			clk := NewManualClock(time.Unix(0, 0))
			cfg := sh.cfg
			cfg.Clock = clk

			// Every shape in this table must stay affordable. A case that costs
			// more than this is a mistake in the table, not a slow test — the
			// point of the narrowed ranges above.
			const budget = 64 << 20 // 64 MiB
			if got := cfg.Bytes(); got > budget {
				t.Fatalf("Bytes()=%d exceeds the %d-byte test budget", got, budget)
			}

			rs := NewRollingStats(cfg)

			window := rs.interval * time.Duration(len(rs.ring))

			for i := range 200 {
				rs.Record(Result{
					Start:     clk.Now(),
					Scheduled: clk.Now(),
					Latency:   time.Millisecond,
				})
				if i%20 == 0 {
					clk.Advance(rs.interval)
				}
			}

			life := rs.Snapshot()
			if life.Count != 200 {
				t.Fatalf("lifetime Count=%d want 200", life.Count)
			}
			if life.Window != 0 {
				t.Fatalf("lifetime Window=%v want 0", life.Window)
			}

			win := rs.Window()
			switch {
			case win.Window <= 0:
				t.Fatalf("window Window=%v want positive", win.Window)
			case win.Window > window:
				t.Fatalf("window Window=%v exceeds the configured window %v", win.Window, window)
			case win.Count < 0 || win.Count > life.Count:
				t.Fatalf("window Count=%d outside [0, %d]", win.Count, life.Count)
			case math.IsNaN(win.RPS) || math.IsInf(win.RPS, 0):
				t.Fatalf("window RPS=%v want finite", win.RPS)
			case win.RPS < 0:
				t.Fatalf("window RPS=%v want non-negative", win.RPS)
			}

			// A gap of a whole window empties it, at any geometry.
			clk.Advance(window + rs.interval)
			if got := rs.Window(); got.Count != 0 {
				t.Fatalf("after a full-window gap Count=%d want 0", got.Count)
			}
			if got := rs.Snapshot().Count; got != 200 {
				t.Fatalf("lifetime Count=%d want 200; a gap must not touch it", got)
			}
		})
	}
}

func TestRollingConcurrentRecordAndWindow(t *testing.T) {
	// Under -race, with a real clock so rotation actually fires while both sides
	// are running. goleak (TestMain in driver_test.go) covers the goroutines.
	rs := NewRollingStats(Rolling{Window: 20 * time.Millisecond, Buckets: 4})

	done := make(chan struct{})
	go func() {
		defer close(done)
		for range 20000 {
			rs.Record(Result{Start: time.Now(), Scheduled: time.Now(), Latency: time.Millisecond})
		}
	}()

	for range 2000 {
		if snap := rs.Window(); snap.Count < 0 {
			t.Errorf("window Count=%d", snap.Count)
		}
	}
	<-done

	if got := rs.Snapshot().Count; got != 20000 {
		t.Fatalf("lifetime Count=%d want 20000", got)
	}
}
