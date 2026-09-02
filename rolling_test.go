package metronome

import (
	"testing"
	"time"
)

func TestNewRollingStatsZeroValueIsUsable(t *testing.T) {
	rs := NewRollingStats(Rolling{})

	if got := len(rs.ring); got != DefaultBuckets {
		t.Fatalf("len(ring)=%d want %d", got, DefaultBuckets)
	}
	if got, want := rs.interval, DefaultWindow/DefaultBuckets; got != want {
		t.Fatalf("interval=%v want %v", got, want)
	}
	if rs.clock == nil {
		t.Fatal("clock is nil; want the wall clock")
	}
	if rs.live != 1 {
		t.Fatalf("live=%d want 1 at construction", rs.live)
	}
	if !rs.origin.Equal(rs.curStart) {
		t.Fatalf("origin=%v curStart=%v want them equal at construction", rs.origin, rs.curStart)
	}
	if rs.life == nil || rs.scratch == nil {
		t.Fatal("life and scratch must be allocated")
	}
	for i, b := range rs.ring {
		if b == nil {
			t.Fatalf("ring[%d] is nil", i)
		}
	}
}

func TestNewRollingStatsTruncatesWindowToWholeBuckets(t *testing.T) {
	// 10s over 3 buckets is 3.333s each, so the ring spans 9.999s. The
	// truncation is documented rather than rounded away.
	rs := NewRollingStats(Rolling{Window: 10 * time.Second, Buckets: 3})
	if got, want := rs.interval, 10*time.Second/3; got != want {
		t.Fatalf("interval=%v want %v", got, want)
	}
}

func TestNewRollingStatsPanicsOnImpossibleConfig(t *testing.T) {
	cases := []struct {
		name string
		cfg  Rolling
	}{
		{"negative window", Rolling{Window: -time.Second}},
		{"negative buckets", Rolling{Buckets: -1}},
		{"window narrower than one nanosecond per bucket", Rolling{Window: 5, Buckets: 10}},
		{"negative sigfigs", Rolling{Sigfigs: -1}},
		{"sigfigs above the histogram maximum", Rolling{Sigfigs: 6}},
		{"negative lo", Rolling{Lo: -time.Second}},
		{"hi below lo", Rolling{Lo: time.Minute, Hi: time.Microsecond}},
	}

	for _, tc := range cases {
		t.Run(tc.name, func(t *testing.T) {
			defer func() {
				if recover() == nil {
					t.Fatalf("NewRollingStats(%+v) returned; want panic", tc.cfg)
				}
			}()
			NewRollingStats(tc.cfg)
		})
	}
}

func TestNewRollingStatsAcceptsTheMinimumLegalWindow(t *testing.T) {
	// One nanosecond per bucket is legal and must not panic; anything less
	// floors interval to zero, which is the case above.
	rs := NewRollingStats(Rolling{Window: 10, Buckets: 10})
	if rs.interval != time.Nanosecond {
		t.Fatalf("interval=%v want 1ns", rs.interval)
	}
}

func TestRollingBytes(t *testing.T) {
	// Exact values, not bounds. Bytes duplicates hdr.New's sizing formula, so a
	// dependency bump that changed that formula would make it silently wrong.
	// These assertions are the tripwire: if they fail after a go.mod update,
	// re-derive histogramCounts against the new hdr.New before touching them.
	cases := []struct {
		name string
		cfg  Rolling
		want int64
	}{
		// countsLen 17,408 -> 136 KiB per histogram, x2 per Stats, x12 Stats.
		{"default", Rolling{}, 3342336},
		// The knob that reads like resolution and is really a multiplier: same
		// range, 100x the buckets, ~100x the memory.
		{"a hundred buckets at the default range", Rolling{Buckets: 100}, 28409856},
		// countsLen 128 -> 1 KiB per histogram. Narrowing the range is what makes
		// a large ring affordable.
		{"a thousand narrow buckets", Rolling{
			Window: 10 * time.Second, Buckets: 1000,
			Lo: time.Millisecond, Hi: time.Second, Sigfigs: 1,
		}, 2052096},
	}

	for _, tc := range cases {
		t.Run(tc.name, func(t *testing.T) {
			if got := tc.cfg.Bytes(); got != tc.want {
				t.Fatalf("Bytes()=%d want %d", got, tc.want)
			}
		})
	}
}

func TestRollingBytesPanicsWhereNewRollingStatsDoes(t *testing.T) {
	// Pricing a configuration that cannot be built must not quietly return a
	// number for it.
	defer func() {
		if recover() == nil {
			t.Fatal("Bytes() returned on an unbuildable config; want panic")
		}
	}()
	_ = Rolling{Window: 5, Buckets: 10}.Bytes()
}
