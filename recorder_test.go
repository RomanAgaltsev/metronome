package metronome

import (
	"testing"
	"time"
)

// Compile-time proof that the seam names something already true.
var (
	_ Recorder = (*Stats)(nil)
	_ Recorder = (*RollingStats)(nil)
)

func TestStatsBytesMatchesTheDerivedHistogramSize(t *testing.T) {
	// Two histograms (raw and corrected) of countsLen int64s each.
	// 1us..60s at 3 sigfigs: subBucketCount 2048, buckets 16,
	// countsLen = 17 * 1024 = 17,408 -> 139,264 bytes per histogram.
	const want = 2 * 17408 * 8

	if got := NewStats().Bytes(); got != want {
		t.Fatalf("NewStats().Bytes()=%d want %d", got, want)
	}
}

func TestStatsBytesTracksANarrowerRange(t *testing.T) {
	// 1ms..1s at 1 sigfig: subBucketCount 32, so a much smaller array.
	s := NewStatsRange(time.Millisecond, time.Second, 1)
	if got := s.Bytes(); got <= 0 || got >= NewStats().Bytes() {
		t.Fatalf("narrow-range Bytes()=%d want >0 and < %d", got, NewStats().Bytes())
	}
}

func TestRollingStatsBytesConvergesOnItsConfigBytes(t *testing.T) {
	// Two derivations of one formula must not drift apart. Since v0.9 they meet
	// at the ceiling rather than on the first call: ring buckets start sparse,
	// so a fresh RollingStats reports far less than the budget and rises toward
	// it as buckets promote. Promoting every bucket by hand puts the ring in the
	// state cfg.Bytes() prices, which is where the two formulas must still agree
	// exactly — the tripwire for an hdr.New sizing change is unchanged.
	for _, cfg := range []Rolling{
		{},
		{Window: time.Second, Buckets: 4},
		{Window: 30 * time.Second, Buckets: 3, Lo: time.Millisecond, Hi: time.Second, Sigfigs: 2},
	} {
		ceiling := cfg.Bytes()
		rs := NewRollingStats(cfg)

		if fresh := rs.Bytes(); fresh >= ceiling {
			t.Fatalf("NewRollingStats(%+v).Bytes()=%d want below the %d ceiling", cfg, fresh, ceiling)
		}

		for _, b := range rs.ring {
			b.lat.promote()
		}
		if got := rs.Bytes(); got != ceiling {
			t.Fatalf("fully promoted NewRollingStats(%+v).Bytes()=%d want cfg.Bytes()=%d", cfg, got, ceiling)
		}
	}
}
