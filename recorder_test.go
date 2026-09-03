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

func TestRollingStatsBytesEqualsItsConfigBytes(t *testing.T) {
	// Two derivations of one formula must not drift apart.
	for _, cfg := range []Rolling{
		{},
		{Window: time.Second, Buckets: 4},
		{Window: 30 * time.Second, Buckets: 3, Lo: time.Millisecond, Hi: time.Second, Sigfigs: 2},
	} {
		want := cfg.Bytes()
		got := NewRollingStats(cfg).Bytes()
		if got != want {
			t.Fatalf("NewRollingStats(%+v).Bytes()=%d want cfg.Bytes()=%d", cfg, got, want)
		}
	}
}
