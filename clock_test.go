package metronome

import (
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
