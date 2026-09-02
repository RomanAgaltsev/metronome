package metronome

import (
	"testing"
)

// newLabeled is the common construction used across this file.
func newLabeled(t *testing.T, cfg Labeled[*Stats]) *LabeledStats[*Stats] {
	t.Helper()
	if cfg.Key == "" {
		cfg.Key = "endpoint"
	}
	if cfg.New == nil {
		cfg.New = NewStats
	}
	return NewLabeledStats(cfg)
}

func TestNewLabeledStatsAcceptsTheZeroValuedOptionalFields(t *testing.T) {
	ls := newLabeled(t, Labeled[*Stats]{})
	if ls == nil {
		t.Fatal("NewLabeledStats returned nil")
	}
	if got := ls.Len(); got != 0 {
		t.Fatalf("Len()=%d want 0 before any Record", got)
	}
}

func TestNewLabeledStatsPanicsOnProgrammerError(t *testing.T) {
	cases := map[string]Labeled[*Stats]{
		"empty Key":                     {New: NewStats},
		"nil New":                       {Key: "endpoint"},
		"negative MaxSeries":            {Key: "endpoint", New: NewStats, MaxSeries: -1},
		"colliding sentinels":           {Key: "endpoint", New: NewStats, Overflow: "x", Unlabeled: "x"},
		"Overflow == default Unlabeled": {Key: "endpoint", New: NewStats, Overflow: DefaultUnlabeledLabel},
	}
	for name, cfg := range cases {
		t.Run(name, func(t *testing.T) {
			defer func() {
				if recover() == nil {
					t.Fatal("NewLabeledStats did not panic")
				}
			}()
			NewLabeledStats(cfg)
		})
	}
}

func TestNewLabeledStatsAllocatesOnlyTheTotalUpFront(t *testing.T) {
	// Series are lazy: a fresh LabeledStats holds exactly one child.
	ls := newLabeled(t, Labeled[*Stats]{})
	if got, want := ls.Bytes(), NewStats().Bytes(); got != want {
		t.Fatalf("Bytes()=%d want %d (the total alone)", got, want)
	}
}

func TestLabeledStatsSatisfiesRecorder(t *testing.T) {
	var _ Recorder = newLabeled(t, Labeled[*Stats]{})
	var _ Recorder = NewLabeledStats(Labeled[*RollingStats]{
		Key: "endpoint",
		New: func() *RollingStats { return NewRollingStats(Rolling{}) },
	})
}
