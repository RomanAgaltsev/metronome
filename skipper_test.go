package metronome

import (
	"testing"
	"time"
)

func TestAfterNSkipsExactlyTheFirstN(t *testing.T) {
	stub := &recordingStub{}
	s := AfterN(3, stub)

	for i := range 10 {
		s.Record(Result{Code: string(rune('0' + i))})
	}

	if got := len(stub.got); got != 7 {
		t.Fatalf("recorded %d Results, want 7", got)
	}
	if got := s.Skipped(); got != 3 {
		t.Fatalf("Skipped()=%d want 3", got)
	}
	if got := stub.got[0].Code; got != "3" {
		t.Fatalf("first recorded Result was %q, want the fourth (%q)", got, "3")
	}
}

func TestSkipperCountAndSkippedCoverThePopulation(t *testing.T) {
	// The audit property: nothing vanishes.
	stub := &recordingStub{}
	s := AfterN(4, stub)

	const population = 25
	for range population {
		s.Record(Result{})
	}

	if got := s.Snapshot().Count + s.Skipped(); got != population {
		t.Fatalf("Count+Skipped=%d want %d", got, population)
	}
}

func TestAfterNZeroAdmitsEverything(t *testing.T) {
	stub := &recordingStub{}
	s := AfterN(0, stub)
	for range 6 {
		s.Record(Result{})
	}

	if got := len(stub.got); got != 6 {
		t.Fatalf("recorded %d Results, want 6", got)
	}
	if got := s.Skipped(); got != 0 {
		t.Fatalf("Skipped()=%d want 0", got)
	}
}

func TestAfterNLargerThanThePopulationAdmitsNothing(t *testing.T) {
	stub := &recordingStub{}
	s := AfterN(100, stub)
	for range 10 {
		s.Record(Result{})
	}

	if got := len(stub.got); got != 0 {
		t.Fatalf("recorded %d Results, want 0", got)
	}
	if got := s.Skipped(); got != 10 {
		t.Fatalf("Skipped()=%d want 10", got)
	}
}

func TestAfterNOneSkipsOnlyTheFirst(t *testing.T) {
	stub := &recordingStub{}
	s := AfterN(1, stub)
	s.Record(Result{Code: "first"})
	s.Record(Result{Code: "second"})

	if got := len(stub.got); got != 1 || stub.got[0].Code != "second" {
		t.Fatalf("recorded %+v, want only the second", stub.got)
	}
}

func TestAfterNPanicsOnANegativeCount(t *testing.T) {
	defer func() {
		if recover() == nil {
			t.Fatal("AfterN(-1) did not panic")
		}
	}()
	AfterN(-1, &recordingStub{})
}

func TestSkipperPanicsOnANilRecorder(t *testing.T) {
	defer func() {
		if recover() == nil {
			t.Fatal("AfterN did not panic on a nil Recorder")
		}
	}()
	AfterN(1, nil)
}

func TestSkipperIsSafeUnderConcurrentRecord(t *testing.T) {
	s := AfterN(50, NewStats())

	const goroutines, each = 8, 100
	done := make(chan struct{})
	for range goroutines {
		go func() {
			defer func() { done <- struct{}{} }()
			for i := range each {
				s.Record(Result{Start: time.Unix(0, 0).Add(time.Duration(i) * time.Millisecond), Latency: time.Millisecond})
			}
		}()
	}
	for range goroutines {
		<-done
	}

	if got, want := s.Snapshot().Count+s.Skipped(), int64(goroutines*each); got != want {
		t.Fatalf("Count+Skipped=%d want %d", got, want)
	}
	if got := s.Skipped(); got != 50 {
		t.Fatalf("Skipped()=%d want exactly 50", got)
	}
}

// timeline builds Results spaced 1ms apart from base, with Scheduled and Start
// separated by lag so the two can be told apart.
func timeline(base time.Time, n int, lag time.Duration) []Result {
	out := make([]Result, 0, n)
	for i := range n {
		sched := base.Add(time.Duration(i) * time.Millisecond)
		out = append(out, Result{
			Scheduled: sched,
			Start:     sched.Add(lag),
			Latency:   time.Millisecond,
		})
	}
	return out
}

func TestAfterExcludesResultsScheduledInsideTheWarmup(t *testing.T) {
	base := time.Unix(0, 0)
	stub := &recordingStub{}
	s := After(10*time.Millisecond, stub)

	for _, r := range timeline(base, 30, 0) {
		s.Record(r)
	}

	// Results at 0..9ms are inside the warmup; 10..29ms are admitted.
	if got := len(stub.got); got != 20 {
		t.Fatalf("recorded %d Results, want 20", got)
	}
	if got := s.Skipped(); got != 10 {
		t.Fatalf("Skipped()=%d want 10", got)
	}
	if got := stub.got[0].Scheduled.Sub(base); got != 10*time.Millisecond {
		t.Fatalf("first admitted Result scheduled at %v, want 10ms", got)
	}
}

func TestAfterAnchorsOnScheduledNotOnStart(t *testing.T) {
	// A sagging generator starts units long after they were due. Anchoring on
	// Start would move the whole boundary by the lag; anchoring on the schedule
	// is the v0.3 principle and is what this asserts.
	base := time.Unix(0, 0)
	const lag = 500 * time.Millisecond

	onSchedule := &recordingStub{}
	s := After(10*time.Millisecond, onSchedule)
	for _, r := range timeline(base, 30, lag) {
		s.Record(r)
	}

	if got := len(onSchedule.got); got != 20 {
		t.Fatalf("recorded %d Results, want 20 — the lag must not move the boundary", got)
	}
	if got := onSchedule.got[0].Scheduled.Sub(base); got != 10*time.Millisecond {
		t.Fatalf("first admitted Result scheduled at %v, want 10ms", got)
	}
}

func TestAfterFallsBackToStartWhenThereIsNoSchedule(t *testing.T) {
	base := time.Unix(0, 0)
	stub := &recordingStub{}
	s := After(10*time.Millisecond, stub)

	for i := range 30 {
		s.Record(Result{Start: base.Add(time.Duration(i) * time.Millisecond), Latency: time.Millisecond})
	}

	if got := len(stub.got); got != 20 {
		t.Fatalf("recorded %d Results, want 20", got)
	}
}

func TestAfterSkipsUnplaceableResults(t *testing.T) {
	// No Scheduled and no Start: the Result cannot be put on the timeline, so
	// it cannot be said to be inside or outside the warmup. It is skipped and
	// counted rather than guessed at, and it does not set the anchor.
	stub := &recordingStub{}
	s := After(10*time.Millisecond, stub)

	for range 5 {
		s.Record(Result{Latency: time.Millisecond})
	}

	if got := len(stub.got); got != 0 {
		t.Fatalf("recorded %d unplaceable Results, want 0", got)
	}
	if got := s.Skipped(); got != 5 {
		t.Fatalf("Skipped()=%d want 5", got)
	}

	// A placeable Result arriving afterwards sets the anchor normally.
	base := time.Unix(100, 0)
	for _, r := range timeline(base, 30, 0) {
		s.Record(r)
	}
	if got := len(stub.got); got != 20 {
		t.Fatalf("recorded %d Results after the anchor was set, want 20", got)
	}
}

func TestAfterAdmitsPerResultNotAsAOneWaySwitch(t *testing.T) {
	// Results arrive off a channel fed by N workers, so Scheduled is not
	// monotonic. A unit due at 9ms arriving after one due at 11ms must still be
	// excluded.
	base := time.Unix(0, 0)
	stub := &recordingStub{}
	s := After(10*time.Millisecond, stub)

	at := func(d time.Duration) Result {
		return Result{Scheduled: base.Add(d), Start: base.Add(d), Latency: time.Millisecond}
	}

	s.Record(at(0))                     // anchor, inside
	s.Record(at(11 * time.Millisecond)) // admitted
	s.Record(at(9 * time.Millisecond))  // late arrival, still inside the warmup
	s.Record(at(12 * time.Millisecond)) // admitted

	if got := len(stub.got); got != 2 {
		t.Fatalf("recorded %d Results, want 2", got)
	}
	if got := s.Skipped(); got != 2 {
		t.Fatalf("Skipped()=%d want 2", got)
	}
}

func TestAfterZeroAdmitsEveryPlaceableResult(t *testing.T) {
	base := time.Unix(0, 0)
	stub := &recordingStub{}
	s := After(0, stub)

	for _, r := range timeline(base, 10, 0) {
		s.Record(r)
	}

	if got := len(stub.got); got != 10 {
		t.Fatalf("recorded %d Results, want 10", got)
	}
	if got := s.Skipped(); got != 0 {
		t.Fatalf("Skipped()=%d want 0", got)
	}
}

func TestAfterZeroDoesNotAnchorOnTheFirstResultSeen(t *testing.T) {
	// A zero window has no boundary, so there is nothing to anchor. Anchoring
	// anyway would put the origin at the first Result *seen*, and out-of-order
	// arrival is routine — so every later Result scheduled before it would be
	// excluded from a warmup the caller never asked for. After(0) is what a
	// --warmup flag left unset produces, and it must mean exactly nothing.
	base := time.Unix(0, 0)
	stub := &recordingStub{}
	s := After(0, stub)

	at := func(d time.Duration) Result {
		return Result{Scheduled: base.Add(d), Start: base.Add(d), Latency: time.Millisecond}
	}

	s.Record(at(10100 * time.Millisecond)) // arrives first
	s.Record(at(9900 * time.Millisecond))  // scheduled earlier, arrives later
	s.Record(at(10200 * time.Millisecond))

	if got := len(stub.got); got != 3 {
		t.Fatalf("recorded %d Results, want 3 — After(0) must admit every placeable Result", got)
	}
	if got := s.Skipped(); got != 0 {
		t.Fatalf("Skipped()=%d want 0", got)
	}
}

func TestAfterZeroStillSkipsUnplaceableResults(t *testing.T) {
	// The placement rule stays uniform at d == 0: a Result carrying neither
	// stamp cannot be put on the timeline, so admitting it would be a guess.
	stub := &recordingStub{}
	s := After(0, stub)

	s.Record(Result{Latency: time.Millisecond}) // no Scheduled, no Start
	s.Record(Result{Scheduled: time.Unix(0, 0), Start: time.Unix(0, 0), Latency: time.Millisecond})

	if got := len(stub.got); got != 1 {
		t.Fatalf("recorded %d Results, want 1", got)
	}
	if got := s.Skipped(); got != 1 {
		t.Fatalf("Skipped()=%d want 1", got)
	}
}

func TestAfterLongerThanTheRunAdmitsNothing(t *testing.T) {
	base := time.Unix(0, 0)
	stub := &recordingStub{}
	s := After(time.Hour, stub)

	for _, r := range timeline(base, 30, 0) {
		s.Record(r)
	}

	if got := len(stub.got); got != 0 {
		t.Fatalf("recorded %d Results, want 0", got)
	}
	if got := s.Skipped(); got != 30 {
		t.Fatalf("Skipped()=%d want 30", got)
	}
}

func TestAfterPanicsOnANegativeDuration(t *testing.T) {
	defer func() {
		if recover() == nil {
			t.Fatal("After(-1) did not panic")
		}
	}()
	After(-time.Nanosecond, &recordingStub{})
}

func TestAfterTimeUsesTheGivenOriginExactly(t *testing.T) {
	// After anchors on the first Result seen, which under N workers need not be
	// the earliest scheduled. AfterTime is the exact form for a caller who
	// recorded the run's origin themselves.
	base := time.Unix(0, 0)
	stub := &recordingStub{}
	s := AfterTime(base.Add(10*time.Millisecond), stub)

	// The stream starts late — at 5ms — so After would anchor there and admit
	// from 15ms. AfterTime admits from 10ms regardless.
	for _, r := range timeline(base.Add(5*time.Millisecond), 20, 0) {
		s.Record(r)
	}

	if got := len(stub.got); got != 15 {
		t.Fatalf("recorded %d Results, want 15", got)
	}
	if got := stub.got[0].Scheduled.Sub(base); got != 10*time.Millisecond {
		t.Fatalf("first admitted Result scheduled at %v, want 10ms", got)
	}
}
