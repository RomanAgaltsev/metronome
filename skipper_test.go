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
