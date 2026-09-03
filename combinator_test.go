package metronome

import (
	"testing"
	"time"
)

// recordingStub captures the Results it is given, in order, so fan-out order
// and map aliasing can both be asserted.
type recordingStub struct {
	got    []Result
	labels []map[string]string
}

func (s *recordingStub) Record(r Result) {
	s.got = append(s.got, r)
	s.labels = append(s.labels, r.Labels)
}

func (s *recordingStub) Snapshot() Snapshot { return Snapshot{Count: int64(len(s.got))} }

func TestMultiRecordsIntoEveryRecorder(t *testing.T) {
	a, b, c := &recordingStub{}, &recordingStub{}, &recordingStub{}
	m := Multi(a, b, c)

	for i := range 5 {
		m.Record(Result{Start: time.Unix(0, 0).Add(time.Duration(i) * time.Millisecond)})
	}

	for i, s := range []*recordingStub{a, b, c} {
		if got := len(s.got); got != 5 {
			t.Fatalf("recorder %d saw %d Results, want 5", i, got)
		}
	}
}

func TestMultiPreservesArgumentOrder(t *testing.T) {
	var order []string
	mark := func(name string) Recorder {
		return recorderFunc(func(Result) { order = append(order, name) })
	}

	Multi(mark("a"), mark("b"), mark("c")).Record(Result{})

	want := []string{"a", "b", "c"}
	if len(order) != len(want) {
		t.Fatalf("order=%v want %v", order, want)
	}
	for i := range want {
		if order[i] != want[i] {
			t.Fatalf("order=%v want %v", order, want)
		}
	}
}

func TestMultiSnapshotDelegatesToTheFirstRecorder(t *testing.T) {
	// Documented and therefore pinned: there is no non-arbitrary aggregate
	// answer across recorders that may measure different populations.
	first, second := &recordingStub{}, &recordingStub{}
	m := Multi(first, second)

	m.Record(Result{})
	second.Record(Result{}) // second now holds two, first holds one

	if got := m.Snapshot().Count; got != 1 {
		t.Fatalf("Multi Snapshot Count=%d want 1 (the first recorder's)", got)
	}
}

func TestMultiDoesNotShareMutationsOfTheResultLabelsMap(t *testing.T) {
	// Result holds Labels by reference, so every recorder in a Multi sees the
	// same map. Nothing in this package may mutate it.
	a, b := &recordingStub{}, &recordingStub{}
	labels := map[string]string{"endpoint": "list"}

	Multi(a, b).Record(Result{Labels: labels})

	if got := a.labels[0]["endpoint"]; got != "list" {
		t.Fatalf("first recorder saw endpoint=%q want %q", got, "list")
	}
	if got := b.labels[0]["endpoint"]; got != "list" {
		t.Fatalf("second recorder saw endpoint=%q want %q", got, "list")
	}
	if len(labels) != 1 {
		t.Fatalf("the caller's map was mutated: %v", labels)
	}
}

func TestMultiPanicsWithNoRecorders(t *testing.T) {
	defer func() {
		if recover() == nil {
			t.Fatal("Multi() did not panic")
		}
	}()
	Multi()
}

func TestMultiIgnoresLaterMutationOfTheCallersSlice(t *testing.T) {
	a, b := &recordingStub{}, &recordingStub{}
	recs := []Recorder{a, b}
	m := Multi(recs...)

	recs[1] = &recordingStub{} // the caller retains and mutates their slice
	m.Record(Result{})

	if got := len(b.got); got != 1 {
		t.Fatalf("second recorder saw %d Results, want 1", got)
	}
}

// recorderFunc adapts a function to a Recorder for tests that only care about
// the Record side.
type recorderFunc func(Result)

func (f recorderFunc) Record(r Result)    { f(r) }
func (f recorderFunc) Snapshot() Snapshot { return Snapshot{} }
