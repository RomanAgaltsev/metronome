package metronome

import (
	"errors"
	"strings"
	"testing"
	"time"
)

func TestResultSuccess(t *testing.T) {
	ok := Result{
		Latency: 5 * time.Millisecond,
		Code:    "200",
	}
	if !ok.Success() {
		t.Fatal("Result with nil Err should be Success")
	}
	bad := Result{
		Err: errors.New("boom"),
	}
	if bad.Success() {
		t.Fatal("Result with Err should not be Success")
	}
}

func TestSnapshotStringMarksAWindow(t *testing.T) {
	lifetime := Snapshot{Count: 1}
	if strings.Contains(lifetime.String(), "last ") {
		t.Fatalf("lifetime String() must not claim a window: %q", lifetime.String())
	}

	windowed := Snapshot{Count: 1, Window: 9700 * time.Millisecond}
	if !strings.HasPrefix(windowed.String(), "last 9.7s: ") {
		t.Fatalf("windowed String()=%q want a %q prefix", windowed.String(), "last 9.7s: ")
	}
}
