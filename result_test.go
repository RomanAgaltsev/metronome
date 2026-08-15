package metronome

import (
	"errors"
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
