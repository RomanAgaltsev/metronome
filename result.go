package metronome

import (
	"fmt"
	"time"
)

// Result is the outcome of one unit of work.
type Result struct {
	Start   time.Time
	Latency time.Duration
	Err     error
	Code    string // status/gRPC code, caller-supplied
	Bytes   int64
	Labels  map[string]string // e.g. {"endpoint": "get_user"} for breakdown
}

// Success reports whether the unit of work completed without error.
func (r Result) Success() bool { return r.Err == nil }

// Snapshot is an aggregated view of many Results over a window.
type Snapshot struct {
	Count         int64
	Errors        int64
	RPS           float64
	ErrorRate     float64
	P50, P95, P99 time.Duration
	Max           time.Duration

	// Clamped counts Results whose Latency fell outside the histogram's range
	// and was clamped to a bound. Non-zero means the percentiles above
	// understate reality at one end — widen the range with NewStatsRange. Max
	// always reports the true maximum regardless.
	Clamped int64
}

// PanicError reports a panic raised inside a Runner. The Driver recovers it so
// that one bad unit of work cannot abort a whole load run; the panic is
// delivered as a failed Result instead, and Stack holds the stack captured at
// the point of recovery.
type PanicError struct {
	Value any
	Stack []byte
}

func (e *PanicError) Error() string {
	return fmt.Sprintf("metronome: runner panicked: %v", e.Value)
}
